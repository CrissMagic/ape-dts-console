import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { useOperationNotification } from '../../components/useOperationNotification';
import { Info, Upload, Plus, RefreshCw } from 'lucide-react';
import { Button } from '../../components/ui/button.js';
import { Input } from '../../components/ui/input.js';
import { Label } from '../../components/ui/label.js';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '../../components/ui/dialog.js';
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogCancel,
  AlertDialogAction,
} from '../../components/ui/alert-dialog.js';
import { Badge } from '../../components/ui/badge.js';
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from '../../components/ui/table.js';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '../../components/ui/tooltip.js';
import { Tabs, TabsList, TabsTrigger } from '../../components/ui/tabs.js';
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '../../components/ui/select.js';
import type { ArtifactFileCandidate, ArtifactSummary } from '@scs/contracts';
import {
  listArtifacts,
  deleteArtifact,
  PlatformApiError,
  listArtifactFiles,
  prepareArtifactFile,
  registerArtifactFile,
  uploadArtifactFile,
} from '../../api/platform-api.js';

const errors: Record<string, string> = {
  ARTIFACT_FILE_ALREADY_EXISTS:
    '容器目录中已存在同名文件，请重命名后上传，或切换至“选择容器文件”。',
  UPLOAD_CANCELLED: '上传已取消，可重新上传。',
  NETWORK_ERROR: '网络连接失败，请检查连接后重试。',
  ARTIFACT_FILE_NOT_FOUND: '未找到制品文件，请核对文件已放入平台容器的存储目录。',
  ARTIFACT_FILE_ACCESS_DENIED: '平台无法读取制品文件，请检查文件权限。',
  ARTIFACT_SHA256_MISMATCH: '文件 SHA-256 与填写值不一致。',
  ARTIFACT_SOURCE_SIZE_MISMATCH: '文件大小与填写值不一致。',
  ARTIFACT_VERSION_ALREADY_EXISTS: '该版本标识已存在，请使用其他版本标识。',
  ARTIFACT_OBJECT_CONFLICT: '该路径已登记为其他内容，请使用新的文件路径。',
  ARTIFACT_LOCAL_STORAGE_UNAVAILABLE: '平台尚未配置可写的本地存储。',
  ARTIFACT_FILE_CHANGED: '文件已变更或移除，请刷新文件列表后重新选择。',
  ARTIFACT_PATH_INVALID: '文件路径不安全，请使用制品目录内的普通文件。',
  ARTIFACT_SIZE_LIMIT: '文件或解压后内容超过大小限制，请检查安装包。',
  ARTIFACT_DIRECTORY_LIMIT: '制品目录文件过多，请整理目录后刷新。',
  ARTIFACT_FILE_BUSY: '已有制品正在校验，请稍后重试。',
  ARTIFACT_ARCHIVE_INVALID: '安装包格式无效或包含不安全的条目，请检查文件。',
  ARTIFACT_BINARY_INVALID: '文件不是受支持的 Linux ELF 可执行文件。',
  ARTIFACT_ARCHIVE_LIMIT: '安装包条目过多，请精简安装包。',
  ARTIFACT_ARCHIVE_AMBIGUOUS: '安装包中存在多个 ape-dts 可执行文件，无法确定登记对象。',
  ARTIFACT_ARCHIVE_BINARY_MISSING: '安装包中未找到 ape-dts、ape_dts 或 dt-main 可执行文件。',
  ARTIFACT_ARCHIVE_CLI_ONLY:
    '所选安装包包含 dtscli 工具，但未找到同步引擎。请选择 ape-dts 引擎安装包。',
  ARTIFACT_XZ_UNAVAILABLE: '容器尚未安装 xz 解压工具，请联系管理员。',
  ARTIFACT_ARCHIVE_TIMEOUT: '安装包解压超时，请检查文件后重试。',
  ARTIFACT_STORAGE_PROFILE_CHANGED: '当前存储配置已变更，请刷新文件列表后重新选择。',
  ARTIFACT_NOT_ELF: '文件不是 Linux ELF 可执行文件。',
  ELF_ARCHITECTURE_MISMATCH: '可执行文件架构与运行环境不一致，请选择匹配的 Linux 制品。',
  ARTIFACT_ELF_ARCHITECTURE_MISMATCH: '可执行文件架构与运行环境不一致，请选择匹配的 Linux 制品。',
};

export function ArtifactManagementPage({
  onSessionInvalidated,
}: {
  onSessionInvalidated: () => void;
}) {
  const [artifacts, setArtifacts] = useState<ArtifactSummary[]>([]);
  const [page, setPage] = useState(1);
  const pageCount = Math.max(1, Math.ceil(artifacts.length / 10));
  const currentPage = Math.min(page, pageCount);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [open, setOpen] = useState(false);
  const [deleteCandidate, setDeleteCandidate] = useState<ArtifactSummary | null>(null);
  const deleting = useRef(false);
  const { holder, setError, setNotice } = useOperationNotification();
  const [versionIdentifier, setVersionIdentifier] = useState('');
  const [declaredSource, setDeclaredSource] = useState('');
  const touched = useRef({ versionIdentifier: false, declaredSource: false });
  const [confirmClose, setConfirmClose] = useState(false);
  const [fileSearch, setFileSearch] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);
  function resetFields() {
    setVersionIdentifier('');
    setDeclaredSource('');
    touched.current = { versionIdentifier: false, declaredSource: false };
  }
  const [files, setFiles] = useState<ArtifactFileCandidate[]>([]);
  const [directory, setDirectory] = useState('');
  const [selectedFile, setSelectedFile] = useState<ArtifactFileCandidate | null>(null);
  const [preview, setPreview] = useState<Awaited<ReturnType<typeof prepareArtifactFile>> | null>(
    null,
  );
  const [fileLoading, setFileLoading] = useState(false);
  const [preparing, setPreparing] = useState(false);
  const [modalError, setModalError] = useState<string | null>(null);
  const [source, setSource] = useState('upload');
  const [localFile, setLocalFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const uploadController = useRef<AbortController | null>(null);
  const requestId = useRef(0);
  const submitting = useRef(false);
  const fail = useCallback(
    (cause: unknown) => {
      if (cause instanceof PlatformApiError) {
        if (cause.status === 401) {
          onSessionInvalidated();
          return;
        }
        setError(
          cause.code === 'ARTIFACT_IN_USE'
            ? `制品仍被任务引用，请先解除绑定或处理运行：${cause.artifactReferences.map((task) => task.displayName).join('、') || '请刷新任务列表查看引用'}。`
            : (errors[cause.code] ?? `操作失败（${cause.code}）。`),
        );
      } else setError('操作失败，请稍后重试。');
    },
    [onSessionInvalidated, setError],
  );
  const load = useCallback(async () => {
    setLoading(true);
    try {
      setArtifacts((await listArtifacts()).artifacts);
    } catch (cause) {
      fail(cause);
    } finally {
      setLoading(false);
    }
  }, [fail]);
  useEffect(() => {
    void load();
    return () => {
      requestId.current += 1;
      uploadController.current?.abort();
    };
  }, [load]);
  function fileFailure(cause: unknown) {
    if (cause instanceof PlatformApiError && cause.status === 401) {
      onSessionInvalidated();
      return;
    }
    setModalError(
      cause instanceof PlatformApiError
        ? (errors[cause.code] ?? `文件操作失败（${cause.code}）。`)
        : '文件操作失败，请稍后重试。',
    );
  }
  async function refreshFiles() {
    const id = ++requestId.current;
    setSelectedFile(null);
    setPreview(null);
    setPreparing(false);
    setModalError(null);
    setFileLoading(true);
    setFiles([]);
    try {
      const result = await listArtifactFiles();
      if (requestId.current !== id) return;
      setDirectory(result.directory);
      setFiles(result.files);
    } catch (cause) {
      if (requestId.current === id) fileFailure(cause);
    } finally {
      if (requestId.current === id) setFileLoading(false);
    }
  }
  async function selectFile(name: string) {
    const file = files.find((item) => item.name === name);
    if (!file || submitting.current) return;
    const id = ++requestId.current;
    setSelectedFile(file);
    setPreview(null);
    setModalError(null);
    setPreparing(true);
    try {
      const result = await prepareArtifactFile(file);
      if (requestId.current !== id) return;
      setPreview(result);
      if (!touched.current.versionIdentifier) setVersionIdentifier(result.suggestedVersion);
      if (!touched.current.declaredSource) setDeclaredSource(`容器文件：${file.name}`);
    } catch (cause) {
      if (requestId.current === id) fileFailure(cause);
    } finally {
      if (requestId.current === id) setPreparing(false);
    }
  }
  async function upload(file: File) {
    if (submitting.current) return;
    uploadController.current?.abort();
    const id = ++requestId.current;
    setLocalFile(file);
    setSelectedFile(null);
    setPreview(null);
    setModalError(null);
    setUploadProgress(0);
    if (file.size === 0 || file.size > 512 * 1024 * 1024) {
      setPreparing(false);
      setModalError('请选择非空文件，且大小不超过 512 MiB。');
      return;
    }
    const controller = new AbortController();
    uploadController.current = controller;
    setPreparing(true);
    try {
      const result = await uploadArtifactFile(file, {
        signal: controller.signal,
        onProgress: (percent) => {
          if (requestId.current === id) setUploadProgress(percent);
        },
      });
      if (requestId.current !== id) return;
      setSelectedFile(result.file);
      setPreview(result.prepared);
      if (!touched.current.versionIdentifier)
        setVersionIdentifier(result.prepared.suggestedVersion);
      if (!touched.current.declaredSource) setDeclaredSource(`本地上传：${file.name}`);
    } catch (cause) {
      if (requestId.current === id) fileFailure(cause);
    } finally {
      if (requestId.current === id) {
        setPreparing(false);
        uploadController.current = null;
      }
    }
  }
  function cancelUpload() {
    requestId.current += 1;
    uploadController.current?.abort();
    uploadController.current = null;
    setPreparing(false);
    setModalError(errors.UPLOAD_CANCELLED ?? null);
  }
  function dismiss() {
    requestId.current += 1;
    uploadController.current?.abort();
    uploadController.current = null;
    setLocalFile(null);
    setPreparing(false);
    resetFields();
    setSelectedFile(null);
    setPreview(null);
    setOpen(false);
  }
  async function register(values: { versionIdentifier: string; declaredSource: string }) {
    if (!selectedFile || !preview || preparing || fileLoading || submitting.current) return;
    submitting.current = true;
    setBusy(true);
    setModalError(null);
    try {
      await registerArtifactFile({ file: selectedFile, expectedSha256: preview.sha256, ...values });
      dismiss();
      setNotice('制品已登记，可在任务中直接选择。运行异常请查看启动记录和日志。');
      await load();
    } catch (cause) {
      fileFailure(cause);
    } finally {
      submitting.current = false;
      setBusy(false);
    }
  }
  function close() {
    if (busy || submitting.current) return;
    if (
      !touched.current.versionIdentifier &&
      !touched.current.declaredSource &&
      !selectedFile &&
      !localFile
    ) {
      dismiss();
      return;
    }
    setConfirmClose(true);
  }
  async function removeArtifact() {
    if (!deleteCandidate || deleting.current) return;
    const generation = requestId.current;
    deleting.current = true;
    setBusy(true);
    try {
      await deleteArtifact(deleteCandidate.id);
      if (generation !== requestId.current) return;
      setArtifacts((rows) => rows.filter((row) => row.id !== deleteCandidate.id));
      setDeleteCandidate(null);
      setNotice('制品登记已删除，历史运行、审计记录和原文件仍保留。');
    } catch (cause) {
      if (generation === requestId.current) fail(cause);
    } finally {
      deleting.current = false;
      setBusy(false);
    }
  }
  return (
    <TooltipProvider>
      <section className="space-y-6">
        <div className="flex flex-wrap items-center justify-end gap-3">
          <div className="flex gap-2">
            <Button variant="outline" disabled={loading || busy} onClick={() => void load()}>
              <RefreshCw className="size-4" />
              刷新
            </Button>
            <Button
              disabled={busy}
              onClick={() => {
                resetFields();
                setSource('upload');
                setModalError(null);
                setFileLoading(false);
                setOpen(true);
              }}
            >
              <Plus className="size-4" />
              登记制品
            </Button>
          </div>
        </div>
        {holder}
        <div className="overflow-hidden rounded-lg border bg-card">
          <Table aria-label="运行制品" aria-busy={loading}>
            <TableHeader>
              <TableRow>
                <TableHead>版本</TableHead>
                <TableHead>来源</TableHead>
                <TableHead>状态</TableHead>
                <TableHead>文件</TableHead>
                <TableHead>操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {artifacts.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5} className="h-36 text-center text-muted-foreground">
                    {loading ? '正在加载制品…' : '暂无运行制品，请先登记 ape-dts 文件。'}
                  </TableCell>
                </TableRow>
              )}
              {artifacts.slice((currentPage - 1) * 10, currentPage * 10).map((row) => (
                <TableRow key={row.id}>
                  <TableCell className="font-medium">{row.versionIdentifier}</TableCell>
                  <TableCell className="max-w-64 truncate" title={row.declaredSource}>
                    {row.declaredSource}
                  </TableCell>
                  <TableCell>
                    <Badge variant={row.state === 'QUARANTINED' ? 'destructive' : 'secondary'}>
                      {row.state === 'QUARANTINED' ? '已隔离' : '可使用'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    <span title={row.objectKey}>{row.sizeBytes} 字节</span>
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-destructive"
                      disabled={busy || loading}
                      aria-label={`删除制品 ${row.versionIdentifier}`}
                      onClick={() => setDeleteCandidate(row)}
                    >
                      删除
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {artifacts.length > 0 && (
            <div className="flex flex-wrap items-center justify-between gap-3 border-t px-4 py-3 text-sm">
              <p className="text-muted-foreground">
                共 {artifacts.length} 个制品 · 第 {currentPage} / {pageCount} 页
              </p>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={currentPage <= 1}
                  onClick={() => setPage(currentPage - 1)}
                >
                  上一页
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={currentPage >= pageCount}
                  onClick={() => setPage(currentPage + 1)}
                >
                  下一页
                </Button>
              </div>
            </div>
          )}
        </div>
        <AlertDialog
          open={deleteCandidate !== null}
          onOpenChange={(value) => {
            if (!value && !deleting.current) setDeleteCandidate(null);
          }}
        >
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>删除制品登记？</AlertDialogTitle>
              <AlertDialogDescription>
                删除“{deleteCandidate?.versionIdentifier}
                ”的登记，保留历史运行、审计记录和原文件。被任务引用的制品不能删除。
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel disabled={busy}>取消</AlertDialogCancel>
              <Button variant="destructive" disabled={busy} onClick={() => void removeArtifact()}>
                {busy ? '正在删除…' : '确认删除'}
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
        <Dialog
          open={open}
          onOpenChange={(value) => {
            if (!value) close();
          }}
        >
          <DialogContent
            className="max-h-[90dvh] overflow-y-auto sm:max-w-xl"
            onPointerDownOutside={(event) => event.preventDefault()}
            onEscapeKeyDown={(event) => {
              event.preventDefault();
              if (!confirmClose) close();
            }}
          >
            <DialogHeader>
              <DialogTitle>登记运行制品</DialogTitle>
              <DialogDescription>选择制品文件，校验后确认版本与来源。</DialogDescription>
            </DialogHeader>
            <form
              className="space-y-5"
              onSubmit={(event) => {
                event.preventDefault();
                if (!versionIdentifier.trim() || !declaredSource.trim()) {
                  setModalError('请填写版本标识与来源。');
                  return;
                }
                void register({ versionIdentifier, declaredSource });
              }}
            >
              {modalError && (
                <p
                  role="alert"
                  className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive"
                >
                  {modalError}
                </p>
              )}
              <Tabs
                value={source}
                onValueChange={(key) => {
                  requestId.current += 1;
                  setSource(key);
                  setSelectedFile(null);
                  setPreview(null);
                  setLocalFile(null);
                  setModalError(null);
                  setFileLoading(false);
                  setFileSearch('');
                  if (key === 'container') void refreshFiles();
                }}
              >
                <TabsList className="w-full">
                  <TabsTrigger className="flex-1" value="upload" disabled={busy || preparing}>
                    本地上传
                  </TabsTrigger>
                  <TabsTrigger className="flex-1" value="container" disabled={busy || preparing}>
                    选择容器文件
                  </TabsTrigger>
                </TabsList>
              </Tabs>
              {source === 'upload' ? (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <Label htmlFor="artifact-upload">制品文件</Label>
                    <Help label="上传说明">
                      上传至容器持久存储目录。支持 Linux ELF 二进制、.tar.gz 和
                      .tar.xz，单文件不超过 512 MiB；安装包仅可包含一个 ape-dts 或 ape_dts
                      可执行文件。登记后即可在任务中选择，同名文件不会覆盖。
                    </Help>
                  </div>
                  <input
                    ref={fileInput}
                    id="artifact-upload"
                    type="file"
                    className="sr-only"
                    disabled={busy || preparing}
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) void upload(file);
                      event.target.value = '';
                    }}
                  />
                  <button
                    type="button"
                    disabled={busy || preparing}
                    className="flex w-full flex-col items-center gap-2 rounded-lg border border-dashed bg-muted/25 px-4 py-8 text-sm transition-colors hover:bg-muted/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                    onClick={() => fileInput.current?.click()}
                    onDragOver={(event) => event.preventDefault()}
                    onDrop={(event) => {
                      event.preventDefault();
                      if (busy || preparing) return;
                      const file = event.dataTransfer.files[0];
                      if (file) void upload(file);
                    }}
                  >
                    <Upload className="mb-1 size-7 text-muted-foreground" />
                    <span className="font-medium">点击或拖入制品文件</span>
                    <span className="text-xs text-muted-foreground">
                      Linux ELF / tar.gz / tar.xz · 最大 512 MiB
                    </span>
                  </button>
                  {localFile && <p className="break-all text-sm">{localFile.name}</p>}
                  {preparing && (
                    <div className="space-y-2">
                      <progress
                        aria-label="上传进度"
                        value={uploadProgress}
                        max={100}
                        className="h-2 w-full accent-foreground"
                      />
                      <div className="flex items-center justify-between gap-2">
                        <p role="status" className="text-sm text-muted-foreground">
                          {uploadProgress === 100 ? '上传完成，正在校验…' : '正在上传…'}
                        </p>
                        <Button type="button" variant="ghost" size="sm" onClick={cancelUpload}>
                          取消上传
                        </Button>
                      </div>
                    </div>
                  )}
                  {localFile && !preparing && !preview && modalError && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => void upload(localFile)}
                    >
                      重新上传
                    </Button>
                  )}
                  {preview && (
                    <p role="status" className="text-sm text-muted-foreground">
                      上传及校验完成，请确认版本与来源后登记。
                    </p>
                  )}
                </div>
              ) : (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <Label>容器文件</Label>
                    <Help label="文件目录说明">{`请将 Linux ELF 二进制文件或 .tar.gz、.tar.xz 安装包放入${directory || '配置的制品目录'}。单文件不超过 512 MiB，压缩包仅可包含一个 ape-dts 或 ape_dts 可执行文件，总展开大小不超过 2 GiB。登记后即可在任务中选择。`}</Help>
                  </div>
                  <Input
                    aria-label="搜索容器文件"
                    placeholder="按文件名搜索"
                    value={fileSearch}
                    disabled={busy || fileLoading}
                    onChange={(event) => setFileSearch(event.target.value)}
                  />
                  <div className="flex gap-2">
                    <Select
                      value={selectedFile?.name ?? ''}
                      disabled={busy || fileLoading}
                      onValueChange={(name) => void selectFile(name)}
                    >
                      <SelectTrigger aria-label="容器文件" className="min-w-0 flex-1">
                        <SelectValue
                          placeholder={fileLoading ? '正在扫描目录…' : '请选择制品文件'}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {files
                          .filter((file) =>
                            file.name.toLowerCase().includes(fileSearch.toLowerCase()),
                          )
                          .map((file) => (
                            <SelectItem key={file.name} value={file.name}>
                              {file.name} ·{' '}
                              {file.kind === 'BINARY'
                                ? '二进制'
                                : file.kind === 'TAR_GZ'
                                  ? 'tar.gz'
                                  : 'tar.xz'}{' '}
                              · {file.sizeBytes.toLocaleString()} 字节
                            </SelectItem>
                          ))}
                      </SelectContent>
                    </Select>
                    <Button
                      type="button"
                      variant="outline"
                      disabled={busy || fileLoading}
                      onClick={() => void refreshFiles()}
                    >
                      <RefreshCw className="size-4" />
                      刷新文件
                    </Button>
                  </div>
                  {!fileLoading && files.length === 0 && (
                    <p className="text-sm text-muted-foreground">
                      目录中没有可选制品，请放入文件后刷新。
                    </p>
                  )}
                  {!fileLoading &&
                    files.length > 0 &&
                    !files.some((file) =>
                      file.name.toLowerCase().includes(fileSearch.toLowerCase()),
                    ) && <p className="text-sm text-muted-foreground">没有匹配的制品文件。</p>}
                  {directory && (
                    <p className="break-all text-xs text-muted-foreground">目录：{directory}</p>
                  )}
                  {preparing && (
                    <p role="status" className="text-sm text-muted-foreground">
                      正在校验文件…
                    </p>
                  )}
                  {selectedFile && !preview && !preparing && !fileLoading && modalError && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={busy}
                      onClick={() => void selectFile(selectedFile.name)}
                    >
                      重新校验
                    </Button>
                  )}
                </div>
              )}
              <div className="space-y-2">
                <Label htmlFor="artifact-version">版本标识</Label>
                <Input
                  id="artifact-version"
                  required
                  disabled={busy}
                  value={versionIdentifier}
                  onChange={(event) => {
                    touched.current.versionIdentifier = true;
                    setVersionIdentifier(event.target.value);
                  }}
                  placeholder="例如 ape-dts-20260928"
                  maxLength={128}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="artifact-source">来源</Label>
                <Input
                  id="artifact-source"
                  required
                  disabled={busy}
                  value={declaredSource}
                  onChange={(event) => {
                    touched.current.declaredSource = true;
                    setDeclaredSource(event.target.value);
                  }}
                  placeholder="仓库版本或制品发布地址"
                  maxLength={512}
                />
              </div>
              {preview && (
                <div className="space-y-2 rounded-md bg-muted/50 p-3 text-xs">
                  <p>
                    可执行文件：{preview.fileName} · {preview.sizeBytes.toLocaleString()} 字节
                  </p>
                  <p className="break-all font-mono text-muted-foreground">
                    SHA-256：{preview.sha256}
                  </p>
                </div>
              )}
              <DialogFooter>
                <Button type="button" variant="outline" disabled={busy} onClick={close}>
                  取消
                </Button>
                <Button type="submit" disabled={busy || !preview || preparing || fileLoading}>
                  {busy ? '正在登记…' : '登记制品'}
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
        <AlertDialog open={confirmClose} onOpenChange={setConfirmClose}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>放弃未登记的制品信息？</AlertDialogTitle>
              <AlertDialogDescription>
                {preparing
                  ? '正在上传的文件将中止。'
                  : preview && source === 'upload'
                    ? '已上传的文件会保留在容器目录，可稍后选择并登记。'
                    : '尚未登记的信息将不会保存。'}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>继续填写</AlertDialogCancel>
              <AlertDialogAction onClick={dismiss}>放弃</AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </section>
    </TooltipProvider>
  );
}

function Help({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label}
          className="rounded-sm text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <Info className="size-4" />
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">{children}</TooltipContent>
    </Tooltip>
  );
}
