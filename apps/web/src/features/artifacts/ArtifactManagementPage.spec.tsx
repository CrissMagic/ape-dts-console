import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import * as api from '../../api/platform-api.js';
import { ArtifactManagementPage } from './ArtifactManagementPage.js';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
describe('ArtifactManagementPage', () => {
  const artifact = {
    id: 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb',
    versionIdentifier: 'ape-dts-test',
    declaredSource: 'test',
    objectKey: 'test/ape-dts',
    sha256: 'a'.repeat(64),
    sizeBytes: '100',
    state: 'ACTIVE' as const,
    stateReason: null,
    verified: true,
    certifications: [],
  };
  it('confirms deletion, removes the row and notifies that files and history remain', async () => {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({ artifacts: [artifact] });
    const remove = vi.spyOn(api, 'deleteArtifact').mockResolvedValue({ deleted: true });
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '删除制品 ape-dts-test' }));
    expect(screen.queryByRole('heading', { name: '运行制品' })).not.toBeInTheDocument();
    expect(screen.queryByLabelText('制品说明')).not.toBeInTheDocument();
    expect(remove).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));
    expect(
      await screen.findByText('制品登记已删除，历史运行、审计记录和原文件仍保留。'),
    ).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '删除制品 ape-dts-test' })).not.toBeInTheDocument();
    expect(remove).toHaveBeenCalledWith(artifact.id);
  });
  it('retains the artifact and identifies referring tasks when deletion is blocked', async () => {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({ artifacts: [artifact] });
    const error = new api.PlatformApiError('ARTIFACT_IN_USE', 409);
    error.artifactReferences = [
      { id: 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa', displayName: '测试同步' },
    ];
    vi.spyOn(api, 'deleteArtifact').mockRejectedValue(error);
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '删除制品 ape-dts-test' }));
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));
    expect(
      await screen.findByText('制品仍被任务引用，请先解除绑定或处理运行：测试同步。'),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(screen.getByRole('button', { name: '删除制品 ape-dts-test' })).toBeInTheDocument();
  });
  const file = {
    name: 'ape-dts.tar.gz',
    sizeBytes: 1024,
    modifiedAt: '2026-09-28T00:00:00.000Z',
    kind: 'TAR_GZ' as const,
  };
  const preview = {
    fileName: 'ape-dts',
    sha256: 'a'.repeat(64),
    sizeBytes: 2048,
    suggestedVersion: 'ape-dts-1',
  };
  async function openPicker() {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({ artifacts: [] });
    vi.spyOn(api, 'listArtifactFiles').mockResolvedValue({
      directory: '/data/artifact-inbox',
      files: [file],
    });
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: '登记制品' }));
    fireEvent.mouseDown(screen.getByRole('tab', { name: '选择容器文件' }), {
      button: 0,
      ctrlKey: false,
    });
    await screen.findByText('目录：/data/artifact-inbox');
    return within(screen.getByRole('dialog'));
  }
  async function chooseFile() {
    fireEvent.keyDown(screen.getByRole('combobox', { name: '容器文件' }), { key: 'ArrowDown' });
    fireEvent.click(
      await screen.findByRole('option', { name: 'ape-dts.tar.gz · tar.gz · 1,024 字节' }),
    );
  }
  function openUpload() {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({ artifacts: [] });
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: '登记制品' }));
    return within(screen.getByRole('dialog'));
  }
  function chooseUpload(fileToUpload = new File(['elf'], file.name)) {
    const input = document.querySelector<HTMLInputElement>('input[type="file"]');
    if (!input) throw new Error('Upload input missing');
    fireEvent.change(input, { target: { files: [fileToUpload] } });
  }
  it('explains an uploaded CLI package and keeps registration disabled', async () => {
    vi.spyOn(api, 'uploadArtifactFile').mockRejectedValue(
      new api.PlatformApiError('ARTIFACT_ARCHIVE_CLI_ONLY', 409),
    );
    const modal = openUpload();
    chooseUpload(new File(['cli'], 'dtscli.tar.gz'));
    expect(
      await modal.findByText(
        '所选安装包包含 dtscli 工具，但未找到同步引擎。请选择 ape-dts 引擎安装包。',
      ),
    ).toBeInTheDocument();
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
  });
  it('defaults to uploading and registers the validated upload with computed metadata', async () => {
    vi.spyOn(api, 'uploadArtifactFile').mockResolvedValue({ file, prepared: preview });
    const scan = vi.spyOn(api, 'listArtifactFiles');
    const register = vi
      .spyOn(api, 'registerArtifactFile')
      .mockResolvedValue({} as Awaited<ReturnType<typeof api.registerArtifactFile>>);
    const modal = openUpload();
    expect(screen.getByRole('tab', { name: '本地上传' })).toHaveAttribute('aria-selected', 'true');
    expect(scan).not.toHaveBeenCalled();
    chooseUpload();
    await waitFor(() => expect(modal.getByLabelText('版本标识')).toHaveValue('ape-dts-1'));
    expect(modal.getByLabelText('来源')).toHaveValue('本地上传：ape-dts.tar.gz');
    fireEvent.click(modal.getByRole('button', { name: '登记制品' }));
    await waitFor(() =>
      expect(register).toHaveBeenCalledWith({
        file,
        expectedSha256: preview.sha256,
        versionIdentifier: 'ape-dts-1',
        declaredSource: '本地上传：ape-dts.tar.gz',
      }),
    );
  });
  it('keeps registration disabled at 100 percent until server validation completes and cancels stale completion', async () => {
    let resolveUpload:
      ((value: { file: typeof file; prepared: typeof preview }) => void) | undefined;
    let signal: AbortSignal | undefined;
    vi.spyOn(api, 'uploadArtifactFile').mockImplementation((_file, options) => {
      signal = options.signal;
      options.onProgress(100);
      return new Promise((resolve) => {
        resolveUpload = resolve;
      });
    });
    const modal = openUpload();
    chooseUpload();
    expect(await modal.findByText('上传完成，正在校验…')).toBeInTheDocument();
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
    fireEvent.click(modal.getByRole('button', { name: '取消上传' }));
    expect(signal?.aborted).toBe(true);
    resolveUpload?.({ file, prepared: preview });
    await waitFor(() =>
      expect(modal.getByRole('button', { name: '重新上传' })).toBeInTheDocument(),
    );
    expect(modal.queryByText(`SHA-256：${preview.sha256}`)).not.toBeInTheDocument();
  });
  it('shows duplicate upload errors and retries without losing manually entered version', async () => {
    vi.spyOn(api, 'uploadArtifactFile')
      .mockRejectedValueOnce(new api.PlatformApiError('ARTIFACT_FILE_ALREADY_EXISTS', 409))
      .mockResolvedValueOnce({ file, prepared: preview });
    const modal = openUpload();
    fireEvent.change(modal.getByLabelText('版本标识'), { target: { value: 'manual-version' } });
    chooseUpload();
    expect(
      await modal.findByText('容器目录中已存在同名文件，请重命名后上传，或切换至“选择容器文件”。'),
    ).toBeInTheDocument();
    fireEvent.click(modal.getByRole('button', { name: '重新上传' }));
    expect(await modal.findByText(`SHA-256：${preview.sha256}`)).toBeInTheDocument();
    expect(modal.getByLabelText('版本标识')).toHaveValue('manual-version');
  });
  it('aborts an active upload after confirming close and ignores its late response', async () => {
    let resolveUpload:
      ((value: { file: typeof file; prepared: typeof preview }) => void) | undefined;
    let signal: AbortSignal | undefined;
    vi.spyOn(api, 'uploadArtifactFile').mockImplementation((_file, options) => {
      signal = options.signal;
      return new Promise((resolve) => {
        resolveUpload = resolve;
      });
    });
    const modal = openUpload();
    chooseUpload();
    await modal.findByText('正在上传…');
    fireEvent.click(modal.getByRole('button', { name: /^取\s*消$/u }));
    const confirmation = screen.getByRole('alertdialog');
    expect(within(confirmation).getByText('放弃未登记的制品信息？')).toBeInTheDocument();
    fireEvent.click(within(confirmation).getByRole('button', { name: '放弃' }));
    await waitFor(() => expect(signal?.aborted).toBe(true));
    resolveUpload?.({ file, prepared: preview });
    fireEvent.click(screen.getByRole('button', { name: '登记制品' }));
    expect(
      within(screen.getByRole('dialog')).getByRole('button', { name: '登记制品' }),
    ).toBeDisabled();
    expect(screen.queryByText(`SHA-256：${preview.sha256}`)).not.toBeInTheDocument();
  });
  it('rejects empty and oversized uploads before making a request', async () => {
    const upload = vi.spyOn(api, 'uploadArtifactFile');
    const modal = openUpload();
    chooseUpload(new File([], 'empty'));
    expect(await modal.findByText('请选择非空文件，且大小不超过 512 MiB。')).toBeInTheDocument();
    const oversized = new File(['elf'], 'large');
    Object.defineProperty(oversized, 'size', { value: 512 * 1024 * 1024 + 1 });
    chooseUpload(oversized);
    await waitFor(() => expect(modal.getByText('large')).toBeInTheDocument());
    expect(upload).not.toHaveBeenCalled();
  });
  it('protects edited fields on Escape and resumes the dialog without losing input', async () => {
    const modal = openUpload();
    fireEvent.change(modal.getByLabelText('来源'), { target: { value: '内部构建' } });
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    const confirmation = screen.getByRole('alertdialog');
    fireEvent.click(within(confirmation).getByRole('button', { name: '继续填写' }));
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument();
    expect(modal.getByLabelText('来源')).toHaveValue('内部构建');
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    fireEvent.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: '放弃' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
  it('accepts a dropped file and retains the manually entered source', async () => {
    const upload = vi
      .spyOn(api, 'uploadArtifactFile')
      .mockResolvedValue({ file, prepared: preview });
    const modal = openUpload();
    fireEvent.change(modal.getByLabelText('来源'), { target: { value: '已审核构建' } });
    const dropped = new File(['elf'], file.name);
    fireEvent.drop(modal.getByRole('button', { name: /点击或拖入制品文件/u }), {
      dataTransfer: { files: [dropped] },
    });
    await waitFor(() => expect(modal.getByLabelText('版本标识')).toHaveValue('ape-dts-1'));
    expect(upload).toHaveBeenCalledWith(dropped, expect.any(Object));
    expect(modal.getByLabelText('来源')).toHaveValue('已审核构建');
  });
  it('rejects whitespace-only metadata without registering the uploaded file', async () => {
    vi.spyOn(api, 'uploadArtifactFile').mockResolvedValue({ file, prepared: preview });
    const register = vi.spyOn(api, 'registerArtifactFile');
    const modal = openUpload();
    chooseUpload();
    await modal.findByText(`SHA-256：${preview.sha256}`);
    fireEvent.change(modal.getByLabelText('版本标识'), { target: { value: '   ' } });
    fireEvent.click(modal.getByRole('button', { name: '登记制品' }));
    expect(modal.getByText('请填写版本标识与来源。')).toBeInTheDocument();
    expect(register).not.toHaveBeenCalled();
  });
  it('selects a container archive and registers computed integrity data without manual hashes', async () => {
    vi.spyOn(api, 'prepareArtifactFile').mockResolvedValue(preview);
    const register = vi
      .spyOn(api, 'registerArtifactFile')
      .mockResolvedValue({} as Awaited<ReturnType<typeof api.registerArtifactFile>>);
    const modal = await openPicker();
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
    await chooseFile();
    await waitFor(() => expect(modal.getByLabelText('版本标识')).toHaveValue('ape-dts-1'));
    expect(modal.getByLabelText('来源')).toHaveValue('容器文件：ape-dts.tar.gz');
    expect(modal.getByText(`SHA-256：${preview.sha256}`)).toBeInTheDocument();
    fireEvent.change(modal.getByLabelText('版本标识'), { target: { value: 'release-2' } });
    fireEvent.click(modal.getByRole('button', { name: '登记制品' }));
    await waitFor(() =>
      expect(register).toHaveBeenCalledWith({
        file,
        expectedSha256: preview.sha256,
        versionIdentifier: 'release-2',
        declaredSource: '容器文件：ape-dts.tar.gz',
      }),
    );
    expect(
      await screen.findByText('制品已登记，可在任务中直接选择。运行异常请查看启动记录和日志。'),
    ).toBeInTheDocument();
  });
  it('shows file errors inside the registration dialog and blocks registration', async () => {
    vi.spyOn(api, 'prepareArtifactFile').mockRejectedValue(
      new api.PlatformApiError('ARTIFACT_FILE_NOT_FOUND', 404),
    );
    const modal = await openPicker();
    await chooseFile();
    expect(
      await modal.findByText('未找到制品文件，请核对文件已放入平台容器的存储目录。'),
    ).toBeInTheDocument();
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
  });
  it('can retry a busy preparation without losing the selected file or user edits', async () => {
    vi.spyOn(api, 'prepareArtifactFile')
      .mockRejectedValueOnce(new api.PlatformApiError('ARTIFACT_FILE_BUSY', 409))
      .mockResolvedValueOnce(preview);
    const modal = await openPicker();
    fireEvent.change(modal.getByLabelText('版本标识'), { target: { value: 'manual-version' } });
    await chooseFile();
    fireEvent.click(await modal.findByRole('button', { name: '重新校验' }));
    expect(await modal.findByText(`SHA-256：${preview.sha256}`)).toBeInTheDocument();
    expect(modal.getByLabelText('版本标识')).toHaveValue('manual-version');
    expect(api.prepareArtifactFile).toHaveBeenCalledTimes(2);
    expect(modal.getByRole('button', { name: '登记制品' })).not.toBeDisabled();
  });
  it('shows an empty directory explicitly and keeps registration disabled', async () => {
    const modal = await openPicker();
    vi.mocked(api.listArtifactFiles).mockResolvedValue({
      directory: '/data/artifact-inbox',
      files: [],
    });
    await waitFor(() =>
      expect(modal.getByRole('button', { name: /刷新文件/u })).not.toBeDisabled(),
    );
    fireEvent.click(modal.getByRole('button', { name: /刷新文件/u }));
    await waitFor(() =>
      expect(modal.getByRole('combobox', { name: '容器文件' })).not.toBeDisabled(),
    );
    expect(await screen.findByText('目录中没有可选制品，请放入文件后刷新。')).toBeInTheDocument();
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
  });
  it('filters container filenames without selecting or preparing a hidden file', async () => {
    const prepare = vi.spyOn(api, 'prepareArtifactFile');
    const modal = await openPicker();
    fireEvent.change(modal.getByLabelText('搜索容器文件'), { target: { value: 'missing' } });
    expect(modal.getByText('没有匹配的制品文件。')).toBeInTheDocument();
    expect(prepare).not.toHaveBeenCalled();
    fireEvent.change(modal.getByLabelText('搜索容器文件'), { target: { value: 'APE-DTS' } });
    expect(modal.queryByText('没有匹配的制品文件。')).not.toBeInTheDocument();
  });
  it('paginates artifact rows and keeps operations available on the next page', async () => {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({
      artifacts: Array.from({ length: 11 }, (_, index) => ({
        id: `artifact-${index}`,
        versionIdentifier: `candidate-${index}`,
        declaredSource: 'local build',
        objectKey: `artifacts/candidate-${index}`,
        sha256: 'a'.repeat(64),
        sizeBytes: '1024',
        state: 'INACTIVE' as const,
        stateReason: 'AWAITING_BEHAVIOR_CERTIFICATION',
        verified: true,
        certifications: [],
      })),
    });
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    await screen.findByText('candidate-0');
    expect(screen.queryByText('candidate-10')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '上一页' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    expect(screen.getByText('candidate-10')).toBeInTheDocument();
    expect(screen.queryByText('candidate-0')).not.toBeInTheDocument();
    expect(screen.getByText('可使用')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '下一页' })).toBeDisabled();
  });
  it('ignores a stale preview after refresh and preserves user-entered version', async () => {
    let resolvePreview: ((value: typeof preview) => void) | undefined;
    vi.spyOn(api, 'prepareArtifactFile').mockReturnValue(
      new Promise((resolve) => {
        resolvePreview = resolve;
      }),
    );
    const modal = await openPicker();
    fireEvent.change(modal.getByLabelText('版本标识'), { target: { value: 'user-version' } });
    await chooseFile();
    await waitFor(() =>
      expect(modal.getByRole('button', { name: /刷新文件/u })).not.toBeDisabled(),
    );
    fireEvent.click(modal.getByRole('button', { name: /刷新文件/u }));
    await waitFor(() => expect(api.listArtifactFiles).toHaveBeenCalledTimes(2));
    resolvePreview?.(preview);
    await waitFor(() =>
      expect(modal.queryByText(`SHA-256：${preview.sha256}`)).not.toBeInTheDocument(),
    );
    expect(modal.getByLabelText('版本标识')).toHaveValue('user-version');
    expect(modal.getByRole('button', { name: '登记制品' })).toBeDisabled();
  });
  it('shows an existing upload as usable without certification or manual activation', async () => {
    vi.spyOn(api, 'listArtifacts').mockResolvedValue({
      artifacts: [
        {
          id: 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb',
          versionIdentifier: 'candidate-1',
          declaredSource: 'local build',
          objectKey: 'artifacts/candidate',
          sha256: 'a'.repeat(64),
          sizeBytes: '1024',
          state: 'INACTIVE',
          stateReason: 'AWAITING_BEHAVIOR_CERTIFICATION',
          verified: true,
          certifications: [],
        },
      ],
    });
    render(<ArtifactManagementPage onSessionInvalidated={vi.fn()} />);
    expect(await screen.findByText('candidate-1')).toBeInTheDocument();
    expect(screen.getByText('可使用')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '启用' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '校验' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '认证指引' })).not.toBeInTheDocument();
  });
  it('invalidates an expired session rather than presenting an empty artifact list as a success', async () => {
    vi.spyOn(api, 'listArtifacts').mockRejectedValue(
      new api.PlatformApiError('SESSION_INVALID', 401),
    );
    const invalidated = vi.fn();
    render(<ArtifactManagementPage onSessionInvalidated={invalidated} />);
    await waitFor(() => expect(invalidated).toHaveBeenCalledOnce());
  });
});
