import { useRef, useState } from 'react';
import { Alert, Button, Checkbox, Collapse, Descriptions, Modal, Space, Tabs, Upload } from 'antd';
import { Download, Upload as UploadIcon } from 'lucide-react';
import {
  CONFIG_BUNDLE_MAX_BYTES,
  configBundleSchema,
  type ConfigBundle,
  type ConfigImportPreview,
} from '@scs/contracts';
import { configTransferApi, PlatformApiError } from '../../api/platform-api';
import { useOperationNotification } from '../../components/useOperationNotification';
import { formatDateTime } from '../../lib/date-time';

const labels = { dataSources: '数据源', tasks: '同步任务及任务组', system: '系统配置' };
const errors: Record<string, string> = {
  CONFIG_BUNDLE_INVALID: '文件格式无效或版本不受支持。',
  CONFIG_BUNDLE_TOO_LARGE: '配置文件超过 512 KiB，超出当前导入导出上限。',
  CONFIG_EXPORT_INVALID_CONFIGURATION: '存在无法迁移的配置，请先检查任务草稿和制品信息。',
  CONFIG_BUNDLE_TASK_INVALID: '文件中的任务配置无效或来源、目标引用不一致。',
  CONFIG_BUNDLE_DEPENDENCY_MISSING: '文件缺少任务依赖的数据源或任务组。',
  CONFIG_BUNDLE_DUPLICATE_NAME: '文件存在重复名称，请先调整。',
  CONFIG_BUNDLE_DUPLICATE_ID: '文件存在重复的来源标识。',
  CONFIG_IMPORT_RECHECK_REQUIRED: '目标配置已变化或存在冲突，请重新读取预览后确认。',
  CONFIG_SYSTEM_SETTINGS_UNAVAILABLE: '平台系统配置尚未就绪，不能导入导出。',
  DATA_SOURCE_NAME_ALREADY_EXISTS: '数据源名称已存在，请调整后重新导入。',
  TASK_NAME_ALREADY_EXISTS: '任务名称已存在，请调整后重新导入。',
};

export function ConfigTransferCard({
  onSessionInvalidated,
  api = configTransferApi,
}: {
  onSessionInvalidated: () => void;
  api?: typeof configTransferApi;
}) {
  const [options, setOptions] = useState({ dataSources: true, tasks: true, system: false });
  const [busy, setBusy] = useState(false);
  const pending = useRef(false);
  const [bundle, setBundle] = useState<ConfigBundle | null>(null);
  const [preview, setPreview] = useState<ConfigImportPreview | null>(null);
  const [confirm, setConfirm] = useState(false);
  const [fileName, setFileName] = useState('');
  const { holder, setError, setNotice } = useOperationNotification();
  function failed(error: unknown) {
    if (error instanceof PlatformApiError && error.status === 401) {
      onSessionInvalidated();
      return;
    }
    setError(
      error instanceof PlatformApiError
        ? (errors[error.code] ?? `操作失败（${error.code}）。`)
        : error instanceof Error
          ? error.message
          : '操作失败，请重试。',
    );
  }
  async function exporting() {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const packet = await api.export(options);
      const blob = new Blob([JSON.stringify(packet, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `scs-config-${packet.exportedAt.replace(/\D/gu, '').slice(0, 14)}.json`;
      link.click();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      setNotice('配置已导出，不包含密码、Webhook 或运行数据。');
    } catch (error) {
      failed(error);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }
  async function readFile(file: File) {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setPreview(null);
    setBundle(null);
    setConfirm(false);
    setFileName(file.name);
    try {
      if (file.size > CONFIG_BUNDLE_MAX_BYTES)
        throw new Error('配置文件超过 512 KiB，超出当前导入导出上限。');
      const parsed = configBundleSchema.safeParse(JSON.parse(await file.text()));
      if (!parsed.success) throw new Error('文件格式无效或版本不受支持。');
      const result = await api.preview(parsed.data);
      setBundle(parsed.data);
      setPreview(result);
    } catch (error) {
      failed(error instanceof SyntaxError ? new Error('文件不是有效的 JSON 配置。') : error);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }
  async function importing() {
    if (!bundle || !preview || preview.conflicts.length || pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await api.import(bundle, preview.previewHash);
      setConfirm(false);
      setBundle(null);
      setPreview(null);
      setNotice(
        `已导入 ${result.dataSources} 个数据源、${result.tasks} 个任务。请补配凭据并重新预检，任务未自动启动。`,
      );
    } catch (error) {
      setConfirm(false);
      setPreview(null);
      failed(error);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }
  return (
    <div className="space-y-4">
      {holder}
      <details className="text-sm text-muted-foreground">
        <summary className="cursor-pointer">继续使用原平台数据库？</summary>
        <p className="mt-2 leading-6">
          保留配套 storage（含 .platform/config.json
          密钥文件），直接启动即可沿用原配置与凭据，无需重复导入。以下功能用于向另一套平台迁入配置。
        </p>
      </details>
      <Alert
        type="info"
        showIcon
        title="仅迁移配置，不包含密码、Webhook、账号、历史数据或运行位点。"
      />
      <Tabs
        items={[
          {
            key: 'export',
            label: '导出配置',
            children: (
              <Space orientation="vertical" size={20}>
                <Checkbox.Group
                  value={Object.keys(options).filter((key) => options[key as keyof typeof options])}
                  disabled={busy}
                  onChange={(keys) =>
                    setOptions({
                      dataSources: keys.includes('dataSources'),
                      tasks: keys.includes('tasks'),
                      system: keys.includes('system'),
                    })
                  }
                  options={Object.entries(labels).map(([value, label]) => ({ value, label }))}
                />
                <p className="text-sm text-muted-foreground">
                  导出任务时自动包含其依赖的数据源。系统配置包含容量、会话策略及告警规则；不包含部署环境参数。制品只保留版本及校验值，不打包二进制文件。
                </p>
                <Button
                  type="primary"
                  icon={<Download className="size-4" />}
                  loading={busy}
                  disabled={!Object.values(options).some(Boolean)}
                  onClick={() => void exporting()}
                >
                  下载配置文件
                </Button>
              </Space>
            ),
          },
          {
            key: 'import',
            label: '导入配置',
            children: (
              <div className="space-y-4">
                <Upload
                  accept=".json,application/json"
                  maxCount={1}
                  showUploadList={false}
                  disabled={busy}
                  beforeUpload={(file) => {
                    void readFile(file);
                    return Upload.LIST_IGNORE;
                  }}
                >
                  <Button icon={<UploadIcon className="size-4" />} loading={busy}>
                    选择文件并预览
                  </Button>
                </Upload>
                {preview && (
                  <>
                    <p className="text-sm text-muted-foreground">
                      {fileName} · 导出于 {bundle ? formatDateTime(bundle.exportedAt) : '—'}
                    </p>
                    <dl className="grid grid-cols-2 gap-4 rounded-lg border p-4 text-sm sm:grid-cols-4">
                      {[
                        ['数据源', preview.dataSources],
                        ['同步任务', preview.tasks],
                        ['任务组', preview.groups],
                        ['系统配置', preview.system ? '包含' : '不包含'],
                      ].map(([label, value]) => (
                        <div key={label}>
                          <dt className="text-muted-foreground">{label}</dt>
                          <dd className="mt-1 font-medium">{value}</dd>
                        </div>
                      ))}
                    </dl>
                    {preview.warnings.map((text) => (
                      <p key={text} className="text-sm text-muted-foreground">
                        {text}
                      </p>
                    ))}
                    {bundle && (
                      <Collapse
                        size="small"
                        items={[
                          {
                            key: 'contents',
                            label: '查看配置清单',
                            children: (
                              <div className="max-h-48 space-y-2 overflow-auto text-sm">
                                <p>
                                  数据源：
                                  {bundle.dataSources.map((item) => item.name).join('、') || '无'}
                                </p>
                                <p>
                                  同步任务：
                                  {bundle.tasks.map((item) => item.name).join('、') || '无'}
                                </p>
                                <p>
                                  任务组：
                                  {bundle.groups.map((item) => item.name).join('、') || '无'}
                                </p>
                                {bundle.system && (
                                  <Descriptions
                                    column={1}
                                    size="small"
                                    items={[
                                      {
                                        key: 'capacity',
                                        label: '运行容量',
                                        children: bundle.system.maxRuntimeUnits ?? '未配置',
                                      },
                                      {
                                        key: 'idle',
                                        label: '登录空闲时长',
                                        children: `${bundle.system.sessions.idleTimeoutMinutes} 分钟`,
                                      },
                                      {
                                        key: 'absolute',
                                        label: '最长登录时长',
                                        children: `${bundle.system.sessions.absoluteTimeoutHours} 小时`,
                                      },
                                      {
                                        key: 'recovery',
                                        label: '任务自动恢复',
                                        children: bundle.system.recovery
                                          ? `${bundle.system.recovery.enabled ? '开启' : '关闭'}，最多 ${bundle.system.recovery.maxAttempts} 次，间隔 ${bundle.system.recovery.retryDelaySeconds} 秒`
                                          : '保留目标平台原设置',
                                      },
                                      {
                                        key: 'alerts',
                                        label: '告警通知',
                                        children: '导入规则后关闭，凭据须单独配置',
                                      },
                                    ]}
                                  />
                                )}
                              </div>
                            ),
                          },
                        ]}
                      />
                    )}
                    {preview.conflicts.length > 0 && (
                      <Alert
                        type="error"
                        showIcon
                        title="存在冲突，未写入任何配置。"
                        description={
                          <ul>
                            {preview.conflicts.map((text) => (
                              <li key={text}>{text}</li>
                            ))}
                          </ul>
                        }
                      />
                    )}
                    <Button
                      type="primary"
                      disabled={preview.conflicts.length > 0 || busy}
                      onClick={() => setConfirm(true)}
                    >
                      确认导入
                    </Button>
                  </>
                )}
              </div>
            ),
          },
        ]}
      />
      <Modal
        open={confirm}
        title="确认导入配置"
        onCancel={() => !busy && setConfirm(false)}
        onOk={() => void importing()}
        confirmLoading={busy}
        okText="执行导入"
        cancelText="取消"
        closable={!busy}
        mask={{ closable: !busy }}
        keyboard={!busy}
      >
        <p>
          数据源和任务将作为新配置导入，任务保持停止。
          {preview?.system ? '系统配置将替换当前容量、会话策略及告警规则，告警会关闭。' : ''}密码与
          Webhook 须重新配置。是否继续？
        </p>
      </Modal>
    </div>
  );
}
