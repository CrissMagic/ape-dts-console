import { useEffect, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Modal,
  Space,
  Switch,
  Table,
  Tabs,
} from 'antd';
import type { AlertDelivery, AlertSettings, UpdateAlertSettingsRequest } from '@scs/contracts';
import { alertsApi, PlatformApiError } from '../../api/platform-api';
import { useOperationNotification } from '../../components/useOperationNotification';
import { formatDateTime } from '../../lib/date-time';
import { useDraftGuard, type LeaveGuard } from '../../components/useDraftGuard';
import { AlertContextDialog } from './AlertContextDialog';
import { alertErrorCategory } from './alert-error-category';

type Fields = Omit<UpdateAlertSettingsRequest, 'expectedRowVersion'>;
const rules = {
  PLATFORM_METADATA_UNAVAILABLE: '平台元数据库不可用',
  PLATFORM_CONTROL_UNAVAILABLE: '平台控制面不可用',
  START_FAILED: '启动失败',
  UNEXPECTED_EXIT: '运行异常',
  RECOVERY_FAILED: '恢复失败',
  PG_SLOT_INACTIVE: '复制槽未连接',
  PG_SLOT_MISSING: '复制槽不存在',
  PG_RETAINED_WAL: '保留 WAL 超限',
  PG_UNCONFIRMED_WAL: '未确认 WAL 超限',
  KAFKA_OFFSET_LAG: 'Kafka Offset 积压超限',
  SYNC_DELAY: '同步延迟超限',
  TEST: '测试通知',
};

export function AlertSettingsCard({
  onSessionInvalidated,
  api = alertsApi,
  onLeaveGuardChange,
}: {
  onSessionInvalidated: () => void;
  api?: Omit<typeof alertsApi, 'context'> & Partial<Pick<typeof alertsApi, 'context'>>;
  onLeaveGuardChange?: (guard: LeaveGuard | null) => void;
}) {
  const [form] = Form.useForm<Fields>();
  const [settings, setSettings] = useState<AlertSettings | null>(null);
  const [deliveries, setDeliveries] = useState<AlertDelivery[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [testOpen, setTestOpen] = useState(false);
  const [contextId, setContextId] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const { holder, notify } = useOperationNotification();
  const generation = useRef(0);
  const busy = useRef(false);
  const { requestLeave, prompt } = useDraftGuard(dirty, saving || testing, {
    title: '放弃未保存的告警配置？',
    description: '离开后，尚未保存的告警规则与 Webhook 修改将被清除。',
  });
  useEffect(() => {
    onLeaveGuardChange?.(requestLeave);
    return () => onLeaveGuardChange?.(null);
  }, [onLeaveGuardChange, requestLeave]);

  function failure(cause: unknown) {
    if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
    else
      notify(
        'error',
        cause instanceof PlatformApiError && cause.code === 'ALERT_SETTINGS_VERSION_CONFLICT'
          ? '告警配置已被其他管理员修改，请刷新后重试。'
          : cause instanceof PlatformApiError && cause.code === 'ALERT_WEBHOOK_REQUIRED'
            ? '请先配置企业微信机器人 Webhook。'
            : cause instanceof PlatformApiError && cause.code === 'INVALID_WECOM_WEBHOOK'
              ? '请使用有效的企业微信机器人官方 Webhook 地址。'
              : `告警操作失败${cause instanceof PlatformApiError ? `（${cause.code}）` : ''}。`,
      );
  }
  function apply(result: AlertSettings) {
    setSettings(result);
    form.setFieldsValue({
      channel: result.channel,
      enabled: result.enabled,
      webhookUrl: '',
      notifyRecovery: result.notifyRecovery,
      durationSeconds: result.durationSeconds,
      repeatSeconds: result.repeatSeconds,
      slotInactive: result.slotInactive,
      slotMissing: result.slotMissing,
      retainedWalBytes: result.retainedWalBytes,
      unconfirmedWalBytes: result.unconfirmedWalBytes,
      kafkaOffsetLag: result.kafkaOffsetLag,
      delayMilliseconds: result.delayMilliseconds,
    });
    setDirty(false);
  }
  useEffect(() => {
    const current = ++generation.current;
    void api
      .get()
      .then((result) => {
        if (current !== generation.current) return;
        setSettings(result);
        form.setFieldsValue({ ...result, webhookUrl: '' });
      })
      .catch((cause: unknown) => {
        if (current !== generation.current) return;
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        setLoadError('告警配置读取失败，请刷新重试。');
      })
      .finally(() => {
        if (current === generation.current) setLoading(false);
      });
    void api
      .deliveries()
      .then((result) => {
        if (current === generation.current) setDeliveries(result.items);
      })
      .catch(() => {
        if (current === generation.current) setHistoryError('通知记录读取失败。');
      });
    return () => {
      generation.current += 1;
    };
  }, [api, form, onSessionInvalidated]);

  async function refresh() {
    if (busy.current) return;
    const current = generation.current;
    busy.current = true;
    setLoading(true);
    setLoadError(null);
    setHistoryError(null);
    try {
      const result = await api.get();
      if (current !== generation.current) return;
      apply(result);
      try {
        const history = await api.deliveries();
        if (current === generation.current) setDeliveries(history.items);
      } catch (cause) {
        if (current === generation.current) {
          setHistoryError('通知记录刷新失败，当前显示上次读取结果。');
          failure(cause);
        }
      }
    } catch (cause) {
      if (current === generation.current) {
        setLoadError('告警配置刷新失败，请重试。');
        failure(cause);
      }
    } finally {
      busy.current = false;
      if (current === generation.current) setLoading(false);
    }
  }
  async function save(fields: Fields) {
    if (settings === null || busy.current) return;
    const current = generation.current;
    busy.current = true;
    setSaving(true);
    try {
      const { webhookUrl, ...configuration } = fields;
      const result = await api.update({
        ...configuration,
        ...(webhookUrl?.trim() ? { webhookUrl: webhookUrl.trim() } : {}),
        expectedRowVersion: settings.rowVersion,
      });
      if (current !== generation.current) return;
      apply(result);
      notify('success', '告警配置已保存。');
    } catch (cause) {
      if (current === generation.current) failure(cause);
    } finally {
      busy.current = false;
      if (current === generation.current) setSaving(false);
    }
  }
  async function test() {
    if (busy.current || dirty) return;
    const current = generation.current;
    busy.current = true;
    setTesting(true);
    try {
      const delivery = await api.test();
      if (current !== generation.current) return;
      setDeliveries((previous) => [delivery, ...previous].slice(0, 50));
      setTestOpen(false);
      notify(
        delivery.status === 'SENT' ? 'success' : 'error',
        delivery.status === 'SENT'
          ? '测试通知已发送，请在企业微信群中确认。'
          : `测试通知发送失败（${delivery.failureCode ?? 'UNKNOWN'}）。`,
      );
    } catch (cause) {
      if (current === generation.current) failure(cause);
    } finally {
      busy.current = false;
      if (current === generation.current) setTesting(false);
    }
  }
  return (
    <Card loading={loading}>
      {holder}
      {prompt}
      {loadError && <Alert type="error" showIcon title={loadError} />}
      <Tabs
        tabBarExtraContent={
          <Button
            disabled={saving || testing}
            onClick={() =>
              dirty
                ? Modal.confirm({
                    title: '刷新告警配置',
                    content: '刷新将放弃尚未保存的修改。',
                    okText: '刷新',
                    cancelText: '取消',
                    onOk: refresh,
                  })
                : void refresh()
            }
          >
            刷新
          </Button>
        }
        items={[
          {
            key: 'configuration',
            label: '配置与规则',
            children: (
              <Form
                form={form}
                layout="vertical"
                disabled={settings === null || saving || testing}
                onValuesChange={() => setDirty(true)}
                onFinish={(fields: Fields) => void save(fields)}
              >
                <Form.Item name="channel" hidden>
                  <Input />
                </Form.Item>
                <div className="grid gap-x-6 sm:grid-cols-2">
                  <Form.Item
                    label="启用告警"
                    name="enabled"
                    valuePropName="checked"
                    tooltip="平台元数据库或控制面不可用、任务启动失败、异常退出及恢复失败纳入告警。平台异常通知使用最近一次已加载的通知配置；主动停止不触发复制槽未连接告警。"
                  >
                    <Switch aria-label="启用告警" />
                  </Form.Item>
                  <Form.Item label="恢复正常后通知" name="notifyRecovery" valuePropName="checked">
                    <Switch />
                  </Form.Item>
                </div>
                <Form.Item
                  label="企业微信机器人 Webhook"
                  name="webhookUrl"
                  extra={
                    settings?.configured
                      ? '已配置；留空保留原地址，填写新地址可替换。'
                      : '请输入企业微信机器人 HTTPS 地址。'
                  }
                  rules={[
                    {
                      validator: async (_, value: unknown) => {
                        if (value === undefined || value === '') return;
                        if (typeof value !== 'string') throw new Error('请输入有效地址。');
                        const url = new URL(value);
                        if (
                          url.protocol !== 'https:' ||
                          url.hostname !== 'qyapi.weixin.qq.com' ||
                          url.pathname !== '/cgi-bin/webhook/send'
                        )
                          throw new Error('请使用企业微信机器人官方 Webhook 地址。');
                      },
                    },
                  ]}
                >
                  <Input.Password
                    autoComplete="new-password"
                    placeholder="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=…"
                  />
                </Form.Item>
                <div className="grid gap-x-6 sm:grid-cols-2">
                  <Form.Item
                    label="异常持续时间（秒）"
                    name="durationSeconds"
                    rules={[{ required: true }]}
                  >
                    <InputNumber min={0} max={86400} precision={0} className="w-full" />
                  </Form.Item>
                  <Form.Item
                    label="重复通知间隔（秒）"
                    name="repeatSeconds"
                    rules={[{ required: true }]}
                  >
                    <InputNumber min={30} max={604800} precision={0} className="w-full" />
                  </Form.Item>
                  <Form.Item
                    label="运行任务的复制槽持续未连接"
                    name="slotInactive"
                    valuePropName="checked"
                  >
                    <Switch />
                  </Form.Item>
                  <Form.Item
                    label="运行任务的复制槽不存在"
                    name="slotMissing"
                    valuePropName="checked"
                  >
                    <Switch />
                  </Form.Item>
                  {(
                    [
                      { name: 'retainedWalBytes', label: '保留 WAL 阈值（字节）' },
                      { name: 'unconfirmedWalBytes', label: '未确认 WAL 阈值（字节）' },
                      { name: 'kafkaOffsetLag', label: 'Kafka Offset 积压阈值' },
                    ] as const
                  ).map(({ name, label }) => (
                    <Form.Item
                      key={name}
                      name={name}
                      label={label}
                      getValueFromEvent={(event: { target: { value: string } }) =>
                        event.target.value === '' ? null : event.target.value
                      }
                      rules={[{ pattern: /^[1-9]\d*$/u, message: '请输入正整数，留空不启用。' }]}
                    >
                      <Input placeholder="留空不启用" />
                    </Form.Item>
                  ))}
                  <Form.Item label="同步时间延迟阈值（毫秒）" name="delayMilliseconds">
                    <InputNumber
                      min={1}
                      max={2147483647}
                      precision={0}
                      className="w-full"
                      placeholder="不设置则不启用"
                    />
                  </Form.Item>
                </div>
                <Space>
                  <Button
                    type="primary"
                    htmlType="submit"
                    loading={saving}
                    disabled={settings === null}
                  >
                    保存配置
                  </Button>
                  <Button
                    disabled={!settings?.configured || dirty || saving}
                    onClick={() => setTestOpen(true)}
                  >
                    发送测试通知
                  </Button>
                </Space>
              </Form>
            ),
          },
          {
            key: 'deliveries',
            label: '通知记录',
            children: (
              <>
                {historyError && <Alert type="error" title={historyError} />}
                <Table
                  rowKey="id"
                  size="small"
                  dataSource={deliveries}
                  pagination={{
                    defaultPageSize: 10,
                    pageSizeOptions: [10, 20, 50],
                    showSizeChanger: true,
                  }}
                  scroll={{ x: 700 }}
                  columns={[
                    {
                      title: '时间',
                      dataIndex: 'createdAt',
                      render: (value: string) => formatDateTime(value),
                    },
                    {
                      title: '任务',
                      dataIndex: 'taskName',
                      render: (value: string | null | undefined, item: AlertDelivery) =>
                        value ??
                        (item.taskId
                          ? '任务名称不可用'
                          : item.rule === 'TEST'
                            ? '测试通知'
                            : '平台'),
                    },
                    {
                      title: '规则',
                      dataIndex: 'rule',
                      render: (value: string) => rules[value as keyof typeof rules] ?? value,
                    },
                    {
                      title: '事件',
                      dataIndex: 'event',
                      render: (value: string) =>
                        value === 'RECOVERY' ? '告警解除' : value === 'TEST' ? '测试' : '异常',
                    },
                    {
                      title: '结果',
                      dataIndex: 'status',
                      render: (value: string) =>
                        value === 'SENT' ? '已发送' : value === 'FAILED' ? '发送失败' : value,
                    },
                    {
                      title: '异常分类',
                      render: (_value: unknown, item: AlertDelivery) => alertErrorCategory(item),
                    },
                    {
                      title: '上下文',
                      render: (_value: unknown, item: AlertDelivery) => (
                        <Button
                          type="link"
                          size="small"
                          onClick={() => setContextId(item.id)}
                          aria-label={`${item.taskName ?? '平台'} 查看告警上下文`}
                        >
                          查看上下文
                        </Button>
                      ),
                    },
                  ]}
                />
              </>
            ),
          },
        ]}
      />
      <Modal
        title="发送测试通知"
        open={testOpen}
        onCancel={() => !testing && setTestOpen(false)}
        onOk={() => void test()}
        confirmLoading={testing}
        cancelButtonProps={{ disabled: testing }}
        okText="发送"
        cancelText="取消"
      >
        <p>向已保存的企业微信机器人发送一条测试消息，不会启动或修改同步任务。</p>
      </Modal>
      <AlertContextDialog
        deliveryId={contextId}
        onClose={() => setContextId(null)}
        onSessionInvalidated={onSessionInvalidated}
        loadContext={api.context ?? alertsApi.context}
      />
    </Card>
  );
}
