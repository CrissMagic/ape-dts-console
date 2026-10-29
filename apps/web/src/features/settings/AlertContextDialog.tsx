import { useEffect, useState } from 'react';
import { Alert, Button, Descriptions, Modal, Select, Space, Spin, Table } from 'antd';
import type { AlertDeliveryContext, RuntimeLogRead } from '@scs/contracts';
import { alertsApi, tasksApi, PlatformApiError } from '../../api/platform-api';
import { formatDateTime } from '../../lib/date-time';
import { auditActionLabels, operationLabel } from '../../lib/operation-labels';
import { alertErrorCategory } from './alert-error-category';

export function AlertContextDialog({
  deliveryId,
  onClose,
  onSessionInvalidated,
  loadContext = alertsApi.context,
  loadLogs = tasksApi.logs,
}: {
  deliveryId: string | null;
  onClose: () => void;
  onSessionInvalidated: () => void;
  loadContext?: typeof alertsApi.context;
  loadLogs?: typeof tasksApi.logs;
}) {
  const [context, setContext] = useState<AlertDeliveryContext | null>(null);
  const [logs, setLogs] = useState<RuntimeLogRead | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [logError, setLogError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [logLoading, setLogLoading] = useState(false);
  const [stream, setStream] = useState<'STDERR' | 'STDOUT' | 'ALL'>('STDERR');
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let active = true;
    setContext(null);
    setError(null);
    setLogs(null);
    setLogError(null);
    setStream('STDERR');
    if (!deliveryId) {
      setLoading(false);
      return;
    }
    setLoading(true);
    void loadContext(deliveryId)
      .then((result) => {
        if (active) setContext(result);
      })
      .catch((cause: unknown) => {
        if (!active) return;
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        setError('告警上下文读取失败，请关闭后重试。');
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [deliveryId, loadContext, onSessionInvalidated]);
  useEffect(() => {
    let active = true;
    setLogs(null);
    setLogError(null);
    setLogLoading(false);
    if (
      !deliveryId ||
      context?.delivery.id !== deliveryId ||
      !context.delivery.taskId ||
      !context.run ||
      !context.logWindow
    )
      return;
    setLogLoading(true);
    void loadLogs(context.delivery.taskId, context.run.id, {
      ...context.logWindow,
      order: 'DESC',
      ...(stream === 'ALL' ? {} : { stream }),
    })
      .then((result) => {
        if (active) setLogs(result);
      })
      .catch((cause: unknown) => {
        if (!active) return;
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        setLogError(
          '该运行日志暂不可读，可能已清理、存储不可用或任务已删除。可导出容器日志进一步排查。',
        );
      })
      .finally(() => {
        if (active) setLogLoading(false);
      });
    return () => {
      active = false;
    };
  }, [deliveryId, context, stream, refresh, loadLogs, onSessionInvalidated]);
  const delivery = context?.delivery;
  return (
    <Modal
      title="告警上下文"
      open={deliveryId !== null}
      onCancel={onClose}
      footer={<Button onClick={onClose}>关闭</Button>}
      width={1000}
      styles={{ body: { maxHeight: '70vh', overflowY: 'auto' } }}
      destroyOnHidden
    >
      <Spin spinning={loading}>
        {error && <Alert type="error" title={error} showIcon />}
        {context && delivery && (
          <div className="space-y-4">
            <Descriptions
              size="small"
              column={2}
              items={[
                {
                  key: 'task',
                  label: '任务',
                  children:
                    delivery.taskName ??
                    (delivery.taskId
                      ? '任务名称不可用'
                      : delivery.rule === 'TEST'
                        ? '测试通知'
                        : '平台'),
                },
                { key: 'type', label: '异常分类', children: alertErrorCategory(delivery) },
                {
                  key: 'event',
                  label: '通知事件',
                  children:
                    delivery.event === 'RECOVERY'
                      ? '告警解除'
                      : delivery.event === 'ALARM'
                        ? '发生异常'
                        : '通道测试',
                },
                { key: 'time', label: '通知时间', children: formatDateTime(delivery.createdAt) },
                {
                  key: 'fault',
                  label: '任务故障码',
                  children: delivery.faultCode ?? context.run?.failureCode ?? '未记录',
                },
                {
                  key: 'send',
                  label: '通知发送',
                  children:
                    delivery.status === 'SENT'
                      ? '已发送'
                      : delivery.status === 'FAILED'
                        ? `发送失败（${delivery.failureCode ?? 'UNKNOWN'}）`
                        : '等待发送',
                },
                ...(context.run
                  ? [{ key: 'run', label: '关联运行', span: 2, children: context.run.id }]
                  : []),
              ]}
            />
            {delivery.event === 'RECOVERY' && (
              <Alert
                type="success"
                showIcon
                title="这条记录表示此前告警已解除，下方展示历史故障上下文。"
              />
            )}
            {context.association === 'TIME' && (
              <Alert
                type="info"
                showIcon
                title="旧通知没有记录精确运行绑定，下方按原异常通知时间关联，请结合日志核对。"
              />
            )}
            {context.association === 'NONE' && (
              <Alert
                type="info"
                showIcon
                title="没有可关联的任务运行记录；平台级故障请查看容器日志。"
              />
            )}
            {context.units.length > 0 && (
              <Table
                size="small"
                rowKey="unitKey"
                pagination={false}
                dataSource={context.units}
                scroll={{ y: 180, x: 500 }}
                columns={[
                  { title: '运行单元', dataIndex: 'unitKey' },
                  {
                    title: '状态',
                    dataIndex: 'status',
                    render: (value: string) => operationLabel(value),
                  },
                  {
                    title: '退出码',
                    dataIndex: 'exitCode',
                    render: (value: number | null) => value ?? '—',
                  },
                  {
                    title: '信号',
                    dataIndex: 'signal',
                    render: (value: string | null) => value ?? '—',
                  },
                ]}
              />
            )}
            {context.preflightChecks.length > 0 && (
              <Alert
                type="warning"
                showIcon
                title="关联预检失败项"
                description={
                  <ul>
                    {context.preflightChecks.map((c) => (
                      <li key={c.key}>
                        {c.summary}（{c.code}）
                      </li>
                    ))}
                  </ul>
                }
              />
            )}
            {context.events.length > 0 && (
              <details>
                <summary className="cursor-pointer text-sm">
                  恢复与操作记录（{context.events.length}）
                </summary>
                <div className="mt-2 max-h-40 overflow-auto rounded border p-3 text-xs">
                  {context.events.map((e, i) => (
                    <p key={i}>
                      {formatDateTime(e.occurredAt)} · {auditActionLabels[e.action] ?? e.action}
                      {e.reason ? ` · ${e.reason}` : ''}
                    </p>
                  ))}
                </div>
              </details>
            )}
            {context.run && (
              <>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="text-sm font-medium">故障时段日志</span>
                  <Space>
                    <Select
                      aria-label="上下文日志流"
                      value={stream}
                      onChange={setStream}
                      options={[
                        { value: 'STDERR', label: '错误输出' },
                        { value: 'STDOUT', label: '标准输出' },
                        { value: 'ALL', label: '全部输出' },
                      ]}
                      className="min-w-28"
                    />
                    <Button
                      aria-label="刷新日志"
                      size="small"
                      loading={logLoading}
                      onClick={() => setRefresh((n) => n + 1)}
                    >
                      刷新日志
                    </Button>
                  </Space>
                </div>
                {context.logWindow && (
                  <p className="text-xs text-muted-foreground">
                    {formatDateTime(context.logWindow.from)} 至{' '}
                    {formatDateTime(context.logWindow.to)} · 最多 500 条
                  </p>
                )}
                {logError && <Alert type="warning" showIcon title={logError} />}
                <Spin spinning={logLoading}>
                  <pre className="max-h-80 min-h-24 overflow-auto whitespace-pre-wrap break-words rounded-md border bg-muted/40 p-3 text-xs leading-5">
                    {logs?.facts.length
                      ? [...logs.facts]
                          .reverse()
                          .map(
                            (f) =>
                              `${formatDateTime(f.occurredAt)} [${f.unitKey ?? '运行'}/${f.stream ?? '日志'}] ${f.kind === 'LINE' ? f.payload : `日志缺口：${f.reason}`}`,
                          )
                          .join('\n')
                      : logError
                        ? '未能读取该时段日志。'
                        : logLoading
                          ? '正在读取日志…'
                          : '该时段没有可用日志，可切换标准输出或全部输出查看。'}
                  </pre>
                </Spin>
                {logs?.limited && (
                  <p className="text-xs text-muted-foreground">
                    仅展示该时段最后 500 条，完整日志可在任务详情选择对应历史运行查看。
                  </p>
                )}
              </>
            )}
          </div>
        )}
      </Spin>
    </Modal>
  );
}
