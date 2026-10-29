import { useEffect, useRef, useState } from 'react';
import type {
  AuditEvent,
  AuthenticatedSessionResponse,
  RuntimePerformanceSummary,
  TaskProgress,
  TaskSummary,
} from '@scs/contracts';
import {
  Alert,
  Button,
  Checkbox,
  Empty,
  Select,
  Spin,
  Table,
  Tag,
  Timeline,
  type TableColumnsType,
} from 'antd';
import {
  CircleAlert,
  Eye,
  ListTodo,
  Play,
  RefreshCw,
  Settings,
  ShieldCheck,
  Square,
  UserRound,
} from 'lucide-react';
import {
  auditApi,
  performanceApi,
  PlatformApiError,
  taskProgressApi,
  tasksApi,
} from '../../api/platform-api';
import { InfoTip } from '../../components/InfoTip';
import { formatDateTime } from '../../lib/date-time';
import { formatWalBytes } from '../../lib/source-progress';
import { auditActionLabels, operationName } from '../../lib/operation-labels';
import './home.css';

const numbers = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 3 });
const stateColors: Record<TaskSummary['observedState'], string> = {
  RUNNING: 'green',
  STOPPED: 'default',
  STARTING: 'blue',
  STOPPING: 'gold',
  FAILED: 'red',
  BLOCKED: 'orange',
  DELETED: 'default',
};

function SourceProgress({ task, sample }: { task: TaskSummary; sample: TaskProgress | undefined }) {
  if (task.observedState !== 'RUNNING') return <span className="text-muted-foreground">—</span>;
  if (!sample || sample.revisionId !== task.currentAppliedRevisionId)
    return <span className="text-muted-foreground">—</span>;
  if (sample.status !== 'AVAILABLE')
    return (
      <span className="text-muted-foreground">
        {sample.status === 'READ_FAILED' ? '读取失败' : '指标不可用'}
      </span>
    );
  if (sample.sourceType === 'POSTGRES')
    return (
      <div className="home-source-progress">
        <strong>
          {sample.replicationLagMilliseconds == null
            ? sample.unconfirmedWalBytes !== null
              ? formatWalBytes(sample.unconfirmedWalBytes)
              : '暂无时间统计'
            : `${numbers.format(sample.replicationLagMilliseconds / 1000)} 秒`}
        </strong>
        <span>
          {sample.replicationLagMilliseconds == null && sample.unconfirmedWalBytes !== null
            ? '未确认 WAL'
            : 'PG 复制延迟'}
        </span>
        {sample.slotStatus === 'INACTIVE' && <Tag color="orange">复制槽未连接</Tag>}
        {sample.slotStatus === 'MISSING' && <Tag color="red">复制槽不存在</Tag>}
      </div>
    );
  if (sample.sourceType === 'KAFKA')
    return (
      <div className="home-source-progress">
        <strong>
          {sample.offsetLag === null ? '未知' : BigInt(sample.offsetLag).toLocaleString('zh-CN')}
        </strong>
        <span>Kafka Offset 积压</span>
      </div>
    );
  return (
    <div className="home-source-progress">
      <strong
        className="home-binlog"
        title={
          sample.mysqlBinlogFilename
            ? `${sample.mysqlBinlogFilename}:${sample.mysqlBinlogPosition ?? '未知'}`
            : undefined
        }
      >
        {sample.mysqlBinlogFilename && sample.mysqlBinlogPosition
          ? `${sample.mysqlBinlogFilename}:${sample.mysqlBinlogPosition}`
          : '—'}
      </strong>
      <span>已确认 Binlog 位点</span>
    </div>
  );
}

export function HomePage({
  session,
  onSessionInvalidated,
  onOpenTask,
  onViewAudit,
}: {
  session: AuthenticatedSessionResponse;
  onSessionInvalidated: () => void;
  onOpenTask: (taskId: string) => void;
  onViewAudit: () => void;
}) {
  const administrator = session.user.role === 'ADMIN';
  const [tasks, setTasks] = useState<TaskSummary[] | null>(null);
  const [events, setEvents] = useState<AuditEvent[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [auditLoading, setAuditLoading] = useState(administrator);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [taskError, setTaskError] = useState(false);
  const [auditError, setAuditError] = useState(false);
  const [progressError, setProgressError] = useState(false);
  const [performanceError, setPerformanceError] = useState(false);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [interval, setIntervalMilliseconds] = useState(15000);
  const [page, setPage] = useState({ current: 1, pageSize: 10 });
  const busy = loading || auditLoading || metricsLoading;
  const busyRef = useRef(true);
  useEffect(() => {
    busyRef.current = busy;
  }, [busy]);
  const refresh = () => {
    if (busyRef.current) return;
    busyRef.current = true;
    setRefreshVersion((value) => value + 1);
  };
  useEffect(() => {
    if (!autoRefresh) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === 'visible' && !busyRef.current) {
        busyRef.current = true;
        setRefreshVersion((value) => value + 1);
      }
    }, interval);
    return () => window.clearInterval(timer);
  }, [autoRefresh, interval]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    void tasksApi
      .list()
      .then((result) => {
        if (!active) return;
        setTasks(
          result.tasks.filter(
            (task) => task.deletedAt === null && task.observedState !== 'DELETED',
          ),
        );
        setTaskError(false);
      })
      .catch((cause: unknown) => {
        if (!active) return;
        setTaskError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [refreshVersion, onSessionInvalidated]);

  useEffect(() => {
    if (!administrator) return;
    let active = true;
    setAuditLoading(true);
    void auditApi
      .list({ limit: 5 })
      .then((result) => {
        if (active) {
          setEvents(result.events);
          setAuditError(false);
        }
      })
      .catch((cause: unknown) => {
        if (!active) return;
        setAuditError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      })
      .finally(() => {
        if (active) setAuditLoading(false);
      });
    return () => {
      active = false;
    };
  }, [administrator, refreshVersion, onSessionInvalidated]);

  const taskList = tasks ?? [];
  const currentPage = Math.min(
    page.current,
    Math.max(1, Math.ceil(taskList.length / page.pageSize)),
  );
  const visibleTasks = taskList.slice(
    (currentPage - 1) * page.pageSize,
    currentPage * page.pageSize,
  );
  const runningKey = JSON.stringify(
    visibleTasks
      .filter((task) => task.observedState === 'RUNNING')
      .map((task) => [task.id, task.currentAppliedRevisionId]),
  );
  const [progress, setProgress] = useState<{ key: string; items: readonly TaskProgress[] } | null>(
    null,
  );
  const [performance, setPerformance] = useState<{
    key: string;
    items: readonly RuntimePerformanceSummary[];
  } | null>(null);
  useEffect(() => {
    const ids = (JSON.parse(runningKey) as [string, string | null][]).map(([id]) => id);
    if (ids.length === 0) {
      setMetricsLoading(false);
      setProgressError(false);
      setPerformanceError(false);
      return;
    }
    let active = true;
    setMetricsLoading(true);
    const sourceRead = taskProgressApi
      .list()
      .then((items) => {
        if (active) {
          setProgress({ key: runningKey, items });
          setProgressError(false);
        }
      })
      .catch((cause: unknown) => {
        if (!active) return;
        setProgressError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      });
    const performanceRead = performanceApi
      .query(ids)
      .then((result) => {
        if (active) {
          setPerformance({ key: runningKey, items: result.items });
          setPerformanceError(false);
        }
      })
      .catch((cause: unknown) => {
        if (!active) return;
        setPerformanceError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      });
    void Promise.all([sourceRead, performanceRead]).finally(() => {
      if (active) setMetricsLoading(false);
    });
    return () => {
      active = false;
    };
  }, [runningKey, refreshVersion, onSessionInvalidated]);

  const progressByTask = new Map(
    (progress?.key === runningKey ? progress.items : []).map((item) => [item.taskId, item]),
  );
  const performanceByTask = new Map(
    (performance?.key === runningKey ? performance.items : []).map((item) => [item.taskId, item]),
  );
  const columns: TableColumnsType<TaskSummary> = [
    {
      title: '任务',
      key: 'task',
      width: 180,
      render: (_, task) => (
        <div className="home-task-name">
          <Button type="link" onClick={() => onOpenTask(task.id)}>
            {task.name}
          </Button>
          <span>{operationName(task.taskType)}</span>
        </div>
      ),
    },
    {
      title: '运行状态',
      key: 'state',
      width: 100,
      render: (_, task) => (
        <Tag color={stateColors[task.observedState]}>{operationName(task.observedState)}</Tag>
      ),
    },
    {
      title: (
        <span className="inline-flex items-center gap-1">
          源端进度
          <InfoTip
            label="首页源端进度说明"
            text="PostgreSQL 优先展示复制确认延迟；源端未提供时间统计时，改为按复制槽名称查询的未确认 WAL 字节差。WAL 是实例级日志位置差，可能包含其他数据库的日志，不能换算为业务记录条数或秒数。Kafka 展示 Offset 积压，MySQL 展示已确认 Binlog 位点。这些不是端到端延迟。"
          />
        </span>
      ),
      key: 'progress',
      width: 172,
      render: (_, task) => <SourceProgress task={task} sample={progressByTask.get(task.id)} />,
    },
    {
      title: '记录吞吐',
      key: 'records',
      width: 130,
      render: (_, task) => {
        const summary = performanceByTask.get(task.id);
        const value = summary?.values.recordsPerSecond;
        return task.observedState === 'RUNNING' &&
          summary &&
          !['NOT_RUNNING', 'NO_DATA'].includes(summary.status) &&
          value != null ? (
          <strong className="font-medium tabular-nums">{numbers.format(value)} 条/秒</strong>
        ) : (
          <span className="text-muted-foreground">—</span>
        );
      },
    },
    {
      title: '操作',
      key: 'actions',
      width: 64,
      fixed: 'right',
      align: 'center',
      render: (_, task) => (
        <Button
          type="text"
          icon={<Eye className="size-4" />}
          aria-label={`查看 ${task.name} 详情`}
          title="查看任务详情"
          onClick={() => onOpenTask(task.id)}
        />
      ),
    },
  ];
  const statistics = [
    { label: '任务总数', value: taskList.length, icon: ListTodo, tone: 'neutral' },
    {
      label: '运行中',
      value: taskList.filter((task) => task.observedState === 'RUNNING').length,
      icon: Play,
      tone: 'running',
    },
    {
      label: '已停止',
      value: taskList.filter((task) => task.observedState === 'STOPPED').length,
      icon: Square,
      tone: 'stopped',
    },
    {
      label: '需处理',
      value: taskList.filter((task) => ['FAILED', 'BLOCKED'].includes(task.observedState)).length,
      icon: CircleAlert,
      tone: 'attention',
    },
  ];
  return (
    <div className="home-dashboard">
      {taskError && (
        <Alert
          type="warning"
          showIcon
          title={
            tasks === null
              ? '任务概况读取失败，请刷新重试。'
              : '任务概况刷新失败，当前保留上次读取结果。'
          }
        />
      )}
      <dl className="home-statistics" aria-label="任务状态汇总">
        {statistics.map(({ label, value, icon: Icon, tone }) => (
          <div
            className="home-statistic"
            key={label}
            title={
              label === '需处理' ? '失败或已阻断的任务；启动中、停止中计入任务总数。' : undefined
            }
          >
            <span className={`home-statistic-icon home-tone-${tone}`}>
              <Icon className="size-6" aria-hidden="true" />
            </span>
            <div>
              <dt>{label}</dt>
              <dd aria-label={`${label}数量`}>
                {tasks === null ? '—' : value.toLocaleString('zh-CN')}
              </dd>
            </div>
          </div>
        ))}
      </dl>
      <div className="home-main-grid">
        <section className="home-panel home-tasks" aria-label="任务运行状态">
          <header className="home-panel-header">
            <h2>任务运行状态</h2>
            <div className="home-refresh-controls">
              <Checkbox
                checked={autoRefresh}
                onChange={(event) => setAutoRefresh(event.target.checked)}
                aria-label="首页自动刷新"
              >
                自动刷新
              </Checkbox>
              <Select
                aria-label="首页刷新间隔"
                value={interval}
                onChange={setIntervalMilliseconds}
                options={[5, 15, 30, 60].map((seconds) => ({
                  value: seconds * 1000,
                  label: `${seconds} 秒`,
                }))}
              />
              <Button
                aria-label="刷新首页"
                title="刷新首页"
                icon={<RefreshCw className={`size-4 ${busy ? 'animate-spin' : ''}`} />}
                disabled={busy}
                onClick={refresh}
              />
            </div>
          </header>
          {(progressError || performanceError) && (
            <p role="status" className="home-read-warning">
              部分指标读取失败，保留上次结果；未采集的指标显示为“—”。
            </p>
          )}
          <Table<TaskSummary>
            aria-label="首页任务运行状态"
            rowKey="id"
            columns={columns}
            dataSource={taskList}
            loading={loading}
            scroll={{ x: 646, y: 'calc(100dvh - 480px)' }}
            locale={{
              emptyText: (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description={taskError ? '未能读取任务' : '暂无同步任务'}
                />
              ),
            }}
            pagination={{
              current: currentPage,
              pageSize: page.pageSize,
              pageSizeOptions: [10, 20, 50],
              showSizeChanger: { 'aria-label': '首页每页条数' },
              onChange: (current, pageSize) => setPage({ current, pageSize }),
            }}
          />
        </section>
        <section className="home-panel home-activity" aria-label="最近操作">
          <header className="home-panel-header">
            <h2>最近操作</h2>
            {administrator && (
              <Button type="link" onClick={onViewAudit}>
                查看全部
              </Button>
            )}
          </header>
          <div className="home-activity-body">
            {!administrator ? (
              <Empty
                image={<ShieldCheck className="mx-auto size-10 text-muted-foreground" />}
                description="仅管理员可查看审计操作"
              />
            ) : (
              <>
                {auditError && (
                  <p role="status" className="home-read-warning">
                    {events === null
                      ? '操作记录读取失败，请刷新重试。'
                      : '操作记录刷新失败，当前显示上次读取结果。'}
                  </p>
                )}
                {auditLoading && events === null ? (
                  <div className="home-empty">
                    <Spin />
                  </div>
                ) : !events?.length ? (
                  <Empty
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    description={auditError ? '未能读取操作记录' : '暂无操作记录'}
                  />
                ) : (
                  <Timeline
                    className="home-activity-timeline"
                    items={events.map((event) => {
                      const target =
                        event.subjectType === 'TASK'
                          ? taskList.find((task) => task.id === event.subjectId)
                          : undefined;
                      const Icon = event.actorType === 'USER' ? UserRound : Settings;
                      return {
                        key: event.eventId,
                        icon: (
                          <span className="home-activity-icon">
                            <Icon className="size-4" aria-hidden="true" />
                          </span>
                        ),
                        content: (
                          <div className="home-activity-entry">
                            <div>
                              <Tag
                                color={
                                  event.actorType === 'USER'
                                    ? 'blue'
                                    : event.actorType === 'SYSTEM'
                                      ? 'purple'
                                      : 'gold'
                                }
                              >
                                {event.actorType === 'USER'
                                  ? '用户'
                                  : event.actorType === 'SYSTEM'
                                    ? '系统'
                                    : '部署管理员'}
                              </Tag>
                              {event.actorUserId === session.user.id && (
                                <strong>{session.user.username}</strong>
                              )}
                            </div>
                            <p>
                              {auditActionLabels[event.action] ?? event.action}
                              {target && (
                                <Button type="link" onClick={() => onOpenTask(target.id)}>
                                  {target.name}
                                </Button>
                              )}
                            </p>
                            <time dateTime={event.occurredAt}>
                              {formatDateTime(event.occurredAt)}
                            </time>
                          </div>
                        ),
                      };
                    })}
                  />
                )}
              </>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
