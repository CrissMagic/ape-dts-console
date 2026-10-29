import { useCallback, useEffect, useRef, useState } from 'react';
import { useOperationNotification } from '../../../components/useOperationNotification';
import {
  Button,
  Card,
  Checkbox,
  Collapse,
  Empty,
  Input,
  Modal,
  Select,
  Spin,
  Table,
  Tabs,
  Tag,
  Timeline,
} from 'antd';
import type {
  ArtifactSummary,
  DataSourceSummary,
  RuntimeLogRead,
  RuntimeRead,
  RuntimeRunList,
  TaskSummary,
} from '@scs/contracts';
import { runtimeLogLevel } from '@scs/contracts';

import { PlatformApiError, tasksApi } from '../../../api/platform-api';
import { InfoTip } from '../../../components/InfoTip';
import { formatDateTime, shanghaiDateTimeLocalToIso } from '../../../lib/date-time';
import { operationName } from '../../../lib/operation-labels';
import { RuntimePerformancePanel } from './RuntimePerformance';
import { TaskProgressSummary } from '../progress/TaskProgress';
import { useTaskProgress } from '../progress/useTaskProgress';
import { mappingSourceLabel, mappingTargets, readTaskMappings } from '../task-mappings';

function operationLabel(code: string) {
  return <span title={code}>{operationName(code)}</span>;
}

function errorCode(error: unknown) {
  return error instanceof PlatformApiError ? error.code : 'UNKNOWN_ERROR';
}

function statusColor(status: string) {
  if (['RUNNING', 'SUCCEEDED', 'COMPLETED', 'HEALTHY'].includes(status)) return 'green';
  if (['FAILED', 'REJECTED', 'DEGRADED'].includes(status)) return 'red';
  if (['STARTING', 'STOPPING', 'PENDING'].includes(status)) return 'blue';
  return 'default';
}

function configurationRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function TaskRuntimeDetails({
  task,
  sourceName,
  targetName,
  artifacts = [],
  dataSources = [],
  onSessionInvalidated,
  onTaskChanged,
}: {
  task: TaskSummary;
  sourceName?: string | undefined;
  targetName?: string | undefined;
  artifacts?: readonly Pick<ArtifactSummary, 'id' | 'versionIdentifier'>[];
  dataSources?: readonly Pick<DataSourceSummary, 'id' | 'name'>[];
  onSessionInvalidated: () => void;
  onTaskChanged?: () => Promise<void> | void;
}) {
  const [stopConfirmOpen, setStopConfirmOpen] = useState(false);
  const [configurationSelection, setConfigurationSelection] = useState({
    taskId: task.id,
    kind: 'APPLIED',
  });
  const progress = useTaskProgress([task.id], onSessionInvalidated, null);
  const refreshProgress = progress.refresh;
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(15_000);
  const [refreshing, setRefreshing] = useState(false);
  const refreshInFlight = useRef(false);
  const detailGeneration = useRef(0);
  const performance = useRef<{ refresh: () => Promise<void> }>(null);
  const [runtimeFacts, setRuntimeFacts] = useState<RuntimeRead | null>(null);
  const [logFacts, setLogFacts] = useState<RuntimeLogRead | null>(null);
  const [logSelection, setLogSelection] = useState({ taskId: task.id, runId: '' });
  const selectedLogRunId = logSelection.taskId === task.id ? logSelection.runId : '';
  const selectedLogRun = useRef('');
  selectedLogRun.current = selectedLogRunId;
  const [runHistory, setRunHistory] = useState<{
    taskId: string;
    runs: RuntimeRunList['runs'];
  } | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(false);
  const [historyRevision, setHistoryRevision] = useState(0);
  const [activeMonitorTab, setActiveMonitorTab] = useState('performance');
  const [liveLogs, setLiveLogs] = useState(true);
  const logReadRunId = selectedLogRunId || runtimeFacts?.run?.id;
  const historicalLogs = selectedLogRunId !== '';
  const canFollowLogs =
    !historicalLogs &&
    runtimeFacts?.run != null &&
    ['STARTING', 'RUNNING', 'STOPPING'].includes(runtimeFacts.run.status);
  const followingLogs = liveLogs && canFollowLogs;
  const [logLevel, setLogLevel] = useState<'ALL' | 'ERROR' | 'WARN' | 'INFO'>('ALL');
  const [logRevision, setLogRevision] = useState(0);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logError, setLogError] = useState<string | null>(null);
  const logInFlight = useRef(false);
  const logViewport = useRef<HTMLOListElement>(null);
  const followLogs = useRef(true);
  const [logStream, setLogStream] = useState<'ALL' | 'STDOUT' | 'STDERR'>('ALL');
  const [logUnit, setLogUnit] = useState('');
  const [logFrom, setLogFrom] = useState('');
  const [logTo, setLogTo] = useState('');
  const appliedLogFilter = useRef<Parameters<typeof tasksApi.logs>[2]>(undefined);
  const logRun = useRef<string | null>(null);
  const { holder, setError, notify } = useOperationNotification();
  const [loading, setLoading] = useState(true);
  const [stopping, setStopping] = useState(false);
  const [retryingLogStorage, setRetryingLogStorage] = useState(false);
  const runtimeRequest = useRef(0);
  const runtimeInFlight = useRef(false);
  const logRequest = useRef(0);
  const stopInFlight = useRef(false);
  const retryInFlight = useRef(false);
  const retryFlight = useRef(0);
  const downloadRequest = useRef(0);
  const showDraft =
    task.applied == null ||
    (configurationSelection.taskId === task.id &&
      configurationSelection.kind === 'DRAFT' &&
      task.draft !== null);
  const displayedConfiguration = showDraft ? task.draft : task.applied;
  const artifactName = artifacts.find(
    (artifact) => artifact.id === displayedConfiguration?.artifactId,
  )?.versionIdentifier;
  const configuration = configurationRecord(displayedConfiguration?.configuration);
  const source = configurationRecord(configuration.source);
  const target = configurationRecord(configuration.target);
  const mappings = readTaskMappings(configuration);

  const resetLogFilter = useCallback(() => {
    appliedLogFilter.current = undefined;
    setLogStream('ALL');
    setLogLevel('ALL');
    setLogUnit('');
    setLogFrom('');
    setLogTo('');
  }, []);

  const loadRuntime = useCallback(async () => {
    if (runtimeInFlight.current) return;
    runtimeInFlight.current = true;
    const request = ++runtimeRequest.current;
    setError(null);
    try {
      const facts = await tasksApi.runtime(task.id);
      if (request !== runtimeRequest.current) return;
      const runId = facts.run?.id ?? null;
      if (logRun.current !== runId) {
        logRun.current = runId;
        if (selectedLogRun.current === '') {
          resetLogFilter();
          logRequest.current += 1;
          logInFlight.current = false;
          setLogFacts(null);
          setLogError(null);
          setLiveLogs(true);
        }
      }
      setRuntimeFacts(facts);
      setLoading(false);
    } catch (cause) {
      if (request === runtimeRequest.current) {
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        setError(errorCode(cause));
      }
    } finally {
      if (request === runtimeRequest.current) {
        runtimeInFlight.current = false;
        setLoading(false);
      }
    }
  }, [onSessionInvalidated, task.id, setError, resetLogFilter]);

  useEffect(() => {
    detailGeneration.current += 1;
    setAutoRefresh(false);
    setRefreshInterval(15_000);
    setRefreshing(false);
    setLoading(true);
    setRuntimeFacts(null);
    setLogFacts(null);
    setLogSelection({ taskId: task.id, runId: '' });
    selectedLogRun.current = '';
    setRunHistory(null);
    setHistoryError(false);
    setLogError(null);
    setLiveLogs(true);
    logRun.current = null;
    resetLogFilter();
    downloadRequest.current += 1;
    retryFlight.current += 1;
    retryInFlight.current = false;
    setRetryingLogStorage(false);
    void loadRuntime();
    return () => {
      detailGeneration.current += 1;
      refreshInFlight.current = false;
      runtimeInFlight.current = false;
      runtimeRequest.current += 1;
      logRequest.current += 1;
      logInFlight.current = false;
      downloadRequest.current += 1;
      retryFlight.current += 1;
    };
  }, [loadRuntime, resetLogFilter, task.id]);

  useEffect(() => {
    if (activeMonitorTab !== 'logs' || runtimeFacts?.taskId !== task.id) return;
    let active = true;
    setHistoryLoading(true);
    setHistoryError(false);
    void tasksApi
      .runtimeRuns(task.id)
      .then((result) => {
        if (active) setRunHistory({ taskId: task.id, runs: result.runs });
      })
      .catch((cause: unknown) => {
        if (!active) return;
        setHistoryError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      })
      .finally(() => {
        if (active) setHistoryLoading(false);
      });
    return () => {
      active = false;
    };
  }, [
    activeMonitorTab,
    task.id,
    runtimeFacts?.taskId,
    runtimeFacts?.run?.id,
    historyRevision,
    onSessionInvalidated,
  ]);

  function selectLogRun(runId: string) {
    logRequest.current += 1;
    logInFlight.current = false;
    downloadRequest.current += 1;
    selectedLogRun.current = runId;
    setLogSelection({ taskId: task.id, runId });
    resetLogFilter();
    setLogFacts(null);
    setLogError(null);
    setLiveLogs(runId === '');
    followLogs.current = true;
  }

  const loadLogs = useCallback(
    async (runId: string) => {
      if (logInFlight.current) return;
      logInFlight.current = true;
      const request = ++logRequest.current;
      setLogsLoading(true);
      try {
        const logs = await tasksApi.logs(
          task.id,
          runId,
          appliedLogFilter.current ?? { order: 'DESC' },
        );
        if (request !== logRequest.current) return;
        setLogFacts(logs);
        setLogError(null);
      } catch (cause) {
        if (request !== logRequest.current) return;
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        setLogError(errorCode(cause));
      } finally {
        if (request === logRequest.current) {
          logInFlight.current = false;
          setLogsLoading(false);
        }
      }
    },
    [task.id, onSessionInvalidated],
  );

  useEffect(() => {
    const runId = logReadRunId;
    if (activeMonitorTab !== 'logs' || runId === undefined || runtimeFacts?.taskId !== task.id)
      return;
    void loadLogs(runId);
    return () => {
      logRequest.current += 1;
      logInFlight.current = false;
    };
  }, [activeMonitorTab, logReadRunId, runtimeFacts?.taskId, task.id, loadLogs, logRevision]);

  useEffect(() => {
    const runId = logReadRunId;
    if (
      !followingLogs ||
      activeMonitorTab !== 'logs' ||
      runId === undefined ||
      runtimeFacts?.taskId !== task.id
    )
      return;
    let active = true;
    let timer = setTimeout(() => void poll(), 2_000);
    async function poll() {
      await loadLogs(runId!);
      if (active) timer = setTimeout(() => void poll(), 2_000);
    }
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [
    followingLogs,
    activeMonitorTab,
    logReadRunId,
    runtimeFacts?.taskId,
    task.id,
    loadLogs,
    logRevision,
  ]);

  useEffect(() => {
    const viewport = logViewport.current;
    if (viewport !== null && followingLogs && followLogs.current)
      viewport.scrollTop = viewport.scrollHeight;
  }, [logFacts, activeMonitorTab, followingLogs]);

  const refresh = useCallback(async () => {
    if (refreshInFlight.current || runtimeInFlight.current) return;
    refreshInFlight.current = true;
    const generation = detailGeneration.current;
    setRefreshing(true);
    if (activeMonitorTab === 'logs') setHistoryRevision((value) => value + 1);
    try {
      const results = await Promise.allSettled([
        loadRuntime(),
        activeMonitorTab === 'logs' && logReadRunId !== undefined
          ? loadLogs(logReadRunId)
          : undefined,
        refreshProgress(),
        performance.current?.refresh(),
        Promise.resolve().then(() => onTaskChanged?.()),
      ]);
      const failure = results.find((result) => result.status === 'rejected');
      if (failure !== undefined) throw failure.reason;
    } catch (cause) {
      if (generation !== detailGeneration.current) return;
      if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      setError(errorCode(cause));
    } finally {
      if (generation === detailGeneration.current) {
        refreshInFlight.current = false;
        setRefreshing(false);
      }
    }
  }, [
    loadRuntime,
    loadLogs,
    activeMonitorTab,
    logReadRunId,
    refreshProgress,
    onTaskChanged,
    onSessionInvalidated,
    setError,
  ]);

  useEffect(() => {
    if (!autoRefresh) return;
    let active = true;
    let timer = setTimeout(() => void poll(), refreshInterval);
    async function poll() {
      await refresh();
      if (active) timer = setTimeout(() => void poll(), refreshInterval);
    }
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [autoRefresh, refreshInterval, refresh]);

  function currentLogFilter() {
    return {
      ...(logFrom.length === 0 ? {} : { from: shanghaiDateTimeLocalToIso(logFrom) }),
      ...(logLevel === 'ALL' ? {} : { level: logLevel }),
      ...(logStream === 'ALL' ? {} : { stream: logStream }),
      ...(logTo.length === 0 ? {} : { to: shanghaiDateTimeLocalToIso(logTo) }),
      ...(logUnit.length === 0 ? {} : { unitKey: logUnit }),
    };
  }

  async function stop() {
    if (stopInFlight.current) return;
    stopInFlight.current = true;
    setStopping(true);
    const requestedRuntime = runtimeRequest.current;
    setError(null);
    try {
      const result = await tasksApi.stop(task);
      if (requestedRuntime === runtimeRequest.current) {
        notify(
          ['FAILED', 'REJECTED'].includes(result.state) ? 'error' : 'info',
          `停止操作：${operationName(result.state)}${result.failureCode === null ? '' : ` / ${result.failureCode}`}`,
        );
      }
      await onTaskChanged?.();
      if (requestedRuntime === runtimeRequest.current) await loadRuntime();
    } catch (cause) {
      if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      if (requestedRuntime === runtimeRequest.current) setError(errorCode(cause));
    } finally {
      stopInFlight.current = false;
      setStopping(false);
    }
  }

  async function retryLogStorage() {
    if (runtimeFacts?.run == null || retryInFlight.current) return;
    retryInFlight.current = true;
    const requestedRetry = ++retryFlight.current;
    const runId = runtimeFacts.run.id;
    const requestedRuntime = runtimeRequest.current;
    setError(null);
    setRetryingLogStorage(true);
    try {
      await tasksApi.retryLogStorage(task.id, runId);
      const facts = await tasksApi.runtime(task.id);
      if (requestedRuntime === runtimeRequest.current) {
        setRuntimeFacts(facts);
        if (activeMonitorTab === 'logs') setLogRevision((value) => value + 1);
      }
    } catch (cause) {
      if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      if (requestedRuntime === runtimeRequest.current) setError(errorCode(cause));
    } finally {
      if (requestedRetry === retryFlight.current) {
        retryInFlight.current = false;
        if (requestedRuntime === runtimeRequest.current) setRetryingLogStorage(false);
      }
    }
  }

  function filterLogs() {
    setError(null);
    try {
      const filter = currentLogFilter();
      if (
        filter.from !== undefined &&
        filter.to !== undefined &&
        Date.parse(filter.from) >= Date.parse(filter.to)
      )
        throw new Error('起始时间必须早于结束时间。');
      appliedLogFilter.current = {
        ...filter,
        order: followingLogs && filter.to === undefined ? 'DESC' : 'ASC',
      };
      if (filter.to !== undefined) setLiveLogs(false);
      setLogFacts(null);
      setLogRevision((value) => value + 1);
      followLogs.current = true;
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : errorCode(cause));
    }
  }

  async function downloadLogs(runId: string) {
    const request = ++downloadRequest.current;
    setError(null);
    try {
      const download = await tasksApi.downloadLogs(task.id, runId, currentLogFilter());
      if (request !== downloadRequest.current) return;
      const url = URL.createObjectURL(download.blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = download.filename;
      document.body.append(anchor);
      anchor.click();
      anchor.remove();
      setTimeout(() => URL.revokeObjectURL(url), 0);
    } catch (cause) {
      if (request !== downloadRequest.current) return;
      if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      setError(errorCode(cause));
    }
  }

  const controls = (
    <div aria-label="运行监控操作" className="flex shrink-0 flex-wrap items-center gap-2 pb-3">
      <Checkbox checked={autoRefresh} onChange={(event) => setAutoRefresh(event.target.checked)}>
        自动刷新
      </Checkbox>
      <Select
        aria-label="自动刷新间隔"
        value={refreshInterval}
        onChange={setRefreshInterval}
        options={[5, 15, 30, 60].map((seconds) => ({
          value: seconds * 1000,
          label: `${seconds} 秒`,
        }))}
        getPopupContainer={(trigger) => trigger.parentElement ?? trigger}
        style={{ width: 100 }}
      />
      <Button disabled={loading || refreshing} onClick={() => void refresh()}>
        刷新运行信息
      </Button>
      <Button
        danger
        disabled={stopping || !['STARTING', 'RUNNING'].includes(task.observedState)}
        onClick={() => setStopConfirmOpen(true)}
      >
        停止整个任务
      </Button>
      <InfoTip
        label="停止说明"
        text="停止请求按 SIGINT → SIGTERM → SIGKILL 的受控窗口执行。元数据库故障时请使用紧急停止。"
      />
    </div>
  );

  return (
    <article className="task-runtime-details flex min-h-0 flex-1 flex-col gap-3 overflow-hidden">
      {holder}
      <div className="grid min-h-0 flex-1 grid-rows-2 gap-4 lg:grid-cols-[minmax(300px,1fr)_minmax(0,2.3fr)] lg:grid-rows-1">
        <section
          aria-label="运行监控与日志"
          className="order-2 flex min-h-0 min-w-0 flex-col overflow-hidden"
        >
          {controls}
          <Tabs
            activeKey={activeMonitorTab}
            onChange={setActiveMonitorTab}
            className="min-h-0 flex-1"
            items={[
              {
                key: 'performance',
                label: '运行监控',
                forceRender: true,
                children: (
                  <RuntimePerformancePanel
                    ref={performance}
                    taskId={task.id}
                    running={task.observedState === 'RUNNING'}
                    onSessionInvalidated={onSessionInvalidated}
                  />
                ),
              },
              {
                key: 'logs',
                label: '运行日志',
                children: (
                  <Card size="small" className="runtime-log-card">
                    {loading ? (
                      <div className="flex items-center justify-center gap-3 py-8" role="status">
                        <Spin size="small" />
                        正在读取运行记录…
                      </div>
                    ) : runtimeFacts?.run != null ? (
                      <div className="flex h-full min-h-0 flex-col gap-3">
                        <div
                          className="flex shrink-0 flex-wrap items-center gap-2"
                          aria-label="日志工具栏"
                        >
                          <Select
                            aria-label="日志所属运行"
                            value={selectedLogRunId}
                            loading={historyLoading}
                            onChange={selectLogRun}
                            className="min-w-48 flex-1"
                            options={[
                              { value: '', label: '当前运行／最近一次运行' },
                              ...(runHistory?.taskId === task.id ? runHistory.runs : [])
                                .filter((run) => run.id !== runtimeFacts.run!.id)
                                .map((run) => ({
                                  value: run.id,
                                  label: `${formatDateTime(run.startedAt)} · ${run.id.slice(0, 8)} · ${operationName(run.status)}`,
                                })),
                            ]}
                          />
                          <Select
                            aria-label="日志级别"
                            value={logLevel}
                            onChange={setLogLevel}
                            options={[
                              { label: '全部级别', value: 'ALL' },
                              { label: 'ERROR · 错误', value: 'ERROR' },
                              { label: 'WARN · 警告', value: 'WARN' },
                              { label: 'INFO · 信息', value: 'INFO' },
                            ]}
                            className="min-w-32"
                          />
                          <Checkbox
                            checked={followingLogs}
                            disabled={!canFollowLogs}
                            onChange={(event) => {
                              setLiveLogs(event.target.checked);
                              if (event.target.checked) {
                                setLogTo('');
                                const { to: _to, ...filter } = appliedLogFilter.current ?? {};
                                void _to;
                                appliedLogFilter.current = { ...filter, order: 'DESC' };
                                followLogs.current = true;
                                setLogRevision((value) => value + 1);
                              }
                            }}
                          >
                            实时日志
                          </Checkbox>
                          <InfoTip
                            label="实时日志说明"
                            text="每 2 秒读取最近 500 条已脱敏日志；离开此页签即停止读取。向上滚动可暂停跟随，滚动到底部后继续。设置结束时间后切换为历史查询。"
                          />
                          <Button onClick={filterLogs}>筛选日志</Button>
                          <Button onClick={() => void downloadLogs(logReadRunId!)}>下载日志</Button>
                          {logsLoading && <Spin size="small" />}
                        </div>
                        {historyError && (
                          <p role="status" className="field-warning">
                            历史运行列表读取失败，可点击“刷新运行信息”重试；当前日志不受影响。
                          </p>
                        )}
                        <Collapse
                          size="small"
                          className="shrink-0"
                          items={[
                            {
                              key: 'filters',
                              label: `更多筛选${(['stream', 'unitKey', 'from', 'to'] as const).some((key) => appliedLogFilter.current?.[key] !== undefined) ? ' · 已应用' : ''}`,
                              extra: logFacts !== null && (
                                <span className="text-xs text-muted-foreground">
                                  {logFacts.facts.length} 条
                                </span>
                              ),
                              forceRender: true,
                              children: (
                                <div className="grid gap-3 sm:grid-cols-2">
                                  <div className="space-y-1">
                                    <label>输出流</label>
                                    <Select
                                      aria-label="日志流"
                                      value={logStream}
                                      onChange={setLogStream}
                                      options={[
                                        { label: '全部输出流', value: 'ALL' },
                                        { label: 'STDOUT', value: 'STDOUT' },
                                        { label: 'STDERR', value: 'STDERR' },
                                      ]}
                                      className="w-full"
                                    />
                                  </div>
                                  <label className="space-y-1">
                                    运行单元
                                    <Input
                                      aria-label="日志运行单元"
                                      placeholder="全部运行单元"
                                      value={logUnit}
                                      onChange={(event) => setLogUnit(event.target.value)}
                                    />
                                  </label>
                                  <label className="space-y-1">
                                    起始时间（包含）
                                    <Input
                                      aria-label="日志起始时间"
                                      step="1"
                                      type="datetime-local"
                                      value={logFrom}
                                      onChange={(event) => setLogFrom(event.target.value)}
                                    />
                                  </label>
                                  <label className="space-y-1">
                                    结束时间（不包含）
                                    <Input
                                      aria-label="日志结束时间"
                                      step="1"
                                      type="datetime-local"
                                      value={logTo}
                                      onChange={(event) => setLogTo(event.target.value)}
                                    />
                                  </label>
                                </div>
                              ),
                            },
                          ]}
                        />
                        {logError !== null && (
                          <p className="field-warning" role="status">
                            日志读取失败，已保留上次结果（{logError}）。
                          </p>
                        )}
                        {logFacts?.limited === true && (
                          <p className="field-warning">
                            {logFacts.scanLimited
                              ? '已达到单次日志扫描上限，当前结果不完整；请缩小时间范围或下载完整日志。'
                              : followingLogs
                                ? '仅显示最近 500 条匹配日志；完整记录可按时间范围查询或下载。'
                                : '结果已达到读取上限，请按级别、输出流、运行单元或时间范围筛选。'}
                          </p>
                        )}
                        <ol
                          aria-label="运行日志内容"
                          ref={logViewport}
                          onScroll={(event) => {
                            const element = event.currentTarget;
                            followLogs.current =
                              element.scrollHeight - element.scrollTop - element.clientHeight < 40;
                          }}
                          className="min-h-0 flex-1 space-y-1 overflow-auto rounded-md bg-muted/50 p-3 font-mono text-xs leading-5 break-words"
                        >
                          {logFacts?.facts.map((fact, index) => (
                            <li
                              key={`${fact.occurredAt}:${index}`}
                              className={
                                fact.kind === 'LINE'
                                  ? {
                                      ERROR: 'text-red-600 dark:text-red-400',
                                      WARN: 'text-amber-700 dark:text-amber-400',
                                      INFO: 'text-foreground',
                                    }[runtimeLogLevel(fact.payload) ?? 'INFO']
                                  : 'text-amber-700 dark:text-amber-400'
                              }
                            >
                              {formatDateTime(fact.occurredAt)} · {fact.unitKey ?? '未知单元'} ·{' '}
                              {fact.kind === 'LINE'
                                ? `${fact.stream} · ${fact.payload}`
                                : `GAP · ${fact.reason}`}
                            </li>
                          ))}
                        </ol>
                        {logFacts !== null &&
                          logFacts.facts.length === 0 &&
                          !logFacts.scanLimited && (
                            <Empty
                              image={Empty.PRESENTED_IMAGE_SIMPLE}
                              description="当前筛选条件下暂无日志。"
                            />
                          )}
                      </div>
                    ) : (
                      <Empty
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        description={
                          runtimeFacts?.run === null
                            ? '任务运行后可查看日志。'
                            : '运行信息尚未读取。'
                        }
                      />
                    )}
                  </Card>
                ),
              },
            ]}
          />
        </section>
        <aside
          className="order-1 flex min-h-0 min-w-0 flex-col overflow-hidden"
          aria-label="运行摘要"
        >
          {[runtimeFacts?.run?.failureCode, runtimeFacts?.operation?.failureCode].some((code) =>
            code?.startsWith('RUNTIME_CGROUP_'),
          ) && (
            <p className="text-sm text-muted-foreground">
              此为旧部署模式的资源隔离失败记录；当前共享容器模式不要求 cgroup v2
              委派，请重新预检并启动。
            </p>
          )}
          <Tabs
            defaultActiveKey="run"
            items={[
              {
                key: 'configuration',
                label: '任务配置',
                children: (
                  <Card size="small">
                    <div className="mb-3 flex justify-end">
                      {task.applied != null && task.draft !== null ? (
                        <Select
                          aria-label="查看配置"
                          size="small"
                          value={showDraft ? 'DRAFT' : 'APPLIED'}
                          onChange={(kind) => setConfigurationSelection({ taskId: task.id, kind })}
                          options={[
                            { value: 'APPLIED', label: '当前已应用' },
                            { value: 'DRAFT', label: '待应用修改' },
                          ]}
                        />
                      ) : (
                        <span className="text-xs text-muted-foreground">
                          {displayedConfiguration == null
                            ? '配置未读取'
                            : showDraft
                              ? '待应用配置'
                              : '当前已应用'}
                        </span>
                      )}
                    </div>
                    <dl className="space-y-3 text-sm break-words">
                      <div>
                        <dt className="text-muted-foreground">来源</dt>
                        <dd>
                          {dataSources.find((item) => item.id === source.dataSourceId)?.name ??
                            sourceName ??
                            '数据源名称未读取'}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">目标</dt>
                        <dd>
                          {dataSources.find((item) => item.id === target.dataSourceId)?.name ??
                            targetName ??
                            '数据源名称未读取'}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">绑定制品</dt>
                        <dd>
                          {artifactName ??
                            (displayedConfiguration == null
                              ? '制品绑定信息未读取'
                              : displayedConfiguration.artifactId === null
                                ? '未绑定制品'
                                : '制品名称未读取')}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">更新时间</dt>
                        <dd>
                          {formatDateTime(displayedConfiguration?.updatedAt ?? task.updatedAt)}
                        </dd>
                      </div>
                    </dl>
                    <div className="mt-4 min-w-0" aria-label="任务映射明细">
                      <Table
                        size="small"
                        pagination={false}
                        scroll={{ x: 450, y: 220 }}
                        rowKey="key"
                        dataSource={mappings.map((row, key) => ({ ...row, key }))}
                        columns={[
                          {
                            title: task.taskType !== 'KAFKA_KAFKA' ? '来源表' : '来源 Topic',
                            width: '40%',
                            render: (_, row) => (
                              <span className="break-all">
                                {mappingSourceLabel(row, task.taskType) || '未配置'}
                              </span>
                            ),
                          },
                          {
                            title: '目标 Topic',
                            render: (_, row) =>
                              row.targets.length > 0
                                ? row.targets.map((item) => (
                                    <Tag
                                      key={item.topic}
                                      className="my-1 max-w-full break-all whitespace-normal"
                                    >
                                      {item.topic}
                                    </Tag>
                                  ))
                                : '未配置',
                          },
                        ]}
                      />
                      <p className="mt-2 text-xs text-muted-foreground">
                        {mappings.length} 个来源 · {mappingTargets(mappings).length} 个目标 Topic
                      </p>
                    </div>
                  </Card>
                ),
              },
              {
                key: 'run',
                label: '本次运行',
                children: (
                  <div className="space-y-3">
                    <Card size="small">
                      <div className="mb-3">
                        {progress.error && (
                          <p className="mb-2 text-xs text-muted-foreground">
                            消费进度刷新失败，保留上次已采集结果。
                          </p>
                        )}
                        <TaskProgressSummary
                          running={task.observedState === 'RUNNING'}
                          progress={progress.items.find((item) => item.taskId === task.id)}
                        />
                      </div>
                      {loading ? (
                        <Spin size="small" />
                      ) : runtimeFacts?.run === null ? (
                        <p className="text-muted-foreground">该任务尚无运行记录。</p>
                      ) : runtimeFacts?.run != null ? (
                        <div className="space-y-3">
                          <p className="break-all text-xs text-muted-foreground">
                            运行记录 {runtimeFacts.run.id}
                          </p>
                          <dl className="space-y-3 text-sm">
                            <div className="flex justify-between gap-3">
                              <dt className="text-muted-foreground">运行状态</dt>
                              <dd>
                                <Tag color={statusColor(runtimeFacts.run.status)}>
                                  {operationLabel(runtimeFacts.run.status)}
                                </Tag>
                              </dd>
                            </div>
                            <div className="flex justify-between gap-3">
                              <dt className="text-muted-foreground">当前阶段</dt>
                              <dd>{operationLabel(runtimeFacts.run.currentStage)}</dd>
                            </div>
                            <div>
                              <dt className="text-muted-foreground">开始时间</dt>
                              <dd>{formatDateTime(runtimeFacts.run.startedAt)}</dd>
                            </div>
                            {runtimeFacts.run.endedAt !== null && (
                              <div>
                                <dt className="text-muted-foreground">结束时间</dt>
                                <dd>{formatDateTime(runtimeFacts.run.endedAt)}</dd>
                              </div>
                            )}
                            <div className="flex justify-between gap-3">
                              <dt className="text-muted-foreground">日志存储</dt>
                              <dd>{operationLabel(runtimeFacts.run.logStorageState)}</dd>
                            </div>
                            <div className="flex justify-between gap-3">
                              <dt className="text-muted-foreground">日志恢复</dt>
                              <dd>{operationLabel(runtimeFacts.run.logRecoveryState)}</dd>
                            </div>
                          </dl>
                          {runtimeFacts.run.failureCode !== null && (
                            <p className="error-banner" role="alert">
                              {runtimeFacts.run.failureCode}：{runtimeFacts.run.failureSummary}
                            </p>
                          )}
                          {runtimeFacts.run.logRecoveryState !== 'NONE' && (
                            <Button
                              disabled={retryingLogStorage}
                              onClick={() => void retryLogStorage()}
                            >
                              {retryingLogStorage ? '正在重试日志存储…' : '重试日志存储'}
                            </Button>
                          )}
                        </div>
                      ) : (
                        <p className="text-muted-foreground">运行信息尚未读取。</p>
                      )}
                    </Card>
                    {runtimeFacts?.operation != null && (
                      <Card title="最近操作" size="small">
                        <p>
                          {operationLabel(runtimeFacts.operation.action)} ·{' '}
                          {operationLabel(runtimeFacts.operation.state)}
                        </p>
                        {runtimeFacts.operation.failureCode !== null && (
                          <p className="error-banner" role="alert">
                            {runtimeFacts.operation.failureSummary ?? '操作未完成'}（
                            {runtimeFacts.operation.failureCode}）
                          </p>
                        )}
                      </Card>
                    )}
                  </div>
                ),
              },
              {
                key: 'stages',
                label: '阶段进度',
                children:
                  runtimeFacts?.run != null ? (
                    <Card size="small">
                      {runtimeFacts.stages.length === 0 ? (
                        <p className="text-muted-foreground">尚无阶段进度。</p>
                      ) : (
                        <Timeline
                          items={runtimeFacts.stages.map((stage) => {
                            const total = stage.total === null ? null : Number(stage.total);
                            const completed = Number(stage.completed);
                            const percent =
                              total !== null && total > 0
                                ? Math.min(100, (completed / total) * 100)
                                : null;
                            return {
                              key: stage.order,
                              color: statusColor(stage.status),
                              content: (
                                <div className="space-y-1 text-sm">
                                  <p>
                                    <strong>{operationLabel(stage.type)}</strong> ·{' '}
                                    {operationLabel(stage.status)}
                                  </p>
                                  <p className="text-muted-foreground">
                                    已处理 {stage.completed}
                                    {stage.total === null ? ' · 总量未知' : ` / ${stage.total}`}
                                    {percent === null ? '' : ` · ${percent.toFixed(1)}%`}
                                  </p>
                                  {stage.throughput !== null && <p>吞吐 {stage.throughput}</p>}
                                  {stage.heartbeatAt !== null && (
                                    <p className="text-xs text-muted-foreground">
                                      心跳 {formatDateTime(stage.heartbeatAt)}
                                    </p>
                                  )}
                                  {stage.failureCode !== null && (
                                    <p className="text-destructive">
                                      {stage.failureCode}：{stage.failureSummary}
                                    </p>
                                  )}
                                  {stage.retryable && <p>可重试</p>}
                                </div>
                              ),
                            };
                          })}
                        />
                      )}
                      {runtimeFacts.units.length > 0 && (
                        <div className="space-y-2 border-t pt-3">
                          <h4 className="text-sm font-medium">运行单元</h4>
                          {runtimeFacts.units.map((unit) => (
                            <div key={unit.key} className="text-sm">
                              <div className="flex justify-between gap-2">
                                <span>{unit.key}</span>
                                <Tag color={statusColor(unit.status)}>
                                  {operationLabel(unit.status)}
                                </Tag>
                              </div>
                              {unit.failureCode !== null && (
                                <p className="text-destructive">
                                  {unit.failureCode}：{unit.failureSummary}
                                </p>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </Card>
                  ) : loading ? (
                    <Spin size="small" />
                  ) : (
                    <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无运行阶段。" />
                  ),
              },
            ]}
          />
        </aside>
      </div>
      <Modal
        cancelText="取消"
        confirmLoading={stopping}
        okButtonProps={{ danger: true }}
        okText="确认停止"
        onCancel={() => setStopConfirmOpen(false)}
        onOk={async () => {
          await stop();
          setStopConfirmOpen(false);
        }}
        open={stopConfirmOpen}
        title="确认停止任务"
      >
        <p>将受控停止同步，保留同步位点和历史记录。</p>
      </Modal>
    </article>
  );
}
