import type {
  RuntimePerformanceDetail,
  RuntimePerformancePoint,
  RuntimePerformanceSummary,
  RuntimePerformanceValues,
} from '@scs/contracts';
import { memo, useEffect, useImperativeHandle, useMemo, useRef, useState, type Ref } from 'react';
import { Alert, Card, Select, Tag } from 'antd';
import type { LucideIcon } from 'lucide-react';
import { Activity, DatabaseZap, Gauge, HardDrive, ListTree, Rows3 } from 'lucide-react';

import { performanceApi, PlatformApiError } from '../../../api/platform-api';
import { InfoTip } from '../../../components/InfoTip';
import { Button } from '../../../components/ui/button';
import { formatDateTime } from '../../../lib/date-time';
import { EChartsTrend } from './EChartsTrend';

const statusLabels = {
  FRESH: '实时',
  PARTIAL: '部分缺失',
  STALE: '已过期',
  NO_DATA: '尚未采集',
  NOT_RUNNING: '未运行',
};
const numberFormatter = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 });
const number = (value: number | null) => (value === null ? '—' : numberFormatter.format(value));
function bytes(value: number | null) {
  if (value === null) return '— B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const index = value === 0 ? 0 : Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
  const safeIndex = Math.max(0, index);
  return `${number(value / 1024 ** safeIndex)} ${units[safeIndex]}`;
}

interface MetricDefinition {
  value: keyof RuntimePerformanceValues;
  label: string;
  shortLabel: string;
  icon: LucideIcon;
  format: (value: number | null) => string;
  suffix: string;
}

const metrics: MetricDefinition[] = [
  {
    value: 'recordsPerSecond',
    label: '记录吞吐（条/秒）',
    shortLabel: '记录吞吐',
    icon: Gauge,
    format: number,
    suffix: '条/秒',
  },
  {
    value: 'bytesPerSecond',
    label: '字节吞吐（字节/秒）',
    shortLabel: '字节吞吐',
    icon: Activity,
    format: bytes,
    suffix: '/秒',
  },
  {
    value: 'sinkRecords',
    label: '运行累计记录（条）',
    shortLabel: '累计记录',
    icon: Rows3,
    format: number,
    suffix: '条',
  },
  {
    value: 'sinkBytes',
    label: '运行累计字节（字节）',
    shortLabel: '累计字节',
    icon: DatabaseZap,
    format: bytes,
    suffix: '',
  },
  {
    value: 'queueRecords',
    label: '队列记录（条）',
    shortLabel: '队列记录',
    icon: ListTree,
    format: number,
    suffix: '条',
  },
  {
    value: 'queueBytes',
    label: '队列大小（字节）',
    shortLabel: '队列大小',
    icon: HardDrive,
    format: bytes,
    suffix: '',
  },
];

function useCurrentStatus(summary?: RuntimePerformanceSummary) {
  const [, refresh] = useState(0);
  const deadline = summary?.collectedAt == null ? 0 : Date.parse(summary.collectedAt) + 45000;
  useEffect(() => {
    if (summary?.status !== 'FRESH' && summary?.status !== 'PARTIAL') return;
    const timer = setTimeout(
      () => refresh((value) => value + 1),
      Math.max(0, deadline - Date.now() + 1),
    );
    return () => clearTimeout(timer);
  }, [deadline, summary?.status]);
  return (summary?.status === 'FRESH' || summary?.status === 'PARTIAL') && Date.now() > deadline
    ? 'STALE'
    : summary?.status;
}

const MetricTicker = memo(function MetricTicker({
  value,
  format,
  suffix,
}: {
  value: number | null;
  format: (value: number | null) => string;
  suffix: string;
}) {
  const previous = useRef(value);
  const [display, setDisplay] = useState(value);
  useEffect(() => {
    if (Object.is(value, previous.current)) return;
    if (value === null || previous.current === null || !Number.isFinite(previous.current)) {
      previous.current = value;
      setDisplay(value);
      return;
    }
    const from = previous.current;
    const startedAt = performance.now();
    let frame = 0;
    const tick = (now: number) => {
      const progress = Math.min(1, (now - startedAt) / 480);
      const eased = 1 - (1 - progress) ** 3;
      const next = from + (value - from) * eased;
      previous.current = next;
      setDisplay(next);
      if (progress < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [value]);
  return (
    <span
      aria-label={`${format(value)}${suffix}`}
      className="tabular-nums"
      data-final-value={value}
    >
      {format(display)}
      {suffix}
    </span>
  );
});

export function PerformanceSummary({
  running,
  summary,
}: {
  running?: boolean | undefined;
  summary?: RuntimePerformanceSummary | undefined;
}) {
  const status = useCurrentStatus(summary);
  const current = status === 'FRESH' || status === 'PARTIAL' ? summary?.values : null;
  if (running === false || status === 'NOT_RUNNING') {
    return (
      <span aria-label="任务未运行，无吞吐数据" className="text-sm text-muted-foreground">
        —
      </span>
    );
  }
  if (current === null || current === undefined) {
    return (
      <span className="text-sm text-muted-foreground">
        {status === undefined ? '读取中…' : statusLabels[status]}
      </span>
    );
  }
  return (
    <div className="text-sm whitespace-nowrap">
      <div className="flex gap-3 font-medium">
        <span>{number(current.recordsPerSecond)} 条/秒</span>
        <span>{bytes(current.bytesPerSecond)}/秒</span>
      </div>
      <div className="text-xs text-muted-foreground">
        <span>{status === undefined ? '暂无摘要' : statusLabels[status]}</span> ·{' '}
        {formatDateTime(summary?.collectedAt)}
      </div>
    </div>
  );
}

export function PerformanceTrend(props: {
  points: RuntimePerformancePoint[];
  field: keyof RuntimePerformanceValues;
  label: string;
  selectedIndex?: number;
  followLatest?: boolean;
  onSelectedIndexChange?: (index: number) => void;
}) {
  return (
    <EChartsTrend
      {...props}
      formatValue={props.field.toLowerCase().includes('bytes') ? bytes : number}
    />
  );
}

export const RuntimePerformancePanel = memo(function RuntimePerformancePanel({
  taskId,
  onSessionInvalidated,
  running,
  ref,
}: {
  taskId: string;
  onSessionInvalidated: () => void;
  running?: boolean;
  ref?: Ref<{ refresh: () => Promise<void> }>;
}) {
  const [window, setWindow] = useState<RuntimePerformanceDetail['window']>(
    running === false ? '7d' : '1h',
  );
  const [runSelection, setRunSelection] = useState({ taskId, runId: '' });
  const runId = runSelection.taskId === taskId ? runSelection.runId : '';
  const [metric, setMetric] = useState<keyof RuntimePerformanceValues>('recordsPerSecond');
  const [selectedAt, setSelectedAt] = useState<string | null>(null);
  const [result, setResult] = useState<{
    taskId: string;
    window: string;
    runId: string;
    detail: RuntimePerformanceDetail;
  } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const loader = useRef<(() => Promise<void>) | null>(null);
  useImperativeHandle(ref, () => ({ refresh: () => loader.current?.() ?? Promise.resolve() }), []);
  useEffect(() => {
    let active = true;
    let pending: Promise<void> | null = null;
    setError(null);
    setResult(null);
    setSelectedAt(null);
    const load = () => {
      if (pending !== null) return pending;
      pending = (async () => {
        try {
          const detail =
            runId === ''
              ? await performanceApi.detail(taskId, window)
              : await performanceApi.detail(taskId, window, runId);
          if (!active) return;
          setResult({ taskId, window, runId, detail });
          setError(null);
        } catch (cause) {
          if (!active) return;
          setResult(null);
          setError('性能数据读取失败，不影响任务操作。');
          if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        } finally {
          pending = null;
        }
      })();
      return pending;
    };
    loader.current = load;
    void load();
    return () => {
      active = false;
      if (loader.current === load) loader.current = null;
    };
  }, [taskId, window, runId, onSessionInvalidated]);
  const detail =
    result?.taskId === taskId && result.window === window && result.runId === runId
      ? result.detail
      : null;
  useEffect(() => setSelectedAt(null), [detail?.summary.runId]);
  const status = useCurrentStatus(detail?.summary);
  const current = status === 'FRESH' || status === 'PARTIAL' ? detail?.summary.values : null;
  const selectedIndex = useMemo(() => {
    if (detail === null || detail.points.length === 0) return -1;
    if (selectedAt === null) return detail.points.length - 1;
    const index = detail.points.findIndex((point) => point.collectedAt === selectedAt);
    return index < 0 ? detail.points.length - 1 : index;
  }, [detail, selectedAt]);
  const selectedPoint = detail?.points[selectedIndex];
  const historical = status === 'NOT_RUNNING';
  const displayedValues =
    selectedAt !== null || historical
      ? selectedPoint?.gap
        ? null
        : (selectedPoint ?? null)
      : (current ?? null);
  const displayTime =
    selectedAt !== null || historical ? selectedPoint?.collectedAt : detail?.summary.collectedAt;

  const trendOptions = useMemo(
    () => (
      <div className="flex flex-wrap items-center gap-2" aria-label="趋势选项">
        <Select
          aria-label="性能所属运行"
          value={runId}
          onChange={(value) => {
            setRunSelection({ taskId, runId: value });
            setSelectedAt(null);
          }}
          getPopupContainer={(trigger) => trigger.parentElement ?? trigger}
          options={[
            { value: '', label: '当前运行／最近一次运行' },
            ...(detail?.runs ?? []).map((run) => ({
              value: run.id,
              label: `${formatDateTime(run.startedAt)} · ${run.id.slice(0, 8)} · ${['STARTING', 'RUNNING', 'STOPPING'].includes(run.status) ? '当前运行' : '历史运行'}`,
            })),
          ]}
          style={{ minWidth: 230 }}
        />
        <Select
          aria-label="趋势时间范围"
          getPopupContainer={(trigger) => trigger.parentElement ?? trigger}
          value={window}
          onChange={setWindow}
          options={[
            { value: '1h', label: '近 1 小时' },
            { value: '24h', label: '近 24 小时' },
            { value: '7d', label: '近 7 天' },
          ]}
        />
        <Select
          aria-label="趋势指标"
          getPopupContainer={(trigger) => trigger.parentElement ?? trigger}
          value={metric}
          onChange={setMetric}
          options={metrics.map(({ value, label }) => ({ value, label }))}
          style={{ minWidth: 190 }}
        />
        <InfoTip
          label="运行监控说明"
          text="页面默认手动刷新，可开启自动刷新并选择间隔；平台持续采集，指标最长保留 7 天。速率由相邻累计量差分计算，首点与缺口不计算速率；趋势限定同一次运行。"
        />
      </div>
    ),
    [taskId, runId, detail?.runs, window, metric],
  );

  return (
    <Card>
      <div className="space-y-4">
        {error !== null ? (
          <Alert role="status" type="warning" title={error} />
        ) : detail === null ? (
          <p role="status">正在读取性能数据…</p>
        ) : (
          <>
            <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
              <Tag
                color={
                  selectedAt !== null || historical
                    ? 'blue'
                    : status === 'FRESH'
                      ? 'green'
                      : 'default'
                }
              >
                {selectedAt !== null
                  ? '已选择历史数据点'
                  : historical && detail.points.length > 0
                    ? '历史运行'
                    : status === 'FRESH'
                      ? '当前运行'
                      : status === undefined
                        ? '暂无摘要'
                        : statusLabels[status]}
              </Tag>
              <span>数据时间：{formatDateTime(displayTime)}</span>
              {selectedAt !== null && (
                <Button type="button" size="sm" variant="ghost" onClick={() => setSelectedAt(null)}>
                  回到最新数据
                </Button>
              )}
            </div>
            {(status !== 'NOT_RUNNING' || selectedAt !== null || detail.points.length > 0) && (
              <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {[metrics[0]!, metrics[1]!, metrics[2]!, metrics[4]!, metrics[3]!, metrics[5]!].map(
                  ({ value, shortLabel, icon: Icon, format, suffix }, index) => (
                    <div
                      key={value}
                      className={
                        index < 4
                          ? 'rounded-xl border border-border bg-muted/20 p-4 transition-colors hover:bg-muted/35'
                          : 'flex items-center justify-between gap-2 rounded-lg bg-muted/20 px-4 py-2 xl:col-span-2'
                      }
                    >
                      <dt className="flex items-center gap-2 text-xs text-muted-foreground">
                        <span className="flex size-7 items-center justify-center rounded-lg bg-primary/10 text-primary">
                          <Icon className="size-4" />
                        </span>
                        {shortLabel}
                      </dt>
                      <dd
                        className={
                          index < 4
                            ? 'mt-3 text-xl font-semibold tracking-tight'
                            : 'text-sm font-semibold'
                        }
                      >
                        <MetricTicker
                          value={displayedValues?.[value] ?? null}
                          format={format}
                          suffix={suffix}
                        />
                      </dd>
                    </div>
                  ),
                )}
              </dl>
            )}
            {detail.maintenance.status !== 'HEALTHY' && (
              <Alert
                role="status"
                type="warning"
                title={
                  detail.maintenance.status === 'FAILED'
                    ? '历史数据维护失败'
                    : '历史数据维护状态未知'
                }
              />
            )}
          </>
        )}
        {trendOptions}
        {detail !== null && (
          <PerformanceTrend
            label={metrics.find((item) => item.value === metric)!.label}
            field={metric}
            points={detail.points}
            selectedIndex={selectedIndex}
            followLatest={selectedAt === null}
            onSelectedIndexChange={(index) => {
              setSelectedAt(detail.points[index]!.collectedAt);
            }}
          />
        )}
      </div>
    </Card>
  );
});
