import { useEffect, useRef } from 'react';
import { init, use as registerCharts, type EChartsType } from 'echarts/core';
import { LineChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, DataZoomComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import type { RuntimePerformancePoint, RuntimePerformanceValues } from '@scs/contracts';
import { formatDateTime } from '../../../lib/date-time';
import { useTheme } from '../../../app/theme-context';
import { InfoTip } from '../../../components/InfoTip';

registerCharts([LineChart, GridComponent, TooltipComponent, DataZoomComponent, SVGRenderer]);

export function EChartsTrend({
  points,
  field,
  label,
  selectedIndex,
  followLatest = false,
  onSelectedIndexChange,
  formatValue,
}: {
  points: RuntimePerformancePoint[];
  field: keyof RuntimePerformanceValues;
  label: string;
  selectedIndex?: number;
  followLatest?: boolean;
  onSelectedIndexChange?: (index: number) => void;
  formatValue: (value: number | null) => string;
}) {
  const { mode } = useTheme();
  const element = useRef<HTMLDivElement>(null);
  const chart = useRef<EChartsType | null>(null);
  const programmatic = useRef(false);
  const activeIndex = selectedIndex ?? points.length - 1;
  const state = useRef({ points, activeIndex, onSelectedIndexChange });
  state.current = { points, activeIndex, onSelectedIndexChange };
  const available = points.some((point) => !point.gap && point[field] !== null);

  useEffect(() => {
    if (!available || element.current === null) return;
    const container = element.current;
    const instance = init(container, undefined, {
      renderer: 'svg',
      width: container.clientWidth || 720,
      height: 280,
    });
    chart.current = instance;
    const select = (index: number) => {
      if (programmatic.current) return;
      const current = state.current;
      const bounded = Math.max(0, Math.min(current.points.length - 1, index));
      if (bounded !== current.activeIndex) current.onSelectedIndexChange?.(bounded);
    };
    instance.on('updateAxisPointer', (event: unknown) => {
      const value = (event as { axesInfo?: { value: number }[] }).axesInfo?.[0]?.value;
      if (value !== undefined) select(Math.round(Number(value)));
    });
    instance.on('datazoom', (event: unknown) => {
      const zoom = event as {
        start?: number;
        end?: number;
        batch?: { start: number; end: number }[];
      };
      const range = zoom.batch?.[0] ?? zoom;
      if (range.start !== undefined && range.end !== undefined) {
        select(Math.round(((range.start + range.end) / 200) * (state.current.points.length - 1)));
      }
    });
    const observer = new ResizeObserver(() =>
      instance.resize({ width: container.clientWidth || 720 }),
    );
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart.current = null;
      instance.dispose();
    };
  }, [available]);

  useEffect(() => {
    const instance = chart.current;
    if (instance === null) return;
    const dark = mode === 'dark';
    const textColor = dark ? '#a1a1aa' : '#64748b';
    const borderColor = dark ? '#333338' : '#e5e7eb';
    const lineColor = dark ? '#60a5fa' : '#2563eb';
    const surface = dark ? '#18181b' : '#ffffff';
    instance.setOption({
      animationDurationUpdate: 480,
      grid: { left: 68, right: 20, top: 20, bottom: 76 },
      tooltip: {
        backgroundColor: surface,
        borderColor,
        textStyle: { color: dark ? '#fafafa' : '#1e293b' },
        trigger: 'axis',
        confine: true,
        axisPointer: { type: 'cross', snap: true },
        formatter: (params: { dataIndex: number }[]) => {
          const point = points[params[0]?.dataIndex ?? -1];
          return point === undefined
            ? ''
            : `${formatDateTime(point.collectedAt)}<br/>${label}：${formatValue(point.gap ? null : point[field])}`;
        },
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: points.map((point) => point.collectedAt),
        axisLine: { lineStyle: { color: borderColor } },
        axisLabel: {
          color: textColor,
          formatter: (value: string) => formatDateTime(value),
          hideOverlap: true,
        },
        axisPointer: {
          label: { formatter: (params: { value: string }) => formatDateTime(params.value) },
        },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: textColor, formatter: (value: number) => formatValue(value) },
        splitLine: { lineStyle: { color: borderColor } },
      },
      dataZoom: [
        { type: 'inside', filterMode: 'none' },
        {
          type: 'slider',
          backgroundColor: surface,
          borderColor,
          textStyle: { color: textColor },
          fillerColor: dark ? 'rgba(96,165,250,0.12)' : 'rgba(37,99,235,0.12)',
          dataBackground: { lineStyle: { color: textColor }, areaStyle: { color: borderColor } },
          selectedDataBackground: {
            lineStyle: { color: lineColor },
            areaStyle: { color: lineColor },
          },
          handleStyle: { color: surface, borderColor: textColor },
          filterMode: 'none',
          height: 22,
          bottom: 8,
          labelFormatter: (_value: number, text: string) => formatDateTime(text),
        },
      ],
      series: [
        {
          type: 'line',
          data: points.map((point) => (point.gap ? null : point[field])),
          connectNulls: false,
          showSymbol: true,
          symbolSize: 5,
          lineStyle: { width: 2.5, color: lineColor },
          itemStyle: { color: lineColor },
          areaStyle: { color: lineColor, opacity: dark ? 0.12 : 0.07 },
        },
      ],
    });
  }, [points, field, label, formatValue, available, mode]);

  useEffect(() => {
    if (activeIndex < 0 || chart.current === null) return;
    programmatic.current = true;
    if (followLatest) chart.current.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
    programmatic.current = false;
  }, [activeIndex, followLatest, available]);

  if (!available) return <p>{label}：暂无有效采样</p>;
  const point = points[activeIndex];
  return (
    <figure className="min-w-0 space-y-2">
      <figcaption className="flex flex-wrap items-center justify-between gap-2 text-sm">
        <span className="flex items-center gap-1 font-medium">
          {label}
          <InfoTip
            label="趋势图操作说明"
            text="悬停查看采样值，横向平移或缩放查看历史；聚焦图表后可用方向键逐点选择。"
          />
        </span>
        <span className="text-muted-foreground">
          {formatDateTime(point?.collectedAt)} ·{' '}
          {formatValue(point === undefined || point.gap ? null : point[field])}
        </span>
      </figcaption>
      <div
        role="region"
        aria-label={`${label}横向趋势，滑动后吸附到最近数据点`}
        tabIndex={0}
        aria-keyshortcuts="ArrowLeft ArrowRight Home End"
        onKeyDown={(event) => {
          const index = {
            ArrowLeft: Math.max(0, activeIndex - 1),
            ArrowRight: Math.min(points.length - 1, activeIndex + 1),
            Home: 0,
            End: points.length - 1,
          }[event.key];
          if (index === undefined) return;
          event.preventDefault();
          onSelectedIndexChange?.(index);
        }}
        className="overflow-hidden rounded-lg border bg-background focus-visible:outline-2 focus-visible:outline-ring"
      >
        <div
          ref={element}
          role="img"
          aria-label={`${label}，缺口处断开`}
          className="h-[280px] w-full"
        />
      </div>
    </figure>
  );
}
