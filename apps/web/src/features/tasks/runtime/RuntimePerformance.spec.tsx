import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  PerformanceSummary,
  PerformanceTrend,
  RuntimePerformancePanel,
} from './RuntimePerformance';
import { usePerformanceSummaries } from './usePerformanceSummaries';
import { performanceApi, PlatformApiError } from '../../../api/platform-api';
import type { RuntimePerformanceDetail } from '@scs/contracts';
import { getInstanceByDom } from 'echarts/core';
import { createRef } from 'react';
import { ThemeProvider } from '../../../app/ThemeProvider';
import { ThemeToggle } from '../../../components/ThemeToggle';
import { THEME_STORAGE_KEY } from '../../../app/theme-context';

beforeEach(() => {
  // jsdom has no canvas; ECharts' SVG renderer only uses it to measure text.
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(
    () =>
      ({
        measureText: (text: string) => ({ width: text.length * 7 }),
      }) as unknown as CanvasRenderingContext2D,
  );
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('performance display', () => {
  it('updates a live chart theme without recreating it or losing its zoom window', () => {
    const values = {
      recordsPerSecond: 2,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    try {
      render(
        <ThemeProvider>
          <ThemeToggle />
          <PerformanceTrend
            field="recordsPerSecond"
            label="主题趋势"
            points={[
              { ...values, collectedAt: '2026-10-22T00:00:00Z', gap: false },
              { ...values, collectedAt: '2026-10-22T00:01:00Z', gap: false },
            ]}
          />
        </ThemeProvider>,
      );
      const element = screen.getByRole('img', { name: '主题趋势，缺口处断开' });
      const chart = getInstanceByDom(element)!;
      act(() => chart.dispatchAction({ type: 'dataZoom', start: 20, end: 80 }));
      fireEvent.click(screen.getByRole('button', { name: '切换为深色主题' }));
      expect(getInstanceByDom(element)).toBe(chart);
      expect(chart.getOption()).toMatchObject({
        tooltip: [{ backgroundColor: '#18181b' }],
        series: [{ lineStyle: { color: '#60a5fa' } }],
        dataZoom: [
          { start: 20, end: 80 },
          { start: 20, end: 80 },
        ],
      });
      fireEvent.click(screen.getByRole('button', { name: '切换为浅色主题' }));
      expect(getInstanceByDom(element)).toBe(chart);
      expect(chart.getOption()).toMatchObject({ series: [{ lineStyle: { color: '#2563eb' } }] });
    } finally {
      localStorage.removeItem(THEME_STORAGE_KEY);
      document.documentElement.classList.remove('dark');
      document.documentElement.style.colorScheme = '';
    }
  });
  it('keeps history keyboard-accessible without an extra point slider', () => {
    const selected = vi.fn();
    const values = {
      recordsPerSecond: 2,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    render(
      <PerformanceTrend
        field="recordsPerSecond"
        label="吞吐趋势"
        selectedIndex={1}
        onSelectedIndexChange={selected}
        points={[0, 1, 2].map((index) => ({
          ...values,
          collectedAt: new Date(1790726400000 + index * 15000).toISOString(),
          gap: false,
        }))}
      />,
    );
    expect(screen.queryByRole('slider', { name: '历史数据点' })).not.toBeInTheDocument();
    const region = screen.getByRole('region', { name: /吞吐趋势横向趋势/u });
    fireEvent.keyDown(region, { key: 'ArrowLeft' });
    expect(selected).toHaveBeenLastCalledWith(0);
    fireEvent.keyDown(region, { key: 'End' });
    expect(selected).toHaveBeenLastCalledWith(2);
  });

  it('does not animate unchanged metric values on initial load or refresh', async () => {
    const values = {
      recordsPerSecond: 12,
      bytesPerSecond: 1024,
      sinkRecords: 24,
      sinkBytes: 2048,
      queueRecords: 0,
      queueBytes: 0,
    };
    vi.spyOn(performanceApi, 'detail').mockImplementation(async () => ({
      summary: {
        taskId: 'a',
        runId: 'r',
        status: 'FRESH',
        collectedAt: new Date().toISOString(),
        values,
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [],
      maintenance: { status: 'HEALTHY', lastAttemptAt: null },
    }));
    const frame = vi.spyOn(globalThis, 'requestAnimationFrame');
    const panel = createRef<{ refresh: () => Promise<void> }>();
    render(<RuntimePerformancePanel ref={panel} taskId="a" onSessionInvalidated={vi.fn()} />);
    await screen.findByLabelText('12条/秒');
    expect(frame).not.toHaveBeenCalled();
    await act(async () => panel.current?.refresh());
    expect(frame).not.toHaveBeenCalled();
  });

  it.each([
    [0, '0 B/秒'],
    [1024, '1 KiB/秒'],
    [1572864, '1.5 MiB/秒'],
    [1073741824, '1 GiB/秒'],
  ])('formats %s bytes without changing its meaning', (value, label) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:00Z'));
    render(
      <PerformanceSummary
        summary={{
          taskId: 'a',
          runId: 'r',
          status: 'FRESH',
          collectedAt: '2026-09-30T00:00:00Z',
          delayStatus: 'UNSUPPORTED',
          values: {
            recordsPerSecond: 0,
            bytesPerSecond: Number(value),
            sinkRecords: 0,
            sinkBytes: 0,
            queueRecords: 0,
            queueBytes: 0,
          },
        }}
      />,
    );
    expect(screen.getByText(String(label))).toBeInTheDocument();
  });
  it('shows known partial values without inventing missing fields', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:00Z'));
    const values = {
      recordsPerSecond: 5,
      bytesPerSecond: null,
      sinkRecords: 25,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    render(
      <PerformanceSummary
        summary={{
          taskId: 'a',
          runId: 'r',
          status: 'PARTIAL',
          collectedAt: '2026-09-30T00:00:00Z',
          values,
          delayStatus: 'UNSUPPORTED',
        }}
      />,
    );
    expect(screen.getByText('部分缺失')).toBeInTheDocument();
    expect(screen.getByText('5 条/秒')).toBeInTheDocument();
    expect(screen.getByText('— B/秒')).toBeInTheDocument();
  });
  it('keeps task controls usable after an independent performance failure and handles current unauthorized responses', async () => {
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(
      new PlatformApiError('SESSION_INVALID', 401),
    );
    const invalidated = vi.fn();
    render(
      <>
        <button>停止任务</button>
        <RuntimePerformancePanel taskId="a" onSessionInvalidated={invalidated} />
      </>,
    );
    expect(await screen.findByText('性能数据读取失败，不影响任务操作。')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '停止任务' })).toBeEnabled();
    expect(invalidated).toHaveBeenCalledOnce();
  });

  it('chunks summary reads at 100 task IDs and does not invent missing summaries', async () => {
    const query = vi.spyOn(performanceApi, 'query').mockResolvedValue({ items: [] });
    const invalidated = vi.fn();
    function List() {
      const data = usePerformanceSummaries(
        Array.from({ length: 101 }, (_, index) => `task-${index}`),
        invalidated,
      );
      return <p>{data.items.length} 条摘要</p>;
    }
    render(<List />);
    await waitFor(() => expect(query).toHaveBeenCalledTimes(2));
    expect(query.mock.calls[0]?.[0]).toHaveLength(100);
    expect(query.mock.calls[1]?.[0]).toEqual(['task-100']);
    expect(screen.getByText('0 条摘要')).toBeInTheDocument();
  });
  it('refreshes on request and ignores a previous task response and a late unauthorized response after unmount', async () => {
    vi.useFakeTimers();
    let resolveOld!: (value: RuntimePerformanceDetail) => void;
    let rejectLast!: (error: unknown) => void;
    const values = {
      recordsPerSecond: null,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    const detail: RuntimePerformanceDetail = {
      summary: {
        taskId: 'b',
        runId: null,
        status: 'NO_DATA',
        collectedAt: null,
        values,
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [],
      maintenance: { status: 'UNKNOWN', lastAttemptAt: null },
    };
    vi.spyOn(performanceApi, 'detail')
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveOld = resolve;
          }),
      )
      .mockResolvedValueOnce(detail)
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            rejectLast = reject;
          }),
      );
    const invalidated = vi.fn();
    const panel = createRef<{ refresh: () => Promise<void> }>();
    const view = render(
      <RuntimePerformancePanel ref={panel} taskId="a" onSessionInvalidated={invalidated} />,
    );
    view.rerender(
      <RuntimePerformancePanel ref={panel} taskId="b" onSessionInvalidated={invalidated} />,
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.getByText('尚未采集')).toBeInTheDocument();
    expect(screen.queryByText('同步延迟：暂不支持')).not.toBeInTheDocument();
    await act(async () =>
      resolveOld({
        ...detail,
        summary: {
          ...detail.summary,
          status: 'FRESH',
          values: { ...values, recordsPerSecond: 999 },
        },
      }),
    );
    expect(screen.queryByText('999 条/秒')).not.toBeInTheDocument();
    await act(async () => {
      void panel.current?.refresh();
      void panel.current?.refresh();
    });
    expect(performanceApi.detail).toHaveBeenCalledTimes(3);
    view.unmount();
    await act(async () => rejectLast(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
  });
  it('breaks trends at missing data and explicit gaps', () => {
    const point = {
      recordsPerSecond: 2,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    render(
      <PerformanceTrend
        field="recordsPerSecond"
        label="吞吐趋势"
        points={[
          { ...point, collectedAt: '2026-09-30T00:00:00Z', gap: false },
          { ...point, collectedAt: '2026-09-30T00:00:15Z', gap: true },
          { ...point, collectedAt: '2026-09-30T00:00:30Z', gap: false },
        ]}
      />,
    );
    const chart = screen.getByRole('img', { name: /吞吐趋势/ });
    const line = chart.querySelector('path[stroke="#2563eb"][fill="none"]');
    expect(line).not.toBeNull();
    expect(line!.getAttribute('d')?.match(/M/g)).toHaveLength(2);
  });
  it('distinguishes unavailable data from real zero and never calls stopped history current', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:00Z'));
    const values = {
      recordsPerSecond: 0,
      bytesPerSecond: null,
      sinkRecords: 25,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    const summary = {
      taskId: 't1',
      runId: 'r1',
      status: 'FRESH' as const,
      collectedAt: '2026-09-30T00:00:00Z',
      values,
      delayStatus: 'UNSUPPORTED' as const,
    };
    const view = render(<PerformanceSummary summary={summary} />);
    expect(screen.getByText('0 条/秒')).toBeInTheDocument();
    expect(screen.getByText('— B/秒')).toBeInTheDocument();
    view.rerender(<PerformanceSummary running={false} summary={summary} />);
    expect(screen.getByLabelText('任务未运行，无吞吐数据')).toHaveTextContent('—');
    expect(screen.queryByText('0 条/秒')).not.toBeInTheDocument();
    expect(screen.queryByText('同步延迟：暂不支持')).not.toBeInTheDocument();
  });
  it('expires a fresh value even while the next network request is pending', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:00Z'));
    const values = {
      recordsPerSecond: 12,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    render(
      <PerformanceSummary
        summary={{
          taskId: 'a',
          runId: 'r',
          status: 'FRESH',
          collectedAt: '2026-09-30T00:00:00Z',
          values,
          delayStatus: 'UNSUPPORTED',
        }}
      />,
    );
    expect(screen.getByText('12 条/秒')).toBeInTheDocument();
    await act(async () => vi.advanceTimersByTimeAsync(45001));
    expect(screen.getByText('已过期')).toBeInTheDocument();
    expect(screen.queryByText('12 条/秒')).not.toBeInTheDocument();
  });
  it('links the rolling metric cards to snapped history points and limits history to seven days', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:30Z'));
    const baseValues = {
      recordsPerSecond: 30,
      bytesPerSecond: 3072,
      sinkRecords: 300,
      sinkBytes: 30720,
      queueRecords: 3,
      queueBytes: 307,
    };
    vi.spyOn(performanceApi, 'detail').mockResolvedValue({
      summary: {
        taskId: 'task-a',
        runId: 'run-a',
        status: 'FRESH',
        collectedAt: '2026-09-30T00:00:30Z',
        values: baseValues,
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [
        {
          ...baseValues,
          recordsPerSecond: 10,
          collectedAt: '2026-09-30T00:00:00Z',
          gap: false,
        },
        {
          ...baseValues,
          recordsPerSecond: 20,
          collectedAt: '2026-09-30T00:00:15Z',
          gap: false,
        },
      ],
      maintenance: { status: 'HEALTHY', lastAttemptAt: '2026-09-30T00:00:30Z' },
    });

    render(<RuntimePerformancePanel taskId="task-a" onSessionInvalidated={vi.fn()} />);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText('数据时间：2026年09月30日 08:00:30')).toBeInTheDocument();
    expect(screen.getByText('当前运行')).toBeInTheDocument();
    expect(screen.getByLabelText('30条/秒')).toBeInTheDocument();
    expect(screen.getByRole('img', { name: /缺口处断开/u }).querySelector('svg')).not.toBeNull();

    fireEvent.mouseDown(screen.getByLabelText('趋势时间范围'));
    expect(screen.getByText('近 7 天')).toBeInTheDocument();
    expect(screen.queryByText('近 30 天')).not.toBeInTheDocument();
    expect(screen.queryByText('近 365 天')).not.toBeInTheDocument();

    const container = screen.getByRole('img', { name: /缺口处断开/u });
    const chart = getInstanceByDom(container)!;
    const redraw = vi.spyOn(chart, 'setOption');
    act(() =>
      chart.dispatchAction({
        type: 'updateAxisPointer',
        currTrigger: 'mousemove',
        x: chart.convertToPixel({ xAxisIndex: 0 }, 0),
        y: 100,
      }),
    );
    expect(screen.getByText('已选择历史数据点')).toBeInTheDocument();
    expect(screen.getByText('数据时间：2026年09月30日 08:00:00')).toBeInTheDocument();
    expect(screen.getByLabelText('10条/秒')).toBeInTheDocument();

    fireEvent.keyDown(screen.getByRole('region', { name: /记录吞吐.*横向趋势/u }), {
      key: 'ArrowRight',
    });
    expect(screen.getByText('数据时间：2026年09月30日 08:00:15')).toBeInTheDocument();
    expect(screen.getByLabelText('20条/秒')).toBeInTheDocument();
    expect(getInstanceByDom(container)).toBe(chart);
    expect(redraw).not.toHaveBeenCalled();
    expect(performanceApi.detail).toHaveBeenCalledOnce();

    fireEvent.click(screen.getByRole('button', { name: '回到最新数据' }));
    expect(screen.getByText('数据时间：2026年09月30日 08:00:30')).toBeInTheDocument();
    expect(screen.getByLabelText('30条/秒')).toBeInTheDocument();
  });
  it('links real chart panning to historical values and shows gaps without falling back to live values', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-30T00:00:30Z'));
    const values = {
      recordsPerSecond: 30,
      bytesPerSecond: 3072,
      sinkRecords: 300,
      sinkBytes: 30720,
      queueRecords: 3,
      queueBytes: 307,
    };
    const detail: RuntimePerformanceDetail = {
      summary: {
        taskId: 'task-a',
        runId: 'run-a',
        status: 'FRESH',
        collectedAt: '2026-09-30T00:00:30Z',
        values,
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [
        { ...values, recordsPerSecond: 10, collectedAt: '2026-09-30T00:00:00Z', gap: false },
        { ...values, collectedAt: '2026-09-30T00:00:15Z', gap: true },
        { ...values, collectedAt: '2026-09-30T00:00:30Z', gap: false },
      ],
      maintenance: { status: 'HEALTHY', lastAttemptAt: '2026-09-30T00:00:30Z' },
    };
    vi.spyOn(performanceApi, 'detail').mockImplementation(async () => structuredClone(detail));
    const panel = createRef<{ refresh: () => Promise<void> }>();
    const view = render(
      <RuntimePerformancePanel ref={panel} taskId="task-a" onSessionInvalidated={vi.fn()} />,
    );
    await act(async () => {
      await Promise.resolve();
    });
    const container = screen.getByRole('img', { name: /缺口处断开/u });
    const chart = getInstanceByDom(container)!;
    act(() => chart.dispatchAction({ type: 'dataZoom', start: 0, end: 10 }));
    expect(screen.getByLabelText('10条/秒')).toBeInTheDocument();
    expect(screen.getByText('数据时间：2026年09月30日 08:00:00')).toBeInTheDocument();
    const position = chart.convertToPixel({ xAxisIndex: 0 }, 0);
    await act(async () => panel.current?.refresh());
    expect(getInstanceByDom(container)).toBe(chart);
    expect(chart.convertToPixel({ xAxisIndex: 0 }, 0)).toEqual(position);
    expect(screen.getByText('已选择历史数据点')).toBeInTheDocument();
    fireEvent.keyDown(screen.getByRole('region', { name: /记录吞吐.*横向趋势/u }), {
      key: 'ArrowRight',
    });
    expect(screen.getByText('数据时间：2026年09月30日 08:00:15')).toBeInTheDocument();
    expect(screen.getByLabelText('—条/秒')).toBeInTheDocument();
    expect(screen.queryByLabelText('30条/秒')).not.toBeInTheDocument();
    view.unmount();
    expect(chart.isDisposed()).toBe(true);
  });
  it('shows actual historical samples after stop without presenting them as live throughput', async () => {
    const values = {
      recordsPerSecond: 10,
      bytesPerSecond: null,
      sinkRecords: 100,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    vi.spyOn(performanceApi, 'detail').mockResolvedValue({
      summary: {
        taskId: 'task-a',
        runId: 'run-a',
        status: 'NOT_RUNNING',
        collectedAt: null,
        values,
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [
        { ...values, collectedAt: '2026-09-30T00:00:00Z', gap: false },
        { ...values, collectedAt: '2026-09-30T00:00:15Z', gap: false },
      ],
      maintenance: { status: 'HEALTHY', lastAttemptAt: null },
    });
    render(
      <RuntimePerformancePanel taskId="task-a" running={false} onSessionInvalidated={vi.fn()} />,
    );
    expect(await screen.findByText('历史运行')).toBeInTheDocument();
    expect(screen.getByText('历史运行')).toBeInTheDocument();
    expect(performanceApi.detail).toHaveBeenCalledWith('task-a', '7d');
    expect(screen.queryByLabelText('—条/秒')).not.toBeInTheDocument();
    expect(screen.getByLabelText('10条/秒')).toBeInTheDocument();
    fireEvent.keyDown(screen.getByRole('region', { name: /记录吞吐.*横向趋势/u }), { key: 'Home' });
    expect(screen.getByLabelText('10条/秒')).toBeInTheDocument();
    fireEvent.keyDown(screen.getByRole('region', { name: /记录吞吐.*横向趋势/u }), {
      key: 'ArrowRight',
    });
    expect(screen.getByText('数据时间：2026年09月30日 08:00:15')).toBeInTheDocument();
    expect(screen.getByLabelText('10条/秒')).toBeInTheDocument();
  });

  it('selects an older run and never reuses that selection for another task', async () => {
    const empty = {
      recordsPerSecond: null,
      bytesPerSecond: null,
      sinkRecords: null,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    };
    const load = vi
      .spyOn(performanceApi, 'detail')
      .mockImplementation(async (taskId, window, runId) => ({
        summary: {
          taskId,
          runId: runId ?? 'new-run',
          status: 'NOT_RUNNING',
          collectedAt: null,
          values: empty,
          delayStatus: 'UNSUPPORTED',
        },
        window: window ?? '1h',
        runs: [
          {
            id: 'old-run',
            status: 'STOPPED',
            startedAt: '2026-10-14T01:00:00Z',
            endedAt: '2026-10-14T02:00:00Z',
          },
        ],
        points:
          runId === undefined
            ? []
            : [{ ...empty, recordsPerSecond: 12, collectedAt: '2026-10-14T01:00:15Z', gap: false }],
        maintenance: { status: 'HEALTHY', lastAttemptAt: null },
      }));
    const onSessionInvalidated = vi.fn();
    const view = render(
      <RuntimePerformancePanel
        taskId="task-a"
        running={false}
        onSessionInvalidated={onSessionInvalidated}
      />,
    );
    expect(await screen.findByText('未运行')).toBeInTheDocument();
    expect(screen.queryByText('记录吞吐')).not.toBeInTheDocument();
    fireEvent.mouseDown(screen.getByLabelText('性能所属运行'));
    fireEvent.click(await screen.findByText('2026年10月14日 09:00:00 · old-run · 历史运行'));
    expect(await screen.findByText('历史运行')).toBeInTheDocument();
    expect(load).toHaveBeenLastCalledWith('task-a', '7d', 'old-run');
    expect(screen.getByLabelText('12条/秒')).toBeInTheDocument();
    view.rerender(
      <RuntimePerformancePanel
        taskId="task-b"
        running={false}
        onSessionInvalidated={onSessionInvalidated}
      />,
    );
    await waitFor(() => expect(load).toHaveBeenLastCalledWith('task-b', '7d'));
    expect(screen.queryByLabelText('12条/秒')).not.toBeInTheDocument();
  });
});
