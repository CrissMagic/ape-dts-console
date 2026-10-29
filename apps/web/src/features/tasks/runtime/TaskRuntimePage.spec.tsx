import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import type { RuntimeLogRead, RuntimeRead, TaskSummary } from '@scs/contracts';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConfigProvider } from 'antd';

import {
  performanceApi,
  PlatformApiError,
  taskProgressApi,
  tasksApi,
} from '../../../api/platform-api';
import { TaskRuntimePage } from './TaskRuntimePage';
import { TaskRuntimeDetails } from './TaskRuntimeDetails';

const task: TaskSummary = {
  createdAt: '2026-09-08T00:00:00.000Z',
  currentAppliedRevisionId: null,
  deletedAt: null,
  desiredState: 'RUNNING',
  draft: null,
  id: '0199c617-249d-7459-bbcb-f97ad47a3101',
  name: 'orders-cdc',
  observabilityHealth: 'DEGRADED',
  observedState: 'RUNNING',
  recoveryDisposition: 'AUTO_ELIGIBLE',
  rowVersion: '3',
  sourceDataSourceId: '0199c617-249d-7459-bbcb-f97ad47a3102',
  targetDataSourceId: '0199c617-249d-7459-bbcb-f97ad47a3103',
  taskType: 'PG_KAFKA',
  updatedAt: '2026-09-08T00:00:00.000Z',
};
const runtime: RuntimeRead = {
  operation: null,
  retryable: false,
  run: {
    currentStage: 'SNAPSHOT',
    endedAt: null,
    failureCode: null,
    failureSummary: null,
    id: '0199c617-249d-7459-bbcb-f97ad47a3105',
    logRecoveryState: 'NONE',
    logStorageState: 'HEALTHY',
    startedAt: '2026-09-08T00:00:00.000Z',
    status: 'RUNNING',
  },
  stages: [
    {
      completed: '42',
      endedAt: null,
      failureCode: null,
      failureSummary: null,
      heartbeatAt: '2026-09-08T00:01:00.000Z',
      order: 1,
      retryable: false,
      startedAt: '2026-09-08T00:00:00.000Z',
      status: 'RUNNING',
      throughput: '3.5',
      total: null,
      type: 'SNAPSHOT',
    },
  ],
  taskId: task.id,
  units: [
    {
      endedAt: null,
      exitCode: null,
      exitSignal: null,
      failureCode: null,
      failureSummary: null,
      key: 'pg',
      startedAt: '2026-09-08T00:00:00.000Z',
      status: 'RUNNING',
    },
  ],
};
const logs: RuntimeLogRead = {
  facts: [
    {
      continuation: 0,
      continues: false,
      kind: 'LINE',
      occurredAt: '2026-09-08T00:00:01.000Z',
      payload: 'snapshot ready',
      stream: 'STDOUT',
      unitId: '0199c617-249d-7459-bbcb-f97ad47a3106',
      unitKey: 'pg',
    },
    {
      kind: 'GAP',
      lostBytes: null,
      occurredAt: '2026-09-08T00:00:02.000Z',
      reason: 'CAPACITY_ROLLOVER',
      stream: 'STDOUT',
      unitId: '0199c617-249d-7459-bbcb-f97ad47a3106',
      unitKey: 'pg',
    },
  ],
  limited: false,
  runId: runtime.run!.id,
  taskId: task.id,
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function logLine(
  payload: string,
  stream: 'STDOUT' | 'STDERR' = 'STDOUT',
): Extract<RuntimeLogRead['facts'][number], { kind: 'LINE' }> {
  return {
    continuation: 0,
    continues: false,
    kind: 'LINE',
    occurredAt: '2026-09-08T00:00:01.000Z',
    payload,
    stream,
    unitId: '0199c617-249d-7459-bbcb-f97ad47a3106',
    unitKey: 'pg',
  };
}

async function openRuntime(name = task.name) {
  fireEvent.click(await screen.findByRole('button', { name: `${name} 查看运行详情` }));
  fireEvent.click(await screen.findByRole('tab', { name: '运行日志' }));
}

function selectLogStream(value: 'STDOUT' | 'STDERR') {
  const filters = screen.getByRole('button', { name: /更多筛选/u });
  if (filters.getAttribute('aria-expanded') === 'false') fireEvent.click(filters);
  fireEvent.mouseDown(screen.getByLabelText('日志流'));
  fireEvent.click(screen.getByText(value, { selector: '.ant-select-item-option-content' }));
}

const historicalRun = {
  id: '0199c617-249d-7459-bbcb-f97ad47a3199',
  status: 'STOPPED' as const,
  startedAt: '2026-09-07T00:00:00.000Z',
  endedAt: '2026-09-07T01:00:00.000Z',
};

function selectLogRun(value: 'history' | 'current') {
  fireEvent.mouseDown(screen.getByLabelText('日志所属运行'));
  fireEvent.click(
    screen.getByText(
      value === 'history' ? /2026年09月07日 08:00:00.*0199c617/u : /当前运行.*最近/u,
      { selector: '.ant-select-item-option-content' },
    ),
  );
}

beforeEach(() => {
  vi.spyOn(tasksApi, 'runtimeRuns').mockResolvedValue({ runs: [historicalRun] });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('TaskRuntimePage', () => {
  it('keeps current log viewing available when the history list fails', async () => {
    vi.mocked(tasksApi.runtimeRuns).mockRejectedValue(
      new PlatformApiError('HISTORY_UNAVAILABLE', 503),
    );
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    expect(await screen.findByText(/历史运行列表读取失败/u)).toBeInTheDocument();
    expect(await screen.findByText(/snapshot ready/u)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: '实时日志' })).toBeChecked();
  });

  it('selects historical logs lazily, disables live tail, resets filters and downloads the selected run', async () => {
    vi.useFakeTimers();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockImplementation(async (_taskId, runId) => ({
      ...logs,
      runId,
      facts: [logLine(runId === historicalRun.id ? 'INFO historical output' : 'INFO live output')],
    }));
    const download = vi
      .spyOn(tasksApi, 'downloadLogs')
      .mockRejectedValue(new PlatformApiError('TEST_DOWNLOAD', 409));
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    expect(tasksApi.runtimeRuns).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await act(async () => {});
    expect(tasksApi.runtimeRuns).toHaveBeenCalledOnce();
    selectLogStream('STDERR');
    selectLogRun('history');
    await act(async () => {});
    expect(readLogs).toHaveBeenLastCalledWith(task.id, historicalRun.id, { order: 'DESC' });
    expect(screen.getByText(/INFO historical output/u)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: '实时日志' })).toBeDisabled();
    expect(screen.getByRole('checkbox', { name: '实时日志' })).not.toBeChecked();
    const reads = readLogs.mock.calls.length;
    await act(async () => vi.advanceTimersByTimeAsync(6_000));
    expect(readLogs).toHaveBeenCalledTimes(reads);
    fireEvent.click(screen.getByRole('button', { name: '下载日志' }));
    await act(async () => {});
    expect(download).toHaveBeenCalledWith(task.id, historicalRun.id, {});
    selectLogRun('current');
    await act(async () => {});
    expect(screen.getByRole('checkbox', { name: '实时日志' })).toBeChecked();
    expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, { order: 'DESC' });
    const resumedReads = readLogs.mock.calls.length;
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    expect(readLogs).toHaveBeenCalledTimes(resumedReads + 1);
  });

  it('ignores a stale current-log unauthorized result after selecting a historical run', async () => {
    const pending = deferred<RuntimeLogRead>();
    const invalidated = vi.fn();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockImplementation(async (_taskId, runId) =>
      runId === historicalRun.id
        ? { ...logs, runId, facts: [logLine('INFO previous run')] }
        : pending.promise,
    );
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await screen.findByLabelText('日志所属运行');
    await waitFor(() => expect(tasksApi.runtimeRuns).toHaveBeenCalled());
    selectLogRun('history');
    expect(await screen.findByText(/INFO previous run/u)).toBeInTheDocument();
    await act(async () => pending.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
    expect(screen.getByText(/INFO previous run/u)).toBeInTheDocument();
  });

  it('keeps the selected historical run when runtime refresh discovers a new current run', async () => {
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockImplementation(async (_taskId, runId) => ({
      ...logs,
      runId,
      facts: [logLine(runId === historicalRun.id ? 'INFO retained history' : 'INFO current run')],
    }));
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await screen.findByText(/INFO current run/u);
    await waitFor(() => expect(tasksApi.runtimeRuns).toHaveBeenCalled());
    selectLogRun('history');
    await screen.findByText(/INFO retained history/u);
    readRuntime.mockResolvedValue({
      ...runtime,
      run: { ...runtime.run!, id: '0199c617-249d-7459-bbcb-f97ad47a3188' },
    });
    fireEvent.click(screen.getByRole('button', { name: '刷新运行信息' }));
    await waitFor(() => expect(readRuntime).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByRole('button', { name: '刷新运行信息' })).toBeEnabled());
    expect(readLogs).toHaveBeenLastCalledWith(task.id, historicalRun.id, { order: 'DESC' });
    expect(screen.getByText(/INFO retained history/u)).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: '实时日志' })).toBeDisabled();
  });

  it('does not sign out after an obsolete log download fails during a run switch', async () => {
    const pending = deferred<Awaited<ReturnType<typeof tasksApi.downloadLogs>>>();
    const invalidated = vi.fn();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(tasksApi, 'downloadLogs').mockReturnValue(pending.promise);
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await screen.findByText(/snapshot ready/u);
    await waitFor(() => expect(tasksApi.runtimeRuns).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: '下载日志' }));
    selectLogRun('history');
    await act(async () => pending.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
  });

  it('loads logs lazily, follows live output, pauses scrolling and stops polling when hidden', async () => {
    vi.useFakeTimers();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const view = render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(readLogs).not.toHaveBeenCalled();
    expect(
      within(screen.getByRole('region', { name: '运行监控与日志' })).getByRole('button', {
        name: '停止整个任务',
      }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await act(async () => {});
    expect(screen.getByRole('checkbox', { name: '实时日志' })).toBeChecked();
    expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, { order: 'DESC' });
    const viewport = screen.getByLabelText('运行日志内容');
    Object.defineProperties(viewport, {
      scrollHeight: { value: 1000 },
      clientHeight: { value: 200 },
    });
    viewport.scrollTop = 100;
    fireEvent.scroll(viewport);
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    expect(readLogs).toHaveBeenCalledTimes(2);
    expect(viewport.scrollTop).toBe(100);
    viewport.scrollTop = 800;
    fireEvent.scroll(viewport);
    readLogs.mockResolvedValue({ ...logs, facts: [logLine('INFO new output')] });
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    expect(viewport.scrollTop).toBe(1000);
    expect(screen.getByText(/INFO new output/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('checkbox', { name: '实时日志' }));
    await act(async () => vi.advanceTimersByTimeAsync(10_000));
    expect(readLogs).toHaveBeenCalledTimes(3);
    fireEvent.click(screen.getByRole('checkbox', { name: '实时日志' }));
    await act(async () => {});
    expect(readLogs).toHaveBeenCalledTimes(4);
    fireEvent.click(screen.getByRole('tab', { name: '运行监控' }));
    await act(async () => vi.advanceTimersByTimeAsync(10_000));
    expect(readLogs).toHaveBeenCalledTimes(4);
    view.unmount();
    await act(async () => vi.advanceTimersByTimeAsync(10_000));
    expect(readLogs).toHaveBeenCalledTimes(4);
  });

  it('does not overlap live reads, retains output on failure and ignores errors after leaving the log tab', async () => {
    vi.useFakeTimers();
    const pending = deferred<RuntimeLogRead>();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const read = vi
      .spyOn(tasksApi, 'logs')
      .mockResolvedValueOnce(logs)
      .mockReturnValueOnce(pending.promise)
      .mockRejectedValueOnce(new PlatformApiError('LOG_READ_FAILED', 409));
    const invalidated = vi.fn();
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(8_000));
    expect(read).toHaveBeenCalledTimes(2);
    await act(async () => pending.resolve(logs));
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    expect(screen.getByText(/LOG_READ_FAILED/)).toBeInTheDocument();
    expect(screen.getByText(/snapshot ready/)).toBeInTheDocument();
    const late = deferred<RuntimeLogRead>();
    read.mockReturnValue(late.promise);
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    fireEvent.click(screen.getByRole('tab', { name: '运行监控' }));
    await act(async () => late.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
  });

  it('does not present a scan-limited empty result as proof that no logs match', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue({
      ...logs,
      facts: [],
      limited: true,
      scanLimited: true,
    });
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    expect(await screen.findByText(/已达到单次日志扫描上限/)).toBeInTheDocument();
    expect(screen.queryByText('当前筛选条件下暂无日志。')).not.toBeInTheDocument();
  });

  it('applies the selected log level to live reads and log downloads', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const read = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const download = vi
      .spyOn(tasksApi, 'downloadLogs')
      .mockRejectedValue(new Error('download-test'));
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await screen.findByText(/snapshot ready/);
    fireEvent.mouseDown(screen.getByLabelText('日志级别'));
    fireEvent.click(
      screen.getByText('ERROR · 错误', { selector: '.ant-select-item-option-content' }),
    );
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    await waitFor(() =>
      expect(read).toHaveBeenLastCalledWith(task.id, runtime.run!.id, {
        level: 'ERROR',
        order: 'DESC',
      }),
    );
    fireEvent.click(screen.getByRole('button', { name: '下载日志' }));
    await waitFor(() =>
      expect(download).toHaveBeenCalledWith(task.id, runtime.run!.id, { level: 'ERROR' }),
    );
  });

  it('labels MySQL applied mappings as tables and keeps refresh manual by default', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({ ...runtime, taskId: task.id });
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(new Error('unavailable'));
    const configured: TaskSummary = {
      ...task,
      taskType: 'MYSQL_KAFKA',
      applied: {
        artifactId: null,
        configuration: {
          schemaVersion: 2,
          kind: 'MYSQL_TO_KAFKA',
          source: { dataSourceId: task.sourceDataSourceId, includeSnapshot: false },
          target: { dataSourceId: task.targetDataSourceId },
          mappings: [
            { source: { schema: 'data_demo', name: 'orders' }, targets: [{ topic: 'orders.cdc' }] },
          ],
        },
        configurationSchemaVersion: 2,
        createdAt: task.createdAt,
        draftVersion: '1',
        id: '0199c617-249d-7459-bbcb-f97ad47a3121',
        revisionNumber: '1',
        state: 'APPLIED',
        updatedAt: task.updatedAt,
      },
    };
    render(<TaskRuntimeDetails task={configured} onSessionInvalidated={vi.fn()} />);
    expect(screen.getByRole('checkbox', { name: '自动刷新' })).not.toBeChecked();
    fireEvent.click(screen.getByRole('tab', { name: '任务配置' }));
    const mappings = within(screen.getByLabelText('任务映射明细'));
    expect(mappings.getByRole('columnheader', { name: '来源表' })).toBeInTheDocument();
    expect(mappings.queryByRole('columnheader', { name: '来源 Topic' })).not.toBeInTheDocument();
    expect(mappings.getByText('data_demo.orders')).toBeInTheDocument();
    expect(mappings.getByText('orders.cdc')).toBeInTheDocument();
  });
  it('keeps the applied log filter during automatic refresh and clears it when the run changes', async () => {
    vi.useFakeTimers();
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi
      .spyOn(tasksApi, 'logs')
      .mockImplementation(async (_taskId, _runId, filter) => ({
        ...logs,
        facts: [logLine(filter?.stream === 'STDERR' ? 'filtered stderr' : 'unfiltered stdout')],
      }));
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(new Error('unavailable'));
    const invalidated = vi.fn();
    const view = render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />
      </ConfigProvider>,
    );
    await act(async () => {});
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    selectLogStream('STDERR');
    fireEvent.change(screen.getByLabelText('日志运行单元'), { target: { value: 'pg' } });
    fireEvent.change(screen.getByLabelText('日志起始时间'), {
      target: { value: '2026-09-08T08:00' },
    });
    fireEvent.change(screen.getByLabelText('日志结束时间'), {
      target: { value: '2026-09-08T09:00' },
    });
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    await act(async () => {});
    expect(screen.getByText(/filtered stderr/)).toBeInTheDocument();
    selectLogStream('STDOUT');
    fireEvent.change(screen.getByLabelText('日志运行单元'), { target: { value: 'not-applied' } });
    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    const appliedFilter = {
      order: 'ASC' as const,
      from: '2026-09-08T00:00:00.000Z',
      stream: 'STDERR' as const,
      to: '2026-09-08T01:00:00.000Z',
      unitKey: 'pg',
    };
    expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, appliedFilter);
    expect(screen.getByText(/filtered stderr/)).toBeInTheDocument();
    expect(screen.getByLabelText('日志运行单元')).toHaveValue('not-applied');
    fireEvent.click(screen.getByRole('button', { name: '刷新运行信息' }));
    await act(async () => {});
    expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, appliedFilter);

    const newRunId = '0199c617-249d-7459-bbcb-f97ad47a3199';
    readRuntime.mockResolvedValue({ ...runtime, run: { ...runtime.run!, id: newRunId } });
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(readLogs).toHaveBeenLastCalledWith(task.id, newRunId, { order: 'DESC' });
    expect(screen.getByLabelText('日志运行单元')).toHaveValue('');
    expect(screen.getByLabelText('日志起始时间')).toHaveValue('');
    expect(screen.getByLabelText('日志结束时间')).toHaveValue('');
    expect(screen.getByText('全部输出流', { selector: '.ant-select-content' })).toBeInTheDocument();
    expect(screen.getByText(/unfiltered stdout/)).toBeInTheDocument();
    selectLogStream('STDERR');
    fireEvent.change(screen.getByLabelText('日志运行单元'), { target: { value: 'pg' } });
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    await act(async () => {});
    const otherTask = { ...task, id: '0199c617-249d-7459-bbcb-f97ad47a3198' };
    readRuntime.mockResolvedValue({ ...runtime, taskId: otherTask.id });
    view.rerender(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskRuntimeDetails task={otherTask} onSessionInvalidated={invalidated} />
      </ConfigProvider>,
    );
    await act(async () => {});
    expect(readLogs).toHaveBeenLastCalledWith(otherTask.id, runtime.run!.id, { order: 'DESC' });
    expect(screen.getByLabelText('日志运行单元')).toHaveValue('');
    expect(screen.getByText('全部输出流', { selector: '.ant-select-content' })).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: '自动刷新' })).not.toBeChecked();
  });

  it.each([
    [409, false],
    [401, false],
    [401, true],
  ] as const)(
    'waits for a failed %s refresh batch to settle before releasing it (closed: %s)',
    async (status, closeBeforeCompletion) => {
      vi.useFakeTimers();
      const pendingProgress = deferred<Awaited<ReturnType<typeof taskProgressApi.list>>>();
      const pendingPerformance = deferred<Awaited<ReturnType<typeof performanceApi.detail>>>();
      const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
      vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
      vi.spyOn(taskProgressApi, 'list')
        .mockResolvedValueOnce([])
        .mockReturnValue(pendingProgress.promise);
      vi.spyOn(performanceApi, 'detail')
        .mockRejectedValueOnce(new Error('unavailable'))
        .mockReturnValue(pendingPerformance.promise);
      const refreshTask = vi
        .fn()
        .mockRejectedValue(new PlatformApiError('TASK_SUMMARY_REFRESH_FAILED', status));
      const invalidated = vi.fn();
      const view = render(
        <ConfigProvider theme={{ token: { motion: false } }}>
          <TaskRuntimeDetails
            task={task}
            onSessionInvalidated={invalidated}
            onTaskChanged={refreshTask}
          />
        </ConfigProvider>,
      );
      await act(async () => {});
      fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
      await act(async () => vi.advanceTimersByTimeAsync(15_000));
      expect(screen.getByRole('button', { name: '刷新运行信息' })).toBeDisabled();
      await act(async () => vi.advanceTimersByTimeAsync(60_000));
      expect(readRuntime).toHaveBeenCalledTimes(2);
      expect(refreshTask).toHaveBeenCalledOnce();
      await act(async () => pendingProgress.resolve([]));
      await act(async () => vi.advanceTimersByTimeAsync(15_000));
      expect(readRuntime).toHaveBeenCalledTimes(2);
      expect(invalidated).not.toHaveBeenCalled();
      if (closeBeforeCompletion) view.unmount();
      await act(async () => pendingPerformance.reject(new Error('performance read failed')));
      expect(invalidated).toHaveBeenCalledTimes(!closeBeforeCompletion && status === 401 ? 1 : 0);
      if (!closeBeforeCompletion) {
        expect(screen.getByRole('button', { name: '刷新运行信息' })).toBeEnabled();
        expect(screen.getByText('TASK_SUMMARY_REFRESH_FAILED')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
      }
    },
  );

  it('pauses list polling while details are open and resumes it after closing', async () => {
    vi.useFakeTimers();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    const readSummaries = vi.spyOn(performanceApi, 'query').mockResolvedValue({ items: [] });
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(new Error('unavailable'));
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskRuntimePage onSessionInvalidated={vi.fn()} />
      </ConfigProvider>,
    );
    await act(async () => {});
    expect(readSummaries).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole('button', { name: `${task.name} 查看运行详情` }));
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(readRuntime).toHaveBeenCalledOnce();
    expect(readSummaries).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(readRuntime).toHaveBeenCalledOnce();
    expect(readSummaries.mock.calls.length).toBeGreaterThan(1);
  });

  it('refreshes all detail data manually by default and only polls at the selected interval while enabled', async () => {
    vi.useFakeTimers();
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const readProgress = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    const readPerformance = vi.spyOn(performanceApi, 'detail').mockResolvedValue({
      summary: {
        taskId: task.id,
        runId: null,
        status: 'NO_DATA',
        collectedAt: null,
        values: {
          recordsPerSecond: null,
          bytesPerSecond: null,
          sinkRecords: null,
          sinkBytes: null,
          queueRecords: null,
          queueBytes: null,
        },
        delayStatus: 'UNSUPPORTED',
      },
      window: '1h',
      points: [],
      maintenance: { status: 'HEALTHY', lastAttemptAt: null },
    });
    const refreshTask = vi.fn().mockResolvedValue(undefined);
    const view = render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskRuntimeDetails
          task={task}
          onSessionInvalidated={vi.fn()}
          onTaskChanged={refreshTask}
        />
      </ConfigProvider>,
    );
    await act(async () => {});
    const expectReads = (count: number) => {
      expect(readRuntime).toHaveBeenCalledTimes(count);
      expect(readLogs).not.toHaveBeenCalled();
      expect(readProgress).toHaveBeenCalledTimes(count);
      expect(readPerformance).toHaveBeenCalledTimes(count);
    };
    expect(screen.getByRole('checkbox', { name: '自动刷新' })).not.toBeChecked();
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expectReads(1);

    fireEvent.click(screen.getByRole('button', { name: '刷新运行信息' }));
    await act(async () => {});
    expectReads(2);
    expect(refreshTask).toHaveBeenCalledOnce();

    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(14_999));
    expectReads(2);
    await act(async () => vi.advanceTimersByTimeAsync(1));
    expectReads(3);

    fireEvent.mouseDown(screen.getByLabelText('自动刷新间隔'));
    fireEvent.click(screen.getByText('5 秒', { selector: '.ant-select-item-option-content' }));
    await act(async () => vi.advanceTimersByTimeAsync(4_999));
    expectReads(3);
    await act(async () => vi.advanceTimersByTimeAsync(1));
    expectReads(4);

    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expectReads(4);
    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    view.unmount();
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expectReads(4);
  });

  it('does not overlap refreshes and ignores a late unauthorized runtime response after closing details', async () => {
    vi.useFakeTimers();
    const pending = deferred<RuntimeRead>();
    const readRuntime = vi
      .spyOn(tasksApi, 'runtime')
      .mockResolvedValueOnce(runtime)
      .mockReturnValue(pending.promise);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(new Error('unavailable'));
    const invalidated = vi.fn();
    const view = render(<TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('checkbox', { name: '自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(readRuntime).toHaveBeenCalledTimes(2);
    fireEvent.click(screen.getByRole('button', { name: '刷新运行信息' }));
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(readRuntime).toHaveBeenCalledTimes(2);
    view.unmount();
    await act(async () => pending.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
  });

  it('displays every source and all target topics from an applied multi-mapping task', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const configured: TaskSummary = {
      ...task,
      applied: {
        artifactId: null,
        configuration: {
          schemaVersion: 2,
          kind: 'PG_TO_KAFKA',
          source: {},
          target: {},
          mappings: [
            {
              source: { schema: 'public', name: 'orders' },
              targets: [{ topic: 'orders-cdc' }, { topic: 'audit' }],
            },
            {
              source: { schema: 'public', name: 'customers' },
              targets: [{ topic: 'customers-cdc' }, { topic: 'audit' }],
            },
          ],
        },
        configurationSchemaVersion: 2,
        createdAt: task.createdAt,
        draftVersion: '1',
        id: '0199c617-249d-7459-bbcb-f97ad47a3121',
        revisionNumber: '1',
        state: 'APPLIED',
        updatedAt: task.updatedAt,
      },
    };
    render(<TaskRuntimeDetails task={configured} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(screen.getByRole('tab', { name: '任务配置' }));
    const mappings = within(screen.getByLabelText('任务映射明细'));
    expect(mappings.getByText('public.orders')).toBeInTheDocument();
    expect(mappings.getByText('public.customers')).toBeInTheDocument();
    expect(mappings.getByText('orders-cdc')).toBeInTheDocument();
    expect(mappings.getByText('customers-cdc')).toBeInTheDocument();
    expect(mappings.getAllByText('audit')).toHaveLength(2);
    expect(mappings.getByText('2 个来源 · 3 个目标 Topic')).toBeInTheDocument();
  });

  it('shows the applied configuration and artifact after start has cleared the draft', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const artifactId = '0199c617-249d-7459-bbcb-f97ad47a3120';
    const configuredTask = {
      ...task,
      currentAppliedRevisionId: '0199c617-249d-7459-bbcb-f97ad47a3121',
      applied: {
        artifactId,
        configuration: {
          source: { table: { schema: 'public', name: 'orders' } },
          target: { topic: 'orders-cdc' },
        },
        configurationSchemaVersion: 1,
        createdAt: task.createdAt,
        draftVersion: '1',
        id: '0199c617-249d-7459-bbcb-f97ad47a3121',
        revisionNumber: '1',
        state: 'APPLIED' as const,
        updatedAt: task.updatedAt,
      },
    };
    const view = render(
      <TaskRuntimeDetails
        task={configuredTask}
        artifacts={[{ id: artifactId, versionIdentifier: 'ape-dts-linux' }]}
        onSessionInvalidated={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole('tab', { name: '任务配置' }));
    expect(screen.getByText('当前已应用')).toBeInTheDocument();
    expect(screen.getByText('ape-dts-linux')).toBeInTheDocument();
    expect(screen.getByText('public.orders')).toBeInTheDocument();
    expect(screen.queryByText('未绑定制品')).not.toBeInTheDocument();
    expect(screen.queryByText('当前草稿')).not.toBeInTheDocument();
    const changedTask = {
      ...configuredTask,
      draft: {
        ...configuredTask.applied,
        id: '0199c617-249d-7459-bbcb-f97ad47a3122',
        state: 'READY_DRAFT' as const,
        configuration: {
          source: { table: { schema: 'public', name: 'orders_next' } },
          target: { topic: 'orders-next' },
        },
      },
    };
    view.rerender(
      <TaskRuntimeDetails
        task={changedTask}
        artifacts={[{ id: artifactId, versionIdentifier: 'ape-dts-linux' }]}
        onSessionInvalidated={vi.fn()}
      />,
    );
    expect(screen.getByText('public.orders')).toBeInTheDocument();
    expect(screen.queryByText('public.orders_next')).not.toBeInTheDocument();
    fireEvent.mouseDown(screen.getByLabelText('查看配置'));
    fireEvent.click(await screen.findByText('待应用修改'));
    expect(await screen.findByText('public.orders_next')).toBeInTheDocument();
  });

  it('defaults to performance and current run, and preserves log filters across tabs without reloading the run', async () => {
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    expect(screen.getByRole('tab', { name: '运行监控' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tab', { name: '本次运行' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getAllByText('运行监控', { exact: true })).toHaveLength(1);
    expect(screen.getByRole('button', { name: '运行监控说明' })).toBeInTheDocument();
    expect(screen.getAllByText('本次运行', { exact: true })).toHaveLength(1);
    expect(screen.queryByLabelText('日志运行单元')).not.toBeInTheDocument();
    expect(screen.queryByText('当前草稿')).not.toBeInTheDocument();
    await screen.findByText(`运行记录 ${runtime.run!.id}`);
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await screen.findByText(/snapshot ready/);
    expect(screen.getAllByText('运行日志', { exact: true })).toHaveLength(1);
    fireEvent.change(screen.getByLabelText('日志运行单元'), { target: { value: 'pg' } });
    fireEvent.click(screen.getByRole('tab', { name: '运行监控' }));
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    expect(screen.getByLabelText('日志运行单元')).toHaveValue('pg');
    fireEvent.click(screen.getByRole('tab', { name: '阶段进度' }));
    expect(await screen.findByText(/总量未知/)).toBeVisible();
    expect(screen.getAllByText('阶段进度', { exact: true })).toHaveLength(1);
    fireEvent.click(screen.getByRole('tab', { name: '任务配置' }));
    expect(screen.getAllByText('任务配置', { exact: true })).toHaveLength(1);
    expect(screen.getByText('配置未读取')).toBeVisible();
    expect(screen.getByText(/总量未知/)).not.toBeVisible();
    expect(readRuntime).toHaveBeenCalledTimes(1);
  });
  it('shows current source, sink and artifact names without substituting identifiers', async () => {
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const configuredTask: TaskSummary = {
      ...task,
      draft: {
        artifactId: '0199c617-249d-7459-bbcb-f97ad47a3120',
        configuration: {
          source: { table: { schema: 'public', name: 'orders' } },
          target: { topic: 'orders-cdc' },
        },
        configurationSchemaVersion: 1,
        createdAt: task.createdAt,
        draftVersion: '1',
        id: '0199c617-249d-7459-bbcb-f97ad47a3121',
        revisionNumber: '1',
        state: 'READY_DRAFT',
        updatedAt: task.updatedAt,
      },
    };
    render(
      <TaskRuntimeDetails
        task={configuredTask}
        sourceName="测试 PostgreSQL"
        targetName="测试 Kafka"
        artifacts={[{ id: configuredTask.draft!.artifactId!, versionIdentifier: 'ape-dts-linux' }]}
        onSessionInvalidated={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    expect(await screen.findByText(/snapshot ready/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: '任务配置' }));
    const summary = within(screen.getByLabelText('运行摘要'));
    expect(summary.getByText('测试 PostgreSQL')).toBeInTheDocument();
    expect(summary.getByText('public.orders')).toBeInTheDocument();
    expect(summary.getByText('测试 Kafka')).toBeInTheDocument();
    expect(summary.getByText('orders-cdc')).toBeInTheDocument();
    expect(summary.getByText('ape-dts-linux')).toBeInTheDocument();
    expect(summary.getByText('待应用配置')).toBeInTheDocument();
    expect(summary.queryByText(configuredTask.draft!.artifactId!)).not.toBeInTheDocument();
  });

  it('ignores an obsolete runtime response after the user switches tasks', async () => {
    const otherTask: TaskSummary = {
      ...task,
      id: '0199c617-249d-7459-bbcb-f97ad47a3111',
      name: 'inventory-cdc',
    };
    const otherRuntime: RuntimeRead = {
      ...runtime,
      run: { ...runtime.run!, id: '0199c617-249d-7459-bbcb-f97ad47a3112' },
      taskId: otherTask.id,
    };
    const otherLogs: RuntimeLogRead = {
      facts: [logLine('inventory snapshot ready')],
      limited: false,
      runId: otherRuntime.run!.id,
      taskId: otherTask.id,
    };
    const obsoleteRuntime = deferred<RuntimeRead>();
    vi.spyOn(tasksApi, 'runtime').mockImplementation((taskId) =>
      taskId === task.id ? obsoleteRuntime.promise : Promise.resolve(otherRuntime),
    );
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(otherLogs);

    const invalidated = vi.fn();
    const view = render(<TaskRuntimeDetails task={task} onSessionInvalidated={invalidated} />);
    view.rerender(<TaskRuntimeDetails task={otherTask} onSessionInvalidated={invalidated} />);
    fireEvent.click(await screen.findByRole('tab', { name: '运行日志' }));
    expect(await screen.findByText(/inventory snapshot ready/)).toBeInTheDocument();

    await act(async () => {
      obsoleteRuntime.resolve(runtime);
      await Promise.resolve();
    });

    expect(screen.getByText(`运行记录 ${otherRuntime.run!.id}`)).toBeInTheDocument();
    expect(screen.queryByText(`运行记录 ${runtime.run!.id}`)).not.toBeInTheDocument();
    expect(readLogs).not.toHaveBeenCalledWith(task.id, runtime.run!.id);
  });

  it('shows run progress without duplicate state tables or invented totals', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(screen.getByRole('tab', { name: '阶段进度' }));
    expect(await screen.findByText(/总量未知/)).toBeInTheDocument();
    expect(screen.queryByText(/%/)).not.toBeInTheDocument();
    const summary = within(screen.getByLabelText('运行摘要'));
    expect(summary.getAllByText('2026年09月08日 08:00:00')).toHaveLength(1);
    expect(summary.getByText('pg')).toBeInTheDocument();
    expect(summary.queryByText('用户期望')).not.toBeInTheDocument();
    expect(summary.queryByText('进程事实')).not.toBeInTheDocument();
    expect(summary.queryByText('恢复资格')).not.toBeInTheDocument();
    expect(summary.queryByText('任务版本')).not.toBeInTheDocument();
  });

  it('shows persisted progress and unit failures beside the logs', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      ...runtime,
      stages: [{ ...runtime.stages[0]!, total: '100' }],
      units: [
        {
          ...runtime.units[0]!,
          status: 'FAILED',
          failureCode: 'UNIT_EXITED',
          failureSummary: '进程退出',
        },
      ],
    });
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(screen.getByRole('tab', { name: '阶段进度' }));
    expect(await screen.findByText(/已处理 42.*100.*42.0%/)).toBeInTheDocument();
    expect(screen.getByText('UNIT_EXITED：进程退出')).toBeInTheDocument();
    expect(await screen.findByText(/snapshot ready/)).toBeInTheDocument();
  });

  it('shows a rejected start without inventing a run or reading logs', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      ...runtime,
      run: null,
      stages: [],
      units: [],
      operation: {
        id: '0199c617-249d-7459-bbcb-f97ad47a3112',
        taskId: task.id,
        runId: null,
        action: 'START',
        state: 'REJECTED',
        failureCode: 'RUNTIME_CGROUP_V2_REQUIRED',
        failureSummary: '运行环境未就绪',
        createdAt: task.createdAt,
        updatedAt: task.createdAt,
        completedAt: task.createdAt,
      },
    });
    const readLogs = vi.spyOn(tasksApi, 'logs');
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    expect(await screen.findByText('已拒绝')).toHaveAttribute('title', 'REJECTED');
    expect(screen.getByText('启动')).toHaveAttribute('title', 'START');
    expect(screen.getByRole('alert')).toHaveTextContent(
      '运行环境未就绪（RUNTIME_CGROUP_V2_REQUIRED）',
    );
    expect(
      screen.getByText(
        '此为旧部署模式的资源隔离失败记录；当前共享容器模式不要求 cgroup v2 委派，请重新预检并启动。',
      ),
    ).toBeInTheDocument();
    expect(screen.getByText('该任务尚无运行记录。')).toBeInTheDocument();
    expect(readLogs).not.toHaveBeenCalled();
  });

  it('keeps the latest log filter result when earlier filtering finishes later', async () => {
    const stderrLogs = deferred<RuntimeLogRead>();
    const stdoutLogs = deferred<RuntimeLogRead>();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockImplementation((_taskId, _runId, filter) => {
      if (filter?.stream === 'STDERR') return stderrLogs.promise;
      if (filter?.stream === 'STDOUT') return stdoutLogs.promise;
      return Promise.resolve(logs);
    });

    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    expect(await screen.findByText(/snapshot ready/)).toBeInTheDocument();
    selectLogStream('STDERR');
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    selectLogStream('STDOUT');
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));

    await act(async () => {
      stdoutLogs.resolve({
        ...logs,
        facts: [logLine('latest stdout result')],
      });
      await Promise.resolve();
    });
    expect(await screen.findByText(/latest stdout result/)).toBeInTheDocument();

    await act(async () => {
      stderrLogs.resolve({
        ...logs,
        facts: [logLine('obsolete stderr result', 'STDERR')],
      });
      await Promise.resolve();
    });
    expect(screen.getByText(/latest stdout result/)).toBeInTheDocument();
    expect(screen.queryByText(/obsolete stderr result/)).not.toBeInTheDocument();
  });

  it('does not surface an obsolete initial-log error after a newer filter succeeds', async () => {
    const initialLogs = deferred<RuntimeLogRead>();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockImplementation((_taskId, _runId, filter) =>
      filter?.stream === 'STDERR'
        ? Promise.resolve({ ...logs, facts: [logLine('new stderr result', 'STDERR')] })
        : initialLogs.promise,
    );

    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    await screen.findByText(`运行记录 ${runtime.run!.id}`);
    selectLogStream('STDERR');
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    expect(await screen.findByText(/new stderr result/)).toBeInTheDocument();

    await act(async () => {
      initialLogs.reject(new PlatformApiError('OBSOLETE_LOG_FAILURE', 409));
      await Promise.resolve();
    });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByText(/new stderr result/)).toBeInTheDocument();
  });

  it('uses controlled stop and exposes no ordinary force-kill action', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const stop = vi.spyOn(tasksApi, 'stop').mockResolvedValue({
      action: 'STOP',
      completedAt: null,
      createdAt: '2026-09-08T00:00:00.000Z',
      failureCode: null,
      failureSummary: null,
      id: '0199c617-249d-7459-bbcb-f97ad47a3104',
      runId: '0199c617-249d-7459-bbcb-f97ad47a3105',
      state: 'ACCEPTED',
      taskId: task.id,
      updatedAt: '2026-09-08T00:00:00.000Z',
    });
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(await screen.findByRole('button', { name: '停止整个任务' }));
    fireEvent.click(screen.getByRole('button', { name: '确认停止' }));
    await waitFor(() => expect(stop).toHaveBeenCalledWith(task));
    expect(screen.queryByRole('button', { name: /强杀/ })).not.toBeInTheDocument();
  });

  it('submits only one stop request while the current request is pending', async () => {
    const pendingStop = deferred<Awaited<ReturnType<typeof tasksApi.stop>>>();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const stop = vi.spyOn(tasksApi, 'stop').mockReturnValue(pendingStop.promise);

    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    const stopButton = await screen.findByRole('button', { name: '停止整个任务' });
    fireEvent.click(stopButton);
    const confirmButton = screen.getByRole('button', { name: '确认停止' });
    fireEvent.click(confirmButton);
    fireEvent.click(confirmButton);

    expect(confirmButton).toHaveClass('ant-btn-loading');
    expect(stop).toHaveBeenCalledTimes(1);

    await act(async () => {
      pendingStop.resolve({
        action: 'STOP',
        completedAt: null,
        createdAt: '2026-09-08T00:00:00.000Z',
        failureCode: null,
        failureSummary: null,
        id: '0199c617-249d-7459-bbcb-f97ad47a3113',
        runId: runtime.run!.id,
        state: 'ACCEPTED',
        taskId: task.id,
        updatedAt: '2026-09-08T00:00:00.000Z',
      });
      await Promise.resolve();
    });
  });

  it('shows merged line and GAP facts and sends only selected filters', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const downloadResult = {
      blob: new Blob(['download']),
      filename: `runtime-${runtime.run!.id}.log`,
    };
    let resolveDownload!: (download: typeof downloadResult) => void;
    const downloadPending = new Promise<typeof downloadResult>((resolve) => {
      resolveDownload = resolve;
    });
    const downloadLogs = vi.spyOn(tasksApi, 'downloadLogs').mockReturnValue(downloadPending);
    const revokeObjectURL = vi.fn();
    let connectedDuringClick = false;
    const clickAnchor = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      connectedDuringClick = this.isConnected;
    });
    vi.stubGlobal('URL', {
      createObjectURL: vi.fn(() => 'blob:runtime-log'),
      revokeObjectURL,
    });
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    expect(await screen.findByText(/snapshot ready/)).toBeInTheDocument();
    expect(screen.getByText(/GAP · CAPACITY_ROLLOVER/)).toBeInTheDocument();
    selectLogStream('STDERR');
    fireEvent.change(screen.getByLabelText('日志运行单元'), { target: { value: 'pg' } });
    fireEvent.change(screen.getByLabelText('日志起始时间'), {
      target: { value: '2026-09-08T08:00' },
    });
    fireEvent.change(screen.getByLabelText('日志结束时间'), {
      target: { value: '2026-09-08T09:00' },
    });
    fireEvent.click(screen.getByRole('button', { name: '筛选日志' }));
    await waitFor(() =>
      expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, {
        from: '2026-09-08T00:00:00.000Z',
        order: 'ASC',
        stream: 'STDERR',
        to: '2026-09-08T01:00:00.000Z',
        unitKey: 'pg',
      }),
    );
    fireEvent.click(screen.getByRole('button', { name: '下载日志' }));
    await waitFor(() =>
      expect(downloadLogs).toHaveBeenCalledWith(task.id, runtime.run!.id, {
        from: '2026-09-08T00:00:00.000Z',
        stream: 'STDERR',
        to: '2026-09-08T01:00:00.000Z',
        unitKey: 'pg',
      }),
    );
    await act(async () => {
      resolveDownload(downloadResult);
      await Promise.resolve();
    });
    const clickedAnchor = clickAnchor.mock.instances[0] as HTMLAnchorElement | undefined;
    expect(connectedDuringClick).toBe(true);
    expect(clickedAnchor?.download).toBe(downloadResult.filename);
    expect(clickedAnchor?.isConnected).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:runtime-log');
  });

  it('ignores an obsolete download failure after switching tasks', async () => {
    const otherTask: TaskSummary = {
      ...task,
      id: '0199c617-249d-7459-bbcb-f97ad47a3111',
      name: 'inventory-cdc',
    };
    const otherRuntime: RuntimeRead = {
      ...runtime,
      run: { ...runtime.run!, id: '0199c617-249d-7459-bbcb-f97ad47a3112' },
      taskId: otherTask.id,
    };
    const pendingDownload = deferred<Awaited<ReturnType<typeof tasksApi.downloadLogs>>>();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task, otherTask] });
    vi.spyOn(tasksApi, 'runtime').mockImplementation((id) =>
      Promise.resolve(id === task.id ? runtime : otherRuntime),
    );
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(tasksApi, 'downloadLogs').mockReturnValue(pendingDownload.promise);

    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(await screen.findByRole('button', { name: '下载日志' }));
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: /inventory-cdc/ }));
      pendingDownload.reject(new PlatformApiError('OBSOLETE_DOWNLOAD_FAILED', 409));
      await Promise.resolve();
    });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('does not create a download anchor when an obsolete download resolves during task switching', async () => {
    const otherTask: TaskSummary = {
      ...task,
      id: '0199c617-249d-7459-bbcb-f97ad47a3111',
      name: 'inventory-cdc',
    };
    const otherRuntime: RuntimeRead = {
      ...runtime,
      run: { ...runtime.run!, id: '0199c617-249d-7459-bbcb-f97ad47a3112' },
      taskId: otherTask.id,
    };
    const pendingDownload = deferred<Awaited<ReturnType<typeof tasksApi.downloadLogs>>>();
    const clickAnchor = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    const createObjectURL = vi.fn(() => 'blob:obsolete-runtime-log');
    vi.stubGlobal('URL', { createObjectURL, revokeObjectURL: vi.fn() });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task, otherTask] });
    vi.spyOn(tasksApi, 'runtime').mockImplementation((id) =>
      Promise.resolve(id === task.id ? runtime : otherRuntime),
    );
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(tasksApi, 'downloadLogs').mockReturnValue(pendingDownload.promise);

    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(await screen.findByRole('button', { name: '下载日志' }));
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: /inventory-cdc/ }));
      pendingDownload.resolve({ blob: new Blob(['obsolete']), filename: 'obsolete.log' });
      await Promise.resolve();
    });
    expect(createObjectURL).not.toHaveBeenCalled();
    expect(clickAnchor).not.toHaveBeenCalled();
  });

  it('only shows log storage retry when recovery is pending', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      ...runtime,
      run: {
        ...runtime.run!,
        logRecoveryState: 'MEMORY_PENDING',
        logStorageState: 'DEGRADED',
      },
    });
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    expect(await screen.findByRole('button', { name: '重试日志存储' })).toBeInTheDocument();

    cleanup();
    vi.restoreAllMocks();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      ...runtime,
      run: { ...runtime.run!, logRecoveryState: 'NONE', logStorageState: 'DEGRADED' },
    });
    vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    expect(await screen.findByText('无需恢复')).toHaveAttribute('title', 'NONE');
    expect(screen.queryByRole('button', { name: '重试日志存储' })).not.toBeInTheDocument();
  });

  it('disables log storage retry while running and refreshes runtime and current logs on success', async () => {
    const pendingRuntime: RuntimeRead = {
      ...runtime,
      run: {
        ...runtime.run!,
        logRecoveryState: 'DURABLE_PENDING',
        logStorageState: 'DEGRADED',
      },
    };
    let resolveRetry!: () => void;
    const retryPending = new Promise<void>((resolve) => {
      resolveRetry = resolve;
    });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    const readRuntime = vi
      .spyOn(tasksApi, 'runtime')
      .mockResolvedValueOnce(pendingRuntime)
      .mockResolvedValueOnce(runtime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    const retry = vi.spyOn(tasksApi, 'retryLogStorage').mockImplementation(async () => {
      await retryPending;
      return {
        durableRecovery: {
          availableSegments: 1,
          capacityDeletedSegments: 0,
          completedDeletions: 0,
          failures: 0,
          integrityFailedSegments: 0,
          memoryPendingGaps: 0,
          missingSegments: 0,
        },
        recoveryState: 'NONE',
        runId: runtime.run!.id,
        storageState: 'HEALTHY',
        taskId: task.id,
      };
    });
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    const summary = within(screen.getByLabelText('运行摘要'));
    fireEvent.click(await summary.findByRole('button', { name: '重试日志存储' }));
    fireEvent.click(summary.getByRole('button', { name: '正在重试日志存储…' }));
    expect(summary.getByRole('button', { name: '正在重试日志存储…' })).toBeDisabled();
    await act(async () => {
      resolveRetry();
      await retryPending;
    });
    await waitFor(() => expect(retry).toHaveBeenCalledWith(task.id, runtime.run!.id));
    expect(retry).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(readRuntime).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(readLogs).toHaveBeenCalledTimes(2));
    expect(readLogs).toHaveBeenLastCalledWith(task.id, runtime.run!.id, { order: 'DESC' });
  });

  it('continues live reads after a failed storage retry while a read is still pending', async () => {
    vi.useFakeTimers();
    const pending = deferred<RuntimeLogRead>();
    vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      ...runtime,
      run: { ...runtime.run!, logRecoveryState: 'MIXED_PENDING' },
    });
    const readLogs = vi
      .spyOn(tasksApi, 'logs')
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValue(logs);
    vi.spyOn(tasksApi, 'retryLogStorage').mockRejectedValue(
      new PlatformApiError('LOG_STORAGE_RETRY_FAILED', 409),
    );
    render(<TaskRuntimeDetails task={task} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('tab', { name: '运行日志' }));
    await act(async () => {});
    fireEvent.click(screen.getByRole('button', { name: '重试日志存储' }));
    await act(async () => {});
    await act(async () => pending.resolve(logs));
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    expect(readLogs).toHaveBeenCalledTimes(2);
    expect(screen.getByText(/snapshot ready/)).toBeInTheDocument();
  });

  it('shows the real error code when log storage retry fails without refreshing facts', async () => {
    const pendingRuntime: RuntimeRead = {
      ...runtime,
      run: {
        ...runtime.run!,
        logRecoveryState: 'MIXED_PENDING',
        logStorageState: 'DEGRADED',
      },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task] });
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue(pendingRuntime);
    const readLogs = vi.spyOn(tasksApi, 'logs').mockResolvedValue(logs);
    vi.spyOn(tasksApi, 'retryLogStorage').mockRejectedValue(
      new PlatformApiError('LOG_STORAGE_RETRY_FAILED', 409),
    );
    render(<TaskRuntimePage onSessionInvalidated={vi.fn()} />);
    await openRuntime();
    fireEvent.click(await screen.findByRole('button', { name: '重试日志存储' }));
    const failure = await screen.findByRole('alert');
    expect(failure).toHaveTextContent('操作失败');
    expect(failure).toHaveTextContent('LOG_STORAGE_RETRY_FAILED');
    expect(failure.closest('.ant-notification-notice')).toHaveClass('operation-notification-error');
    expect(readRuntime).toHaveBeenCalledTimes(1);
    expect(readLogs).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: '重试日志存储' })).toBeEnabled();
  });
});
