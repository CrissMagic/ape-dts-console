import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  AuditEvent,
  AuthenticatedSessionResponse,
  RuntimePerformanceSummary,
  TaskProgress,
  TaskSummary,
} from '@scs/contracts';
import {
  auditApi,
  performanceApi,
  PlatformApiError,
  taskProgressApi,
  tasksApi,
} from '../../api/platform-api';
import { HomePage } from './HomePage';

const session: AuthenticatedSessionResponse = {
  status: 'AUTHENTICATED',
  mustChangePassword: false,
  csrfToken: 'test',
  absoluteExpiresAt: '2026-10-23T12:00:00Z',
  idleExpiresAt: '2026-10-23T12:00:00Z',
  user: { id: 'user-1', username: 'admin', displayName: '管理员', role: 'ADMIN', status: 'ACTIVE' },
};
function task(
  id: string,
  observedState: TaskSummary['observedState'] = 'RUNNING',
  taskType: TaskSummary['taskType'] = 'PG_KAFKA',
): TaskSummary {
  return {
    id,
    name: `同步-${id}`,
    taskType,
    observedState,
    desiredState: observedState === 'RUNNING' ? 'RUNNING' : 'STOPPED',
    draft: null,
    currentAppliedRevisionId: `revision-${id}`,
    observabilityHealth: 'HEALTHY',
    recoveryDisposition: 'NOT_APPLICABLE',
    rowVersion: '1',
    sourceDataSourceId: 'source',
    targetDataSourceId: 'target',
    createdAt: '2026-10-23T00:00:00Z',
    updatedAt: '2026-10-23T00:00:00Z',
    deletedAt: null,
  };
}
const progress: TaskProgress = {
  taskId: 'pg',
  revisionId: 'revision-pg',
  collectedAt: '2026-10-23T01:00:00Z',
  sourceType: 'POSTGRES',
  status: 'AVAILABLE',
  replicationLagMilliseconds: 13000,
  slotStatus: 'ACTIVE',
  unconfirmedWalBytes: null,
  retainedWalBytes: null,
  offsetLag: null,
  timeDelayMilliseconds: null,
  timeDelayStatus: 'UNSUPPORTED',
};
const performance: RuntimePerformanceSummary = {
  taskId: 'pg',
  runId: 'run-1',
  collectedAt: '2026-10-23T01:00:00Z',
  status: 'FRESH',
  delayStatus: 'UNSUPPORTED',
  values: {
    recordsPerSecond: 1200,
    bytesPerSecond: null,
    sinkRecords: null,
    sinkBytes: null,
    queueRecords: null,
    queueBytes: null,
  },
};
const event: AuditEvent = {
  eventId: 'event-1',
  sequence: '1',
  action: 'TASK_RUNTIME_STARTED',
  actorType: 'USER',
  actorUserId: 'user-1',
  subjectType: 'TASK',
  subjectId: 'pg',
  occurredAt: '2026-10-23T01:18:30Z',
  details: {},
};
function setup(role: 'ADMIN' | 'DEVELOPER' = 'ADMIN') {
  const props = {
    session: { ...session, user: { ...session.user, role } },
    onSessionInvalidated: vi.fn(),
    onOpenTask: vi.fn(),
    onViewAudit: vi.fn(),
  };
  const view = render(<HomePage {...props} />);
  return { ...props, ...view };
}
beforeEach(() => {
  vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task('pg')] });
  vi.spyOn(taskProgressApi, 'list').mockResolvedValue([progress]);
  vi.spyOn(performanceApi, 'query').mockResolvedValue({ items: [performance] });
  vi.spyOn(auditApi, 'list').mockResolvedValue({ events: [event], nextCursor: null });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('HomePage', () => {
  it('shows scoped task counts and labels source-specific metrics without inventing values', async () => {
    vi.mocked(tasksApi.list).mockResolvedValue({
      tasks: [
        task('pg'),
        task('kafka', 'RUNNING', 'KAFKA_KAFKA'),
        task('mysql', 'RUNNING', 'MYSQL_KAFKA'),
        task('stopped', 'STOPPED'),
        task('failed', 'FAILED'),
        task('blocked', 'BLOCKED'),
        task('starting', 'STARTING'),
        task('stopping', 'STOPPING'),
        { ...task('deleted', 'DELETED'), deletedAt: '2026-10-23T00:00:00Z' },
      ],
    });
    vi.mocked(taskProgressApi.list).mockResolvedValue([
      progress,
      {
        ...progress,
        taskId: 'kafka',
        revisionId: 'revision-kafka',
        sourceType: 'KAFKA',
        offsetLag: '9007199254740993',
      },
      {
        ...progress,
        taskId: 'mysql',
        revisionId: 'revision-mysql',
        sourceType: 'MYSQL',
        mysqlBinlogFilename: 'mysql-bin.000001',
        mysqlBinlogPosition: '123',
      },
    ]);
    setup();
    await screen.findByText('1,200 条/秒');
    expect(screen.getByLabelText('任务总数数量')).toHaveTextContent('8');
    expect(screen.getByLabelText('运行中数量')).toHaveTextContent('3');
    expect(screen.getByLabelText('已停止数量')).toHaveTextContent('1');
    expect(screen.getByLabelText('需处理数量')).toHaveTextContent('2');
    expect(screen.getByText('13 秒')).toBeInTheDocument();
    expect(screen.getByText('9,007,199,254,740,993')).toBeInTheDocument();
    expect(screen.getByText('mysql-bin.000001:123')).toBeInTheDocument();
    expect(screen.queryByText('同步-deleted')).not.toBeInTheDocument();
    expect(screen.queryByText(/采集于|已过期/u)).not.toBeInTheDocument();
    expect(performanceApi.query).toHaveBeenCalledWith(['pg', 'kafka', 'mysql']);
  });

  it('opens task details and audit without performing mutations and distinguishes audit actors', async () => {
    vi.mocked(auditApi.list).mockResolvedValue({
      events: [
        event,
        {
          ...event,
          eventId: 'event-2',
          actorType: 'SYSTEM',
          actorUserId: null,
          action: 'TASK_RUNTIME_RECOVERED',
          occurredAt: '2026-10-23T01:17:30Z',
        },
      ],
      nextCursor: null,
    });
    const props = setup();
    await screen.findByText('1,200 条/秒');
    expect(auditApi.list).toHaveBeenCalledWith({ limit: 5 });
    expect(screen.getByText('用户')).toBeInTheDocument();
    expect(screen.getByText('系统')).toBeInTheDocument();
    const activity = within(screen.getByRole('region', { name: '最近操作' }));
    const times = activity.getAllByText(/2026年10月23日/u);
    expect(times.map((element) => element.textContent)).toEqual([
      '2026年10月23日 09:18:30',
      '2026年10月23日 09:17:30',
    ]);
    fireEvent.click(screen.getByRole('button', { name: '查看 同步-pg 详情' }));
    expect(props.onOpenTask).toHaveBeenCalledWith('pg');
    expect(screen.queryByRole('button', { name: '查看任务' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '查看全部' }));
    expect(props.onViewAudit).toHaveBeenCalledOnce();
  });

  it('does not request administrator audit data for developers', async () => {
    setup('DEVELOPER');
    await screen.findByText('1,200 条/秒');
    expect(auditApi.list).not.toHaveBeenCalled();
    expect(screen.getByText('仅管理员可查看审计操作')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '查看全部' })).not.toBeInTheDocument();
  });

  it('hides stopped and mismatched-revision metrics, preserves unknowns, and displays a real zero', async () => {
    vi.mocked(tasksApi.list).mockResolvedValue({
      tasks: [task('pg'), task('stopped', 'STOPPED'), task('wrong')],
    });
    vi.mocked(taskProgressApi.list).mockResolvedValue([
      { ...progress, replicationLagMilliseconds: null },
      {
        ...progress,
        taskId: 'stopped',
        revisionId: 'revision-stopped',
        replicationLagMilliseconds: 99000,
      },
      {
        ...progress,
        taskId: 'wrong',
        revisionId: 'old-revision',
        replicationLagMilliseconds: 88000,
      },
    ]);
    vi.mocked(performanceApi.query).mockResolvedValue({
      items: [
        { ...performance, values: { ...performance.values, recordsPerSecond: 0 } },
        {
          ...performance,
          taskId: 'stopped',
          values: { ...performance.values, recordsPerSecond: 999 },
        },
      ],
    });
    setup();
    await screen.findByText('0 条/秒');
    expect(screen.getByText('暂无时间统计')).toBeInTheDocument();
    expect(screen.queryByText('99 秒')).not.toBeInTheDocument();
    expect(screen.queryByText('88 秒')).not.toBeInTheDocument();
    expect(screen.queryByText('999 条/秒')).not.toBeInTheDocument();
  });

  it('falls back to slot WAL bytes when PostgreSQL supplies no time lag, including a real zero', async () => {
    vi.mocked(tasksApi.list).mockResolvedValue({ tasks: [task('pg'), task('zero')] });
    vi.mocked(taskProgressApi.list).mockResolvedValue([
      { ...progress, replicationLagMilliseconds: null, unconfirmedWalBytes: '106576296' },
      {
        ...progress,
        taskId: 'zero',
        revisionId: 'revision-zero',
        replicationLagMilliseconds: null,
        unconfirmedWalBytes: '0',
      },
    ]);
    setup();
    await screen.findByText('101.63 MiB');
    expect(screen.getByText('0 B')).toBeInTheDocument();
    expect(screen.getAllByText('未确认 WAL')).toHaveLength(2);
    expect(screen.queryByText('0 秒')).not.toBeInTheDocument();
    expect(screen.queryByText('暂无时间统计')).not.toBeInTheDocument();
  });

  it('leaves counts unknown after an initial read failure instead of showing zero', async () => {
    vi.mocked(tasksApi.list).mockRejectedValue(new Error('network'));
    setup();
    await screen.findByText('任务概况读取失败，请刷新重试。');
    expect(screen.getByLabelText('任务总数数量')).toHaveTextContent('—');
    expect(performanceApi.query).not.toHaveBeenCalled();
  });

  it('retains the last successful values when refreshing fails', async () => {
    setup();
    await screen.findByText('1,200 条/秒');
    await waitFor(() => expect(screen.getByRole('button', { name: '刷新首页' })).toBeEnabled());
    vi.mocked(tasksApi.list).mockRejectedValue(new Error('network'));
    vi.mocked(auditApi.list).mockRejectedValue(new Error('network'));
    vi.mocked(taskProgressApi.list).mockRejectedValue(new Error('network'));
    vi.mocked(performanceApi.query).mockRejectedValue(new Error('network'));
    fireEvent.click(screen.getByRole('button', { name: '刷新首页' }));
    fireEvent.click(screen.getByRole('button', { name: '刷新首页' }));
    await screen.findByText('任务概况刷新失败，当前保留上次读取结果。');
    expect(screen.getByText('1,200 条/秒')).toBeInTheDocument();
    expect(screen.getByText('13 秒')).toBeInTheDocument();
    expect(tasksApi.list).toHaveBeenCalledTimes(2);
    expect(await screen.findByText('操作记录刷新失败，当前显示上次读取结果。')).toBeInTheDocument();
  });

  it('defaults to manual refresh, supports timed refresh, and stops polling after leaving', async () => {
    vi.useFakeTimers();
    const view = setup();
    await act(async () => {});
    expect(screen.getByRole('checkbox', { name: '首页自动刷新' })).not.toBeChecked();
    await act(async () => vi.advanceTimersByTimeAsync(60000));
    expect(tasksApi.list).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('checkbox', { name: '首页自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(15000));
    expect(tasksApi.list).toHaveBeenCalledTimes(2);
    view.unmount();
    await act(async () => vi.advanceTimersByTimeAsync(60000));
    expect(tasksApi.list).toHaveBeenCalledTimes(2);
  });

  it('queries performance only for the visible page and ignores a late response from the previous page', async () => {
    vi.mocked(tasksApi.list).mockResolvedValue({
      tasks: Array.from({ length: 11 }, (_, index) => task(`t-${index}`)),
    });
    let resolveOld!: (result: { items: RuntimePerformanceSummary[] }) => void;
    vi.mocked(performanceApi.query)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveOld = resolve;
          }),
      )
      .mockResolvedValue({
        items: [
          {
            ...performance,
            taskId: 't-10',
            values: { ...performance.values, recordsPerSecond: 88 },
          },
        ],
      });
    setup();
    await screen.findByRole('button', { name: '查看 同步-t-0 详情' });
    fireEvent.click(screen.getByTitle('Next Page'));
    await screen.findByText('88 条/秒');
    expect(performanceApi.query).toHaveBeenLastCalledWith(['t-10']);
    await act(async () => {
      resolveOld({
        items: [
          {
            ...performance,
            taskId: 't-10',
            values: { ...performance.values, recordsPerSecond: 777 },
          },
        ],
      });
    });
    expect(screen.queryByText('777 条/秒')).not.toBeInTheDocument();
    expect(screen.getByText('88 条/秒')).toBeInTheDocument();
    expect(
      within(screen.getByRole('region', { name: '任务运行状态' })).queryByText('同步-t-0'),
    ).not.toBeInTheDocument();
  });

  it('invalidates the login session on an authenticated request failure', async () => {
    vi.mocked(tasksApi.list).mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    const props = setup();
    await waitFor(() => expect(props.onSessionInvalidated).toHaveBeenCalledOnce());
  });
});
