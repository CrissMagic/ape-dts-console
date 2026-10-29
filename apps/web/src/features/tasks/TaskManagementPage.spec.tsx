import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import type {
  AuthenticatedSessionResponse,
  DataSourceSummary,
  TaskPreflightRunSummary,
  TaskSummary,
} from '@scs/contracts';
import { ConfigProvider } from 'antd';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as platformApi from '../../api/platform-api';

import {
  dataSourcesApi,
  PlatformApiError,
  tasksApi,
  recoveryApi,
  performanceApi,
  taskProgressApi,
  taskGroupsApi,
} from '../../api/platform-api';
import { TaskManagementPage } from './TaskManagementPage';

const session: AuthenticatedSessionResponse = {
  mustChangePassword: false,
  absoluteExpiresAt: '2026-09-18T12:00:00.000Z',
  csrfToken: 'scs-csrf-v1_test',
  idleExpiresAt: '2026-09-18T10:00:00.000Z',
  status: 'AUTHENTICATED',
  user: {
    displayName: 'Admin',
    id: '11111111-1111-4111-8111-111111111111',
    role: 'ADMIN',
    status: 'ACTIVE',
    username: 'admin',
  },
};

const kafkaSource: DataSourceSummary = {
  configuration: {
    bootstrapServers: ['kafka-source.internal:9092'],
    plaintextRiskAcknowledged: true,
    type: 'KAFKA',
  },
  createdAt: '2026-09-18T08:00:00.000Z',
  deletedAt: null,
  id: '22222222-2222-4222-8222-222222222222',
  name: '源 Kafka',
  rowVersion: '1',
  state: 'ACTIVE',
  updatedAt: '2026-09-18T08:00:00.000Z',
};

const kafkaTarget: DataSourceSummary = {
  ...kafkaSource,
  configuration: {
    bootstrapServers: ['kafka-target.internal:9092'],
    plaintextRiskAcknowledged: true,
    type: 'KAFKA',
  },
  id: '33333333-3333-4333-8333-333333333333',
  name: '目标 Kafka',
};

const mysqlSource: DataSourceSummary = {
  ...kafkaSource,
  id: '77777777-7777-4777-8777-777777777777',
  name: '测试 MySQL',
  configuration: {
    type: 'MYSQL',
    host: 'mysql',
    port: 3306,
    database: 'data_demo',
    username: 'reader',
    tls: { mode: 'REQUIRE' },
  },
};

function task(state: NonNullable<TaskSummary['draft']>['state'] = 'READY_DRAFT'): TaskSummary {
  return {
    createdAt: '2026-09-18T08:00:00.000Z',
    currentAppliedRevisionId: null,
    deletedAt: null,
    desiredState: 'STOPPED',
    draft: {
      artifactId: null,
      configuration: {
        kind: 'KAFKA_RAW_FORWARD',
        schemaVersion: 1,
        source: {
          dataSourceId: kafkaSource.id,
          groupId: 'ape-dts-task-44444444-4444-4444-8444-444444444444',
          topic: 'orders.source',
        },
        target: { dataSourceId: kafkaTarget.id, topic: 'orders.target' },
        tuning: {
          ackIntervalSeconds: 5,
          batchSize: 100,
          bufferSize: 16_000,
          checkpointIntervalSeconds: 15,
          requiredAcks: 'all',
        },
      },
      configurationSchemaVersion: 1,
      createdAt: '2026-09-18T08:00:00.000Z',
      draftVersion: '1',
      id: '55555555-5555-4555-8555-555555555555',
      revisionNumber: '1',
      state,
      updatedAt: '2026-09-18T08:00:00.000Z',
    },
    id: '44444444-4444-4444-8444-444444444444',
    name: '订单同步',
    observabilityHealth: 'UNAVAILABLE',
    observedState: 'STOPPED',
    recoveryDisposition: 'NOT_APPLICABLE',
    rowVersion: '2',
    sourceDataSourceId: kafkaSource.id,
    targetDataSourceId: kafkaTarget.id,
    taskType: 'KAFKA_KAFKA',
    updatedAt: '2026-09-18T08:00:00.000Z',
  };
}

function mysqlTask(
  state: NonNullable<TaskSummary['draft']>['state'] = 'READY_DRAFT',
  includeSnapshot = true,
): TaskSummary {
  const base = task(state);
  return {
    ...base,
    taskType: 'MYSQL_KAFKA',
    sourceDataSourceId: mysqlSource.id,
    draft: {
      ...base.draft!,
      configuration: {
        kind: 'MYSQL_TO_KAFKA',
        schemaVersion: 2,
        source: { dataSourceId: mysqlSource.id, includeSnapshot },
        target: { dataSourceId: kafkaTarget.id },
        mappings: [
          { source: { schema: 'data_demo', name: 'orders' }, targets: [{ topic: 'orders.cdc' }] },
        ],
        tuning: { requiredAcks: 'all' },
      },
    },
  };
}

function mysqlPreflight(existing: TaskSummary, passed = true): TaskPreflightRunSummary {
  return {
    id: '88888888-8888-4888-8888-888888888888',
    taskId: existing.id,
    revisionId: existing.draft!.id,
    startedAt: '2026-10-16T08:00:00.000Z',
    completedAt: '2026-10-16T08:00:01.000Z',
    state: passed ? 'PASSED' : 'FAILED',
    failureCode: null,
    failureSummary: null,
    result: {
      capacity: { available: 1, limit: 2, requested: 1, used: 1 },
      checks: [
        {
          key: 'CONFIGURATION',
          code: 'CONFIGURATION_VALID',
          details: { reinitializeMysqlSnapshot: true },
          state: 'PASSED',
          summary: '任务配置有效',
        },
        {
          key: 'MYSQL_CDC',
          code: passed ? 'MYSQL_CDC_READY' : 'MYSQL_BINLOG_UNAVAILABLE',
          details: {},
          state: passed ? 'PASSED' : 'FAILED',
          summary: passed ? 'Binlog检查通过' : 'Binlog不可用',
        },
      ],
      dataSourceVersions: { source: '1', target: '1' },
      identities: {
        source: 'uuid:data_demo',
        target: 'cluster',
        sourceTopic: null,
        targetTopic: null,
      },
      taskRevision: {
        draftVersion: existing.draft!.draftVersion,
        taskRowVersion: existing.rowVersion,
      },
    },
  };
}

const startOperation = {
  action: 'START' as const,
  completedAt: null,
  createdAt: '2026-10-16T08:00:01.000Z',
  failureCode: null,
  failureSummary: null,
  id: '88888888-8888-4888-8888-888888888888',
  runId: '99999999-9999-4999-8999-999999999999',
  state: 'EXECUTING' as const,
  taskId: '44444444-4444-4444-8444-444444444444',
  updatedAt: '2026-10-16T08:00:01.000Z',
};

function metadata(dataSource: DataSourceSummary, resource: 'TOPICS') {
  return {
    attemptedAt: '2026-09-18T08:00:00.000Z',
    dataSourceId: dataSource.id,
    items: [
      {
        attributes: { partitionCount: '2' },
        identity: `identity-${dataSource.id}`,
        name: dataSource === kafkaSource ? 'orders.source' : 'orders.target',
      },
    ],
    refreshedAt: '2026-09-18T08:00:00.000Z',
    resource,
    state: 'SUCCESS' as const,
  };
}

function replaceTargetTopic(topic: string) {
  const input = screen.getByLabelText('目标 Topic');
  fireEvent.keyDown(input, { key: 'Backspace', code: 'Backspace', keyCode: 8 });
  fireEvent.keyUp(input, { key: 'Backspace', code: 'Backspace', keyCode: 8 });
  fireEvent.change(input, { target: { value: topic } });
  fireEvent.keyDown(input, { key: 'Enter', code: 'Enter', keyCode: 13 });
  fireEvent.keyUp(input, { key: 'Enter', code: 'Enter', keyCode: 13 });
}

function addTargetTopic(topic: string, label = '目标 Topic') {
  const input = screen.getByLabelText(label);
  fireEvent.change(input, { target: { value: topic } });
  fireEvent.keyDown(input, { key: 'Enter', code: 'Enter', keyCode: 13 });
  fireEvent.keyUp(input, { key: 'Enter', code: 'Enter', keyCode: 13 });
}

const artifact = {
  id: 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb',
  versionIdentifier: 'ape-dts-test',
  declaredSource: 'local build',
  objectKey: 'artifacts/test',
  sha256: 'a'.repeat(64),
  sizeBytes: '1024',
  state: 'ACTIVE' as const,
  stateReason: null,
  verified: true,
  certifications: [],
};

beforeEach(() => {
  vi.spyOn(platformApi, 'listArtifacts').mockResolvedValue({ artifacts: [artifact] });
  vi.spyOn(taskGroupsApi, 'list').mockResolvedValue({ groups: [] });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

function taskMenuAction(action: string, name = '订单同步') {
  const existing = screen.queryByRole('menuitem', { name: `${name} ${action}` });
  if (existing) return existing;
  fireEvent.pointerDown(screen.getByRole('button', { name: `${name} 更多操作` }), {
    button: 0,
    ctrlKey: false,
  });
  return screen.getByRole('menuitem', { name: `${name} ${action}` });
}
async function findTaskMenuAction(action: string) {
  await screen.findByRole('button', { name: '订单同步 更多操作' });
  return taskMenuAction(action);
}
function closeTaskMenu() {
  const menu = screen.queryByRole('menu');
  if (menu) fireEvent.keyDown(menu, { key: 'Escape' });
}

function openTaskEditor(details: HTMLElement) {
  fireEvent.click(details);
  fireEvent.click(screen.getByRole('button', { name: '编辑任务' }));
}

describe('TaskManagementPage', () => {
  it('paginates cards locally and resets the page when filtering or changing page size', async () => {
    const items = Array.from({ length: 25 }, (_, index) => ({
      ...task(),
      id: `88888888-8888-4888-8888-${String(index + 1).padStart(12, '0')}`,
      name: `同步任务 ${index + 1}`,
    }));
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: items });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const cards = within(await screen.findByRole('list', { name: '任务列表' }));
    expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(10);
    expect(cards.queryByText('同步任务 11')).not.toBeInTheDocument();
    const viewport = screen.getByRole('region', { name: '任务卡片' });
    viewport.scrollTop = 400;
    fireEvent.click(screen.getByTitle('Next Page'));
    expect(viewport.scrollTop).toBe(0);
    expect(cards.getByText('同步任务 11')).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务每页条数' }));
    fireEvent.click(screen.getByText('20 / page', { selector: '.ant-select-item-option-content' }));
    expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(20);
    expect(cards.getByText('同步任务 1')).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务每页条数' }));
    fireEvent.click(screen.getByText('50 / page', { selector: '.ant-select-item-option-content' }));
    expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(25);
    fireEvent.change(screen.getByLabelText('筛选任务'), { target: { value: '同步任务 25' } });
    expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(1);
    expect(cards.getByText('同步任务 25')).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(1);
  });

  it('returns to an available page after deleting the only card on the last page', async () => {
    const items = Array.from({ length: 11 }, (_, index) => ({
      ...task(),
      id: `88888888-8888-4888-8888-${String(index + 1).padStart(12, '0')}`,
      name: `同步任务 ${index + 1}`,
    }));
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: items });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const remove = vi.spyOn(tasksApi, 'remove').mockResolvedValue({
      ...items[10]!,
      desiredState: 'DELETED',
      observedState: 'DELETED',
      deletedAt: '2026-10-28T02:00:00Z',
    });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const cards = within(await screen.findByRole('list', { name: '任务列表' }));
    fireEvent.click(screen.getByTitle('Next Page'));
    expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(1);
    list.mockResolvedValue({ tasks: items.slice(0, 10) });
    fireEvent.click(taskMenuAction('删除', '同步任务 11'));
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));
    await waitFor(() => expect(remove).toHaveBeenCalledWith(items[10]));
    await waitFor(() => expect(cards.getAllByRole('button', { name: / 详情$/u })).toHaveLength(10));
    expect(cards.queryByText('同步任务 11')).not.toBeInTheDocument();
    expect(cards.getByText('同步任务 1')).toBeInTheDocument();
    list.mockResolvedValue({
      tasks: [
        ...items.slice(0, 10),
        { ...items[10]!, id: '99999999-9999-4999-8999-999999999999', name: '新建同步任务' },
      ],
    });
    fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
    expect(cards.getByText('同步任务 1')).toBeInTheDocument();
    expect(cards.queryByText('新建同步任务')).not.toBeInTheDocument();
  });

  it('does not invent mapping counts for a task without a saved configuration', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [{ ...task(), draft: null }] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await screen.findByRole('button', { name: '订单同步 详情' });
    expect(
      within(screen.getByRole('list', { name: '任务列表' })).queryByText(/个来源/u),
    ).not.toBeInTheDocument();
  });

  it('opens a homepage-selected task from the refreshed accessible list only once', async () => {
    const existing = task();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(
      <TaskManagementPage
        initialDetailTaskId={existing.id}
        session={session}
        onSessionInvalidated={vi.fn()}
      />,
    );
    const dialog = await screen.findByRole('dialog', { name: '任务详情' });
    expect(within(dialog).getByText('订单同步')).toBeInTheDocument();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: '任务详情' })).not.toBeInTheDocument(),
    );
    fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' }));
    await waitFor(() => expect(tasksApi.list).toHaveBeenCalledTimes(2));
    expect(screen.queryByRole('dialog', { name: '任务详情' })).not.toBeInTheDocument();
  });

  it('does not open stale homepage task details after access is removed or the task is deleted', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(
      <TaskManagementPage
        initialDetailTaskId={task().id}
        session={session}
        onSessionInvalidated={vi.fn()}
      />,
    );
    await screen.findByText('任务已不存在或当前账号无权查看，请刷新任务列表。');
    expect(screen.queryByRole('dialog', { name: '任务详情' })).not.toBeInTheDocument();
  });

  it('keeps one runtime status and moves editing into details without redundant list actions', async () => {
    const base = task();
    const running: TaskSummary = {
      ...base,
      applied: { ...base.draft!, state: 'APPLIED' },
      draft: null,
      desiredState: 'RUNNING',
      observedState: 'RUNNING',
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [running] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([
      {
        taskId: running.id,
        revisionId: running.applied!.id,
        sourceType: 'KAFKA',
        status: 'AVAILABLE',
        currentStage: 'MONITORING_READY',
        collectedAt: '2026-10-23T01:01:24.000Z',
        slotStatus: null,
        unconfirmedWalBytes: null,
        retainedWalBytes: null,
        offsetLag: '42',
        timeDelayMilliseconds: null,
        timeDelayStatus: 'UNSUPPORTED',
      },
    ]);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await screen.findByText('Offset 积压：42');
    const list = within(screen.getByRole('list', { name: '任务列表' }));
    expect(list.getAllByText('运行中')).toHaveLength(1);
    expect(list.queryByText('配置状态')).not.toBeInTheDocument();
    expect(list.queryByText(/采集于/u)).not.toBeInTheDocument();
    expect(list.queryByRole('button', { name: '订单同步 编辑' })).not.toBeInTheDocument();
    expect(list.queryByRole('button', { name: '订单同步 分组' })).not.toBeInTheDocument();
    expect(list.queryByRole('button', { name: '订单同步 开始' })).not.toBeInTheDocument();
    expect(list.getByRole('button', { name: '订单同步 停止' })).toBeEnabled();
    expect(taskMenuAction('复制')).not.toHaveAttribute('aria-disabled', 'true');
    expect(taskMenuAction('删除')).toHaveAttribute('aria-disabled', 'true');
    closeTaskMenu();
    openTaskEditor(list.getByRole('button', { name: '订单同步 详情' }));
    expect(screen.getByRole('heading', { name: '编辑 订单同步' })).toBeInTheDocument();
    expect(screen.getByLabelText('任务名称')).toHaveValue('订单同步');
  });

  it('defaults to manual list refresh and refreshes progress with the task list', async () => {
    vi.useFakeTimers();
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const read = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    expect(screen.getByRole('checkbox', { name: '列表自动刷新' })).not.toBeChecked();
    expect(read).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(list).toHaveBeenCalledTimes(1);
    expect(read).toHaveBeenCalledTimes(1);
    await act(async () => fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' })));
    expect(list).toHaveBeenCalledTimes(2);
    expect(read).toHaveBeenCalledTimes(2);
  });

  it('does not let an earlier list refresh overwrite a completed start', async () => {
    const existing = task('PREFLIGHT_PASSED');
    const running: TaskSummary = { ...existing, desiredState: 'RUNNING', observedState: 'RUNNING' };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValue({ tasks: [running] });
    let finishRefresh!: (value: { dataSources: DataSourceSummary[] }) => void;
    vi.spyOn(dataSourcesApi, 'list')
      .mockResolvedValueOnce({ dataSources: [kafkaSource, kafkaTarget] })
      .mockReturnValueOnce(
        new Promise((resolve) => {
          finishRefresh = resolve;
        }),
      );
    vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing));
    vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await screen.findByRole('button', { name: '订单同步 开始' });
    fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' }));
    fireEvent.click(screen.getByRole('button', { name: '订单同步 开始' }));
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '订单同步 停止' })).toBeEnabled(),
    );
    await act(async () => finishRefresh({ dataSources: [kafkaSource, kafkaTarget] }));
    expect(screen.getByRole('button', { name: '订单同步 停止' })).toBeEnabled();
    expect(screen.queryByRole('button', { name: '订单同步 开始' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '刷新任务列表' })).toBeEnabled();
  });

  it('does not let an earlier list refresh overwrite a saved draft', async () => {
    const existing = task();
    const saved: TaskSummary = {
      ...existing,
      draft: {
        ...existing.draft!,
        draftVersion: '2',
        configuration: { ...existing.draft!.configuration, tuning: { batchSize: 300 } },
      },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    let finishRefresh!: (value: { dataSources: DataSourceSummary[] }) => void;
    vi.spyOn(dataSourcesApi, 'list')
      .mockResolvedValueOnce({ dataSources: [kafkaSource, kafkaTarget] })
      .mockReturnValueOnce(
        new Promise((resolve) => {
          finishRefresh = resolve;
        }),
      );
    vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(saved);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await screen.findByRole('button', { name: '订单同步 详情' });
    fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' }));
    openTaskEditor(screen.getByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    fireEvent.change(screen.getByLabelText('批次大小'), { target: { value: '300' } });
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await screen.findByText('任务草稿已保存。');
    await act(async () => finishRefresh({ dataSources: [kafkaSource, kafkaTarget] }));
    fireEvent.click(screen.getByRole('button', { name: '返回任务列表' }));
    openTaskEditor(screen.getByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    expect(screen.getByLabelText('批次大小')).toHaveValue(300);
  });

  it('filters shared groups and moves a running task without saving its synchronization draft', async () => {
    const grouped = {
      ...task(),
      groupId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      name: '分组内任务',
    };
    const ungrouped = {
      ...task(),
      id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      name: '未分组任务',
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    vi.mocked(taskGroupsApi.list).mockResolvedValue({
      groups: [{ id: grouped.groupId, name: '订单组' }],
    });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [grouped, ungrouped] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const assign = vi
      .spyOn(taskGroupsApi, 'assign')
      .mockResolvedValue({ ...ungrouped, groupId: grouped.groupId });
    const save = vi.spyOn(tasksApi, 'saveDraft');
    const stop = vi.spyOn(tasksApi, 'stop');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const table = within(await screen.findByRole('list', { name: '任务列表' }));
    const navigation = within(screen.getByLabelText('任务分组导航'));
    const target = await navigation.findByText('订单组');
    fireEvent.click(target);
    expect(table.getByText('分组内任务')).toBeInTheDocument();
    expect(table.queryByText('未分组任务')).not.toBeInTheDocument();
    fireEvent.click(navigation.getByText('全部任务'));
    const moving = table.getByText('未分组任务');
    fireEvent.dragStart(moving.closest('li')!, {
      dataTransfer: { setData: vi.fn(), effectAllowed: '' },
    });
    fireEvent.dragOver(target, {
      dataTransfer: { types: ['application/x-scs-task-id'], getData: () => ungrouped.id },
    });
    fireEvent.drop(target, {
      dataTransfer: { types: ['application/x-scs-task-id'], getData: () => ungrouped.id },
    });
    await waitFor(() => expect(assign).toHaveBeenCalledWith(ungrouped, grouped.groupId));
    expect(save).not.toHaveBeenCalled();
    expect(stop).not.toHaveBeenCalled();
    fireEvent.click(target);
    expect(table.getByText('分组内任务')).toBeInTheDocument();
    expect(table.getByText('未分组任务')).toBeInTheDocument();
  });

  it('renames a shared folder from its menu and retains the selected task list', async () => {
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '订单组' };
    vi.mocked(taskGroupsApi.list).mockResolvedValue({ groups: [group] });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [{ ...task(), groupId: group.id }] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const rename = vi
      .spyOn(taskGroupsApi, 'rename')
      .mockResolvedValue({ ...group, name: '新的订单组' });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '订单组 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '重命名' }));
    const dialog = within(await screen.findByRole('dialog', { name: '重命名任务组' }));
    expect(dialog.getByLabelText('任务组名称')).toHaveValue('订单组');
    fireEvent.change(dialog.getByLabelText('任务组名称'), { target: { value: '新的订单组' } });
    fireEvent.click(dialog.getByRole('button', { name: '保存名称' }));
    await waitFor(() => expect(rename).toHaveBeenCalledWith(group, '新的订单组'));
    expect(await screen.findByRole('button', { name: '新的订单组 分组操作' })).toBeInTheDocument();
    expect(screen.getByRole('list', { name: '任务列表' })).toHaveTextContent('订单同步');
  });

  it('confirms occupied folder deletion, keeps running tasks and returns to All', async () => {
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '订单组' };
    const running = {
      ...task(),
      groupId: group.id,
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    vi.mocked(taskGroupsApi.list).mockResolvedValue({ groups: [group] });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [running] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const remove = vi.spyOn(taskGroupsApi, 'delete').mockResolvedValue({ deleted: true });
    const stop = vi.spyOn(tasksApi, 'stop');
    const deleteTask = vi.spyOn(tasksApi, 'remove');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const navigation = within(await screen.findByLabelText('任务分组导航'));
    fireEvent.click(await navigation.findByText('订单组'));
    fireEvent.click(screen.getByRole('button', { name: '订单组 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '删除分组' }));
    expect(remove).not.toHaveBeenCalled();
    const dialog = within(await screen.findByRole('dialog', { name: '删除任务组' }));
    expect(dialog.getByText(/该分组内仍有任务/u)).toBeInTheDocument();
    fireEvent.click(dialog.getByRole('button', { name: '删除分组并保留任务' }));
    await waitFor(() => expect(remove).toHaveBeenCalledWith(group, true));
    await waitFor(() => expect(navigation.queryByText('订单组')).not.toBeInTheDocument());
    expect(screen.getByRole('list', { name: '任务列表' })).toHaveTextContent('订单同步');
    expect(screen.getByRole('list', { name: '任务列表' })).toHaveTextContent('运行中');
    expect(stop).not.toHaveBeenCalled();
    expect(deleteTask).not.toHaveBeenCalled();
  });

  it('closes a stale rename form and reloads the current folder name for a new attempt', async () => {
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '原名' };
    vi.mocked(taskGroupsApi.list)
      .mockResolvedValueOnce({ groups: [group] })
      .mockResolvedValue({ groups: [{ ...group, name: '其他用户的新名' }] });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskGroupsApi, 'rename').mockRejectedValue(
      new PlatformApiError('TASK_GROUP_CHANGED', 409),
    );
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '原名 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '重命名' }));
    fireEvent.click(
      within(await screen.findByRole('dialog', { name: '重命名任务组' })).getByRole('button', {
        name: '保存名称',
      }),
    );
    await screen.findByRole('button', { name: '其他用户的新名 分组操作' });
    expect(screen.queryByRole('dialog', { name: '重命名任务组' })).not.toBeInTheDocument();
  });

  it('asks again when the server finds tasks missing from the visible folder snapshot', async () => {
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '共享组' };
    vi.mocked(taskGroupsApi.list).mockResolvedValue({ groups: [group] });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const remove = vi
      .spyOn(taskGroupsApi, 'delete')
      .mockRejectedValueOnce(new PlatformApiError('TASK_GROUP_NOT_EMPTY', 409))
      .mockResolvedValue({ deleted: true });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '共享组 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '删除分组' }));
    const dialog = within(await screen.findByRole('dialog', { name: '删除任务组' }));
    fireEvent.click(dialog.getByRole('button', { name: '删除分组' }));
    await waitFor(() => expect(remove).toHaveBeenCalledWith(group, false));
    expect(await dialog.findByText(/该分组内仍有任务/u)).toBeInTheDocument();
    expect(remove).toHaveBeenCalledTimes(1);
    fireEvent.click(dialog.getByRole('button', { name: '删除分组并保留任务' }));
    await waitFor(() => expect(remove).toHaveBeenLastCalledWith(group, true));
  });

  it('refreshes list and progress at the selected interval and pauses while editing', async () => {
    vi.useFakeTimers();
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const read = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('checkbox', { name: '列表自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(list).toHaveBeenCalledTimes(2);
    expect(read).toHaveBeenCalledTimes(2);
    fireEvent.mouseDown(screen.getByLabelText('列表刷新间隔'));
    fireEvent.click(screen.getByText('5 秒', { selector: '.ant-select-item-option-content' }));
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(list).toHaveBeenCalledTimes(3);
    openTaskEditor(screen.getByRole('button', { name: '订单同步 详情' }));
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(list).toHaveBeenCalledTimes(3);
    fireEvent.click(screen.getByRole('button', { name: '返回任务列表' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '列表自动刷新' }));
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(list).toHaveBeenCalledTimes(3);
  });

  it('pauses automatic reads throughout a slow deletion from the list', async () => {
    vi.useFakeTimers();
    const existing = task();
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    let finishDelete!: (value: TaskSummary) => void;
    const remove = vi.spyOn(tasksApi, 'remove').mockReturnValue(
      new Promise((resolve) => {
        finishDelete = resolve;
      }),
    );
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('checkbox', { name: '列表自动刷新' }));
    fireEvent.click(taskMenuAction('删除'));
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(list).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(remove).toHaveBeenCalledOnce();
    expect(list).toHaveBeenCalledTimes(1);
    list.mockResolvedValue({ tasks: [] });
    await act(async () =>
      finishDelete({ ...existing, desiredState: 'DELETED', observedState: 'DELETED' }),
    );
    expect(list).toHaveBeenCalledTimes(2);
    expect(
      within(screen.getByRole('list', { name: '任务列表' })).queryByText('订单同步'),
    ).not.toBeInTheDocument();
  });

  it('creates a shared group on the list and selects its empty folder', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const create = vi
      .spyOn(taskGroupsApi, 'create')
      .mockResolvedValue({ id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '业务组' });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '新增任务组' }));
    fireEvent.change(screen.getByLabelText('任务组名称'), { target: { value: '业务组' } });
    fireEvent.click(screen.getByRole('button', { name: '创建任务组' }));
    await waitFor(() => expect(create).toHaveBeenCalledWith('业务组'));
    await screen.findByText('没有匹配的任务');
    expect(
      within(screen.getByRole('list', { name: '任务列表' })).queryByText('订单同步'),
    ).not.toBeInTheDocument();
    expect(screen.getByRole('treeitem', { name: /业务组.*0/ })).toHaveAttribute(
      'aria-selected',
      'true',
    );
  });

  it('pauses automatic reads while a new group is being confirmed', async () => {
    vi.useFakeTimers();
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(screen.getByRole('checkbox', { name: '列表自动刷新' }));
    fireEvent.click(screen.getByRole('button', { name: '新增任务组' }));
    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    expect(list).toHaveBeenCalledTimes(1);
    expect(taskGroupsApi.list).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: '取消' }));
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(list).toHaveBeenCalledTimes(2);
  });

  it('quick creates and selects a group while copying a task, then saves it atomically with the new task', async () => {
    const existing = task();
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '快捷组' };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskGroupsApi, 'create').mockResolvedValue(group);
    const create = vi
      .spyOn(tasksApi, 'create')
      .mockResolvedValue({ ...existing, name: '订单同步 副本', groupId: group.id });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('复制'));
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务分组' }));
    fireEvent.click(screen.getByRole('button', { name: '快捷创建任务组' }));
    fireEvent.change(screen.getByLabelText('任务组名称'), { target: { value: group.name } });
    fireEvent.click(screen.getByRole('button', { name: '创建并选择' }));
    await screen.findByText('任务组已创建。');
    fireEvent.click(screen.getByRole('button', { name: '创建并保存草稿' }));
    await waitFor(() =>
      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: group.id, name: '订单同步 副本' }),
      ),
    );
  });

  it('keeps editor changes intact when changing group and does not restart a running task', async () => {
    const existing = {
      ...task(),
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    const group = { id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '已有组' };
    vi.mocked(taskGroupsApi.list).mockResolvedValue({ groups: [group] });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    let assigned!: (value: TaskSummary) => void;
    const assign = vi.spyOn(taskGroupsApi, 'assign').mockReturnValue(
      new Promise((resolve) => {
        assigned = resolve;
      }),
    );
    const stop = vi.spyOn(tasksApi, 'stop');
    const save = vi.spyOn(tasksApi, 'saveDraft');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    fireEvent.change(screen.getByLabelText('批次大小'), { target: { value: '201' } });
    fireEvent.click(screen.getByRole('button', { name: '1. 来源 Source' }));
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务分组' }));
    fireEvent.click(screen.getByText(group.name, { selector: '.ant-select-item-option-content' }));
    await waitFor(() => expect(assign).toHaveBeenCalledWith(existing, group.id));
    expect(screen.getByRole('button', { name: '保存草稿' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    expect(stop).not.toHaveBeenCalled();
    expect(save).not.toHaveBeenCalled();
    await act(async () => assigned({ ...existing, groupId: group.id }));
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    expect(screen.getByLabelText('批次大小')).toHaveValue(201);
    expect(screen.getByRole('button', { name: '保存草稿' })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: '1. 来源 Source' }));
    expect(screen.getByText('分组修改即时保存，不影响同步配置。')).toBeInTheDocument();
  });

  it.each(['STOPPED', 'STARTING', 'RUNNING', 'STOPPING', 'FAILED', 'BLOCKED'] as const)(
    'disables list runtime actions that do not apply to %s',
    async (state) => {
      const base = task();
      const existing: TaskSummary = {
        ...base,
        applied: { ...base.draft!, state: 'APPLIED' },
        observedState: state,
        desiredState: ['STARTING', 'RUNNING', 'STOPPING'].includes(state) ? 'RUNNING' : 'STOPPED',
      };
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
      await screen.findByRole('button', { name: '订单同步 详情' });
      const canStop = ['STARTING', 'RUNNING', 'STOPPING'].includes(state);
      const action = screen.getByRole('button', { name: `订单同步 ${canStop ? '停止' : '开始'}` });
      expect(action).toHaveProperty(
        'disabled',
        canStop ? state === 'STOPPING' : state !== 'STOPPED',
      );
      expect(
        screen.queryByRole('button', { name: `订单同步 ${canStop ? '开始' : '停止'}` }),
      ).not.toBeInTheDocument();
      fireEvent.pointerDown(screen.getByRole('button', { name: '订单同步 更多操作' }), {
        button: 0,
        ctrlKey: false,
      });
      if (state === 'RUNNING') {
        expect(taskMenuAction('重新启动')).not.toHaveAttribute('aria-disabled', 'true');
      } else {
        expect(
          screen.queryByRole('menuitem', { name: '订单同步 重新启动' }),
        ).not.toBeInTheDocument();
      }
    },
  );

  it('starts a listed task only after a successful preflight and prevents duplicate requests', async () => {
    const existing = task('PREFLIGHT_PASSED');
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing));
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const button = await screen.findByRole('button', { name: '订单同步 开始' });
    fireEvent.click(button);
    fireEvent.click(button);
    await waitFor(() => expect(start).toHaveBeenCalledWith(existing));
    expect(preflight).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledTimes(1);
  });

  it('does not start a listed task when preflight fails', async () => {
    const existing = task();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing, false));
    const start = vi.spyOn(tasksApi, 'start');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '订单同步 开始' }));
    await screen.findByText('预检未通过，任务未启动。请编辑任务查看并处理阻断项。');
    expect(start).not.toHaveBeenCalled();
  });

  it.each(['task', 'draft'] as const)(
    'does not start after the %s version changes despite the same draft ID',
    async (kind) => {
      const existing = task('PREFLIGHT_PASSED');
      const changed: TaskSummary = {
        ...existing,
        rowVersion: kind === 'task' ? '3' : existing.rowVersion,
        draft: {
          ...existing.draft!,
          draftVersion: kind === 'draft' ? '2' : existing.draft!.draftVersion,
        },
      };
      vi.spyOn(tasksApi, 'list')
        .mockResolvedValueOnce({ tasks: [existing] })
        .mockResolvedValue({ tasks: [changed] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing));
      const start = vi.spyOn(tasksApi, 'start');
      render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
      fireEvent.click(await screen.findByRole('button', { name: '订单同步 开始' }));
      await screen.findByText('任务配置已变化，请重新预检。');
      expect(start).not.toHaveBeenCalled();
    },
  );

  it('restarts from applied configuration only after stop completion and preflight', async () => {
    const base = task('PREFLIGHT_PASSED');
    const stopped: TaskSummary = { ...base, applied: { ...base.draft!, state: 'APPLIED' } };
    const running: TaskSummary = { ...stopped, desiredState: 'RUNNING', observedState: 'RUNNING' };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [running] })
      .mockResolvedValue({ tasks: [stopped] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const stop = vi
      .spyOn(tasksApi, 'stop')
      .mockResolvedValue({ ...startOperation, action: 'STOP', state: 'SUCCEEDED' });
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(stopped));
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新启动'));
    expect(stop).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    await waitFor(() => expect(start).toHaveBeenCalledWith(stopped));
    expect(stop).toHaveBeenCalledWith(running);
    expect(preflight).toHaveBeenCalledWith(stopped);
    expect(stop.mock.invocationCallOrder[0]).toBeLessThan(preflight.mock.invocationCallOrder[0]!);
    expect(preflight.mock.invocationCallOrder[0]).toBeLessThan(start.mock.invocationCallOrder[0]!);
  });

  it('waits through successive STOPPING observations before preflight and restarting', async () => {
    vi.useFakeTimers();
    const base = task('PREFLIGHT_PASSED');
    const stopped: TaskSummary = { ...base, applied: { ...base.draft!, state: 'APPLIED' } };
    const running: TaskSummary = { ...stopped, desiredState: 'RUNNING', observedState: 'RUNNING' };
    const stopping: TaskSummary = { ...stopped, observedState: 'STOPPING' };
    const readTasks = vi
      .spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [running] })
      .mockResolvedValueOnce({ tasks: [stopping] })
      .mockResolvedValueOnce({ tasks: [stopping] })
      .mockResolvedValue({ tasks: [stopped] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(tasksApi, 'stop').mockResolvedValue({ ...startOperation, action: 'STOP' });
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(stopped));
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(taskMenuAction('重新启动'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    await act(async () => {});
    expect(readTasks).toHaveBeenCalledTimes(2);
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    await act(async () => vi.advanceTimersByTimeAsync(1_000));
    expect(readTasks).toHaveBeenCalledTimes(3);
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    await act(async () => vi.advanceTimersByTimeAsync(1_000));
    expect(preflight).toHaveBeenCalledExactlyOnceWith(stopped);
    expect(start).toHaveBeenCalledExactlyOnceWith(stopped);
    expect(preflight.mock.invocationCallOrder[0]).toBeLessThan(start.mock.invocationCallOrder[0]!);
  });

  it.each(['draft version', 'external draft', 'applied revision'] as const)(
    'rejects a changed %s during stop polling even when its configuration is identical',
    async (change) => {
      vi.useFakeTimers();
      const base = task('PREFLIGHT_PASSED');
      const applied: NonNullable<TaskSummary['applied']> = { ...base.draft!, state: 'APPLIED' };
      const running: TaskSummary = {
        ...base,
        applied,
        currentAppliedRevisionId: applied.id,
        draft: change === 'external draft' ? null : base.draft,
        desiredState: 'RUNNING',
        observedState: 'RUNNING',
      };
      const stopping: TaskSummary = {
        ...running,
        desiredState: 'STOPPED',
        observedState: 'STOPPING',
      };
      const changedApplied =
        change === 'applied revision'
          ? { ...applied, id: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd' }
          : applied;
      const changed: TaskSummary = {
        ...stopping,
        rowVersion: '3',
        observedState: 'STOPPED',
        applied: changedApplied,
        currentAppliedRevisionId: changedApplied.id,
        draft: {
          ...base.draft!,
          draftVersion: change === 'draft version' ? '2' : base.draft!.draftVersion,
        },
      };
      vi.spyOn(tasksApi, 'list')
        .mockResolvedValueOnce({ tasks: [running] })
        .mockResolvedValueOnce({ tasks: [stopping] })
        .mockResolvedValue({ tasks: [changed] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
      vi.spyOn(tasksApi, 'stop').mockResolvedValue({ ...startOperation, action: 'STOP' });
      const save = vi.spyOn(tasksApi, 'saveDraft');
      const preflight = vi.spyOn(tasksApi, 'preflight');
      const start = vi.spyOn(tasksApi, 'start');
      render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
      await act(async () => {});
      fireEvent.click(taskMenuAction('重新启动'));
      fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
      await act(async () => {});
      expect(preflight).not.toHaveBeenCalled();
      await act(async () => vi.advanceTimersByTimeAsync(1_000));
      expect(
        screen.getByText('任务配置在确认后已变化，未继续重启，请重新确认。'),
      ).toBeInTheDocument();
      expect(save).not.toHaveBeenCalled();
      expect(preflight).not.toHaveBeenCalled();
      expect(start).not.toHaveBeenCalled();
    },
  );

  it('does not restart after 30 STOPPING observations and releases the list busy state', async () => {
    vi.useFakeTimers();
    const base = task('PREFLIGHT_PASSED');
    const running: TaskSummary = {
      ...base,
      applied: { ...base.draft!, state: 'APPLIED' },
      desiredState: 'RUNNING',
      observedState: 'RUNNING',
    };
    const stopping: TaskSummary = {
      ...running,
      desiredState: 'STOPPED',
      observedState: 'STOPPING',
    };
    const idle: TaskSummary = {
      ...base,
      id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      name: '待启动任务',
    };
    const readTasks = vi
      .spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [running, idle] })
      .mockResolvedValue({ tasks: [stopping, idle] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(tasksApi, 'stop').mockResolvedValue({ ...startOperation, action: 'STOP' });
    const preflight = vi.spyOn(tasksApi, 'preflight');
    const start = vi.spyOn(tasksApi, 'start');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await act(async () => {});
    fireEvent.click(taskMenuAction('重新启动'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    await act(async () => {});
    expect(screen.getByLabelText('待启动任务 开始')).toBeDisabled();
    await act(async () => vi.advanceTimersByTimeAsync(29_000));
    expect(readTasks).toHaveBeenCalledTimes(31);
    expect(screen.getByLabelText('待启动任务 开始')).toBeDisabled();
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    await act(async () => vi.advanceTimersByTimeAsync(1_000));
    expect(readTasks).toHaveBeenCalledTimes(31);
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    expect(
      screen.getByText('任务尚未停止，未继续启动。请刷新列表确认状态后操作。'),
    ).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: '确认重新启动任务' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '待启动任务 开始' })).toBeEnabled();
  });

  it('derives an applied-only restart draft from the original configuration and artifact', async () => {
    const base = task('PREFLIGHT_PASSED');
    const applied: NonNullable<TaskSummary['applied']> = {
      ...base.draft!,
      state: 'APPLIED',
      artifactId: artifact.id,
    };
    const stopped: TaskSummary = {
      ...base,
      applied,
      draft: null,
      currentAppliedRevisionId: applied.id,
    };
    const running: TaskSummary = { ...stopped, desiredState: 'RUNNING', observedState: 'RUNNING' };
    const saved: TaskSummary = {
      ...stopped,
      rowVersion: '3',
      draft: { ...applied, id: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', state: 'READY_DRAFT' },
    };
    const checked: TaskSummary = {
      ...saved,
      draft: { ...saved.draft!, state: 'PREFLIGHT_PASSED' },
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [running] })
      .mockResolvedValueOnce({ tasks: [stopped] })
      .mockResolvedValue({ tasks: [checked] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(tasksApi, 'stop').mockResolvedValue({ ...startOperation, action: 'STOP' });
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(saved);
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(saved));
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新启动'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    await waitFor(() => expect(start).toHaveBeenCalledExactlyOnceWith(checked));
    expect(save).toHaveBeenCalledExactlyOnceWith(stopped, applied.configuration, artifact.id);
    expect(preflight).toHaveBeenCalledExactlyOnceWith(saved);
    expect(save.mock.invocationCallOrder[0]).toBeLessThan(preflight.mock.invocationCallOrder[0]!);
  });

  it('disables restart when a different draft exists and still allows stopping', async () => {
    const base = task();
    const existing: TaskSummary = {
      ...base,
      desiredState: 'RUNNING',
      observedState: 'RUNNING',
      applied: { ...base.draft!, state: 'APPLIED', artifactId: artifact.id },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const stop = vi
      .spyOn(tasksApi, 'stop')
      .mockResolvedValue({ ...startOperation, action: 'STOP' });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    expect(await findTaskMenuAction('重新启动')).toHaveAttribute('aria-disabled', 'true');
    closeTaskMenu();
    fireEvent.click(screen.getByRole('button', { name: '订单同步 停止' }));
    expect(stop).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '确认停止' }));
    await waitFor(() => expect(stop).toHaveBeenCalledWith(existing));
  });

  it('does not start again when a restart stop is rejected', async () => {
    const base = task('PREFLIGHT_PASSED');
    const existing: TaskSummary = {
      ...base,
      applied: { ...base.draft!, state: 'APPLIED' },
      desiredState: 'RUNNING',
      observedState: 'RUNNING',
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(tasksApi, 'stop').mockResolvedValue({
      ...startOperation,
      action: 'STOP',
      state: 'REJECTED',
      failureSummary: '停止请求被拒绝',
    });
    const preflight = vi.spyOn(tasksApi, 'preflight');
    const start = vi.spyOn(tasksApi, 'start');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新启动'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    await screen.findByText('停止未完成：停止请求被拒绝');
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it('cancels the remaining restart sequence when leaving the page', async () => {
    const base = task('PREFLIGHT_PASSED');
    const existing: TaskSummary = {
      ...base,
      applied: { ...base.draft!, state: 'APPLIED' },
      desiredState: 'RUNNING',
      observedState: 'RUNNING',
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    let finishStop!: (value: typeof startOperation) => void;
    vi.spyOn(tasksApi, 'stop').mockImplementation(
      () =>
        new Promise((resolve) => {
          finishStop = resolve;
        }),
    );
    const preflight = vi.spyOn(tasksApi, 'preflight');
    const start = vi.spyOn(tasksApi, 'start');
    const { unmount } = render(
      <TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />,
    );
    fireEvent.click(await findTaskMenuAction('重新启动'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新启动' }));
    unmount();
    await act(async () => finishStop(startOperation));
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  async function openAppliedPgEditor(includeSnapshot = false) {
    const source: DataSourceSummary = {
      ...kafkaSource,
      configuration: {
        type: 'POSTGRES',
        database: 'data_demo',
        host: 'postgres',
        port: 5432,
        tls: { mode: 'REQUIRE' },
        username: 'reader',
      },
    };
    const base = task();
    const applied: NonNullable<TaskSummary['applied']> = {
      ...base.draft!,
      state: 'APPLIED',
      configurationSchemaVersion: 2,
      configuration: {
        kind: 'PG_TO_KAFKA',
        schemaVersion: 2,
        source: {
          dataSourceId: source.id,
          resourceMode: 'PLATFORM_MANAGED',
          slotName: 'cdc_slot',
          publicationName: 'cdc_pub',
          includeSnapshot,
        },
        target: { dataSourceId: kafkaTarget.id },
        mappings: [
          { source: { schema: 'public', name: 'samples' }, targets: [{ topic: 'samples' }] },
        ],
        tuning: {
          batchSize: 100,
          bufferSize: 16000,
          checkpointIntervalSeconds: 15,
          requiredAcks: 'all',
        },
      },
    };
    const existing: TaskSummary = {
      ...base,
      taskType: 'PG_KAFKA',
      draft: null,
      applied,
      currentAppliedRevisionId: applied.id,
    };
    vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [source, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (item, query) => ({
      ...metadata(item, 'TOPICS'),
      resource: query.resource,
      items: [],
    }));
    const view = render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByLabelText('订单同步 详情'));
    return { existing, ...view };
  }

  function addPgTable(name = 'customers') {
    fireEvent.click(screen.getByText('2. 目标 Sink', { selector: 'button' }));
    fireEvent.click(screen.getByText('添加来源').closest('button')!);
    fireEvent.change(screen.getByLabelText('源 Schema 2'), { target: { value: 'public' } });
    fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: name } });
    addTargetTopic('customers.cdc', '目标 Topic 2');
  }

  it('confirms only actual added PG tables, cancels without writing and submits the confirmed CDC-only snapshot once', async () => {
    const { existing } = await openAppliedPgEditor();
    let resolveSave!: (value: TaskSummary) => void;
    const save = vi.spyOn(tasksApi, 'saveDraft').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveSave = resolve;
        }),
    );
    const prepare = vi.spyOn(tasksApi, 'preparePgResources');
    fireEvent.click(screen.getByLabelText('首次运行包含初始快照'));
    addPgTable();
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    let confirmation = screen
      .getByText('确认新增同步表', { selector: 'h2' })
      .closest<HTMLElement>('[role="dialog"]')!;
    expect(confirmation).toHaveTextContent('public.customers');
    expect(confirmation).toHaveTextContent('不扫描存量数据');
    expect(confirmation).toHaveTextContent('原复制槽位点');
    expect(confirmation).toHaveTextContent('WAL 积压');
    expect(confirmation).toHaveTextContent('不会启动任务');
    expect(save).not.toHaveBeenCalled();
    fireEvent.click(within(confirmation).getByRole('button', { name: '取消' }));
    expect(prepare).not.toHaveBeenCalled();
    expect(save).not.toHaveBeenCalled();
    fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: 'customers_new' } });
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    confirmation = screen
      .getByText('确认新增同步表', { selector: 'h2' })
      .closest<HTMLElement>('[role="dialog"]')!;
    expect(confirmation).toHaveTextContent('public.customers_new');
    const confirm = within(confirmation).getByRole('button', { name: '确认并保存草稿' });
    fireEvent.click(confirm);
    fireEvent.click(confirm);
    expect(save).toHaveBeenCalledExactlyOnceWith(
      existing,
      expect.objectContaining({ source: expect.objectContaining({ includeSnapshot: false }) }),
      null,
      true,
    );
    const configuration = save.mock.calls[0]![1];
    await act(async () =>
      resolveSave({
        ...existing,
        rowVersion: '3',
        draft: {
          ...existing.applied!,
          id: '88888888-8888-4888-8888-888888888888',
          state: 'READY_DRAFT',
          configuration,
        },
      }),
    );
    expect(screen.queryByText('确认新增同步表', { selector: 'h2' })).not.toBeInTheDocument();
    expect(screen.getByLabelText('源表 2')).toHaveValue('customers_new');
    fireEvent.click(screen.getByText('1. 来源 Source', { selector: 'button' }));
    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    expect(screen.getByText('准备复制资源', { selector: 'button' })).toBeEnabled();
  });

  it.each(['snapshot', 'slot'] as const)(
    'does not allow PG table addition to bypass an incompatible %s baseline',
    async (reason) => {
      await openAppliedPgEditor(reason === 'snapshot');
      const save = vi.spyOn(tasksApi, 'saveDraft');
      if (reason === 'slot')
        fireEvent.change(screen.getByLabelText('Slot 名称'), {
          target: { value: 'different_slot' },
        });
      addPgTable();
      fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
      expect(
        await screen.findByText(
          reason === 'snapshot'
            ? /原配置包含初始快照.*无法确认/u
            : /新增表必须保留原连接.*旧表与目标 Topic/u,
        ),
      ).toBeInTheDocument();
      expect(screen.queryByText('确认新增同步表', { selector: 'h2' })).not.toBeInTheDocument();
      expect(save).not.toHaveBeenCalled();
    },
  );

  it('does not overwrite newer edits with a late PG addition save response', async () => {
    const { existing } = await openAppliedPgEditor();
    let resolveSave!: (value: TaskSummary) => void;
    const save = vi.spyOn(tasksApi, 'saveDraft').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveSave = resolve;
        }),
    );
    addPgTable();
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    fireEvent.click(screen.getByText('确认并保存草稿', { selector: 'button' }));
    fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: 'later_edit' } });
    await act(async () =>
      resolveSave({
        ...existing,
        rowVersion: '3',
        draft: {
          ...existing.applied!,
          state: 'READY_DRAFT',
          configuration: save.mock.calls[0]![1],
        },
      }),
    );
    expect(screen.getByLabelText('源表 2')).toHaveValue('later_edit');
    expect(screen.queryByText('任务草稿已保存。')).not.toBeInTheDocument();
  });

  it('saves an additional target for the same PG table without a table addition prompt', async () => {
    const { existing } = await openAppliedPgEditor();
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    fireEvent.click(screen.getByText('2. 目标 Sink', { selector: 'button' }));
    addTargetTopic('samples.archive');
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    expect(screen.queryByText('确认新增同步表', { selector: 'h2' })).not.toBeInTheDocument();
    expect(save).toHaveBeenCalledExactlyOnceWith(
      existing,
      expect.objectContaining({
        mappings: [
          {
            source: { schema: 'public', name: 'samples' },
            targets: [{ topic: 'samples' }, { topic: 'samples.archive' }],
          },
        ],
      }),
      null,
    );
  });

  it('invalidates a table addition confirmation when its form changes, even if changed back', async () => {
    await openAppliedPgEditor();
    const save = vi.spyOn(tasksApi, 'saveDraft');
    addPgTable();
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    const oldConfirm = screen.getByText('确认并保存草稿', { selector: 'button' });
    fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: 'changed' } });
    fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: 'customers' } });
    expect(screen.queryByText('确认新增同步表', { selector: 'h2' })).not.toBeInTheDocument();
    fireEvent.click(oldConfirm);
    expect(save).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
    expect(screen.getByText('确认新增同步表', { selector: 'h2' })).toBeInTheDocument();
    fireEvent.click(screen.getByText('取消', { selector: 'button' }));
  });

  it.each([
    { taskType: 'PG_KAFKA' as const, schemaVersion: 1 },
    { taskType: 'PG_KAFKA' as const, schemaVersion: 2 },
    { taskType: 'KAFKA_KAFKA' as const, schemaVersion: 1 },
  ])(
    'restores applied-only $taskType v$schemaVersion configuration and saves edits',
    async ({ taskType, schemaVersion }) => {
      const base = task();
      const pgSource: DataSourceSummary = {
        ...kafkaSource,
        configuration: {
          type: 'POSTGRES',
          database: 'data_demo',
          host: 'postgres',
          port: 5432,
          tls: { mode: 'REQUIRE' },
          username: 'reader',
        },
      };
      const configuration =
        taskType === 'PG_KAFKA'
          ? {
              kind: 'PG_TO_KAFKA',
              schemaVersion,
              source: {
                dataSourceId: pgSource.id,
                resourceMode: 'PLATFORM_MANAGED',
                slotName: 'existing_slot',
                publicationName: 'existing_pub',
                includeSnapshot: false,
                ...(schemaVersion === 1 ? { table: { schema: 'public', name: 'orders' } } : {}),
              },
              target: {
                dataSourceId: kafkaTarget.id,
                messageFormat: 'json',
                jsonTemplate: 'cloudcanal',
                withFieldDefs: false,
                ...(schemaVersion === 1 ? { topic: 'orders.target' } : {}),
              },
              ...(schemaVersion === 2
                ? {
                    mappings: [
                      {
                        source: { schema: 'public', name: 'orders' },
                        targets: [{ topic: 'orders.target' }, { topic: 'orders.archive' }],
                      },
                    ],
                  }
                : {}),
              tuning: { batchSize: 200, bufferSize: 16000, checkpointIntervalSeconds: 15 },
            }
          : base.draft!.configuration;
      const applied: NonNullable<TaskSummary['applied']> = {
        ...base.draft!,
        state: 'APPLIED',
        artifactId: artifact.id,
        configurationSchemaVersion: schemaVersion,
        configuration,
      };
      const originalApplied = structuredClone(applied);
      const existing: TaskSummary = {
        ...base,
        taskType,
        draft: null,
        applied,
        currentAppliedRevisionId: applied.id,
      };
      const saved: TaskSummary = {
        ...existing,
        rowVersion: '3',
        draft: {
          ...applied,
          state: 'READY_DRAFT',
          id: '88888888-8888-4888-8888-888888888888',
          configuration: {
            ...configuration,
            tuning: { ...(configuration.tuning as Record<string, unknown>), batchSize: 300 },
          },
        },
      };
      vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [taskType === 'PG_KAFKA' ? pgSource : kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source, query) => ({
        ...metadata(source, 'TOPICS'),
        resource: query.resource,
        items: [],
      }));
      const transport = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify(saved), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
      );
      await expect(
        tasksApi.saveDraft(existing, saved.draft!.configuration, artifact.id),
      ).resolves.toMatchObject({ draft: saved.draft });
      expect(transport).toHaveBeenCalledOnce();
      transport.mockRestore();
      const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(saved);
      render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      if (taskType === 'PG_KAFKA') {
        expect(screen.getByLabelText('Slot 名称')).toHaveValue('existing_slot');
        expect(screen.getByLabelText('Publication 名称')).toHaveValue('existing_pub');
      }
      fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
      if (taskType === 'PG_KAFKA') {
        expect(screen.getByLabelText('源 Schema')).toHaveValue('public');
        expect(screen.getByLabelText('源表')).toHaveValue('orders');
        expect(screen.getByLabelText('消息格式')).toHaveValue('json');
        expect(screen.getByLabelText('JSON 模板')).toHaveValue('cloudcanal');
        expect(screen.getByLabelText('字段定义（Avro）')).toHaveValue('false');
        if (schemaVersion === 2)
          expect(screen.getAllByText('orders.archive').length).toBeGreaterThan(0);
      } else {
        expect(screen.getByLabelText('源 Topic')).toHaveValue('orders.source');
      }
      expect(screen.getAllByText('orders.target').length).toBeGreaterThan(0);
      expect(screen.getByText('保存草稿', { selector: 'button' })).toBeEnabled();
      fireEvent.click(screen.getByText('3. 运行配置', { selector: 'button' }));
      expect(screen.getByLabelText('运行制品')).toHaveValue(artifact.id);
      fireEvent.change(screen.getByLabelText('批次大小'), { target: { value: '300' } });
      fireEvent.click(screen.getByText('保存草稿', { selector: 'button' }));
      expect(await screen.findByText('任务草稿已保存。')).toBeInTheDocument();
      expect(save).toHaveBeenCalledOnce();
      expect(screen.getByLabelText('批次大小')).toHaveValue(300);
      expect(existing.applied).toEqual(originalApplied);
    },
  );

  it('requires explicit confirmation and successful fresh preflight before reinitializing MySQL', async () => {
    const existing = mysqlTask();
    const checked = { ...mysqlTask('PREFLIGHT_PASSED'), rowVersion: '3' };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValue({ tasks: [checked] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing));
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新全量'));
    const dialog = screen.getByRole('dialog', { name: '重新执行 MySQL 全量同步？' });
    expect(dialog).toHaveTextContent('可能产生重复数据');
    expect(dialog).toHaveTextContent('不会删除源库数据、Kafka 消息或历史运行');
    expect(preflight).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole('button', { name: '确认重新全量' }));
    await waitFor(() =>
      expect(start).toHaveBeenCalledWith(checked, { reinitializeMysqlSnapshot: true }),
    );
    expect(preflight).toHaveBeenCalledExactlyOnceWith(existing, {
      reinitializeMysqlSnapshot: true,
    });
  });

  it('does not reset or start MySQL when reinitialization preflight fails', async () => {
    const existing = mysqlTask();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(existing, false));
    const start = vi.spyOn(tasksApi, 'start');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新全量'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新全量' }));
    expect(
      await screen.findByText('重新全量预检未通过，任务未启动，原检查点未重置。'),
    ).toBeInTheDocument();
    expect(start).not.toHaveBeenCalled();
  });

  it('guides snapshot-disabled MySQL tasks to edit without silently enabling snapshots', async () => {
    const existing = mysqlTask('READY_DRAFT', false);
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const preflight = vi.spyOn(tasksApi, 'preflight');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新全量'));
    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    expect(
      await screen.findByText('请先开启“首次运行包含初始快照”并保存草稿，再执行重新全量。'),
    ).toBeInTheDocument();
    expect(preflight).not.toHaveBeenCalled();
  });

  it('does not expose MySQL reinitialization while the task is running', async () => {
    const existing = {
      ...mysqlTask(),
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    await screen.findByRole('button', { name: '订单同步 详情' });
    fireEvent.pointerDown(screen.getByRole('button', { name: '订单同步 更多操作' }), {
      button: 0,
      ctrlKey: false,
    });
    expect(screen.queryByRole('menuitem', { name: '订单同步 重新全量' })).not.toBeInTheDocument();
  });

  it('keeps ordinary MySQL start as resume without a reset flag or extra confirmation', async () => {
    const existing = mysqlTask('PREFLIGHT_PASSED');
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const start = vi.spyOn(tasksApi, 'start').mockResolvedValue(startOperation);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '启动任务' }));
    await waitFor(() => expect(start).toHaveBeenCalledExactlyOnceWith(existing));
    expect(
      screen.queryByRole('dialog', { name: '重新执行 MySQL 全量同步？' }),
    ).not.toBeInTheDocument();
  });

  it('opens applied-only MySQL configuration for an explicit draft save before reinitializing', async () => {
    const base = mysqlTask();
    const applied: TaskSummary = {
      ...base,
      draft: null,
      applied: { ...base.draft!, state: 'APPLIED', artifactId: artifact.id },
      currentAppliedRevisionId: base.draft!.id,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [applied] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const preflight = vi.spyOn(tasksApi, 'preflight');
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(base);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新全量'));
    expect(
      await screen.findByText('请先保存当前已应用配置为草稿，再执行重新全量。'),
    ).toBeInTheDocument();
    expect(screen.getByLabelText('首次运行包含初始快照')).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('源表')).toHaveValue('orders');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        applied,
        expect.objectContaining(applied.applied!.configuration),
        artifact.id,
      ),
    );
    expect(preflight).not.toHaveBeenCalled();
  });

  it('prepares an unchanged MySQL draft for normal resume without making users save it manually', async () => {
    const saved = mysqlTask();
    const existing: TaskSummary = {
      ...saved,
      draft: null,
      applied: { ...saved.draft!, state: 'APPLIED' },
      currentAppliedRevisionId: saved.draft!.id,
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValue({ tasks: [saved] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(saved);
    const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(mysqlPreflight(saved));
    const start = vi.spyOn(tasksApi, 'start');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    await waitFor(() => expect(preflight).toHaveBeenCalledExactlyOnceWith(saved));
    expect(save).toHaveBeenCalledWith(
      existing,
      existing.applied!.configuration,
      existing.applied!.artifactId,
    );
    expect(start).not.toHaveBeenCalled();
  });

  it('preserves edits made during automatic MySQL draft creation and saves against its new version', async () => {
    const saved = { ...mysqlTask(), rowVersion: '3' };
    const existing: TaskSummary = {
      ...saved,
      rowVersion: '2',
      draft: null,
      applied: { ...saved.draft!, state: 'APPLIED' },
      currentAppliedRevisionId: saved.draft!.id,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    let resolveSave!: (value: TaskSummary) => void;
    const save = vi
      .spyOn(tasksApi, 'saveDraft')
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveSave = resolve;
          }),
      )
      .mockResolvedValue(saved);
    const preflight = vi.spyOn(tasksApi, 'preflight');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByLabelText('首次运行包含初始快照'));
    await act(async () => resolveSave(saved));

    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    expect(preflight).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(2));
    expect(save.mock.calls[1]?.[0]).toEqual(saved);
    expect(save.mock.calls[1]?.[1]).toMatchObject({ source: { includeSnapshot: false } });
  });

  it('keeps another task in the editor after an earlier automatic MySQL draft response arrives', async () => {
    const saved = { ...mysqlTask(), rowVersion: '3' };
    const existing: TaskSummary = {
      ...saved,
      rowVersion: '2',
      draft: null,
      applied: { ...saved.draft!, state: 'APPLIED' },
      currentAppliedRevisionId: saved.draft!.id,
    };
    const other = {
      ...mysqlTask('READY_DRAFT', false),
      id: '66666666-6666-4666-8666-666666666666',
      name: '库存同步',
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing, other] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    let resolveSave!: (value: TaskSummary) => void;
    const save = vi.spyOn(tasksApi, 'saveDraft').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveSave = resolve;
        }),
    );
    const preflight = vi.spyOn(tasksApi, 'preflight');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: '返回任务列表' }));
    openTaskEditor(screen.getByRole('button', { name: '库存同步 详情' }));
    await act(async () => resolveSave(saved));

    expect(screen.getByRole('heading', { name: '编辑 库存同步' })).toBeInTheDocument();
    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    expect(screen.getByRole('button', { name: '保存草稿' })).toBeDisabled();
    expect(preflight).not.toHaveBeenCalled();
  });

  it('preserves edits while an ordinary preflight response is pending', async () => {
    const existing = mysqlTask();
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    let resolvePreflight!: (value: TaskPreflightRunSummary) => void;
    const preflight = vi.spyOn(tasksApi, 'preflight').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvePreflight = resolve;
        }),
    );
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    await waitFor(() => expect(preflight).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByLabelText('首次运行包含初始快照'));
    await act(async () => resolvePreflight(mysqlPreflight(existing)));

    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    expect(screen.getByRole('button', { name: '保存草稿' })).toBeEnabled();
    expect(list).toHaveBeenCalledTimes(1);
  });

  it('does not continue ordinary preflight after automatic draft creation finishes on an unmounted page', async () => {
    const saved = mysqlTask();
    const existing: TaskSummary = {
      ...saved,
      draft: null,
      applied: { ...saved.draft!, state: 'APPLIED' },
      currentAppliedRevisionId: saved.draft!.id,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    let resolveSave!: (value: TaskSummary) => void;
    const save = vi.spyOn(tasksApi, 'saveDraft').mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveSave = resolve;
        }),
    );
    const preflight = vi.spyOn(tasksApi, 'preflight');
    const view = render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
    view.unmount();
    await act(async () => resolveSave(saved));
    expect(preflight).not.toHaveBeenCalled();
  });

  it('does not continue a destructive MySQL action after its page has unmounted', async () => {
    const existing = mysqlTask();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    let resolve!: (result: TaskPreflightRunSummary) => void;
    vi.spyOn(tasksApi, 'preflight').mockImplementation(
      () =>
        new Promise((next) => {
          resolve = next;
        }),
    );
    const start = vi.spyOn(tasksApi, 'start');
    const view = render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('重新全量'));
    fireEvent.click(screen.getByRole('button', { name: '确认重新全量' }));
    view.unmount();
    await act(async () => resolve(mysqlPreflight(existing)));
    expect(start).not.toHaveBeenCalled();
  });
  it('creates a MySQL task with database tables, fanout, topic creation and CloudCanal preview', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    const readMetadata = vi
      .spyOn(dataSourcesApi, 'metadata')
      .mockImplementation(async (source, query) => ({
        ...metadata(source, 'TOPICS'),
        resource: query.resource,
        items:
          query.resource === 'TABLES' ? [{ name: 'orders', identity: null, attributes: {} }] : [],
      }));
    const created = {
      ...task(),
      taskType: 'MYSQL_KAFKA' as const,
      sourceDataSourceId: mysqlSource.id,
    };
    const create = vi.spyOn(tasksApi, 'create').mockResolvedValue(created);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '新建任务' }));
    fireEvent.change(screen.getByLabelText('任务名称'), { target: { value: 'MySQL 同步' } });
    fireEvent.change(screen.getByLabelText('任务类型'), { target: { value: 'MYSQL_KAFKA' } });
    expect(screen.queryByLabelText('Slot 名称')).not.toBeInTheDocument();
    expect(screen.queryByText('Kafka Group ID 由系统生成')).not.toBeInTheDocument();
    expect(screen.getByLabelText('首次运行包含初始快照')).not.toBeChecked();
    fireEvent.click(screen.getByLabelText('首次运行包含初始快照'));
    fireEvent.change(screen.getByLabelText('源数据源'), { target: { value: mysqlSource.id } });
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('来源数据库')).toHaveValue('data_demo');
    expect(screen.queryByRole('heading', { name: '配置映射' })).not.toBeInTheDocument();
    expect(screen.getByLabelText('来源数据库')).toHaveAttribute('readonly');
    expect(screen.queryByLabelText('源 Schema')).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('源表'), { target: { value: 'orders' } });
    fireEvent.change(screen.getByLabelText('目标 Kafka'), { target: { value: kafkaTarget.id } });
    addTargetTopic('orders.primary');
    addTargetTopic('orders.audit');
    fireEvent.click(screen.getByText('Topic 创建设置'));
    fireEvent.click(screen.getByLabelText('首次启动时创建缺失的 Topic（orders.audit）'));
    fireEvent.change(screen.getByLabelText('orders.audit分区数'), { target: { value: '2' } });
    fireEvent.change(screen.getByLabelText('orders.audit副本数'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('消息格式'), { target: { value: 'json' } });
    fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
    expect(screen.getByLabelText('预计消息内容（JSON）')).toHaveTextContent(
      '"schema": "data_demo"',
    );
    expect(screen.queryByRole('heading', { name: '确认与预检' })).not.toBeInTheDocument();
    expect(screen.queryByText('复制资源', { exact: true })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '创建并保存草稿' }));
    await waitFor(() =>
      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({
          taskType: 'MYSQL_KAFKA',
          sourceDataSourceId: mysqlSource.id,
          configuration: {
            kind: 'MYSQL_TO_KAFKA',
            schemaVersion: 2,
            source: { dataSourceId: mysqlSource.id, includeSnapshot: true },
            target: {
              dataSourceId: kafkaTarget.id,
              messageFormat: 'json',
              jsonTemplate: 'cloudcanal',
            },
            mappings: [
              {
                source: { schema: 'data_demo', name: 'orders' },
                targets: [
                  { topic: 'orders.primary' },
                  {
                    topic: 'orders.audit',
                    topicCreation: { numPartitions: 2, replicationFactor: 1 },
                  },
                ],
              },
            ],
            tuning: {
              batchSize: 100,
              bufferSize: 16000,
              checkpointIntervalSeconds: 15,
              requiredAcks: 'all',
            },
          },
        }),
      ),
    );
    expect(readMetadata).toHaveBeenCalledWith(mysqlSource, {
      resource: 'TABLES',
      database: 'data_demo',
    });
    expect(
      readMetadata.mock.calls
        .filter(([source]) => source.id === mysqlSource.id)
        .some(([, query]) => query.resource === 'SCHEMAS' || query.resource === 'TOPICS'),
    ).toBe(false);
  });

  it('preserves MySQL encoding and all table mappings when editing a saved task', async () => {
    const base = task();
    const configured: TaskSummary = {
      ...base,
      taskType: 'MYSQL_KAFKA',
      sourceDataSourceId: mysqlSource.id,
      draft: {
        ...base.draft!,
        configuration: {
          kind: 'MYSQL_TO_KAFKA',
          schemaVersion: 2,
          source: { dataSourceId: mysqlSource.id, includeSnapshot: true },
          target: {
            dataSourceId: kafkaTarget.id,
            messageFormat: 'json',
            jsonTemplate: 'cloudcanal',
            withFieldDefs: false,
          },
          mappings: [
            {
              source: { schema: 'data_demo', name: 'orders' },
              targets: [{ topic: 'orders.primary' }],
            },
            {
              source: { schema: 'data_demo', name: 'items' },
              targets: [{ topic: 'items.primary' }],
            },
          ],
          tuning: { batchSize: 200, requiredAcks: 'all' },
        },
      },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [configured] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source, query) => ({
      ...metadata(source, 'TOPICS'),
      resource: query.resource,
    }));
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(configured);
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    expect(await screen.findByText('MySQL → Kafka')).toBeInTheDocument();
    openTaskEditor(screen.getByRole('button', { name: '订单同步 详情' }));
    expect(screen.queryByLabelText('Slot 名称')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('源表 2')).toHaveValue('items');
    expect(screen.getByLabelText('消息格式')).toHaveValue('json');
    addTargetTopic('items.audit', '目标 Topic 2');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        configured,
        expect.objectContaining({
          ...configured.draft!.configuration,
          mappings: [
            {
              source: { schema: 'data_demo', name: 'orders' },
              targets: [{ topic: 'orders.primary' }],
            },
            {
              source: { schema: 'data_demo', name: 'items' },
              targets: [{ topic: 'items.primary' }, { topic: 'items.audit' }],
            },
          ],
        }),
        null,
      ),
    );
  });
  it('pauses list progress polling during details and refreshes the displayed task state manually', async () => {
    vi.useFakeTimers();
    const running: TaskSummary = { ...task(), observedState: 'RUNNING' };
    const readTasks = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [running] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const readProgress = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    const readRuntime = vi.spyOn(tasksApi, 'runtime').mockResolvedValue({
      taskId: running.id,
      run: null,
      operation: null,
      stages: [],
      units: [],
      retryable: false,
    });
    vi.spyOn(performanceApi, 'detail').mockRejectedValue(new Error('unavailable'));
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );
    await act(async () => {});
    fireEvent.click(screen.getByRole('checkbox', { name: '列表自动刷新' }));
    fireEvent.click(screen.getByRole('button', { name: '订单同步 详情' }));
    await act(async () => {});
    const reads = readProgress.mock.calls.length;
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(readProgress).toHaveBeenCalledTimes(reads);
    expect(readTasks).toHaveBeenCalledOnce();
    expect(readRuntime).toHaveBeenCalledOnce();
    readTasks.mockResolvedValue({
      tasks: [{ ...running, observedState: 'STOPPED', rowVersion: '3' }],
    });
    const dialog = screen.getByRole('dialog', { name: '任务详情' });
    fireEvent.click(within(dialog).getByRole('button', { name: '刷新运行信息' }));
    await act(async () => {});
    expect(within(dialog).getByRole('button', { name: '停止整个任务' })).toBeDisabled();
    expect(readProgress).toHaveBeenCalledTimes(reads + 1);
    expect(readRuntime).toHaveBeenCalledTimes(2);
    fireEvent.click(within(dialog).getByText('关闭', { selector: 'button[data-slot="button"]' }));
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(readProgress).toHaveBeenCalledTimes(reads + 2);
    expect(readRuntime).toHaveBeenCalledTimes(2);
  });

  it.each(['PG_KAFKA', 'KAFKA_KAFKA'] as const)(
    'saves two sources with two targets each and shared topic policy for %s',
    async (taskType) => {
      const pgSource: DataSourceSummary = {
        ...kafkaSource,
        configuration: {
          type: 'POSTGRES',
          database: 'data_demo',
          host: 'postgres',
          port: 5432,
          tls: { mode: 'REQUIRE' },
          username: 'reader',
        },
      };
      const source = taskType === 'PG_KAFKA' ? pgSource : kafkaSource;
      const existing = task();
      const firstSource =
        taskType === 'PG_KAFKA' ? { schema: 'public', name: 'orders' } : 'orders.source';
      const secondSource =
        taskType === 'PG_KAFKA' ? { schema: 'public', name: 'customers' } : 'customers.source';
      const configured: TaskSummary = {
        ...existing,
        taskType,
        draft: {
          ...existing.draft!,
          configuration: {
            schemaVersion: 2,
            kind: taskType === 'PG_KAFKA' ? 'PG_TO_KAFKA' : 'KAFKA_RAW_FORWARD',
            source: { dataSourceId: source.id, resourceMode: 'PLATFORM_MANAGED' },
            target: { dataSourceId: kafkaTarget.id },
            mappings: [{ source: firstSource, targets: [{ topic: 'orders.target' }] }],
          },
        },
      };
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [configured] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [source, kafkaTarget] });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (item, query) => ({
        ...metadata(item, 'TOPICS'),
        resource: query.resource,
      }));
      const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(configured);
      render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
      addTargetTopic('shared.audit');
      fireEvent.click(screen.getByRole('button', { name: '添加来源' }));
      if (taskType === 'PG_KAFKA') {
        fireEvent.change(screen.getByLabelText('源 Schema 2'), { target: { value: 'public' } });
        fireEvent.change(screen.getByLabelText('源表 2'), { target: { value: 'customers' } });
      } else
        fireEvent.change(screen.getByLabelText('源 Topic 2'), { target: { value: secondSource } });
      addTargetTopic('orders.target', '目标 Topic 2');
      addTargetTopic('shared.audit', '目标 Topic 2');
      fireEvent.click(screen.getByText('Topic 创建设置'));
      fireEvent.click(screen.getByLabelText('首次启动时创建缺失的 Topic（shared.audit）'));
      fireEvent.change(screen.getByLabelText('shared.audit分区数'), { target: { value: 2 } });
      fireEvent.change(screen.getByLabelText('shared.audit副本数'), { target: { value: 1 } });
      fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
      const targets = [
        { topic: 'orders.target' },
        { topic: 'shared.audit', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
      ];
      await waitFor(() =>
        expect(save).toHaveBeenCalledWith(
          configured,
          expect.objectContaining({
            schemaVersion: 2,
            mappings: [
              { source: firstSource, targets },
              { source: secondSource, targets },
            ],
          }),
          null,
        ),
      );
    },
  );

  it('blocks an empty mapping when advancing and allows deleting the last source', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    const removeSource = screen.getByLabelText('删除来源 1');
    expect(removeSource).toHaveRole('button');
    fireEvent.click(removeSource);
    fireEvent.click(screen.getByRole('button', { name: '下一步' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('至少添加一个来源');
    expect(screen.getByRole('button', { name: '2. 目标 Sink' })).toHaveAttribute(
      'aria-current',
      'step',
    );
    fireEvent.click(screen.getByRole('button', { name: '添加来源' }));
    fireEvent.change(screen.getByLabelText('源 Topic'), { target: { value: 'orders.source' } });
    fireEvent.click(screen.getByRole('button', { name: '下一步' }));
    expect(await screen.findByText('第 1 行请至少选择一个目标 Topic。')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '2. 目标 Sink' })).toHaveAttribute(
      'aria-current',
      'step',
    );
  });

  it('clears source mappings and target selections when their connections change in a new task', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    fireEvent.click(await screen.findByRole('button', { name: '新建任务' }));
    fireEvent.change(screen.getByLabelText('任务类型'), { target: { value: 'KAFKA_KAFKA' } });
    fireEvent.change(screen.getByLabelText('源数据源'), { target: { value: kafkaSource.id } });
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    fireEvent.change(screen.getByLabelText('目标 Kafka'), { target: { value: kafkaTarget.id } });
    fireEvent.change(screen.getByLabelText('源 Topic'), { target: { value: 'orders.source' } });
    addTargetTopic('orders.target');
    fireEvent.change(screen.getByLabelText('目标 Kafka'), { target: { value: kafkaSource.id } });
    expect(screen.getByLabelText('源 Topic')).toHaveValue('orders.source');
    expect(screen.queryByText('Topic 创建设置')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '上一步' }));
    fireEvent.change(screen.getByLabelText('源数据源'), { target: { value: kafkaTarget.id } });
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('源 Topic')).toHaveValue('');
    expect(screen.queryByText('Topic 创建设置')).not.toBeInTheDocument();
  });

  it('loads the applied artifact name when opening details without opening the editor first', async () => {
    const configured = task();
    const applied = { ...configured.draft!, artifactId: artifact.id, state: 'APPLIED' as const };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({
      tasks: [
        {
          ...configured,
          draft: null,
          applied,
          currentAppliedRevisionId: applied.id,
          observedState: 'RUNNING',
        },
      ],
    });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    fireEvent.click(await screen.findByRole('button', { name: '订单同步 详情' }));
    const dialog = screen.getByRole('dialog', { name: '任务详情' });
    expect(dialog).toHaveClass('task-detail-dialog', 'overflow-hidden');
    fireEvent.click(within(dialog).getByRole('tab', { name: '任务配置' }));
    expect(await within(dialog).findByText(artifact.versionIdentifier)).toBeInTheDocument();
    expect(within(dialog).getByText('当前已应用')).toBeInTheDocument();
    expect(within(dialog).queryByText('未绑定制品')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('任务名称')).not.toBeInTheDocument();
  });

  it('recovers the same blocked task after explicit old-instance confirmation without deleting or copying it', async () => {
    const blocked: TaskSummary = {
      ...task(),
      desiredState: 'RUNNING',
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [blocked] })
      .mockResolvedValue({ tasks: [{ ...blocked, observedState: 'RUNNING', rowVersion: '3' }] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const retry = vi.spyOn(recoveryApi, 'retryTask').mockResolvedValue({
      id: 'session',
      rowVersion: '1',
      state: 'READY',
      candidates: [],
      steps: [],
    });
    const discard = vi.spyOn(tasksApi, 'discardRecovery');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: '订单同步 恢复任务' }));
    expect(screen.getByRole('button', { name: '确认恢复任务' })).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已确认旧容器及该任务的旧进程已停止' }));
    fireEvent.click(screen.getByRole('button', { name: '确认恢复任务' }));
    await waitFor(() => expect(retry).toHaveBeenCalledWith(blocked));
    expect(discard).not.toHaveBeenCalled();
    await waitFor(() =>
      expect(screen.queryByRole('button', { name: '订单同步 恢复任务' })).not.toBeInTheDocument(),
    );
  });
  it('allows retrying a stopped manual-recovery task and handles the current-instance operation response', async () => {
    const blocked: TaskSummary = {
      ...task(),
      desiredState: 'STOPPED',
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [blocked] })
      .mockResolvedValue({
        tasks: [{ ...blocked, desiredState: 'RUNNING', observedState: 'RUNNING', rowVersion: '3' }],
      });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const retry = vi
      .spyOn(recoveryApi, 'retryTask')
      .mockResolvedValue({ operation: { ...startOperation, state: 'SUCCEEDED' } });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    const button = await screen.findByRole('button', { name: '订单同步 恢复任务' });
    expect(button).toBeEnabled();
    fireEvent.click(button);
    fireEvent.click(screen.getByRole('checkbox', { name: '我已确认旧容器及该任务的旧进程已停止' }));
    fireEvent.click(screen.getByRole('button', { name: '确认恢复任务' }));
    await waitFor(() => expect(retry).toHaveBeenCalledWith(blocked));
    expect(await screen.findByText('任务已恢复运行。')).toBeInTheDocument();
  });
  it('keeps a successfully discarded task non-actionable until a failed refresh is retried', async () => {
    const blocked: TaskSummary = {
      ...task(),
      desiredState: 'RUNNING',
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    };
    const list = vi
      .spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [blocked] })
      .mockRejectedValueOnce(new Error('network unavailable'));
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(tasksApi, 'discardRecovery').mockResolvedValue({ discarded: true });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('放弃恢复'));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已确认旧容器及该任务的旧进程已停止' }));
    fireEvent.click(screen.getByRole('button', { name: '确认放弃恢复' }));
    expect(await screen.findByText('已停止，待刷新')).toBeInTheDocument();
    expect(taskMenuAction('放弃恢复')).toHaveAttribute('aria-disabled', 'true');
    closeTaskMenu();
    expect(screen.getByRole('button', { name: '订单同步 详情' })).toBeDisabled();
    list.mockResolvedValue({
      tasks: [
        {
          ...blocked,
          desiredState: 'STOPPED',
          observedState: 'STOPPED',
          recoveryDisposition: 'NOT_APPLICABLE',
          rowVersion: '3',
        },
      ],
    });
    fireEvent.click(screen.getByRole('button', { name: '刷新任务列表' }));
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '订单同步 详情' })).not.toBeDisabled(),
    );
    expect(screen.queryByText('已停止，待刷新')).not.toBeInTheDocument();
  });

  it('ignores an expired discard response after the task page unmounts', async () => {
    const blocked: TaskSummary = {
      ...task(),
      desiredState: 'RUNNING',
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [blocked] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    let reject!: (error: Error) => void;
    const discard = vi.spyOn(tasksApi, 'discardRecovery').mockImplementation(
      () =>
        new Promise((_resolve, rejectPromise) => {
          reject = rejectPromise;
        }),
    );
    const invalidate = vi.fn();
    const { unmount } = render(
      <TaskManagementPage session={session} onSessionInvalidated={invalidate} />,
    );
    fireEvent.click(await findTaskMenuAction('放弃恢复'));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已确认旧容器及该任务的旧进程已停止' }));
    fireEvent.click(screen.getByRole('button', { name: '确认放弃恢复' }));
    await waitFor(() => expect(discard).toHaveBeenCalledOnce());
    unmount();
    reject(new PlatformApiError('SESSION_INVALID', 401));
    await discard.mock.results[0]?.value.catch(() => undefined);
    expect(invalidate).not.toHaveBeenCalled();
  });

  it('requires confirmation to discard a blocked old recovery and refreshes the task', async () => {
    const blocked: TaskSummary = {
      ...task(),
      desiredState: 'RUNNING',
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    };
    const list = vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [blocked] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    const discard = vi.spyOn(tasksApi, 'discardRecovery').mockResolvedValue({ discarded: true });
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('放弃恢复'));
    const confirm = screen.getByRole('button', { name: '确认放弃恢复' });
    expect(confirm).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已确认旧容器及该任务的旧进程已停止' }));
    list.mockResolvedValue({
      tasks: [
        {
          ...blocked,
          desiredState: 'STOPPED',
          observedState: 'STOPPED',
          recoveryDisposition: 'NOT_APPLICABLE',
          rowVersion: '3',
        },
      ],
    });
    fireEvent.click(confirm);
    await waitFor(() => expect(discard).toHaveBeenCalledExactlyOnceWith(blocked));
    expect(await screen.findByText('已放弃旧运行恢复，请重新预检后启动任务。')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.queryByRole('menuitem', { name: '订单同步 放弃恢复' })).not.toBeInTheDocument(),
    );
  });

  async function openPgEditor(
    mode = 'PLATFORM_MANAGED',
    role: 'ADMIN' | 'DEVELOPER' = 'ADMIN',
    invalidate = vi.fn(),
    observedState: TaskSummary['observedState'] = 'STOPPED',
    targetSettings: Record<string, unknown> = {},
  ) {
    const pgSource: DataSourceSummary = {
      ...kafkaSource,
      name: '测试 PG',
      configuration: {
        type: 'POSTGRES',
        database: 'data_demo',
        host: 'postgres',
        port: 5432,
        tls: { mode: 'REQUIRE' },
        username: 'reader',
      },
    };
    const base = task();
    const existing: TaskSummary = {
      ...base,
      observedState,
      taskType: 'PG_KAFKA',
      draft: {
        ...base.draft!,
        configuration: {
          kind: 'PG_TO_KAFKA',
          source: {
            dataSourceId: pgSource.id,
            resourceMode: mode,
            slotName: 'cdc_slot',
            publicationName: 'cdc_pub',
            table: { schema: 'public', name: 'samples' },
            includeSnapshot: false,
          },
          target: { dataSourceId: kafkaTarget.id, topic: 'samples', ...targetSettings },
        },
      },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [pgSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source, query) => ({
      ...metadata(source, 'TOPICS'),
      resource: query.resource,
      items: [],
    }));
    const view = render(
      <TaskManagementPage
        onSessionInvalidated={invalidate}
        session={{ ...session, user: { ...session.user, role } }}
      />,
    );
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    return { existing, ...view };
  }

  const pgPrepared = {
    taskId: '44444444-4444-4444-8444-444444444444',
    revisionId: '55555555-5555-4555-8555-555555555555',
    databaseName: 'data_demo',
    resourceMode: 'PLATFORM_MANAGED' as const,
    slot: { name: 'cdc_slot', created: true, stableIdentity: 'slot-identity' },
    publication: { name: 'cdc_pub', created: true, stableIdentity: 'publication-identity' },
    requiresPreflight: true as const,
  };

  function confirmPgPreparation() {
    const dialog = within(screen.getByRole('dialog', { name: '确认 PostgreSQL 复制资源' }));
    fireEvent.click(dialog.getByLabelText('我已核对资源名称，并理解起始位置与 WAL 保留风险。'));
    fireEvent.click(dialog.getByRole('button', { name: '确认复制资源' }));
  }

  it('keeps the source and PostgreSQL replication settings together in the first step', async () => {
    await openPgEditor();

    const firstStep = screen.getByRole('group', { name: '来源与 PostgreSQL 复制资源' });
    expect(within(firstStep).getByText('来源 Source')).toBeInTheDocument();
    expect(within(firstStep).getByText('PostgreSQL 复制资源')).toBeInTheDocument();
  });

  it('saves CloudCanal JSON settings and preserves them across wizard steps', async () => {
    const { existing } = await openPgEditor();
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('消息格式')).toHaveValue('');
    fireEvent.change(screen.getByLabelText('消息格式'), { target: { value: 'json' } });
    expect(screen.getByLabelText('JSON 模板')).toHaveValue('cloudcanal');
    fireEvent.change(screen.getByLabelText('字段定义（Avro）'), { target: { value: 'true' } });
    fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
    expect(screen.getByText('JSON / CloudCanal')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('消息格式')).toHaveValue('json');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        existing,
        expect.objectContaining({
          target: {
            dataSourceId: kafkaTarget.id,
            messageFormat: 'json',
            jsonTemplate: 'cloudcanal',
            withFieldDefs: true,
          },
        }),
        null,
      ),
    );
  });

  it.each([
    { messageFormat: 'avro', expected: { operation: 'update', schema: 'public', tb: 'samples' } },
    {
      messageFormat: 'json',
      jsonTemplate: 'standard',
      expected: { operation: 'update', schema: 'public', tb: 'samples' },
    },
    {
      messageFormat: 'json',
      jsonTemplate: 'cloudcanal',
      expected: { action: 'UPDATE', db: 'data_demo', schema: 'public', table: 'samples' },
    },
  ])(
    'shows a formatted $messageFormat / $jsonTemplate example in the sink and review steps',
    async (settings) => {
      const { expected, ...encoding } = settings;
      await openPgEditor('PLATFORM_MANAGED', 'ADMIN', vi.fn(), 'STOPPED', encoding);
      fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
      fireEvent.click(screen.getByText('消息编码'));
      const content = screen.getByLabelText('消息示例（JSON）').textContent!;
      const message = JSON.parse(content);
      expect(message).toMatchObject(expected);
      expect(content).toBe(JSON.stringify(message, null, 2));
      expect(
        within(screen.getByRole('region', { name: '消息示例' })).getByText('samples'),
      ).toBeInTheDocument();

      fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
      expect(screen.getByLabelText('预计消息内容（JSON）').textContent).toBe(content);
    },
  );

  it('updates the example when switching templates without changing the saved draft', async () => {
    await openPgEditor();
    const save = vi.spyOn(tasksApi, 'saveDraft');
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    fireEvent.click(screen.getByText('消息编码'));
    fireEvent.change(screen.getByLabelText('消息格式'), { target: { value: 'json' } });
    expect(JSON.parse(screen.getByLabelText('消息示例（JSON）').textContent!)).toHaveProperty(
      'action',
      'UPDATE',
    );
    fireEvent.change(screen.getByLabelText('JSON 模板'), { target: { value: 'standard' } });
    const message = JSON.parse(screen.getByLabelText('消息示例（JSON）').textContent!);
    expect(message).toHaveProperty('operation', 'update');
    expect(message).not.toHaveProperty('action');
    expect(save).not.toHaveBeenCalled();
  });

  it('loads JSON settings and removes the JSON template when switching to Avro', async () => {
    const { existing } = await openPgEditor('PLATFORM_MANAGED', 'ADMIN', vi.fn(), 'STOPPED', {
      messageFormat: 'json',
      jsonTemplate: 'cloudcanal',
      withFieldDefs: false,
    });
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    expect(screen.getByLabelText('消息格式')).toHaveValue('json');
    expect(screen.getByLabelText('字段定义（Avro）')).toHaveValue('false');
    fireEvent.change(screen.getByLabelText('消息格式'), { target: { value: 'avro' } });
    expect(screen.queryByLabelText('JSON 模板')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        existing,
        expect.objectContaining({
          target: {
            dataSourceId: kafkaTarget.id,
            messageFormat: 'avro',
            withFieldDefs: false,
          },
        }),
        null,
      ),
    );
  });

  it('does not add encoding defaults when saving an unrelated edit to a legacy task', async () => {
    const { existing } = await openPgEditor();
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    replaceTargetTopic('samples.next');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        existing,
        expect.objectContaining({
          target: { dataSourceId: kafkaTarget.id },
          mappings: [
            { source: { schema: 'public', name: 'samples' }, targets: [{ topic: 'samples.next' }] },
          ],
        }),
        null,
      ),
    );
  });

  it.each(['BLOCKED', 'FAILED'] as const)(
    'guides a %s task with invalid PG resources to a fresh copied task',
    async (observedState) => {
      await openPgEditor('PLATFORM_MANAGED', 'ADMIN', vi.fn(), observedState);

      expect(
        screen.getByText(
          '该任务已有失败运行记录，不能重新执行首次资源准备；请复制为新任务，并使用新的 Slot 和 Publication 名称。',
        ),
      ).toBeInTheDocument();
      expect(screen.queryByText('任务停止后可准备复制资源。')).not.toBeInTheDocument();
    },
  );

  it('prioritizes failed checks, collapses passed checks and prepares PG from its failed check', async () => {
    const { existing } = await openPgEditor('PLATFORM_MANAGED');
    const prepare = vi.spyOn(tasksApi, 'preparePgResources').mockResolvedValue(pgPrepared);
    const run: TaskPreflightRunSummary = {
      completedAt: null,
      failureCode: 'PREFLIGHT_FAILED',
      failureSummary: '复制资源未准备',
      id: '66666666-6666-4666-8666-666666666666',
      revisionId: existing.draft!.id,
      startedAt: '2026-09-29T00:00:00.000Z',
      state: 'FAILED',
      taskId: existing.id,
      result: {
        capacity: { available: 4, limit: 4, requested: 2, used: 0 },
        checks: [
          {
            key: 'SOURCE_CONNECTION',
            code: 'CONNECTED',
            state: 'PASSED',
            summary: '源库连接正常',
            details: {},
          },
          {
            key: 'PG_RESOURCES',
            code: 'PG_RESOURCES_MISSING',
            state: 'FAILED',
            summary: '复制资源未准备',
            details: {},
          },
        ],
        dataSourceVersions: { source: '1', target: '1' },
        identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
        taskRevision: { draftVersion: '1', taskRowVersion: '2' },
      },
    };
    vi.spyOn(tasksApi, 'preflight').mockResolvedValue(run);
    fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
    fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
    const failed = await screen.findByRole('list', { name: '未通过检查' });
    expect(failed).toHaveTextContent('PostgreSQL 资源');
    expect(failed).not.toHaveTextContent('源库连接正常');
    expect(screen.getByText('共 2 项，1 项未通过，1 项通过。')).toBeInTheDocument();
    const passedSummary = screen.getByText('查看通过及跳过的检查（1）');
    expect(passedSummary.closest('details')).not.toHaveAttribute('open');
    fireEvent.click(passedSummary);
    fireEvent.focus(screen.getByRole('button', { name: '源端连接检查说明' }));
    expect(await screen.findByRole('tooltip')).toHaveTextContent('源库连接正常');
    expect(screen.getByText('PG_RESOURCES_MISSING')).not.toBeVisible();
    fireEvent.click(within(failed).getByText('诊断代码'));
    expect(screen.getByText('PG_RESOURCES_MISSING')).toBeVisible();
    fireEvent.click(within(failed).getByRole('button', { name: '准备复制资源' }));
    expect(screen.getByRole('button', { name: '确认复制资源' })).toBeDisabled();
    expect(prepare).not.toHaveBeenCalled();
    confirmPgPreparation();
    await waitFor(() => expect(prepare).toHaveBeenCalledWith(existing));
  });

  it.each(['PLATFORM_MANAGED', 'SELF_MANAGED'])(
    'prepares saved PG resources in %s only after explicit risk acknowledgment then preflights',
    async (mode) => {
      const { existing } = await openPgEditor(mode);
      const prepare = vi.spyOn(tasksApi, 'preparePgResources').mockResolvedValue(pgPrepared);
      const run: TaskPreflightRunSummary = {
        completedAt: null,
        failureCode: 'PREFLIGHT_FAILED',
        failureSummary: '未绑定认证制品',
        id: '66666666-6666-4666-8666-666666666666',
        revisionId: existing.draft!.id,
        startedAt: '2026-09-29T00:00:00.000Z',
        state: 'FAILED',
        taskId: existing.id,
        result: {
          capacity: { available: 4, limit: 4, requested: 2, used: 0 },
          checks: [
            {
              key: 'ARTIFACT',
              code: 'ARTIFACT_NOT_AVAILABLE',
              state: 'FAILED',
              summary: '未绑定认证制品',
              details: {},
            },
          ],
          dataSourceVersions: { source: '1', target: '1' },
          identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
          taskRevision: { draftVersion: '1', taskRowVersion: '2' },
        },
      };
      const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(run);
      const start = vi.spyOn(tasksApi, 'start');
      fireEvent.click(
        screen.getByRole('button', {
          name: mode === 'SELF_MANAGED' ? '核验并绑定复制资源' : '准备复制资源',
        }),
      );
      expect(screen.getByRole('dialog')).toHaveTextContent('当前位置');
      expect(screen.getByRole('dialog')).toHaveTextContent('WAL');
      expect(screen.getByRole('button', { name: '确认复制资源' })).toBeDisabled();
      expect(prepare).not.toHaveBeenCalled();
      confirmPgPreparation();
      const message = await screen.findByText(
        'PostgreSQL 复制资源已核验并绑定，请继续处理预检阻断项。任务尚未启动。',
      );
      expect(message.parentElement).toContainElement(
        screen.getByRole('button', {
          name: mode === 'SELF_MANAGED' ? '核验并绑定复制资源' : '准备复制资源',
        }),
      );
      expect(prepare).toHaveBeenCalledWith(existing);
      expect(preflight).toHaveBeenCalledWith(existing);
      expect(start).not.toHaveBeenCalled();
      fireEvent.change(screen.getByLabelText('Slot 名称'), { target: { value: 'changed_slot' } });
      expect(
        screen.getByRole('button', {
          name: mode === 'SELF_MANAGED' ? '核验并绑定复制资源' : '准备复制资源',
        }),
      ).toBeDisabled();
    },
  );

  it('reports partial PG preparation failure without preflighting or starting', async () => {
    const scroll = vi.spyOn(HTMLElement.prototype, 'scrollIntoView');
    await openPgEditor();
    vi.spyOn(tasksApi, 'preparePgResources').mockRejectedValue(
      new PlatformApiError('PG_RESOURCE_PREPARATION_FAILED', 409),
    );
    const preflight = vi.spyOn(tasksApi, 'preflight');
    fireEvent.click(screen.getByRole('button', { name: '准备复制资源' }));
    confirmPgPreparation();
    const message = await screen.findByText(/发布可能已创建或扩展并保留，但新配置尚未应用/);
    expect(message.parentElement).toContainElement(
      screen.getByRole('button', { name: '准备复制资源' }),
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(scroll).not.toHaveBeenCalled();
    expect(preflight).not.toHaveBeenCalled();
    expect(screen.getByLabelText('Slot 名称')).toHaveValue('cdc_slot');
  });

  it('requires a new login when PG preparation encounters an expired session', async () => {
    const invalidate = vi.fn();
    await openPgEditor('PLATFORM_MANAGED', 'ADMIN', invalidate);
    const prepare = vi
      .spyOn(tasksApi, 'preparePgResources')
      .mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    fireEvent.click(screen.getByRole('button', { name: '准备复制资源' }));
    confirmPgPreparation();
    await waitFor(() => expect(prepare).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(1));
    expect(screen.queryByLabelText('当前密码')).not.toBeInTheDocument();
  });

  it.each([
    ['APE_DTS_MANAGED', 'ADMIN'],
    ['SELF_MANAGED', 'DEVELOPER'],
  ] as const)('does not expose PG preparation for %s as %s', async (mode, role) => {
    await openPgEditor(mode, role);
    expect(screen.queryByRole('button', { name: '准备复制资源' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '核验并绑定复制资源' })).not.toBeInTheDocument();
  });

  it('ignores a PG preparation response after the editor unmounts', async () => {
    const { unmount } = await openPgEditor();
    let resolve!: (value: typeof pgPrepared) => void;
    const prepare = vi.spyOn(tasksApi, 'preparePgResources').mockImplementation(
      () =>
        new Promise((done) => {
          resolve = done;
        }),
    );
    const preflight = vi.spyOn(tasksApi, 'preflight');
    fireEvent.click(screen.getByRole('button', { name: '准备复制资源' }));
    confirmPgPreparation();
    unmount();
    resolve(pgPrepared);
    await prepare.mock.results[0]?.value;
    expect(preflight).not.toHaveBeenCalled();
  });

  async function openTopicStep(role: 'ADMIN' | 'DEVELOPER' = 'ADMIN', invalidate = vi.fn()) {
    const existing = task();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const view = render(
      <TaskManagementPage
        onSessionInvalidated={invalidate}
        session={{ ...session, user: { ...session.user, role } }}
      />,
    );
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    return { existing, ...view };
  }

  function fillTopicParameters() {
    fireEvent.change(screen.getByLabelText('分区数'), { target: { value: '3' } });
    fireEvent.change(screen.getByLabelText('副本数'), { target: { value: '1' } });
  }

  it('keeps source and sink visible in the summary while preserving values between steps', async () => {
    await openTopicStep();
    expect(screen.queryByLabelText('消息格式')).not.toBeInTheDocument();
    replaceTargetTopic('new.topic');
    fireEvent.click(screen.getByRole('button', { name: '上一步' }));
    expect(screen.queryByLabelText('源 Topic')).not.toBeInTheDocument();
    expect(within(screen.getByLabelText('数据流摘要')).getByText('new.topic')).toBeInTheDocument();
    expect(screen.queryByLabelText('目标 Topic')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '下一步' }));
    expect(screen.getByLabelText('源 Topic')).toHaveValue('orders.source');
    expect(screen.getByLabelText('目标 Topic').closest('.ant-select')).toHaveTextContent(
      'new.topic',
    );
    fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
    expect(screen.getByText('使用已有 Topic')).toBeInTheDocument();
  });

  it('requires explicit creation parameters and persists auto creation only when enabled', async () => {
    const { existing } = await openTopicStep();
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    const create = vi.spyOn(dataSourcesApi, 'createTopic');
    expect(screen.getByLabelText('分区数')).toHaveValue(null);
    fireEvent.click(screen.getByLabelText('首次启动时创建缺失的 Topic'));
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    expect(screen.getByRole('alert')).toHaveTextContent('分区数必须是正整数');
    expect(save).not.toHaveBeenCalled();
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        existing,
        expect.objectContaining({
          target: { dataSourceId: kafkaTarget.id },
          mappings: [
            {
              source: 'orders.source',
              targets: [
                {
                  topic: 'orders.target',
                  topicCreation: { numPartitions: 3, replicationFactor: 1 },
                },
              ],
            },
          ],
        }),
        null,
      ),
    );
    expect(create).not.toHaveBeenCalled();
    await screen.findByText('任务草稿已保存。');
    fillTopicParameters();
    replaceTargetTopic('existing.topic');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenLastCalledWith(
        existing,
        expect.objectContaining({
          target: { dataSourceId: kafkaTarget.id },
          mappings: [{ source: 'orders.source', targets: [{ topic: 'existing.topic' }] }],
        }),
        null,
      ),
    );
  });

  it('creates only after confirmation and reports existing topics without changing them', async () => {
    await openTopicStep();
    const create = vi
      .spyOn(dataSourcesApi, 'createTopic')
      .mockResolvedValue({ topic: 'orders.target', created: false });
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '立即创建 Topic' }));
    expect(create).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog', { name: '确认创建 Topic' })).toHaveTextContent(
      '即使放弃任务草稿，Topic 仍会保留',
    );
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    await screen.findByText('Topic「orders.target」已存在，未修改分区或副本。');
    expect(create).toHaveBeenCalledWith(kafkaTarget, {
      topic: 'orders.target',
      numPartitions: 3,
      replicationFactor: 1,
    });
    expect(screen.getByLabelText('首次启动时创建缺失的 Topic')).not.toBeChecked();
  });

  it('reports creation failure and retains the entered parameters for a deliberate retry', async () => {
    await openTopicStep();
    vi.spyOn(dataSourcesApi, 'createTopic').mockRejectedValue(
      new PlatformApiError('KAFKA_CREATE_FAILED', 502),
    );
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '立即创建 Topic' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    await screen.findByText('创建 Topic 失败：KAFKA_CREATE_FAILED');
    expect(screen.getByLabelText('分区数')).toHaveValue(3);
    expect(screen.getByLabelText('目标 Topic').closest('.ant-select')).toHaveTextContent(
      'orders.target',
    );
  });

  it('reports completed topic mutation separately from an audit failure and refreshes metadata', async () => {
    await openTopicStep();
    const reads = vi.mocked(dataSourcesApi.metadata).mock.calls.length;
    vi.spyOn(dataSourcesApi, 'createTopic').mockRejectedValue(
      new PlatformApiError('KAFKA_TOPIC_AUDIT_FAILED', 503),
    );
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '立即创建 Topic' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    await screen.findByText(
      'Topic 操作已完成，但审计记录写入失败。请刷新目标 Topic 核实结果，勿重复创建。',
    );
    await waitFor(() =>
      expect(vi.mocked(dataSourcesApi.metadata).mock.calls.length).toBeGreaterThan(reads),
    );
  });

  it('returns to login rather than asking for a password when topic creation session expires', async () => {
    const invalidate = vi.fn();
    await openTopicStep('ADMIN', invalidate);
    const create = vi
      .spyOn(dataSourcesApi, 'createTopic')
      .mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '立即创建 Topic' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(1));
    expect(create).toHaveBeenCalledTimes(1);
    expect(screen.queryByLabelText('当前密码')).not.toBeInTheDocument();
  });

  it('does not expose manual creation to a regular user', async () => {
    await openTopicStep('DEVELOPER');
    expect(screen.queryByRole('button', { name: '立即创建 Topic' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('首次启动时创建缺失的 Topic'));
    expect(screen.getByLabelText('分区数')).toHaveValue(null);
  });

  it('discards a creation failure arriving after the editor unmounts', async () => {
    const invalidate = vi.fn();
    const { unmount } = await openTopicStep('ADMIN', invalidate);
    let reject!: (error: Error) => void;
    const create = vi.spyOn(dataSourcesApi, 'createTopic').mockImplementation(
      () =>
        new Promise((_, rejectPromise) => {
          reject = rejectPromise;
        }),
    );
    fillTopicParameters();
    fireEvent.click(screen.getByRole('button', { name: '立即创建 Topic' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    unmount();
    reject(new PlatformApiError('SESSION_INVALID', 401));
    await create.mock.results[0]?.value.catch(() => undefined);
    expect(invalidate).not.toHaveBeenCalled();
  });

  it('explains PostgreSQL replication ownership and preserves source choices across the wizard', async () => {
    const pgSource: DataSourceSummary = {
      ...kafkaSource,
      name: '测试 PG',
      configuration: {
        type: 'POSTGRES',
        database: 'data_demo',
        host: 'postgres',
        port: 5432,
        tls: { mode: 'REQUIRE' },
        username: 'reader',
      },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [pgSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source, query) => ({
      ...metadata(source, 'TOPICS'),
      resource: query.resource,
      items: [],
    }));
    const create = vi.spyOn(tasksApi, 'create').mockResolvedValue(task());
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    fireEvent.click(await screen.findByRole('button', { name: '新建任务' }));
    fireEvent.pointerMove(screen.getByRole('button', { name: '当前管理方式说明' }));
    fireEvent.focus(screen.getByRole('button', { name: '当前管理方式说明' }));
    expect(await screen.findByRole('tooltip')).toHaveTextContent('平台创建缺失的复制槽与发布');
    fireEvent.change(screen.getByLabelText('任务名称'), { target: { value: 'PG 样例' } });
    fireEvent.change(screen.getByLabelText('源数据源'), { target: { value: pgSource.id } });
    fireEvent.click(screen.getByLabelText('使用已有资源'));
    expect(screen.getByText(/平台不负责创建/)).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('首次运行包含初始快照'));
    fireEvent.click(screen.getByLabelText('引擎管理（高级）'));
    expect(screen.getByRole('alert')).toHaveTextContent('关闭初始快照');
    expect(screen.getByLabelText('使用已有资源')).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: '下一步' }));
    fireEvent.change(screen.getByLabelText('目标 Kafka'), { target: { value: kafkaTarget.id } });
    fireEvent.change(screen.getByLabelText('源 Schema'), { target: { value: 'public' } });
    fireEvent.change(screen.getByLabelText('源表'), { target: { value: 'cdc_samples' } });
    replaceTargetTopic('pg.samples');
    fireEvent.click(screen.getByRole('button', { name: '4. 确认与预检' }));
    expect(
      within(screen.getByLabelText('数据流摘要')).getByText('public.cdc_samples'),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '创建并保存草稿' }));
    await waitFor(() =>
      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({
          configuration: expect.objectContaining({
            source: expect.objectContaining({
              includeSnapshot: true,
              resourceMode: 'SELF_MANAGED',
            }),
            target: { dataSourceId: kafkaTarget.id },
            mappings: [
              {
                source: { schema: 'public', name: 'cdc_samples' },
                targets: [{ topic: 'pg.samples' }],
              },
            ],
          }),
        }),
      ),
    );
  });

  it.each(['RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED', 'RUNTIME_TOPIC_PREPARATION_FAILED'])(
    'handles confirmed pending topic preparation outcome %s without starting automatically',
    async (code) => {
      const base = task();
      const existing = {
        ...base,
        draft: {
          ...base.draft!,
          configuration: {
            ...base.draft!.configuration,
            target: {
              dataSourceId: kafkaTarget.id,
              topic: 'orders.target',
              topicCreation: { numPartitions: 3, replicationFactor: 1 },
            },
          },
        },
      };
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
        metadata(source, 'TOPICS'),
      );
      const pending: TaskPreflightRunSummary = {
        completedAt: '2026-09-29T00:00:01.000Z',
        failureCode: 'PREFLIGHT_FAILED',
        failureSummary: '需要创建 Topic',
        id: '66666666-6666-4666-8666-666666666666',
        revisionId: existing.draft.id,
        startedAt: '2026-09-29T00:00:00.000Z',
        state: 'FAILED',
        taskId: existing.id,
        result: {
          capacity: { available: 4, limit: 4, requested: 2, used: 0 },
          checks: [
            {
              code: 'TARGET_TOPIC_CREATION_PENDING',
              details: {},
              key: 'TOPIC_IDENTITIES',
              state: 'FAILED',
              summary: '需要创建 Topic',
            },
          ],
          dataSourceVersions: { source: '1', target: '1' },
          identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
          taskRevision: { draftVersion: '1', taskRowVersion: '2' },
        },
      };
      const preflight = vi
        .spyOn(tasksApi, 'preflight')
        .mockResolvedValueOnce(pending)
        .mockResolvedValue({
          ...pending,
          state: 'PASSED',
          result: { ...pending.result, checks: [] },
        });
      const start = vi.spyOn(tasksApi, 'start').mockRejectedValue(new PlatformApiError(code, 409));
      const accept = vi.spyOn(tasksApi, 'acceptRisk');
      render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
      fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
      fireEvent.click(await screen.findByRole('button', { name: '准备 Topic 并重新预检' }));
      expect(start).not.toHaveBeenCalled();
      const reads = vi.mocked(dataSourcesApi.metadata).mock.calls.length;
      fireEvent.click(screen.getByRole('button', { name: '确认准备' }));
      if (code === 'RUNTIME_TOPIC_PREPARATION_FAILED') {
        await screen.findByText('Topic 准备未完成，Topic 可能已创建；请刷新核实，任务未启动。');
        await waitFor(() =>
          expect(vi.mocked(dataSourcesApi.metadata).mock.calls.length).toBeGreaterThan(reads),
        );
        expect(preflight).toHaveBeenCalledTimes(1);
        expect(screen.queryByText('预检已通过。任务仍保持停止状态。')).not.toBeInTheDocument();
      } else {
        await screen.findByText('预检已通过。任务仍保持停止状态。');
        expect(preflight).toHaveBeenCalledTimes(2);
        expect(
          screen.queryByRole('button', { name: '准备 Topic 并重新预检' }),
        ).not.toBeInTheDocument();
      }
      expect(start).toHaveBeenCalledTimes(1);
      expect(accept).not.toHaveBeenCalled();
    },
  );

  it('keeps a pending deletion confirmation open when Escape is pressed', async () => {
    const existing = task();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    let rejectDeletion!: (reason: Error) => void;
    vi.spyOn(tasksApi, 'remove').mockImplementation(
      () =>
        new Promise((_, reject) => {
          rejectDeletion = reject;
        }),
    );
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '删除任务' }));
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));
    const dialog = screen.getByRole('dialog', { name: '确认删除任务' });
    expect(within(dialog).getByRole('button', { name: '取消' })).toBeDisabled();
    expect(within(dialog).queryByRole('button', { name: '关闭' })).not.toBeInTheDocument();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    expect(screen.getByRole('dialog', { name: '确认删除任务' })).toBeInTheDocument();
    rejectDeletion(new Error('删除失败'));
    await waitFor(() => expect(within(dialog).getByRole('button', { name: '取消' })).toBeEnabled());
    fireEvent.click(within(dialog).getByRole('button', { name: '取消' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(screen.getByText('删除失败')).toBeInTheDocument();
  });

  it('filters the task list and opens read-only details from the row action', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    fireEvent.click(await screen.findByRole('button', { name: '订单同步 详情' }));
    const dialog = screen.getByRole('dialog', { name: '任务详情' });
    expect(within(dialog).getByText('订单同步')).toBeInTheDocument();
    expect(
      within(within(dialog).getByLabelText('同步链路')).getByText('源 Kafka', { exact: false }),
    ).toBeInTheDocument();
    expect(within(dialog).getByRole('tab', { name: '运行监控' })).toHaveAttribute(
      'aria-selected',
      'true',
    );
    expect(within(dialog).getByRole('button', { name: '刷新运行信息' })).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole('tab', { name: '任务配置' }));
    expect(within(dialog).getByText('2026年09月18日 16:00:00')).toBeInTheDocument();
    expect(parseFloat(window.getComputedStyle(dialog).maxHeight)).toBeCloseTo(
      window.innerHeight * 0.9,
    );
    expect(within(dialog).queryByText('草稿版本')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('任务名称')).not.toBeInTheDocument();
    fireEvent.keyDown(dialog, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    fireEvent.change(screen.getByLabelText('筛选任务'), { target: { value: '不存在' } });
    expect(screen.getByText('没有匹配的任务')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '订单同步 详情' })).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('筛选任务'), { target: { value: '订单' } });
    expect(screen.getByRole('button', { name: '订单同步 详情' })).toBeInTheDocument();
  });

  it('focuses the detail heading without opening help until hover or keyboard focus', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );
    fireEvent.click(await screen.findByRole('button', { name: '订单同步 详情' }));
    const dialog = screen.getByRole('dialog', { name: '任务详情' });
    const heading = within(dialog).getByRole('heading', { name: '任务详情' });
    await waitFor(() => expect(heading).toHaveFocus());
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();

    const help = within(dialog).getByRole('button', { name: '运行监控说明' });
    fireEvent.mouseEnter(help);
    expect(await screen.findByRole('tooltip')).toHaveTextContent('指标最长保留 7 天');
    fireEvent.mouseLeave(help);
    await waitFor(() => expect(screen.queryByRole('tooltip')).not.toBeInTheDocument());
    fireEvent.focus(help);
    expect(await screen.findByRole('tooltip')).toHaveTextContent('指标最长保留 7 天');
  });

  it('colors runtime states and omits throughput details for tasks that are not running', async () => {
    const states = ['STOPPED', 'STARTING', 'RUNNING', 'STOPPING', 'FAILED', 'BLOCKED'] as const;
    vi.spyOn(tasksApi, 'list').mockResolvedValue({
      tasks: states.map((observedState, index) => ({
        ...task(),
        id: `44444444-4444-4444-8444-44444444444${index}`,
        name: `${observedState} task`,
        observedState,
      })),
    });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(platformApi.performanceApi, 'query').mockResolvedValue({ items: [] });

    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);

    expect(await screen.findByTitle('RUNNING')).toHaveClass('bg-emerald-50', 'text-emerald-700');
    expect(screen.getByTitle('STARTING')).toHaveClass('bg-sky-50', 'text-sky-700');
    expect(screen.getByTitle('STOPPING')).toHaveClass('bg-amber-50', 'text-amber-800');
    expect(screen.getByTitle('STOPPED')).toHaveClass('bg-slate-100', 'text-slate-700');
    expect(screen.getByTitle('FAILED')).toHaveClass('bg-red-50', 'text-red-700');
    expect(screen.getByTitle('BLOCKED')).toHaveClass('bg-orange-50', 'text-orange-800');
    expect(screen.queryByLabelText('任务未运行，无消费进度')).not.toBeInTheDocument();
    expect(screen.queryByText('— 条/秒')).not.toBeInTheDocument();
    expect(screen.queryByText('— B/秒')).not.toBeInTheDocument();
  });

  it('does not show a draft version badge in the task editor', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);

    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));

    expect(screen.queryByText(/待预检\s*·\s*v1/u)).not.toBeInTheDocument();
  });

  it('keeps topics editable when metadata cannot be read', async () => {
    const existing = task();
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockRejectedValue(new Error('metadata unavailable'));
    const save = vi.spyOn(tasksApi, 'saveDraft').mockResolvedValue(existing);
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    await screen.findAllByText('元数据读取失败，可手动填写。');
    replaceTargetTopic('manual.topic');
    fireEvent.click(screen.getByRole('button', { name: '保存草稿' }));
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(
        existing,
        expect.objectContaining({
          target: { dataSourceId: kafkaTarget.id },
          mappings: [{ source: 'orders.source', targets: [{ topic: 'manual.topic' }] }],
        }),
        null,
      ),
    );
  });

  it('edits in a full-page region and asks before discarding an unsaved draft', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [task()] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    const editor = screen.getByRole('region', { name: '任务配置' });
    expect(within(editor).getByLabelText('任务名称')).toHaveValue('订单同步');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.queryByRole('list', { name: '任务列表' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    replaceTargetTopic('orders.changed');
    fireEvent.click(screen.getByRole('button', { name: '返回任务列表' }));
    expect(await screen.findByText('放弃未保存的任务配置？')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(screen.getByLabelText('目标 Topic').closest('.ant-select')).toHaveTextContent(
      'orders.changed',
    );
    fireEvent.click(screen.getByRole('button', { name: '返回任务列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '放弃修改并继续' }));
    expect(await screen.findByRole('button', { name: '订单同步 详情' })).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: '任务配置' })).not.toBeInTheDocument();
  }, 15000);

  it('saves the selected artifact and sends null when the administrator clears the binding', async () => {
    const existing = task();
    const bound = { ...existing, draft: { ...existing.draft!, artifactId: artifact.id } };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const save = vi
      .spyOn(tasksApi, 'saveDraft')
      .mockResolvedValueOnce(bound)
      .mockResolvedValue(existing);
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    await waitFor(() => expect(platformApi.listArtifacts).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    fireEvent.change(screen.getByRole('combobox', { name: '运行制品' }), {
      target: { value: artifact.id },
    });
    fireEvent.click(screen.getByText('保存草稿').closest('button')!);
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith(existing, expect.any(Object), artifact.id),
    );
    expect(await screen.findByText('任务草稿已保存。')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    fireEvent.change(screen.getByRole('combobox', { name: '运行制品' }), { target: { value: '' } });
    fireEvent.click(screen.getByText('保存草稿').closest('button')!);
    await waitFor(() => expect(save).toHaveBeenLastCalledWith(bound, expect.any(Object), null));
  });
  it('persists an incomplete server draft after the task endpoints are selected', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const create = vi.spyOn(tasksApi, 'create').mockResolvedValue(task('INCOMPLETE_DRAFT'));

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    await screen.findByText('暂无任务');
    fireEvent.click(screen.getByRole('button', { name: '新建任务' }));
    fireEvent.change(screen.getByLabelText('任务名称'), { target: { value: '未完成任务' } });
    fireEvent.change(screen.getByRole('combobox', { name: '任务类型' }), {
      target: { value: 'KAFKA_KAFKA' },
    });
    fireEvent.change(screen.getByRole('combobox', { name: '源数据源' }), {
      target: { value: kafkaSource.id },
    });
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    fireEvent.change(screen.getByRole('combobox', { name: '目标 Kafka' }), {
      target: { value: kafkaTarget.id },
    });
    fireEvent.click(screen.getByText('创建并保存草稿').closest('button')!);

    await waitFor(() => expect(create).toHaveBeenCalledTimes(1));
    expect(create.mock.calls[0]?.[0].configuration).toMatchObject({
      kind: 'KAFKA_RAW_FORWARD',
      source: { dataSourceId: kafkaSource.id },
      target: { dataSourceId: kafkaTarget.id },
    });
    expect(create.mock.calls[0]?.[0].configuration).not.toMatchObject({
      source: { topic: expect.anything() },
    });
  });

  it('creates a Kafka task without asking the user to manufacture a Group ID', async () => {
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const create = vi.spyOn(tasksApi, 'create').mockResolvedValue(task());

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    await screen.findByText('暂无任务');
    fireEvent.click(screen.getByRole('button', { name: '新建任务' }));
    fireEvent.change(screen.getByLabelText('任务名称'), { target: { value: '订单同步' } });

    fireEvent.change(screen.getByRole('combobox', { name: '任务类型' }), {
      target: { value: 'KAFKA_KAFKA' },
    });
    fireEvent.change(screen.getByRole('combobox', { name: '源数据源' }), {
      target: { value: kafkaSource.id },
    });
    fireEvent.click(screen.getByRole('button', { name: '2. 目标 Sink' }));
    fireEvent.change(screen.getByRole('combobox', { name: '目标 Kafka' }), {
      target: { value: kafkaTarget.id },
    });

    fireEvent.change(screen.getByLabelText('源 Topic'), { target: { value: 'orders.source' } });
    replaceTargetTopic('orders.target');
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    await screen.findByRole('option', { name: 'ape-dts-test' });
    fireEvent.click(screen.getByRole('button', { name: '3. 运行配置' }));
    fireEvent.change(screen.getByRole('combobox', { name: '运行制品' }), {
      target: { value: artifact.id },
    });
    fireEvent.click(screen.getByText('创建并保存草稿').closest('button')!);

    await waitFor(() => expect(create).toHaveBeenCalledTimes(1));
    const request = create.mock.calls[0]?.[0];
    expect(request).toMatchObject({
      artifactId: artifact.id,
      name: '订单同步',
      sourceDataSourceId: kafkaSource.id,
      targetDataSourceId: kafkaTarget.id,
      taskType: 'KAFKA_KAFKA',
    });
    expect(request?.configuration).toMatchObject({
      kind: 'KAFKA_RAW_FORWARD',
      source: { dataSourceId: kafkaSource.id },
      target: { dataSourceId: kafkaTarget.id },
      mappings: [{ source: 'orders.source', targets: [{ topic: 'orders.target' }] }],
    });
    expect((request?.configuration.source as Record<string, unknown>).groupId).toBeUndefined();
    expect(await screen.findByText('任务已创建并保存为服务端草稿。')).toBeInTheDocument();
  });

  it.each(['FAILED', 'STALE'] as const)(
    'renders the saved revision preflight result in %s state',
    async (state) => {
      const existing = task();
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
        metadata(source, 'TOPICS'),
      );
      const result: TaskPreflightRunSummary = {
        completedAt: '2026-09-18T08:01:00.000Z',
        failureCode: 'PREFLIGHT_FAILED',
        failureSummary: '制品尚未激活',
        id: '66666666-6666-4666-8666-666666666666',
        result: {
          capacity: { available: 4, limit: 4, requested: 2, used: 0 },
          checks: [
            {
              code: 'ARTIFACT_NOT_AVAILABLE',
              details: {},
              key: 'ARTIFACT',
              state: 'FAILED',
              summary: '没有适用于当前任务场景的已激活制品。',
            },
          ],
          dataSourceVersions: { source: '1', target: '1' },
          identities: {
            source: 'source-cluster',
            sourceTopic: 'source-topic-id',
            target: 'target-cluster',
            targetTopic: 'target-topic-id',
          },
          taskRevision: { draftVersion: '1', taskRowVersion: '2' },
        },
        revisionId: existing.draft!.id,
        startedAt: '2026-09-18T08:00:30.000Z',
        state,
        taskId: existing.id,
      };
      const preflight = vi.spyOn(tasksApi, 'preflight').mockResolvedValue(result);

      render(
        <ConfigProvider theme={{ token: { motion: false } }}>
          <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
        </ConfigProvider>,
      );

      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      const button = (await screen.findByText('运行预检')).closest('button')!;
      expect(button).toBeEnabled();
      fireEvent.click(button);

      await waitFor(() => expect(preflight).toHaveBeenCalledWith(existing));
      const resultCard = await screen.findByText('最近一次预检');
      expect(
        within(resultCard.closest('[aria-label="最近一次预检"]')!).getByText('运行制品'),
      ).toBeInTheDocument();
      expect(screen.getByText(/没有适用于当前任务场景的已激活制品/)).toBeInTheDocument();
      expect(await screen.findByText('预检未通过，请处理阻断项后重试。')).toBeInTheDocument();
      if (state === 'STALE') {
        expect(screen.getByText('已过期')).toBeInTheDocument();
        expect(screen.getByText('预检结果已过期，请重新预检。')).toBeInTheDocument();
        expect(screen.queryByText('检查中')).not.toBeInTheDocument();
      }
    },
  );

  it.each(['environment', 'incomplete', 'stale'])(
    'blocks Topic preparation for %s preflight',
    async (reason) => {
      const base = task();
      const existing = {
        ...base,
        draft: {
          ...base.draft!,
          configuration: {
            ...base.draft!.configuration,
            target: {
              dataSourceId: kafkaTarget.id,
              topic: 'orders.target',
              topicCreation: { numPartitions: 1, replicationFactor: 1 },
            },
          },
        },
      };
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
        metadata(source, 'TOPICS'),
      );
      vi.spyOn(tasksApi, 'preflight').mockResolvedValue({
        completedAt: reason === 'incomplete' ? null : '2026-09-29T00:00:01.000Z',
        failureCode: 'PREFLIGHT_FAILED',
        failureSummary: '运行环境不可用',
        id: '66666666-6666-4666-8666-666666666666',
        revisionId: existing.draft.id,
        startedAt: '2026-09-29T00:00:00.000Z',
        state: 'FAILED',
        taskId: existing.id,
        result: {
          capacity: { available: 4, limit: 4, requested: 2, used: 0 },
          checks: [
            {
              key: 'TOPIC_IDENTITIES',
              code: 'TARGET_TOPIC_CREATION_PENDING',
              state: 'FAILED',
              summary: 'Topic 待创建',
              details: {},
            },
            ...(reason === 'environment'
              ? [
                  {
                    key: 'RUNTIME_ENVIRONMENT' as const,
                    code: 'RUNTIME_CGROUP_V2_REQUIRED',
                    state: 'FAILED' as const,
                    summary: '运行环境不可用',
                    details: {},
                  },
                ]
              : []),
          ],
          dataSourceVersions: { source: '1', target: '1' },
          identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
          taskRevision: { draftVersion: reason === 'stale' ? '0' : '1', taskRowVersion: '2' },
        },
      });
      const start = vi.spyOn(tasksApi, 'start');
      render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      fireEvent.click(screen.getByRole('button', { name: '运行预检' }));
      const prepare = await screen.findByRole('button', { name: '准备 Topic 并重新预检' });
      expect(prepare).toBeDisabled();
      expect(
        screen.getByText(
          reason === 'environment'
            ? '请先处理其他未通过的检查，再准备 Topic。'
            : '预检结果不完整或已过期，请重新预检。',
        ),
      ).toBeInTheDocument();
      fireEvent.click(prepare);
      expect(start).not.toHaveBeenCalled();
      expect(screen.queryByRole('button', { name: '确认准备' })).not.toBeInTheDocument();
    },
  );

  it('copies, edits and saves a task without requiring crypto.randomUUID or starting a runtime', async () => {
    const existing = task('PREFLIGHT_PASSED');
    vi.stubGlobal('crypto', {
      getRandomValues: globalThis.crypto.getRandomValues.bind(globalThis.crypto),
    });
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const copied = { ...task(), id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', name: '复制后修改' };
    const transport = vi.fn(async () => new Response(JSON.stringify(copied)));
    vi.stubGlobal('fetch', transport);
    const start = vi.spyOn(tasksApi, 'start');
    const stop = vi.spyOn(tasksApi, 'stop');
    render(<TaskManagementPage session={session} onSessionInvalidated={vi.fn()} />);
    fireEvent.click(await findTaskMenuAction('复制'));
    fireEvent.change(screen.getByLabelText('任务名称'), { target: { value: copied.name } });
    fireEvent.click(screen.getByRole('button', { name: '创建并保存草稿' }));
    await screen.findByText('任务已创建并保存为服务端草稿。');
    expect(
      transport.mock.calls.filter((call) => {
        const [url, init] = call as unknown as [string, RequestInit];
        return url === '/api/tasks' && init.method === 'POST';
      }),
    ).toHaveLength(1);
    expect(transport).toHaveBeenCalledWith(
      '/api/tasks',
      expect.objectContaining({ method: 'POST' }),
    );
    expect(screen.getByLabelText('任务名称')).toHaveValue(copied.name);
    expect(start).not.toHaveBeenCalled();
    expect(stop).not.toHaveBeenCalled();
    expect(screen.queryByText('crypto.randomUUID is not a function')).not.toBeInTheDocument();
  });

  it('copies only editable configuration into a new task draft', async () => {
    const existing = task('PREFLIGHT_PASSED');
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    fireEvent.click(await findTaskMenuAction('复制'));

    expect(screen.getByLabelText('任务名称')).toHaveValue('订单同步 副本');
    expect(screen.getByLabelText('任务名称')).toBeEnabled();
    expect(
      screen.getByText('已复制任务配置；预检、风险确认和运行状态不会复制。'),
    ).toBeInTheDocument();
    expect(screen.getByText('创建并保存草稿').closest('button')).toBeEnabled();
  });

  it('deletes an eligible task directly from the task list action column', async () => {
    const existing = task();
    const deleted = {
      ...existing,
      deletedAt: '2026-09-18T08:04:00.000Z',
      desiredState: 'DELETED' as const,
      observedState: 'DELETED' as const,
      rowVersion: '3',
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    const remove = vi.spyOn(tasksApi, 'remove').mockResolvedValue(deleted);

    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);

    fireEvent.click(await findTaskMenuAction('删除'));
    expect(screen.queryByRole('region', { name: '任务配置' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));

    await waitFor(() => expect(remove).toHaveBeenCalledWith(existing));
    expect(await screen.findByText('暂无任务')).toBeInTheDocument();
  });

  it('keeps the row delete action visible but disabled while a task is running', async () => {
    const running = {
      ...task('RISK_CONFIRMED'),
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [running] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });

    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);

    expect(await findTaskMenuAction('删除')).toHaveAttribute('aria-disabled', 'true');
  });

  it('deletes a stopped task and selects the next available task', async () => {
    const existing = task();
    const siblingBase = task();
    const sibling = {
      ...siblingBase,
      draft: {
        ...siblingBase.draft!,
        id: '66666666-6666-4666-8666-666666666666',
      },
      id: '77777777-7777-4777-8777-777777777777',
      name: '库存同步',
    };
    const deleted = {
      ...existing,
      deletedAt: '2026-09-18T08:04:00.000Z',
      desiredState: 'DELETED' as const,
      observedState: 'DELETED' as const,
      rowVersion: '3',
    };
    const list = vi
      .spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing, sibling] })
      .mockResolvedValue({ tasks: [sibling] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const remove = vi.spyOn(tasksApi, 'remove').mockResolvedValue(deleted);

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(await screen.findByRole('button', { name: /删除任务/ }));
    const description = await screen.findByText(/将删除任务“订单同步”/);
    const dialog = description.closest<HTMLElement>('[role="dialog"]')!;
    expect(remove).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole('button', { name: '确认删除' }));

    await waitFor(() => expect(remove).toHaveBeenCalledWith(existing));
    expect(list).toHaveBeenCalledTimes(2);
    expect(
      await screen.findByText('任务已删除。历史修订、操作记录和审计记录仍由系统保留。'),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText('任务名称')).not.toBeInTheDocument();
    expect(screen.getByText('库存同步')).toBeInTheDocument();
  });

  it('does not offer deletion while a task is running', async () => {
    const running = {
      ...task('RISK_CONFIRMED'),
      desiredState: 'RUNNING' as const,
      observedState: 'RUNNING' as const,
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [running] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    await screen.findByDisplayValue('订单同步');
    expect(screen.queryByRole('button', { name: /删除任务/ })).not.toBeInTheDocument();
  });

  it.each(['FAILED', 'BLOCKED'] as const)(
    'offers deletion when a stopped task is observed as %s',
    async (observedState) => {
      const stopped = { ...task(), observedState };
      vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [stopped] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
        metadata(source, 'TOPICS'),
      );

      render(
        <ConfigProvider theme={{ token: { motion: false } }}>
          <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
        </ConfigProvider>,
      );

      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      expect(await screen.findByRole('button', { name: /删除任务/ })).toBeInTheDocument();
    },
  );

  it('keeps the deletion result when refreshing the task list fails', async () => {
    const existing = task();
    const deleted = {
      ...existing,
      deletedAt: '2026-09-18T08:04:00.000Z',
      desiredState: 'DELETED' as const,
      observedState: 'DELETED' as const,
      rowVersion: '3',
    };
    vi.spyOn(tasksApi, 'list')
      .mockResolvedValueOnce({ tasks: [existing] })
      .mockRejectedValueOnce(new Error('network unavailable'));
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const remove = vi.spyOn(tasksApi, 'remove').mockResolvedValue(deleted);

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
      </ConfigProvider>,
    );

    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(await screen.findByRole('button', { name: /删除任务/ }));
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));

    await waitFor(() => expect(remove).toHaveBeenCalledOnce());
    expect(
      await screen.findByText('任务已删除。历史修订、操作记录和审计记录仍由系统保留。'),
    ).toBeInTheDocument();
    expect(
      screen.getByText('任务已删除，但刷新任务列表失败：network unavailable'),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText('任务名称')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /删除任务/ })).not.toBeInTheDocument();
  });

  it.each([
    ['KAFKA_IDENTITY_MISSING', '缺少 Kafka 集群身份信息，请检查连接并重新预检'],
    ['DATA_SOURCE_IDENTITY_CHANGED', '数据源集群身份与预检记录不一致，请核对连接并重新预检'],
    ['KAFKA_PARTITION_BASELINE_MISSING', '缺少有效的来源 Topic 分区信息，请重新预检'],
    ['KAFKA_PARTITION_SET_CHANGED', '来源 Topic 分区与预检记录不一致，请重新预检'],
    [null, '外部资源绑定与预检记录不一致，请核对资源并重新预检'],
    ['UNKNOWN_REASON', '外部资源绑定与预检记录不一致，请核对资源并重新预检'],
  ])('explains a rejected start without retrying: %s', async (reason, message) => {
    const existing = task('PREFLIGHT_PASSED');
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafkaSource, kafkaTarget] });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const error = new PlatformApiError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', 409);
    error.reason = reason;
    const start = vi.spyOn(tasksApi, 'start').mockRejectedValue(error);
    render(<TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />);
    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(await screen.findByRole('button', { name: '启动任务' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(message!);
    expect(start).toHaveBeenCalledTimes(1);
  });

  it('requires login on session expiry without retrying task deletion', async () => {
    const existing = task();
    const developerSession: AuthenticatedSessionResponse = {
      ...session,
      user: { ...session.user, role: 'DEVELOPER' },
    };
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [existing] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [kafkaSource, kafkaTarget],
    });
    vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
      metadata(source, 'TOPICS'),
    );
    const remove = vi
      .spyOn(tasksApi, 'remove')
      .mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    const invalidate = vi.fn();

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <TaskManagementPage onSessionInvalidated={invalidate} session={developerSession} />
      </ConfigProvider>,
    );

    openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
    fireEvent.click(await screen.findByRole('button', { name: /删除任务/ }));
    fireEvent.click(screen.getByRole('button', { name: '确认删除' }));

    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(1));
    expect(remove).toHaveBeenCalledTimes(1);
    expect(screen.queryByLabelText('当前密码')).not.toBeInTheDocument();
  });

  it.each([
    ['PREFLIGHT_PASSED', 'EXECUTING'],
    ['RISK_CONFIRMED', 'EXECUTING'],
    ['PREFLIGHT_PASSED', 'BLOCKED_MANUAL'],
  ] as const)(
    'starts %s directly and reports %s without risk or launch dialogs',
    async (state, operationState) => {
      const existing = task(state);
      const startingTask = {
        ...existing,
        desiredState: 'RUNNING' as const,
        observedState: 'STARTING' as const,
      };
      vi.spyOn(tasksApi, 'list')
        .mockResolvedValueOnce({ tasks: [existing] })
        .mockResolvedValue({ tasks: [startingTask] });
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
        dataSources: [kafkaSource, kafkaTarget],
      });
      vi.spyOn(dataSourcesApi, 'metadata').mockImplementation(async (source) =>
        metadata(source, 'TOPICS'),
      );
      const start = vi.spyOn(tasksApi, 'start').mockResolvedValue({
        action: 'START',
        completedAt: null,
        createdAt: '2026-09-18T08:03:00.000Z',
        failureCode: operationState === 'BLOCKED_MANUAL' ? 'RUNTIME_CGROUP_V2_REQUIRED' : null,
        failureSummary: null,
        id: '88888888-8888-4888-8888-888888888888',
        runId: '99999999-9999-4999-8999-999999999999',
        state: operationState,
        taskId: existing.id,
        updatedAt: '2026-09-18T08:03:00.000Z',
      });

      render(
        <ConfigProvider theme={{ token: { motion: false } }}>
          <TaskManagementPage onSessionInvalidated={vi.fn()} session={session} />
        </ConfigProvider>,
      );

      openTaskEditor(await screen.findByRole('button', { name: '订单同步 详情' }));
      fireEvent.click(await screen.findByRole('button', { name: '启动任务' }));
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();

      await waitFor(() => expect(start).toHaveBeenCalledWith(existing));
      expect(start).toHaveBeenCalledTimes(1);
      if (operationState === 'BLOCKED_MANUAL') {
        expect(await screen.findByRole('alert')).toHaveTextContent('启动被阻断');
        expect(screen.getByRole('alert')).toHaveTextContent('旧部署模式的资源隔离错误');
        expect(screen.getByRole('alert')).not.toHaveTextContent('核对 cgroup v2 委派');
        expect(screen.queryByText(/启动操作已受理/)).not.toBeInTheDocument();
      } else {
        expect(
          await screen.findByText('启动操作已受理：EXECUTING。可在任务详情中查看进度。'),
        ).toBeInTheDocument();
      }
    },
  );
});
