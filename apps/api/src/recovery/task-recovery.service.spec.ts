import { expect, it, vi } from 'vitest';
import { TaskRecoveryService } from './task-recovery.service.js';
import type { UserSummary } from '@scs/contracts';

const admin: UserSummary = {
  id: 'actor',
  username: 'admin',
  displayName: '管理员',
  role: 'ADMIN',
  status: 'ACTIVE',
};
function fixture(overrides: Record<string, unknown> = {}) {
  const task = {
    id: 'task',
    row_version: '4',
    current_applied_revision_id: 'revision',
    desired_state: 'RUNNING',
    observed_state: 'BLOCKED',
    recovery_disposition: 'MANUAL_REQUIRED',
    display_name: 'CDC',
    task_type: 'PG_KAFKA',
    ...overrides,
  };
  const run = {
    id: 'old-run',
    task_revision_id: 'revision',
    platform_instance_id: 'old-platform',
    status: 'BLOCKED',
    current_stage: 'RECONCILIATION',
  };
  const query = vi.fn(async (sql: string) => {
    if (sql.includes('SELECT id, row_version')) return { rows: [task], rowCount: 1 };
    if (sql.includes('SELECT id, task_revision_id')) return { rows: [run], rowCount: 1 };
    if (sql.includes('SELECT unit_key')) return { rows: [{ unit_key: 'primary' }], rowCount: 1 };
    return { rows: [], rowCount: 1 };
  });
  const release = vi.fn();
  const coordinator = { runCandidate: vi.fn(async () => ({ state: 'READY' })) };
  const runtime = {
    listActiveRuntimeTasks: vi.fn(() => [] as { taskId: string }[]),
    retryFailedTask: vi.fn(async () => ({ state: 'SUCCEEDED' })),
  };
  const audit = { append: vi.fn() };
  const service = new TaskRecoveryService(
    { requirePool: () => ({ connect: async () => ({ query, release }) }) } as never,
    { getStatus: () => ({ phase: 'READY', platformInstanceId: 'new-platform' }) } as never,
    runtime as never,
    coordinator as never,
    audit as never,
  );
  return { service, task, run, query, release, coordinator, runtime, audit };
}
it.each(['PG_KAFKA', 'MYSQL_KAFKA'])(
  'retries only the selected %s task with the same applied revision and persisted primary unit',
  async (taskType) => {
    const f = fixture({ task_type: taskType });
    await expect(f.service.retry(admin, 'task', '4')).resolves.toMatchObject({ state: 'READY' });
    expect(f.coordinator.runCandidate).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        taskId: 'task',
        revisionId: 'revision',
        unitKeys: ['primary'],
        units: 1,
        taskType,
      }),
    );
    expect(f.audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        actorType: 'USER',
        details: expect.objectContaining({ oldRunId: 'old-run', oldRuntimeStoppedConfirmed: true }),
      }),
    );
    expect(f.query).toHaveBeenCalledWith('COMMIT');
    expect(f.release).toHaveBeenCalledOnce();
  },
);
it.each([
  { row_version: '5' },
  { desired_state: 'STOPPED' },
  { observed_state: 'RUNNING' },
  { recovery_disposition: 'PERMANENTLY_BLOCKED' },
])('does not revive a stale or non-recoverable task %j', async (overrides) => {
  const f = fixture(overrides);
  await expect(f.service.retry(admin, 'task', '4')).rejects.toThrow();
  expect(f.coordinator.runCandidate).not.toHaveBeenCalled();
  expect(f.audit.append).not.toHaveBeenCalled();
  expect(f.query).toHaveBeenCalledWith('ROLLBACK');
});
it('does not confirm away a process owned by this platform', async () => {
  const f = fixture();
  f.runtime.listActiveRuntimeTasks.mockReturnValue([{ taskId: 'task' }]);
  await expect(f.service.retry(admin, 'task', '4')).rejects.toThrow('RECOVERY_STEP_NOT_ACTIONABLE');
  expect(f.query).not.toHaveBeenCalled();
});
it('rejects a newest run belonging to this platform or another revision', async () => {
  for (const patch of [{ platform_instance_id: 'new-platform' }, { task_revision_id: 'other' }]) {
    const f = fixture();
    Object.assign(f.run, patch);
    await expect(f.service.retry(admin, 'task', '4')).rejects.toThrow(
      'RECOVERY_STEP_NOT_ACTIONABLE',
    );
    expect(f.coordinator.runCandidate).not.toHaveBeenCalled();
  }
});
it('rejects non-administrators before reading or writing metadata', async () => {
  const f = fixture();
  await expect(f.service.retry({ ...admin, role: 'DEVELOPER' }, 'task', '4')).rejects.toThrow(
    'RECOVERY_ACCESS_DENIED',
  );
  expect(f.query).not.toHaveBeenCalled();
});

it('retries a confirmed failed run on the current instance after releasing the task lock', async () => {
  const f = fixture({ desired_state: 'STOPPED' });
  Object.assign(f.run, {
    platform_instance_id: 'new-platform',
    status: 'FAILED',
    current_stage: 'RUNNING',
  });
  await expect(f.service.retry(admin, 'task', '4')).resolves.toEqual({
    operation: { state: 'SUCCEEDED' },
  });
  expect(f.release).toHaveBeenCalledBefore(f.runtime.retryFailedTask);
  expect(f.runtime.retryFailedTask).toHaveBeenCalledWith(admin, 'task', '4');
  expect(f.coordinator.runCandidate).not.toHaveBeenCalled();
});
