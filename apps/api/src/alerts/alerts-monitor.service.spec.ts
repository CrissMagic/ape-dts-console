import { describe, expect, it, vi } from 'vitest';
import { AlertsMonitorService } from './alerts-monitor.service.js';
import { AlertsService } from './alerts.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { TaskProgressService } from '../task-progress/task-progress.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import type { TaskProgress } from '@scs/contracts';

const configuration = {
  enabled: true,
  slotInactive: true,
  slotMissing: true,
  retainedWalBytes: '100',
  unconfirmedWalBytes: '100',
  kafkaOffsetLag: '10',
  delayMilliseconds: 1000,
};
const normalTask = {
  id: 'task',
  display_name: 'Test',
  desired_state: 'RUNNING',
  observed_state: 'RUNNING',
  current_applied_revision_id: 'revision',
  action: 'START',
  operation_state: 'SUCCEEDED',
  initiated_by_user_id: 'user' as string | null,
  run_status: 'RUNNING',
  run_stage: 'MONITORING_READY',
  failure_code: null as string | null,
  recovery_failed: false,
};
const sample: TaskProgress = {
  taskId: 'task',
  revisionId: 'revision',
  collectedAt: new Date().toISOString(),
  status: 'AVAILABLE',
  sourceType: 'POSTGRES',
  slotStatus: 'INACTIVE',
  unconfirmedWalBytes: '200',
  retainedWalBytes: '300',
  offsetLag: null,
  timeDelayMilliseconds: null,
  timeDelayStatus: 'UNSUPPORTED',
};
function setup(task = normalTask, progress = sample) {
  const observe = vi.fn();
  const alerts = { get: async () => configuration, observe } as unknown as AlertsService;
  const query = vi.fn(async () => ({ rows: [task] }));
  const database = {
    hasPool: () => true,
    requirePool: () => ({ query }),
  } as unknown as PlatformDatabaseService;
  const keys = new UnlockedKeyService();
  keys.set(Buffer.alloc(32));
  const source = { listForMonitoring: async () => [progress] } as unknown as TaskProgressService;
  const readiness = new PlatformReadinessService();
  readiness.markReady();
  return {
    monitor: new AlertsMonitorService(database, keys, alerts, source, readiness),
    observe,
    query,
    source,
    readiness,
  };
}
describe('alert background task observation', () => {
  it('still reports task failures when the progress snapshot fails', async () => {
    const f = setup({
      ...normalTask,
      observed_state: 'FAILED',
      failure_code: 'RUNTIME_UNEXPECTED_EXIT',
    });
    vi.spyOn(f.source, 'listForMonitoring').mockRejectedValue(new Error('collector unavailable'));
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'UNEXPECTED_EXIT',
      true,
      true,
      '故障码：RUNTIME_UNEXPECTED_EXIT',
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      null,
      false,
      expect.any(String),
    );
  });
  it('does not interpret MySQL progress as PostgreSQL WAL or Kafka consumer lag', async () => {
    const f = setup(normalTask, { ...sample, sourceType: 'MYSQL', offsetLag: '200' });
    await f.monitor.tick();
    for (const rule of [
      'PG_SLOT_INACTIVE',
      'PG_SLOT_MISSING',
      'PG_RETAINED_WAL',
      'PG_UNCONFIRMED_WAL',
      'KAFKA_OFFSET_LAG',
      'SYNC_DELAY',
    ])
      expect(f.observe).toHaveBeenCalledWith('task', 'Test', rule, null, false, expect.any(String));
  });
  it('does not classify a snapshot phase waiting for CDC as a disconnected replication consumer', async () => {
    const f = setup({ ...normalTask, run_stage: 'SNAPSHOT' });
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      null,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      true,
      false,
      expect.any(String),
    );
  });
  it('detects slot disconnect and WAL thresholds only for running tasks', async () => {
    const f = setup();
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      true,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      true,
      false,
      expect.stringContaining('当前值：300 B\n阈值：100 B'),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'SYNC_DELAY',
      null,
      false,
      expect.any(String),
    );
  });
  it('intentional stop suppresses inactive-slot alerts but still detects retained WAL', async () => {
    const f = setup({
      ...normalTask,
      desired_state: 'STOPPED',
      observed_state: 'STOPPED',
      action: 'STOP',
    });
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      false,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      true,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_UNCONFIRMED_WAL',
      true,
      false,
      expect.any(String),
    );
  });
  it.each([
    { ...sample, status: 'READ_FAILED' as const },
    { ...sample, collectedAt: new Date(Date.now() - 61000).toISOString() },
    { ...sample, revisionId: 'old-revision' },
  ])('does not clear WAL alarms from unavailable stopped-task measurements', async (progress) => {
    const f = setup(
      { ...normalTask, desired_state: 'STOPPED', observed_state: 'STOPPED', action: 'STOP' },
      progress,
    );
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      null,
      false,
      expect.any(String),
    );
  });
  it('recovers WAL alarms on a stopped task only from a fresh below-threshold sample', async () => {
    const f = setup(
      { ...normalTask, desired_state: 'STOPPED', observed_state: 'STOPPED', action: 'STOP' },
      { ...sample, retainedWalBytes: '10', unconfirmedWalBytes: '20' },
    );
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      false,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_UNCONFIRMED_WAL',
      false,
      false,
      expect.any(String),
    );
  });
  it.each([
    { ...sample, status: 'READ_FAILED' as const },
    { ...sample, collectedAt: new Date(Date.now() - 61000).toISOString() },
    { ...sample, revisionId: 'old-revision' },
  ])('missing or stale measurements never signal recovery', async (progress) => {
    const f = setup(normalTask, progress);
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      null,
      false,
      expect.any(String),
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_RETAINED_WAL',
      null,
      false,
      expect.any(String),
    );
  });
  it('reports rejected starts even while desired state remains stopped', async () => {
    const f = setup({
      ...normalTask,
      desired_state: 'STOPPED',
      observed_state: 'STOPPED',
      operation_state: 'REJECTED',
    });
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith('task', 'Test', 'START_FAILED', true, true, '');
  });
  it('starting a replacement process does not falsely report recovery', async () => {
    const f = setup({ ...normalTask, observed_state: 'STARTING', operation_state: 'EXECUTING' });
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith('task', 'Test', 'START_FAILED', null, true, '');
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'PG_SLOT_INACTIVE',
      null,
      false,
      expect.any(String),
    );
  });
  it('recognizes unexpected exit and system initiated recovery failure separately', async () => {
    const exited = setup({
      ...normalTask,
      observed_state: 'FAILED',
      run_status: 'FAILED',
      run_stage: 'RUNNING',
      failure_code: 'RUNTIME_UNEXPECTED_EXIT',
    });
    await exited.monitor.tick();
    expect(exited.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'UNEXPECTED_EXIT',
      true,
      true,
      '故障码：RUNTIME_UNEXPECTED_EXIT',
    );
    const recovered = setup({
      ...normalTask,
      observed_state: 'BLOCKED',
      operation_state: 'BLOCKED_MANUAL',
      initiated_by_user_id: null,
    });
    await recovered.monitor.tick();
    expect(recovered.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'RECOVERY_FAILED',
      true,
      true,
      '',
    );
  });
  it('notification subsystem failures do not throw into task control', async () => {
    const f = setup();
    f.observe.mockRejectedValueOnce(new Error('transport unavailable'));
    await expect(f.monitor.tick()).resolves.toBeUndefined();
    expect(f.query.mock.calls).toHaveLength(1);
  });
  it('standby and bootstrapping instances do not send alerts', async () => {
    const f = setup();
    f.readiness.markConcurrentInstanceBlocked();
    await f.monitor.tick();
    expect(f.query).not.toHaveBeenCalled();
    expect(f.observe).not.toHaveBeenCalled();
  });
  it('recognizes durable recovery preflight failure without a new system start operation', async () => {
    const f = setup({
      ...normalTask,
      desired_state: 'STOPPED',
      observed_state: 'BLOCKED',
      recovery_failed: true,
      failure_code: 'RUNTIME_UNEXPECTED_EXIT',
    });
    await f.monitor.tick();
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'RECOVERY_FAILED',
      true,
      true,
      '故障码：RUNTIME_UNEXPECTED_EXIT',
    );
    expect(f.observe).toHaveBeenCalledWith(
      'task',
      'Test',
      'UNEXPECTED_EXIT',
      null,
      true,
      '故障码：RUNTIME_UNEXPECTED_EXIT',
    );
  });
});
