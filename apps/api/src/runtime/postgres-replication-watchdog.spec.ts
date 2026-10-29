import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { TaskProgress } from '@scs/contracts';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { PlatformReadinessService } from '../health/platform-readiness.service.js';
import type { TaskProgressService } from '../task-progress/task-progress.service.js';
import type { RuntimeSupervisorService } from './runtime-supervisor.service.js';
import { PostgresReplicationWatchdog } from './postgres-replication-watchdog.service.js';

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-10-27T01:00:00Z'));
});
afterEach(() => vi.useRealTimers());
function fixture() {
  let sample: TaskProgress = {
    taskId: 'task',
    revisionId: 'revision',
    collectedAt: new Date().toISOString(),
    sourceType: 'POSTGRES',
    status: 'AVAILABLE',
    currentStage: 'MONITORING_READY',
    slotStatus: 'INACTIVE',
    unconfirmedWalBytes: '0',
    retainedWalBytes: '100',
    offsetLag: null,
    timeDelayMilliseconds: null,
    timeDelayStatus: 'UNSUPPORTED',
  };
  const database = { hasPool: vi.fn(() => true) };
  const readiness = { getPhase: vi.fn(() => 'READY') };
  const progress = { listForMonitoring: vi.fn(async () => [sample]) };
  const runtime = {
    listActiveRuntimeTasks: vi.fn(() => [{ taskId: 'task', runId: 'run', revisionId: 'revision' }]),
    recoverDisconnectedPostgres: vi.fn(async () => true),
  };
  const watchdog = new PostgresReplicationWatchdog(
    database as unknown as PlatformDatabaseService,
    readiness as unknown as PlatformReadinessService,
    progress as unknown as TaskProgressService,
    runtime as unknown as RuntimeSupervisorService,
  );
  return {
    watchdog,
    runtime,
    progress,
    database,
    readiness,
    async sampleAfter(ms: number, changes: Partial<TaskProgress> = {}) {
      await vi.advanceTimersByTimeAsync(ms);
      sample = { ...sample, collectedAt: new Date().toISOString(), ...changes };
      await watchdog.tick();
    },
  };
}
it('requires a full minute of consecutive fresh disconnected CDC samples before recovery', async () => {
  const f = fixture();
  await f.watchdog.tick();
  for (let n = 0; n < 3; n++) await f.sampleAfter(15000);
  expect(f.runtime.recoverDisconnectedPostgres).not.toHaveBeenCalled();
  await f.sampleAfter(15000);
  expect(f.runtime.recoverDisconnectedPostgres).toHaveBeenCalledOnce();
  expect(f.runtime.recoverDisconnectedPostgres).toHaveBeenCalledWith({
    taskId: 'task',
    runId: 'run',
    revisionId: 'revision',
    collectedAt: new Date().toISOString(),
  });
});
it.each([
  { currentStage: 'SNAPSHOT' },
  { slotStatus: 'ACTIVE' },
  { slotStatus: 'MISSING' },
  { status: 'READ_FAILED' },
  { sourceType: 'MYSQL' },
  { revisionId: 'other' },
] as Partial<TaskProgress>[])(
  'does not stop a task for a snapshot, unavailable reading, different source or changed binding: %o',
  async (changes) => {
    const f = fixture();
    await f.sampleAfter(0, changes);
    for (let n = 0; n < 6; n++) await f.sampleAfter(15000, changes);
    expect(f.runtime.recoverDisconnectedPostgres).not.toHaveBeenCalled();
  },
);
it('does not advance on cached data, and restarts its grace period after a healthy or missing sample', async () => {
  const f = fixture();
  await f.watchdog.tick();
  await vi.advanceTimersByTimeAsync(60000);
  await f.watchdog.tick();
  expect(f.runtime.recoverDisconnectedPostgres).not.toHaveBeenCalled();
  await f.sampleAfter(0);
  await f.sampleAfter(30000);
  await f.sampleAfter(15000, { slotStatus: 'ACTIVE' });
  await f.sampleAfter(15000, { slotStatus: 'INACTIVE' });
  await f.sampleAfter(30000);
  expect(f.runtime.recoverDisconnectedPostgres).not.toHaveBeenCalled();
  f.readiness.getPhase.mockReturnValue('METADATA_DEGRADED');
  await f.watchdog.tick();
  f.readiness.getPhase.mockReturnValue('READY');
  await f.sampleAfter(15000);
  await f.sampleAfter(30000);
  expect(f.runtime.recoverDisconnectedPostgres).not.toHaveBeenCalled();
});
it('stops monitoring after shutdown and isolates one failed recovery from the next tick', async () => {
  const f = fixture();
  f.runtime.recoverDisconnectedPostgres.mockRejectedValueOnce(new Error('TIMEOUT'));
  await f.watchdog.tick();
  for (let n = 0; n < 4; n++) await f.sampleAfter(15000);
  expect(f.runtime.recoverDisconnectedPostgres).toHaveBeenCalledTimes(1);
  await f.sampleAfter(15000);
  expect(f.runtime.recoverDisconnectedPostgres).toHaveBeenCalledTimes(2);
  f.watchdog.onApplicationShutdown();
  await f.sampleAfter(60000);
  expect(f.runtime.recoverDisconnectedPostgres).toHaveBeenCalledTimes(2);
});
