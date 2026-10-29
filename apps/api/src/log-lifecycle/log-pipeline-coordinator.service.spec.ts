import type { UserSummary } from '@scs/contracts';
import type { LogPipeline } from '@scs/observability';
import { describe, expect, it, vi } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { PostgresLogSegmentStore } from '../storage/postgres-log-segment-store.js';
import {
  LogPipelineCoordinatorError,
  LogPipelineCoordinatorService,
} from './log-pipeline-coordinator.service.js';

const actor: UserSummary = {
  displayName: 'Developer',
  id: '33333333-3333-4333-8333-333333333333',
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: 'developer',
};
const taskId = '11111111-1111-4111-8111-111111111111';
const runId = '22222222-2222-4222-8222-222222222222';

function setup(
  options: {
    readonly access?: boolean;
    readonly persisted?: boolean;
    readonly persistedRecoveryState?:
      'NONE' | 'MEMORY_PENDING' | 'DURABLE_PENDING' | 'MIXED_PENDING';
    readonly recoverError?: unknown;
    readonly recoveryGate?: Promise<void>;
    readonly settingsGate?: Promise<void>;
    readonly settingsRowVersion?: string;
    readonly settleStates?: Array<'NONE' | 'MEMORY_PENDING' | 'DURABLE_PENDING' | 'MIXED_PENDING'>;
    readonly snapshotRecoveryState?:
      'NONE' | 'MEMORY_PENDING' | 'DURABLE_PENDING' | 'MIXED_PENDING';
  } = {},
) {
  const query = vi.fn(async (sql: string) => {
    if (sql.includes('FROM ape_dts_platform.platform_settings')) {
      if (options.settingsGate !== undefined) await options.settingsGate;
      return {
        rowCount: 1,
        rows: [
          {
            log_failure_buffer_bytes: '4096',
            row_version: options.settingsRowVersion ?? '8',
            run_log_limit_bytes: '8192',
          },
        ],
      };
    }
    if (sql.includes('JOIN ape_dts_platform.tasks AS task')) {
      return { rowCount: options.access === false ? 0 : 1, rows: [{ id: runId }] };
    }
    if (sql.includes('SELECT log_recovery_state')) {
      return {
        rowCount: options.persisted === false ? 0 : 1,
        rows:
          options.persisted === false
            ? []
            : [
                {
                  log_recovery_state: options.persistedRecoveryState ?? 'NONE',
                  log_storage_state: 'DEGRADED',
                },
              ],
      };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const applyLimits = vi.fn(async () => ({
    current: {
      activeRuns: 0,
      maxBufferedBytes: 4096,
      maxRunBytes: 8192,
      pendingBufferedBytes: 0,
    },
    droppedBufferedBytes: 0,
    droppedBufferedSegments: 0,
    previous: {
      activeRuns: 0,
      maxBufferedBytes: 67_108_864,
      maxRunBytes: 1_073_741_824,
      pendingBufferedBytes: 0,
    },
    sealedSegments: 0,
  }));
  const retryStorage = vi.fn(async () => undefined);
  const settleStates = [...(options.settleStates ?? [])];
  const settleRunWithStatus = vi.fn(async () => ({
    recoveryState: settleStates.shift() ?? 'NONE',
    statePersisted: true,
    storageState: 'DEGRADED' as const,
  }));
  const recoverPendingOperations = vi.fn(async () => {
    if (options.recoveryGate !== undefined) await options.recoveryGate;
    if (options.recoverError !== undefined) throw options.recoverError;
    return {
      availableSegments: 0,
      capacityDeletedSegments: 2,
      completedDeletions: 1,
      failures: 0,
      integrityFailedSegments: 0,
      memoryPendingGaps: 0,
      missingSegments: 0,
    };
  });
  const enforceAllRunCapacities = vi.fn(async () => ({
    affectedRuns: 0,
    deletedSegments: 0,
    failures: 0,
  }));
  const service = new LogPipelineCoordinatorService(
    { requirePool: () => ({ query }) } as unknown as PlatformDatabaseService,
    {
      applyLimits,
      retryStorage,
      settleRunWithStatus,
      snapshotRunState: () => ({
        recoveryState: options.snapshotRecoveryState ?? 'NONE',
        storageState: 'HEALTHY',
      }),
      snapshotLimits: () => ({
        activeRuns: 0,
        maxBufferedBytes: 67_108_864,
        maxRunBytes: 1_073_741_824,
        pendingBufferedBytes: 0,
      }),
    } as unknown as LogPipeline,
    { enforceAllRunCapacities, recoverPendingOperations } as unknown as PostgresLogSegmentStore,
  );
  return {
    applyLimits,
    enforceAllRunCapacities,
    query,
    recoverPendingOperations,
    retryStorage,
    service,
    settleRunWithStatus,
  };
}

describe('LogPipelineCoordinatorService', () => {
  it('loads PostgreSQL limits and applies persistent run capacity before a runtime starts', async () => {
    const test = setup();

    await test.service.synchronizeFromDatabase();
    await test.service.synchronizeFromDatabase();

    expect(test.applyLimits).toHaveBeenCalledTimes(1);
    expect(test.applyLimits).toHaveBeenCalledWith({ maxBufferedBytes: 4096, maxRunBytes: 8192 });
    expect(test.enforceAllRunCapacities).toHaveBeenCalledWith(8192);
  });

  it('serializes a delayed database snapshot with a newer administrator limit update', async () => {
    let releaseSettings!: () => void;
    const settingsGate = new Promise<void>((resolve) => {
      releaseSettings = resolve;
    });
    const test = setup({ settingsGate, settingsRowVersion: '8' });

    const delayedSynchronization = test.service.synchronizeFromDatabase();
    const administratorUpdate = test.service.applyLimits({
      maxBufferedBytes: 16_384,
      maxRunBytes: 32_768,
      rowVersion: '9',
    });
    releaseSettings();
    await Promise.all([delayedSynchronization, administratorUpdate]);

    expect(test.applyLimits).toHaveBeenLastCalledWith({
      maxBufferedBytes: 16_384,
      maxRunBytes: 32_768,
    });
  });

  it('retries only the authorized run and returns the persisted recovery state', async () => {
    const test = setup();

    await expect(test.service.retryStorage(actor, taskId, runId)).resolves.toEqual({
      durableRecovery: {
        availableSegments: 0,
        capacityDeletedSegments: 2,
        completedDeletions: 1,
        failures: 0,
        integrityFailedSegments: 0,
        memoryPendingGaps: 0,
        missingSegments: 0,
      },
      recoveryState: 'NONE',
      runId,
      storageState: 'DEGRADED',
      taskId,
    });
    expect(test.retryStorage).toHaveBeenCalledTimes(2);
    expect(test.retryStorage).toHaveBeenNthCalledWith(1, runId);
    expect(test.recoverPendingOperations).toHaveBeenCalledWith(runId);
  });

  it('does not retry storage when the actor cannot access the exact task run', async () => {
    const test = setup({ access: false });

    await expect(test.service.retryStorage(actor, taskId, runId)).rejects.toBeInstanceOf(
      LogPipelineCoordinatorError,
    );
    expect(test.retryStorage).not.toHaveBeenCalled();
    expect(test.recoverPendingOperations).not.toHaveBeenCalled();
  });

  it('does not claim durable recovery while memory-only log content remains pending', async () => {
    const test = setup({
      persistedRecoveryState: 'MEMORY_PENDING',
      settleStates: ['MEMORY_PENDING'],
      snapshotRecoveryState: 'MEMORY_PENDING',
    });

    await expect(test.service.retryStorage(actor, taskId, runId)).resolves.toMatchObject({
      recoveryState: 'MEMORY_PENDING',
    });
    expect(test.retryStorage).toHaveBeenCalledTimes(1);
    expect(test.recoverPendingOperations).not.toHaveBeenCalled();
  });

  it('runs a second durable recovery pass when the first pass exposes durable pending work', async () => {
    const test = setup({ settleStates: ['DURABLE_PENDING', 'NONE'] });

    await test.service.retryStorage(actor, taskId, runId);

    expect(test.recoverPendingOperations).toHaveBeenCalledTimes(2);
    expect(test.retryStorage).toHaveBeenCalledTimes(3);
    expect(test.settleRunWithStatus).toHaveBeenCalledTimes(2);
  });

  it('reports not found if the run disappears before the persisted state is read', async () => {
    const test = setup({ persisted: false, snapshotRecoveryState: 'MEMORY_PENDING' });

    await expect(test.service.retryStorage(actor, taskId, runId)).rejects.toMatchObject({
      code: 'RUNTIME_LOG_RUN_NOT_FOUND',
    });
  });

  it('preserves a stable uppercase storage error code for the HTTP layer', async () => {
    const error = Object.assign(new Error('object storage unavailable'), {
      code: 'LOG_OBJECT_STORE_UNAVAILABLE',
    });
    const test = setup({ recoverError: error });

    await expect(test.service.retryStorage(actor, taskId, runId)).rejects.toMatchObject({
      code: 'LOG_OBJECT_STORE_UNAVAILABLE',
    });
  });

  it('shares one durable recovery operation across concurrent retries of the same run', async () => {
    let releaseRecovery!: () => void;
    const recoveryGate = new Promise<void>((resolve) => {
      releaseRecovery = resolve;
    });
    const test = setup({ recoveryGate });

    const first = test.service.retryStorage(actor, taskId, runId);
    const second = test.service.retryStorage(actor, taskId, runId);
    releaseRecovery();
    await Promise.all([first, second]);

    expect(test.recoverPendingOperations).toHaveBeenCalledTimes(1);
  });
});
