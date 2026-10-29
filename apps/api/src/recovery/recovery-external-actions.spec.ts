import { describe, expect, it, vi } from 'vitest';

import {
  PostgresRecoveryExternalActions,
  type RecoveryRuntimePort,
} from './recovery-external-actions.js';
import type { RecoveryCandidate } from './recovery.types.js';

const candidate: RecoveryCandidate = {
  revisionId: '00000000-0000-4000-8000-000000000010',
  taskId: '00000000-0000-4000-8000-000000000001',
  taskName: 'task',
  taskType: 'PG_KAFKA',
  unitKeys: ['pg'],
  units: 1,
};

function runtime(overrides: Partial<RecoveryRuntimePort> = {}): RecoveryRuntimePort {
  return {
    reconcilePersistedRuns: vi.fn(async () => 0),
    recoverPersistedTask: vi.fn(async () => ({ runId: 'run-new' })),
    recoveryObservability: vi.fn(async () => [
      { logsReady: true, metricsReady: true, runId: 'run-new', taskId: candidate.taskId },
    ]),
    verifyRecoveryArtifact: vi.fn(async () => undefined),
    ...overrides,
  };
}

function actions(rows: unknown[], port = runtime(), preflight = vi.fn()) {
  const query = vi.fn(async () => ({ rows: [rows.shift()].filter((row) => row !== undefined) }));
  return {
    actions: new PostgresRecoveryExternalActions(
      { requirePool: () => ({ query }) } as never,
      { runForSystemRecovery: preflight } as never,
      port,
    ),
    port,
    preflight,
  };
}

const binding = {
  clean_shutdown: false,
  current_stage: 'RECONCILIATION',
  old_run_id: '00000000-0000-4000-8000-000000000020',
  platform_instance_id: '00000000-0000-4000-8000-000000000030',
  revision_id: candidate.revisionId,
  row_version: '7',
  run_status: 'BLOCKED',
  task_id: candidate.taskId,
  unit_keys: ['pg'],
};

describe('PostgresRecoveryExternalActions', () => {
  it('uses SYSTEM preflight for MySQL including a persisted snapshot without requesting reinitialization', async () => {
    const context = actions(
      [{ confirmed: true }, { ...binding, include_snapshot: true }],
      runtime(),
      vi.fn(async () => ({ state: 'PASSED' })),
    );
    expect(
      await context.actions.verifyContinuity('session', [
        { ...candidate, taskType: 'MYSQL_KAFKA' },
      ]),
    ).toMatchObject({ kind: 'SUCCEEDED' });
    expect(context.preflight).toHaveBeenCalledWith(
      expect.objectContaining({ taskId: candidate.taskId, expectedOldRunId: binding.old_run_id }),
    );
    expect(context.preflight.mock.calls[0]?.[0]).not.toHaveProperty('reinitializeMysqlSnapshot');
  });
  it('does not silently restart a full snapshot when there is no durable snapshot resume position', async () => {
    const context = actions([{ confirmed: true }, { ...binding, include_snapshot: true }]);
    await expect(context.actions.verifyContinuity('session', [candidate])).resolves.toMatchObject({
      kind: 'FAILED',
      code: 'RECOVERY_SNAPSHOT_RESTART_REQUIRES_DECISION',
    });
    expect(context.preflight).not.toHaveBeenCalled();
  });
  it('reports the exact task when real artifact verification fails', async () => {
    const context = actions(
      [],
      runtime({
        verifyRecoveryArtifact: vi.fn(async () => {
          throw new Error('ARTIFACT_HASH_MISMATCH');
        }),
      }),
    );
    await expect(context.actions.ensureArtifacts('session', [candidate])).resolves.toMatchObject({
      code: 'ARTIFACT_HASH_MISMATCH',
      kind: 'FAILED',
      progress: { taskId: candidate.taskId },
    });
  });

  it('pauses an unclean old instance until a separate administrator confirmation exists', async () => {
    const context = actions([{ confirmed: false }, binding]);
    await expect(context.actions.verifyContinuity('session', [candidate])).resolves.toMatchObject({
      code: 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED',
      kind: 'PAUSED',
    });
    expect(context.preflight).not.toHaveBeenCalled();
  });

  it('runs SYSTEM preflight after persisted confirmation and surfaces its failure', async () => {
    const context = actions(
      [{ confirmed: true }, binding],
      runtime(),
      vi.fn(async () => ({ failureCode: 'PREFLIGHT_SOURCE_IDENTITY_MISMATCH', state: 'FAILED' })),
    );
    await expect(context.actions.verifyContinuity('session', [candidate])).resolves.toMatchObject({
      code: 'PREFLIGHT_SOURCE_IDENTITY_MISMATCH',
      kind: 'FAILED',
    });
  });

  it('reports missing log or Metrics collection facts instead of declaring readiness', async () => {
    const context = actions(
      [],
      runtime({
        recoveryObservability: vi.fn(async () => [
          { logsReady: true, metricsReady: false, runId: 'run-new', taskId: candidate.taskId },
        ]),
      }),
    );
    await expect(context.actions.readyObservability('session', [candidate])).resolves.toMatchObject(
      {
        code: 'RECOVERY_OBSERVABILITY_NOT_READY',
        kind: 'FAILED',
      },
    );
  });

  it('surfaces a single-task persisted restore failure with the task id', async () => {
    const context = actions(
      [binding],
      runtime({
        recoverPersistedTask: vi.fn(async () => {
          throw new Error('RUNTIME_RECOVERY_START_FAILED');
        }),
      }),
    );
    await expect(context.actions.restoreTask('session', candidate)).resolves.toMatchObject({
      code: 'RUNTIME_RECOVERY_START_FAILED',
      kind: 'FAILED',
      progress: { taskId: candidate.taskId },
    });
  });
});
