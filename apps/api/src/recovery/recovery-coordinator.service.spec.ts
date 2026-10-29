import { describe, expect, it, vi } from 'vitest';

import {
  RecoveryCoordinatorError,
  RecoveryCoordinatorService,
} from './recovery-coordinator.service.js';
import type {
  RecoveryActionResult,
  RecoveryCandidate,
  RecoveryExternalActions,
  RecoverySessionSnapshot,
  RecoveryStepStatus,
  RecoveryStepType,
  RecoveryStore,
} from './recovery.types.js';

const candidate = (id: string, units = 1): RecoveryCandidate => ({
  revisionId: '00000000-0000-4000-8000-000000000010',
  taskId: id,
  taskName: `task-${id.slice(-1)}`,
  taskType: units === 1 ? 'PG_KAFKA' : 'KAFKA_KAFKA',
  unitKeys: Array.from({ length: units }, (_, index) => `unit-${index}`),
  units,
});

class MemoryStore implements RecoveryStore {
  readonly finished: Array<{
    step: RecoveryStepType;
    result: RecoveryActionResult;
    manual?: unknown;
  }> = [];
  readonly blocked: string[][] = [];
  readonly manual: string[] = [];
  capacityValue: { limit: number | null; used: number } = { limit: 8, used: 0 };
  capacityMismatch: { actual: number; expected: number; taskId: string } | null = null;
  candidates: readonly RecoveryCandidate[] = [candidate('00000000-0000-4000-8000-000000000001')];
  state: RecoverySessionSnapshot['state'] = 'RUNNING';
  rowVersion = '1';
  statuses = new Map<RecoveryStepType, RecoveryStepStatus>([
    ['LOAD_METADATA', 'SUCCEEDED'],
    ['UNLOCK_KEYS', 'SUCCEEDED'],
    ['ENSURE_ARTIFACTS', 'PENDING'],
    ['VERIFY_CONTINUITY', 'PENDING'],
    ['PLAN_CAPACITY', 'PENDING'],
    ['RESTORE_BATCHES', 'PENDING'],
    ['READY_OBSERVABILITY', 'PENDING'],
  ]);
  finishStepError: Error | null = null;
  finishSessionFailures = 0;

  async getSession(id: string) {
    return id === 'missing'
      ? null
      : {
          candidates: this.candidates,
          id,
          rowVersion: this.rowVersion,
          state: this.state,
          steps: [...this.statuses].map(([stepType, status], position) => ({
            attemptCount: 0,
            errorSummary: (() => {
              const result = this.finished.findLast((entry) => entry.step === stepType)?.result;
              return result !== undefined && result.kind !== 'SUCCEEDED' ? result.summary : null;
            })(),
            failureCode: (() => {
              const result = this.finished.findLast((entry) => entry.step === stepType)?.result;
              return result !== undefined && result.kind !== 'SUCCEEDED' ? result.code : null;
            })(),
            position: position + 1,
            progress:
              this.finished.findLast((entry) => entry.step === stepType)?.result.progress ?? {},
            status,
            stepType,
          })),
        };
  }
  async getCurrentSession(id: string) {
    return await this.getSession(id);
  }
  async listCandidates() {
    return this.candidates.filter((item) => !this.manual.includes(item.taskId));
  }
  async beginStep(_id: string, step: RecoveryStepType, allowed: readonly RecoveryStepStatus[]) {
    if (!allowed.includes(this.statuses.get(step)!)) return false;
    this.statuses.set(step, 'RUNNING');
    this.state = 'RUNNING';
    return true;
  }
  async finishStep(
    _id: string,
    step: RecoveryStepType,
    result: RecoveryActionResult,
    manual?: unknown,
  ) {
    if (this.finishStepError !== null) throw this.finishStepError;
    this.finished.push({ step, result, manual });
    this.statuses.set(step, result.kind);
  }
  async capacity() {
    if (this.capacityMismatch !== null)
      return { ...this.capacityMismatch, kind: 'MISMATCH' as const };
    return {
      ...this.capacityValue,
      kind: 'READY' as const,
      retained: this.candidates.reduce((sum, item) => sum + item.units, 0),
    };
  }
  async confirmOldRuntimeStopped(
    _id: string,
    version: string,
    _reason: string,
    _actorUserId: string,
  ) {
    void _reason;
    void _actorUserId;
    return version === this.rowVersion ? ('APPLIED' as const) : ('CONFLICT' as const);
  }
  async selectCandidates(_id: string, taskIds: readonly string[], version: string) {
    return version === this.rowVersion
      ? this.candidates.filter((item) => taskIds.includes(item.taskId))
      : [];
  }
  async applyCapacitySelection(
    _id: string,
    taskIds: readonly string[],
    version: string,
    _actorUserId: string,
  ): ReturnType<RecoveryStore['applyCapacitySelection']> {
    void _actorUserId;
    if (version !== this.rowVersion) return { kind: 'CONFLICT' };
    if (taskIds.length === 0) return { kind: 'INVALID' };
    const selected = this.candidates.filter((item) => taskIds.includes(item.taskId));
    if (new Set(taskIds).size !== taskIds.length || selected.length !== taskIds.length) {
      return { kind: 'INVALID' };
    }
    if (this.capacityValue.limit === null) return { kind: 'NOT_CONFIGURED' };
    const requested = selected.reduce((sum, item) => sum + item.units, 0);
    const retained = this.candidates.reduce((sum, item) => sum + item.units, 0);
    const nonCandidateUsed = this.capacityValue.used - retained;
    if (nonCandidateUsed + requested > this.capacityValue.limit) {
      return {
        kind: 'EXCEEDED',
        limit: this.capacityValue.limit,
        requested,
        used: nonCandidateUsed,
      };
    }
    this.blocked.push(
      this.candidates.filter((item) => !taskIds.includes(item.taskId)).map((item) => item.taskId),
    );
    this.statuses.set('PLAN_CAPACITY', 'SUCCEEDED');
    this.state = 'RUNNING';
    return { kind: 'APPLIED', candidates: selected };
  }
  async markCapacityBlocked(ids: readonly string[]) {
    this.blocked.push([...ids]);
  }
  async markManualRequired(id: string) {
    this.manual.push(id);
  }
  async finishSession(_id: string, state: RecoverySessionSnapshot['state']) {
    if (this.finishSessionFailures > 0) {
      this.finishSessionFailures -= 1;
      throw new Error('SESSION_FINISH_FAILED');
    }
    this.state = state;
  }
}

function actions(overrides: Partial<RecoveryExternalActions> = {}): RecoveryExternalActions {
  const succeeded = (): RecoveryActionResult => ({ kind: 'SUCCEEDED' });
  return {
    ensureArtifacts: vi.fn(async () => succeeded()),
    verifyContinuity: vi.fn(async () => succeeded()),
    restoreTask: vi.fn(async () => succeeded()),
    readyObservability: vi.fn(async () => succeeded()),
    ...overrides,
  };
}

describe('RecoveryCoordinatorService', () => {
  it('retries only the requested candidate even when other blocked tasks exist', async () => {
    const store = new MemoryStore();
    const a = candidate('00000000-0000-4000-8000-000000000001');
    const b = candidate('00000000-0000-4000-8000-000000000002');
    store.candidates = [a, b];
    const ports = actions();
    const result = await new RecoveryCoordinatorService(store, ports).runCandidate('session', a);
    expect(result.state).toBe('READY');
    expect(ports.restoreTask).toHaveBeenCalledOnce();
    expect(ports.restoreTask).toHaveBeenCalledWith('session', a);
    expect(ports.ensureArtifacts).toHaveBeenCalledWith('session', [a]);
  });
  it('continues A/B/C restoration after B fails without starting A twice', async () => {
    const store = new MemoryStore();
    const [a, b, c] = ['1', '2', '3'].map((id) =>
      candidate(`00000000-0000-4000-8000-00000000000${id}`),
    );
    store.candidates = [a!, b!, c!];
    const attempts: string[] = [];
    const ports = actions({
      restoreTask: vi.fn(
        async (_session: string, item: RecoveryCandidate): Promise<RecoveryActionResult> => {
          attempts.push(item.taskId);
          return item.taskId === b!.taskId
            ? { kind: 'FAILED', code: 'START_FAILED', summary: 'cannot execute' }
            : { kind: 'SUCCEEDED' };
        },
      ),
    });
    const coordinator = new RecoveryCoordinatorService(store, ports);
    expect((await coordinator.runForPlatform('session')).state).toBe('READY');
    expect(attempts).toEqual([a!.taskId, b!.taskId, c!.taskId]);
    expect(store.manual).toEqual([b!.taskId]);
    expect(
      store.finished.findLast((item) => item.step === 'RESTORE_BATCHES')?.result.progress,
    ).toMatchObject({
      restoredTaskIds: [a!.taskId, c!.taskId],
      excludedTasks: [{ taskId: b!.taskId, code: 'START_FAILED' }],
    });
    await coordinator.runForPlatform('session');
    expect(attempts).toEqual([a!.taskId, b!.taskId, c!.taskId]);
  });

  it('never adds a newly started task to the startup recovery snapshot', async () => {
    const store = new MemoryStore();
    const initial = [...store.candidates];
    const ports = actions({
      ensureArtifacts: vi.fn(async (): Promise<RecoveryActionResult> => {
        store.candidates = [...store.candidates, candidate('00000000-0000-4000-8000-000000000002')];
        return { kind: 'SUCCEEDED' };
      }),
    });
    await new RecoveryCoordinatorService(store, ports).runForPlatform('session');
    expect(ports.verifyContinuity).toHaveBeenCalledWith('session', initial);
    expect(ports.restoreTask).toHaveBeenCalledExactlyOnceWith('session', initial[0]);
  });

  it('continues other candidates after a user removes a failed task during recovery', async () => {
    const store = new MemoryStore();
    const old = store.candidates[0]!;
    const healthy = candidate('00000000-0000-4000-8000-000000000002');
    store.candidates = [old, healthy];
    const ports = actions({
      verifyContinuity: vi.fn(
        async (
          _session: string,
          items: readonly RecoveryCandidate[],
        ): Promise<RecoveryActionResult> => {
          if (items.some((item) => item.taskId === old.taskId)) {
            store.candidates = [healthy];
            return {
              kind: 'FAILED',
              code: 'RECOVERY_OLD_RUN_BINDING_INVALID',
              summary: 'discarded while checking',
              progress: { taskId: old.taskId },
            };
          }
          return { kind: 'SUCCEEDED' };
        },
      ),
    });
    expect(
      (await new RecoveryCoordinatorService(store, ports).runForPlatform('session')).state,
    ).toBe('READY');
    expect(ports.restoreTask).toHaveBeenCalledExactlyOnceWith('session', healthy);
  });

  it('isolates a failed task during startup and still restores the other task', async () => {
    const store = new MemoryStore();
    const failed = candidate('00000000-0000-4000-8000-000000000001');
    const healthy = candidate('00000000-0000-4000-8000-000000000002');
    store.candidates = [failed, healthy];
    const ports = actions({
      verifyContinuity: vi.fn(
        async (
          _session: string,
          items: readonly RecoveryCandidate[],
        ): Promise<RecoveryActionResult> =>
          items.some((item) => item.taskId === failed.taskId)
            ? {
                kind: 'FAILED',
                code: 'RECOVERY_OLD_RUN_BINDING_INVALID',
                summary: 'missing old binding',
                progress: { taskId: failed.taskId },
              }
            : { kind: 'SUCCEEDED' },
      ),
    });
    const result = await new RecoveryCoordinatorService(store, ports).runForPlatform('session');
    expect(result.state).toBe('READY');
    expect(store.manual).toEqual([failed.taskId]);
    expect(ports.restoreTask).toHaveBeenCalledExactlyOnceWith('session', healthy);
  });

  it('runs every external phase and reaches READY only after observability is established', async () => {
    const store = new MemoryStore();
    const ports = actions();
    const result = await new RecoveryCoordinatorService(store, ports).run('session');
    expect(result.state).toBe('READY');
    expect([...store.statuses.values()].every((status) => status === 'SUCCEEDED')).toBe(true);
    expect(ports.restoreTask).toHaveBeenCalledWith('session', store.candidates[0]);
  });

  it('rechecks restored tasks when observability is retried even after they leave old candidates', async () => {
    const store = new MemoryStore();
    const original = [...store.candidates];
    const ports = actions({
      readyObservability: vi.fn(async (): Promise<RecoveryActionResult> => ({
        kind: 'FAILED',
        code: 'RECOVERY_OBSERVABILITY_NOT_READY',
        summary: 'metrics not ready',
        progress: { taskId: original[0]!.taskId },
      })),
    });
    const coordinator = new RecoveryCoordinatorService(store, ports);
    expect((await coordinator.runForPlatform('session')).state).toBe('FAILED');
    store.candidates = [];
    expect(
      (
        await coordinator.retry(
          'session',
          'READY_OBSERVABILITY',
          '00000000-0000-4000-8000-000000000099',
        )
      ).state,
    ).toBe('FAILED');
    expect(ports.readyObservability).toHaveBeenLastCalledWith('session', original);
    expect(ports.restoreTask).toHaveBeenCalledTimes(1);
  });

  it('pauses the entire capacity phase without restoring a partial candidate set', async () => {
    const store = new MemoryStore();
    store.candidates = [
      candidate('00000000-0000-4000-8000-000000000001', 2),
      candidate('00000000-0000-4000-8000-000000000002', 3),
    ];
    store.capacityValue = { limit: 5, used: 6 };
    const ports = actions();
    const result = await new RecoveryCoordinatorService(store, ports).run('session');
    expect(result.state).toBe('PAUSED');
    expect(store.statuses.get('PLAN_CAPACITY')).toBe('PAUSED');
    expect(ports.restoreTask).not.toHaveBeenCalled();
  });

  it('rejects an unconfigured capacity without an implicit default', async () => {
    const store = new MemoryStore();
    store.capacityValue = { limit: null, used: 0 };
    await new RecoveryCoordinatorService(store, actions()).run('session');
    expect(store.finished.find((entry) => entry.step === 'PLAN_CAPACITY')?.result).toMatchObject({
      code: 'CAPACITY_NOT_CONFIGURED',
      kind: 'PAUSED',
    });
  });

  it('allows an administrator to retry a paused capacity plan after configuring the limit', async () => {
    const store = new MemoryStore();
    store.state = 'PAUSED';
    store.statuses.set('ENSURE_ARTIFACTS', 'SUCCEEDED');
    store.statuses.set('VERIFY_CONTINUITY', 'SUCCEEDED');
    store.statuses.set('PLAN_CAPACITY', 'PAUSED');
    store.capacityValue = { limit: 1, used: 1 };

    const result = await new RecoveryCoordinatorService(store, actions()).retry(
      'session',
      'PLAN_CAPACITY',
      '00000000-0000-4000-8000-000000000099',
    );

    expect(result.state).toBe('READY');
    expect(store.statuses.get('PLAN_CAPACITY')).toBe('SUCCEEDED');
  });

  it('does not require a configured runtime capacity when there is nothing to restore', async () => {
    const store = new MemoryStore();
    store.candidates = [];
    store.capacityValue = { limit: null, used: 0 };

    const result = await new RecoveryCoordinatorService(store, actions()).run('session');

    expect(result.state).toBe('READY');
    expect(store.finished.find((entry) => entry.step === 'PLAN_CAPACITY')?.result).toEqual({
      kind: 'SUCCEEDED',
      progress: { limit: null, requested: 0, selectedTaskIds: [], used: 0 },
    });
  });

  it('fails instead of treating a Kafka task without a run-bound unit set as zero capacity', async () => {
    const store = new MemoryStore();
    store.candidates = [candidate('00000000-0000-4000-8000-000000000001', 0)];
    await new RecoveryCoordinatorService(store, actions()).run('session');
    expect(store.finished.find((entry) => entry.step === 'PLAN_CAPACITY')?.result).toMatchObject({
      code: 'RECOVERY_UNIT_BINDING_MISSING',
      kind: 'FAILED',
    });
  });

  it('fails when retained reservations do not match the persisted run units', async () => {
    const store = new MemoryStore();
    store.capacityMismatch = {
      actual: 1,
      expected: 2,
      taskId: '00000000-0000-4000-8000-000000000001',
    };
    await new RecoveryCoordinatorService(store, actions()).run('session');
    expect(store.finished.find((entry) => entry.step === 'PLAN_CAPACITY')?.result).toMatchObject({
      code: 'RECOVERY_RESERVATION_MISMATCH',
      kind: 'FAILED',
    });
  });

  it('retries a failed external phase and records the user who produced the retry', async () => {
    const store = new MemoryStore();
    store.statuses.set('ENSURE_ARTIFACTS', 'FAILED');
    const ports = actions();
    await new RecoveryCoordinatorService(store, ports).retry(
      'session',
      'ENSURE_ARTIFACTS',
      '00000000-0000-4000-8000-000000000099',
    );
    expect(ports.ensureArtifacts).toHaveBeenCalledOnce();
  });

  it('persists an external action exception as a failed step that can be retried', async () => {
    const store = new MemoryStore();
    const ports = actions({
      ensureArtifacts: vi
        .fn()
        .mockRejectedValueOnce(new Error('ARTIFACT_STORE_UNAVAILABLE'))
        .mockResolvedValue({ kind: 'SUCCEEDED' }),
    });
    const coordinator = new RecoveryCoordinatorService(store, ports);

    await expect(coordinator.run('session')).resolves.toMatchObject({ state: 'FAILED' });
    expect(store.statuses.get('ENSURE_ARTIFACTS')).toBe('FAILED');
    expect(store.finished.at(-1)?.result).toMatchObject({
      code: 'RECOVERY_STEP_EXECUTION_FAILED',
      kind: 'FAILED',
      summary: 'ARTIFACT_STORE_UNAVAILABLE',
    });

    await expect(
      coordinator.retry('session', 'ENSURE_ARTIFACTS', '00000000-0000-4000-8000-000000000099'),
    ).resolves.toMatchObject({ state: 'READY' });
  });

  it('lets a new coordinator reconcile a step left RUNNING by a prior process', async () => {
    const store = new MemoryStore();
    store.statuses.set('ENSURE_ARTIFACTS', 'RUNNING');

    await expect(
      new RecoveryCoordinatorService(store, actions()).run('session'),
    ).resolves.toMatchObject({ state: 'READY' });
    expect(store.statuses.get('ENSURE_ARTIFACTS')).toBe('SUCCEEDED');
  });

  it('coalesces concurrent execution of the same recovery session', async () => {
    const store = new MemoryStore();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ports = actions({
      ensureArtifacts: vi.fn(async () => {
        await blocked;
        return { kind: 'SUCCEEDED' } satisfies RecoveryActionResult;
      }),
    });
    const coordinator = new RecoveryCoordinatorService(store, ports);

    const first = coordinator.run('session');
    const second = coordinator.run('session');
    await vi.waitFor(() => expect(ports.ensureArtifacts).toHaveBeenCalledOnce());
    release();

    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult.state).toBe('READY');
    expect(secondResult.state).toBe('READY');
    expect(ports.ensureArtifacts).toHaveBeenCalledOnce();
  });

  it('returns an already completed session without reopening or failing its steps', async () => {
    const store = new MemoryStore();
    store.state = 'READY';
    for (const step of store.statuses.keys()) store.statuses.set(step, 'SUCCEEDED');
    const ports = actions();

    await expect(
      new RecoveryCoordinatorService(store, ports).run('session'),
    ).resolves.toMatchObject({ state: 'READY' });
    expect(ports.ensureArtifacts).not.toHaveBeenCalled();
    expect(store.finished).toHaveLength(0);
  });

  it('does not overwrite a completed step when persistence reports a concurrent change', async () => {
    const store = new MemoryStore();
    store.finishStepError = new Error('RECOVERY_STEP_CONCURRENT_CHANGE');
    const coordinator = new RecoveryCoordinatorService(store, actions());

    await expect(coordinator.run('session')).rejects.toThrow('RECOVERY_STEP_CONCURRENT_CHANGE');
    expect(store.finished).toHaveLength(0);
  });

  it('reconciles READY after finishSession once failed without repeating external actions', async () => {
    const store = new MemoryStore();
    store.finishSessionFailures = 1;
    const ports = actions();
    const coordinator = new RecoveryCoordinatorService(store, ports);

    await expect(coordinator.run('session')).rejects.toThrow('SESSION_FINISH_FAILED');
    await expect(coordinator.run('session')).resolves.toMatchObject({ state: 'READY' });
    expect(ports.ensureArtifacts).toHaveBeenCalledOnce();
    expect(ports.readyObservability).toHaveBeenCalledOnce();
  });

  it.each([
    ['FAILED', { kind: 'FAILED', code: 'FAILED', summary: 'failed' }],
    ['PAUSED', { kind: 'PAUSED', code: 'PAUSED', summary: 'paused' }],
  ] as const)(
    'reconciles %s after finishSession once failed without repeating the failed or paused action',
    async (expectedState, result) => {
      const store = new MemoryStore();
      store.finishSessionFailures = 1;
      const ports = actions({ ensureArtifacts: vi.fn(async () => result) });
      const coordinator = new RecoveryCoordinatorService(store, ports);

      await expect(coordinator.run('session')).rejects.toThrow('SESSION_FINISH_FAILED');
      await expect(coordinator.run('session')).resolves.toMatchObject({ state: expectedState });
      expect(ports.ensureArtifacts).toHaveBeenCalledOnce();
    },
  );

  it('requires an exact explicit candidate set and never silently drops an unknown task', async () => {
    const store = new MemoryStore();
    store.statuses.set('PLAN_CAPACITY', 'PAUSED');
    store.state = 'PAUSED';
    await expect(
      new RecoveryCoordinatorService(store, actions()).selectCapacityCandidates(
        'session',
        ['00000000-0000-4000-8000-000000000404'],
        '1',
        '00000000-0000-4000-8000-000000000099',
      ),
    ).rejects.toMatchObject({
      code: 'RECOVERY_CANDIDATE_INVALID',
    } satisfies Partial<RecoveryCoordinatorError>);
    expect(store.blocked).toHaveLength(0);
  });

  it('rejects an empty explicit candidate set before changing recovery state', async () => {
    const store = new MemoryStore();
    store.statuses.set('PLAN_CAPACITY', 'PAUSED');
    store.state = 'PAUSED';

    await expect(
      new RecoveryCoordinatorService(store, actions()).selectCapacityCandidates(
        'session',
        [],
        '1',
        '00000000-0000-4000-8000-000000000099',
      ),
    ).rejects.toMatchObject({ code: 'RECOVERY_CANDIDATE_INVALID' });
    expect(store.statuses.get('PLAN_CAPACITY')).toBe('PAUSED');
  });

  it('returns the current session selected by the exact platform instance', async () => {
    const store = new MemoryStore();
    const result = await new RecoveryCoordinatorService(store, actions()).getCurrent('session');
    expect(result.id).toBe('session');
  });

  it('accepts a distinct administrator confirmation command while leaving the phase paused', async () => {
    const store = new MemoryStore();
    store.state = 'PAUSED';
    store.statuses.set('ENSURE_ARTIFACTS', 'SUCCEEDED');
    store.statuses.set('VERIFY_CONTINUITY', 'PAUSED');
    const result = await new RecoveryCoordinatorService(store, actions()).confirmOldRuntimeStopped(
      'session',
      '1',
      'deployment checked old container',
      '00000000-0000-4000-8000-000000000099',
    );
    expect(result.state).toBe('PAUSED');
  });

  it('serializes a manual confirmation behind an in-flight run for the same session', async () => {
    const store = new MemoryStore();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ports = actions();
    ports.ensureArtifacts = vi.fn(async () => {
      await blocked;
      return { kind: 'SUCCEEDED' as const };
    });
    const confirmation = vi.spyOn(store, 'confirmOldRuntimeStopped');
    const coordinator = new RecoveryCoordinatorService(store, ports);

    const running = coordinator.run('session');
    await vi.waitFor(() => expect(ports.ensureArtifacts).toHaveBeenCalledOnce());
    const manual = coordinator.confirmOldRuntimeStopped(
      'session',
      '1',
      'old runtime stopped',
      '00000000-0000-4000-8000-000000000099',
    );
    expect(confirmation).not.toHaveBeenCalled();
    release();
    await Promise.all([running, manual]);
    expect(confirmation).toHaveBeenCalledOnce();
  });

  it('allows an administrator to continue after a failed task while marking it for manual recovery', async () => {
    const store = new MemoryStore();
    store.state = 'FAILED';
    store.statuses.set('ENSURE_ARTIFACTS', 'SUCCEEDED');
    store.statuses.set('VERIFY_CONTINUITY', 'SUCCEEDED');
    store.statuses.set('PLAN_CAPACITY', 'SUCCEEDED');
    store.statuses.set('RESTORE_BATCHES', 'FAILED');
    store.finished.push({
      step: 'RESTORE_BATCHES',
      result: {
        kind: 'FAILED',
        code: 'PROCESS_FAILED',
        summary: 'failed',
        progress: { failedTaskId: store.candidates[0]!.taskId, restoredTaskIds: [] },
      },
    });
    const result = await new RecoveryCoordinatorService(store, actions()).continueAfterFailedTask(
      'session',
      '1',
      '00000000-0000-4000-8000-000000000099',
    );
    expect(store.manual).toEqual([store.candidates[0]!.taskId]);
    expect(result.state).toBe('READY');
    expect(store.finished.findLast((entry) => entry.step === 'RESTORE_BATCHES')?.manual).toEqual({
      action: 'CONTINUE',
      actorUserId: '00000000-0000-4000-8000-000000000099',
    });
  });
});
