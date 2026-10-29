import { Injectable, Logger, type OnApplicationShutdown } from '@nestjs/common';
import { recoveryCandidateSchema } from '@scs/contracts';

import type {
  RecoveryActionResult,
  RecoveryCandidate,
  RecoveryExternalActions,
  RecoverySessionSnapshot,
  RecoveryStepStatus,
  RecoveryStepType,
  RecoveryStore,
} from './recovery.types.js';

export type RecoveryCoordinatorErrorCode =
  | 'RECOVERY_ACCESS_DENIED'
  | 'RECOVERY_CAPACITY_NOT_CONFIGURED'
  | 'RECOVERY_CAPACITY_EXCEEDED'
  | 'RECOVERY_CANDIDATE_INVALID'
  | 'RECOVERY_SESSION_CONFLICT'
  | 'RECOVERY_SESSION_NOT_FOUND'
  | 'RECOVERY_STEP_NOT_ACTIONABLE';

export class RecoveryCoordinatorError extends Error {
  constructor(
    readonly code: RecoveryCoordinatorErrorCode,
    readonly details?: Readonly<Record<string, unknown>>,
  ) {
    super(code);
    this.name = 'RecoveryCoordinatorError';
  }
}

const automaticSteps: readonly RecoveryStepType[] = [
  'ENSURE_ARTIFACTS',
  'VERIFY_CONTINUITY',
  'PLAN_CAPACITY',
  'RESTORE_BATCHES',
  'READY_OBSERVABILITY',
];

@Injectable()
export class RecoveryCoordinatorService implements OnApplicationShutdown {
  private readonly sessionFlights = new Map<string, Promise<RecoverySessionSnapshot>>();

  constructor(
    private readonly store: RecoveryStore,
    private readonly actions: RecoveryExternalActions,
  ) {}

  async get(sessionId: string): Promise<RecoverySessionSnapshot> {
    const session = await this.store.getSession(sessionId);
    if (session === null) throw new RecoveryCoordinatorError('RECOVERY_SESSION_NOT_FOUND');
    return session;
  }

  async getCurrent(platformInstanceId: string): Promise<RecoverySessionSnapshot> {
    const session = await this.store.getCurrentSession(platformInstanceId);
    if (session === null) throw new RecoveryCoordinatorError('RECOVERY_SESSION_NOT_FOUND');
    return session;
  }

  async run(sessionId: string): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, () => this.runUnclaimed(sessionId));
  }

  async runCandidate(
    sessionId: string,
    candidate: RecoveryCandidate,
  ): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, async () => {
      const session = await this.runUnclaimed(sessionId, [candidate]);
      if (session.state === 'FAILED' || session.state === 'PAUSED') {
        const step = session.steps.find((item) => ['FAILED', 'PAUSED'].includes(item.status));
        await this.store.markManualRequired(
          candidate.taskId,
          step?.failureCode ?? 'RECOVERY_TASK_FAILED',
          sessionId,
        );
      }
      return {
        ...session,
        candidates: session.candidates.filter((item) => item.taskId === candidate.taskId),
      };
    });
  }

  startForPlatform(sessionId: string): void {
    void this.runForPlatform(sessionId).catch(() => {
      // Exceptions may contain connection secrets; task errors are persisted separately.
      Logger.error(
        'TASK_RECOVERY_FAILED: inspect the recovery session and metadata database availability',
        'PlatformRecovery',
      );
    });
  }

  async onApplicationShutdown(): Promise<void> {
    await Promise.allSettled([...this.sessionFlights.values()]);
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.onApplicationShutdown();
  }

  async runForPlatform(sessionId: string): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, async () => {
      let candidates = await this.store.listCandidates(sessionId);
      let session = await this.runUnclaimed(sessionId, candidates);
      const excluded = new Set<string>();
      while (session.state === 'FAILED' || session.state === 'PAUSED') {
        const step = session.steps.find((item) => ['FAILED', 'PAUSED'].includes(item.status));
        if (step === undefined || step.stepType === 'READY_OBSERVABILITY') break;
        const taskId = step.progress.taskId ?? step.progress.failedTaskId;
        if (
          typeof taskId !== 'string' ||
          excluded.has(taskId) ||
          !candidates.some((item) => item.taskId === taskId)
        )
          break;
        excluded.add(taskId);
        candidates = candidates.filter((item) => item.taskId !== taskId);
        await this.store.markManualRequired(
          taskId,
          step.failureCode ?? 'RECOVERY_TASK_FAILED',
          sessionId,
        );
        const excludedTasks = [
          ...(Array.isArray(step.progress.excludedTasks) ? step.progress.excludedTasks : []),
          { taskId, code: step.failureCode, summary: step.errorSummary },
        ];
        await this.store.finishStep(sessionId, step.stepType, {
          kind: step.status === 'PAUSED' ? 'PAUSED' : 'FAILED',
          code: step.failureCode ?? 'RECOVERY_TASK_FAILED',
          summary: step.errorSummary ?? '任务恢复失败，转为人工处理。',
          progress: { ...step.progress, excludedTasks },
        });
        const result = await this.executeStep(
          sessionId,
          step.stepType,
          ['FAILED', 'PAUSED'],
          candidates,
        );
        session =
          result.kind === 'SUCCEEDED'
            ? await this.runUnclaimed(sessionId, candidates)
            : await this.reconcileSessionState(sessionId);
      }
      return session;
    });
  }

  private async runUnclaimed(
    sessionId: string,
    candidates?: readonly RecoveryCandidate[],
  ): Promise<RecoverySessionSnapshot> {
    let session = await this.reconcileSessionState(sessionId);
    if (session.state !== 'RUNNING') return session;
    for (const step of automaticSteps) {
      const current = session.steps.find((candidate) => candidate.stepType === step);
      if (current?.status === 'SUCCEEDED') continue;
      if (current === undefined || !['PENDING', 'RUNNING'].includes(current.status)) break;
      const result = await this.executeStep(sessionId, step, ['PENDING', 'RUNNING'], candidates);
      if (result.kind !== 'SUCCEEDED') break;
      session = await this.get(sessionId);
    }
    return await this.reconcileSessionState(sessionId);
  }

  async retry(
    sessionId: string,
    stepType: RecoveryStepType,
    actorUserId: string,
  ): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, async () => {
      await this.reconcileSessionState(sessionId);
      const result = await this.executeStep(
        sessionId,
        stepType,
        ['VERIFY_CONTINUITY', 'PLAN_CAPACITY'].includes(stepType)
          ? ['FAILED', 'PAUSED', 'RUNNING']
          : ['FAILED', 'RUNNING'],
        undefined,
        actorUserId,
      );
      if (result.kind === 'SUCCEEDED') {
        return await this.runUnclaimed(sessionId);
      }
      return await this.reconcileSessionState(sessionId);
    });
  }

  private async reconcileSessionState(sessionId: string): Promise<RecoverySessionSnapshot> {
    let session = await this.get(sessionId);
    if (session.state !== 'RUNNING') return session;
    const target = session.steps.some((step) => step.status === 'FAILED')
      ? 'FAILED'
      : session.steps.some((step) => step.status === 'PAUSED')
        ? 'PAUSED'
        : session.steps.find((step) => step.stepType === 'READY_OBSERVABILITY')?.status ===
            'SUCCEEDED'
          ? 'READY'
          : null;
    if (target !== null) {
      await this.store.finishSession(sessionId, target);
      session = await this.get(sessionId);
    }
    return session;
  }

  async confirmOldRuntimeStopped(
    sessionId: string,
    expectedRowVersion: string,
    reason: string,
    actorUserId: string,
  ): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, async () => {
      const result = await this.store.confirmOldRuntimeStopped(
        sessionId,
        expectedRowVersion,
        reason,
        actorUserId,
      );
      if (result === 'CONFLICT') throw new RecoveryCoordinatorError('RECOVERY_SESSION_CONFLICT');
      if (result === 'NOT_ACTIONABLE')
        throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE', {
          stepType: 'VERIFY_CONTINUITY',
        });
      return await this.get(sessionId);
    });
  }

  async selectCapacityCandidates(
    sessionId: string,
    taskIds: readonly string[],
    expectedRowVersion: string,
    actorUserId: string,
  ): Promise<RecoverySessionSnapshot> {
    if (taskIds.length === 0) {
      throw new RecoveryCoordinatorError('RECOVERY_CANDIDATE_INVALID');
    }
    return await this.withSessionFlight(sessionId, async () => {
      const selection = await this.store.applyCapacitySelection(
        sessionId,
        taskIds,
        expectedRowVersion,
        actorUserId,
      );
      if (selection.kind === 'CONFLICT')
        throw new RecoveryCoordinatorError('RECOVERY_SESSION_CONFLICT');
      if (selection.kind === 'INVALID')
        throw new RecoveryCoordinatorError('RECOVERY_CANDIDATE_INVALID');
      if (selection.kind === 'NOT_CONFIGURED')
        throw new RecoveryCoordinatorError('RECOVERY_CAPACITY_NOT_CONFIGURED');
      if (selection.kind === 'EXCEEDED') {
        throw new RecoveryCoordinatorError('RECOVERY_CAPACITY_EXCEEDED', {
          limit: selection.limit,
          requested: selection.requested,
          used: selection.used,
        });
      }
      await this.executeStep(sessionId, 'RESTORE_BATCHES', ['PENDING'], selection.candidates);
      await this.executeStep(sessionId, 'READY_OBSERVABILITY', ['PENDING']);
      return await this.get(sessionId);
    });
  }

  async continueAfterFailedTask(
    sessionId: string,
    expectedRowVersion: string,
    actorUserId: string,
  ): Promise<RecoverySessionSnapshot> {
    return await this.withSessionFlight(sessionId, async () => {
      const session = await this.get(sessionId);
      if (session.rowVersion !== expectedRowVersion)
        throw new RecoveryCoordinatorError('RECOVERY_SESSION_CONFLICT');
      const restore = session.steps.find((step) => step.stepType === 'RESTORE_BATCHES');
      if (restore?.status !== 'FAILED')
        throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE');
      const failedTaskId =
        typeof restore.progress.failedTaskId === 'string' ? restore.progress.failedTaskId : null;
      if (failedTaskId !== null) await this.store.markManualRequired(failedTaskId);
      await this.store.finishStep(
        sessionId,
        'RESTORE_BATCHES',
        { kind: 'SUCCEEDED', progress: { ...restore.progress, continuedManually: true } },
        { action: 'CONTINUE', actorUserId },
      );
      await this.executeStep(sessionId, 'READY_OBSERVABILITY', ['PENDING']);
      return await this.get(sessionId);
    });
  }

  private async executeStep(
    sessionId: string,
    stepType: RecoveryStepType,
    allowed: readonly RecoveryStepStatus[],
    selected?: readonly RecoveryCandidate[],
    actorUserId?: string,
  ): Promise<RecoveryActionResult> {
    const priorStep = (await this.get(sessionId)).steps.find((step) => step.stepType === stepType);
    if (!(await this.store.beginStep(sessionId, stepType, allowed, actorUserId)))
      throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE', { stepType });
    let result: RecoveryActionResult;
    try {
      let candidates = selected ?? (await this.store.listCandidates(sessionId));
      if (stepType === 'READY_OBSERVABILITY' && selected === undefined) {
        const restore = (await this.get(sessionId)).steps.find(
          (step) => step.stepType === 'RESTORE_BATCHES',
        );
        if (restore?.progress.restoredCandidates !== undefined) {
          candidates = recoveryCandidateSchema.array().parse(restore.progress.restoredCandidates);
        } else if (
          Array.isArray(restore?.progress.restoredTaskIds) &&
          restore.progress.restoredTaskIds.length > 0
        ) {
          throw new Error('RECOVERY_OBSERVABILITY_BINDING_MISSING');
        }
      }
      switch (stepType) {
        case 'ENSURE_ARTIFACTS':
          result = await this.actions.ensureArtifacts(sessionId, candidates);
          break;
        case 'VERIFY_CONTINUITY':
          result = await this.actions.verifyContinuity(sessionId, candidates);
          break;
        case 'PLAN_CAPACITY':
          result = await this.planCapacity(candidates);
          break;
        case 'RESTORE_BATCHES':
          result = await this.restore(sessionId, candidates);
          break;
        case 'READY_OBSERVABILITY':
          result = await this.actions.readyObservability(sessionId, candidates);
          break;
        default:
          result = {
            kind: 'FAILED',
            code: 'RECOVERY_STEP_NOT_OWNED',
            summary: `${stepType} is owned by bootstrap`,
          };
      }
    } catch (error) {
      result = {
        kind: 'FAILED',
        code: 'RECOVERY_STEP_EXECUTION_FAILED',
        summary: recoveryErrorSummary(error),
      };
    }
    if (Array.isArray(priorStep?.progress.excludedTasks)) {
      result = {
        ...result,
        progress: { ...result.progress, excludedTasks: priorStep.progress.excludedTasks },
      };
    }
    await this.store.finishStep(sessionId, stepType, result);
    if (stepType === 'READY_OBSERVABILITY' && result.kind === 'SUCCEEDED')
      await this.store.finishSession(sessionId, 'READY');
    else if (result.kind === 'FAILED') await this.store.finishSession(sessionId, 'FAILED');
    else if (result.kind === 'PAUSED') await this.store.finishSession(sessionId, 'PAUSED');
    return result;
  }

  private async withSessionFlight(
    sessionId: string,
    work: () => Promise<RecoverySessionSnapshot>,
  ): Promise<RecoverySessionSnapshot> {
    const existing = this.sessionFlights.get(sessionId);
    const flight = existing === undefined ? work() : existing.catch(() => undefined).then(work);
    this.sessionFlights.set(sessionId, flight);
    try {
      return await flight;
    } finally {
      if (this.sessionFlights.get(sessionId) === flight) this.sessionFlights.delete(sessionId);
    }
  }

  private async planCapacity(
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult> {
    if (candidates.length === 0) {
      return {
        kind: 'SUCCEEDED',
        progress: { limit: null, requested: 0, selectedTaskIds: [], used: 0 },
      };
    }
    const missingBinding = candidates.find((candidate) => candidate.units === 0);
    if (missingBinding !== undefined)
      return {
        kind: 'FAILED',
        code: 'RECOVERY_UNIT_BINDING_MISSING',
        summary: '恢复候选缺少最近一次运行的运行单元绑定。',
        progress: { taskId: missingBinding.taskId },
      };
    const capacity = await this.store.capacity(candidates);
    if (capacity.kind === 'MISMATCH')
      return {
        kind: 'FAILED',
        code: 'RECOVERY_RESERVATION_MISMATCH',
        summary: '旧运行的容量 reservation 与恢复单元数量不一致。',
        progress: { actual: capacity.actual, expected: capacity.expected, taskId: capacity.taskId },
      };
    const requested = candidates.reduce((sum, candidate) => sum + candidate.units, 0);
    if (capacity.limit === null)
      return {
        kind: 'PAUSED',
        code: 'CAPACITY_NOT_CONFIGURED',
        summary: '最大并发运行单元数尚未配置。',
        progress: { requested, used: capacity.used },
      };
    if (capacity.used > capacity.limit)
      return {
        kind: 'PAUSED',
        code: 'CAPACITY_EXCEEDED',
        summary: '全部自动恢复候选超过容量上限，恢复阶段整体暂停。',
        progress: { candidates, limit: capacity.limit, requested, used: capacity.used },
      };
    return {
      kind: 'SUCCEEDED',
      progress: {
        limit: capacity.limit,
        requested,
        selectedTaskIds: candidates.map((candidate) => candidate.taskId),
        used: capacity.used,
      },
    };
  }

  private async restore(
    sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult> {
    const previous = (await this.get(sessionId)).steps.find(
      (step) => step.stepType === 'RESTORE_BATCHES',
    )?.progress.restoredTaskIds;
    const restoredTaskIds: string[] = Array.isArray(previous)
      ? previous.filter((id): id is string => typeof id === 'string')
      : [];
    const previousCandidates = (await this.get(sessionId)).steps.find(
      (step) => step.stepType === 'RESTORE_BATCHES',
    )?.progress.restoredCandidates;
    const restoredCandidates: RecoveryCandidate[] =
      previousCandidates === undefined
        ? []
        : recoveryCandidateSchema.array().parse(previousCandidates);
    for (const candidate of candidates) {
      if (restoredTaskIds.includes(candidate.taskId)) continue;
      const result = await this.actions.restoreTask(sessionId, candidate);
      if (result.kind !== 'SUCCEEDED')
        return {
          kind: 'FAILED',
          code: result.code,
          summary: result.summary,
          progress: { failedTaskId: candidate.taskId, restoredTaskIds, restoredCandidates },
        };
      restoredTaskIds.push(candidate.taskId);
      restoredCandidates.push(candidate);
    }
    return { kind: 'SUCCEEDED', progress: { restoredTaskIds, restoredCandidates } };
  }
}

function recoveryErrorSummary(error: unknown): string {
  return error instanceof Error && error.message.length > 0
    ? error.message
    : 'Recovery step execution failed';
}
