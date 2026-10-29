import type { TaskType } from '@scs/contracts';

export type RecoveryStepType =
  | 'LOAD_METADATA'
  | 'UNLOCK_KEYS'
  | 'ENSURE_ARTIFACTS'
  | 'VERIFY_CONTINUITY'
  | 'PLAN_CAPACITY'
  | 'RESTORE_BATCHES'
  | 'READY_OBSERVABILITY';
export type RecoveryStepStatus = 'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'PAUSED';

export interface RecoveryCandidate {
  readonly revisionId: string;
  readonly taskId: string;
  readonly taskName: string;
  readonly taskType: TaskType;
  readonly unitKeys: readonly string[];
  readonly units: number;
}

export interface RecoveryStepSnapshot {
  readonly attemptCount: number;
  readonly errorSummary: string | null;
  readonly failureCode: string | null;
  readonly position: number;
  readonly progress: Readonly<Record<string, unknown>>;
  readonly status: RecoveryStepStatus;
  readonly stepType: RecoveryStepType;
}

export interface RecoverySessionSnapshot {
  readonly candidates: readonly RecoveryCandidate[];
  readonly id: string;
  readonly rowVersion: string;
  readonly state: 'RUNNING' | 'READY' | 'FAILED' | 'PAUSED';
  readonly steps: readonly RecoveryStepSnapshot[];
}

export type RecoveryActionResult =
  | { readonly kind: 'SUCCEEDED'; readonly progress?: Readonly<Record<string, unknown>> }
  | {
      readonly kind: 'FAILED';
      readonly code: string;
      readonly summary: string;
      readonly progress?: Readonly<Record<string, unknown>>;
    }
  | {
      readonly kind: 'PAUSED';
      readonly code: string;
      readonly summary: string;
      readonly progress?: Readonly<Record<string, unknown>>;
    };

export interface RecoveryExternalActions {
  ensureArtifacts(
    sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult>;
  verifyContinuity(
    sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult>;
  restoreTask(sessionId: string, candidate: RecoveryCandidate): Promise<RecoveryActionResult>;
  readyObservability(
    sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult>;
}

export interface RecoveryStore {
  getSession(sessionId: string): Promise<RecoverySessionSnapshot | null>;
  getCurrentSession(platformInstanceId: string): Promise<RecoverySessionSnapshot | null>;
  listCandidates(sessionId: string): Promise<readonly RecoveryCandidate[]>;
  beginStep(
    sessionId: string,
    stepType: RecoveryStepType,
    allowed: readonly RecoveryStepStatus[],
    actorUserId?: string,
  ): Promise<boolean>;
  finishStep(
    sessionId: string,
    stepType: RecoveryStepType,
    result: RecoveryActionResult,
    manualAction?: { readonly action: 'CONTINUE' | 'SELECT'; readonly actorUserId: string },
  ): Promise<void>;
  capacity(candidates: readonly RecoveryCandidate[]): Promise<
    | {
        readonly kind: 'MISMATCH';
        readonly actual: number;
        readonly expected: number;
        readonly taskId: string;
      }
    | {
        readonly kind: 'READY';
        readonly limit: number | null;
        readonly retained: number;
        readonly used: number;
      }
  >;
  confirmOldRuntimeStopped(
    sessionId: string,
    expectedRowVersion: string,
    reason: string,
    actorUserId: string,
  ): Promise<'APPLIED' | 'CONFLICT' | 'NOT_ACTIONABLE'>;
  selectCandidates(
    sessionId: string,
    taskIds: readonly string[],
    expectedRowVersion: string,
  ): Promise<readonly RecoveryCandidate[]>;
  applyCapacitySelection(
    sessionId: string,
    taskIds: readonly string[],
    expectedRowVersion: string,
    actorUserId: string,
  ): Promise<
    | { readonly kind: 'CONFLICT' }
    | { readonly kind: 'INVALID' }
    | { readonly kind: 'NOT_CONFIGURED' }
    | {
        readonly kind: 'EXCEEDED';
        readonly limit: number;
        readonly requested: number;
        readonly used: number;
      }
    | {
        readonly kind: 'APPLIED';
        readonly candidates: readonly RecoveryCandidate[];
      }
  >;
  markCapacityBlocked(taskIds: readonly string[]): Promise<void>;
  markManualRequired(taskId: string, reason?: string, sessionId?: string): Promise<void>;
  finishSession(sessionId: string, state: 'READY' | 'FAILED' | 'PAUSED'): Promise<void>;
}
