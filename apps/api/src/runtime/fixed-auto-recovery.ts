import { Injectable, Logger, type OnApplicationShutdown } from '@nestjs/common';
import type { RuntimeRecoveryPolicy } from '@scs/contracts';
import { recoveryFailureReason } from './recovery-failure.js';

export const AUTO_RECOVERY_RETRY_DELAY_MS = 30_000;
export const AUTO_RECOVERY_ROLLING_WINDOW_MS = 10 * 60_000;
export const AUTO_RECOVERY_HEALTHY_RESET_MS = 30 * 60_000;

export type AutoRecoveryActorType = 'SYSTEM';

export class AutoRecoveryManualRequiredError extends Error {
  constructor(readonly reason: string) {
    super(reason);
    this.name = 'AutoRecoveryManualRequiredError';
  }
}

export type AutoRecoveryState =
  | 'HEALTHY'
  | 'RETRY_WAIT'
  | 'AUTO_PREFLIGHT'
  | 'RECOVERED'
  | 'MANUAL_REQUIRED'
  | 'MANUAL_PERSIST_FAILED';

export interface AutoRecoveryBinding {
  readonly expectedTaskRowVersion: string;
  readonly platformInstanceId: string;
  readonly processToken: string;
  readonly revisionId: string;
  readonly runId: string;
  readonly taskId: string;
}

export interface AutoRecoveryExit extends AutoRecoveryBinding {
  /**
   * Eligibility is decided by the runtime preflight/error classifier.  False
   * covers user stop, hard configuration/identity/resource/artifact errors,
   * capacity errors and Metrics-only degradation; those events neither start
   * nor consume an automatic recovery allowance.
   */
  readonly eligible: boolean;
}

export interface AutoRecoveryContext extends AutoRecoveryBinding {
  readonly actorType: AutoRecoveryActorType;
  readonly firstFailureAt: Date;
}

export interface AutoRecoveryPort {
  policy?(): Promise<RuntimeRecoveryPolicy>;
  /** Re-run the real task preflight against the exact retained binding. */
  preflight(context: AutoRecoveryContext): Promise<void>;
  /**
   * Restart atomically while retaining/transferring the old run reservation.
   * Implementations must reject stale desired state, revision or process token.
   */
  restartWithRetainedCapacity(context: AutoRecoveryContext): Promise<AutoRecoveryBinding>;
  /** Persist the manual gate and release the retained reservation. */
  requireManualRecovery(context: AutoRecoveryContext, reason: string): Promise<void>;
  /** Emit the R32 warning for first exit and automatic recovery attempt. */
  warn(context: AutoRecoveryContext, event: 'FIRST_EXIT' | 'AUTO_RECOVERY_ATTEMPT'): Promise<void>;
}

export interface AutoRecoveryScheduler {
  clear(handle: unknown): void;
  now(): number;
  schedule(callback: () => void, delayMs: number): unknown;
}

const systemScheduler: AutoRecoveryScheduler = {
  clear: (handle) => clearTimeout(handle as NodeJS.Timeout),
  now: () => Date.now(),
  schedule: (callback, delayMs) => setTimeout(callback, delayMs),
};

interface RecoveryEntry {
  pendingExit?: AutoRecoveryExit;
  attempts: number;
  policy: RuntimeRecoveryPolicy;
  binding: AutoRecoveryBinding;
  firstFailureAtMs: number;
  generation: number;
  manualReason: string | null;
  state: AutoRecoveryState;
  timer: unknown | null;
}

@Injectable()
export class FixedAutoRecoveryCoordinator implements OnApplicationShutdown {
  private readonly entries = new Map<string, RecoveryEntry>();
  private shuttingDown = false;
  private readonly logger = new Logger('TaskAutoRecovery');

  constructor(
    private readonly port: AutoRecoveryPort,
    private readonly scheduler: AutoRecoveryScheduler = systemScheduler,
  ) {}

  state(taskId: string): AutoRecoveryState {
    return this.entries.get(taskId)?.state ?? 'HEALTHY';
  }

  async observeUnexpectedExit(exit: AutoRecoveryExit): Promise<AutoRecoveryState> {
    if (this.shuttingDown || !exit.eligible) {
      return this.state(exit.taskId);
    }
    const existing = this.entries.get(exit.taskId);
    const now = this.scheduler.now();
    if (existing !== undefined) {
      if (!sameRecoveryRun(existing.binding, exit)) {
        // A newly started process may exit before restartWithRetainedCapacity returns its binding.
        if (
          existing.state === 'AUTO_PREFLIGHT' &&
          existing.binding.revisionId === exit.revisionId &&
          existing.binding.platformInstanceId === exit.platformInstanceId
        )
          existing.pendingExit = exit;
        return existing.state;
      }
      if (existing.state !== 'HEALTHY') {
        if (existing.state === 'RETRY_WAIT' || existing.state === 'AUTO_PREFLIGHT')
          return existing.state;
        existing.binding = copyBinding(exit);
        if (existing.state === 'MANUAL_PERSIST_FAILED' && existing.manualReason !== null) {
          await this.moveToManual(existing, existing.manualReason);
          return 'MANUAL_REQUIRED';
        }
        if (existing.state === 'MANUAL_REQUIRED') return existing.state;
        existing.policy = await this.readPolicy();
        if (this.shuttingDown || this.entries.get(exit.taskId) !== existing)
          return this.state(exit.taskId);
        if (existing.attempts < existing.policy.maxAttempts && existing.policy.enabled) {
          this.scheduleAttempt(existing);
          return existing.state;
        }
        const withinRollingWindow =
          now - existing.firstFailureAtMs <= AUTO_RECOVERY_ROLLING_WINDOW_MS;
        await this.moveToManual(
          existing,
          withinRollingWindow
            ? 'RUNTIME_AUTO_RECOVERY_LIMIT_REACHED'
            : 'RUNTIME_AUTO_RECOVERY_ALLOWANCE_NOT_RESET',
        );
        return 'MANUAL_REQUIRED';
      }
      this.clearEntryTimer(existing);
      this.entries.delete(exit.taskId);
    }

    const entry: RecoveryEntry = {
      attempts: 0,
      policy: { enabled: false, maxAttempts: 1, retryDelaySeconds: 30 },
      binding: copyBinding(exit),
      firstFailureAtMs: now,
      generation: 1,
      manualReason: null,
      state: 'RETRY_WAIT',
      timer: null,
    };
    this.entries.set(exit.taskId, entry);
    entry.policy = await this.readPolicy();
    if (this.shuttingDown || this.entries.get(exit.taskId) !== entry)
      return this.state(exit.taskId);
    if (!entry.policy.enabled) {
      await this.moveToManual(entry, 'RUNTIME_AUTO_RECOVERY_DISABLED');
      return entry.state;
    }
    const context = recoveryContext(entry);
    try {
      await this.port.warn(context, 'FIRST_EXIT');
    } catch (error) {
      await this.moveToManual(entry, 'RUNTIME_AUTO_RECOVERY_WARNING_PERSIST_FAILED');
      throw error;
    }
    if (!this.shuttingDown && this.entries.get(exit.taskId) === entry) this.scheduleAttempt(entry);
    return entry.state;
  }

  private readPolicy(): Promise<RuntimeRecoveryPolicy> {
    return (
      this.port.policy?.() ??
      Promise.resolve({
        enabled: true,
        maxAttempts: 1,
        retryDelaySeconds: AUTO_RECOVERY_RETRY_DELAY_MS / 1000,
      })
    );
  }

  private scheduleAttempt(entry: RecoveryEntry) {
    this.clearEntryTimer(entry);
    entry.state = 'RETRY_WAIT';
    const delaySeconds = Math.min(
      entry.policy.retryDelaySeconds * 2 ** entry.attempts,
      Math.max(entry.policy.retryDelaySeconds, 900),
    );
    this.logger.warn({
      code: 'TASK_AUTO_RECOVERY_SCHEDULED',
      taskId: entry.binding.taskId,
      runId: entry.binding.runId,
      nextAttempt: entry.attempts + 1,
      maxAttempts: entry.policy.maxAttempts,
      delaySeconds,
    });
    entry.timer = this.scheduler.schedule(() => {
      void this.attempt(entry).catch(() =>
        this.logger.error({
          code: 'TASK_AUTO_RECOVERY_PERSIST_FAILED',
          taskId: entry.binding.taskId,
          runId: entry.binding.runId,
        }),
      );
    }, delaySeconds * 1000);
  }

  /** Call only after the restarted binding is confirmed continuously healthy. */
  markRecovered(binding: AutoRecoveryBinding): void {
    const entry = this.entries.get(binding.taskId);
    if (entry === undefined || !sameBinding(entry.binding, binding)) {
      return;
    }
    this.clearEntryTimer(entry);
    entry.state = 'RECOVERED';
    const generation = ++entry.generation;
    entry.timer = this.scheduler.schedule(() => {
      const current = this.entries.get(binding.taskId);
      if (current === entry && current.generation === generation && current.state === 'RECOVERED') {
        this.entries.delete(binding.taskId);
      }
    }, AUTO_RECOVERY_HEALTHY_RESET_MS);
  }

  /** User stop and explicit manual recovery cancel in-memory scheduling. */
  cancel(taskId: string): void {
    const entry = this.entries.get(taskId);
    if (entry !== undefined) {
      this.clearEntryTimer(entry);
      this.entries.delete(taskId);
    }
  }

  onApplicationShutdown(): void {
    this.shuttingDown = true;
    for (const entry of this.entries.values()) {
      this.clearEntryTimer(entry);
    }
    this.entries.clear();
  }

  private async attempt(entry: RecoveryEntry): Promise<void> {
    if (this.shuttingDown || this.entries.get(entry.binding.taskId) !== entry) {
      return;
    }
    entry.timer = null;
    entry.state = 'AUTO_PREFLIGHT';
    const context = recoveryContext(entry);
    try {
      entry.policy = await this.readPolicy();
      if (this.shuttingDown || this.entries.get(context.taskId) !== entry) return;
      if (!entry.policy.enabled || entry.attempts >= entry.policy.maxAttempts) {
        await this.moveToManual(
          entry,
          !entry.policy.enabled
            ? 'RUNTIME_AUTO_RECOVERY_DISABLED'
            : 'RUNTIME_AUTO_RECOVERY_LIMIT_REACHED',
        );
        return;
      }
      entry.attempts += 1;
      this.logger.warn({
        code: 'TASK_AUTO_RECOVERY_ATTEMPT',
        taskId: context.taskId,
        runId: context.runId,
        attempt: entry.attempts,
        maxAttempts: entry.policy.maxAttempts,
      });
      await this.port.warn(context, 'AUTO_RECOVERY_ATTEMPT');
      if (this.shuttingDown || this.entries.get(context.taskId) !== entry) return;
      await this.port.preflight(context);
      if (this.shuttingDown || this.entries.get(entry.binding.taskId) !== entry) {
        return;
      }
      const restartedBinding = await this.port.restartWithRetainedCapacity(context);
      if (this.entries.get(entry.binding.taskId) === entry) {
        if (restartedBinding.taskId !== entry.binding.taskId) {
          throw new Error('RUNTIME_AUTO_RECOVERY_TASK_BINDING_CHANGED');
        }
        entry.binding = copyBinding(restartedBinding);
        this.markRecovered(entry.binding);
        this.logger.log({
          code: 'TASK_AUTO_RECOVERY_SUCCEEDED',
          taskId: entry.binding.taskId,
          runId: entry.binding.runId,
          attempts: entry.attempts,
        });
        const pending = entry.pendingExit;
        delete entry.pendingExit;
        if (pending && sameRecoveryRun(entry.binding, pending))
          await this.observeUnexpectedExit(pending);
      }
    } catch (error) {
      if (this.shuttingDown || this.entries.get(context.taskId) !== entry) return;
      if (error instanceof AutoRecoveryManualRequiredError) {
        entry.state = 'MANUAL_REQUIRED';
        return;
      }
      this.logger.error({
        code: 'TASK_AUTO_RECOVERY_ATTEMPT_FAILED',
        taskId: context.taskId,
        runId: context.runId,
        attempt: entry.attempts,
        reason: safeRecoveryReason(error),
      });
      if (
        !this.shuttingDown &&
        this.entries.get(context.taskId) === entry &&
        entry.attempts < entry.policy.maxAttempts
      ) {
        this.scheduleAttempt(entry);
        return;
      }
      await this.moveToManual(entry, safeRecoveryReason(error));
    }
  }

  private async moveToManual(entry: RecoveryEntry, reason: string): Promise<void> {
    if (this.entries.get(entry.binding.taskId) !== entry) {
      return;
    }
    this.clearEntryTimer(entry);
    entry.manualReason = reason;
    this.logger.error({
      code: 'TASK_AUTO_RECOVERY_MANUAL_REQUIRED',
      taskId: entry.binding.taskId,
      runId: entry.binding.runId,
      attempts: entry.attempts,
      reason: safeRecoveryReason(new Error(reason)),
    });
    try {
      await this.port.requireManualRecovery(recoveryContext(entry), reason);
    } catch (error) {
      entry.state = 'MANUAL_PERSIST_FAILED';
      throw error;
    }
    entry.manualReason = null;
    entry.state = 'MANUAL_REQUIRED';
  }

  private clearEntryTimer(entry: RecoveryEntry): void {
    if (entry.timer !== null) {
      this.scheduler.clear(entry.timer);
      entry.timer = null;
    }
  }
}

function copyBinding(binding: AutoRecoveryBinding): AutoRecoveryBinding {
  return {
    processToken: binding.processToken,
    expectedTaskRowVersion: binding.expectedTaskRowVersion,
    platformInstanceId: binding.platformInstanceId,
    revisionId: binding.revisionId,
    runId: binding.runId,
    taskId: binding.taskId,
  };
}

function sameBinding(left: AutoRecoveryBinding, right: AutoRecoveryBinding): boolean {
  return (
    left.taskId === right.taskId &&
    left.runId === right.runId &&
    left.revisionId === right.revisionId &&
    left.processToken === right.processToken &&
    left.expectedTaskRowVersion === right.expectedTaskRowVersion &&
    left.platformInstanceId === right.platformInstanceId
  );
}

function sameRecoveryRun(left: AutoRecoveryBinding, right: AutoRecoveryBinding): boolean {
  return (
    left.taskId === right.taskId &&
    left.runId === right.runId &&
    left.revisionId === right.revisionId &&
    left.platformInstanceId === right.platformInstanceId
  );
}

function recoveryContext(entry: RecoveryEntry): AutoRecoveryContext {
  return {
    ...entry.binding,
    actorType: 'SYSTEM',
    firstFailureAt: new Date(entry.firstFailureAtMs),
  };
}

function safeRecoveryReason(error: unknown) {
  return recoveryFailureReason(error, 'RUNTIME_AUTO_RECOVERY_FAILED');
}
