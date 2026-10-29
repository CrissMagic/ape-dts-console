import { Injectable, Logger, type OnModuleInit, type OnApplicationShutdown } from '@nestjs/common';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { TaskProgressService } from '../task-progress/task-progress.service.js';
import { RuntimeSupervisorService } from './runtime-supervisor.service.js';
import { recoveryFailureReason } from './recovery-failure.js';

export const PG_DISCONNECTED_GRACE_MS = 60_000;
const SAMPLE_MAX_AGE_MS = 45_000;

@Injectable()
export class PostgresReplicationWatchdog implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(PostgresReplicationWatchdog.name);
  private readonly disconnected = new Map<
    string,
    { since: number; lastSample: number; reported: boolean }
  >();
  private timer?: ReturnType<typeof setInterval>;
  private running = false;
  private stopping = false;
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly readiness: PlatformReadinessService,
    private readonly progress: TaskProgressService,
    private readonly runtime: RuntimeSupervisorService,
  ) {}
  onModuleInit() {
    this.timer = setInterval(() => void this.tick(), 15_000);
    this.timer.unref();
  }
  onApplicationShutdown() {
    this.stopping = true;
    clearInterval(this.timer);
    this.disconnected.clear();
  }
  async tick(): Promise<void> {
    if (this.running || this.stopping) return;
    if (!this.database.hasPool() || this.readiness.getPhase() !== 'READY') {
      this.disconnected.clear();
      return;
    }
    this.running = true;
    try {
      const snapshots = await this.progress.listForMonitoring();
      const active = this.runtime.listActiveRuntimeTasks();
      const retained = new Set<string>();
      for (const run of active) {
        if (this.stopping) return;
        const sample = snapshots.find(
          (s) => s.taskId === run.taskId && s.revisionId === run.revisionId,
        );
        const sampledAt = Date.parse(sample?.collectedAt ?? '');
        const age = Date.now() - sampledAt;
        if (
          sample?.sourceType !== 'POSTGRES' ||
          sample.status !== 'AVAILABLE' ||
          sample.slotStatus !== 'INACTIVE' ||
          !['CDC_STARTED', 'MONITORING_READY'].includes(sample.currentStage ?? '') ||
          !Number.isFinite(age) ||
          age < 0 ||
          age > SAMPLE_MAX_AGE_MS
        )
          continue;
        retained.add(run.runId);
        let state = this.disconnected.get(run.runId);
        if (
          !state ||
          sampledAt < state.lastSample ||
          sampledAt - state.lastSample > SAMPLE_MAX_AGE_MS
        ) {
          state = { since: sampledAt, lastSample: sampledAt, reported: false };
          this.disconnected.set(run.runId, state);
        }
        state.lastSample = sampledAt;
        // A repeated cached sample must not advance the grace period.
        if (sampledAt - state.since < PG_DISCONNECTED_GRACE_MS) continue;
        if (!state.reported) {
          this.logger.error({
            code: 'TASK_PG_REPLICATION_STALLED',
            taskId: run.taskId,
            runId: run.runId,
          });
          state.reported = true;
        }
        try {
          if (
            await this.runtime.recoverDisconnectedPostgres({
              taskId: run.taskId,
              runId: run.runId,
              revisionId: run.revisionId,
              collectedAt: sample.collectedAt,
            })
          )
            this.disconnected.delete(run.runId);
        } catch (error) {
          this.logger.error({
            code: 'TASK_PG_WATCHDOG_RECOVERY_FAILED',
            taskId: run.taskId,
            runId: run.runId,
            reason: recoveryFailureReason(error),
          });
        }
      }
      for (const runId of this.disconnected.keys())
        if (!retained.has(runId)) this.disconnected.delete(runId);
    } catch (error) {
      this.disconnected.clear();
      this.logger.warn({
        code: 'PG_REPLICATION_WATCHDOG_UNAVAILABLE',
        reason: recoveryFailureReason(error),
      });
    } finally {
      this.running = false;
    }
  }
}
