import { Inject, Injectable } from '@nestjs/common';
import type { UserSummary } from '@scs/contracts';
import {
  LogPipeline,
  type LogPipelineLimitUpdateResult,
  type LogPipelineLimitsSnapshot,
  type LogPipelineSettlementResult,
} from '@scs/observability';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import type {
  LogStorageRecoverySummary,
  PostgresLogSegmentStore,
} from '../storage/postgres-log-segment-store.js';
import { LOG_PIPELINE, POSTGRES_LOG_SEGMENT_STORE } from '../storage/storage.tokens.js';

export class LogPipelineCoordinatorError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'LogPipelineCoordinatorError';
  }
}

export interface LogStorageRetryResult {
  readonly durableRecovery: LogStorageRecoverySummary | null;
  readonly recoveryState: LogPipelineSettlementResult['recoveryState'];
  readonly runId: string;
  readonly storageState: LogPipelineSettlementResult['storageState'];
  readonly taskId: string;
}

@Injectable()
export class LogPipelineCoordinatorService {
  private highestSeenRowVersion: string | null = null;
  private limitMutation: Promise<void> = Promise.resolve();
  private readonly retryOperations = new Map<string, Promise<LogStorageRetryResult>>();
  private synchronization: Promise<LogPipelineLimitUpdateResult | null> | null = null;
  private synchronizedRunLimitBytes: number | null = null;
  private synchronizedRowVersion: string | null = null;

  constructor(
    private readonly database: PlatformDatabaseService,
    @Inject(LOG_PIPELINE) private readonly pipeline: LogPipeline,
    @Inject(POSTGRES_LOG_SEGMENT_STORE) private readonly store: PostgresLogSegmentStore,
  ) {}

  snapshotLimits(): LogPipelineLimitsSnapshot {
    return this.pipeline.snapshotLimits();
  }

  async synchronizeFromDatabase(): Promise<LogPipelineLimitUpdateResult | null> {
    if (this.synchronization !== null) return await this.synchronization;
    const work = this.serializeLimitMutation(async () => await this.synchronizeOnce()).finally(
      () => {
        if (this.synchronization === work) this.synchronization = null;
      },
    );
    this.synchronization = work;
    return await work;
  }

  async applyLimits(input: {
    readonly maxBufferedBytes: number;
    readonly maxRunBytes: number;
    readonly rowVersion: string;
  }): Promise<LogPipelineLimitUpdateResult> {
    return await this.serializeLimitMutation(async () => await this.applyLimitsOnce(input));
  }

  private async applyLimitsOnce(input: {
    readonly maxBufferedBytes: number;
    readonly maxRunBytes: number;
    readonly rowVersion: string;
  }): Promise<LogPipelineLimitUpdateResult> {
    const rowVersion = parseRowVersion(input.rowVersion);
    const highestSeen =
      this.highestSeenRowVersion === null ? null : parseRowVersion(this.highestSeenRowVersion);
    const synchronized =
      this.synchronizedRowVersion === null ? null : parseRowVersion(this.synchronizedRowVersion);
    if (
      (highestSeen !== null && rowVersion < highestSeen) ||
      (synchronized !== null && rowVersion <= synchronized)
    ) {
      return this.noopLimitUpdate();
    }
    if (highestSeen === null || rowVersion > highestSeen) {
      this.highestSeenRowVersion = input.rowVersion;
    }
    const result = await this.pipeline.applyLimits({
      maxBufferedBytes: input.maxBufferedBytes,
      maxRunBytes: input.maxRunBytes,
    });
    if (
      this.synchronizedRunLimitBytes === null ||
      this.synchronizedRunLimitBytes !== input.maxRunBytes
    ) {
      await this.store.enforceAllRunCapacities(input.maxRunBytes);
    }
    this.synchronizedRunLimitBytes = input.maxRunBytes;
    this.synchronizedRowVersion = input.rowVersion;
    return result;
  }

  async retryStorage(
    actor: UserSummary,
    taskId: string,
    runId: string,
  ): Promise<LogStorageRetryResult> {
    await this.assertRunAccess(actor, taskId, runId);
    const key = `${taskId}\u0000${runId}`;
    const existing = this.retryOperations.get(key);
    if (existing !== undefined) return await existing;
    const work = this.retryStorageOnce(taskId, runId).finally(() => {
      if (this.retryOperations.get(key) === work) this.retryOperations.delete(key);
    });
    this.retryOperations.set(key, work);
    return await work;
  }

  private async retryStorageOnce(taskId: string, runId: string): Promise<LogStorageRetryResult> {
    try {
      await this.pipeline.retryStorage(runId);
      const memoryState = this.pipeline.snapshotRunState(runId);
      let state: LogPipelineSettlementResult;
      let durableRecovery: LogStorageRecoverySummary | null = null;
      if (memoryState.recoveryState === 'NONE') {
        durableRecovery = await this.store.recoverPendingOperations(runId);
        await this.pipeline.retryStorage(runId);
        state = await this.pipeline.settleRunWithStatus(runId);
      } else {
        state = await this.pipeline.settleRunWithStatus(runId);
      }
      if (state.recoveryState === 'DURABLE_PENDING') {
        durableRecovery = await this.store.recoverPendingOperations(runId);
        await this.pipeline.retryStorage(runId);
        state = await this.pipeline.settleRunWithStatus(runId);
      }
      const persisted = await this.database.requirePool().query<{
        log_recovery_state: LogStorageRetryResult['recoveryState'];
        log_storage_state: LogStorageRetryResult['storageState'];
      }>(
        `SELECT log_recovery_state, log_storage_state
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid
            AND task_id = $2::uuid
            AND record_purged_at IS NULL`,
        [runId, taskId],
      );
      const row = persisted.rows[0];
      if (row === undefined) throw new LogPipelineCoordinatorError('RUNTIME_LOG_RUN_NOT_FOUND');
      return {
        durableRecovery,
        recoveryState: row.log_recovery_state,
        runId,
        storageState: row.log_storage_state,
        taskId,
      };
    } catch (error) {
      if (error instanceof LogPipelineCoordinatorError) throw error;
      const code =
        typeof error === 'object' &&
        error !== null &&
        'code' in error &&
        typeof error.code === 'string' &&
        /^[A-Z][A-Z0-9_]+$/u.test(error.code)
          ? error.code
          : 'LOG_STORAGE_RETRY_FAILED';
      throw new LogPipelineCoordinatorError(code);
    }
  }

  private async synchronizeOnce(): Promise<LogPipelineLimitUpdateResult | null> {
    const result = await this.database.requirePool().query<{
      log_failure_buffer_bytes: string;
      row_version: string;
      run_log_limit_bytes: string;
    }>(
      `SELECT log_failure_buffer_bytes::text, run_log_limit_bytes::text, row_version::text
         FROM ape_dts_platform.platform_settings
        WHERE singleton_id = 1`,
    );
    const row = result.rows[0];
    if (row === undefined) throw new LogPipelineCoordinatorError('LOG_SETTINGS_UNAVAILABLE');
    if (
      this.synchronizedRowVersion !== null &&
      parseRowVersion(row.row_version) <= parseRowVersion(this.synchronizedRowVersion)
    ) {
      return null;
    }
    const maxBufferedBytes = Number(row.log_failure_buffer_bytes);
    const maxRunBytes = Number(row.run_log_limit_bytes);
    if (
      !Number.isSafeInteger(maxBufferedBytes) ||
      maxBufferedBytes <= 0 ||
      !Number.isSafeInteger(maxRunBytes) ||
      maxRunBytes <= 0
    ) {
      throw new LogPipelineCoordinatorError('LOG_SETTINGS_VALUE_INVALID');
    }
    return await this.applyLimitsOnce({
      maxBufferedBytes,
      maxRunBytes,
      rowVersion: row.row_version,
    });
  }

  private noopLimitUpdate(): LogPipelineLimitUpdateResult {
    const snapshot = this.pipeline.snapshotLimits();
    return {
      current: snapshot,
      droppedBufferedBytes: 0,
      droppedBufferedSegments: 0,
      previous: snapshot,
      sealedSegments: 0,
    };
  }

  private async serializeLimitMutation<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.limitMutation.then(operation, operation);
    this.limitMutation = result.then(
      () => undefined,
      () => undefined,
    );
    return await result;
  }

  private async assertRunAccess(actor: UserSummary, taskId: string, runId: string): Promise<void> {
    const result = await this.database.requirePool().query(
      `SELECT run.id
         FROM ape_dts_platform.task_runs AS run
         JOIN ape_dts_platform.tasks AS task ON task.id = run.task_id
        WHERE task.id = $1::uuid
          AND run.id = $2::uuid
          AND task.deleted_at IS NULL
          AND run.record_purged_at IS NULL
          AND ($3::text = 'ADMIN' OR NOT EXISTS (
            SELECT required.data_source_id
              FROM (VALUES (task.source_data_source_id), (task.target_data_source_id)) AS required(data_source_id)
             WHERE NOT EXISTS (
               SELECT 1
                 FROM ape_dts_platform.data_source_grants AS grant_row
                WHERE grant_row.user_id = $4::uuid
                  AND grant_row.data_source_id = required.data_source_id
                  AND grant_row.revoked_at IS NULL
             )
          ))`,
      [taskId, runId, actor.role, actor.id],
    );
    if (result.rowCount !== 1) throw new LogPipelineCoordinatorError('RUNTIME_LOG_RUN_NOT_FOUND');
  }
}

function parseRowVersion(value: string): bigint {
  if (!/^[1-9]\d*$/u.test(value)) {
    throw new LogPipelineCoordinatorError('LOG_SETTINGS_VERSION_INVALID');
  }
  return BigInt(value);
}
