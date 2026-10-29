import { randomUUID } from 'node:crypto';
import path from 'node:path';

import {
  createStorageProfileSnapshot,
  LOCAL_STORAGE_ROOT,
  LogSegmentPersistenceError,
  LocalLogSegmentObjectStore,
  segmentObjectKey,
  segmentSha256,
  type LogGapRecord,
  type LogRecoveryState,
  type LogSegmentRecord,
  type LogSegmentStore,
  type LogStorageState,
} from '@scs/storage';
import type { PoolClient } from 'pg';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

export type PostgresLogSegmentStoreErrorCode =
  | 'LOG_GAP_METADATA_CONFLICT'
  | 'LOG_RUN_NOT_FOUND'
  | 'LOG_SEGMENT_CONTENT_MISMATCH'
  | 'LOG_SEGMENT_METADATA_CONFLICT'
  | 'LOG_STORAGE_DRIVER_UNSUPPORTED'
  | 'LOG_STORAGE_PROFILE_CONFIG_INVALID'
  | 'LOG_STORAGE_PROFILE_NOT_FOUND'
  | 'LOG_STORAGE_PROFILE_NOT_WRITABLE'
  | 'LOG_STORAGE_PROFILE_UNAVAILABLE';

export class PostgresLogSegmentStoreError extends Error {
  constructor(readonly code: PostgresLogSegmentStoreErrorCode) {
    super(code);
    this.name = 'PostgresLogSegmentStoreError';
  }
}

interface StorageProfileRow {
  readonly config: unknown;
  readonly driver: string;
  readonly id: string;
  readonly profile_key: string;
  readonly secret_id: string | null;
  readonly state: string;
  readonly version: number;
}

interface ExistingSegmentRow {
  readonly byte_size: string;
  readonly created_at: Date;
  readonly created_by_actor_type: string;
  readonly created_by_user_id: string | null;
  readonly ended_at: Date | null;
  readonly object_id: string;
  readonly object_key: string;
  readonly object_sha256: Buffer | null;
  readonly object_size_bytes: string;
  readonly object_state: string;
  readonly run_id: string;
  readonly run_unit_id: string | null;
  readonly segment_sha256: Buffer | null;
  readonly segment_state: string;
  readonly sequence: string;
  readonly started_at: Date;
  readonly storage_profile_id: string;
  readonly stream: string;
}

interface PreparedSegment {
  readonly objectId: string;
  readonly objectKey: string;
  readonly profile: StorageProfileRow;
}

interface DeletionRow extends StorageProfileRow {
  readonly deleted_reason: string | null;
  readonly object_id: string;
  readonly object_key: string;
  readonly object_state: string;
  readonly segment_state: string;
}

interface DeletionRequestRow {
  readonly completed_at: Date | null;
  readonly deleted_at: Date;
  readonly log_segment_id: string;
  readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
  readonly run_id: string;
  readonly stored_object_id: string;
}

interface PreparedDeletion extends DeletionRow {
  readonly deletedAt: Date;
  readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
  readonly runId: string;
  readonly segmentId: string;
}

interface PendingSegmentRow extends StorageProfileRow {
  readonly byte_size: string;
  readonly ended_at: Date;
  readonly object_id: string;
  readonly object_key: string;
  readonly run_id: string;
  readonly run_unit_id: string | null;
  readonly segment_id: string;
  readonly sha256: Buffer;
  readonly started_at: Date;
}

interface PendingDeletionRow extends StorageProfileRow {
  readonly deleted_at: Date;
  readonly object_id: string;
  readonly object_key: string;
  readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
  readonly run_id: string;
  readonly segment_id: string;
}

interface CapacitySegmentRow {
  readonly byte_size: string;
  readonly created_at: Date;
  readonly ended_at: Date | null;
  readonly id: string;
  readonly run_id: string;
  readonly run_unit_id: string;
  readonly sequence: string;
  readonly sha256: Buffer;
  readonly started_at: Date;
  readonly stream: 'STDERR' | 'STDOUT';
  readonly total_bytes: string;
}

export interface LogStorageRecoverySummary {
  readonly availableSegments: number;
  readonly capacityDeletedSegments: number;
  readonly completedDeletions: number;
  readonly failures: number;
  readonly integrityFailedSegments: number;
  readonly memoryPendingGaps: number;
  readonly missingSegments: number;
}

function sameInstant(left: Date | null, right: Date | null): boolean {
  return left?.getTime() === right?.getTime();
}

function bytesEqualHex(value: Buffer | null, expected: string): boolean {
  return value !== null && value.toString('hex') === expected;
}

function storageFailureCode(error: unknown): string {
  if (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string' &&
    error.code.length > 0
  ) {
    return error.code;
  }
  if (error instanceof Error && /^[A-Z][A-Z0-9_]+$/u.test(error.message)) {
    return error.message;
  }
  return 'LOG_SEGMENT_PERSIST_FAILED';
}

/** PostgreSQL metadata plus immutable LOCAL log objects for RuntimeLogPipelineSink. */
export class PostgresLogSegmentStore implements LogSegmentStore {
  private recoveryWork: Promise<void> = Promise.resolve();

  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly localRoot = LOCAL_STORAGE_ROOT,
  ) {
    // Validate the configured deployment root before the first runtime starts.
    new LocalLogSegmentObjectStore(localRoot);
  }

  async persistSegment(input: {
    readonly content: Buffer;
    readonly segment: LogSegmentRecord;
  }): Promise<void> {
    let prepared: PreparedSegment;
    try {
      this.validateSegmentContent(input.segment, input.content);
      prepared = await this.prepareSegment(input.segment);
    } catch (error) {
      throw new LogSegmentPersistenceError('MEMORY_PENDING', storageFailureCode(error), {
        cause: error,
      });
    }
    try {
      const objects = this.localObjects(prepared.profile);
      await objects.put({ content: input.content, objectKey: prepared.objectKey });
      await this.finalizeSegment(input.segment.id, input.segment.runId, prepared.objectId);
    } catch (error) {
      throw new LogSegmentPersistenceError('DURABLE_PENDING', storageFailureCode(error), {
        cause: error,
      });
    }
  }

  async deleteSegment(input: {
    readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
    readonly segment: LogSegmentRecord;
  }): Promise<void> {
    const prepared = await this.prepareDeletion(input);
    if (prepared === null) {
      return;
    }
    try {
      await this.localObjects(prepared).delete(prepared.object_key);
    } catch (error) {
      await this.recordDeletionFailure(prepared.segmentId).catch(() => undefined);
      throw new LogSegmentPersistenceError('DURABLE_PENDING', 'LOG_SEGMENT_DELETE_FAILED', {
        cause: error,
      });
    }
    try {
      await this.finalizeDeletion(prepared);
    } catch (error) {
      throw new LogSegmentPersistenceError(
        'DURABLE_PENDING',
        'LOG_SEGMENT_DELETE_FINALIZE_FAILED',
        {
          cause: error,
        },
      );
    }
  }

  async recordGap(gap: LogGapRecord): Promise<void> {
    await this.transaction(async (client) => {
      const result = await client.query(
        `INSERT INTO ape_dts_platform.log_gaps (
         id, run_id, run_unit_id, reason, started_at, ended_at,
         lost_bytes, occurred_at, container_restart
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::timestamptz,
                 $6::timestamptz, $7::bigint, $8::timestamptz, $9::boolean)
       ON CONFLICT (id) DO UPDATE
         SET id = EXCLUDED.id
       WHERE log_gaps.run_id = EXCLUDED.run_id
         AND log_gaps.run_unit_id IS NOT DISTINCT FROM EXCLUDED.run_unit_id
         AND log_gaps.reason = EXCLUDED.reason
         AND log_gaps.started_at IS NOT DISTINCT FROM EXCLUDED.started_at
         AND log_gaps.ended_at IS NOT DISTINCT FROM EXCLUDED.ended_at
         AND log_gaps.lost_bytes IS NOT DISTINCT FROM EXCLUDED.lost_bytes
         AND log_gaps.occurred_at = EXCLUDED.occurred_at
         AND log_gaps.container_restart = EXCLUDED.container_restart
         RETURNING id`,
        [
          gap.id,
          gap.runId,
          gap.runUnitId,
          gap.reason,
          gap.startedAt,
          gap.endedAt,
          gap.lostBytes,
          gap.occurredAt,
          gap.containerRestart,
        ],
      );
      if (result.rowCount !== 1) {
        throw new PostgresLogSegmentStoreError('LOG_GAP_METADATA_CONFLICT');
      }
      const run = await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET has_gap = true
          WHERE id = $1::uuid
        RETURNING id`,
        [gap.runId],
      );
      if (run.rowCount !== 1) {
        throw new PostgresLogSegmentStoreError('LOG_RUN_NOT_FOUND');
      }
    });
  }

  async recoverPendingOperations(runId?: string): Promise<LogStorageRecoverySummary> {
    const result = this.recoveryWork.then(
      async () => await this.recoverPendingOperationsOnce(runId),
      async () => await this.recoverPendingOperationsOnce(runId),
    );
    this.recoveryWork = result.then(
      () => undefined,
      () => undefined,
    );
    return await result;
  }

  private async recoverPendingOperationsOnce(runId?: string): Promise<LogStorageRecoverySummary> {
    const summary = {
      availableSegments: 0,
      capacityDeletedSegments: 0,
      completedDeletions: 0,
      failures: 0,
      integrityFailedSegments: 0,
      memoryPendingGaps: 0,
      missingSegments: 0,
    };
    const memoryPendingRuns = await this.database.requirePool().query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.task_runs
        WHERE log_recovery_state IN ('MEMORY_PENDING', 'MIXED_PENDING')
          AND ($1::uuid IS NULL OR id = $1::uuid)
        ORDER BY started_at, id`,
      [runId ?? null],
    );
    for (const run of memoryPendingRuns.rows) {
      try {
        if (await this.recordRestartedMemoryGap(run.id)) {
          summary.memoryPendingGaps += 1;
        }
      } catch {
        summary.failures += 1;
      }
    }
    await this.markDurableRecoveryRunsDegraded(runId);
    const deletions = await this.database.requirePool().query<PendingDeletionRow>(
      `SELECT requests.log_segment_id AS segment_id,
              requests.run_id,
              requests.stored_object_id AS object_id,
              requests.reason,
              requests.deleted_at,
              objects.object_key,
              profiles.id,
              profiles.profile_key,
              profiles.version,
              profiles.driver,
              profiles.config,
              profiles.secret_id,
              profiles.state
         FROM ape_dts_platform.log_segment_deletion_requests AS requests
         JOIN ape_dts_platform.stored_objects AS objects
           ON objects.id = requests.stored_object_id
         JOIN ape_dts_platform.storage_profiles AS profiles
           ON profiles.id = objects.storage_profile_id
        WHERE requests.completed_at IS NULL
          AND ($1::uuid IS NULL OR requests.run_id = $1::uuid)
        ORDER BY requests.requested_at, requests.log_segment_id`,
      [runId ?? null],
    );
    for (const deletion of deletions.rows) {
      try {
        await this.localObjects(deletion).delete(deletion.object_key);
        await this.finalizeDeletion({
          ...deletion,
          deleted_reason: null,
          deletedAt: deletion.deleted_at,
          object_id: deletion.object_id,
          object_state: 'AVAILABLE',
          reason: deletion.reason,
          runId: deletion.run_id,
          segmentId: deletion.segment_id,
          segment_state: 'AVAILABLE',
        });
        summary.completedDeletions += 1;
      } catch {
        summary.failures += 1;
        await this.recordDeletionFailure(deletion.segment_id).catch(() => undefined);
      }
    }

    const segments = await this.database.requirePool().query<PendingSegmentRow>(
      `SELECT segments.id AS segment_id,
              segments.run_id,
              segments.run_unit_id,
              segments.started_at,
              segments.ended_at,
              segments.byte_size::text,
              objects.id AS object_id,
              objects.object_key,
              objects.sha256,
              profiles.id,
              profiles.profile_key,
              profiles.version,
              profiles.driver,
              profiles.config,
              profiles.secret_id,
              profiles.state
         FROM ape_dts_platform.log_segments AS segments
         JOIN ape_dts_platform.stored_objects AS objects
           ON objects.id = segments.stored_object_id
         JOIN ape_dts_platform.storage_profiles AS profiles
           ON profiles.id = objects.storage_profile_id
        WHERE segments.state = 'SEALED'
          AND objects.state = 'PENDING'
          AND ($1::uuid IS NULL OR segments.run_id = $1::uuid)
          AND NOT EXISTS (
            SELECT 1
              FROM ape_dts_platform.log_segment_deletion_requests AS requests
             WHERE requests.log_segment_id = segments.id
               AND requests.completed_at IS NULL
          )
        ORDER BY segments.created_at, segments.id`,
      [runId ?? null],
    );
    for (const segment of segments.rows) {
      try {
        const inspection = await this.localObjects(segment).inspect({
          objectKey: segment.object_key,
          sha256: segment.sha256.toString('hex'),
          sizeBytes: Number(segment.byte_size),
        });
        if (inspection === 'MATCH') {
          await this.finalizeSegment(segment.segment_id, segment.run_id, segment.object_id);
          summary.availableSegments += 1;
          continue;
        }
        await this.markRecoveredSegmentUnavailable(
          segment,
          inspection === 'MISSING' ? 'MISSING' : 'INTEGRITY_FAILED',
        );
        if (inspection === 'MISSING') {
          summary.missingSegments += 1;
        } else {
          summary.integrityFailedSegments += 1;
        }
      } catch {
        summary.failures += 1;
      }
    }
    const configuredLimit = await this.database.requirePool().query<{ max_bytes: string }>(
      `SELECT run_log_limit_bytes::text AS max_bytes
         FROM ape_dts_platform.platform_settings
        WHERE singleton_id = 1`,
    );
    const maxBytes = Number(configuredLimit.rows[0]?.max_bytes);
    if (Number.isSafeInteger(maxBytes) && maxBytes > 0) {
      if (runId === undefined) {
        const capacity = await this.enforceAllRunCapacities(maxBytes);
        summary.capacityDeletedSegments += capacity.deletedSegments;
        summary.failures += capacity.failures;
      } else {
        const before = await this.availableSegmentCount(runId);
        try {
          await this.enforceRunCapacity({ maxBytes, runId });
          summary.capacityDeletedSegments += before - (await this.availableSegmentCount(runId));
        } catch {
          summary.failures += 1;
        }
      }
    } else {
      summary.failures += 1;
    }
    await this.reconcileRunRecoveryStates(runId);
    return summary;
  }

  async enforceRunCapacity(input: {
    readonly maxBytes: number;
    readonly runId: string;
  }): Promise<void> {
    if (!Number.isSafeInteger(input.maxBytes) || input.maxBytes <= 0) {
      throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_CONFIG_INVALID');
    }
    while (true) {
      const result = await this.database.requirePool().query<CapacitySegmentRow>(
        `SELECT segment.id,
                segment.run_id,
                segment.run_unit_id,
                segment.stream,
                segment.sequence::text,
                segment.started_at,
                segment.ended_at,
                segment.byte_size::text,
                segment.sha256,
                segment.created_at,
                totals.total_bytes::text
           FROM ape_dts_platform.log_segments AS segment
           CROSS JOIN LATERAL (
             SELECT COALESCE(sum(candidate.byte_size), 0) AS total_bytes
               FROM ape_dts_platform.log_segments AS candidate
              WHERE candidate.run_id = $1::uuid
                AND candidate.state = 'AVAILABLE'
           ) AS totals
          WHERE segment.run_id = $1::uuid
            AND segment.state = 'AVAILABLE'
          ORDER BY segment.started_at, segment.sequence, segment.id
          LIMIT 1`,
        [input.runId],
      );
      const candidate = result.rows[0];
      if (candidate === undefined || BigInt(candidate.total_bytes) <= BigInt(input.maxBytes))
        return;
      await this.deleteSegment({
        reason: 'CAPACITY_ROLLOVER',
        segment: {
          byteSize: Number(candidate.byte_size),
          createdAt: candidate.created_at,
          deletedAt: new Date(),
          deletedReason: 'CAPACITY_ROLLOVER',
          endedAt: candidate.ended_at,
          id: candidate.id,
          runId: candidate.run_id,
          runUnitId: candidate.run_unit_id,
          sequence: Number(candidate.sequence),
          sha256: candidate.sha256.toString('hex'),
          startedAt: candidate.started_at,
          state: 'AVAILABLE',
          stream: candidate.stream,
        },
      });
    }
  }

  async enforceAllRunCapacities(maxBytes: number): Promise<{
    readonly affectedRuns: number;
    readonly deletedSegments: number;
    readonly failures: number;
  }> {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
      throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_CONFIG_INVALID');
    }
    const runs = await this.database.requirePool().query<{ id: string }>(
      `SELECT run.id
         FROM ape_dts_platform.task_runs AS run
         JOIN ape_dts_platform.log_segments AS segment ON segment.run_id = run.id
        WHERE run.record_purged_at IS NULL
          AND segment.state = 'AVAILABLE'
        GROUP BY run.id
       HAVING sum(segment.byte_size) > $1::bigint
        ORDER BY run.started_at, run.id`,
      [maxBytes],
    );
    let deletedSegments = 0;
    let failures = 0;
    for (const run of runs.rows) {
      const before = await this.availableSegmentCount(run.id);
      try {
        await this.enforceRunCapacity({ maxBytes, runId: run.id });
        deletedSegments += before - (await this.availableSegmentCount(run.id));
      } catch {
        failures += 1;
        await this.updateRunLogState({
          recoveryState: 'DURABLE_PENDING',
          runId: run.id,
          storageState: 'DEGRADED',
        }).catch(() => undefined);
      }
    }
    return { affectedRuns: runs.rows.length, deletedSegments, failures };
  }

  async updateRunLogState(input: {
    readonly recoveryState: LogRecoveryState;
    readonly runId: string;
    readonly storageState: LogStorageState;
  }): Promise<void> {
    const result = await this.database.requirePool().query(
      `UPDATE ape_dts_platform.task_runs AS runs
          SET log_storage_state = CASE
                WHEN runs.log_storage_state = 'DEGRADED' OR $2 = 'DEGRADED' THEN 'DEGRADED'
                WHEN runs.log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                ELSE $2
              END,
              log_recovery_state = CASE
                WHEN $3 = 'MIXED_PENDING' THEN 'MIXED_PENDING'
                WHEN $3 = 'MEMORY_PENDING' AND (
                  EXISTS (
                    SELECT 1
                      FROM ape_dts_platform.log_segments AS segments
                      JOIN ape_dts_platform.stored_objects AS objects
                        ON objects.id = segments.stored_object_id
                     WHERE segments.run_id = runs.id
                       AND segments.state = 'SEALED'
                       AND objects.state = 'PENDING'
                  ) OR EXISTS (
                    SELECT 1
                      FROM ape_dts_platform.log_segment_deletion_requests AS requests
                     WHERE requests.run_id = runs.id
                       AND requests.completed_at IS NULL
                  )
                ) THEN 'MIXED_PENDING'
                WHEN $3 = 'NONE' AND (
                  EXISTS (
                    SELECT 1
                      FROM ape_dts_platform.log_segments AS segments
                      JOIN ape_dts_platform.stored_objects AS objects
                        ON objects.id = segments.stored_object_id
                     WHERE segments.run_id = runs.id
                       AND segments.state = 'SEALED'
                       AND objects.state = 'PENDING'
                  ) OR EXISTS (
                    SELECT 1
                      FROM ape_dts_platform.log_segment_deletion_requests AS requests
                     WHERE requests.run_id = runs.id
                       AND requests.completed_at IS NULL
                  )
                ) THEN 'DURABLE_PENDING'
                ELSE $3
              END
        WHERE runs.id = $1::uuid
      RETURNING id`,
      [input.runId, input.storageState, input.recoveryState],
    );
    if (result.rowCount !== 1) {
      throw new PostgresLogSegmentStoreError('LOG_RUN_NOT_FOUND');
    }
  }

  private async recordRestartedMemoryGap(runId: string): Promise<boolean> {
    return await this.transaction(async (client) => {
      const run = await client.query<{ log_recovery_state: LogRecoveryState }>(
        `SELECT log_recovery_state
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid
          FOR UPDATE`,
        [runId],
      );
      const recoveryState = run.rows[0]?.log_recovery_state;
      if (recoveryState !== 'MEMORY_PENDING' && recoveryState !== 'MIXED_PENDING') {
        return false;
      }
      await client.query(
        `INSERT INTO ape_dts_platform.log_gaps (
           id, run_id, run_unit_id, reason, started_at, ended_at,
           lost_bytes, occurred_at, container_restart
         ) VALUES ($1::uuid, $2::uuid, NULL, 'CONTAINER_RESTART',
                   NULL, NULL, NULL, statement_timestamp(), true)`,
        [randomUUID(), runId],
      );
      const updated = await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET has_gap = true,
                log_storage_state = 'DEGRADED',
                log_recovery_state = CASE
                  WHEN $2 = 'MIXED_PENDING' THEN 'DURABLE_PENDING'
                  ELSE 'NONE'
                END
          WHERE id = $1::uuid
        RETURNING id`,
        [runId, recoveryState],
      );
      if (updated.rowCount !== 1) {
        throw new PostgresLogSegmentStoreError('LOG_RUN_NOT_FOUND');
      }
      return true;
    });
  }

  private async availableSegmentCount(runId: string): Promise<number> {
    const result = await this.database.requirePool().query<{ count: string }>(
      `SELECT count(*)::text AS count
         FROM ape_dts_platform.log_segments
        WHERE run_id = $1::uuid
          AND state = 'AVAILABLE'`,
      [runId],
    );
    return Number(result.rows[0]?.count ?? '0');
  }

  private async markDurableRecoveryRunsDegraded(runId?: string): Promise<void> {
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.task_runs AS runs
          SET log_storage_state = 'DEGRADED',
              log_recovery_state = CASE
                WHEN runs.log_recovery_state IN ('MEMORY_PENDING', 'MIXED_PENDING')
                  THEN 'MIXED_PENDING'
                ELSE 'DURABLE_PENDING'
              END
        WHERE ($1::uuid IS NULL OR runs.id = $1::uuid)
          AND (EXISTS (
                SELECT 1
                  FROM ape_dts_platform.log_segments AS segments
                  JOIN ape_dts_platform.stored_objects AS objects
                    ON objects.id = segments.stored_object_id
                 WHERE segments.run_id = runs.id
                   AND segments.state = 'SEALED'
                   AND objects.state = 'PENDING'
              )
           OR EXISTS (
                SELECT 1
                  FROM ape_dts_platform.log_segment_deletion_requests AS requests
                 WHERE requests.run_id = runs.id
                   AND requests.completed_at IS NULL
               ))`,
      [runId ?? null],
    );
  }

  private async reconcileRunRecoveryStates(runId?: string): Promise<void> {
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.task_runs AS runs
          SET log_recovery_state = CASE
                WHEN EXISTS (
                  SELECT 1
                    FROM ape_dts_platform.log_segments AS segments
                    JOIN ape_dts_platform.stored_objects AS objects
                      ON objects.id = segments.stored_object_id
                   WHERE segments.run_id = runs.id
                     AND segments.state = 'SEALED'
                     AND objects.state = 'PENDING'
                ) OR EXISTS (
                  SELECT 1
                    FROM ape_dts_platform.log_segment_deletion_requests AS requests
                   WHERE requests.run_id = runs.id
                     AND requests.completed_at IS NULL
                ) THEN CASE
                  WHEN runs.log_recovery_state IN ('MEMORY_PENDING', 'MIXED_PENDING')
                    THEN 'MIXED_PENDING'
                  ELSE 'DURABLE_PENDING'
                END
                WHEN runs.log_recovery_state IN ('MEMORY_PENDING', 'MIXED_PENDING')
                  THEN 'MEMORY_PENDING'
                ELSE 'NONE'
              END
        WHERE ($1::uuid IS NULL OR runs.id = $1::uuid)
          AND (runs.log_recovery_state <> 'NONE'
           OR EXISTS (
                SELECT 1
                  FROM ape_dts_platform.log_segments AS segments
                  JOIN ape_dts_platform.stored_objects AS objects
                    ON objects.id = segments.stored_object_id
                 WHERE segments.run_id = runs.id
                   AND segments.state = 'SEALED'
                   AND objects.state = 'PENDING'
              )
           OR EXISTS (
                SELECT 1
                  FROM ape_dts_platform.log_segment_deletion_requests AS requests
                 WHERE requests.run_id = runs.id
                   AND requests.completed_at IS NULL
               ))`,
      [runId ?? null],
    );
  }

  private async prepareDeletion(input: {
    readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
    readonly segment: LogSegmentRecord;
  }): Promise<PreparedDeletion | null> {
    return await this.transaction(async (client) => {
      const result = await client.query<DeletionRow>(
        `SELECT segments.state AS segment_state,
                segments.deleted_reason,
                objects.id AS object_id,
                objects.object_key,
                objects.state AS object_state,
                profiles.id,
                profiles.profile_key,
                profiles.version,
                profiles.driver,
                profiles.config,
                profiles.secret_id,
                profiles.state
           FROM ape_dts_platform.log_segments AS segments
           JOIN ape_dts_platform.stored_objects AS objects
             ON objects.id = segments.stored_object_id
           JOIN ape_dts_platform.storage_profiles AS profiles
             ON profiles.id = objects.storage_profile_id
          WHERE segments.id = $1::uuid
            AND segments.run_id = $2::uuid
          FOR UPDATE OF segments, objects
          FOR SHARE OF profiles`,
        [input.segment.id, input.segment.runId],
      );
      const row = result.rows[0];
      if (row === undefined) {
        throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
      }
      const request = await client.query<DeletionRequestRow>(
        `SELECT log_segment_id, run_id, stored_object_id, reason,
                deleted_at, completed_at
           FROM ape_dts_platform.log_segment_deletion_requests
          WHERE log_segment_id = $1::uuid
          FOR UPDATE`,
        [input.segment.id],
      );
      const requestRow = request.rows[0];
      if (row.segment_state === 'DELETED') {
        if (
          row.deleted_reason !== input.reason ||
          row.object_state !== 'DELETED' ||
          (requestRow !== undefined &&
            (requestRow.run_id !== input.segment.runId ||
              requestRow.stored_object_id !== row.object_id ||
              requestRow.reason !== input.reason ||
              requestRow.completed_at === null))
        ) {
          throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
        }
        return null;
      }
      if (
        !['SEALED', 'AVAILABLE', 'MISSING', 'INTEGRITY_FAILED'].includes(row.segment_state) ||
        !['PENDING', 'AVAILABLE', 'MISSING', 'INTEGRITY_FAILED'].includes(row.object_state)
      ) {
        throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
      }
      this.localObjects(row);

      if (requestRow !== undefined) {
        if (
          requestRow.run_id !== input.segment.runId ||
          requestRow.stored_object_id !== row.object_id ||
          requestRow.reason !== input.reason ||
          requestRow.completed_at !== null
        ) {
          throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
        }
        return {
          ...row,
          deletedAt: requestRow.deleted_at,
          reason: input.reason,
          runId: input.segment.runId,
          segmentId: input.segment.id,
        };
      }

      const deletedAt = input.segment.deletedAt ?? new Date();
      await client.query(
        `INSERT INTO ape_dts_platform.log_segment_deletion_requests (
           log_segment_id, run_id, stored_object_id, reason, deleted_at
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::timestamptz)`,
        [input.segment.id, input.segment.runId, row.object_id, input.reason, deletedAt],
      );
      return {
        ...row,
        deletedAt,
        reason: input.reason,
        runId: input.segment.runId,
        segmentId: input.segment.id,
      };
    });
  }

  private async finalizeDeletion(deletion: PreparedDeletion): Promise<void> {
    await this.transaction(async (client) => {
      const object = await client.query(
        `UPDATE ape_dts_platform.stored_objects
            SET state = 'DELETED', deleted_at = $2::timestamptz,
                failure_code = NULL
          WHERE id = $1::uuid
            AND state IN ('PENDING', 'AVAILABLE', 'MISSING', 'INTEGRITY_FAILED')
        RETURNING id`,
        [deletion.object_id, deletion.deletedAt],
      );
      const segment = await client.query(
        `UPDATE ape_dts_platform.log_segments
            SET state = 'DELETED', deleted_at = $2::timestamptz,
                deleted_reason = $3, unavailable_reason = NULL
          WHERE id = $1::uuid
            AND run_id = $4::uuid
            AND stored_object_id = $5::uuid
            AND state IN ('SEALED', 'AVAILABLE', 'MISSING', 'INTEGRITY_FAILED')
        RETURNING id`,
        [
          deletion.segmentId,
          deletion.deletedAt,
          deletion.reason,
          deletion.runId,
          deletion.object_id,
        ],
      );
      const request = await client.query(
        `UPDATE ape_dts_platform.log_segment_deletion_requests
            SET completed_at = statement_timestamp(), last_error_code = NULL
          WHERE log_segment_id = $1::uuid
            AND run_id = $2::uuid
            AND stored_object_id = $3::uuid
            AND reason = $4
            AND deleted_at = $5::timestamptz
            AND completed_at IS NULL
        RETURNING log_segment_id`,
        [
          deletion.segmentId,
          deletion.runId,
          deletion.object_id,
          deletion.reason,
          deletion.deletedAt,
        ],
      );
      const run =
        deletion.reason === 'CAPACITY_ROLLOVER'
          ? await client.query(
              `UPDATE ape_dts_platform.task_runs
                  SET has_gap = true, truncated = true
                WHERE id = $1::uuid
              RETURNING id`,
              [deletion.runId],
            )
          : null;
      if (
        object.rowCount !== 1 ||
        segment.rowCount !== 1 ||
        request.rowCount !== 1 ||
        (run !== null && run.rowCount !== 1)
      ) {
        throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
      }
    });
  }

  private async recordDeletionFailure(segmentId: string): Promise<void> {
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.log_segment_deletion_requests
          SET last_error_code = 'LOG_SEGMENT_DELETE_FAILED'
        WHERE log_segment_id = $1::uuid
          AND completed_at IS NULL`,
      [segmentId],
    );
  }

  private async markRecoveredSegmentUnavailable(
    segment: PendingSegmentRow,
    state: 'MISSING' | 'INTEGRITY_FAILED',
  ): Promise<void> {
    const unavailableReason = state === 'MISSING' ? 'STORAGE_MISSING' : 'INTEGRITY_MISMATCH';
    const occurredAt = segment.ended_at;
    await this.transaction(async (client) => {
      const object = await client.query(
        `UPDATE ape_dts_platform.stored_objects
            SET state = $2, failure_code = $3
          WHERE id = $1::uuid
            AND state = 'PENDING'
        RETURNING id`,
        [segment.object_id, state, unavailableReason],
      );
      const logSegment = await client.query(
        `UPDATE ape_dts_platform.log_segments
            SET state = $2, unavailable_reason = $3
          WHERE id = $1::uuid
            AND run_id = $4::uuid
            AND stored_object_id = $5::uuid
            AND state = 'SEALED'
        RETURNING id`,
        [segment.segment_id, state, unavailableReason, segment.run_id, segment.object_id],
      );
      const gap = await client.query(
        `INSERT INTO ape_dts_platform.log_gaps (
           id, run_id, run_unit_id, reason, started_at, ended_at,
           lost_bytes, occurred_at, container_restart
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'CONTAINER_RESTART',
                   $4::timestamptz, $5::timestamptz, $6::bigint,
                   $5::timestamptz, true)
         ON CONFLICT (id) DO UPDATE
           SET id = EXCLUDED.id
         WHERE log_gaps.run_id = EXCLUDED.run_id
           AND log_gaps.run_unit_id IS NOT DISTINCT FROM EXCLUDED.run_unit_id
           AND log_gaps.reason = EXCLUDED.reason
           AND log_gaps.started_at IS NOT DISTINCT FROM EXCLUDED.started_at
           AND log_gaps.ended_at IS NOT DISTINCT FROM EXCLUDED.ended_at
           AND log_gaps.lost_bytes IS NOT DISTINCT FROM EXCLUDED.lost_bytes
           AND log_gaps.occurred_at = EXCLUDED.occurred_at
           AND log_gaps.container_restart = EXCLUDED.container_restart
         RETURNING id`,
        [
          segment.segment_id,
          segment.run_id,
          segment.run_unit_id,
          segment.started_at,
          occurredAt,
          segment.byte_size,
        ],
      );
      const run = await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET has_gap = true
          WHERE id = $1::uuid
        RETURNING id`,
        [segment.run_id],
      );
      if (
        object.rowCount !== 1 ||
        logSegment.rowCount !== 1 ||
        gap.rowCount !== 1 ||
        run.rowCount !== 1
      ) {
        throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
      }
    });
  }

  private async prepareSegment(segment: LogSegmentRecord): Promise<PreparedSegment> {
    return await this.transaction(async (client) => {
      const run = await client.query<{ log_storage_profile_id: string | null }>(
        `SELECT log_storage_profile_id
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid
          FOR UPDATE`,
        [segment.runId],
      );
      const runRow = run.rows[0];
      if (runRow === undefined) {
        throw new PostgresLogSegmentStoreError('LOG_RUN_NOT_FOUND');
      }

      let profile: StorageProfileRow | undefined;
      if (runRow.log_storage_profile_id === null) {
        const active = await client.query<StorageProfileRow>(
          `SELECT id, profile_key, version, driver, config, secret_id, state
             FROM ape_dts_platform.storage_profiles
            WHERE state = 'ACTIVE_WRITE'
            FOR SHARE`,
        );
        profile = active.rows[0];
        if (profile === undefined) {
          throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_UNAVAILABLE');
        }
        this.assertWritableLocalProfile(profile);
        const bound = await client.query(
          `UPDATE ape_dts_platform.task_runs
              SET log_storage_profile_id = $2::uuid
            WHERE id = $1::uuid
              AND log_storage_profile_id IS NULL
          RETURNING id`,
          [segment.runId, profile.id],
        );
        if (bound.rowCount !== 1) {
          throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
        }
      } else {
        const existingProfile = await client.query<StorageProfileRow>(
          `SELECT id, profile_key, version, driver, config, secret_id, state
             FROM ape_dts_platform.storage_profiles
            WHERE id = $1::uuid
            FOR SHARE`,
          [runRow.log_storage_profile_id],
        );
        profile = existingProfile.rows[0];
        if (profile === undefined) {
          throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_NOT_FOUND');
        }
      }
      if (!['ACTIVE_WRITE', 'READ_ONLY_REFERENCED'].includes(profile.state)) {
        throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_NOT_WRITABLE');
      }
      this.assertWritableLocalProfile(profile);

      const existing = await client.query<ExistingSegmentRow>(
        `SELECT segments.run_id,
                segments.run_unit_id,
                segments.stream,
                segments.sequence::text,
                segments.started_at,
                segments.ended_at,
                segments.byte_size::text,
                segments.sha256 AS segment_sha256,
                segments.state AS segment_state,
                segments.created_at,
                objects.id AS object_id,
                objects.storage_profile_id,
                objects.object_key,
                objects.size_bytes::text AS object_size_bytes,
                objects.sha256 AS object_sha256,
                objects.state AS object_state,
                objects.created_by_actor_type,
                objects.created_by_user_id
           FROM ape_dts_platform.log_segments AS segments
           JOIN ape_dts_platform.stored_objects AS objects
             ON objects.id = segments.stored_object_id
          WHERE segments.id = $1::uuid
          FOR UPDATE OF segments, objects`,
        [segment.id],
      );
      const existingRow = existing.rows[0];
      if (existingRow !== undefined) {
        this.validateExistingMetadata(segment, profile.id, existingRow);
        return {
          objectId: existingRow.object_id,
          objectKey: existingRow.object_key,
          profile,
        };
      }

      const objectId = randomUUID();
      const objectKey = segmentObjectKey(segment);
      await client.query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, $4::bigint,
                   decode($5, 'hex'), 'text/plain; charset=utf-8', 'PENDING',
                   'SYSTEM', NULL)`,
        [objectId, profile.id, objectKey, segment.byteSize, segment.sha256],
      );
      await client.query(
        `INSERT INTO ape_dts_platform.log_segments (
           id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
           stored_object_id, byte_size, sha256, state, created_at
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::bigint,
                   $6::timestamptz, $7::timestamptz, $8::uuid, $9::bigint,
                   decode($10, 'hex'), 'SEALED', $11::timestamptz)`,
        [
          segment.id,
          segment.runId,
          segment.runUnitId,
          segment.stream,
          segment.sequence,
          segment.startedAt,
          segment.endedAt,
          objectId,
          segment.byteSize,
          segment.sha256,
          segment.createdAt,
        ],
      );
      return { objectId, objectKey, profile };
    });
  }

  private async finalizeSegment(segmentId: string, runId: string, objectId: string): Promise<void> {
    await this.transaction(async (client) => {
      const object = await client.query(
        `UPDATE ape_dts_platform.stored_objects
            SET state = 'AVAILABLE',
                available_at = COALESCE(available_at, statement_timestamp()),
                failure_code = NULL
          WHERE id = $1::uuid
            AND state IN ('PENDING', 'AVAILABLE')
        RETURNING id`,
        [objectId],
      );
      const logSegment = await client.query(
        `UPDATE ape_dts_platform.log_segments
            SET state = 'AVAILABLE'
          WHERE id = $1::uuid
            AND run_id = $2::uuid
            AND stored_object_id = $3::uuid
            AND state IN ('SEALED', 'AVAILABLE')
        RETURNING id`,
        [segmentId, runId, objectId],
      );
      if (object.rowCount !== 1 || logSegment.rowCount !== 1) {
        throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
      }
    });
  }

  private validateSegmentContent(segment: LogSegmentRecord, content: Buffer): void {
    if (
      segment.state !== 'SEALED' ||
      segment.endedAt === null ||
      segment.deletedAt !== null ||
      segment.deletedReason !== null ||
      segment.sha256 === null ||
      segment.byteSize !== content.length ||
      segmentSha256(content) !== segment.sha256
    ) {
      throw new PostgresLogSegmentStoreError('LOG_SEGMENT_CONTENT_MISMATCH');
    }
  }

  private validateExistingMetadata(
    segment: LogSegmentRecord,
    profileId: string,
    existing: ExistingSegmentRow,
  ): void {
    if (
      existing.run_id !== segment.runId ||
      existing.run_unit_id !== segment.runUnitId ||
      existing.stream !== segment.stream ||
      existing.sequence !== String(segment.sequence) ||
      !sameInstant(existing.started_at, segment.startedAt) ||
      !sameInstant(existing.ended_at, segment.endedAt) ||
      !sameInstant(existing.created_at, segment.createdAt) ||
      existing.byte_size !== String(segment.byteSize) ||
      existing.object_size_bytes !== String(segment.byteSize) ||
      segment.sha256 === null ||
      !bytesEqualHex(existing.segment_sha256, segment.sha256) ||
      !bytesEqualHex(existing.object_sha256, segment.sha256) ||
      !['SEALED', 'AVAILABLE'].includes(existing.segment_state) ||
      !['PENDING', 'AVAILABLE'].includes(existing.object_state) ||
      existing.storage_profile_id !== profileId ||
      existing.created_by_actor_type !== 'SYSTEM' ||
      existing.created_by_user_id !== null ||
      existing.object_key !== segmentObjectKey(segment)
    ) {
      throw new PostgresLogSegmentStoreError('LOG_SEGMENT_METADATA_CONFLICT');
    }
  }

  private assertWritableLocalProfile(profile: StorageProfileRow): void {
    this.localObjects(profile);
  }

  private localObjects(profile: StorageProfileRow): LocalLogSegmentObjectStore {
    if (profile.driver !== 'LOCAL') {
      throw new PostgresLogSegmentStoreError('LOG_STORAGE_DRIVER_UNSUPPORTED');
    }
    if (
      typeof profile.config !== 'object' ||
      profile.config === null ||
      Array.isArray(profile.config) ||
      !('relative_prefix' in profile.config) ||
      typeof profile.config.relative_prefix !== 'string'
    ) {
      throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_CONFIG_INVALID');
    }
    let prefix: string;
    try {
      createStorageProfileSnapshot({
        config: { relativePrefix: profile.config.relative_prefix },
        driver: 'LOCAL',
        id: profile.id,
        logicalName: profile.profile_key,
        secretId: null,
        version: Number(profile.version),
      });
      prefix = profile.config.relative_prefix;
    } catch {
      throw new PostgresLogSegmentStoreError('LOG_STORAGE_PROFILE_CONFIG_INVALID');
    }
    const root =
      prefix.length === 0 ? this.localRoot : path.join(this.localRoot, ...prefix.split('/'));
    return new LocalLogSegmentObjectStore(root);
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
