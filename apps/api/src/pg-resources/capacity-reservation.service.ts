import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { PoolClient, QueryResultRow } from 'pg';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

export type CapacityReservationErrorCode =
  | 'CAPACITY_ALREADY_RESERVED'
  | 'CAPACITY_NOT_CONFIGURED'
  | 'CAPACITY_INSUFFICIENT'
  | 'CAPACITY_INVALID_REQUEST'
  | 'CAPACITY_RUN_NOT_FOUND'
  | 'CAPACITY_TASK_NOT_FOUND';

export class CapacityReservationError extends Error {
  constructor(
    readonly code: CapacityReservationErrorCode,
    readonly details?: {
      readonly limit?: number;
      readonly requested?: number;
      readonly used?: number;
    },
  ) {
    super(code);
    this.name = 'CapacityReservationError';
  }
}

export interface CapacityReservationSummary {
  readonly id: string;
  readonly runId: string;
  readonly state: 'RESERVED';
  readonly taskId: string;
  readonly taskRevisionId: string;
  readonly unitKey: string;
}

export interface CapacitySnapshot {
  readonly available: number | null;
  readonly limit: number | null;
  readonly requested: number;
  readonly used: number;
}

export interface CapacityTransferBinding {
  readonly expectedTaskRowVersion: string;
  readonly newRunId: string;
  readonly oldProcessToken: string;
  readonly oldRunId: string;
  readonly platformInstanceId: string;
  readonly taskId: string;
  readonly taskRevisionId: string;
}

interface SettingsRow extends QueryResultRow {
  max_runtime_units: number | null;
}

interface ReservationRow extends QueryResultRow {
  id: string;
  run_id: string;
  state: 'RESERVED';
  task_id: string;
  task_revision_id: string;
  unit_key: string;
}

@Injectable()
export class CapacityReservationService {
  constructor(private readonly database: PlatformDatabaseService) {}

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

  async snapshot(requestedUnits: number): Promise<CapacitySnapshot> {
    if (!Number.isSafeInteger(requestedUnits) || requestedUnits < 0) {
      throw new CapacityReservationError('CAPACITY_INVALID_REQUEST');
    }
    const result = await this.database.requirePool().query<SettingsRow>(
      `SELECT max_runtime_units
         FROM ape_dts_platform.platform_settings
        WHERE singleton_id = 1`,
    );
    const limit = result.rows[0]?.max_runtime_units ?? null;
    const usedResult = await this.database.requirePool().query<{ used: string }>(
      `SELECT count(*)::text AS used
         FROM ape_dts_platform.capacity_reservations
        WHERE state IN ('RESERVED', 'ACTIVE')`,
    );
    const used = Number(usedResult.rows[0]?.used ?? '0');
    if (!Number.isSafeInteger(used) || used < 0) {
      throw new CapacityReservationError('CAPACITY_INVALID_REQUEST');
    }
    return {
      available: limit === null ? null : Math.max(0, limit - used),
      limit,
      requested: requestedUnits,
      used,
    };
  }

  async reserve(
    taskId: string,
    taskRevisionId: string,
    unitKeys: readonly string[],
    runId: string,
  ): Promise<readonly CapacityReservationSummary[]> {
    const keys = [...unitKeys];
    this.assertUnitKeys(keys);
    this.assertRunId(runId);
    return await this.transaction(async (client) => {
      return await this.reserveWithin(client, taskId, taskRevisionId, keys, runId);
    });
  }

  async reserveWithin(
    client: PoolClient,
    taskId: string,
    taskRevisionId: string,
    unitKeys: readonly string[],
    runId: string,
  ): Promise<readonly CapacityReservationSummary[]> {
    const keys = [...unitKeys];
    this.assertUnitKeys(keys);
    this.assertRunId(runId);
    const task = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid
          AND deleted_at IS NULL
          AND desired_state <> 'DELETED'`,
      [taskId],
    );
    if (task.rows[0] === undefined) {
      throw new CapacityReservationError('CAPACITY_TASK_NOT_FOUND');
    }
    const revision = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid
          AND task_id = $2::uuid`,
      [taskRevisionId, taskId],
    );
    if (revision.rows[0] === undefined) {
      throw new CapacityReservationError('CAPACITY_TASK_NOT_FOUND');
    }
    const run = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.task_runs
        WHERE id = $1::uuid
          AND task_id = $2::uuid
          AND task_revision_id = $3::uuid`,
      [runId, taskId, taskRevisionId],
    );
    if (run.rows[0] === undefined) {
      throw new CapacityReservationError('CAPACITY_RUN_NOT_FOUND');
    }

    const settings = await client.query<SettingsRow>(
      `SELECT max_runtime_units
         FROM ape_dts_platform.platform_settings
        WHERE singleton_id = 1
        FOR UPDATE`,
    );
    const limit = settings.rows[0]?.max_runtime_units;
    if (limit === undefined || limit === null) {
      throw new CapacityReservationError('CAPACITY_NOT_CONFIGURED');
    }
    const usedResult = await client.query<{ used: string }>(
      `SELECT count(*)::text AS used
         FROM ape_dts_platform.capacity_reservations
        WHERE state IN ('RESERVED', 'ACTIVE')`,
    );
    const used = Number(usedResult.rows[0]?.used ?? '0');
    if (used + keys.length > limit) {
      throw new CapacityReservationError('CAPACITY_INSUFFICIENT', {
        limit,
        requested: keys.length,
        used,
      });
    }

    const ids = keys.map(() => randomUUID());
    try {
      const inserted = await client.query<ReservationRow>(
        `INSERT INTO ape_dts_platform.capacity_reservations (
           id, task_id, task_revision_id, run_id, unit_key, state
         )
         SELECT input_rows.id, $1::uuid, $2::uuid, $3::uuid, input_rows.unit_key, 'RESERVED'
           FROM unnest($4::uuid[], $5::text[]) AS input_rows(id, unit_key)
         RETURNING id, task_id, task_revision_id, run_id, unit_key, state`,
        [taskId, taskRevisionId, runId, ids, keys],
      );
      if (inserted.rows.length !== keys.length) {
        throw new CapacityReservationError('CAPACITY_ALREADY_RESERVED');
      }
      return inserted.rows.map((row) => ({
        id: row.id,
        runId: row.run_id,
        state: row.state,
        taskId: row.task_id,
        taskRevisionId: row.task_revision_id,
        unitKey: row.unit_key,
      }));
    } catch (error) {
      if (
        error instanceof CapacityReservationError ||
        (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505')
      ) {
        if (error instanceof CapacityReservationError) {
          throw error;
        }
        throw new CapacityReservationError('CAPACITY_ALREADY_RESERVED');
      }
      throw error;
    }
  }

  async activateWithin(client: PoolClient, runId: string): Promise<number> {
    const result = await client.query(
      `UPDATE ape_dts_platform.capacity_reservations
          SET state = 'ACTIVE'
        WHERE run_id = $1::uuid
          AND state = 'RESERVED'`,
      [runId],
    );
    return result.rowCount ?? 0;
  }

  async transferWithin(
    client: PoolClient,
    binding: CapacityTransferBinding,
    expectedUnitCount: number,
  ): Promise<number> {
    if (!Number.isSafeInteger(expectedUnitCount) || expectedUnitCount <= 0) {
      throw new CapacityReservationError('CAPACITY_INVALID_REQUEST');
    }
    const transferred = await client.query(
      `UPDATE ape_dts_platform.capacity_reservations AS reservation
          SET run_id = $2::uuid
        WHERE reservation.run_id = $1::uuid
          AND reservation.task_id = $3::uuid
          AND reservation.task_revision_id = $4::uuid
          AND reservation.state IN ('RESERVED', 'ACTIVE')
          AND EXISTS (
            SELECT 1
              FROM ape_dts_platform.tasks AS task
             WHERE task.id = reservation.task_id
               AND task.deleted_at IS NULL
               AND task.desired_state = 'RUNNING'
               AND task.row_version = $5::bigint
               AND task.current_applied_revision_id = reservation.task_revision_id
          )
          AND EXISTS (
            SELECT 1
              FROM ape_dts_platform.task_runs AS old_run
              JOIN ape_dts_platform.task_run_units AS old_unit
                ON old_unit.run_id = old_run.id
               AND old_unit.process_token = $7::text
             WHERE old_run.id = reservation.run_id
               AND old_run.task_id = reservation.task_id
               AND old_run.task_revision_id = reservation.task_revision_id
               AND old_run.platform_instance_id = $6::uuid
               AND old_run.status = 'FAILED'
          )
          AND EXISTS (
            SELECT 1
              FROM ape_dts_platform.task_runs AS new_run
             WHERE new_run.id = $2::uuid
               AND new_run.task_id = reservation.task_id
               AND new_run.task_revision_id = reservation.task_revision_id
               AND new_run.platform_instance_id = $6::uuid
               AND new_run.status = 'STARTING'
               AND NOT EXISTS (
                 SELECT 1
                   FROM ape_dts_platform.task_runs AS newer_run
                  WHERE newer_run.task_id = new_run.task_id
                    AND (newer_run.started_at, newer_run.id) > (new_run.started_at, new_run.id)
               )
          )`,
      [
        binding.oldRunId,
        binding.newRunId,
        binding.taskId,
        binding.taskRevisionId,
        binding.expectedTaskRowVersion,
        binding.platformInstanceId,
        binding.oldProcessToken,
      ],
    );
    const count = transferred.rowCount ?? 0;
    if (count !== expectedUnitCount) {
      throw new CapacityReservationError('CAPACITY_ALREADY_RESERVED', {
        requested: expectedUnitCount,
        used: count,
      });
    }
    return count;
  }

  async release(runId: string): Promise<number> {
    return await this.transaction(async (client) => await this.releaseWithin(client, runId));
  }

  async releaseWithin(client: PoolClient, runId: string): Promise<number> {
    const result = await client.query(
      `UPDATE ape_dts_platform.capacity_reservations
          SET state = 'RELEASED', released_at = statement_timestamp()
        WHERE run_id = $1::uuid
          AND state IN ('RESERVED', 'ACTIVE')`,
      [runId],
    );
    return result.rowCount ?? 0;
  }

  private assertUnitKeys(keys: readonly string[]): void {
    if (
      keys.length === 0 ||
      keys.some((key) => key.trim().length === 0) ||
      new Set(keys).size !== keys.length
    ) {
      throw new CapacityReservationError('CAPACITY_INVALID_REQUEST');
    }
  }

  private assertRunId(runId: string): void {
    if (typeof runId !== 'string' || runId.trim().length === 0) {
      throw new CapacityReservationError('CAPACITY_RUN_NOT_FOUND');
    }
  }
}
