import { Injectable } from '@nestjs/common';
import type { TaskType } from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import type {
  RecoveryActionResult,
  RecoveryCandidate,
  RecoverySessionSnapshot,
  RecoveryStepStatus,
  RecoveryStepType,
  RecoveryStore,
} from './recovery.types.js';

interface CandidateRow extends QueryResultRow {
  revision_id: string;
  task_id: string;
  task_name: string;
  task_type: TaskType;
  unit_keys: string[];
}

@Injectable()
export class PostgresRecoveryStore implements RecoveryStore {
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

  async getSession(sessionId: string): Promise<RecoverySessionSnapshot | null> {
    const pool = this.database.requirePool();
    const session = await pool.query<{
      id: string;
      row_version: string;
      state: RecoverySessionSnapshot['state'];
    }>(
      `SELECT id, row_version::text, state
       FROM ape_dts_platform.recovery_sessions
       WHERE id = $1::uuid`,
      [sessionId],
    );
    const row = session.rows[0];
    if (row === undefined) return null;
    const steps = await pool.query<{
      attempt_count: number;
      error_summary: string | null;
      failure_code: string | null;
      position: number;
      progress: Record<string, unknown>;
      status: RecoveryStepStatus;
      step_type: RecoveryStepType;
    }>(
      `SELECT step_type, position, status, progress, failure_code, error_summary, attempt_count
       FROM ape_dts_platform.recovery_steps
       WHERE recovery_session_id = $1::uuid
       ORDER BY position`,
      [sessionId],
    );
    return {
      candidates: await this.listCandidates(sessionId),
      id: row.id,
      rowVersion: row.row_version,
      state: row.state,
      steps: steps.rows.map((step) => ({
        attemptCount: step.attempt_count,
        errorSummary: step.error_summary,
        failureCode: step.failure_code,
        position: step.position,
        progress: step.progress,
        status: step.status,
        stepType: step.step_type,
      })),
    };
  }

  async getCurrentSession(platformInstanceId: string): Promise<RecoverySessionSnapshot | null> {
    const result = await this.database.requirePool().query<{ current_recovery_session_id: string }>(
      `SELECT current_recovery_session_id
       FROM ape_dts_platform.platform_instances
       WHERE id = $1::uuid
         AND current_recovery_session_id IS NOT NULL`,
      [platformInstanceId],
    );
    const sessionId = result.rows[0]?.current_recovery_session_id;
    return sessionId === undefined ? null : await this.getSession(sessionId);
  }

  async listCandidates(sessionId: string): Promise<readonly RecoveryCandidate[]> {
    const result = await this.database.requirePool().query<CandidateRow>(
      `SELECT tasks.id AS task_id,
              tasks.display_name AS task_name,
              tasks.task_type,
              tasks.current_applied_revision_id AS revision_id,
              COALESCE(last_run.unit_keys, ARRAY[]::text[]) AS unit_keys
       FROM ape_dts_platform.tasks AS tasks
       LEFT JOIN LATERAL (
         SELECT array_agg(units.unit_key ORDER BY units.unit_key) AS unit_keys
         FROM ape_dts_platform.task_runs AS runs
         JOIN ape_dts_platform.task_run_units AS units ON units.run_id = runs.id
         WHERE runs.task_id = tasks.id
           AND runs.id = (SELECT latest.id FROM ape_dts_platform.task_runs AS latest
                          WHERE latest.task_id = tasks.id ORDER BY latest.started_at DESC, latest.id DESC LIMIT 1)
       ) AS last_run ON true
       WHERE tasks.deleted_at IS NULL
         AND tasks.desired_state = 'RUNNING'
         AND tasks.recovery_disposition = 'AUTO_ELIGIBLE'
         AND tasks.current_applied_revision_id IS NOT NULL
         AND NOT EXISTS (
           SELECT 1 FROM ape_dts_platform.task_runs AS newer
           JOIN ape_dts_platform.recovery_sessions AS session ON session.id=$1::uuid
            WHERE newer.task_id=tasks.id
              AND (newer.platform_instance_id=session.platform_instance_id OR newer.started_at>session.started_at)
         )
       ORDER BY tasks.id`,
      [sessionId],
    );
    return result.rows.map((row) => ({
      revisionId: row.revision_id,
      taskId: row.task_id,
      taskName: row.task_name,
      taskType: row.task_type,
      unitKeys: row.unit_keys,
      units: row.unit_keys.length,
    }));
  }

  async beginStep(
    sessionId: string,
    stepType: RecoveryStepType,
    allowed: readonly RecoveryStepStatus[],
    actorUserId?: string,
  ): Promise<boolean> {
    return await this.transaction(async (client) => {
      const result = await client.query(
        `UPDATE ape_dts_platform.recovery_steps AS step
         SET status = 'RUNNING', attempt_count = attempt_count + 1,
             failure_code = NULL, error_summary = NULL,
             started_at = statement_timestamp(), finished_at = NULL, updated_at = statement_timestamp(),
             last_manual_action = CASE
               WHEN $4::uuid IS NULL THEN last_manual_action
               WHEN step.step_type = 'VERIFY_CONTINUITY'
                 AND COALESCE((step.progress ->> 'oldRuntimeStoppedConfirmed')::boolean, false)
                 THEN last_manual_action
               ELSE jsonb_build_object('action', 'RETRY', 'actorType', 'USER', 'actorUserId', $4::uuid, 'at', statement_timestamp())
             END
         WHERE step.recovery_session_id = $1::uuid
           AND step.step_type = $2
           AND step.status = ANY($3::text[])
           AND NOT EXISTS (
             SELECT 1 FROM ape_dts_platform.recovery_steps AS predecessor
             WHERE predecessor.recovery_session_id = step.recovery_session_id
               AND predecessor.position < step.position
               AND predecessor.status <> 'SUCCEEDED'
           )`,
        [sessionId, stepType, [...allowed], actorUserId ?? null],
      );
      if (result.rowCount === 1)
        await client.query(
          `UPDATE ape_dts_platform.recovery_sessions
         SET state = 'RUNNING', ended_at = NULL, row_version = row_version + 1
         WHERE id = $1::uuid AND state IN ('RUNNING', 'FAILED', 'PAUSED')`,
          [sessionId],
        );
      return result.rowCount === 1;
    });
  }

  async finishStep(
    sessionId: string,
    stepType: RecoveryStepType,
    result: RecoveryActionResult,
    manualAction?: { readonly action: 'CONTINUE' | 'SELECT'; readonly actorUserId: string },
  ): Promise<void> {
    const status = result.kind;
    const progress =
      result.kind === 'SUCCEEDED' ? (result.progress ?? {}) : (result.progress ?? {});
    const failureCode = result.kind === 'SUCCEEDED' ? null : result.code;
    const summary = result.kind === 'SUCCEEDED' ? null : result.summary;
    const updated = await this.database.requirePool().query(
      `UPDATE ape_dts_platform.recovery_steps
       SET status = $3,
           progress = CASE
             WHEN step_type = 'VERIFY_CONTINUITY' THEN progress || $4::jsonb
             ELSE $4::jsonb
           END,
           failure_code = $5,
           error_summary = $6,
           finished_at = CASE WHEN $3 IN ('SUCCEEDED', 'FAILED') THEN statement_timestamp() ELSE NULL END,
           updated_at = statement_timestamp(),
           last_manual_action = CASE WHEN $7::text IS NULL THEN last_manual_action
             ELSE jsonb_build_object('action', $7, 'actorType', 'USER', 'actorUserId', $8::uuid, 'at', statement_timestamp()) END
       WHERE recovery_session_id = $1::uuid AND step_type = $2 AND status IN ('RUNNING', 'PAUSED', 'FAILED')`,
      [
        sessionId,
        stepType,
        status,
        JSON.stringify(progress),
        failureCode,
        summary,
        manualAction?.action ?? null,
        manualAction?.actorUserId ?? null,
      ],
    );
    if (updated.rowCount !== 1) throw new Error('RECOVERY_STEP_CONCURRENT_CHANGE');
  }

  async capacity(candidates: readonly RecoveryCandidate[]): ReturnType<RecoveryStore['capacity']> {
    const capacity = await this.queryCapacity(this.database.requirePool());
    let retained = 0;
    for (const candidate of candidates) {
      const result = await this.database.requirePool().query<{ count: string }>(
        `SELECT count(*)::text AS count
           FROM ape_dts_platform.capacity_reservations AS reservation
          WHERE reservation.task_id = $1::uuid
            AND reservation.task_revision_id = $2::uuid
            AND reservation.state IN ('RESERVED', 'ACTIVE')
            AND reservation.run_id = (
              SELECT run.id
                FROM ape_dts_platform.task_runs AS run
               WHERE run.task_id = $1::uuid
               ORDER BY run.started_at DESC, run.id DESC
               LIMIT 1
            )`,
        [candidate.taskId, candidate.revisionId],
      );
      const actual = Number(result.rows[0]?.count ?? '0');
      if (actual !== candidate.units)
        return { actual, expected: candidate.units, kind: 'MISMATCH', taskId: candidate.taskId };
      retained += actual;
    }
    return { ...capacity, kind: 'READY', retained };
  }

  async confirmOldRuntimeStopped(
    sessionId: string,
    expectedRowVersion: string,
    reason: string,
    actorUserId: string,
  ): Promise<'APPLIED' | 'CONFLICT' | 'NOT_ACTIONABLE'> {
    return await this.transaction(async (client) => {
      const session = await client.query(
        `SELECT 1 FROM ape_dts_platform.recovery_sessions
          WHERE id = $1::uuid AND row_version = $2::bigint AND state = 'PAUSED'
          FOR UPDATE`,
        [sessionId, expectedRowVersion],
      );
      if (session.rowCount !== 1) return 'CONFLICT';
      const updated = await client.query(
        `UPDATE ape_dts_platform.recovery_steps
            SET progress = progress || jsonb_build_object(
                  'oldRuntimeStoppedConfirmed', true,
                  'oldRuntimeStoppedReason', $2::text,
                  'oldRuntimeStoppedConfirmedAt', statement_timestamp()),
                last_manual_action = jsonb_build_object(
                  'action', 'CONFIRM_OLD_RUNTIME_STOPPED',
                  'actorType', 'USER',
                  'actorUserId', $3::uuid,
                  'reason', $2::text,
                  'at', statement_timestamp()),
                updated_at = statement_timestamp()
          WHERE recovery_session_id = $1::uuid
            AND step_type = 'VERIFY_CONTINUITY'
            AND status = 'PAUSED'
            AND failure_code = 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED'`,
        [sessionId, reason, actorUserId],
      );
      if (updated.rowCount !== 1) return 'NOT_ACTIONABLE';
      await client.query(
        `UPDATE ape_dts_platform.recovery_sessions
            SET row_version = row_version + 1
          WHERE id = $1::uuid AND state = 'PAUSED'`,
        [sessionId],
      );
      return 'APPLIED';
    });
  }

  async selectCandidates(
    sessionId: string,
    taskIds: readonly string[],
    expectedRowVersion: string,
  ): Promise<readonly RecoveryCandidate[]> {
    return await this.transaction(async (client) => {
      const locked = await client.query(
        `SELECT 1 FROM ape_dts_platform.recovery_sessions
         WHERE id = $1::uuid AND row_version = $2::bigint AND state = 'PAUSED'
         FOR UPDATE`,
        [sessionId, expectedRowVersion],
      );
      if (locked.rowCount !== 1) return [];
      const all = await this.queryCandidates(client, sessionId);
      const requested = new Set(taskIds);
      return all.filter((candidate) => requested.has(candidate.taskId));
    });
  }

  async applyCapacitySelection(
    sessionId: string,
    taskIds: readonly string[],
    expectedRowVersion: string,
    actorUserId: string,
  ): ReturnType<RecoveryStore['applyCapacitySelection']> {
    return await this.transaction(async (client) => {
      const locked = await client.query(
        `SELECT 1 FROM ape_dts_platform.recovery_sessions
         WHERE id = $1::uuid AND row_version = $2::bigint AND state = 'PAUSED'
         FOR UPDATE`,
        [sessionId, expectedRowVersion],
      );
      if (locked.rowCount !== 1) return { kind: 'CONFLICT' };

      const step = await client.query(
        `SELECT 1 FROM ape_dts_platform.recovery_steps
         WHERE recovery_session_id = $1::uuid
           AND step_type = 'PLAN_CAPACITY'
           AND status = 'PAUSED'
         FOR UPDATE`,
        [sessionId],
      );
      if (step.rowCount !== 1) return { kind: 'CONFLICT' };

      const all = await this.queryCandidates(client, sessionId);
      const requestedIds = new Set(taskIds);
      const selected = all.filter((candidate) => requestedIds.has(candidate.taskId));
      if (
        taskIds.length === 0 ||
        requestedIds.size !== taskIds.length ||
        selected.length !== requestedIds.size
      ) {
        return { kind: 'INVALID' };
      }
      const capacity = await this.queryCapacity(client);
      if (capacity.limit === null) return { kind: 'NOT_CONFIGURED' };
      const requested = selected.reduce((sum, candidate) => sum + candidate.units, 0);
      let retained = 0;
      for (const candidate of selected) {
        const reservations = await client.query<{ count: string }>(
          `SELECT count(*)::text AS count
             FROM ape_dts_platform.capacity_reservations AS reservation
            WHERE reservation.task_id = $1::uuid
              AND reservation.task_revision_id = $2::uuid
              AND reservation.state IN ('RESERVED', 'ACTIVE')
              AND reservation.run_id = (
                SELECT run.id FROM ape_dts_platform.task_runs AS run
                 WHERE run.task_id = $1::uuid
                 ORDER BY run.started_at DESC, run.id DESC LIMIT 1
              )`,
          [candidate.taskId, candidate.revisionId],
        );
        if (Number(reservations.rows[0]?.count ?? '0') !== candidate.units) {
          return { kind: 'INVALID' };
        }
        retained += candidate.units;
      }
      for (const candidate of all.filter((item) => !requestedIds.has(item.taskId))) {
        const reservations = await client.query<{ count: string }>(
          `SELECT count(*)::text AS count
             FROM ape_dts_platform.capacity_reservations AS reservation
            WHERE reservation.task_id = $1::uuid
              AND reservation.task_revision_id = $2::uuid
              AND reservation.state IN ('RESERVED', 'ACTIVE')
              AND reservation.run_id = (
                SELECT run.id FROM ape_dts_platform.task_runs AS run
                 WHERE run.task_id = $1::uuid
                 ORDER BY run.started_at DESC, run.id DESC LIMIT 1
              )`,
          [candidate.taskId, candidate.revisionId],
        );
        const count = Number(reservations.rows[0]?.count ?? '0');
        if (count !== candidate.units) return { kind: 'INVALID' };
        retained += count;
      }
      const nonCandidateUsed = capacity.used - retained;
      if (nonCandidateUsed < 0) return { kind: 'INVALID' };
      if (nonCandidateUsed + requested > capacity.limit) {
        return {
          kind: 'EXCEEDED',
          limit: capacity.limit,
          requested,
          used: nonCandidateUsed,
        };
      }

      const unselectedIds = all
        .filter((candidate) => !requestedIds.has(candidate.taskId))
        .map((candidate) => candidate.taskId);
      if (unselectedIds.length > 0) {
        await client.query(
          `UPDATE ape_dts_platform.tasks
           SET recovery_disposition = 'CAPACITY_BLOCKED',
               manual_gate_reason = 'RECOVERY_CAPACITY_NOT_SELECTED',
               row_version = row_version + 1,
               updated_at = statement_timestamp()
           WHERE id = ANY($1::uuid[])
             AND deleted_at IS NULL
             AND desired_state = 'RUNNING'
             AND recovery_disposition = 'AUTO_ELIGIBLE'`,
          [unselectedIds],
        );
        await client.query(
          `UPDATE ape_dts_platform.capacity_reservations AS reservation
              SET state = 'RELEASED', released_at = statement_timestamp()
            FROM ape_dts_platform.tasks AS task
            JOIN LATERAL (
              SELECT run.id
                FROM ape_dts_platform.task_runs AS run
               WHERE run.task_id = task.id
               ORDER BY run.started_at DESC, run.id DESC
               LIMIT 1
            ) AS current_run ON true
           WHERE task.id = reservation.task_id
             AND task.id = ANY($1::uuid[])
             AND task.current_applied_revision_id = reservation.task_revision_id
             AND reservation.run_id = current_run.id
             AND reservation.state IN ('RESERVED', 'ACTIVE')`,
          [unselectedIds],
        );
      }
      const completed = await client.query(
        `UPDATE ape_dts_platform.recovery_steps
         SET status = 'SUCCEEDED',
             progress = $2::jsonb,
             failure_code = NULL,
             error_summary = NULL,
             finished_at = statement_timestamp(),
             updated_at = statement_timestamp(),
             last_manual_action = jsonb_build_object(
               'action', 'SELECT', 'actorType', 'USER',
               'actorUserId', $3::uuid, 'at', statement_timestamp())
         WHERE recovery_session_id = $1::uuid
           AND step_type = 'PLAN_CAPACITY'
           AND status = 'PAUSED'`,
        [
          sessionId,
          JSON.stringify({
            limit: capacity.limit,
            requested,
            selectedTaskIds: [...taskIds],
            used: nonCandidateUsed,
          }),
          actorUserId,
        ],
      );
      if (completed.rowCount !== 1) throw new Error('RECOVERY_STEP_CONCURRENT_CHANGE');
      await client.query(
        `UPDATE ape_dts_platform.recovery_sessions
         SET state = 'RUNNING', ended_at = NULL, row_version = row_version + 1
         WHERE id = $1::uuid AND state = 'PAUSED'`,
        [sessionId],
      );
      return { kind: 'APPLIED', candidates: selected };
    });
  }

  private async queryCandidates(
    client: PoolClient,
    sessionId: string,
  ): Promise<readonly RecoveryCandidate[]> {
    const result = await client.query<CandidateRow>(
      `SELECT tasks.id AS task_id,
              tasks.display_name AS task_name,
              tasks.task_type,
              tasks.current_applied_revision_id AS revision_id,
              COALESCE(last_run.unit_keys, ARRAY[]::text[]) AS unit_keys
       FROM ape_dts_platform.tasks AS tasks
       LEFT JOIN LATERAL (
         SELECT array_agg(units.unit_key ORDER BY units.unit_key) AS unit_keys
         FROM ape_dts_platform.task_runs AS runs
         JOIN ape_dts_platform.task_run_units AS units ON units.run_id = runs.id
         WHERE runs.task_id = tasks.id
           AND runs.id = (SELECT latest.id FROM ape_dts_platform.task_runs AS latest
                          WHERE latest.task_id = tasks.id ORDER BY latest.started_at DESC, latest.id DESC LIMIT 1)
       ) AS last_run ON true
       WHERE tasks.deleted_at IS NULL
         AND tasks.desired_state = 'RUNNING'
         AND tasks.recovery_disposition = 'AUTO_ELIGIBLE'
         AND tasks.current_applied_revision_id IS NOT NULL
         AND NOT EXISTS (
           SELECT 1 FROM ape_dts_platform.task_runs AS newer
           JOIN ape_dts_platform.recovery_sessions AS session ON session.id=$1::uuid
            WHERE newer.task_id=tasks.id
              AND (newer.platform_instance_id=session.platform_instance_id OR newer.started_at>session.started_at)
         )
       ORDER BY tasks.id
       FOR UPDATE OF tasks`,
      [sessionId],
    );
    return result.rows.map((row) => ({
      revisionId: row.revision_id,
      taskId: row.task_id,
      taskName: row.task_name,
      taskType: row.task_type,
      unitKeys: row.unit_keys,
      units: row.unit_keys.length,
    }));
  }

  private async queryCapacity(
    client: Pick<PoolClient, 'query'>,
  ): Promise<{ readonly limit: number | null; readonly used: number }> {
    const settings = await client.query<{ limit: number | null }>(
      `SELECT max_runtime_units AS limit
       FROM ape_dts_platform.platform_settings
       WHERE singleton_id = 1
       FOR UPDATE`,
    );
    const reservations = await client.query<{ used: string }>(
      `SELECT count(id)::text AS used
       FROM ape_dts_platform.capacity_reservations
       WHERE state IN ('RESERVED', 'ACTIVE')`,
    );
    return {
      limit: settings.rows[0]?.limit ?? null,
      used: Number(reservations.rows[0]?.used ?? '0'),
    };
  }

  async markCapacityBlocked(taskIds: readonly string[]): Promise<void> {
    if (taskIds.length === 0) return;
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.tasks
       SET recovery_disposition = 'CAPACITY_BLOCKED',
           manual_gate_reason = 'RECOVERY_CAPACITY_NOT_SELECTED',
           row_version = row_version + 1,
           updated_at = statement_timestamp()
       WHERE id = ANY($1::uuid[])
         AND deleted_at IS NULL
         AND desired_state = 'RUNNING'
         AND recovery_disposition = 'AUTO_ELIGIBLE'`,
      [[...taskIds]],
    );
  }

  async markManualRequired(
    taskId: string,
    reason = 'RECOVERY_RESTORE_FAILED_CONTINUED',
    sessionId?: string,
  ): Promise<void> {
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.tasks
       SET recovery_disposition = 'MANUAL_REQUIRED',
           observed_state = 'BLOCKED', observability_health = 'UNAVAILABLE',
           manual_gate_reason = $2,
           row_version = row_version + 1,
           updated_at = statement_timestamp()
       WHERE id = $1::uuid
         AND deleted_at IS NULL
         AND desired_state = 'RUNNING'
         AND recovery_disposition = 'AUTO_ELIGIBLE'
         AND ($3::uuid IS NULL OR NOT EXISTS (
           SELECT 1 FROM ape_dts_platform.task_runs AS run
            WHERE run.task_id = tasks.id
              AND run.platform_instance_id = (SELECT platform_instance_id FROM ape_dts_platform.recovery_sessions WHERE id=$3::uuid)
              AND run.status IN ('STARTING','RUNNING','STOPPING')
         ))`,
      [taskId, reason, sessionId ?? null],
    );
  }

  async finishSession(sessionId: string, state: 'READY' | 'FAILED' | 'PAUSED'): Promise<void> {
    await this.database.requirePool().query(
      `UPDATE ape_dts_platform.recovery_sessions
       SET state = $2,
           ended_at = CASE WHEN $2 IN ('READY', 'FAILED') THEN statement_timestamp() ELSE NULL END,
           row_version = row_version + 1
       WHERE id = $1::uuid AND state = 'RUNNING'`,
      [sessionId, state],
    );
  }
}
