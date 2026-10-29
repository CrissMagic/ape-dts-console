import { Injectable } from '@nestjs/common';
import type {
  RuntimeOperationSummary,
  RuntimeRead,
  RuntimeRunList,
  UserSummary,
} from '@scs/contracts';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

export class RuntimeReadNotFoundError extends Error {}

const iso = (value: Date | null): string | null => value?.toISOString() ?? null;
interface RunRow {
  id: string;
  operation_id: string | null;
  current_stage: string;
  status: RuntimeRead['run'] extends infer R ? (R extends { status: infer S } ? S : never) : never;
  started_at: Date;
  ended_at: Date | null;
  failure_code: string | null;
  failure_summary: string | null;
  log_recovery_state: NonNullable<RuntimeRead['run']>['logRecoveryState'];
  log_storage_state: NonNullable<RuntimeRead['run']>['logStorageState'];
}
interface OperationRow {
  action: RuntimeOperationSummary['action'];
  completed_at: Date | null;
  created_at: Date;
  failure_code: string | null;
  failure_summary: string | null;
  id: string;
  run_id: string | null;
  state: RuntimeOperationSummary['state'];
  updated_at: Date;
}
interface StageRow {
  progress_completed: string;
  ended_at: Date | null;
  failure_code: string | null;
  failure_summary: string | null;
  heartbeat_at: Date | null;
  stage_order: number;
  started_at: Date | null;
  status: RuntimeRead['stages'][number]['status'];
  throughput: string | null;
  progress_total: string | null;
  stage_type: string;
}
interface UnitRow {
  ended_at: Date | null;
  exit_code: number | null;
  exit_signal: string | null;
  failure_code: string | null;
  failure_summary: string | null;
  started_at: Date | null;
  status: RuntimeRead['units'][number]['status'];
  unit_key: string;
}

@Injectable()
export class RuntimeReadService {
  constructor(private readonly database: PlatformDatabaseService) {}

  private async requireVisibleTask(actor: UserSummary, taskId: string): Promise<void> {
    const pool = this.database.requirePool();
    const task = await pool.query<{ id: string }>(
      `SELECT task.id
         FROM ape_dts_platform.tasks AS task
        WHERE task.id = $1::uuid
          AND task.deleted_at IS NULL
          AND ($2::text = 'ADMIN' OR NOT EXISTS (
            SELECT required.data_source_id
              FROM (VALUES (task.source_data_source_id), (task.target_data_source_id)) AS required(data_source_id)
             WHERE NOT EXISTS (
               SELECT 1 FROM ape_dts_platform.data_source_grants AS grant_row
                WHERE grant_row.user_id = $3::uuid
                  AND grant_row.data_source_id = required.data_source_id
                  AND grant_row.revoked_at IS NULL
             )
          ))`,
      [taskId, actor.role, actor.id],
    );
    if (task.rowCount !== 1) throw new RuntimeReadNotFoundError();
  }

  async listRuns(actor: UserSummary, taskId: string): Promise<RuntimeRunList> {
    await this.requireVisibleTask(actor, taskId);
    const result = await this.database
      .requirePool()
      .query<Pick<RunRow, 'id' | 'status' | 'started_at' | 'ended_at'>>(
        `SELECT id,status,started_at,ended_at FROM ape_dts_platform.task_runs
        WHERE task_id=$1::uuid AND record_purged_at IS NULL
        ORDER BY started_at DESC,id DESC`,
        [taskId],
      );
    return {
      runs: result.rows.map((row) => ({
        id: row.id,
        status: row.status,
        startedAt: row.started_at.toISOString(),
        endedAt: iso(row.ended_at),
      })),
    };
  }

  async get(actor: UserSummary, taskId: string): Promise<RuntimeRead> {
    await this.requireVisibleTask(actor, taskId);
    const pool = this.database.requirePool();

    const runs = await pool.query<RunRow>(
      `SELECT run.* FROM ape_dts_platform.task_runs AS run
        WHERE run.task_id = $1::uuid
          AND run.record_purged_at IS NULL
        ORDER BY CASE WHEN run.status IN ('STARTING','RUNNING','STOPPING') THEN 0 ELSE 1 END,
                 run.started_at DESC, run.id DESC LIMIT 1`,
      [taskId],
    );
    const run = runs.rows[0];
    const operations = await pool.query<OperationRow>(
      `SELECT * FROM ape_dts_platform.task_operations
        WHERE task_id = $1::uuid
        ORDER BY created_at DESC, id DESC LIMIT 1`,
      [taskId],
    );
    const operationRow = operations.rows[0];
    const operation: RuntimeOperationSummary | null =
      operationRow === undefined
        ? null
        : {
            action: operationRow.action,
            completedAt: iso(operationRow.completed_at),
            createdAt: operationRow.created_at.toISOString(),
            failureCode: operationRow.failure_code,
            failureSummary: operationRow.failure_summary,
            id: operationRow.id,
            runId: operationRow.run_id,
            state: operationRow.state,
            taskId,
            updatedAt: operationRow.updated_at.toISOString(),
          };
    if (run === undefined)
      return { operation, retryable: false, run: null, stages: [], taskId, units: [] };
    const [stages, units] = await Promise.all([
      pool.query<StageRow>(
        `SELECT * FROM ape_dts_platform.task_run_stages WHERE run_id = $1::uuid ORDER BY stage_order`,
        [run.id],
      ),
      pool.query<UnitRow>(
        `SELECT * FROM ape_dts_platform.task_run_units WHERE run_id = $1::uuid ORDER BY unit_key`,
        [run.id],
      ),
    ]);
    const retryable = operation?.state === 'FAILED_RETRYABLE' && operation.runId === run.id;
    return {
      operation,
      retryable,
      run: {
        currentStage: run.current_stage,
        endedAt: iso(run.ended_at),
        failureCode: run.failure_code,
        failureSummary: run.failure_summary,
        id: run.id,
        logRecoveryState: run.log_recovery_state,
        logStorageState: run.log_storage_state,
        startedAt: run.started_at.toISOString(),
        status: run.status,
      },
      stages: stages.rows.map((stage) => ({
        completed: String(stage.progress_completed),
        endedAt: iso(stage.ended_at),
        failureCode: stage.failure_code,
        failureSummary: stage.failure_summary,
        heartbeatAt: iso(stage.heartbeat_at),
        order: stage.stage_order,
        retryable: retryable && stage.status === 'FAILED',
        startedAt: iso(stage.started_at),
        status: stage.status,
        throughput: stage.throughput === null ? null : String(stage.throughput),
        total: stage.progress_total === null ? null : String(stage.progress_total),
        type: stage.stage_type,
      })),
      taskId,
      units: units.rows.map((unit) => ({
        endedAt: iso(unit.ended_at),
        exitCode: unit.exit_code,
        exitSignal: unit.exit_signal,
        failureCode: unit.failure_code,
        failureSummary: unit.failure_summary,
        key: unit.unit_key,
        startedAt: iso(unit.started_at),
        status: unit.status,
      })),
    };
  }
}
