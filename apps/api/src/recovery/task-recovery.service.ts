import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { TaskType, UserSummary } from '@scs/contracts';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { RuntimeSupervisorService } from '../runtime/runtime-supervisor.service.js';
import {
  RecoveryCoordinatorError,
  RecoveryCoordinatorService,
} from './recovery-coordinator.service.js';
import type { RecoveryCandidate } from './recovery.types.js';

@Injectable()
export class TaskRecoveryService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly state: BootstrapStateService,
    private readonly runtime: RuntimeSupervisorService,
    private readonly coordinator: RecoveryCoordinatorService,
    private readonly audit: AuditLedgerService,
  ) {}

  async retry(actor: UserSummary, taskId: string, expectedRowVersion: string) {
    if (actor.role !== 'ADMIN') throw new RecoveryCoordinatorError('RECOVERY_ACCESS_DENIED');
    const platform = this.state.getStatus();
    if (
      platform.phase !== 'READY' ||
      this.runtime.listActiveRuntimeTasks().some((item) => item.taskId === taskId)
    )
      throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE');
    const client = await this.database.requirePool().connect();
    const sessionId = randomUUID();
    let candidate: RecoveryCandidate | undefined;
    let retryCurrent = false;
    try {
      await client.query('BEGIN');
      const task = (
        await client.query<{
          id: string;
          row_version: string;
          current_applied_revision_id: string | null;
          desired_state: string;
          observed_state: string;
          recovery_disposition: string;
          display_name: string;
          task_type: TaskType;
        }>(
          `SELECT id, row_version::text, current_applied_revision_id, desired_state,
                 observed_state, recovery_disposition, display_name, task_type
            FROM ape_dts_platform.tasks WHERE id=$1::uuid AND deleted_at IS NULL FOR UPDATE`,
          [taskId],
        )
      ).rows[0];
      if (task === undefined || task.row_version !== expectedRowVersion)
        throw new RecoveryCoordinatorError('RECOVERY_SESSION_CONFLICT');
      if (
        task.observed_state !== 'BLOCKED' ||
        task.current_applied_revision_id === null ||
        !['AUTO_ELIGIBLE', 'MANUAL_REQUIRED', 'CAPACITY_BLOCKED'].includes(
          task.recovery_disposition,
        )
      )
        throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE');
      const run = (
        await client.query<{
          id: string;
          task_revision_id: string;
          platform_instance_id: string;
          status: string;
          current_stage: string;
        }>(
          `SELECT id, task_revision_id, platform_instance_id, status, current_stage
            FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid
           ORDER BY started_at DESC, id DESC LIMIT 1 FOR UPDATE`,
          [taskId],
        )
      ).rows[0];
      if (
        run?.platform_instance_id === platform.platformInstanceId &&
        ['RUNNING', 'STOPPED'].includes(task.desired_state) &&
        run.status === 'FAILED' &&
        run.task_revision_id === task.current_applied_revision_id &&
        task.recovery_disposition === 'MANUAL_REQUIRED'
      ) {
        retryCurrent = true;
        await client.query('COMMIT');
      } else {
        if (
          task.desired_state !== 'RUNNING' ||
          run === undefined ||
          run.platform_instance_id === platform.platformInstanceId ||
          run.task_revision_id !== task.current_applied_revision_id ||
          run.status !== 'BLOCKED' ||
          run.current_stage !== 'RECONCILIATION'
        )
          throw new RecoveryCoordinatorError('RECOVERY_STEP_NOT_ACTIONABLE');
        const units = await client.query<{ unit_key: string }>(
          `SELECT unit_key FROM ape_dts_platform.task_run_units WHERE run_id=$1::uuid AND task_id=$2::uuid
          AND status='BLOCKED' AND metrics_port_released_at IS NULL ORDER BY unit_key FOR SHARE`,
          [run.id, taskId],
        );
        if (units.rows.length === 0)
          throw new RecoveryCoordinatorError('RECOVERY_CANDIDATE_INVALID');
        candidate = {
          taskId,
          revisionId: run.task_revision_id,
          taskName: task.display_name,
          taskType: task.task_type,
          unitKeys: units.rows.map((unit) => unit.unit_key),
          units: units.rows.length,
        };
        await client.query(
          `UPDATE ape_dts_platform.tasks SET recovery_disposition='AUTO_ELIGIBLE',
        manual_gate_reason=NULL, row_version=row_version+1, updated_at=statement_timestamp() WHERE id=$1::uuid`,
          [taskId],
        );
        await client.query(
          `INSERT INTO ape_dts_platform.recovery_sessions (id, platform_instance_id, state)
        VALUES ($1::uuid,$2::uuid,'RUNNING')`,
          [sessionId, platform.platformInstanceId],
        );
        const steps = [
          'LOAD_METADATA',
          'UNLOCK_KEYS',
          'ENSURE_ARTIFACTS',
          'VERIFY_CONTINUITY',
          'PLAN_CAPACITY',
          'RESTORE_BATCHES',
          'READY_OBSERVABILITY',
        ];
        await client.query(
          `INSERT INTO ape_dts_platform.recovery_steps
        (id, recovery_session_id, step_type, position, status, progress, last_manual_action)
        SELECT id, $1::uuid, step_type, position,
               CASE WHEN position<3 THEN 'SUCCEEDED' ELSE 'PENDING' END,
               CASE WHEN step_type='VERIFY_CONTINUITY' THEN jsonb_build_object('oldRuntimeStoppedConfirmed', true)
                    ELSE '{}'::jsonb END,
               jsonb_build_object('action','RETRY_TASK','actorType','USER','actorUserId',$5::uuid)
          FROM unnest($2::uuid[], $3::text[], $4::smallint[]) AS step(id,step_type,position)`,
          [
            sessionId,
            steps.map(() => randomUUID()),
            steps,
            steps.map((_, index) => index + 1),
            actor.id,
          ],
        );
        await this.audit.append(client, {
          action: 'TASK_RUNTIME_RECOVERY_RETRY_REQUESTED',
          actorType: 'USER',
          actorUserId: actor.id,
          subjectId: taskId,
          subjectType: 'TASK',
          details: {
            sessionId,
            oldRunId: run.id,
            revisionId: run.task_revision_id,
            oldRuntimeStoppedConfirmed: true,
          },
        });
        await client.query('COMMIT');
      }
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
    // Release the task lock before the runtime opens its own transaction.
    if (retryCurrent)
      return { operation: await this.runtime.retryFailedTask(actor, taskId, expectedRowVersion) };
    if (!candidate) throw new RecoveryCoordinatorError('RECOVERY_CANDIDATE_INVALID');
    return await this.coordinator.runCandidate(sessionId, candidate);
  }
}
