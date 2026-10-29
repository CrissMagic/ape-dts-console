import { Inject, Injectable } from '@nestjs/common';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PreflightService } from '../preflight/preflight.service.js';
import type {
  RecoveryActionResult,
  RecoveryCandidate,
  RecoveryExternalActions,
} from './recovery.types.js';

export const RECOVERY_RUNTIME_PORT = Symbol('RECOVERY_RUNTIME_PORT');

export interface PersistedRecoveryBinding {
  readonly expectedTaskRowVersion: string;
  readonly oldRunId: string;
  readonly platformInstanceId: string;
  readonly revisionId: string;
  readonly taskId: string;
  readonly unitKeys: readonly string[];
}

export interface RecoveryRuntimePort {
  reconcilePersistedRuns(): Promise<number>;
  verifyRecoveryArtifact(
    binding: Pick<PersistedRecoveryBinding, 'revisionId' | 'taskId'>,
  ): Promise<void>;
  recoverPersistedTask(binding: PersistedRecoveryBinding): Promise<{ readonly runId: string }>;
  recoveryObservability(taskIds: readonly string[]): Promise<
    readonly {
      readonly logsReady: boolean;
      readonly metricsReady: boolean;
      readonly runId: string;
      readonly taskId: string;
    }[]
  >;
}

interface BindingRow {
  include_snapshot: boolean;
  clean_shutdown: boolean | null;
  current_stage: string;
  old_run_id: string;
  platform_instance_id: string;
  revision_id: string;
  row_version: string;
  run_status: string;
  task_id: string;
  unit_keys: string[];
}

function failure(taskId: string, code: string, summary: string): RecoveryActionResult {
  return { code, kind: 'FAILED', progress: { taskId }, summary };
}

function externalFailure(taskId: string, error: unknown, fallback: string): RecoveryActionResult {
  const code = error instanceof Error && error.message.length > 0 ? error.message : fallback;
  return failure(taskId, code, `任务 ${taskId} 的恢复检查失败：${code}`);
}

@Injectable()
export class PostgresRecoveryExternalActions implements RecoveryExternalActions {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly preflight: PreflightService,
    @Inject(RECOVERY_RUNTIME_PORT) private readonly runtime: RecoveryRuntimePort,
  ) {}

  async ensureArtifacts(
    _sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult> {
    await this.runtime.reconcilePersistedRuns();
    for (const candidate of candidates) {
      try {
        await this.runtime.verifyRecoveryArtifact({
          revisionId: candidate.revisionId,
          taskId: candidate.taskId,
        });
      } catch (error) {
        return externalFailure(candidate.taskId, error, 'RECOVERY_ARTIFACT_VERIFICATION_FAILED');
      }
    }
    return {
      kind: 'SUCCEEDED',
      progress: { verifiedTaskIds: candidates.map(({ taskId }) => taskId) },
    };
  }

  async verifyContinuity(
    sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult> {
    const confirmed = await this.hasOldRuntimeConfirmation(sessionId);
    for (const candidate of candidates) {
      const binding = await this.loadBinding(candidate);
      if (binding === null)
        return failure(
          candidate.taskId,
          'RECOVERY_OLD_RUN_BINDING_INVALID',
          '未找到与任务、当前应用修订和旧平台实例唯一绑定的最近一次运行。',
        );
      if (binding.clean_shutdown !== true && !confirmed)
        return {
          code: 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED',
          kind: 'PAUSED',
          progress: { taskId: candidate.taskId },
          summary: '旧平台实例不是 clean shutdown；必须由管理员明确确认部署侧旧容器已经停止。',
        };
      if (candidate.taskType === 'PG_KAFKA' && binding.include_snapshot === true)
        return failure(
          candidate.taskId,
          'RECOVERY_SNAPSHOT_RESTART_REQUIRES_DECISION',
          '该任务包含全量快照，当前没有可验证的快照续传位点；系统未自动重做全量，需明确选择重做策略。',
        );
      try {
        const result = await this.preflight.runForSystemRecovery({
          expectedOldRunId: binding.old_run_id,
          expectedPlatformInstanceId: binding.platform_instance_id,
          expectedTaskRowVersion: binding.row_version,
          revisionId: binding.revision_id,
          taskId: binding.task_id,
        });
        if (result.state !== 'PASSED')
          return failure(
            candidate.taskId,
            result.failureCode ?? 'RECOVERY_SYSTEM_PREFLIGHT_FAILED',
            'SYSTEM 恢复预检未通过；源实例、消息目标或运行资源连续性未得到确认。',
          );
      } catch (error) {
        return externalFailure(candidate.taskId, error, 'RECOVERY_SYSTEM_PREFLIGHT_FAILED');
      }
    }
    return {
      kind: 'SUCCEEDED',
      progress: { verifiedTaskIds: candidates.map(({ taskId }) => taskId) },
    };
  }

  async restoreTask(
    _sessionId: string,
    candidate: RecoveryCandidate,
  ): Promise<RecoveryActionResult> {
    const binding = await this.loadBinding(candidate);
    if (
      binding === null ||
      binding.run_status !== 'BLOCKED' ||
      binding.current_stage !== 'RECONCILIATION'
    )
      return failure(
        candidate.taskId,
        'RECOVERY_OLD_RUN_NOT_RECONCILIATION_BLOCKED',
        '仅允许恢复处于 RECONCILIATION/BLOCKED 的唯一最近旧运行。',
      );
    try {
      const restored = await this.runtime.recoverPersistedTask({
        expectedTaskRowVersion: binding.row_version,
        oldRunId: binding.old_run_id,
        platformInstanceId: binding.platform_instance_id,
        revisionId: binding.revision_id,
        taskId: binding.task_id,
        unitKeys: binding.unit_keys,
      });
      return { kind: 'SUCCEEDED', progress: { runId: restored.runId, taskId: candidate.taskId } };
    } catch (error) {
      return externalFailure(candidate.taskId, error, 'RECOVERY_RUNTIME_RESTORE_FAILED');
    }
  }

  async readyObservability(
    _sessionId: string,
    candidates: readonly RecoveryCandidate[],
  ): Promise<RecoveryActionResult> {
    try {
      const facts = await this.runtime.recoveryObservability(
        candidates.map(({ taskId }) => taskId),
      );
      for (const candidate of candidates) {
        const fact = facts.find(({ taskId }) => taskId === candidate.taskId);
        if (fact === undefined || !fact.logsReady || !fact.metricsReady)
          return failure(
            candidate.taskId,
            'RECOVERY_OBSERVABILITY_NOT_READY',
            '恢复运行的日志或 Metrics 采集端口尚未建立。',
          );
      }
      return { kind: 'SUCCEEDED', progress: { runIds: facts.map(({ runId }) => runId) } };
    } catch (error) {
      return externalFailure('', error, 'RECOVERY_OBSERVABILITY_CHECK_FAILED');
    }
  }

  private async hasOldRuntimeConfirmation(sessionId: string): Promise<boolean> {
    const result = await this.database.requirePool().query<{ confirmed: boolean }>(
      `SELECT COALESCE((progress ->> 'oldRuntimeStoppedConfirmed')::boolean, false) AS confirmed
         FROM ape_dts_platform.recovery_steps
        WHERE recovery_session_id = $1::uuid AND step_type = 'VERIFY_CONTINUITY'`,
      [sessionId],
    );
    return result.rows[0]?.confirmed === true;
  }

  private async loadBinding(candidate: RecoveryCandidate): Promise<BindingRow | null> {
    const result = await this.database.requirePool().query<BindingRow>(
      `SELECT task.id AS task_id, task.current_applied_revision_id AS revision_id,
              task.row_version::text, run.id AS old_run_id, run.status AS run_status,
              run.current_stage, run.platform_instance_id, instance.clean_shutdown,
              COALESCE(pg_config.include_snapshot, false) AS include_snapshot,
              array_agg(unit.unit_key ORDER BY unit.unit_key) AS unit_keys
         FROM ape_dts_platform.tasks AS task
         JOIN LATERAL (
           SELECT candidate_run.* FROM ape_dts_platform.task_runs AS candidate_run
            WHERE candidate_run.task_id = task.id
            ORDER BY candidate_run.started_at DESC, candidate_run.id DESC LIMIT 1
         ) AS run ON true
         JOIN ape_dts_platform.platform_instances AS instance ON instance.id = run.platform_instance_id
         LEFT JOIN ape_dts_platform.pg_kafka_revision_configs AS pg_config
           ON pg_config.task_revision_id=task.current_applied_revision_id
         JOIN ape_dts_platform.task_run_units AS unit ON unit.run_id = run.id
        WHERE task.id = $1::uuid AND task.deleted_at IS NULL
          AND task.desired_state = 'RUNNING' AND task.recovery_disposition = 'AUTO_ELIGIBLE'
          AND task.current_applied_revision_id = $2::uuid
          AND run.task_revision_id = task.current_applied_revision_id
        GROUP BY task.id, run.id, run.status, run.current_stage,
                 run.platform_instance_id, instance.id, pg_config.include_snapshot`,
      [candidate.taskId, candidate.revisionId],
    );
    const row = result.rows[0];
    if (
      row === undefined ||
      row.platform_instance_id === null ||
      row.unit_keys.length !== candidate.units ||
      row.unit_keys.some((key, index) => key !== candidate.unitKeys[index])
    )
      return null;
    return row;
  }
}
