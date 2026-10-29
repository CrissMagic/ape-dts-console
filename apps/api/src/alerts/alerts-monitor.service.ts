import { Injectable, Logger, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { TaskProgressService } from '../task-progress/task-progress.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { AlertsService } from './alerts.service.js';

@Injectable()
export class AlertsMonitorService implements OnModuleInit, OnApplicationShutdown {
  private timer: ReturnType<typeof setInterval> | undefined;
  private running = false;
  private stopping = false;
  private readonly logger = new Logger(AlertsMonitorService.name);
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly keys: UnlockedKeyService,
    private readonly alerts: AlertsService,
    private readonly progress: TaskProgressService,
    private readonly readiness: PlatformReadinessService,
  ) {}
  onModuleInit() {
    this.timer = setInterval(() => {
      void this.tick();
    }, 15000);
    this.timer.unref();
  }
  onApplicationShutdown() {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
  }

  async tick(): Promise<void> {
    if (
      this.running ||
      this.stopping ||
      !this.database.hasPool() ||
      this.readiness.getPhase() !== 'READY'
    )
      return;
    this.running = true;
    try {
      this.keys.require();
      const settings = await this.alerts.get();
      if (!settings.enabled) return;
      const rows = await this.database.requirePool().query<{
        id: string;
        display_name: string;
        desired_state: string;
        observed_state: string;
        recovery_disposition: string;
        current_applied_revision_id: string | null;
        action: string | null;
        operation_state: string | null;
        initiated_by_user_id: string | null;
        run_status: string | null;
        run_stage: string | null;
        failure_code: string | null;
        recovery_failed: boolean;
      }>(
        `SELECT t.id,t.display_name,t.desired_state,t.observed_state,t.recovery_disposition,t.current_applied_revision_id,o.action,o.state AS operation_state,o.initiated_by_user_id,r.status AS run_status,r.current_stage AS run_stage,r.failure_code,
         (t.observed_state='BLOCKED' AND EXISTS (SELECT 1 FROM ape_dts_platform.audit_events a
           WHERE a.subject_id=t.id AND a.action='TASK_RUNTIME_AUTO_RECOVERY_MANUAL_REQUIRED'
             AND a.occurred_at >= COALESCE(o.created_at,t.created_at))) AS recovery_failed
         FROM ape_dts_platform.tasks t
         LEFT JOIN LATERAL (SELECT action,state,initiated_by_user_id,created_at FROM ape_dts_platform.task_operations WHERE task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1) o ON true
         LEFT JOIN LATERAL (SELECT status,current_stage,failure_code FROM ape_dts_platform.task_runs WHERE task_id=t.id ORDER BY started_at DESC,id DESC LIMIT 1) r ON true
         WHERE t.deleted_at IS NULL`,
      );
      for (const task of rows.rows) {
        if (this.stopping) return;
        const failed =
          ['FAILED', 'BLOCKED'].includes(task.observed_state) &&
          !['STOP', 'EMERGENCY_STOP', 'EMERGENCY_KILL'].includes(task.action ?? '');
        const recovering =
          failed &&
          (task.recovery_failed || task.action === 'RESTORE' || task.initiated_by_user_id === null);
        const unexpected =
          failed &&
          !task.recovery_failed &&
          ['RUNTIME_UNEXPECTED_EXIT', 'RUNTIME_PG_REPLICATION_DISCONNECTED'].includes(
            task.failure_code ?? '',
          );
        const failedStartOperation =
          ['START', 'RESTART', 'APPLY_RESTART'].includes(task.action ?? '') &&
          ['FAILED_RETRYABLE', 'BLOCKED_MANUAL', 'BLOCKED_PERMANENT', 'REJECTED'].includes(
            task.operation_state ?? '',
          );
        const startFailed =
          !recovering &&
          (failedStartOperation || (failed && !unexpected && task.run_stage !== 'STOPPING'));
        const knownNormal = task.observed_state === 'STOPPED' || task.observed_state === 'RUNNING';
        for (const [rule, bad] of [
          ['RECOVERY_FAILED', recovering && !unexpected],
          ['START_FAILED', startFailed],
          ['UNEXPECTED_EXIT', unexpected],
        ] as const) {
          await this.alerts.observe(
            task.id,
            task.display_name,
            rule,
            bad ? true : knownNormal ? false : null,
            true,
            task.failure_code && /^[A-Z0-9_]{1,100}$/u.test(task.failure_code)
              ? `故障码：${task.failure_code}`
              : '',
          );
        }
      }
      // Task failures must be reported even when the progress collector is unavailable.
      const snapshots = await this.progress.listForMonitoring().catch(() => {
        this.logger.error({ code: 'ALERT_PROGRESS_SNAPSHOT_UNAVAILABLE' });
        return [];
      });
      for (const task of rows.rows) {
        if (this.stopping) return;
        const shouldRun = task.desired_state === 'RUNNING';
        const sample = snapshots.find((value) => value.taskId === task.id);
        const age =
          sample?.collectedAt == null ? Infinity : Date.now() - Date.parse(sample.collectedAt);
        const valid =
          sample?.status === 'AVAILABLE' &&
          sample.revisionId === task.current_applied_revision_id &&
          age >= 0 &&
          age <= 60000;
        const monitor = shouldRun && task.observed_state === 'RUNNING';
        const inactiveMeasurement =
          task.desired_state === 'STOPPED' && task.observed_state === 'STOPPED' ? false : null;
        const collected = sample?.collectedAt
          ? new Intl.DateTimeFormat('zh-CN', {
              timeZone: 'Asia/Shanghai',
              year: 'numeric',
              month: '2-digit',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              hour12: false,
            })
              .format(new Date(sample.collectedAt))
              .replace(/^(\d{4})\/(\d{2})\/(\d{2})/u, '$1年$2月$3日')
          : '未知';
        const slotDetail = `复制槽状态：${sample?.slotStatus === 'MISSING' ? '不存在' : sample?.slotStatus === 'INACTIVE' ? '未连接' : sample?.slotStatus === 'ACTIVE' ? '使用中' : '未知'}\n采集时间：${collected}`;
        const slotRule = (expected: string) =>
          !monitor
            ? inactiveMeasurement
            : expected === 'INACTIVE' && task.run_stage === 'SNAPSHOT'
              ? null
              : valid && sample?.sourceType === 'POSTGRES' && sample.slotStatus !== null
                ? sample.slotStatus === expected
                : null;
        if (settings.slotInactive)
          await this.alerts.observe(
            task.id,
            task.display_name,
            'PG_SLOT_INACTIVE',
            slotRule('INACTIVE'),
            false,
            slotDetail,
          );
        if (settings.slotMissing)
          await this.alerts.observe(
            task.id,
            task.display_name,
            'PG_SLOT_MISSING',
            slotRule('MISSING'),
            false,
            slotDetail,
          );
        for (const [rule, limit, value] of [
          ['PG_RETAINED_WAL', settings.retainedWalBytes, sample?.retainedWalBytes],
          ['PG_UNCONFIRMED_WAL', settings.unconfirmedWalBytes, sample?.unconfirmedWalBytes],
          ['KAFKA_OFFSET_LAG', settings.kafkaOffsetLag, sample?.offsetLag],
        ] as const) {
          if (limit === null) continue;
          let exceeded: boolean | null = null;
          if (rule === 'KAFKA_OFFSET_LAG' && !monitor) exceeded = inactiveMeasurement;
          else if (
            valid &&
            value != null &&
            sample?.sourceType === (rule === 'KAFKA_OFFSET_LAG' ? 'KAFKA' : 'POSTGRES')
          )
            exceeded = BigInt(value) > BigInt(limit);
          await this.alerts.observe(
            task.id,
            task.display_name,
            rule,
            exceeded,
            false,
            `当前值：${value ?? '未知'}${rule === 'KAFKA_OFFSET_LAG' ? ' 条' : ' B'}\n阈值：${limit}${rule === 'KAFKA_OFFSET_LAG' ? ' 条' : ' B'}\n采集时间：${collected}`,
          );
        }
        if (settings.delayMilliseconds !== null)
          await this.alerts.observe(
            task.id,
            task.display_name,
            'SYNC_DELAY',
            !monitor
              ? inactiveMeasurement
              : valid && sample?.timeDelayMilliseconds != null
                ? sample.timeDelayMilliseconds > settings.delayMilliseconds
                : null,
            false,
            `当前值：${sample?.timeDelayMilliseconds ?? '未知'} ms\n阈值：${settings.delayMilliseconds} ms\n采集时间：${collected}`,
          );
      }
    } catch {
      // Startup/collection/notification failures must never change a task's state.
      this.logger.warn('ALERT_MONITOR_CYCLE_FAILED');
    } finally {
      this.running = false;
    }
  }
}
