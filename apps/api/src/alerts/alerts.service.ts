import { randomUUID } from 'node:crypto';
import {
  BadRequestException,
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import type {
  AlertDelivery,
  AlertDeliveryContext,
  AlertSettings,
  UpdateAlertSettingsRequest,
} from '@scs/contracts';
import { decryptSecret, encryptSecret, type EncryptedSecret } from '@scs/security';
import type { PoolClient } from 'pg';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { isWecomWebhook, WecomAlertSender } from './alert-sender.js';
import { evaluateAlertRule } from './alert-rules.js';

interface SettingsRow {
  enabled: boolean;
  configuration: Omit<AlertSettings, 'enabled' | 'configured' | 'rowVersion'>;
  encrypted_webhook: EncryptedSecret | null;
  row_version: string;
}
interface DeliveryRow {
  id: string;
  task_id: string | null;
  task_name: string | null;
  rule: string;
  event: AlertDelivery['event'];
  status: AlertDelivery['status'];
  failure_code: string | null;
  fault_code: string | null;
  created_at: Date;
}
const safeFaultCode = (value: unknown): string | null =>
  typeof value === 'string' && /^[A-Z0-9_]{1,100}$/u.test(value) ? value : null;
const deliveryView = (row: DeliveryRow): AlertDelivery => ({
  id: row.id,
  taskId: row.task_id,
  taskName: row.task_name,
  rule: row.rule,
  event: row.event,
  status: row.status,
  failureCode: row.failure_code,
  faultCode: safeFaultCode(row.fault_code),
  createdAt: row.created_at.toISOString(),
});

const deliverySelect = `SELECT d.*, t.display_name AS task_name, NULL::text AS fault_code
  FROM ape_dts_platform.alert_deliveries d LEFT JOIN ape_dts_platform.tasks t ON t.id=d.task_id`;

// Read diagnostics only when opening one notification, bounded by its incident and time.
const contextDeliverySelect = `SELECT d.*, t.display_name AS task_name, diagnostic.canonical_details->>'reason' AS fault_code,
    diagnostic.canonical_details AS diagnostic_details
  FROM ape_dts_platform.alert_deliveries d LEFT JOIN ape_dts_platform.tasks t ON t.id=d.task_id
  LEFT JOIN LATERAL (
    SELECT canonical_details FROM ape_dts_platform.audit_events a
    WHERE a.subject_id=d.task_id AND a.occurred_at<=d.created_at
      AND a.occurred_at>=COALESCE((SELECT max(previous.created_at) FROM ape_dts_platform.alert_deliveries previous
        WHERE previous.task_id=d.task_id AND previous.rule=d.rule AND previous.event='RECOVERY' AND previous.created_at<d.created_at),'epoch'::timestamptz)
      AND a.action=CASE WHEN d.rule='RECOVERY_FAILED' THEN 'TASK_RUNTIME_AUTO_RECOVERY_MANUAL_REQUIRED'
                       WHEN d.rule='UNEXPECTED_EXIT' THEN 'TASK_RUNTIME_UNEXPECTED_EXIT' ELSE NULL END
    ORDER BY a.occurred_at DESC,a.sequence DESC LIMIT 1
  ) diagnostic ON true`;
const aad = {
  aadVersion: 1 as const,
  ownerType: 'ALERT_SETTINGS',
  ownerId: '1',
  purpose: 'WECOM_WEBHOOK',
  secretId: '1',
};
const ruleNames: Readonly<Record<string, string>> = {
  START_FAILED: '任务启动失败',
  UNEXPECTED_EXIT: '运行进程异常退出',
  RECOVERY_FAILED: '任务恢复失败',
  PLATFORM_METADATA_UNAVAILABLE: '平台元数据库连接异常',
  PLATFORM_CONTROL_UNAVAILABLE: '平台控制面异常',
  PG_SLOT_INACTIVE: 'PG 复制槽未连接',
  PG_SLOT_MISSING: 'PG 复制槽不存在',
  PG_RETAINED_WAL: 'PG 保留 WAL 超限',
  PG_UNCONFIRMED_WAL: 'PG 未确认 WAL 超限',
  KAFKA_OFFSET_LAG: 'Kafka 消费积压超限',
  SYNC_DELAY: '同步时间延迟超限',
};

@Injectable()
export class AlertsService {
  private readonly sender = new WecomAlertSender();
  private readonly logger = new Logger(AlertsService.name);
  private cachedSettings: SettingsRow | undefined;
  private readonly pendingPlatformDeliveries: Array<AlertDelivery & { completedAt: Date }> = [];
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly keys: UnlockedKeyService,
    private readonly audit: AuditLedgerService,
  ) {}

  private async row(
    client: Pick<PoolClient, 'query'> = this.database.requirePool(),
    lock = false,
  ): Promise<SettingsRow> {
    const result = await client.query<SettingsRow>(
      `SELECT enabled, configuration, encrypted_webhook, row_version::text FROM ape_dts_platform.alert_settings WHERE singleton_id=1${lock ? ' FOR UPDATE' : ''}`,
    );
    if (!result.rows[0]) throw new Error('ALERT_SETTINGS_UNAVAILABLE');
    return result.rows[0];
  }
  private view(row: SettingsRow): AlertSettings {
    return {
      ...row.configuration,
      enabled: row.enabled,
      configured: row.encrypted_webhook !== null,
      rowVersion: row.row_version,
    };
  }
  async get(): Promise<AlertSettings> {
    const row = await this.row();
    this.cachedSettings = row;
    return this.view(row);
  }

  async update(actorUserId: string, input: UpdateAlertSettingsRequest): Promise<AlertSettings> {
    if (input.webhookUrl && !isWecomWebhook(input.webhookUrl))
      throw new BadRequestException({ code: 'INVALID_WECOM_WEBHOOK' });
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const current = await this.row(client, true);
      if (current.row_version !== input.expectedRowVersion)
        throw new ConflictException({ code: 'ALERT_SETTINGS_VERSION_CONFLICT' });
      let encrypted = current.encrypted_webhook;
      if (input.webhookUrl) {
        const plaintext = Buffer.from(input.webhookUrl);
        try {
          encrypted = encryptSecret(this.keys.require(), plaintext, aad);
        } finally {
          plaintext.fill(0);
        }
      }
      if (input.enabled && encrypted === null)
        throw new BadRequestException({ code: 'ALERT_WEBHOOK_REQUIRED' });
      const enabled = input.enabled;
      const configuration = {
        channel: input.channel,
        notifyRecovery: input.notifyRecovery,
        durationSeconds: input.durationSeconds,
        repeatSeconds: input.repeatSeconds,
        slotInactive: input.slotInactive,
        slotMissing: input.slotMissing,
        retainedWalBytes: input.retainedWalBytes,
        unconfirmedWalBytes: input.unconfirmedWalBytes,
        kafkaOffsetLag: input.kafkaOffsetLag,
        delayMilliseconds: input.delayMilliseconds,
      };
      await client.query(
        `UPDATE ape_dts_platform.alert_settings SET enabled=$1, configuration=$2::jsonb, encrypted_webhook=$3::jsonb, row_version=row_version+1, updated_at=statement_timestamp() WHERE singleton_id=1`,
        [
          enabled,
          JSON.stringify(configuration),
          encrypted === null ? null : JSON.stringify(encrypted),
        ],
      );
      await this.audit.append(client, {
        action: 'ALERT_SETTINGS_UPDATED',
        actorType: 'USER',
        actorUserId,
        subjectType: 'ALERT_SETTINGS',
        subjectId: null,
        details: {
          enabled,
          channel: configuration.channel,
          destinationChanged: Boolean(input.webhookUrl),
        },
      });
      const result = this.view(await this.row(client));
      await client.query('COMMIT');
      this.cachedSettings = {
        enabled,
        configuration,
        encrypted_webhook: encrypted,
        row_version: result.rowVersion,
      };
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async deliveries(): Promise<{ items: AlertDelivery[] }> {
    const result = await this.database
      .requirePool()
      .query<DeliveryRow>(`${deliverySelect} ORDER BY d.created_at DESC,d.id DESC LIMIT 100`);
    return {
      items: result.rows.map(deliveryView),
    };
  }

  async deliveryContext(id: string): Promise<AlertDeliveryContext> {
    const pool = this.database.requirePool();
    const row = (
      await pool.query<DeliveryRow & { diagnostic_details: Record<string, unknown> | null }>(
        `${contextDeliverySelect} WHERE d.id=$1::uuid`,
        [id],
      )
    ).rows[0];
    if (!row) throw new NotFoundException({ code: 'ALERT_DELIVERY_NOT_FOUND' });
    const empty: AlertDeliveryContext = {
      delivery: deliveryView(row),
      association: 'NONE',
      run: null,
      units: [],
      events: [],
      preflightChecks: [],
      logWindow: null,
    };
    if (!row.task_id) return empty;
    const linkedId = row.diagnostic_details?.oldRunId ?? row.diagnostic_details?.runId;
    const auditRunId =
      typeof linkedId === 'string' &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(linkedId)
        ? linkedId
        : null;
    const alarm = (
      await pool.query<{ created_at: Date }>(
        `SELECT created_at FROM ape_dts_platform.alert_deliveries
      WHERE task_id=$1::uuid AND rule=$2 AND event='ALARM' AND created_at<=$3::timestamptz
      ORDER BY created_at DESC,id DESC LIMIT 1`,
        [row.task_id, row.rule, row.created_at],
      )
    ).rows[0];
    const faultAt = row.event === 'RECOVERY' ? alarm?.created_at : row.created_at;
    // Never infer a recovery's old run from the time of the recovery notification itself.
    if (auditRunId === null && !faultAt) return empty;
    const run = (
      await pool.query<{
        id: string;
        task_revision_id: string;
        status: string;
        current_stage: string;
        failure_code: string | null;
        started_at: Date;
        ended_at: Date | null;
      }>(
        `SELECT id,task_revision_id,status,current_stage,failure_code,started_at,ended_at FROM ape_dts_platform.task_runs
       WHERE task_id=$1::uuid AND (($2::uuid IS NOT NULL AND id=$2::uuid) OR ($2::uuid IS NULL AND started_at<=$3::timestamptz))
       ORDER BY started_at DESC,id DESC LIMIT 1`,
        [row.task_id, auditRunId, faultAt ?? row.created_at],
      )
    ).rows[0];
    if (!run) return empty;
    const units = (
      await pool.query<{
        unit_key: string;
        status: string;
        exit_code: number | null;
        exit_signal: string | null;
        failure_code: string | null;
      }>(
        'SELECT unit_key,status,exit_code,exit_signal,failure_code FROM ape_dts_platform.task_run_units WHERE run_id=$1::uuid ORDER BY unit_key LIMIT 100',
        [run.id],
      )
    ).rows;
    const events = (
      await pool.query<{
        occurred_at: Date;
        action: string;
        canonical_details: Record<string, unknown>;
      }>(
        `SELECT occurred_at,action,canonical_details FROM ape_dts_platform.audit_events
       WHERE subject_id=$1::uuid AND occurred_at<=$3::timestamptz
         AND (canonical_details->>'runId'=$2 OR canonical_details->>'oldRunId'=$2 OR canonical_details->>'newRunId'=$2)
       ORDER BY occurred_at DESC,sequence DESC LIMIT 20`,
        [row.task_id, run.id, row.created_at],
      )
    ).rows;
    const checks = (
      await pool.query<{ key: string; code: string; summary: string }>(
        `SELECT item->>'key' AS key,item->>'code' AS code,item->>'summary' AS summary
       FROM (SELECT result FROM ape_dts_platform.preflight_runs
         WHERE task_revision_id=$1::uuid AND state='FAILED' AND started_at>=$2::timestamptz AND started_at<=$3::timestamptz
         ORDER BY started_at DESC,id DESC LIMIT 1) p CROSS JOIN LATERAL jsonb_array_elements(p.result->'checks') AS item
       WHERE item->>'state'='FAILED' LIMIT 20`,
        [run.task_revision_id, run.started_at, faultAt ?? row.created_at],
      )
    ).rows;
    const center = ['UNEXPECTED_EXIT', 'RECOVERY_FAILED', 'START_FAILED'].includes(row.rule)
      ? (run.ended_at ?? faultAt ?? row.created_at)
      : (faultAt ?? row.created_at);
    return {
      ...empty,
      association: auditRunId ? 'AUDIT' : 'TIME',
      run: {
        id: run.id,
        status: run.status,
        stage: run.current_stage,
        failureCode: safeFaultCode(run.failure_code),
        startedAt: run.started_at.toISOString(),
        endedAt: run.ended_at?.toISOString() ?? null,
      },
      units: units.map((u) => ({
        unitKey: u.unit_key,
        status: u.status,
        exitCode: u.exit_code,
        signal: u.exit_signal,
        failureCode: safeFaultCode(u.failure_code),
      })),
      events: events.map((e) => ({
        occurredAt: e.occurred_at.toISOString(),
        action: e.action,
        reason: safeFaultCode(e.canonical_details.reason),
      })),
      preflightChecks: checks.map((c) => ({
        key: c.key,
        code: safeFaultCode(c.code) ?? 'UNKNOWN',
        summary: c.summary,
      })),
      logWindow: {
        from: new Date(center.getTime() - 2 * 60000).toISOString(),
        to: new Date(center.getTime() + 2 * 60000).toISOString(),
      },
    };
  }

  /** Uses the last committed encrypted destination without requiring the metadata database. */
  async notifyPlatform(
    rule: string,
    event: 'ALARM' | 'RECOVERY',
    detail: string,
  ): Promise<boolean> {
    const row = this.cachedSettings;
    if (
      !row?.enabled ||
      !row.encrypted_webhook ||
      (event === 'RECOVERY' && !row.configuration.notifyRecovery)
    ) {
      this.logger.warn({
        code: 'PLATFORM_NOTIFICATION_UNAVAILABLE',
        rule,
        event,
        cachedDestination: row?.encrypted_webhook != null,
      });
      return false;
    }
    const id = randomUUID();
    const createdAt = new Date().toISOString();
    const result = await this.send(
      row,
      `ape-dts ${event === 'ALARM' ? '平台告警' : '平台告警解除'}\n规则：${ruleNames[rule] ?? rule}\n${detail}`,
    );
    this.logger[result.status === 'SENT' ? 'log' : 'error']({
      code: 'PLATFORM_NOTIFICATION_RESULT',
      rule,
      event,
      status: result.status,
      failureCode: result.failureCode,
      deliveryId: id,
    });
    if (this.pendingPlatformDeliveries.length >= 100) {
      this.pendingPlatformDeliveries.shift();
      this.logger.error({ code: 'PLATFORM_NOTIFICATION_HISTORY_OVERFLOW' });
    }
    this.pendingPlatformDeliveries.push({
      id,
      taskId: null,
      rule,
      event,
      ...result,
      createdAt,
      completedAt: new Date(),
    });
    return result.status === 'SENT';
  }

  async flushPlatformDeliveries() {
    for (const item of [...this.pendingPlatformDeliveries]) {
      await this.database.requirePool().query(
        `INSERT INTO ape_dts_platform.alert_deliveries(id,task_id,rule,event,status,failure_code,created_at,completed_at)
         VALUES($1,NULL,$2,$3,$4,$5,$6::timestamptz,$7::timestamptz) ON CONFLICT(id) DO NOTHING`,
        [
          item.id,
          item.rule,
          item.event,
          item.status,
          item.failureCode,
          item.createdAt,
          item.completedAt,
        ],
      );
      const index = this.pendingPlatformDeliveries.findIndex((entry) => entry.id === item.id);
      if (index >= 0) this.pendingPlatformDeliveries.splice(index, 1);
    }
  }

  private async send(row: SettingsRow, text: string) {
    try {
      if (!row.encrypted_webhook) throw new Error('NO_WEBHOOK');
      const destination = decryptSecret(this.keys.require(), row.encrypted_webhook, aad);
      try {
        return await this.sender.send(destination.toString('utf8'), text);
      } finally {
        destination.fill(0);
      }
    } catch {
      return { status: 'FAILED' as const, failureCode: 'ALERT_DESTINATION_UNAVAILABLE' };
    }
  }

  async test(actorUserId: string): Promise<AlertDelivery> {
    const row = await this.row();
    if (row.encrypted_webhook === null)
      throw new BadRequestException({ code: 'ALERT_WEBHOOK_REQUIRED' });
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      await this.audit.append(client, {
        action: 'ALERT_TEST_REQUESTED',
        actorType: 'USER',
        actorUserId,
        subjectType: 'ALERT_SETTINGS',
        subjectId: null,
        details: { channel: 'WECOM' },
      });
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
    return this.deliver(row, null, 'TEST', 'TEST', 'ape-dts 数据同步平台：告警通道测试。');
  }

  private async deliver(
    row: SettingsRow,
    taskId: string | null,
    rule: string,
    event: AlertDelivery['event'],
    text: string,
  ): Promise<AlertDelivery> {
    const id = randomUUID();
    const created = await this.database
      .requirePool()
      .query<{ created_at: Date }>(
        `INSERT INTO ape_dts_platform.alert_deliveries (id,task_id,rule,event,status) VALUES ($1,$2,$3,$4,'PENDING') RETURNING created_at`,
        [id, taskId, rule, event],
      );
    const result = await this.send(row, text);
    if (result.status === 'FAILED')
      this.logger.error({
        code: 'TASK_NOTIFICATION_FAILED',
        taskId,
        rule,
        event,
        failureCode: result.failureCode,
      });
    await this.database
      .requirePool()
      .query(
        `UPDATE ape_dts_platform.alert_deliveries SET status=$2,failure_code=$3,completed_at=statement_timestamp() WHERE id=$1`,
        [id, result.status, result.failureCode],
      );
    return {
      id,
      taskId,
      rule,
      event,
      ...result,
      createdAt: created.rows[0]!.created_at.toISOString(),
    };
  }

  async observe(
    taskId: string,
    displayName: string,
    rule: string,
    bad: boolean | null,
    immediate = false,
    detail = '',
  ): Promise<void> {
    const row = await this.row();
    if (!row.enabled || !row.encrypted_webhook || bad === null) return;
    const client = await this.database.requirePool().connect();
    let event: 'ALARM' | 'RECOVERY' | null;
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO ape_dts_platform.alert_rule_states (task_id,rule) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
        [taskId, rule],
      );
      const result = await client.query<{
        first_bad_at: Date | null;
        last_attempt_at: Date | null;
        alarm_sent: boolean;
      }>(
        `SELECT first_bad_at,last_attempt_at,alarm_sent FROM ape_dts_platform.alert_rule_states WHERE task_id=$1 AND rule=$2 FOR UPDATE`,
        [taskId, rule],
      );
      const state = result.rows[0]!;
      const decision = evaluateAlertRule(
        {
          firstBadAt: state.first_bad_at?.getTime() ?? null,
          lastAttemptAt: state.last_attempt_at?.getTime() ?? null,
          alarmSent: state.alarm_sent,
        },
        bad,
        Date.now(),
        immediate ? 0 : row.configuration.durationSeconds * 1000,
        row.configuration.repeatSeconds * 1000,
        row.configuration.notifyRecovery,
      );
      event = decision.event;
      await client.query(
        `UPDATE ape_dts_platform.alert_rule_states SET first_bad_at=$3,last_attempt_at=$4,alarm_sent=$5 WHERE task_id=$1 AND rule=$2`,
        [
          taskId,
          rule,
          decision.state.firstBadAt === null ? null : new Date(decision.state.firstBadAt),
          decision.state.lastAttemptAt === null ? null : new Date(decision.state.lastAttemptAt),
          decision.state.alarmSent,
        ],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
    if (event !== null) {
      const result = await this.deliver(
        row,
        taskId,
        rule,
        event,
        `ape-dts ${event === 'ALARM' ? '任务告警' : '告警解除'}\n任务：${displayName.slice(0, 120)}\n规则：${ruleNames[rule] ?? rule}${detail ? `\n${detail.slice(0, 500)}` : ''}`,
      );
      if (result.status === 'SENT')
        await this.database
          .requirePool()
          .query(
            `UPDATE ape_dts_platform.alert_rule_states SET alarm_sent=$3 WHERE task_id=$1 AND rule=$2`,
            [taskId, rule, event === 'ALARM'],
          );
    }
  }
}
