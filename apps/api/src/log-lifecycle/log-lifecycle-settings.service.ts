import { createHash, randomBytes } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { UserSummary } from '@scs/contracts';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { LogPipelineCoordinatorService } from './log-pipeline-coordinator.service.js';

export interface LogLifecycleSettings {
  readonly logFailureBufferBytes: string;
  readonly logRetentionDays: number;
  readonly rowVersion: string;
  readonly runLogLimitBytes: string;
  readonly runRetentionDays: number;
}

export type LogLifecycleSettingKey =
  'logRetentionDays' | 'runLogLimitBytes' | 'logFailureBufferBytes' | 'runRetentionDays';

export class LogLifecycleSettingsError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'LogLifecycleSettingsError';
  }
}

interface Confirmation {
  readonly actorId: string;
  readonly expiresAt: number;
  readonly fingerprint: string;
}

@Injectable()
export class LogLifecycleSettingsService {
  private readonly confirmations = new Map<string, Confirmation>();

  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    private readonly pipeline: LogPipelineCoordinatorService,
  ) {}

  async get(): Promise<LogLifecycleSettings> {
    const result = await this.database.requirePool().query<{
      log_failure_buffer_bytes: string;
      log_retention_days: number;
      row_version: string;
      run_log_limit_bytes: string;
      run_retention_days: number;
    }>(`SELECT log_retention_days, run_log_limit_bytes::text, log_failure_buffer_bytes::text,
               run_retention_days, row_version::text
          FROM ape_dts_platform.platform_settings WHERE singleton_id=1`);
    const row = result.rows[0];
    if (row === undefined) throw new LogLifecycleSettingsError('LOG_SETTINGS_UNAVAILABLE');
    return {
      logFailureBufferBytes: row.log_failure_buffer_bytes,
      logRetentionDays: row.log_retention_days,
      rowVersion: row.row_version,
      runLogLimitBytes: row.run_log_limit_bytes,
      runRetentionDays: row.run_retention_days,
    };
  }

  async preview(
    actor: UserSummary,
    input: { key: LogLifecycleSettingKey; rowVersion: string; value: number },
  ): Promise<unknown> {
    this.assertValue(input.value);
    const current = await this.get();
    if (current.rowVersion !== input.rowVersion)
      throw new LogLifecycleSettingsError('LOG_SETTINGS_VERSION_CONFLICT');
    const oldValue = this.settingValue(current, input.key);
    const impact = await this.impact(input.key, input.value);
    const summary = {
      impact,
      key: input.key,
      newValue: input.value,
      oldValue,
      rowVersion: input.rowVersion,
    };
    const token = randomBytes(32).toString('base64url');
    this.confirmations.set(token, {
      actorId: actor.id,
      expiresAt: Date.now() + 5 * 60_000,
      fingerprint: fingerprint(summary),
    });
    return {
      ...summary,
      confirmationRequired: input.value < oldValue && impact.affectedRuns > 0,
      confirmationToken: token,
    };
  }

  async save(
    actor: UserSummary,
    input: {
      confirmationToken?: string | undefined;
      key: LogLifecycleSettingKey;
      rowVersion: string;
      value: number;
    },
  ): Promise<LogLifecycleSettings> {
    this.assertValue(input.value);
    const preview = await this.previewWithoutToken({
      key: input.key,
      rowVersion: input.rowVersion,
      value: input.value,
    });
    if (input.value < preview.oldValue && preview.impact.affectedRuns > 0)
      this.consume(actor.id, input.confirmationToken, fingerprint(preview));
    const column = {
      logFailureBufferBytes: 'log_failure_buffer_bytes',
      logRetentionDays: 'log_retention_days',
      runLogLimitBytes: 'run_log_limit_bytes',
      runRetentionDays: 'run_retention_days',
    }[input.key];
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
        `UPDATE ape_dts_platform.platform_settings SET ${column}=$1, row_version=row_version+1,
              updated_by=$2::uuid, updated_at=statement_timestamp()
          WHERE singleton_id=1 AND row_version=$3::bigint`,
        [input.value, actor.id, input.rowVersion],
      );
      if (result.rowCount !== 1)
        throw new LogLifecycleSettingsError('LOG_SETTINGS_VERSION_CONFLICT');
      await this.audit.append(client, {
        action: 'LOG_LIFECYCLE_SETTING_UPDATED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: {
          key: input.key,
          newValue: input.value,
          oldValue: preview.oldValue,
          previousRowVersion: input.rowVersion,
        },
        subjectId: null,
        subjectType: 'PLATFORM_SETTINGS',
      });
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
    const updated = await this.get();
    if (input.key === 'runLogLimitBytes' || input.key === 'logFailureBufferBytes') {
      await this.pipeline.applyLimits({
        maxBufferedBytes: Number(updated.logFailureBufferBytes),
        maxRunBytes: Number(updated.runLogLimitBytes),
        rowVersion: updated.rowVersion,
      });
    }
    return updated;
  }

  private async previewWithoutToken(input: {
    key: LogLifecycleSettingKey;
    rowVersion: string;
    value: number;
  }) {
    const current = await this.get();
    if (current.rowVersion !== input.rowVersion)
      throw new LogLifecycleSettingsError('LOG_SETTINGS_VERSION_CONFLICT');
    const oldValue = this.settingValue(current, input.key);
    const impact = await this.impact(input.key, input.value);
    return {
      impact,
      key: input.key,
      newValue: input.value,
      oldValue,
      rowVersion: input.rowVersion,
    };
  }

  private settingValue(current: LogLifecycleSettings, key: LogLifecycleSettingKey): number {
    switch (key) {
      case 'logRetentionDays':
        return current.logRetentionDays;
      case 'runRetentionDays':
        return current.runRetentionDays;
      case 'runLogLimitBytes':
        return this.runtimeSettingNumber(current.runLogLimitBytes);
      case 'logFailureBufferBytes':
        return this.runtimeSettingNumber(current.logFailureBufferBytes);
    }
  }

  private runtimeSettingNumber(value: string): number {
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed <= 0)
      throw new LogLifecycleSettingsError('LOG_SETTINGS_VALUE_INVALID');
    return parsed;
  }

  private async impact(key: LogLifecycleSettingKey, value: number) {
    switch (key) {
      case 'logRetentionDays':
        return await this.logRetentionImpact(value);
      case 'runRetentionDays':
        return await this.runRetentionImpact(value);
      case 'runLogLimitBytes':
        return await this.runLogLimitImpact(value);
      case 'logFailureBufferBytes': {
        const snapshot = this.pipeline.snapshotLimits();
        const droppedBytes = Math.max(0, snapshot.pendingBufferedBytes - value);
        return {
          affectedRuns: droppedBytes > 0 ? snapshot.activeRuns : 0,
          bufferedBytes: String(snapshot.pendingBufferedBytes),
          bytes: String(droppedBytes),
          segments: null,
        };
      }
    }
  }

  private async runLogLimitImpact(bytes: number) {
    const result = await this.database.requirePool().query<{
      affected_runs: string;
      bytes: string;
      segments: string;
    }>(
      `WITH affected AS (
         SELECT run.id
           FROM ape_dts_platform.task_runs AS run
           JOIN ape_dts_platform.log_segments AS segment ON segment.run_id = run.id
          WHERE run.record_purged_at IS NULL
            AND segment.state = 'AVAILABLE'
          GROUP BY run.id
         HAVING sum(segment.byte_size) > $1::bigint
       )
       SELECT count(DISTINCT affected.id)::text AS affected_runs,
              count(segment.id)::text AS segments,
              COALESCE(sum(segment.byte_size), 0)::text AS bytes
         FROM affected
         JOIN ape_dts_platform.log_segments AS segment ON segment.run_id = affected.id
        WHERE segment.state = 'AVAILABLE'`,
      [bytes],
    );
    const row = result.rows[0]!;
    return {
      affectedRuns: Number(row.affected_runs),
      bytes: row.bytes,
      segments: Number(row.segments),
    };
  }

  private async logRetentionImpact(days: number) {
    const result = await this.database
      .requirePool()
      .query<{ affected_runs: string; bytes: string; segments: string }>(
        `SELECT count(DISTINCT run.id)::text affected_runs, count(segment.id)::text segments,
              COALESCE(sum(segment.byte_size) FILTER (WHERE segment.state<>'DELETED'),0)::text bytes
         FROM ape_dts_platform.task_runs run JOIN ape_dts_platform.log_segments segment ON segment.run_id=run.id
        WHERE run.ended_at IS NOT NULL AND run.record_purged_at IS NULL
          AND run.ended_at <= statement_timestamp()-($1::integer * interval '1 day')
          AND segment.state<>'DELETED'`,
        [days],
      );
    const row = result.rows[0]!;
    return {
      affectedRuns: Number(row.affected_runs),
      bytes: row.bytes,
      segments: Number(row.segments),
    };
  }

  private async runRetentionImpact(days: number) {
    const result = await this.database.requirePool().query<{
      affected_runs: string;
      bytes: string;
      capacity: string;
      gaps: string;
      segments: string;
      stages: string;
      units: string;
    }>(
      `WITH eligible AS (
         SELECT run.id FROM ape_dts_platform.task_runs run
          WHERE run.ended_at IS NOT NULL AND run.record_purged_at IS NULL
            AND run.ended_at <= statement_timestamp()-($1::integer * interval '1 day')
       )
       SELECT (SELECT count(*)::text FROM eligible) affected_runs,
         (SELECT count(*)::text FROM ape_dts_platform.task_run_stages stage JOIN eligible ON eligible.id=stage.run_id) stages,
         (SELECT count(*)::text FROM ape_dts_platform.task_run_units unit JOIN eligible ON eligible.id=unit.run_id) units,
         (SELECT count(*)::text FROM ape_dts_platform.log_segments segment JOIN eligible ON eligible.id=segment.run_id WHERE segment.state<>'DELETED') segments,
         (SELECT COALESCE(sum(segment.byte_size),0)::text FROM ape_dts_platform.log_segments segment JOIN eligible ON eligible.id=segment.run_id WHERE segment.state<>'DELETED') bytes,
         (SELECT count(*)::text FROM ape_dts_platform.log_gaps gap JOIN eligible ON eligible.id=gap.run_id) gaps,
         (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations reservation JOIN eligible ON eligible.id=reservation.run_id) capacity`,
      [days],
    );
    const row = result.rows[0]!;
    return {
      affectedRuns: Number(row.affected_runs),
      bytes: row.bytes,
      capacity: Number(row.capacity),
      gaps: Number(row.gaps),
      segments: Number(row.segments),
      stages: Number(row.stages),
      units: Number(row.units),
    };
  }

  private consume(actorId: string, token: string | undefined, expected: string) {
    if (token === undefined)
      throw new LogLifecycleSettingsError('LOG_SETTINGS_CONFIRMATION_REQUIRED');
    const value = this.confirmations.get(token);
    this.confirmations.delete(token);
    if (
      value === undefined ||
      value.actorId !== actorId ||
      value.expiresAt < Date.now() ||
      value.fingerprint !== expected
    )
      throw new LogLifecycleSettingsError('LOG_SETTINGS_CONFIRMATION_INVALID');
  }
  private assertValue(value: number) {
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new LogLifecycleSettingsError('LOG_SETTINGS_VALUE_INVALID');
  }
}

function fingerprint(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
