import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import type { LogSegmentRecord } from '@scs/storage';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PostgresLogSegmentStore } from '../storage/postgres-log-segment-store.js';
import { POSTGRES_LOG_SEGMENT_STORE } from '../storage/storage.tokens.js';

interface RetentionSegmentRow {
  readonly byte_size: string;
  readonly created_at: Date;
  readonly ended_at: Date;
  readonly id: string;
  readonly run_id: string;
  readonly run_unit_id: string;
  readonly sequence: string;
  readonly sha256: string;
  readonly started_at: Date;
  readonly state: LogSegmentRecord['state'];
  readonly stream: LogSegmentRecord['stream'];
}

const RETENTION_INTERVAL_MS = 60 * 60 * 1_000;

@Injectable()
export class LogRetentionService implements OnModuleDestroy, OnModuleInit {
  private readonly logger = new Logger(LogRetentionService.name);
  private timer: NodeJS.Timeout | undefined;
  private inFlight: Promise<void> | undefined;

  constructor(
    private readonly database: PlatformDatabaseService,
    @Inject(POSTGRES_LOG_SEGMENT_STORE) private readonly store: PostgresLogSegmentStore,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.runScheduled(), RETENTION_INTERVAL_MS);
    this.timer.unref();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.timer !== undefined) clearInterval(this.timer);
    this.timer = undefined;
    await this.inFlight;
  }

  private async runScheduled(): Promise<void> {
    if (this.inFlight !== undefined || !this.database.hasPool()) return;
    const current = this.executeScheduled();
    this.inFlight = current;
    try {
      await current;
    } finally {
      if (this.inFlight === current) this.inFlight = undefined;
    }
  }

  private async executeScheduled(): Promise<void> {
    try {
      const result = await this.runOnce();
      this.logger.log(JSON.stringify({ event: 'log.retention.completed', ...result }));
    } catch (error) {
      this.logger.error(
        JSON.stringify({
          error: error instanceof Error ? error.message : 'UNKNOWN_ERROR',
          event: 'log.retention.failed',
        }),
      );
    }
  }

  async runOnce(): Promise<{ logSegmentsDeleted: number; runsPurged: number }> {
    const settings = await this.database
      .requirePool()
      .query<{ log_retention_days: number; run_retention_days: number }>(
        `SELECT log_retention_days,run_retention_days FROM ape_dts_platform.platform_settings WHERE singleton_id=1`,
      );
    const row = settings.rows[0];
    if (row === undefined) throw new Error('LOG_SETTINGS_UNAVAILABLE');
    const segments = await this.database.requirePool().query<RetentionSegmentRow>(
      `SELECT segment.id,segment.run_id,segment.run_unit_id,segment.stream,segment.sequence,
              segment.started_at,segment.ended_at,segment.byte_size,
              encode(segment.sha256,'hex') sha256,segment.state,segment.created_at
         FROM ape_dts_platform.log_segments segment JOIN ape_dts_platform.task_runs run ON run.id=segment.run_id
        WHERE run.ended_at IS NOT NULL AND run.record_purged_at IS NULL
          AND (run.ended_at <= statement_timestamp()-($1::integer*interval '1 day')
            OR run.ended_at <= statement_timestamp()-($2::integer*interval '1 day'))
          AND segment.state IN ('SEALED','AVAILABLE','MISSING','INTEGRITY_FAILED')
        ORDER BY run.ended_at,segment.run_id,segment.stream,segment.sequence`,
      [row.log_retention_days, row.run_retention_days],
    );
    let deleted = 0;
    for (const segment of segments.rows) {
      await this.store.deleteSegment({
        reason: 'RETENTION',
        segment: {
          byteSize: Number(segment.byte_size),
          createdAt: segment.created_at,
          deletedAt: new Date(),
          deletedReason: 'RETENTION',
          endedAt: segment.ended_at,
          id: segment.id,
          runId: segment.run_id,
          runUnitId: segment.run_unit_id,
          sequence: Number(segment.sequence),
          sha256: segment.sha256,
          startedAt: segment.started_at,
          state: 'DELETED',
          stream: segment.stream,
        },
      });
      deleted += 1;
    }
    const candidates = await this.database.requirePool().query<{ id: string }>(
      `SELECT run.id FROM ape_dts_platform.task_runs run
        WHERE run.ended_at IS NOT NULL AND run.record_purged_at IS NULL
          AND run.ended_at <= statement_timestamp()-($1::integer*interval '1 day')
          AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.log_segments s WHERE s.run_id=run.id AND s.state<>'DELETED')
          AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.log_segment_deletion_requests r WHERE r.run_id=run.id AND r.completed_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.capacity_reservations c WHERE c.run_id=run.id AND c.state<>'RELEASED')
        ORDER BY run.ended_at,run.id`,
      [row.run_retention_days],
    );
    let purged = 0;
    for (const candidate of candidates.rows) {
      if (await this.purgeRun(candidate.id, row.run_retention_days)) purged += 1;
    }
    return { logSegmentsDeleted: deleted, runsPurged: purged };
  }

  private async purgeRun(runId: string, runRetentionDays: number): Promise<boolean> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const locked = await client.query(
        `SELECT id FROM ape_dts_platform.task_runs WHERE id=$1::uuid AND ended_at IS NOT NULL AND record_purged_at IS NULL
        AND ended_at <= statement_timestamp()-($2::integer*interval '1 day')
        AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.log_segments s WHERE s.run_id=task_runs.id AND s.state<>'DELETED')
        AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.log_segment_deletion_requests r WHERE r.run_id=task_runs.id AND r.completed_at IS NULL)
        AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.capacity_reservations c WHERE c.run_id=task_runs.id AND c.state<>'RELEASED') FOR UPDATE`,
        [runId, runRetentionDays],
      );
      if (locked.rowCount !== 1) {
        await client.query('ROLLBACK');
        return false;
      }
      const objects = await client.query<{ id: string }>(
        `SELECT stored_object_id id FROM ape_dts_platform.log_segments WHERE run_id=$1::uuid`,
        [runId],
      );
      await client.query(
        `DELETE FROM ape_dts_platform.log_segment_deletion_requests WHERE run_id=$1::uuid AND completed_at IS NOT NULL`,
        [runId],
      );
      await client.query(`DELETE FROM ape_dts_platform.log_gaps WHERE run_id=$1::uuid`, [runId]);
      await client.query(
        `DELETE FROM ape_dts_platform.log_segments WHERE run_id=$1::uuid AND state='DELETED'`,
        [runId],
      );
      await client.query(`DELETE FROM ape_dts_platform.task_run_stages WHERE run_id=$1::uuid`, [
        runId,
      ]);
      await client.query(
        `DELETE FROM ape_dts_platform.capacity_reservations WHERE run_id=$1::uuid AND state='RELEASED'`,
        [runId],
      );
      await client.query(`DELETE FROM ape_dts_platform.task_run_units WHERE run_id=$1::uuid`, [
        runId,
      ]);
      for (const object of objects.rows)
        await client.query(
          `DELETE FROM ape_dts_platform.stored_objects o WHERE o.id=$1::uuid AND o.state='DELETED'
             AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.log_segments s WHERE s.stored_object_id=o.id)
             AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.artifacts a WHERE a.stored_object_id=o.id)
             AND NOT EXISTS(SELECT 1 FROM ape_dts_platform.artifact_probe_runs probe WHERE probe.evidence_object_id=o.id)`,
          [object.id],
        );
      await client.query(
        `UPDATE ape_dts_platform.task_runs SET record_purged_at=statement_timestamp() WHERE id=$1::uuid AND record_purged_at IS NULL`,
        [runId],
      );
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
