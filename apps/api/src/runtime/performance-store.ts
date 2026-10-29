import { Injectable } from '@nestjs/common';
import { runtimePerformanceValuesSchema, type RuntimePerformanceValues } from '@scs/contracts';
import type { PoolClient } from 'pg';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

export interface RuntimePerformanceBinding {
  readonly taskId: string;
  readonly runId: string;
  readonly unitId: string;
  readonly processToken: string;
  readonly pid: number;
  readonly port: number;
}

export interface RuntimePerformanceSample {
  readonly collectedAt: Date;
  readonly values: RuntimePerformanceValues;
  readonly gapReason: string | null;
}

export interface RuntimePerformanceGap {
  readonly startedAt: Date;
  readonly endedAt: Date;
  readonly reason: string;
}

type PartitionKind = 'metric_samples_high' | 'metric_rollups_1m' | 'metric_rollups_1h';

function partition(kind: PartitionKind, date: Date): { name: string; from: string; to: string } {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth();
  const day = date.getUTCDate();
  if (kind === 'metric_samples_high') {
    const from = new Date(Date.UTC(year, month, day));
    const to = new Date(Date.UTC(year, month, day + 1));
    return {
      name: `${kind}_${from.toISOString().slice(0, 10).replace(/-/gu, '')}`,
      from: from.toISOString(),
      to: to.toISOString(),
    };
  }
  if (kind === 'metric_rollups_1m') {
    const from = new Date(Date.UTC(year, month, 1));
    const to = new Date(Date.UTC(year, month + 1, 1));
    return {
      name: `${kind}_${from.toISOString().slice(0, 7).replace(/-/gu, '')}`,
      from: from.toISOString(),
      to: to.toISOString(),
    };
  }
  const from = new Date(Date.UTC(year, 0, 1));
  const to = new Date(Date.UTC(year + 1, 0, 1));
  return { name: `${kind}_${year}`, from: from.toISOString(), to: to.toISOString() };
}

function datesBetween(from: Date, to: Date, step: 'day' | 'month' | 'year'): Date[] {
  const dates: Date[] = [];
  const cursor = new Date(from);
  cursor.setUTCHours(0, 0, 0, 0);
  if (step === 'month' || step === 'year') cursor.setUTCDate(1);
  if (step === 'year') cursor.setUTCMonth(0);
  while (cursor <= to) {
    dates.push(new Date(cursor));
    if (step === 'day') cursor.setUTCDate(cursor.getUTCDate() + 1);
    else if (step === 'month') cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    else cursor.setUTCFullYear(cursor.getUTCFullYear() + 1);
  }
  return dates;
}

@Injectable()
export class RuntimePerformanceStore {
  constructor(private readonly database: PlatformDatabaseService) {}

  private async ensurePartition(
    client: PoolClient,
    kind: PartitionKind,
    date: Date,
  ): Promise<void> {
    const target = partition(kind, date);
    // Every identifier comes from fixed enum values and UTC date digits, never user input.
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))', [
      'runtime-performance-partition',
      target.name,
    ]);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ape_dts_platform.${target.name}
         PARTITION OF ape_dts_platform.${kind}
         FOR VALUES FROM ('${target.from}') TO ('${target.to}')`,
    );
  }

  private async dropExpiredPartitions(
    client: PoolClient,
    kind: PartitionKind,
    cutoff: Date,
  ): Promise<void> {
    const partitions = await client.query<{ relname: string }>(
      `SELECT child.relname FROM pg_inherits AS inheritance
         JOIN pg_class AS child ON child.oid = inheritance.inhrelid
         JOIN pg_namespace AS namespace ON namespace.oid = child.relnamespace
         JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
         JOIN pg_namespace AS parent_namespace ON parent_namespace.oid = parent.relnamespace
        WHERE namespace.nspname = 'ape_dts_platform'
          AND parent_namespace.nspname = 'ape_dts_platform' AND parent.relname = $1`,
      [kind],
    );
    const digits = kind === 'metric_samples_high' ? 8 : kind === 'metric_rollups_1m' ? 6 : 4;
    for (const { relname } of partitions.rows) {
      const suffix = relname.slice(kind.length + 1);
      if (relname !== `${kind}_${suffix}` || !new RegExp(`^[0-9]{${digits}}$`, 'u').test(suffix))
        continue;
      const year = Number(suffix.slice(0, 4));
      const month = digits >= 6 ? Number(suffix.slice(4, 6)) - 1 : 0;
      const day = digits === 8 ? Number(suffix.slice(6, 8)) : 1;
      const start = new Date(Date.UTC(year, month, day));
      if (
        start.getUTCFullYear() !== year ||
        start.getUTCMonth() !== month ||
        start.getUTCDate() !== day
      )
        continue;
      const end =
        digits === 8
          ? Date.UTC(year, month, day + 1)
          : digits === 6
            ? Date.UTC(year, month + 1, 1)
            : Date.UTC(year + 1, 0, 1);
      if (end <= cutoff.getTime()) await client.query(`DROP TABLE ape_dts_platform.${relname}`);
    }
  }

  async append(
    binding: RuntimePerformanceBinding,
    sample: RuntimePerformanceSample,
  ): Promise<boolean> {
    const values = runtimePerformanceValuesSchema.parse(sample.values);
    if (
      !Number.isSafeInteger(binding.pid) ||
      binding.pid <= 0 ||
      !Number.isSafeInteger(binding.port) ||
      binding.port < 1 ||
      binding.port > 65535 ||
      !Number.isFinite(sample.collectedAt.getTime()) ||
      (sample.gapReason !== null && !/^[A-Z][A-Z0-9_]{0,127}$/u.test(sample.gapReason))
    ) {
      throw new Error('PERFORMANCE_SAMPLE_INVALID');
    }
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const current = await client.query(
        `SELECT 1 FROM ape_dts_platform.tasks AS task
           JOIN ape_dts_platform.task_runs AS run ON run.task_id = task.id
           JOIN ape_dts_platform.task_run_units AS unit
             ON unit.run_id = run.id AND unit.task_id = task.id
          WHERE task.id = $1::uuid AND task.deleted_at IS NULL
            AND run.id = $2::uuid AND run.status = 'RUNNING' AND run.record_purged_at IS NULL
            AND unit.id = $3::uuid AND unit.status = 'RUNNING'
            AND unit.process_token = $4 AND unit.process_pid = $5
            AND unit.metrics_port = $6 AND unit.metrics_port_released_at IS NULL
          FOR SHARE OF task, run, unit`,
        [
          binding.taskId,
          binding.runId,
          binding.unitId,
          binding.processToken,
          binding.pid,
          binding.port,
        ],
      );
      if (current.rowCount !== 1) {
        await client.query('ROLLBACK');
        return false;
      }
      await this.ensurePartition(client, 'metric_samples_high', sample.collectedAt);
      const inserted = await client.query(
        `INSERT INTO ape_dts_platform.metric_samples_high
           (task_id, run_id, unit_id, collected_at, records_per_second, bytes_per_second,
            sink_records, sink_bytes, queue_records, queue_bytes, gap_reason)
         VALUES ($1::uuid,$2::uuid,$3::uuid,$4::timestamptz,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT (task_id, run_id, unit_id, collected_at) DO NOTHING`,
        [
          binding.taskId,
          binding.runId,
          binding.unitId,
          sample.collectedAt,
          values.recordsPerSecond,
          values.bytesPerSecond,
          values.sinkRecords,
          values.sinkBytes,
          values.queueRecords,
          values.queueBytes,
          sample.gapReason,
        ],
      );
      if (sample.gapReason !== null && inserted.rowCount === 1) {
        await this.insertGap(client, binding, {
          startedAt: sample.collectedAt,
          endedAt: sample.collectedAt,
          reason: sample.gapReason,
        });
      }
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async insertGap(
    client: PoolClient,
    binding: RuntimePerformanceBinding,
    gap: RuntimePerformanceGap,
  ): Promise<void> {
    await client.query(
      `INSERT INTO ape_dts_platform.observability_gaps
         (task_id, run_id, unit_id, started_at, ended_at, reason)
       VALUES ($1::uuid,$2::uuid,$3::uuid,$4::timestamptz,$5::timestamptz,$6)`,
      [binding.taskId, binding.runId, binding.unitId, gap.startedAt, gap.endedAt, gap.reason],
    );
  }

  async recordGap(binding: RuntimePerformanceBinding, gap: RuntimePerformanceGap): Promise<void> {
    if (
      !(gap.startedAt instanceof Date) ||
      !(gap.endedAt instanceof Date) ||
      !Number.isFinite(gap.startedAt.getTime()) ||
      !Number.isFinite(gap.endedAt.getTime()) ||
      gap.endedAt < gap.startedAt ||
      !/^[A-Z][A-Z0-9_]{0,127}$/u.test(gap.reason)
    )
      throw new Error('PERFORMANCE_GAP_INVALID');
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const identity = await client.query(
        `SELECT 1 FROM ape_dts_platform.tasks AS task
           JOIN ape_dts_platform.task_runs AS run ON run.task_id = task.id
           JOIN ape_dts_platform.task_run_units AS unit
             ON unit.run_id = run.id AND unit.task_id = task.id
          WHERE task.id = $1::uuid AND task.deleted_at IS NULL
            AND run.id = $2::uuid AND run.record_purged_at IS NULL
            AND unit.id = $3::uuid AND unit.process_token = $4
            AND unit.process_pid = $5 AND unit.metrics_port = $6
          FOR SHARE OF task, run, unit`,
        [
          binding.taskId,
          binding.runId,
          binding.unitId,
          binding.processToken,
          binding.pid,
          binding.port,
        ],
      );
      if (identity.rowCount !== 1) throw new Error('PERFORMANCE_BINDING_NOT_FOUND');
      await this.insertGap(client, binding, gap);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async maintain(now = new Date()): Promise<void> {
    if (!Number.isFinite(now.getTime())) throw new Error('PERFORMANCE_TIME_INVALID');
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext('runtime-performance-maintenance'))",
      );
      const highCutoff = new Date(now.getTime() - 7 * 86400_000);
      const minuteCutoff = new Date(now.getTime() - 7 * 86400_000);
      const hourCutoff = new Date(now.getTime() - 7 * 86400_000);
      const previous = await client.query<{ status: string; last_attempt_at: Date }>(
        `SELECT status, last_attempt_at FROM ape_dts_platform.metric_maintenance_state
          WHERE id = 'global' FOR UPDATE`,
      );
      const lastSuccess =
        previous.rows[0]?.status === 'HEALTHY' ? previous.rows[0].last_attempt_at.getTime() : null;
      // Never overwrite an old complete rollup using only the surviving tail of
      // a partially expired source bucket. Revisit complete buckets only.
      const minuteFrom = new Date(
        Math.max(
          Math.ceil(highCutoff.getTime() / 60_000) * 60_000,
          lastSuccess === null ? 0 : Math.floor((lastSuccess - 120_000) / 60_000) * 60_000,
        ),
      );
      const hourFrom = new Date(
        Math.max(
          Math.ceil(minuteCutoff.getTime() / 3_600_000) * 3_600_000,
          lastSuccess === null ? 0 : Math.floor((lastSuccess - 7_200_000) / 3_600_000) * 3_600_000,
        ),
      );
      for (const date of datesBetween(highCutoff, new Date(now.getTime() + 86400_000), 'day'))
        await this.ensurePartition(client, 'metric_samples_high', date);
      for (const date of datesBetween(
        minuteCutoff,
        new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1)),
        'month',
      ))
        await this.ensurePartition(client, 'metric_rollups_1m', date);
      for (const date of datesBetween(
        hourCutoff,
        new Date(Date.UTC(now.getUTCFullYear() + 1, 0, 1)),
        'year',
      ))
        await this.ensurePartition(client, 'metric_rollups_1h', date);
      await client.query(
        `INSERT INTO ape_dts_platform.metric_rollups_1m
           (task_id, run_id, unit_id, bucket_start, records_per_second, bytes_per_second,
            sink_records, sink_bytes, queue_records, queue_bytes, gap, sample_count)
         SELECT task_id, run_id, unit_id, date_trunc('minute', collected_at),
                CASE WHEN count(records_per_second) = count(*) THEN avg(records_per_second) END,
                CASE WHEN count(bytes_per_second) = count(*) THEN avg(bytes_per_second) END,
                CASE WHEN count(sink_records) = count(*) THEN max(sink_records) END,
                CASE WHEN count(sink_bytes) = count(*) THEN max(sink_bytes) END,
                CASE WHEN count(queue_records) = count(*) THEN avg(queue_records) END,
                CASE WHEN count(queue_bytes) = count(*) THEN avg(queue_bytes) END,
                bool_or(gap_reason IS NOT NULL) OR count(DISTINCT floor(extract(epoch FROM collected_at) / 15)) < 4,
                count(*)::integer
           FROM ape_dts_platform.metric_samples_high
          WHERE collected_at >= $1::timestamptz
            AND collected_at < date_trunc('minute', $2::timestamptz)
          GROUP BY task_id, run_id, unit_id, date_trunc('minute', collected_at)
         ON CONFLICT (task_id, run_id, unit_id, bucket_start) DO UPDATE SET
           records_per_second=EXCLUDED.records_per_second,
           bytes_per_second=EXCLUDED.bytes_per_second,
           sink_records=EXCLUDED.sink_records, sink_bytes=EXCLUDED.sink_bytes,
           queue_records=EXCLUDED.queue_records, queue_bytes=EXCLUDED.queue_bytes,
           gap=EXCLUDED.gap, sample_count=EXCLUDED.sample_count`,
        [minuteFrom, now],
      );
      await client.query(
        `INSERT INTO ape_dts_platform.metric_rollups_1h
           (task_id, run_id, unit_id, bucket_start, records_per_second, bytes_per_second,
            sink_records, sink_bytes, queue_records, queue_bytes, gap, sample_count)
         SELECT task_id, run_id, unit_id, date_trunc('hour', bucket_start),
                CASE WHEN count(records_per_second) = count(*) THEN avg(records_per_second) END,
                CASE WHEN count(bytes_per_second) = count(*) THEN avg(bytes_per_second) END,
                CASE WHEN count(sink_records) = count(*) THEN max(sink_records) END,
                CASE WHEN count(sink_bytes) = count(*) THEN max(sink_bytes) END,
                CASE WHEN count(queue_records) = count(*) THEN avg(queue_records) END,
                CASE WHEN count(queue_bytes) = count(*) THEN avg(queue_bytes) END,
                bool_or(gap) OR count(DISTINCT bucket_start) < 60,
                sum(sample_count)::integer
           FROM ape_dts_platform.metric_rollups_1m
          WHERE bucket_start >= $1::timestamptz
            AND bucket_start < date_trunc('hour', $2::timestamptz)
          GROUP BY task_id, run_id, unit_id, date_trunc('hour', bucket_start)
         ON CONFLICT (task_id, run_id, unit_id, bucket_start) DO UPDATE SET
           records_per_second=EXCLUDED.records_per_second,
           bytes_per_second=EXCLUDED.bytes_per_second,
           sink_records=EXCLUDED.sink_records, sink_bytes=EXCLUDED.sink_bytes,
           queue_records=EXCLUDED.queue_records, queue_bytes=EXCLUDED.queue_bytes,
           gap=EXCLUDED.gap, sample_count=EXCLUDED.sample_count`,
        [hourFrom, now],
      );
      await client.query(
        'DELETE FROM ape_dts_platform.metric_samples_high WHERE collected_at < $1',
        [highCutoff],
      );
      await client.query('DELETE FROM ape_dts_platform.metric_rollups_1m WHERE bucket_start < $1', [
        minuteCutoff,
      ]);
      await client.query('DELETE FROM ape_dts_platform.metric_rollups_1h WHERE bucket_start < $1', [
        hourCutoff,
      ]);
      await client.query('DELETE FROM ape_dts_platform.observability_gaps WHERE ended_at < $1', [
        hourCutoff,
      ]);
      await this.dropExpiredPartitions(client, 'metric_samples_high', highCutoff);
      await this.dropExpiredPartitions(client, 'metric_rollups_1m', minuteCutoff);
      await this.dropExpiredPartitions(client, 'metric_rollups_1h', hourCutoff);
      await client.query(
        `INSERT INTO ape_dts_platform.metric_maintenance_state(id,status,last_attempt_at)
         VALUES ('global','HEALTHY',$1)
         ON CONFLICT (id) DO UPDATE SET status='HEALTHY',last_attempt_at=EXCLUDED.last_attempt_at`,
        [now],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      await this.database
        .requirePool()
        .query(
          `INSERT INTO ape_dts_platform.metric_maintenance_state(id,status,last_attempt_at)
         VALUES ('global','FAILED',$1)
         ON CONFLICT (id) DO UPDATE SET status='FAILED',last_attempt_at=EXCLUDED.last_attempt_at`,
          [now],
        )
        .catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
