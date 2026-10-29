import { Injectable } from '@nestjs/common';
import type {
  RuntimePerformanceDetail,
  RuntimePerformancePoint,
  RuntimePerformanceSummary,
  RuntimePerformanceValues,
  RuntimePerformanceWindow,
  UserSummary,
} from '@scs/contracts';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

export class RuntimePerformanceNotFoundError extends Error {}

const emptyValues = (): RuntimePerformanceValues => ({
  recordsPerSecond: null,
  bytesPerSecond: null,
  sinkRecords: null,
  sinkBytes: null,
  queueRecords: null,
  queueBytes: null,
});
const fields = [
  'recordsPerSecond',
  'bytesPerSecond',
  'sinkRecords',
  'sinkBytes',
  'queueRecords',
  'queueBytes',
] as const;
const windowMilliseconds: Record<RuntimePerformanceWindow, number> = {
  '1h': 3600_000,
  '24h': 86400_000,
  '7d': 7 * 86400_000,
};
interface RunRow {
  id: string;
  task_id: string;
  status: string;
}
interface SampleRow {
  task_id: string;
  unit_id: string;
  collected_at: Date;
  records_per_second: number | null;
  bytes_per_second: number | null;
  sink_records: number | null;
  sink_bytes: number | null;
  queue_records: number | null;
  queue_bytes: number | null;
  gap: boolean;
}
function values(row: SampleRow): RuntimePerformanceValues {
  return {
    recordsPerSecond: row.records_per_second,
    bytesPerSecond: row.bytes_per_second,
    sinkRecords: row.sink_records,
    sinkBytes: row.sink_bytes,
    queueRecords: row.queue_records,
    queueBytes: row.queue_bytes,
  };
}

@Injectable()
export class RuntimePerformanceReadService {
  constructor(private readonly database: PlatformDatabaseService) {}

  private async visibleIds(actor: UserSummary, taskIds: readonly string[]): Promise<string[]> {
    if (taskIds.length === 0) return [];
    const result = await this.database.requirePool().query<{ id: string }>(
      `SELECT task.id FROM ape_dts_platform.tasks AS task
        WHERE task.id = ANY($1::uuid[]) AND task.deleted_at IS NULL
          AND ($2::text = 'ADMIN' OR NOT EXISTS (
            SELECT 1 FROM (VALUES (task.source_data_source_id), (task.target_data_source_id))
              AS required(data_source_id)
             WHERE NOT EXISTS (
               SELECT 1 FROM ape_dts_platform.data_source_grants AS grant_row
                WHERE grant_row.user_id = $3::uuid
                  AND grant_row.data_source_id = required.data_source_id
                  AND grant_row.revoked_at IS NULL)))`,
      [taskIds, actor.role, actor.id],
    );
    return result.rows.map((row) => row.id);
  }

  async query(
    actor: UserSummary,
    taskIds: readonly string[],
  ): Promise<{ items: RuntimePerformanceSummary[] }> {
    const visible = new Set(await this.visibleIds(actor, taskIds));
    const summaries = await this.summaries([...visible]);
    return {
      items: [...new Set(taskIds)].filter((id) => visible.has(id)).map((id) => summaries.get(id)!),
    };
  }

  private async summaries(taskIds: string[]): Promise<Map<string, RuntimePerformanceSummary>> {
    const result = new Map<string, RuntimePerformanceSummary>();
    if (taskIds.length === 0) return result;
    const pool = this.database.requirePool();
    const runs = await pool.query<RunRow>(
      `SELECT DISTINCT ON (task_id) id, task_id, status FROM ape_dts_platform.task_runs
        WHERE task_id = ANY($1::uuid[]) AND record_purged_at IS NULL
        ORDER BY task_id, CASE WHEN status IN ('STARTING','RUNNING','STOPPING') THEN 0 ELSE 1 END,
                 started_at DESC, id DESC`,
      [taskIds],
    );
    const runningRuns = runs.rows.filter((row) => row.status === 'RUNNING');
    const units = await pool.query<{ id: string; task_id: string }>(
      `SELECT id, task_id FROM ape_dts_platform.task_run_units
        WHERE run_id = ANY($1::uuid[]) AND status = 'RUNNING'`,
      runningRuns.length === 0 ? [[]] : [runningRuns.map((row) => row.id)],
    );
    const latest = await pool.query<SampleRow>(
      `SELECT DISTINCT ON (unit_id) task_id, unit_id, collected_at, records_per_second,
              bytes_per_second, sink_records, sink_bytes, queue_records, queue_bytes,
              gap_reason IS NOT NULL AS gap
         FROM ape_dts_platform.metric_samples_high
        WHERE unit_id = ANY($1::uuid[])
        ORDER BY unit_id, collected_at DESC`,
      [units.rows.map((row) => row.id)],
    );
    const now = Date.now();
    for (const taskId of taskIds) {
      const run = runs.rows.find((row) => row.task_id === taskId);
      const base = { taskId, runId: run?.id ?? null, delayStatus: 'UNSUPPORTED' as const };
      if (run?.status !== 'RUNNING') {
        result.set(taskId, {
          ...base,
          status: 'NOT_RUNNING',
          collectedAt: null,
          values: emptyValues(),
        });
        continue;
      }
      const taskUnits = units.rows.filter((row) => row.task_id === taskId);
      const taskSamples = latest.rows.filter((row) => row.task_id === taskId);
      if (taskSamples.length === 0 || taskUnits.length === 0) {
        result.set(taskId, {
          ...base,
          status: 'NO_DATA',
          collectedAt: null,
          values: emptyValues(),
        });
        continue;
      }
      const fresh = taskSamples.filter(
        (row) =>
          !row.gap &&
          now - row.collected_at.getTime() <= 45_000 &&
          now >= row.collected_at.getTime(),
      );
      const allFresh = fresh.length === taskUnits.length;
      const aggregate = emptyValues();
      if (allFresh)
        for (const field of fields) {
          if (fresh.every((row) => values(row)[field] !== null))
            aggregate[field] = fresh.reduce((total, row) => total + (values(row)[field] ?? 0), 0);
        }
      const status =
        allFresh && fields.every((field) => aggregate[field] !== null)
          ? 'FRESH'
          : fresh.length === 0
            ? 'STALE'
            : 'PARTIAL';
      result.set(taskId, {
        ...base,
        status,
        collectedAt: new Date(
          Math.min(...taskSamples.map((row) => row.collected_at.getTime())),
        ).toISOString(),
        values: aggregate,
      });
    }
    return result;
  }

  async detail(
    actor: UserSummary,
    taskId: string,
    window: RuntimePerformanceWindow,
    requestedRunId?: string,
  ): Promise<RuntimePerformanceDetail> {
    if (!(await this.visibleIds(actor, [taskId])).includes(taskId))
      throw new RuntimePerformanceNotFoundError();
    const pool = this.database.requirePool();
    const currentSummary = (await this.summaries([taskId])).get(taskId)!;
    const history = await pool.query<{
      id: string;
      status: string;
      started_at: Date;
      ended_at: Date | null;
    }>(
      `SELECT id, status, started_at, ended_at FROM ape_dts_platform.task_runs
        WHERE task_id=$1::uuid AND record_purged_at IS NULL
          AND (ended_at IS NULL OR ended_at >= statement_timestamp()-interval '7 days')
        ORDER BY started_at DESC, id DESC`,
      [taskId],
    );
    if (requestedRunId !== undefined && !history.rows.some((run) => run.id === requestedRunId))
      throw new RuntimePerformanceNotFoundError();
    const summary: RuntimePerformanceSummary =
      requestedRunId !== undefined && requestedRunId !== currentSummary.runId
        ? {
            ...currentSummary,
            runId: requestedRunId,
            status: 'NOT_RUNNING',
            collectedAt: null,
            values: emptyValues(),
          }
        : currentSummary;
    const since = new Date(Date.now() - windowMilliseconds[window]);
    const table = window === '1h' ? 'metric_samples_high' : 'metric_rollups_1m';
    const timeColumn = table === 'metric_samples_high' ? 'collected_at' : 'bucket_start';
    const gapColumn =
      table === 'metric_samples_high' ? 'metric.gap_reason IS NOT NULL' : 'metric.gap';
    const seconds = window === '1h' ? 15 : window === '24h' ? 60 : 600;
    const sourceSeconds =
      table === 'metric_samples_high' ? 15 : table === 'metric_rollups_1m' ? 60 : 3600;
    const samples = await pool.query<SampleRow>(
      `WITH per_unit AS (
         SELECT metric.task_id, metric.unit_id,
                floor(extract(epoch FROM metric.${timeColumn}) / $4::integer) AS bucket,
                CASE WHEN count(metric.records_per_second) = count(*) THEN avg(metric.records_per_second) END AS records_per_second,
                CASE WHEN count(metric.bytes_per_second) = count(*) THEN avg(metric.bytes_per_second) END AS bytes_per_second,
                CASE WHEN count(metric.sink_records) = count(*) THEN max(metric.sink_records) END AS sink_records,
                CASE WHEN count(metric.sink_bytes) = count(*) THEN max(metric.sink_bytes) END AS sink_bytes,
                CASE WHEN count(metric.queue_records) = count(*) THEN avg(metric.queue_records) END AS queue_records,
                CASE WHEN count(metric.queue_bytes) = count(*) THEN avg(metric.queue_bytes) END AS queue_bytes,
                bool_or(${gapColumn}) OR
                  count(DISTINCT floor(extract(epoch FROM metric.${timeColumn}) / $5::integer)) <
                    $4::integer / $5::integer AS gap
           FROM ape_dts_platform.${table} AS metric
          WHERE metric.task_id = $1::uuid AND metric.run_id = $3::uuid
            AND metric.${timeColumn} >= $2::timestamptz
          GROUP BY metric.task_id, metric.unit_id,
                   floor(extract(epoch FROM metric.${timeColumn}) / $4::integer)
       )
       SELECT task_id, min(unit_id::text)::uuid AS unit_id,
              to_timestamp(bucket * $4::integer) AS collected_at,
              CASE WHEN count(records_per_second) = count(*) THEN sum(records_per_second) END AS records_per_second,
              CASE WHEN count(bytes_per_second) = count(*) THEN sum(bytes_per_second) END AS bytes_per_second,
              CASE WHEN count(sink_records) = count(*) THEN sum(sink_records) END AS sink_records,
              CASE WHEN count(sink_bytes) = count(*) THEN sum(sink_bytes) END AS sink_bytes,
              CASE WHEN count(queue_records) = count(*) THEN sum(queue_records) END AS queue_records,
              CASE WHEN count(queue_bytes) = count(*) THEN sum(queue_bytes) END AS queue_bytes,
              bool_or(gap) OR count(*) <
                (SELECT count(*) FROM ape_dts_platform.task_run_units AS unit
                  WHERE unit.run_id = $3::uuid
                    AND (unit.started_at IS NULL OR unit.started_at < to_timestamp((bucket + 1) * $4::integer))
                    AND (unit.ended_at IS NULL OR unit.ended_at >= to_timestamp(bucket * $4::integer))) AS gap
         FROM per_unit GROUP BY task_id, bucket ORDER BY bucket LIMIT 2000`,
      [taskId, since, summary.runId, seconds, sourceSeconds],
    );
    const gaps = await pool.query<{ started_at: Date; ended_at: Date }>(
      `SELECT min(greatest(gap.started_at, $2::timestamptz)) AS started_at,
              max(gap.ended_at) AS ended_at
         FROM ape_dts_platform.observability_gaps AS gap
         JOIN ape_dts_platform.task_runs AS run ON run.id = gap.run_id
        WHERE gap.task_id = $1::uuid AND gap.run_id = $3::uuid
          AND gap.ended_at >= $2::timestamptz AND gap.started_at <= now()
          AND run.record_purged_at IS NULL
        GROUP BY floor(extract(epoch FROM greatest(gap.started_at, $2::timestamptz)) / $4::integer)
        ORDER BY started_at`,
      [taskId, since, summary.runId, seconds],
    );
    const bucketMs = seconds * 1000;
    const byBucket = new Map<number, RuntimePerformancePoint>();
    for (const row of samples.rows) {
      const bucket = Math.floor(row.collected_at.getTime() / bucketMs);
      byBucket.set(bucket, {
        ...(row.gap ? emptyValues() : values(row)),
        collectedAt: new Date(bucket * bucketMs).toISOString(),
        gap: row.gap,
      });
    }
    for (const row of gaps.rows) {
      const first = Math.floor(row.started_at.getTime() / bucketMs);
      const last = Math.min(
        Math.floor(row.ended_at.getTime() / bucketMs),
        Math.floor(Date.now() / bucketMs),
      );
      for (let bucket = first; bucket <= last; bucket++)
        byBucket.set(bucket, {
          ...emptyValues(),
          collectedAt: new Date(bucket * bucketMs).toISOString(),
          gap: true,
        });
    }
    const known = [...byBucket.keys()].sort((a, b) => a - b);
    const points: RuntimePerformancePoint[] = [];
    if (known.length > 0)
      for (let bucket = known[0]!; bucket <= known[known.length - 1]!; bucket++) {
        points.push(
          byBucket.get(bucket) ?? {
            ...emptyValues(),
            collectedAt: new Date(bucket * bucketMs).toISOString(),
            gap: true,
          },
        );
      }
    const state = await pool.query<{ status: 'HEALTHY' | 'FAILED'; last_attempt_at: Date }>(
      `SELECT status, last_attempt_at FROM ape_dts_platform.metric_maintenance_state WHERE id = 'global'`,
    );
    return {
      summary,
      window,
      points,
      runs: history.rows.map((run) => ({
        id: run.id,
        status: run.status,
        startedAt: run.started_at.toISOString(),
        endedAt: run.ended_at?.toISOString() ?? null,
      })),
      maintenance: {
        status: state.rows[0]?.status ?? 'UNKNOWN',
        lastAttemptAt: state.rows[0]?.last_attempt_at.toISOString() ?? null,
      },
    };
  }
}
