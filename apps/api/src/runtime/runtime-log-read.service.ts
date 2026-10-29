import path from 'node:path';
import { Readable } from 'node:stream';
import { Injectable, Optional } from '@nestjs/common';
import {
  runtimeLogFactSchema,
  runtimeLogLevel,
  type RuntimeLogDownloadQuery,
  type RuntimeLogQuery,
  type RuntimeLogRead,
  type UserSummary,
} from '@scs/contracts';
import { LocalLogSegmentObjectStore } from '@scs/storage';
import { LogPipeline } from '@scs/observability';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { LOCAL_LOG_STORAGE_ROOT, LOG_PIPELINE } from '../storage/storage.tokens.js';
import { Inject } from '@nestjs/common';
import { RuntimeReadNotFoundError } from './runtime-read.service.js';

interface SegmentRow {
  id: string;
  sequence: string;
  started_at: Date;
  state: string;
  stream: 'STDOUT' | 'STDERR';
  run_unit_id: string | null;
  unit_key: string | null;
  object_key: string | null;
  size_bytes: string | null;
  sha256: Buffer | null;
  driver: string | null;
  config: unknown;
  ended_at: Date | null;
}
interface GapRow {
  id: string;
  reason: string;
  lost_bytes: string | null;
  occurred_at: Date;
  run_unit_id: string | null;
  unit_key: string | null;
}
interface DeletionRow {
  log_segment_id: string;
  deleted_at: Date;
  run_unit_id: string | null;
  stream: 'STDOUT' | 'STDERR';
  unit_key: string | null;
}
type RuntimeLogDownloadFilter = RuntimeLogDownloadQuery & { order?: 'ASC' | 'DESC' | undefined };
type RuntimeLogFact = RuntimeLogRead['facts'][number];
const DEFAULT_LOG_SEGMENT_BYTES = 1024 * 1024;
const DEFAULT_LOG_STORAGE_BUFFER_BYTES = 64 * 1024 * 1024;
interface LogReadBudget {
  bytes: number;
  segments: number;
}
class LogScanLimitReached extends Error {}
export class RuntimeLogReadError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

@Injectable()
export class RuntimeLogReadService {
  constructor(
    private readonly database: PlatformDatabaseService,
    @Inject(LOCAL_LOG_STORAGE_ROOT) private readonly localRoot: string,
    @Optional() @Inject(LOG_PIPELINE) private readonly pipeline?: LogPipeline,
  ) {}

  async list(
    actor: UserSummary,
    taskId: string,
    runId: string,
    filter: RuntimeLogQuery,
  ): Promise<RuntimeLogRead> {
    const pool = this.database.requirePool();
    const visible = await pool.query(
      `SELECT 1 FROM ape_dts_platform.tasks task JOIN ape_dts_platform.task_runs run ON run.task_id=task.id
      WHERE task.id=$1::uuid AND run.id=$2::uuid AND task.deleted_at IS NULL AND run.record_purged_at IS NULL AND ($3::text='ADMIN' OR NOT EXISTS (
        SELECT required.data_source_id FROM (VALUES(task.source_data_source_id),(task.target_data_source_id)) required(data_source_id)
        WHERE NOT EXISTS (SELECT 1 FROM ape_dts_platform.data_source_grants grant_row WHERE grant_row.user_id=$4::uuid AND grant_row.data_source_id=required.data_source_id AND grant_row.revoked_at IS NULL)))`,
      [taskId, runId, actor.role, actor.id],
    );
    if (visible.rowCount !== 1) throw new RuntimeReadNotFoundError();
    if (filter.order === 'DESC') await this.pipeline?.flushRun(runId);
    const facts: RuntimeLogFact[] = [];
    let scanLimited = false;
    try {
      for await (const fact of this.timelineFacts(runId, filter, {
        bytes: 8 * 1024 * 1024,
        segments: 64,
      })) {
        facts.push(fact);
        if (facts.length > filter.limit) break;
      }
    } catch (error) {
      if (!(error instanceof LogScanLimitReached)) throw error;
      scanLimited = true;
    }
    return {
      facts:
        filter.order === 'DESC'
          ? facts.slice(0, filter.limit).reverse()
          : facts.slice(0, filter.limit),
      limited: facts.length > filter.limit || scanLimited,
      ...(scanLimited ? { scanLimited: true } : {}),
      runId,
      taskId,
    };
  }
  async download(
    actor: UserSummary,
    taskId: string,
    runId: string,
    filter: RuntimeLogDownloadFilter,
  ) {
    const pool = this.database.requirePool();
    const visible = await pool.query(
      `SELECT 1 FROM ape_dts_platform.tasks task JOIN ape_dts_platform.task_runs run ON run.task_id=task.id
      WHERE task.id=$1::uuid AND run.id=$2::uuid AND task.deleted_at IS NULL AND run.record_purged_at IS NULL AND ($3::text='ADMIN' OR NOT EXISTS (
        SELECT required.data_source_id FROM (VALUES(task.source_data_source_id),(task.target_data_source_id)) required(data_source_id)
        WHERE NOT EXISTS (SELECT 1 FROM ape_dts_platform.data_source_grants grant_row WHERE grant_row.user_id=$4::uuid AND grant_row.data_source_id=required.data_source_id AND grant_row.revoked_at IS NULL)))`,
      [taskId, runId, actor.role, actor.id],
    );
    if (visible.rowCount !== 1) throw new RuntimeReadNotFoundError();
    await this.preflightDownload(runId, filter);
    return {
      content: Readable.from(this.downloadLines(runId, filter)),
      filename: `runtime-${runId}.log`,
    };
  }

  private async preflightDownload(runId: string, filter: RuntimeLogDownloadFilter): Promise<void> {
    const pool = this.database.requirePool();
    let cursor: string | null = null;
    do {
      const result: { rows: SegmentRow[] } = await pool.query<SegmentRow>(
        `SELECT segment.id,segment.sequence::text,segment.started_at,segment.state,segment.stream,segment.run_unit_id,unit.unit_key,object.object_key,object.size_bytes::text,object.sha256,profile.driver,profile.config,segment.ended_at
       FROM ape_dts_platform.log_segments segment LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id
       LEFT JOIN ape_dts_platform.stored_objects object ON object.id=segment.stored_object_id LEFT JOIN ape_dts_platform.storage_profiles profile ON profile.id=object.storage_profile_id
       WHERE segment.run_id=$1::uuid AND segment.state='AVAILABLE' AND ($2::text IS NULL OR segment.stream=$2) AND ($3::text IS NULL OR unit.unit_key=$3)
         AND ($4::timestamptz IS NULL OR segment.ended_at >= $4::timestamptz) AND ($5::timestamptz IS NULL OR segment.started_at < $5::timestamptz)
         AND ($6::uuid IS NULL OR segment.id>$6::uuid) ORDER BY segment.id LIMIT 100`,
        [
          runId,
          filter.stream ?? null,
          filter.unitKey ?? null,
          filter.from ?? null,
          filter.to ?? null,
          cursor,
        ],
      );
      for (const segment of result.rows) {
        if (segment.ended_at === null)
          throw new RuntimeLogReadError('LOG_SEGMENT_BOUNDARY_INVALID');
        this.validateAvailableMetadata(segment);
      }
      cursor = result.rows.at(-1)?.id ?? null;
      if (result.rows.length < 100) break;
    } while (cursor !== null);
    const overlap = await pool.query<{ max_active_bytes: string | null }>(
      `WITH events AS (
         SELECT segment.started_at AS occurred_at,0 AS event_order,object.size_bytes::numeric AS delta
         FROM ape_dts_platform.log_segments segment JOIN ape_dts_platform.stored_objects object ON object.id=segment.stored_object_id
         LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id
         WHERE segment.run_id=$1::uuid AND segment.state='AVAILABLE' AND ($2::text IS NULL OR segment.stream=$2) AND ($3::text IS NULL OR unit.unit_key=$3)
           AND ($4::timestamptz IS NULL OR segment.ended_at >= $4::timestamptz) AND ($5::timestamptz IS NULL OR segment.started_at < $5::timestamptz)
         UNION ALL
         SELECT segment.ended_at,1,-object.size_bytes::numeric
         FROM ape_dts_platform.log_segments segment JOIN ape_dts_platform.stored_objects object ON object.id=segment.stored_object_id
         LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id
         WHERE segment.run_id=$1::uuid AND segment.state='AVAILABLE' AND ($2::text IS NULL OR segment.stream=$2) AND ($3::text IS NULL OR unit.unit_key=$3)
           AND ($4::timestamptz IS NULL OR segment.ended_at >= $4::timestamptz) AND ($5::timestamptz IS NULL OR segment.started_at < $5::timestamptz)
       ), active AS (SELECT SUM(delta) OVER (ORDER BY occurred_at,event_order) AS active_bytes FROM events)
       SELECT MAX(active_bytes)::text AS max_active_bytes FROM active`,
      [
        runId,
        filter.stream ?? null,
        filter.unitKey ?? null,
        filter.from ?? null,
        filter.to ?? null,
      ],
    );
    if (Number(overlap.rows[0]?.max_active_bytes ?? 0) > DEFAULT_LOG_STORAGE_BUFFER_BYTES)
      throw new RuntimeLogReadError('LOG_STORAGE_DECODE_BUDGET_EXCEEDED');
  }

  private async *downloadLines(runId: string, filter: RuntimeLogDownloadFilter) {
    for await (const fact of this.timelineFacts(runId, filter)) {
      yield `${JSON.stringify(fact)}\n`;
    }
  }

  private async *timelineFacts(
    runId: string,
    filter: RuntimeLogDownloadFilter,
    budget?: LogReadBudget,
  ): AsyncGenerator<RuntimeLogFact> {
    yield* mergeLogTimelines(
      [
        this.segmentFacts(runId, filter, budget),
        this.gapFacts(runId, filter),
        this.capacityGapFacts(runId, filter),
      ],
      filter.order === 'DESC',
    );
  }

  private async *segmentFacts(
    runId: string,
    filter: RuntimeLogDownloadFilter,
    budget?: LogReadBudget,
  ): AsyncGenerator<RuntimeLogFact> {
    const segments = this.segmentRows(runId, filter)[Symbol.asyncIterator]();
    const descending = filter.order === 'DESC';
    let nextSegment = await segments.next();
    const active: Array<{
      facts: RuntimeLogFact[];
      index: number;
      sizeBytes: number;
    }> = [];
    let activeBytes = 0;

    while (!nextSegment.done || active.length > 0) {
      if (active.length === 0 && !nextSegment.done) {
        const segment = nextSegment.value;
        const facts = await this.readChronologicalSegmentFacts(segment, filter, runId, budget);
        nextSegment = await segments.next();
        if (facts.length > 0) {
          const sizeBytes = this.segmentBufferBytes(segment);
          if (activeBytes + sizeBytes > DEFAULT_LOG_STORAGE_BUFFER_BYTES)
            throw new RuntimeLogReadError('LOG_STORAGE_DECODE_BUDGET_EXCEEDED');
          active.push({ facts, index: 0, sizeBytes });
          activeBytes += sizeBytes;
        }
        continue;
      }

      let activeIndex = earliestActiveFactIndex(active, descending);
      if (activeIndex < 0) continue;
      let candidate = active[activeIndex]?.facts[active[activeIndex]?.index ?? 0];
      if (candidate === undefined) continue;

      while (
        !nextSegment.done &&
        (descending
          ? segmentBoundaryMilliseconds(nextSegment.value, true) >= Date.parse(candidate.occurredAt)
          : segmentBoundaryMilliseconds(nextSegment.value, false) <=
            Date.parse(candidate.occurredAt))
      ) {
        const segment = nextSegment.value;
        const facts = await this.readChronologicalSegmentFacts(segment, filter, runId, budget);
        nextSegment = await segments.next();
        if (facts.length > 0) {
          const sizeBytes = this.segmentBufferBytes(segment);
          if (activeBytes + sizeBytes > DEFAULT_LOG_STORAGE_BUFFER_BYTES)
            throw new RuntimeLogReadError('LOG_STORAGE_DECODE_BUDGET_EXCEEDED');
          active.push({ facts, index: 0, sizeBytes });
          activeBytes += sizeBytes;
        }
        activeIndex = earliestActiveFactIndex(active, descending);
        candidate = active[activeIndex]?.facts[active[activeIndex]?.index ?? 0];
        if (candidate === undefined) break;
      }

      if (candidate === undefined) continue;
      yield candidate;
      const source = active[activeIndex];
      if (source === undefined) continue;
      source.index += 1;
      if (source.index >= source.facts.length) {
        activeBytes -= source.sizeBytes;
        active.splice(activeIndex, 1);
      }
    }
  }

  private async readChronologicalSegmentFacts(
    segment: SegmentRow,
    filter: RuntimeLogDownloadFilter,
    runId: string,
    budget?: LogReadBudget,
  ): Promise<RuntimeLogFact[]> {
    const records = await this.readSegmentFacts(segment, budget);
    let level: ReturnType<typeof runtimeLogLevel> | undefined;
    const first = records[0];
    if (filter.level !== undefined && first?.kind === 'LINE' && first.continuation > 0) {
      level = await this.previousLogLevel(runId, segment, first.continuation, budget);
    }
    const facts: RuntimeLogFact[] = [];
    let contextGapAdded = false;
    for (const fact of records) {
      if (fact.kind === 'LINE' && fact.continuation === 0) level = runtimeLogLevel(fact.payload);
      if (!isWithinLogTimeRange(fact.occurredAt, filter)) continue;
      if (filter.level === undefined || fact.kind === 'GAP' || level === filter.level)
        facts.push(fact);
      else if (level === undefined && !contextGapAdded) {
        facts.push({
          kind: 'GAP',
          lostBytes: null,
          occurredAt: fact.occurredAt,
          reason: 'LOG_LEVEL_CONTEXT_UNAVAILABLE',
          stream: fact.stream,
          unitId: fact.unitId,
          unitKey: fact.unitKey,
        });
        contextGapAdded = true;
      }
    }
    facts.sort(compareLogFacts);
    if (filter.order === 'DESC') facts.reverse();
    return facts;
  }

  private async previousLogLevel(
    runId: string,
    segment: SegmentRow,
    continuation: number,
    budget?: LogReadBudget,
  ): Promise<ReturnType<typeof runtimeLogLevel> | undefined> {
    let sequence = segment.sequence;
    let expectedContinuation = continuation;
    while (expectedContinuation > 0) {
      const previous = await this.database.requirePool().query<SegmentRow>(
        `SELECT segment.id,segment.sequence::text,segment.started_at,segment.state,segment.stream,segment.run_unit_id,unit.unit_key,object.object_key,object.size_bytes::text,object.sha256,profile.driver,profile.config,segment.ended_at
         FROM ape_dts_platform.log_segments segment LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id
         LEFT JOIN ape_dts_platform.stored_objects object ON object.id=segment.stored_object_id LEFT JOIN ape_dts_platform.storage_profiles profile ON profile.id=object.storage_profile_id
         WHERE segment.run_id=$1::uuid AND segment.stream=$2 AND segment.run_unit_id=$3::uuid AND segment.sequence < $4::bigint AND segment.state <> 'OPEN'
         ORDER BY segment.sequence DESC LIMIT 1`,
        [runId, segment.stream, segment.run_unit_id, sequence],
      );
      const row = previous.rows[0];
      if (row === undefined || row.state !== 'AVAILABLE') return undefined;
      const records = await this.readSegmentFacts(row, budget);
      const last = records.at(-1);
      if (
        last?.kind !== 'LINE' ||
        !last.continues ||
        last.continuation !== expectedContinuation - 1
      )
        return undefined;
      const head = records.findLast((fact) => fact.kind === 'LINE' && fact.continuation === 0);
      if (head?.kind === 'LINE') return runtimeLogLevel(head.payload);
      const first = records[0];
      if (first?.kind !== 'LINE') return undefined;
      expectedContinuation = first.continuation;
      sequence = row.sequence;
    }
    return undefined;
  }

  private async *segmentRows(
    runId: string,
    filter: RuntimeLogDownloadFilter,
  ): AsyncGenerator<SegmentRow> {
    const pool = this.database.requirePool();
    let segmentCursor: SegmentRow | undefined;
    const descending = filter.order === 'DESC';
    const column = descending ? 'ended_at' : 'started_at';
    const comparison = descending ? '<' : '>';
    const direction = descending ? 'DESC' : 'ASC';
    do {
      const page = await pool.query<SegmentRow>(
        `SELECT segment.id,segment.sequence::text,segment.started_at,segment.state,segment.stream,segment.run_unit_id,unit.unit_key,object.object_key,object.size_bytes::text,object.sha256,profile.driver,profile.config,segment.ended_at
         FROM ape_dts_platform.log_segments segment LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id
         LEFT JOIN ape_dts_platform.stored_objects object ON object.id=segment.stored_object_id LEFT JOIN ape_dts_platform.storage_profiles profile ON profile.id=object.storage_profile_id
          WHERE segment.run_id=$1::uuid AND segment.state NOT IN ('OPEN','DELETED') AND ($2::text IS NULL OR segment.stream=$2) AND ($3::text IS NULL OR unit.unit_key=$3)
            AND ($4::timestamptz IS NULL OR segment.ended_at >= $4::timestamptz)
            AND ($5::timestamptz IS NULL OR segment.started_at < $5::timestamptz)
            AND ($6::timestamptz IS NULL OR (segment.${column},segment.id) ${comparison} ($6::timestamptz,$7::uuid))
          ORDER BY segment.${column} ${direction},segment.id ${direction} LIMIT 100`,
        [
          runId,
          filter.stream ?? null,
          filter.unitKey ?? null,
          filter.from ?? null,
          filter.to ?? null,
          (descending ? segmentCursor?.ended_at : segmentCursor?.started_at) ?? null,
          segmentCursor?.id ?? null,
        ],
      );
      for (const segment of page.rows) {
        if (segment.state !== 'OPEN') yield segment;
      }
      segmentCursor = page.rows.at(-1);
      if (page.rows.length < 100) break;
    } while (segmentCursor !== undefined);
  }

  private async *gapFacts(
    runId: string,
    filter: RuntimeLogDownloadFilter,
  ): AsyncGenerator<RuntimeLogFact> {
    const pool = this.database.requirePool();
    let gapCursor: GapRow | undefined;
    const direction = filter.order === 'DESC' ? 'DESC' : 'ASC';
    const comparison = filter.order === 'DESC' ? '<' : '>';
    do {
      const page = await pool.query<GapRow>(
        `SELECT gap.id,gap.reason,gap.lost_bytes::text,gap.occurred_at,gap.run_unit_id,unit.unit_key FROM ape_dts_platform.log_gaps gap LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=gap.run_unit_id WHERE gap.run_id=$1::uuid AND ($2::text IS NULL OR unit.unit_key=$2) AND ($3::timestamptz IS NULL OR gap.occurred_at >= $3::timestamptz) AND ($4::timestamptz IS NULL OR gap.occurred_at < $4::timestamptz) AND ($5::timestamptz IS NULL OR (gap.occurred_at,gap.id)${comparison}($5::timestamptz,$6::uuid)) ORDER BY gap.occurred_at ${direction},gap.id ${direction} LIMIT 100`,
        [
          runId,
          filter.unitKey ?? null,
          filter.from ?? null,
          filter.to ?? null,
          gapCursor?.occurred_at ?? null,
          gapCursor?.id ?? null,
        ],
      );
      for (const gap of page.rows) {
        const fact: RuntimeLogFact = {
          kind: 'GAP',
          lostBytes: gap.lost_bytes,
          occurredAt: gap.occurred_at.toISOString(),
          reason: gap.reason,
          stream: null,
          unitId: gap.run_unit_id,
          unitKey: gap.unit_key,
        };
        if (isWithinLogTimeRange(fact.occurredAt, filter)) yield fact;
      }
      gapCursor = page.rows.at(-1);
      if (page.rows.length < 100) break;
    } while (gapCursor !== undefined);
  }

  private async *capacityGapFacts(
    runId: string,
    filter: RuntimeLogDownloadFilter,
  ): AsyncGenerator<RuntimeLogFact> {
    const pool = this.database.requirePool();
    let deletionCursor: DeletionRow | undefined;
    const direction = filter.order === 'DESC' ? 'DESC' : 'ASC';
    const comparison = filter.order === 'DESC' ? '<' : '>';
    do {
      const page = await pool.query<DeletionRow>(
        `SELECT request.log_segment_id,request.deleted_at,segment.run_unit_id,segment.stream,unit.unit_key FROM ape_dts_platform.log_segment_deletion_requests request JOIN ape_dts_platform.log_segments segment ON segment.id=request.log_segment_id LEFT JOIN ape_dts_platform.task_run_units unit ON unit.id=segment.run_unit_id WHERE request.run_id=$1::uuid AND request.reason='CAPACITY_ROLLOVER' AND ($2::text IS NULL OR segment.stream=$2) AND ($3::text IS NULL OR unit.unit_key=$3) AND ($4::timestamptz IS NULL OR request.deleted_at >= $4::timestamptz) AND ($5::timestamptz IS NULL OR request.deleted_at < $5::timestamptz) AND ($6::timestamptz IS NULL OR (request.deleted_at,request.log_segment_id)${comparison}($6::timestamptz,$7::uuid)) ORDER BY request.deleted_at ${direction},request.log_segment_id ${direction} LIMIT 100`,
        [
          runId,
          filter.stream ?? null,
          filter.unitKey ?? null,
          filter.from ?? null,
          filter.to ?? null,
          deletionCursor?.deleted_at ?? null,
          deletionCursor?.log_segment_id ?? null,
        ],
      );
      for (const deletion of page.rows) {
        const fact: RuntimeLogFact = {
          kind: 'GAP',
          lostBytes: null,
          occurredAt: deletion.deleted_at.toISOString(),
          reason: 'CAPACITY_ROLLOVER',
          stream: deletion.stream,
          unitId: deletion.run_unit_id,
          unitKey: deletion.unit_key,
        };
        if (isWithinLogTimeRange(fact.occurredAt, filter)) yield fact;
      }
      deletionCursor = page.rows.at(-1);
      if (page.rows.length < 100) break;
    } while (deletionCursor !== undefined);
  }

  private async readSegmentFacts(
    segment: SegmentRow,
    budget?: LogReadBudget,
  ): Promise<RuntimeLogRead['facts']> {
    if (segment.state === 'OPEN') return [];
    if (segment.ended_at === null) {
      throw new RuntimeLogReadError('LOG_SEGMENT_BOUNDARY_INVALID');
    }
    const endedAt = segment.ended_at;
    const gap = (reason: string, lostBytes: string | null = null) => ({
      kind: 'GAP' as const,
      lostBytes,
      occurredAt: endedAt.toISOString(),
      reason,
      stream: segment.stream,
      unitId: segment.run_unit_id,
      unitKey: segment.unit_key,
    });
    if (segment.state !== 'AVAILABLE') return [gap(segment.state)];
    this.validateAvailableMetadata(segment);
    if (budget !== undefined) {
      if (budget.segments <= 0 || Number(segment.size_bytes) > budget.bytes)
        throw new LogScanLimitReached();
      budget.segments -= 1;
      budget.bytes -= Number(segment.size_bytes);
    }
    const config = segment.config as { relative_prefix: string };
    const objectKey = segment.object_key as string;
    const sha256 = segment.sha256 as Buffer;
    const sizeBytes = Number(segment.size_bytes);
    const root =
      config.relative_prefix.length === 0
        ? this.localRoot
        : path.join(this.localRoot, ...config.relative_prefix.split('/'));
    let content: Buffer;
    try {
      content = await new LocalLogSegmentObjectStore(root).read({
        objectKey,
        sha256: sha256.toString('hex'),
        sizeBytes,
      });
    } catch (error) {
      return [
        gap(
          error instanceof Error && error.message === 'LOG_SEGMENT_OBJECT_MISSING'
            ? 'MISSING'
            : 'INTEGRITY_FAILED',
          segment.size_bytes,
        ),
      ];
    }
    const facts: RuntimeLogRead['facts'][number][] = [];
    for (const line of content.toString('utf8').split('\n').filter(Boolean)) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        return [gap('LEGACY_FORMAT', segment.size_bytes)];
      }
      const record = runtimeLogFactSchema.safeParse({
        kind: 'LINE',
        ...(typeof parsed === 'object' && parsed !== null ? parsed : {}),
        unitKey: segment.unit_key,
      });
      if (
        !record.success ||
        record.data.kind !== 'LINE' ||
        record.data.stream !== segment.stream ||
        record.data.unitId !== segment.run_unit_id
      )
        return [gap('INTEGRITY_FAILED', segment.size_bytes)];
      facts.push(record.data);
    }
    const startedAt = segment.started_at instanceof Date ? segment.started_at.getTime() : null;
    if (
      startedAt !== null &&
      facts.some((fact) => {
        const timestamp = Date.parse(fact.occurredAt);
        return timestamp < startedAt || timestamp > endedAt.getTime();
      })
    )
      return [gap('INTEGRITY_FAILED', segment.size_bytes)];
    return facts;
  }

  private validateAvailableMetadata(segment: SegmentRow): void {
    if (segment.driver !== 'LOCAL') throw new RuntimeLogReadError('LOG_STORAGE_DRIVER_UNSUPPORTED');
    const config = segment.config as { relative_prefix?: unknown };
    if (
      !isSafeRelativePrefix(config?.relative_prefix) ||
      segment.object_key === null ||
      segment.sha256 === null ||
      segment.size_bytes === null ||
      !Number.isSafeInteger(Number(segment.size_bytes)) ||
      Number(segment.size_bytes) < 0
    )
      throw new RuntimeLogReadError('LOG_STORAGE_PROFILE_CONFIG_INVALID');
    if (Number(segment.size_bytes) > DEFAULT_LOG_SEGMENT_BYTES)
      throw new RuntimeLogReadError('LOG_SEGMENT_OBJECT_TOO_LARGE');
  }

  private segmentBufferBytes(segment: SegmentRow): number {
    return segment.state === 'AVAILABLE' ? Number(segment.size_bytes) : 0;
  }
}

function compareLogFacts(first: RuntimeLogFact, second: RuntimeLogFact): number {
  return (
    first.occurredAt.localeCompare(second.occurredAt) ||
    first.kind.localeCompare(second.kind) ||
    JSON.stringify(first).localeCompare(JSON.stringify(second))
  );
}

function earliestActiveFactIndex(
  active: ReadonlyArray<{ readonly facts: RuntimeLogFact[]; readonly index: number }>,
  descending = false,
): number {
  let earliest = -1;
  for (let index = 0; index < active.length; index += 1) {
    const candidate = active[index]?.facts[active[index]?.index ?? 0];
    const current =
      earliest < 0 ? undefined : active[earliest]?.facts[active[earliest]?.index ?? 0];
    if (
      candidate !== undefined &&
      (current === undefined || compareLogFacts(candidate, current) * (descending ? -1 : 1) < 0)
    ) {
      earliest = index;
    }
  }
  return earliest;
}

function segmentBoundaryMilliseconds(segment: SegmentRow, descending: boolean): number {
  if (descending)
    return segment.ended_at instanceof Date ? segment.ended_at.getTime() : Number.POSITIVE_INFINITY;
  if (segment.started_at instanceof Date) return segment.started_at.getTime();
  if (segment.ended_at instanceof Date) return segment.ended_at.getTime();
  return Number.NEGATIVE_INFINITY;
}

async function* mergeLogTimelines(
  sources: ReadonlyArray<AsyncGenerator<RuntimeLogFact>>,
  descending = false,
): AsyncGenerator<RuntimeLogFact> {
  const cursors: Array<{
    readonly iterator: AsyncGenerator<RuntimeLogFact>;
    value: RuntimeLogFact;
  }> = [];
  try {
    for (const iterator of sources) {
      const next = await iterator.next();
      if (!next.done) cursors.push({ iterator, value: next.value });
    }
    while (cursors.length > 0) {
      let earliest = 0;
      for (let index = 1; index < cursors.length; index += 1) {
        const candidate = cursors[index];
        const current = cursors[earliest];
        if (
          candidate !== undefined &&
          current !== undefined &&
          compareLogFacts(candidate.value, current.value) * (descending ? -1 : 1) < 0
        ) {
          earliest = index;
        }
      }
      const cursor = cursors[earliest];
      if (cursor === undefined) return;
      yield cursor.value;
      const next = await cursor.iterator.next();
      if (next.done) cursors.splice(earliest, 1);
      else cursor.value = next.value;
    }
  } finally {
    await Promise.all(cursors.map(async ({ iterator }) => await iterator.return(undefined)));
  }
}

function isSafeRelativePrefix(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    !value.includes('\\') &&
    !value.includes('\u0000') &&
    !path.posix.isAbsolute(value) &&
    !path.win32.isAbsolute(value) &&
    value.split('/').every((segment) => segment !== '.' && segment !== '..')
  );
}

function isWithinLogTimeRange(
  occurredAt: string,
  filter: Pick<RuntimeLogDownloadFilter, 'from' | 'to'>,
) {
  const timestamp = Date.parse(occurredAt);
  return (
    (filter.from === undefined || timestamp >= Date.parse(filter.from)) &&
    (filter.to === undefined || timestamp < Date.parse(filter.to))
  );
}
