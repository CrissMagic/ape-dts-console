import { createHash, randomUUID } from 'node:crypto';
import { StringDecoder } from 'node:string_decoder';

import type {
  LogGapReason,
  LogGapRecord,
  LogPendingRecoveryState,
  LogRecoveryState,
  LogSegmentRecord,
  LogSegmentStore,
  LogSegmentStream,
  LogStorageState,
} from '@scs/storage';
import { LogSegmentPersistenceError } from '@scs/storage';

export const DEFAULT_LOG_SEGMENT_BYTES = 1 * 1024 * 1024;
export const DEFAULT_LOG_RUN_BYTES = 1 * 1024 * 1024 * 1024;
export const DEFAULT_LOG_STORAGE_BUFFER_BYTES = 64 * 1024 * 1024;

const LOOK_BEHIND_BYTES = 1024;
const DEFAULT_LOG_RECORD_PAYLOAD_BYTES = 64 * 1024;
const LOG_RECORD_ENVELOPE_BYTES = 2048;
const SENSITIVE_PREFIX_PATTERN =
  /\b(?:password|passwd|pwd|secret|token|authorization|api[_-]?key)\b\s*[:=]|(?:https?|postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|rediss|kafka):\/\/[^\s/:@]+:/giu;

export function redactLogText(value: string): string {
  return value
    .replace(
      /(\b(?:password|passwd|pwd|secret|token|authorization|api[_-]?key)\b\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s"'&,;]+)/giu,
      '$1[REDACTED]',
    )
    .replace(
      /((?:https?|postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|rediss|kafka):\/\/[^\s/:@]+:)([^@\s/]+)(@)/giu,
      '$1[REDACTED]$3',
    )
    .replace(/([?&](?:password|passwd|pwd|secret|token|api[_-]?key)=)[^&\s]*/giu, '$1[REDACTED]');
}

export class LogPipelineRedactionError extends Error {
  readonly code = 'LOG_REDACTION_FAILED';

  constructor() {
    super('LOG_REDACTION_FAILED');
    this.name = 'LogPipelineRedactionError';
  }
}

/** A bounded raw look-behind that prevents credentials split across chunks. */
export class StreamingLogRedactor {
  private pending = '';
  private decoder = new StringDecoder('utf8');
  private undecodedBytes = 0;

  constructor(
    private readonly redact: (value: string) => string = redactLogText,
    private readonly lookBehindBytes = LOOK_BEHIND_BYTES,
  ) {}

  append(value: string): string {
    this.pending += value;
    const boundary = this.safeBoundary();
    if (boundary <= 0) {
      return '';
    }
    const candidate = this.pending.slice(0, boundary);
    const remaining = this.pending.slice(boundary);
    let redacted: string;
    try {
      redacted = this.redact(candidate);
    } catch {
      throw new LogPipelineRedactionError();
    }
    this.pending = remaining;
    return redacted;
  }

  flush(): string {
    this.pending += this.decoder.end();
    this.decoder = new StringDecoder('utf8');
    this.undecodedBytes = 0;
    if (this.pending.length === 0) {
      return '';
    }
    const candidate = this.pending;
    let redacted: string;
    try {
      redacted = this.redact(candidate);
    } catch {
      throw new LogPipelineRedactionError();
    }
    this.pending = '';
    return redacted;
  }

  flushCompleteLines(): string {
    let boundary = this.pending.lastIndexOf('\n') + 1;
    if (boundary === 0) return '';
    // A credential value or even its key may continue on the following line.
    let previousBoundary: number;
    do {
      previousBoundary = boundary;
      const complete = this.pending.slice(0, boundary);
      for (const match of complete.matchAll(SENSITIVE_PREFIX_PATTERN)) {
        const value = complete.slice(match.index + match[0].length).trimStart();
        if (
          value.length === 0 ||
          ((value[0] === '"' || value[0] === "'") && value.indexOf(value[0], 1) < 0)
        ) {
          boundary = Math.min(boundary, complete.lastIndexOf('\n', match.index - 1) + 1);
        }
      }
      const unfinishedKey =
        /\b(?:password|passwd|pwd|secret|token|authorization|api[_-]?key)\b\s*$/iu.exec(complete);
      if (unfinishedKey !== null)
        boundary = Math.min(boundary, complete.lastIndexOf('\n', unfinishedKey.index - 1) + 1);
    } while (boundary !== previousBoundary);
    if (boundary === 0) return '';
    let redacted: string;
    try {
      redacted = this.redact(this.pending.slice(0, boundary));
    } catch {
      throw new LogPipelineRedactionError();
    }
    this.pending = this.pending.slice(boundary);
    return redacted;
  }

  discard(): number {
    const bytes = Buffer.byteLength(this.pending, 'utf8') + this.undecodedBytes;
    this.pending = '';
    this.decoder = new StringDecoder('utf8');
    this.undecodedBytes = 0;
    return bytes;
  }

  appendBytes(value: Buffer): string {
    const decoded = this.decoder.write(value);
    this.undecodedBytes = Math.max(
      0,
      this.undecodedBytes + value.length - Buffer.byteLength(decoded),
    );
    return this.append(decoded);
  }

  private safeBoundary(): number {
    const hardBoundary = this.pending.length - this.lookBehindBytes;
    if (hardBoundary <= 0) {
      return 0;
    }
    let boundary = hardBoundary;
    for (const match of this.pending.matchAll(SENSITIVE_PREFIX_PATTERN)) {
      const index = match.index ?? 0;
      if (index < boundary && index >= hardBoundary - this.lookBehindBytes) {
        boundary = index;
      }
    }
    const previous = this.pending.charCodeAt(boundary - 1);
    if (previous >= 0xd800 && previous <= 0xdbff) boundary--;
    return boundary;
  }
}

export interface LogPipelineEntry {
  readonly chunk: Buffer | Uint8Array | string;
  readonly runId: string;
  readonly stream: LogSegmentStream;
  readonly unitId: string;
}

export interface LogPipelineOptions {
  readonly maxBufferedBytes?: number;
  readonly maxRunBytes?: number;
  readonly maxSegmentBytes?: number;
  readonly now?: () => Date;
  readonly redact?: (value: string) => string;
  readonly retryDelayMilliseconds?: number;
  readonly store: LogSegmentStore;
  /** Test-only compatibility for historical byte-segmentation assertions. */
  readonly structuredRecords?: boolean;
  readonly maxRecordPayloadBytes?: number;
}

export interface LogPipelineCloseInput {
  readonly runId: string;
  readonly unitId: string;
}

export interface LogPipelineCloseResult {
  readonly recoveryState: LogRecoveryState;
  readonly storageState: LogStorageState;
}

export interface LogPipelineSettlementResult extends LogPipelineCloseResult {
  readonly statePersisted: boolean;
}

export interface LogPipelineGapInput {
  readonly lostBytes: number | null;
  readonly reason: LogGapReason;
  readonly runId: string;
  readonly unitId: string;
}

export interface LogPipelineLimits {
  readonly maxBufferedBytes: number;
  readonly maxRunBytes: number;
}

export interface LogPipelineLimitsSnapshot extends LogPipelineLimits {
  readonly activeRuns: number;
  readonly pendingBufferedBytes: number;
}

export interface LogPipelineLimitUpdateResult {
  readonly current: LogPipelineLimitsSnapshot;
  readonly droppedBufferedBytes: number;
  readonly droppedBufferedSegments: number;
  readonly previous: LogPipelineLimitsSnapshot;
  readonly sealedSegments: number;
}

/** Structural adapter for RuntimeProcessSupervisor's sink interface. */
export class RuntimeLogPipelineSink {
  constructor(private readonly pipeline: LogPipeline) {}

  append(entry: {
    readonly chunk: Buffer;
    readonly runId?: string;
    readonly stream: LogSegmentStream;
    readonly unitId: string;
  }): void {
    if (entry.runId === undefined || entry.runId.trim().length === 0) {
      throw new Error('LOG_RUN_ID_REQUIRED');
    }
    this.pipeline.append({
      chunk: entry.chunk,
      runId: entry.runId,
      stream: entry.stream,
      unitId: entry.unitId,
    });
  }

  async close(input: {
    readonly runId?: string;
    readonly unitId: string;
  }): Promise<LogPipelineCloseResult> {
    if (input.runId === undefined || input.runId.trim().length === 0) {
      throw new Error('LOG_RUN_ID_REQUIRED');
    }
    return await this.pipeline.close({ runId: input.runId, unitId: input.unitId });
  }

  recordGap(input: {
    readonly lostBytes: number | null;
    readonly reason: LogGapReason;
    readonly runId?: string;
    readonly unitId: string;
  }): void {
    if (input.runId === undefined || input.runId.trim().length === 0) {
      throw new Error('LOG_RUN_ID_REQUIRED');
    }
    this.pipeline.recordGap({
      lostBytes: input.lostBytes,
      reason: input.reason,
      runId: input.runId,
      unitId: input.unitId,
    });
  }

  async settleRun(runId: string): Promise<LogPipelineSettlementResult> {
    return await this.pipeline.settleRunWithStatus(runId);
  }

  async retryStorage(runId?: string): Promise<void> {
    await this.pipeline.retryStorage(runId);
  }

  finalizeRun(runId: string): void {
    this.pipeline.finalizeRun(runId);
  }
}

interface ActiveStream {
  readonly key: string;
  readonly redactor: StreamingLogRedactor;
  readonly runId: string;
  readonly stream: LogSegmentStream;
  readonly unitId: string;
  chunks: Buffer[];
  startedAt: Date;
  bytes: number;
  pendingLine: string;
  continuation: number;
}

interface InternalSegment {
  content: Buffer | null;
  readonly record: LogSegmentRecord;
  deleted: boolean;
  persisted: boolean;
}

interface PendingSegment {
  readonly content: Buffer;
  readonly internal: InternalSegment;
  attempted: boolean;
  recoveryState: LogPendingRecoveryState;
  status: WorkStatus;
}

interface PendingGap {
  attempted: boolean;
  gap: LogGapRecord;
  status: WorkStatus;
}

interface PendingCapacityCleanup {
  recoveryState: LogPendingRecoveryState;
  status: WorkStatus;
}

type WorkStatus = 'FAILED' | 'IN_FLIGHT' | 'QUEUED';

function assertFinitePositiveInteger(value: number, code: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(code);
  }
}

function takeUtf8Prefix(value: string, maxBytes: number): string {
  let bytes = 0;
  let end = 0;
  for (const character of value) {
    const size = Buffer.byteLength(character, 'utf8');
    if (bytes + size > maxBytes) break;
    bytes += size;
    end += character.length;
  }
  if (end === 0) throw new Error('LOG_RECORD_PAYLOAD_LIMIT_TOO_SMALL');
  return value.slice(0, end);
}

function streamKey(runId: string, unitId: string, stream: LogSegmentStream): string {
  return `${runId}\u0000${unitId}\u0000${stream}`;
}

/**
 * Durable log orchestration independent of the runtime process lifecycle.
 * The store owns PostgreSQL/object-storage writes; this class owns bounded
 * redacted buffering, segment sequence, logical-run capacity and gap flags.
 */
export class LogPipeline {
  private maxBufferedBytes: number;
  private maxRunBytes: number;
  private readonly maxSegmentBytes: number;
  private readonly now: () => Date;
  private readonly redact: (value: string) => string;
  private readonly retryDelayMilliseconds: number;
  private readonly store: LogSegmentStore;
  private readonly structuredRecords: boolean;
  private readonly maxRecordPayloadBytes: number;
  private readonly streams = new Map<string, ActiveStream>();
  private readonly closedUnits = new Set<string>();
  private readonly runSegments = new Map<string, InternalSegment[]>();
  private readonly degradedRuns = new Set<string>();
  private readonly dirtyRunStates = new Set<string>();
  private readonly finalizedRuns = new Set<string>();
  private readonly pending: PendingSegment[] = [];
  private readonly pendingGaps: PendingGap[] = [];
  private readonly pendingCapacityCleanup = new Map<string, PendingCapacityCleanup>();
  private readonly nextSequenceByRunStream = new Map<string, number>();
  private readonly runStateVersions = new Map<string, number>();
  private readonly runStateWrites = new Map<string, Promise<void>>();
  private readonly runWorkWaiters = new Map<string, Set<() => void>>();
  private pendingBytes = 0;
  private workPromise: Promise<void> | undefined;
  private retryScheduled = false;

  constructor(options: LogPipelineOptions) {
    this.maxBufferedBytes = options.maxBufferedBytes ?? DEFAULT_LOG_STORAGE_BUFFER_BYTES;
    this.maxRunBytes = options.maxRunBytes ?? DEFAULT_LOG_RUN_BYTES;
    this.maxSegmentBytes = options.maxSegmentBytes ?? DEFAULT_LOG_SEGMENT_BYTES;
    this.retryDelayMilliseconds = options.retryDelayMilliseconds ?? 1_000;
    assertFinitePositiveInteger(this.maxBufferedBytes, 'LOG_STORAGE_BUFFER_LIMIT_INVALID');
    assertFinitePositiveInteger(this.maxRunBytes, 'LOG_RUN_LIMIT_INVALID');
    assertFinitePositiveInteger(this.maxSegmentBytes, 'LOG_SEGMENT_LIMIT_INVALID');
    assertFinitePositiveInteger(this.retryDelayMilliseconds, 'LOG_RETRY_DELAY_INVALID');
    this.now = options.now ?? (() => new Date());
    this.redact = options.redact ?? redactLogText;
    this.store = options.store;
    this.structuredRecords = options.structuredRecords ?? true;
    this.maxRecordPayloadBytes = options.maxRecordPayloadBytes ?? DEFAULT_LOG_RECORD_PAYLOAD_BYTES;
    assertFinitePositiveInteger(this.maxRecordPayloadBytes, 'LOG_RECORD_PAYLOAD_LIMIT_INVALID');
    if (
      this.structuredRecords &&
      this.maxRecordPayloadBytes + LOG_RECORD_ENVELOPE_BYTES > this.maxSegmentBytes
    ) {
      throw new Error('LOG_RECORD_EXCEEDS_SEGMENT_LIMIT');
    }
  }

  append(entry: LogPipelineEntry): void {
    const raw = Buffer.isBuffer(entry.chunk) ? entry.chunk : Buffer.from(entry.chunk);
    if (raw.length === 0) {
      return;
    }
    const unitKey = `${entry.runId}\u0000${entry.unitId}`;
    if (this.closedUnits.has(unitKey)) {
      return;
    }
    const state = this.getStream(entry.runId, entry.unitId, entry.stream);
    try {
      const redacted = state.redactor.appendBytes(raw);
      this.appendRedacted(state, redacted);
    } catch {
      const lostBytes = raw.length + state.redactor.discard();
      this.appendRedacted(state, '[REDACTION_FAILED]');
      this.recordGap({
        lostBytes,
        reason: 'REDACTION_FAILURE',
        runId: state.runId,
        unitId: state.unitId,
      });
    }
  }

  async close(input: LogPipelineCloseInput): Promise<LogPipelineCloseResult> {
    const unitKey = `${input.runId}\u0000${input.unitId}`;
    if (this.closedUnits.has(unitKey)) {
      return await this.settleRun(input.runId);
    }
    this.closedUnits.add(unitKey);
    for (const state of [...this.streams.values()]) {
      if (state.runId !== input.runId || state.unitId !== input.unitId) {
        continue;
      }
      try {
        this.appendRedacted(state, state.redactor.flush());
      } catch {
        const lostBytes = state.redactor.discard();
        this.appendRedacted(state, '[REDACTION_FAILED]');
        this.recordGap({
          lostBytes,
          reason: 'REDACTION_FAILURE',
          runId: state.runId,
          unitId: state.unitId,
        });
      }
      this.flushLogicalLine(state);
      this.seal(state);
      this.streams.delete(state.key);
    }
    return await this.settleRun(input.runId);
  }

  recordGap(input: LogPipelineGapInput): void {
    const occurredAt = this.now();
    const isStreamError = input.reason === 'STREAM_ERROR';
    const identity = {
      id: randomUUID(),
      occurredAt,
      runId: input.runId,
      runUnitId: input.unitId,
    };
    const gap: LogGapRecord = isStreamError
      ? {
          ...identity,
          containerRestart: false,
          endedAt: null,
          lostBytes: null,
          reason: 'STREAM_ERROR',
          startedAt: null,
        }
      : {
          ...identity,
          containerRestart: false,
          endedAt: occurredAt,
          lostBytes:
            input.lostBytes === null ||
            (Number.isSafeInteger(input.lostBytes) && input.lostBytes >= 0)
              ? input.lostBytes
              : null,
          reason: input.reason,
          startedAt: occurredAt,
        };
    const mergeTarget = isStreamError
      ? undefined
      : [...this.pendingGaps]
          .reverse()
          .find(
            (pending) =>
              !pending.attempted &&
              pending.gap.runId === gap.runId &&
              pending.gap.runUnitId === gap.runUnitId &&
              pending.gap.reason === gap.reason,
          );
    if (mergeTarget === undefined) {
      this.pendingGaps.push({ attempted: false, gap, status: 'QUEUED' });
    } else {
      mergeTarget.gap = this.mergeGap(mergeTarget.gap, gap);
    }
    this.degradedRuns.add(input.runId);
    this.markRunDirty(input.runId);
    void this.scheduleWork();
  }

  async settleRun(runId: string): Promise<LogPipelineCloseResult> {
    const settled = await this.settleRunWithStatus(runId);
    return {
      recoveryState: settled.recoveryState,
      storageState: settled.storageState,
    };
  }

  async settleRunWithStatus(runId: string): Promise<LogPipelineSettlementResult> {
    await this.awaitRunWork(runId);
    this.markRunDirty(runId);
    await this.synchronizeRunState(runId);
    return {
      ...this.runLogState(runId),
      statePersisted: !this.dirtyRunStates.has(runId),
    };
  }

  finalizeRun(runId: string): void {
    this.finalizedRuns.add(runId);
    this.cleanupFinalizedRun(runId);
  }

  snapshotLimits(): LogPipelineLimitsSnapshot {
    return {
      activeRuns: new Set([
        ...this.runSegments.keys(),
        ...[...this.streams.values()].map((stream) => stream.runId),
        ...this.pending.map((pending) => pending.internal.record.runId),
      ]).size,
      maxBufferedBytes: this.maxBufferedBytes,
      maxRunBytes: this.maxRunBytes,
      pendingBufferedBytes: this.pendingBytes,
    };
  }

  snapshotRunState(runId: string): LogPipelineCloseResult {
    return this.runLogState(runId);
  }

  async flushRun(runId: string): Promise<void> {
    for (const state of this.streams.values()) {
      if (state.runId !== runId) continue;
      this.appendRedacted(state, state.redactor.flushCompleteLines());
      this.seal(state);
    }
    await this.scheduleWork();
  }

  async applyLimits(limits: LogPipelineLimits): Promise<LogPipelineLimitUpdateResult> {
    assertFinitePositiveInteger(limits.maxBufferedBytes, 'LOG_STORAGE_BUFFER_LIMIT_INVALID');
    assertFinitePositiveInteger(limits.maxRunBytes, 'LOG_RUN_LIMIT_INVALID');
    const previous = this.snapshotLimits();
    const loweringRunLimit = limits.maxRunBytes < this.maxRunBytes;
    this.maxBufferedBytes = limits.maxBufferedBytes;
    this.maxRunBytes = limits.maxRunBytes;

    let sealedSegments = 0;
    if (loweringRunLimit) {
      for (const state of this.streams.values()) {
        if (state.bytes === 0) continue;
        this.seal(state);
        sealedSegments += 1;
      }
    }
    const dropped = this.shrinkPendingBuffer();
    const runIds = new Set([
      ...this.runSegments.keys(),
      ...[...this.streams.values()].map((stream) => stream.runId),
    ]);
    for (const runId of runIds) {
      const existing = this.pendingCapacityCleanup.get(runId);
      if (existing?.status !== 'IN_FLIGHT') {
        this.pendingCapacityCleanup.set(runId, {
          recoveryState: existing?.recoveryState ?? 'MEMORY_PENDING',
          status: 'QUEUED',
        });
      }
    }
    await this.scheduleWork();
    return {
      current: this.snapshotLimits(),
      droppedBufferedBytes: dropped.bytes,
      droppedBufferedSegments: dropped.segments,
      previous,
      sealedSegments,
    };
  }

  async retryStorage(runId?: string): Promise<void> {
    for (const pending of this.pending) {
      if (
        pending.status === 'FAILED' &&
        (runId === undefined || pending.internal.record.runId === runId)
      ) {
        pending.attempted = true;
        pending.status = 'QUEUED';
      }
    }
    const retriedGapKeys = new Set<string>();
    for (const pending of this.pendingGaps) {
      const key = this.gapKey(pending.gap);
      if (
        pending.status === 'FAILED' &&
        (runId === undefined || pending.gap.runId === runId) &&
        !retriedGapKeys.has(key)
      ) {
        pending.status = 'QUEUED';
        retriedGapKeys.add(key);
      }
    }
    for (const [pendingRunId, pending] of this.pendingCapacityCleanup) {
      if (pending.status === 'FAILED' && (runId === undefined || pendingRunId === runId)) {
        pending.status = 'QUEUED';
      }
    }
    await this.scheduleWork();
  }

  private getStream(runId: string, unitId: string, stream: LogSegmentStream): ActiveStream {
    const key = streamKey(runId, unitId, stream);
    const existing = this.streams.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const state: ActiveStream = {
      bytes: 0,
      chunks: [],
      key,
      redactor: new StreamingLogRedactor(this.redact),
      continuation: 0,
      pendingLine: '',
      runId,
      startedAt: this.now(),
      stream,
      unitId,
    };
    this.streams.set(key, state);
    return state;
  }

  private appendRedacted(state: ActiveStream, value: string): void {
    if (value.length === 0) {
      return;
    }
    if (!this.structuredRecords) {
      this.appendPhysical(state, Buffer.from(value, 'utf8'));
      return;
    }
    state.pendingLine += value;
    while (state.pendingLine.length > 0) {
      const newline = state.pendingLine.indexOf('\n');
      if (newline >= 0) {
        const line = state.pendingLine.slice(0, newline).replace(/\r$/u, '');
        state.pendingLine = state.pendingLine.slice(newline + 1);
        this.writeLogicalLine(state, line);
        continue;
      }
      if (Buffer.byteLength(state.pendingLine, 'utf8') <= this.maxRecordPayloadBytes) return;
      const fragment = takeUtf8Prefix(state.pendingLine, this.maxRecordPayloadBytes);
      state.pendingLine = state.pendingLine.slice(fragment.length);
      this.writeRecord(state, fragment, true);
    }
  }

  private flushLogicalLine(state: ActiveStream): void {
    if (!this.structuredRecords || state.pendingLine.length === 0) return;
    const line = state.pendingLine;
    state.pendingLine = '';
    this.writeLogicalLine(state, line);
  }

  private writeLogicalLine(state: ActiveStream, line: string): void {
    if (line.length === 0) {
      this.writeRecord(state, '', false);
      state.continuation = 0;
      return;
    }
    let remaining = line;
    do {
      const fragment = takeUtf8Prefix(remaining, this.maxRecordPayloadBytes);
      remaining = remaining.slice(fragment.length);
      this.writeRecord(state, fragment, remaining.length > 0);
    } while (remaining.length > 0);
    state.continuation = 0;
  }

  private writeRecord(state: ActiveStream, payload: string, continues: boolean): void {
    const content = Buffer.from(
      `${JSON.stringify({
        continuation: state.continuation++,
        continues,
        occurredAt: this.now().toISOString(),
        payload,
        stream: state.stream,
        unitId: state.unitId,
      })}\n`,
      'utf8',
    );
    if (content.length > this.maxSegmentBytes) throw new Error('LOG_RECORD_EXCEEDS_SEGMENT_LIMIT');
    if (state.bytes > 0 && state.bytes + content.length > this.maxSegmentBytes) this.seal(state);
    this.appendPhysical(state, content);
  }

  private appendPhysical(state: ActiveStream, content: Buffer): void {
    let offset = 0;
    while (offset < content.length) {
      const available = this.maxSegmentBytes - state.bytes;
      if (available === 0) {
        this.seal(state);
        continue;
      }
      const end = Math.min(content.length, offset + available);
      const part = Buffer.from(content.subarray(offset, end));
      state.chunks.push(part);
      state.bytes += part.length;
      offset = end;
      if (state.bytes === this.maxSegmentBytes) {
        this.seal(state);
      }
    }
  }

  private seal(state: ActiveStream): void {
    if (state.bytes === 0) {
      return;
    }
    const content = Buffer.concat(state.chunks, state.bytes);
    const now = this.now();
    const record: LogSegmentRecord = {
      byteSize: content.length,
      createdAt: now,
      deletedAt: null,
      deletedReason: null,
      endedAt: now,
      id: randomUUID(),
      runId: state.runId,
      runUnitId: state.unitId,
      sequence: this.takeNextSequence(state.runId, state.stream),
      sha256: createHash('sha256').update(content).digest('hex'),
      startedAt: state.startedAt,
      state: 'SEALED',
      stream: state.stream,
    };
    const internal: InternalSegment = { content, deleted: false, persisted: false, record };
    const segments = this.runSegments.get(state.runId) ?? [];
    segments.push(internal);
    this.runSegments.set(state.runId, segments);
    state.startedAt = now;
    state.bytes = 0;
    state.chunks = [];
    this.enqueuePersist(internal);
  }

  private takeNextSequence(runId: string, stream: LogSegmentStream): number {
    const key = `${runId}\u0000${stream}`;
    const sequence = this.nextSequenceByRunStream.get(key) ?? 1;
    this.nextSequenceByRunStream.set(key, sequence + 1);
    return sequence;
  }

  private enqueuePersist(internal: InternalSegment): void {
    this.queuePending(internal, 'MEMORY_PENDING');
    void this.scheduleWork();
  }

  private queuePending(internal: InternalSegment, recoveryState: LogPendingRecoveryState): void {
    if (internal.deleted || this.pending.some((item) => item.internal === internal)) {
      return;
    }
    const content = internal.content;
    if (content === null) {
      return;
    }
    if (content.length > this.maxBufferedBytes) {
      this.dropSegment(internal, content.length);
      return;
    }
    while (this.pendingBytes + content.length > this.maxBufferedBytes) {
      const index = this.pending.findIndex((pending) => pending.status !== 'IN_FLIGHT');
      if (index < 0) {
        this.dropSegment(internal, content.length);
        return;
      }
      const dropped = this.pending.splice(index, 1)[0];
      if (dropped === undefined) {
        break;
      }
      this.pendingBytes -= dropped.content.length;
      this.dropSegment(dropped.internal, dropped.content.length);
      this.notifyRunWork(dropped.internal.record.runId);
    }
    const blockedByFailure = this.pending.some(
      (pending) =>
        pending.status === 'FAILED' && pending.internal.record.runId === internal.record.runId,
    );
    this.pending.push({
      attempted: false,
      content,
      internal,
      recoveryState,
      status: blockedByFailure ? 'FAILED' : 'QUEUED',
    });
    this.pendingBytes += content.length;
  }

  private dropSegment(internal: InternalSegment, lostBytes: number): void {
    internal.deleted = true;
    internal.content = null;
    this.removeRunSegment(internal);
    this.recordGap({
      lostBytes,
      reason: 'STORAGE_BUFFER_OVERFLOW',
      runId: internal.record.runId,
      unitId: internal.record.runUnitId,
    });
  }

  private shrinkPendingBuffer(): { readonly bytes: number; readonly segments: number } {
    let bytes = 0;
    let segments = 0;
    while (this.pendingBytes > this.maxBufferedBytes) {
      const index = this.pending.findIndex((pending) => pending.status !== 'IN_FLIGHT');
      if (index < 0) break;
      const dropped = this.pending.splice(index, 1)[0];
      if (dropped === undefined) break;
      this.pendingBytes -= dropped.content.length;
      bytes += dropped.content.length;
      segments += 1;
      this.dropSegment(dropped.internal, dropped.content.length);
      this.notifyRunWork(dropped.internal.record.runId);
    }
    return { bytes, segments };
  }

  private removeRunSegment(internal: InternalSegment): void {
    const segments = this.runSegments.get(internal.record.runId);
    if (segments === undefined) {
      return;
    }
    const index = segments.indexOf(internal);
    if (index >= 0) {
      segments.splice(index, 1);
    }
  }

  private recoveryStateFor(error: unknown): LogPendingRecoveryState {
    return error instanceof LogSegmentPersistenceError ? error.recoveryState : 'MEMORY_PENDING';
  }

  private runLogState(runId: string): LogPipelineCloseResult {
    let hasDurablePending = false;
    let hasMemoryPending = false;
    for (const pending of this.pending) {
      if (pending.internal.record.runId !== runId) {
        continue;
      }
      if (pending.recoveryState === 'DURABLE_PENDING') {
        hasDurablePending = true;
      } else {
        hasMemoryPending = true;
      }
    }
    const capacityPending = this.pendingCapacityCleanup.get(runId);
    if (capacityPending?.recoveryState === 'DURABLE_PENDING') {
      hasDurablePending = true;
    } else if (capacityPending !== undefined) {
      hasMemoryPending = true;
    }
    if (this.pendingGaps.some((pending) => pending.gap.runId === runId)) {
      hasMemoryPending = true;
    }
    const recoveryState: LogRecoveryState =
      hasMemoryPending && hasDurablePending
        ? 'MIXED_PENDING'
        : hasMemoryPending
          ? 'MEMORY_PENDING'
          : hasDurablePending
            ? 'DURABLE_PENDING'
            : 'NONE';
    return {
      recoveryState,
      storageState: this.degradedRuns.has(runId) ? 'DEGRADED' : 'HEALTHY',
    };
  }

  private markRunDirty(runId: string): void {
    this.dirtyRunStates.add(runId);
    this.runStateVersions.set(runId, (this.runStateVersions.get(runId) ?? 0) + 1);
  }

  private async synchronizeRunState(runId: string): Promise<void> {
    while (this.dirtyRunStates.has(runId)) {
      const existing = this.runStateWrites.get(runId);
      if (existing !== undefined) {
        await existing;
        continue;
      }
      const version = this.runStateVersions.get(runId) ?? 0;
      const state = this.runLogState(runId);
      const write = (async () => {
        try {
          await this.store.updateRunLogState({ runId, ...state });
          if ((this.runStateVersions.get(runId) ?? 0) === version) {
            this.dirtyRunStates.delete(runId);
          }
        } catch {
          this.scheduleRetry();
        }
      })();
      this.runStateWrites.set(runId, write);
      try {
        await write;
      } finally {
        if (this.runStateWrites.get(runId) === write) {
          this.runStateWrites.delete(runId);
        }
        this.notifyRunWork(runId);
        this.cleanupFinalizedRun(runId);
      }
      if (this.dirtyRunStates.has(runId) && (this.runStateVersions.get(runId) ?? 0) === version) {
        return;
      }
    }
  }

  private async enforceRunCapacity(runId: string): Promise<void> {
    if (this.store.enforceRunCapacity !== undefined) {
      await this.store.enforceRunCapacity({ maxBytes: this.maxRunBytes, runId });
      return;
    }
    const segments = this.runSegments.get(runId) ?? [];
    const retained = () =>
      segments
        .filter((segment) => !segment.deleted)
        .reduce((total, segment) => total + segment.record.byteSize, 0);
    while (retained() > this.maxRunBytes) {
      const candidate = segments.find((segment) => !segment.deleted && segment.persisted);
      if (candidate === undefined) {
        return;
      }
      const deletedAt = this.now();
      const deletedRecord: LogSegmentRecord = {
        ...candidate.record,
        deletedAt,
        deletedReason: 'CAPACITY_ROLLOVER',
        endedAt: candidate.record.endedAt ?? deletedAt,
        state: 'DELETED',
      };
      await this.store.deleteSegment({ reason: 'CAPACITY_ROLLOVER', segment: deletedRecord });
      candidate.deleted = true;
      candidate.content = null;
      this.removeRunSegment(candidate);
    }
  }

  private scheduleWork(): Promise<void> {
    if (this.workPromise !== undefined) {
      return this.workPromise;
    }
    const work = this.drainWork();
    const tracked = work.finally(() => {
      if (this.workPromise === tracked) {
        this.workPromise = undefined;
      }
      for (const runId of this.runWorkWaiters.keys()) {
        this.notifyRunWork(runId);
      }
      for (const runId of this.finalizedRuns) {
        this.cleanupFinalizedRun(runId);
      }
      if (this.hasRunnableWork()) {
        void this.scheduleWork();
      }
    });
    this.workPromise = tracked;
    return tracked;
  }

  private async drainWork(): Promise<void> {
    await this.drainSegments();
    await this.drainCapacityCleanup();
    await this.flushPendingGaps();
    for (const runId of [...this.dirtyRunStates]) {
      await this.synchronizeRunState(runId);
    }
  }

  private async drainSegments(): Promise<void> {
    while (true) {
      const pending = this.nextRunnableSegment();
      if (pending === undefined) {
        return;
      }
      const wasRetry = pending.attempted;
      pending.status = 'IN_FLIGHT';
      pending.attempted = true;
      try {
        await this.store.persistSegment({
          content: pending.content,
          segment: pending.internal.record,
        });
      } catch (error) {
        pending.status = 'FAILED';
        pending.recoveryState = this.recoveryStateFor(error);
        this.degradedRuns.add(pending.internal.record.runId);
        this.markRunDirty(pending.internal.record.runId);
        const failedIndex = this.pending.indexOf(pending);
        for (const blocked of this.pending.slice(failedIndex + 1)) {
          if (
            blocked.status === 'QUEUED' &&
            blocked.internal.record.runId === pending.internal.record.runId
          ) {
            blocked.status = 'FAILED';
            this.notifyRunWork(blocked.internal.record.runId);
          }
        }
        this.notifyRunWork(pending.internal.record.runId);
        this.shrinkPendingBuffer();
        this.scheduleRetry();
        return;
      }
      const persistedIndex = this.pending.indexOf(pending);
      if (persistedIndex >= 0) {
        this.pending.splice(persistedIndex, 1);
      }
      this.pendingBytes -= pending.content.length;
      pending.internal.persisted = true;
      pending.internal.content = null;
      // A durable store owns retention metadata as well as segment deletion.
      // Keeping every persisted segment here would grow for the entire run.
      if (this.store.enforceRunCapacity !== undefined) this.removeRunSegment(pending.internal);
      this.notifyRunWork(pending.internal.record.runId);
      if (wasRetry) {
        this.markRunDirty(pending.internal.record.runId);
      }
      const existingCapacity = this.pendingCapacityCleanup.get(pending.internal.record.runId);
      if (existingCapacity?.status !== 'FAILED') {
        await this.attemptRunCapacity(pending.internal.record.runId);
      }
    }
  }

  private nextRunnableSegment(): PendingSegment | undefined {
    const blockedRuns = new Set<string>();
    for (const pending of this.pending) {
      const runId = pending.internal.record.runId;
      if (blockedRuns.has(runId)) {
        continue;
      }
      if (pending.status === 'QUEUED') {
        return pending;
      }
      blockedRuns.add(runId);
    }
    return undefined;
  }

  private async drainCapacityCleanup(): Promise<void> {
    for (const [runId, pending] of this.pendingCapacityCleanup) {
      if (pending.status === 'QUEUED') {
        await this.attemptRunCapacity(runId);
      }
    }
  }

  private async attemptRunCapacity(runId: string): Promise<void> {
    const existing = this.pendingCapacityCleanup.get(runId);
    if (existing?.status === 'FAILED' || existing?.status === 'IN_FLIGHT') {
      return;
    }
    const wasPending = existing !== undefined;
    this.pendingCapacityCleanup.set(runId, {
      recoveryState: existing?.recoveryState ?? 'MEMORY_PENDING',
      status: 'IN_FLIGHT',
    });
    try {
      await this.enforceRunCapacity(runId);
    } catch (error) {
      this.pendingCapacityCleanup.set(runId, {
        recoveryState: this.recoveryStateFor(error),
        status: 'FAILED',
      });
      this.degradedRuns.add(runId);
      this.markRunDirty(runId);
      this.notifyRunWork(runId);
      this.scheduleRetry();
      return;
    }
    this.pendingCapacityCleanup.delete(runId);
    if (wasPending) {
      this.markRunDirty(runId);
    }
    this.notifyRunWork(runId);
  }

  private async flushPendingGaps(): Promise<void> {
    while (true) {
      const pending = this.nextRunnableGap();
      if (pending === undefined) {
        return;
      }
      pending.status = 'IN_FLIGHT';
      pending.attempted = true;
      try {
        await this.store.recordGap(pending.gap);
      } catch {
        pending.status = 'FAILED';
        this.degradedRuns.add(pending.gap.runId);
        this.markRunDirty(pending.gap.runId);
        this.notifyRunWork(pending.gap.runId);
        this.scheduleRetry();
        continue;
      }
      const index = this.pendingGaps.indexOf(pending);
      if (index >= 0) {
        this.pendingGaps.splice(index, 1);
      }
      this.markRunDirty(pending.gap.runId);
      this.notifyRunWork(pending.gap.runId);
    }
  }

  private nextRunnableGap(): PendingGap | undefined {
    const blockedKeys = new Set<string>();
    for (const pending of this.pendingGaps) {
      const key = this.gapKey(pending.gap);
      if (blockedKeys.has(key)) {
        continue;
      }
      if (pending.status === 'QUEUED') {
        return pending;
      }
      blockedKeys.add(key);
    }
    return undefined;
  }

  private gapKey(gap: LogGapRecord): string {
    return `${gap.runId}\u0000${gap.runUnitId}\u0000${gap.reason}`;
  }

  private mergeGap(first: LogGapRecord, second: LogGapRecord): LogGapRecord {
    if (first.reason === 'STREAM_ERROR') {
      return first;
    }
    const lostBytes =
      first.lostBytes === null || second.lostBytes === null
        ? null
        : Number.isSafeInteger(first.lostBytes + second.lostBytes)
          ? first.lostBytes + second.lostBytes
          : null;
    return {
      ...first,
      endedAt:
        first.endedAt === null || second.endedAt === null
          ? null
          : new Date(Math.max(first.endedAt.getTime(), second.endedAt.getTime())),
      lostBytes,
      occurredAt: new Date(Math.min(first.occurredAt.getTime(), second.occurredAt.getTime())),
      startedAt:
        first.startedAt === null || second.startedAt === null
          ? null
          : new Date(Math.min(first.startedAt.getTime(), second.startedAt.getTime())),
    };
  }

  private hasRunnableWork(): boolean {
    return (
      this.nextRunnableSegment() !== undefined ||
      this.nextRunnableGap() !== undefined ||
      [...this.pendingCapacityCleanup.values()].some((pending) => pending.status === 'QUEUED')
    );
  }

  private hasUnsettledRunWork(runId: string): boolean {
    if (
      this.pending.some(
        (pending) =>
          pending.internal.record.runId === runId &&
          (pending.status === 'QUEUED' || pending.status === 'IN_FLIGHT'),
      )
    ) {
      return true;
    }
    const capacity = this.pendingCapacityCleanup.get(runId);
    if (capacity?.status === 'QUEUED' || capacity?.status === 'IN_FLIGHT') {
      return true;
    }
    const blockedKeys = new Set<string>();
    for (const pending of this.pendingGaps) {
      const key = this.gapKey(pending.gap);
      if (pending.gap.runId !== runId) {
        if (pending.status !== 'QUEUED') {
          blockedKeys.add(key);
        }
        continue;
      }
      if (pending.status === 'IN_FLIGHT') {
        return true;
      }
      if (pending.status === 'QUEUED' && !blockedKeys.has(key)) {
        return true;
      }
      if (pending.status !== 'QUEUED') {
        blockedKeys.add(key);
      }
    }
    return false;
  }

  private async awaitRunWork(runId: string): Promise<void> {
    while (this.hasUnsettledRunWork(runId)) {
      await new Promise<void>((resolve) => {
        const waiters = this.runWorkWaiters.get(runId) ?? new Set<() => void>();
        const done = (): void => {
          waiters.delete(done);
          resolve();
        };
        waiters.add(done);
        this.runWorkWaiters.set(runId, waiters);
        if (!this.hasUnsettledRunWork(runId)) {
          done();
        } else {
          void this.scheduleWork();
        }
      });
    }
  }

  private notifyRunWork(runId: string): void {
    const waiters = this.runWorkWaiters.get(runId);
    if (waiters === undefined) {
      return;
    }
    this.runWorkWaiters.delete(runId);
    for (const resolve of waiters) {
      resolve();
    }
  }

  private cleanupFinalizedRun(runId: string): void {
    if (
      !this.finalizedRuns.has(runId) ||
      this.dirtyRunStates.has(runId) ||
      this.runStateWrites.has(runId) ||
      [...this.streams.values()].some((stream) => stream.runId === runId) ||
      this.pending.some((pending) => pending.internal.record.runId === runId) ||
      this.pendingGaps.some((pending) => pending.gap.runId === runId) ||
      this.pendingCapacityCleanup.has(runId) ||
      (this.runSegments.get(runId) ?? []).some(
        (segment) => !segment.deleted && !segment.persisted && segment.content !== null,
      )
    ) {
      return;
    }
    this.runSegments.delete(runId);
    this.degradedRuns.delete(runId);
    this.finalizedRuns.delete(runId);
    this.runStateVersions.delete(runId);
    this.runWorkWaiters.delete(runId);
    const prefix = `${runId}\u0000`;
    for (const key of this.nextSequenceByRunStream.keys()) {
      if (key.startsWith(prefix)) {
        this.nextSequenceByRunStream.delete(key);
      }
    }
    for (const unitKey of this.closedUnits) {
      if (unitKey.startsWith(prefix)) {
        this.closedUnits.delete(unitKey);
      }
    }
  }

  private scheduleRetry(): void {
    if (this.retryScheduled) {
      return;
    }
    this.retryScheduled = true;
    const timer = setTimeout(() => {
      this.retryScheduled = false;
      void this.retryStorage();
    }, this.retryDelayMilliseconds);
    timer.unref();
  }
}
