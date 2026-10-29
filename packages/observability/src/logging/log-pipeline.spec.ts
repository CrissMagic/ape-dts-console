import { describe, expect, it, vi } from 'vitest';

import {
  LogSegmentPersistenceError,
  type LogGapRecord,
  type LogPendingRecoveryState,
  type LogRecoveryState,
  type LogSegmentRecord,
  type LogSegmentStore,
  type LogStorageState,
} from '@scs/storage';

import { LogPipeline, redactLogText, StreamingLogRedactor } from './log-pipeline.js';

class MemoryStore implements LogSegmentStore {
  readonly deleted: Array<{ reason: string; segment: LogSegmentRecord }> = [];
  readonly gaps: LogGapRecord[] = [];
  readonly flags: Array<{ hasGap?: boolean; runId: string; truncated?: boolean }> = [];
  readonly persisted: Array<{ content: Buffer; segment: LogSegmentRecord }> = [];
  readonly states: Array<{
    recoveryState: LogRecoveryState;
    runId: string;
    storageState: LogStorageState;
  }> = [];
  failRecordGap = false;
  failDelete: false | LogPendingRecoveryState = false;
  failPersist: false | LogPendingRecoveryState = false;
  readonly failPersistRuns = new Map<string, LogPendingRecoveryState>();
  failUpdateRunState = false;
  private persistGate: Promise<void> | undefined;
  private releasePersistGate: (() => void) | undefined;
  private deleteGate: Promise<void> | undefined;
  private releaseDeleteGate: (() => void) | undefined;

  holdDelete(): void {
    this.deleteGate = new Promise<void>((resolve) => {
      this.releaseDeleteGate = resolve;
    });
  }

  releaseDelete(): void {
    this.releaseDeleteGate?.();
    this.releaseDeleteGate = undefined;
    this.deleteGate = undefined;
  }

  holdPersist(): void {
    this.persistGate = new Promise<void>((resolve) => {
      this.releasePersistGate = resolve;
    });
  }

  releasePersist(): void {
    this.releasePersistGate?.();
    this.releasePersistGate = undefined;
    this.persistGate = undefined;
  }

  async deleteSegment(input: {
    reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
    segment: LogSegmentRecord;
  }) {
    if (this.deleteGate !== undefined) await this.deleteGate;
    if (this.failDelete) {
      throw new LogSegmentPersistenceError(this.failDelete, 'delete unavailable');
    }
    this.deleted.push(input);
    if (input.reason === 'CAPACITY_ROLLOVER') {
      this.flags.push({ hasGap: true, runId: input.segment.runId, truncated: true });
    }
  }

  async persistSegment(input: { content: Buffer; segment: LogSegmentRecord }) {
    if (this.persistGate !== undefined) {
      await this.persistGate;
    }
    const runFailure = this.failPersistRuns.get(input.segment.runId);
    if (this.failPersist || runFailure !== undefined) {
      throw new LogSegmentPersistenceError(
        this.failPersist || runFailure || 'MEMORY_PENDING',
        'storage unavailable',
      );
    }
    this.persisted.push(input);
  }

  async recordGap(gap: LogGapRecord) {
    if (this.failRecordGap) {
      throw new Error('gap metadata unavailable');
    }
    this.gaps.push(gap);
    this.flags.push({ hasGap: true, runId: gap.runId });
  }

  async updateRunLogState(input: {
    recoveryState: LogRecoveryState;
    runId: string;
    storageState: LogStorageState;
  }) {
    if (this.failUpdateRunState) {
      throw new Error('run state unavailable');
    }
    this.states.push(input);
  }
}

describe('LogPipeline', () => {
  it('retains multibyte log text and redaction across arbitrary byte chunks and long suffix boundaries', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ store });
    const text = `中文日志${'😀'.repeat(700)} password=secret-value\n同步完成\n`;
    for (const byte of Buffer.from(text))
      pipeline.append({
        chunk: Buffer.from([byte]),
        runId: 'utf8',
        stream: 'STDOUT',
        unitId: 'primary',
      });
    await pipeline.close({ runId: 'utf8', unitId: 'primary' });
    const output = Buffer.concat(store.persisted.map((item) => item.content)).toString('utf8');
    expect(output).toContain('中文日志');
    expect(output).toContain('😀'.repeat(700));
    expect(output).toContain('同步完成');
    expect(output).toContain('[REDACTED]');
    expect(output).not.toContain('secret-value');
    expect(output).not.toContain('\uFFFD');
  });
  it('releases persisted segment metadata when retention is delegated to the durable store', async () => {
    const store = Object.assign(new MemoryStore(), {
      enforceRunCapacity: vi.fn(async ({ maxBytes }: { maxBytes: number; runId: string }) => {
        while (store.persisted.reduce((sum, item) => sum + item.segment.byteSize, 0) > maxBytes)
          store.persisted.shift();
      }),
    });
    const pipeline = new LogPipeline({
      maxSegmentBytes: 3,
      maxRunBytes: 6,
      store,
      structuredRecords: false,
    });
    for (let index = 0; index < 200; index++) {
      pipeline.append({ chunk: 'ab\n', runId: 'live', stream: 'STDOUT', unitId: 'primary' });
      await pipeline.flushRun('live');
    }
    expect(store.persisted).toHaveLength(2);
    expect(store.persisted.at(-1)?.segment.sequence).toBe(200);
    const internals = pipeline as unknown as { runSegments: Map<string, unknown[]> };
    expect(internals.runSegments.get('live') ?? []).toHaveLength(0);
    expect(pipeline.snapshotRunState('live')).toMatchObject({
      recoveryState: 'NONE',
      storageState: 'HEALTHY',
    });
    await pipeline.close({ runId: 'live', unitId: 'primary' });
    pipeline.finalizeRun('live');
    expect(internals.runSegments.has('live')).toBe(false);
  });

  it('retains delegated capacity recovery after releasing the persisted in-memory segment', async () => {
    const store = Object.assign(new MemoryStore(), {
      enforceRunCapacity: vi.fn(async (): Promise<void> => {
        throw new LogSegmentPersistenceError('DURABLE_PENDING', 'capacity storage unavailable');
      }),
    });
    const pipeline = new LogPipeline({
      store,
      structuredRecords: false,
      maxSegmentBytes: 3,
      retryDelayMilliseconds: 60_000,
    });
    pipeline.append({ chunk: 'ab\n', runId: 'retry', stream: 'STDOUT', unitId: 'primary' });
    await pipeline.flushRun('retry');
    expect(store.persisted).toHaveLength(1);
    expect(pipeline.snapshotRunState('retry')).toMatchObject({
      recoveryState: 'DURABLE_PENDING',
      storageState: 'DEGRADED',
    });
    expect(pipeline.snapshotLimits().pendingBufferedBytes).toBe(0);
    store.enforceRunCapacity.mockResolvedValue(undefined);
    await pipeline.retryStorage('retry');
    expect(pipeline.snapshotRunState('retry')).toMatchObject({
      recoveryState: 'NONE',
      storageState: 'DEGRADED',
    });
    expect(store.persisted).toHaveLength(1);
    await pipeline.close({ runId: 'retry', unitId: 'primary' });
    pipeline.finalizeRun('retry');
  });
  it.each(['super-secret', 'access-token', 'password'])(
    'keeps cross-line credential context at every live flush boundary for %s',
    (secret) => {
      const input = `INFO ready\npassword\n=\n${secret}\nINFO done\n`;
      for (let split = 0; split <= input.length; split += 1) {
        const redactor = new StreamingLogRedactor();
        let published = redactor.append(input.slice(0, split));
        published += redactor.flushCompleteLines();
        published += redactor.append(input.slice(split));
        published += redactor.flushCompleteLines();
        published += redactor.flush();
        expect(published).not.toContain(`=\n${secret}`);
        expect(published).toContain('[REDACTED]');
        expect(published).toContain('INFO done');
      }
    },
  );

  it('publishes completed lines for a live run without closing its stream or flushing another run', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ store });
    const input = { runId: 'live', unitId: 'primary', stream: 'STDOUT' as const };
    pipeline.append({ ...input, chunk: 'INFO first\npassword="hidden\n' });
    pipeline.append({ ...input, runId: 'other', chunk: 'INFO other run\n' });
    await pipeline.flushRun('live');
    expect(store.persisted).toHaveLength(1);
    expect(store.persisted[0]!.content.toString()).toContain('INFO first');
    expect(store.persisted[0]!.content.toString()).not.toContain('hidden');
    pipeline.append({ ...input, chunk: 'secret"\nINFO second\nunfinished' });
    await pipeline.flushRun('live');
    const output = store.persisted.map((item) => item.content.toString()).join('');
    expect(output).toContain('INFO second');
    expect(output).toContain('[REDACTED]');
    expect(output).not.toContain('hidden');
    expect(output).not.toContain('secret');
    expect(output).not.toContain('unfinished');
    expect(output).not.toContain('other run');
    await pipeline.close(input);
    expect(store.persisted.map((item) => item.content.toString()).join('')).toContain('unfinished');
    await pipeline.close({ ...input, runId: 'other' });
  });

  it('retains split credential keys and values while publishing complete live lines', () => {
    const redactor = new StreamingLogRedactor();
    expect(redactor.append('INFO ready\npassword\n')).toBe('');
    expect(redactor.flushCompleteLines()).toBe('INFO ready\n');
    expect(redactor.append('=\n')).toBe('');
    expect(redactor.flushCompleteLines()).toBe('');
    expect(redactor.append('top-secret\nINFO next\npart')).toBe('');
    expect(redactor.flushCompleteLines()).toBe('password\n=\n[REDACTED]\nINFO next\n');
    expect(redactor.flush()).toBe('part');
  });

  it('writes bounded JSONL records with source, occurrence time, and lossless continuation', async () => {
    const store = new MemoryStore();
    const occurredAt = new Date('2026-09-08T00:00:00.000Z');
    const pipeline = new LogPipeline({
      maxRecordPayloadBytes: 4,
      maxSegmentBytes: 4096,
      now: () => occurredAt,
      store,
      structuredRecords: true,
    });
    pipeline.append({
      chunk: 'abcdef\nnext',
      runId: 'run-jsonl',
      stream: 'STDERR',
      unitId: 'unit-7',
    });
    await pipeline.close({ runId: 'run-jsonl', unitId: 'unit-7' });

    const records = store.persisted
      .map((entry) => entry.content.toString('utf8'))
      .join('')
      .trimEnd()
      .split('\n')
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(records.map((record) => record.payload).join('')).toBe('abcdefnext');
    expect(records).toEqual([
      expect.objectContaining({
        continuation: 0,
        continues: true,
        occurredAt: occurredAt.toISOString(),
        payload: 'abcd',
        stream: 'STDERR',
        unitId: 'unit-7',
      }),
      expect.objectContaining({ continuation: 1, continues: false, payload: 'ef' }),
      expect.objectContaining({ continuation: 0, continues: false, payload: 'next' }),
    ]);
  });

  it('rejects a configured record that cannot fit in one physical segment', () => {
    expect(
      () =>
        new LogPipeline({
          maxRecordPayloadBytes: 64,
          maxSegmentBytes: 128,
          store: new MemoryStore(),
          structuredRecords: true,
        }),
    ).toThrow('LOG_RECORD_EXCEEDS_SEGMENT_LIMIT');
  });
  it('redacts split credentials, seals ordered segments, and records hashes', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ maxSegmentBytes: 8, store, structuredRecords: false });

    pipeline.append({
      chunk: 'password=split-',
      runId: 'run-1',
      stream: 'STDOUT',
      unitId: 'unit-1',
    });
    pipeline.append({ chunk: 'secret\n', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    const persisted = store.persisted.map((entry) => entry.content.toString('utf8')).join('');
    expect(persisted).not.toContain('split-secret');
    expect(persisted).toContain('[REDACTED]');
    expect(store.persisted.map((entry) => entry.segment.sequence)).toEqual([1, 2, 3]);
    expect(store.persisted.every((entry) => entry.segment.state === 'SEALED')).toBe(true);
    expect(store.persisted.every((entry) => entry.segment.byteSize <= 8)).toBe(true);
    expect(store.persisted.every((entry) => entry.segment.sha256 !== null)).toBe(true);
  });

  it('allocates one segment sequence per run stream across runtime units', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ maxSegmentBytes: 4, store, structuredRecords: false });

    pipeline.append({ chunk: 'aaaa', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    pipeline.append({ chunk: 'bbbb', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-2' });
    pipeline.append({ chunk: 'cccc', runId: 'run-1', stream: 'STDERR', unitId: 'unit-2' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-2' });

    expect(
      store.persisted.map(({ segment }) => ({
        sequence: segment.sequence,
        stream: segment.stream,
        unitId: segment.runUnitId,
      })),
    ).toEqual([
      { sequence: 1, stream: 'STDOUT', unitId: 'unit-1' },
      { sequence: 2, stream: 'STDOUT', unitId: 'unit-2' },
      { sequence: 1, stream: 'STDERR', unitId: 'unit-2' },
    ]);
  });

  it('allocates sequence when a segment seals instead of when its unit opens', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ maxSegmentBytes: 4, store, structuredRecords: false });

    pipeline.append({ chunk: 'aa', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    pipeline.append({ chunk: 'bbbb', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-2' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-2' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    expect(
      store.persisted.map(({ segment }) => ({
        sequence: segment.sequence,
        unitId: segment.runUnitId,
      })),
    ).toEqual([
      { sequence: 1, unitId: 'unit-2' },
      { sequence: 2, unitId: 'unit-1' },
    ]);
  });

  it('rolls the oldest sealed segments at logical-run capacity without deleting the current open segment', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({
      maxRunBytes: 6,
      maxSegmentBytes: 3,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: 'abcdefghi', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    expect(store.deleted.length).toBe(1);
    expect(store.deleted[0]?.reason).toBe('CAPACITY_ROLLOVER');
    expect(store.deleted[0]?.segment.sequence).toBe(1);
    expect(store.flags).toContainEqual({ hasGap: true, runId: 'run-1', truncated: true });
  });

  it('does not settle a run while its first capacity cleanup is still in flight', async () => {
    const store = new MemoryStore();
    store.holdDelete();
    const pipeline = new LogPipeline({
      maxRunBytes: 6,
      maxSegmentBytes: 3,
      store,
      structuredRecords: false,
    });
    pipeline.append({ chunk: 'abcdefghi', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    const closing = pipeline.close({ runId: 'run-1', unitId: 'unit-1' });
    await vi.waitFor(() => expect(store.persisted).toHaveLength(1));
    let settled = false;
    const settlement = pipeline.settleRun('run-1').then((result) => {
      settled = true;
      return result;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    store.releaseDelete();
    await closing;
    await expect(settlement).resolves.toMatchObject({ recoveryState: 'NONE' });
  });

  it('uses one storage-outage buffer across units and drops the oldest redacted segment', async () => {
    const store = new MemoryStore();
    store.failPersist = 'MEMORY_PENDING';
    const pipeline = new LogPipeline({
      maxBufferedBytes: 4,
      maxSegmentBytes: 4,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await expect(pipeline.close({ runId: 'run-1', unitId: 'unit-1' })).resolves.toEqual({
      recoveryState: 'MEMORY_PENDING',
      storageState: 'DEGRADED',
    });
    pipeline.append({ chunk: 'abcd', runId: 'run-2', stream: 'STDERR', unitId: 'unit-2' });
    await pipeline.close({ runId: 'run-2', unitId: 'unit-2' });

    expect(store.states.at(-1)).toEqual({
      recoveryState: 'MEMORY_PENDING',
      runId: 'run-2',
      storageState: 'DEGRADED',
    });

    expect(store.gaps).toContainEqual(
      expect.objectContaining({
        reason: 'STORAGE_BUFFER_OVERFLOW',
        runId: 'run-1',
        runUnitId: 'unit-1',
        lostBytes: 4,
      }),
    );
    store.failPersist = false;
    await pipeline.retryStorage();
    expect(store.persisted.map((entry) => entry.content.toString('utf8'))).toEqual(['abcd']);
    expect(store.states.at(-1)).toEqual({
      recoveryState: 'NONE',
      runId: 'run-2',
      storageState: 'DEGRADED',
    });
  });

  it('distinguishes durable recovery from memory-only pending content', async () => {
    const store = new MemoryStore();
    store.failPersist = 'DURABLE_PENDING';
    const pipeline = new LogPipeline({ maxSegmentBytes: 4, store, structuredRecords: false });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });

    await expect(pipeline.close({ runId: 'run-1', unitId: 'unit-1' })).resolves.toEqual({
      recoveryState: 'DURABLE_PENDING',
      storageState: 'DEGRADED',
    });
    expect(store.states).toContainEqual({
      recoveryState: 'DURABLE_PENDING',
      runId: 'run-1',
      storageState: 'DEGRADED',
    });
  });

  it('does not let a failed run block persistence for an independent run', async () => {
    const store = new MemoryStore();
    store.failPersistRuns.set('run-a', 'MEMORY_PENDING');
    const pipeline = new LogPipeline({
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: 'aaaa', runId: 'run-a', stream: 'STDOUT', unitId: 'unit-a' });
    await pipeline.close({ runId: 'run-a', unitId: 'unit-a' });
    pipeline.append({ chunk: 'bbbb', runId: 'run-b', stream: 'STDOUT', unitId: 'unit-b' });

    await expect(pipeline.close({ runId: 'run-b', unitId: 'unit-b' })).resolves.toEqual({
      recoveryState: 'NONE',
      storageState: 'HEALTHY',
    });
    expect(store.persisted.map(({ segment }) => segment.runId)).toEqual(['run-b']);
  });

  it('retries one run without spinning behind another failed run', async () => {
    const store = new MemoryStore();
    store.failPersistRuns.set('run-a', 'MEMORY_PENDING');
    store.failPersistRuns.set('run-b', 'MEMORY_PENDING');
    const pipeline = new LogPipeline({
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: 'aaaa', runId: 'run-a', stream: 'STDOUT', unitId: 'unit-a' });
    await pipeline.close({ runId: 'run-a', unitId: 'unit-a' });
    pipeline.append({ chunk: 'bbbb', runId: 'run-b', stream: 'STDOUT', unitId: 'unit-b' });
    await pipeline.close({ runId: 'run-b', unitId: 'unit-b' });

    store.failPersistRuns.delete('run-b');
    await pipeline.retryStorage('run-b');

    expect(store.persisted.map(({ segment }) => segment.runId)).toEqual(['run-b']);
    await expect(pipeline.settleRun('run-a')).resolves.toMatchObject({
      recoveryState: 'MEMORY_PENDING',
    });
    await expect(pipeline.settleRun('run-b')).resolves.toMatchObject({ recoveryState: 'NONE' });
  });

  it('reports whether the authoritative run state reached durable storage', async () => {
    const store = new MemoryStore();
    store.failPersist = 'MEMORY_PENDING';
    store.failUpdateRunState = true;
    const pipeline = new LogPipeline({
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    await expect(pipeline.settleRunWithStatus('run-1')).resolves.toEqual({
      recoveryState: 'MEMORY_PENDING',
      statePersisted: false,
      storageState: 'DEGRADED',
    });

    store.failPersist = false;
    store.failUpdateRunState = false;
    await pipeline.retryStorage();

    await expect(pipeline.settleRunWithStatus('run-1')).resolves.toEqual({
      recoveryState: 'NONE',
      statePersisted: true,
      storageState: 'DEGRADED',
    });
  });

  it('keeps a failed gap write memory-pending and retries the same durable fact', async () => {
    const store = new MemoryStore();
    store.failPersist = 'MEMORY_PENDING';
    const pipeline = new LogPipeline({
      maxBufferedBytes: 4,
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });
    store.failRecordGap = true;
    pipeline.append({ chunk: 'abcd', runId: 'run-2', stream: 'STDOUT', unitId: 'unit-2' });
    await pipeline.close({ runId: 'run-2', unitId: 'unit-2' });

    await expect(pipeline.close({ runId: 'run-1', unitId: 'unit-1' })).resolves.toEqual({
      recoveryState: 'MEMORY_PENDING',
      storageState: 'DEGRADED',
    });
    expect(store.gaps).toEqual([]);

    store.failPersist = false;
    store.failRecordGap = false;
    await pipeline.retryStorage();
    await pipeline.retryStorage();

    expect(store.gaps).toHaveLength(1);
    expect(store.gaps[0]).toMatchObject({
      lostBytes: 4,
      reason: 'STORAGE_BUFFER_OVERFLOW',
      runId: 'run-1',
      runUnitId: 'unit-1',
    });
    expect(store.flags).toContainEqual({ hasGap: true, runId: 'run-1' });
    await expect(pipeline.close({ runId: 'run-1', unitId: 'unit-1' })).resolves.toEqual({
      recoveryState: 'NONE',
      storageState: 'DEGRADED',
    });
  });

  it('records a stream error with unknown loss and time boundaries at the error event time', async () => {
    const store = new MemoryStore();
    const occurredAt = new Date('2026-09-07T08:30:00.000Z');
    const pipeline = new LogPipeline({ now: () => occurredAt, store });

    pipeline.recordGap({
      lostBytes: 42,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    await pipeline.settleRun('run-1');

    expect(store.gaps).toHaveLength(1);
    expect(store.gaps[0]).toMatchObject({
      containerRestart: false,
      endedAt: null,
      lostBytes: null,
      occurredAt,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      runUnitId: 'unit-1',
      startedAt: null,
    });
  });

  it('persists each stream error with its own event time', async () => {
    const store = new MemoryStore();
    const occurredAt = [new Date('2026-09-07T08:30:00.000Z'), new Date('2026-09-07T08:30:01.000Z')];
    let nextTime = 0;
    const pipeline = new LogPipeline({
      now: () => occurredAt[nextTime++] ?? new Date('2026-09-07T08:30:02.000Z'),
      store,
    });

    pipeline.recordGap({
      lostBytes: null,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    pipeline.recordGap({
      lostBytes: null,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    await pipeline.settleRun('run-1');

    expect(store.gaps).toHaveLength(2);
    expect(store.gaps.map((gap) => gap.occurredAt)).toEqual(occurredAt);
    expect(new Set(store.gaps.map((gap) => gap.id)).size).toBe(2);
  });

  it('bounds queued and in-flight content before the store reports a failure', async () => {
    const store = new MemoryStore();
    store.holdPersist();
    const pipeline = new LogPipeline({
      maxBufferedBytes: 4,
      maxSegmentBytes: 4,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    const firstClose = pipeline.close({ runId: 'run-1', unitId: 'unit-1' });
    await Promise.resolve();
    pipeline.append({ chunk: 'abcd', runId: 'run-2', stream: 'STDOUT', unitId: 'unit-2' });
    const secondClose = pipeline.close({ runId: 'run-2', unitId: 'unit-2' });

    const internals = pipeline as unknown as {
      readonly pendingBytes: number;
      readonly runSegments: Map<string, Array<{ readonly content: Buffer | null }>>;
    };
    expect(internals.pendingBytes).toBeLessThanOrEqual(4);
    expect(internals.runSegments.get('run-2')).toEqual([]);

    store.releasePersist();
    await Promise.all([firstClose, secondClose]);

    expect(store.persisted.map((entry) => entry.content.toString('utf8'))).toEqual(['1234']);
    expect(store.gaps).toContainEqual(
      expect.objectContaining({
        lostBytes: 4,
        reason: 'STORAGE_BUFFER_OVERFLOW',
        runId: 'run-2',
      }),
    );
  });

  it('keeps attempted gaps immutable and merges only the unattempted adjacent fact', async () => {
    const store = new MemoryStore();
    store.failRecordGap = true;
    const pipeline = new LogPipeline({ retryDelayMilliseconds: 60_000, store });

    pipeline.recordGap({
      lostBytes: 2,
      reason: 'RAW_QUEUE_OVERFLOW',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    await pipeline.settleRun('run-1');
    pipeline.recordGap({
      lostBytes: 3,
      reason: 'RAW_QUEUE_OVERFLOW',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    pipeline.recordGap({
      lostBytes: 5,
      reason: 'RAW_QUEUE_OVERFLOW',
      runId: 'run-1',
      unitId: 'unit-1',
    });
    await pipeline.settleRun('run-1');

    const internals = pipeline as unknown as {
      readonly pendingGaps: Array<{ readonly gap: LogGapRecord }>;
    };
    expect(internals.pendingGaps).toHaveLength(2);
    const attemptedId = internals.pendingGaps[0]?.gap.id;
    expect(internals.pendingGaps[1]?.gap.lostBytes).toBe(8);

    store.failRecordGap = false;
    await pipeline.retryStorage();

    expect(store.gaps).toHaveLength(2);
    expect(store.gaps[0]?.id).toBe(attemptedId);
    expect(store.gaps[1]?.lostBytes).toBe(8);
    await expect(pipeline.settleRun('run-1')).resolves.toEqual({
      recoveryState: 'NONE',
      storageState: 'DEGRADED',
    });
  });

  it('retries capacity cleanup independently after releasing persisted content', async () => {
    const store = new MemoryStore();
    store.failDelete = 'DURABLE_PENDING';
    const pipeline = new LogPipeline({
      maxBufferedBytes: 12,
      maxRunBytes: 6,
      maxSegmentBytes: 3,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: 'abcdefghijkl', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });

    await expect(pipeline.close({ runId: 'run-1', unitId: 'unit-1' })).resolves.toEqual({
      recoveryState: 'DURABLE_PENDING',
      storageState: 'DEGRADED',
    });
    expect(store.persisted).toHaveLength(4);

    store.failDelete = false;
    await pipeline.retryStorage();

    expect(store.deleted.map((entry) => entry.segment.sequence)).toEqual([1, 2]);
    expect(store.flags).toEqual([
      { hasGap: true, runId: 'run-1', truncated: true },
      { hasGap: true, runId: 'run-1', truncated: true },
    ]);
    await expect(pipeline.settleRun('run-1')).resolves.toEqual({
      recoveryState: 'NONE',
      storageState: 'DEGRADED',
    });
  });

  it('does not finalize a run while its close write is still in flight', async () => {
    const store = new MemoryStore();
    store.holdPersist();
    const pipeline = new LogPipeline({
      maxBufferedBytes: 4,
      maxSegmentBytes: 4,
      store,
      structuredRecords: false,
    });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    const close = pipeline.close({ runId: 'run-1', unitId: 'unit-1' });
    await Promise.resolve();
    pipeline.finalizeRun('run-1');

    const internals = pipeline as unknown as {
      readonly runSegments: Map<string, Array<{ readonly content: Buffer | null }>>;
    };
    expect(internals.runSegments.has('run-1')).toBe(true);

    store.releasePersist();
    await close;

    expect(internals.runSegments.has('run-1')).toBe(false);
  });

  it('releases persisted content and finalized run state', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({ maxSegmentBytes: 4, store, structuredRecords: false });

    pipeline.append({ chunk: '1234', runId: 'run-1', stream: 'STDOUT', unitId: 'unit-1' });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    const internals = pipeline as unknown as {
      readonly closedUnits: Set<string>;
      readonly nextSequenceByRunStream: Map<string, number>;
      readonly runSegments: Map<string, Array<{ readonly content: Buffer | null }>>;
    };
    expect(internals.runSegments.get('run-1')?.[0]?.content).toBeNull();
    expect(internals.nextSequenceByRunStream.has('run-1\u0000STDOUT')).toBe(true);

    pipeline.finalizeRun('run-1');

    expect(internals.runSegments.has('run-1')).toBe(false);
    expect(
      [...internals.nextSequenceByRunStream.keys()].some((key) => key.startsWith('run-1\u0000')),
    ).toBe(false);
    expect([...internals.closedUnits].some((key) => key.startsWith('run-1\u0000'))).toBe(false);
  });

  it('applies lower run limits to an open segment before accepting more log data', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({
      maxBufferedBytes: 12,
      maxRunBytes: 12,
      maxSegmentBytes: 12,
      store,
      structuredRecords: false,
    });
    pipeline.append({
      chunk: `123456${'x'.repeat(1024)}`,
      runId: 'run-1',
      stream: 'STDOUT',
      unitId: 'unit-1',
    });

    const result = await pipeline.applyLimits({ maxBufferedBytes: 12, maxRunBytes: 3 });

    expect(result).toMatchObject({
      droppedBufferedBytes: 0,
      droppedBufferedSegments: 0,
      sealedSegments: 1,
    });
    expect(result.previous).toMatchObject({ maxBufferedBytes: 12, maxRunBytes: 12 });
    expect(result.current).toMatchObject({ maxBufferedBytes: 12, maxRunBytes: 3 });
    expect(store.persisted.map((entry) => entry.content.toString('utf8'))).toEqual(['123456']);
    expect(store.deleted).toHaveLength(1);
    expect(store.deleted[0]).toMatchObject({
      reason: 'CAPACITY_ROLLOVER',
      segment: { runId: 'run-1' },
    });
  });

  it('shrinks the shared failure buffer by dropping the oldest redacted segments', async () => {
    const store = new MemoryStore();
    store.failPersist = 'MEMORY_PENDING';
    const pipeline = new LogPipeline({
      maxBufferedBytes: 12,
      maxRunBytes: 100,
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });
    pipeline.append({
      chunk: `abcdefghijkl${'x'.repeat(1024)}`,
      runId: 'run-1',
      stream: 'STDOUT',
      unitId: 'unit-1',
    });
    await expect(pipeline.settleRun('run-1')).resolves.toMatchObject({
      recoveryState: 'MEMORY_PENDING',
    });

    const result = await pipeline.applyLimits({ maxBufferedBytes: 4, maxRunBytes: 100 });

    expect(result.droppedBufferedBytes).toBe(8);
    expect(result.droppedBufferedSegments).toBe(2);
    expect(result.current.pendingBufferedBytes).toBe(4);
    expect(store.gaps).toContainEqual(
      expect.objectContaining({ lostBytes: 8, reason: 'STORAGE_BUFFER_OVERFLOW' }),
    );
  });

  it('retries only the requested run when storage is recovered manually', async () => {
    const store = new MemoryStore();
    store.failPersist = 'MEMORY_PENDING';
    const pipeline = new LogPipeline({
      maxBufferedBytes: 8,
      maxSegmentBytes: 4,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });
    pipeline.append({
      chunk: `1111${'x'.repeat(1024)}`,
      runId: 'run-1',
      stream: 'STDOUT',
      unitId: 'unit-1',
    });
    pipeline.append({
      chunk: `2222${'x'.repeat(1024)}`,
      runId: 'run-2',
      stream: 'STDOUT',
      unitId: 'unit-2',
    });
    await expect(pipeline.settleRun('run-1')).resolves.toMatchObject({
      recoveryState: 'MEMORY_PENDING',
    });
    store.failPersist = false;

    await pipeline.retryStorage('run-1');

    expect(store.persisted.map((entry) => entry.segment.runId)).toEqual(['run-1']);
    await expect(pipeline.settleRun('run-2')).resolves.toMatchObject({
      recoveryState: 'MEMORY_PENDING',
    });
  });

  it('fails closed when redaction throws and never persists the raw block', async () => {
    const store = new MemoryStore();
    const pipeline = new LogPipeline({
      redact: () => {
        throw new Error('redactor unavailable');
      },
      store,
    });

    pipeline.append({
      chunk: 'password=do-not-store',
      runId: 'run-1',
      stream: 'STDOUT',
      unitId: 'unit-1',
    });
    await pipeline.close({ runId: 'run-1', unitId: 'unit-1' });

    const persisted = store.persisted.map((entry) => entry.content.toString('utf8')).join('');
    expect(persisted).toContain('[REDACTION_FAILED]');
    expect(persisted).not.toContain('do-not-store');
    expect(store.gaps).toContainEqual(expect.objectContaining({ reason: 'REDACTION_FAILURE' }));
  });

  it('redacts URI and query-string credentials', () => {
    const output = redactLogText(
      'postgresql://user:secret@db/app?password=another-secret&token=third-secret',
    );
    expect(output).not.toContain('user:secret');
    expect(output).not.toContain('another-secret');
    expect(output).not.toContain('third-secret');
  });
});
