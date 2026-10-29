import { spawn } from 'node:child_process';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { Readable } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';

import type { LogGapReason, LogRecoveryState, LogStorageState } from '@scs/storage';

export const RUNTIME_DEFAULT_MAX_BUFFERED_LOG_BYTES = 64 * 1024 * 1024;
export const RUNTIME_DEFAULT_LOG_SINK_TIMEOUT_MILLISECONDS = 5_000;
export const RUNTIME_DEFAULT_MAX_RAW_LOG_BLOCK_BYTES = 256 * 1024;
export const RUNTIME_DEFAULT_RAW_LOG_RATE_BYTES_PER_SECOND = 1024 * 1024;
export const RUNTIME_DEFAULT_RAW_LOG_BURST_BYTES = 2 * 1024 * 1024;
export const RUNTIME_DEFAULT_MAX_QUEUED_RAW_LOG_BYTES = 16 * 1024 * 1024;
export const RUNTIME_DEFAULT_STOP_POLICY = {
  interruptGraceMilliseconds: 15_000,
  terminateGraceMilliseconds: 5_000,
} as const;

export function redactRuntimeLog(value: string): string {
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

const REDACTION_LOOK_BEHIND_BYTES = 1024;
const SENSITIVE_PREFIX_PATTERN =
  /\b(?:password|passwd|pwd|secret|token|authorization|api[_-]?key)\b\s*[:=]|(?:https?|postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|rediss|kafka):\/\/[^\s/:@]+:/giu;

/**
 * Keeps a small raw look-behind so a credential split across stdout chunks is
 * never emitted before the following chunk has been inspected.  The pending
 * raw suffix is deliberately bounded and is discarded on a redaction error.
 */
export class StreamingRuntimeRedactor {
  private pending = '';
  private decoder = new StringDecoder('utf8');
  private undecodedBytes = 0;
  private readonly sensitiveValues: readonly string[];

  constructor(
    private readonly redact: (value: string) => string,
    private readonly lookBehindBytes = REDACTION_LOOK_BEHIND_BYTES,
    sensitiveValues: readonly string[] = [],
  ) {
    if (sensitiveValues.some((value) => value.length === 0)) {
      throw new Error('RUNTIME_SENSITIVE_VALUE_INVALID');
    }
    this.sensitiveValues = [...new Set(sensitiveValues)].sort(
      (left, right) => right.length - left.length,
    );
  }

  append(value: string): string {
    this.pending += value;
    const boundary = this.safeBoundary();
    if (boundary <= 0) {
      return '';
    }
    const candidate = this.pending.slice(0, boundary);
    const remaining = this.pending.slice(boundary);
    const redacted = this.redactExact(candidate);
    this.pending = remaining;
    return redacted;
  }

  appendBytes(value: Buffer): string {
    const decoded = this.decoder.write(value);
    this.undecodedBytes = Math.max(
      0,
      this.undecodedBytes + value.length - Buffer.byteLength(decoded),
    );
    return this.append(decoded);
  }

  flush(): string {
    this.pending += this.decoder.end();
    this.decoder = new StringDecoder('utf8');
    this.undecodedBytes = 0;
    if (this.pending.length === 0) {
      return '';
    }
    const candidate = this.pending;
    const redacted = this.redactExact(candidate);
    this.pending = '';
    return redacted;
  }

  discard(): number {
    const bytes = Buffer.byteLength(this.pending, 'utf8') + this.undecodedBytes;
    this.pending = '';
    this.decoder = new StringDecoder('utf8');
    this.undecodedBytes = 0;
    return bytes;
  }

  private safeBoundary(): number {
    const exactLookBehind = Math.max(0, ...this.sensitiveValues.map((value) => value.length - 1));
    const hardBoundary = this.pending.length - Math.max(this.lookBehindBytes, exactLookBehind);
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
    for (const sensitive of this.sensitiveValues) {
      const index = this.pending.lastIndexOf(sensitive, boundary);
      if (index >= 0 && index < boundary && index + sensitive.length > boundary) {
        boundary = index;
      }
    }
    // A retained suffix must not split a UTF-16 surrogate pair before UTF-8 encoding.
    const previous = this.pending.charCodeAt(boundary - 1);
    if (previous >= 0xd800 && previous <= 0xdbff) boundary--;
    return boundary;
  }

  private redactExact(value: string): string {
    let redacted = value;
    for (const sensitive of this.sensitiveValues) {
      redacted = redacted.split(sensitive).join('[REDACTED]');
    }
    return this.redact(redacted);
  }
}

export interface RuntimeLogEntry {
  readonly chunk: Buffer;
  readonly runId?: string;
  readonly stream: 'STDOUT' | 'STDERR';
  readonly unitId: string;
}

export interface RuntimeLogSink {
  append(entry: RuntimeLogEntry): Promise<void> | void;
  close(input: {
    readonly runId?: string;
    readonly unitId: string;
  }): Promise<RuntimeLogCloseResult | void> | RuntimeLogCloseResult | void;
  finalizeRun?(runId: string): void;
  recordGap?(input: {
    readonly lostBytes: number | null;
    readonly reason: LogGapReason;
    readonly runId?: string;
    readonly unitId: string;
  }): void;
  settleRun?(runId: string): Promise<RuntimeLogCloseResult> | RuntimeLogCloseResult;
}

export interface RuntimeLogCloseResult {
  readonly recoveryState: LogRecoveryState;
  /** Present for run settlement; true only after the authoritative state reached durable storage. */
  readonly statePersisted?: boolean;
  readonly storageState: LogStorageState;
}

export interface RuntimeChildProcess {
  readonly pid?: number;
  readonly stdout: NodeJS.ReadableStream | null;
  readonly stderr: NodeJS.ReadableStream | null;
  once(event: string, listener: (...args: unknown[]) => void): RuntimeChildProcess;
  kill(signal: NodeJS.Signals): boolean;
}

export interface RuntimeProcessFactory {
  spawn(spec: RuntimeSpawnSpec): RuntimeChildProcess;
}

export interface RuntimeSpawnSpec {
  readonly args: readonly string[];
  readonly cwd: string;
  readonly env: Readonly<Record<string, string>>;
  readonly executablePath: string;
  readonly iniContent: string;
  readonly iniPath: string;
  readonly log4rsContent: string;
  readonly log4rsPath: string;
  readonly runtimeDirectory: string;
  readonly caCertificate?: string;
  readonly postgresCaCertificate?: string;
  /** Exact decrypted values used only by the in-memory per-unit log redactor. */
  readonly sensitiveValues?: readonly string[];
  readonly runId?: string;
  readonly unitId: string;
}

export interface RuntimeStopPolicy {
  readonly interruptGraceMilliseconds: number;
  readonly terminateGraceMilliseconds: number;
}

export interface RuntimeProcessSupervisorOptions {
  /** Fail-closed hook used to load durable log limits before writing runtime configuration. */
  readonly beforeStart?: (runId: string) => Promise<void>;
  readonly factory?: RuntimeProcessFactory;
  readonly logSink: RuntimeLogSink;
  readonly maxBufferedLogBytes?: number;
  readonly maxQueuedRawLogBytes?: number;
  readonly maxRawLogBlockBytes?: number;
  readonly rawLogBurstBytes?: number;
  readonly rawLogRateBytesPerSecond?: number;
  readonly monotonicNowMilliseconds?: () => number;
  readonly logSinkTimeoutMilliseconds?: number;
  readonly redact: (value: string) => string;
  /** 等待操作系统发出 spawn 后再把启动视为成功。 */
  readonly awaitSpawn?: boolean;
  /** 可选的真实健康探针；在 spawn 后、start 返回前执行。 */
  readonly readinessProbe?: RuntimeReadinessProbe;
  /** Injectable file removal boundary for deterministic cleanup-failure handling. */
  readonly removeFile?: (filePath: string) => Promise<void>;
  readonly stopPolicy?: RuntimeStopPolicy;
}

export interface RuntimeProcessExit {
  /** True only after the child emitted its terminal close event. */
  readonly confirmed: boolean;
  /** The OS exit is confirmed, but one or more plaintext runtime configuration files remain. */
  readonly configurationCleanupFailed?: boolean;
  readonly code: number | null;
  readonly forcedKill: boolean;
  readonly logGapBytes: number;
  readonly logRecoveryState: LogRecoveryState;
  readonly logStorageState: LogStorageState;
  /** A stdout/stderr stream emitted an error; the exact lost byte count may be unknown. */
  readonly logStreamError: boolean;
  readonly signal: NodeJS.Signals | null;
  readonly signals: readonly NodeJS.Signals[];
  readonly terminationTimedOut: boolean;
}

export class RuntimeConfigurationCleanupError extends Error {
  constructor(
    readonly originalError: unknown,
    options?: { readonly cause?: unknown },
  ) {
    super('RUNTIME_CONFIGURATION_CLEANUP_FAILED', options);
    this.name = 'RuntimeConfigurationCleanupError';
  }
}

export interface ManagedRuntimeProcess {
  readonly pid: number | undefined;
  forceKill(): Promise<RuntimeProcessExit>;
  stop(): Promise<RuntimeProcessExit>;
  waitForExit(): Promise<RuntimeProcessExit>;
  waitForSpawn(): Promise<void>;
}

export type RuntimeReadinessProbe = (
  spec: RuntimeSpawnSpec,
  process: ManagedRuntimeProcess,
) => Promise<void>;

class NodeRuntimeProcessFactory implements RuntimeProcessFactory {
  spawn(spec: RuntimeSpawnSpec): RuntimeChildProcess {
    return spawn(spec.executablePath, [...spec.args], {
      cwd: spec.cwd,
      detached: process.platform !== 'win32',
      env: { ...spec.env },
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    }) as unknown as RuntimeChildProcess;
  }
}

interface QueuedLogEntry {
  readonly chunk: Buffer;
  readonly runId?: string;
  readonly stream: 'STDOUT' | 'STDERR';
  readonly unitId: string;
}

interface RawLogEntry {
  readonly chunk: Buffer;
  readonly stream: 'STDOUT' | 'STDERR';
  readonly unitId: string;
}

class FairRawLogIngress {
  private readonly queues = new Map<string, RawLogEntry[]>();
  private readonly unitOrder: string[] = [];
  private readonly pendingByUnit = new Map<string, number>();
  private readonly waiters = new Map<string, Set<() => void>>();
  private readonly buckets = new Map<string, { at: number; tokens: number }>();
  private bytes = 0;
  private scheduled = false;
  private cursor = 0;

  constructor(
    private readonly options: {
      readonly burstBytes: number;
      readonly maxBlockBytes: number;
      readonly maxQueuedBytes: number;
      readonly now: () => number;
      readonly rateBytesPerSecond: number;
    },
    private readonly process: (entry: RawLogEntry) => void,
    private readonly drop: (entry: RawLogEntry) => void,
  ) {}

  enqueue(entry: RawLogEntry): void {
    if (
      entry.chunk.length > this.options.maxBlockBytes ||
      !this.consume(entry.unitId, entry.chunk.length) ||
      this.bytes + entry.chunk.length > this.options.maxQueuedBytes
    ) {
      this.drop(entry);
      return;
    }
    let queue = this.queues.get(entry.unitId);
    if (queue === undefined) {
      queue = [];
      this.queues.set(entry.unitId, queue);
      this.unitOrder.push(entry.unitId);
    }
    queue.push(entry);
    this.bytes += entry.chunk.length;
    this.pendingByUnit.set(entry.unitId, (this.pendingByUnit.get(entry.unitId) ?? 0) + 1);
    this.schedule();
  }

  async settleUnit(unitId: string): Promise<void> {
    if ((this.pendingByUnit.get(unitId) ?? 0) === 0) return;
    await new Promise<void>((resolve) => {
      const values = this.waiters.get(unitId) ?? new Set<() => void>();
      values.add(resolve);
      this.waiters.set(unitId, values);
    });
  }

  releaseUnit(unitId: string): void {
    if ((this.pendingByUnit.get(unitId) ?? 0) !== 0) {
      throw new Error('RUNTIME_RAW_LOG_UNIT_NOT_SETTLED');
    }
    this.queues.delete(unitId);
    const index = this.unitOrder.indexOf(unitId);
    if (index >= 0) {
      this.unitOrder.splice(index, 1);
      if (index < this.cursor) this.cursor -= 1;
      if (this.unitOrder.length === 0) this.cursor = 0;
      else this.cursor %= this.unitOrder.length;
    }
    this.buckets.delete(unitId);
    this.pendingByUnit.delete(unitId);
    this.waiters.delete(unitId);
  }

  private consume(unitId: string, bytes: number): boolean {
    const now = this.options.now();
    const existing = this.buckets.get(unitId) ?? { at: now, tokens: this.options.burstBytes };
    const elapsed = Math.max(0, now - existing.at);
    existing.tokens = Math.min(
      this.options.burstBytes,
      existing.tokens + (elapsed * this.options.rateBytesPerSecond) / 1000,
    );
    existing.at = now;
    this.buckets.set(unitId, existing);
    if (bytes > existing.tokens) return false;
    existing.tokens -= bytes;
    return true;
  }

  private schedule(): void {
    if (this.scheduled) return;
    this.scheduled = true;
    setImmediate(() => {
      this.scheduled = false;
      this.processNext();
    });
  }

  private processNext(): void {
    if (this.unitOrder.length === 0) return;
    this.cursor %= this.unitOrder.length;
    const unitId = this.unitOrder[this.cursor]!;
    const queue = this.queues.get(unitId)!;
    const entry = queue.shift();
    if (entry !== undefined) {
      this.bytes -= entry.chunk.length;
      this.process(entry);
      const pending = (this.pendingByUnit.get(unitId) ?? 1) - 1;
      if (pending === 0) {
        this.pendingByUnit.delete(unitId);
        for (const resolve of this.waiters.get(unitId) ?? []) resolve();
        this.waiters.delete(unitId);
      } else {
        this.pendingByUnit.set(unitId, pending);
      }
    }
    if (queue.length === 0) {
      this.queues.delete(unitId);
      this.unitOrder.splice(this.cursor, 1);
    } else {
      this.cursor = (this.cursor + 1) % this.unitOrder.length;
    }
    if (this.unitOrder.length > 0) this.schedule();
  }
}

/**
 * One process-wide FIFO buffer is shared by every managed unit.  A per-unit
 * queue would multiply the configured outage budget by the number of
 * partitions and could still exhaust the container when several processes
 * emit logs concurrently.
 */
class BoundedLogBuffer {
  private readonly entries: QueuedLogEntry[] = [];
  private readonly closedUnits = new Set<string>();
  private readonly closeFinalizing = new Set<string>();
  private readonly runIdByUnit = new Map<string, string | undefined>();
  private readonly closeResolvers = new Map<string, (result: RuntimeLogCloseResult) => void>();
  private readonly closePromises = new Map<string, Promise<RuntimeLogCloseResult>>();
  private readonly pendingByUnit = new Map<string, number>();
  private readonly gapBytesByUnit = new Map<string, number>();
  private readonly sinkFailuresByUnit = new Set<string>();
  private bytes = 0;
  private pumping = false;
  private pumpScheduled = false;
  private pumpPromise: Promise<void> | undefined;

  constructor(
    private readonly limit: number,
    private readonly sink: RuntimeLogSink,
    private readonly sinkTimeoutMilliseconds: number,
  ) {}

  getLogGapBytes(unitId: string): number {
    return this.gapBytesByUnit.get(unitId) ?? 0;
  }

  releaseUnit(unitId: string): number {
    const gapBytes = this.getLogGapBytes(unitId);
    this.closedUnits.delete(unitId);
    this.closeFinalizing.delete(unitId);
    this.closePromises.delete(unitId);
    this.closeResolvers.delete(unitId);
    this.gapBytesByUnit.delete(unitId);
    this.pendingByUnit.delete(unitId);
    this.runIdByUnit.delete(unitId);
    this.sinkFailuresByUnit.delete(unitId);
    return gapBytes;
  }

  finalizeRun(runId: string): void {
    this.sink.finalizeRun?.(runId);
  }

  async settleRun(runId: string): Promise<RuntimeLogCloseResult | undefined> {
    if (this.sink.settleRun === undefined) {
      return undefined;
    }
    try {
      return await this.withTimeout(Promise.resolve(this.sink.settleRun(runId)));
    } catch {
      return {
        recoveryState: 'MEMORY_PENDING',
        statePersisted: false,
        storageState: 'DEGRADED',
      };
    }
  }

  enqueueGap(
    unitId: string,
    bytes: number | null,
    runId?: string,
    reason: LogGapReason = 'REDACTION_FAILURE',
  ): void {
    if (bytes === null || (Number.isSafeInteger(bytes) && bytes > 0)) {
      this.addGap(unitId, bytes, runId, reason);
    }
  }

  enqueue(entry: QueuedLogEntry): void {
    if (this.closedUnits.has(entry.unitId) || entry.chunk.length === 0) {
      return;
    }
    let retained = Buffer.from(entry.chunk);
    while (this.bytes + retained.length > this.limit && this.entries.length > 0) {
      const oldest = this.entries.shift();
      if (oldest !== undefined) {
        this.bytes -= oldest.chunk.length;
        this.decrementPending(oldest.unitId);
        this.addGap(oldest.unitId, oldest.chunk.length, oldest.runId, 'RAW_QUEUE_OVERFLOW');
        this.resolveCloseIfReady(oldest.unitId);
      }
    }
    if (retained.length > this.limit) {
      const dropBytes = retained.length - this.limit;
      this.addGap(entry.unitId, dropBytes, entry.runId, 'RAW_QUEUE_OVERFLOW');
      retained = Buffer.from(retained.subarray(dropBytes));
    }
    if (this.bytes + retained.length > this.limit) {
      const dropBytes = this.bytes + retained.length - this.limit;
      this.addGap(entry.unitId, dropBytes, entry.runId, 'RAW_QUEUE_OVERFLOW');
      retained = Buffer.from(retained.subarray(dropBytes));
    }
    if (retained.length === 0) {
      return;
    }
    this.entries.push({ ...entry, chunk: retained });
    this.bytes += retained.length;
    this.incrementPending(entry.unitId);
    this.schedulePump();
  }

  async close(unitId: string, runId?: string): Promise<RuntimeLogCloseResult> {
    const existing = this.closePromises.get(unitId);
    if (existing !== undefined) {
      return await existing;
    }
    this.closedUnits.add(unitId);
    this.runIdByUnit.set(unitId, runId);
    const promise = new Promise<RuntimeLogCloseResult>((resolve) => {
      this.closeResolvers.set(unitId, resolve);
    });
    this.closePromises.set(unitId, promise);
    this.resolveCloseIfReady(unitId);
    this.schedulePump();
    return await promise;
  }

  private schedulePump(): void {
    if (this.pumpScheduled || this.pumping) {
      return;
    }
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      void this.pump();
    });
  }

  private pump(): Promise<void> {
    if (this.pumpPromise !== undefined) {
      return this.pumpPromise;
    }
    this.pumpPromise = (async () => {
      this.pumping = true;
      try {
        while (this.entries.length > 0) {
          const entry = this.entries.shift();
          if (entry === undefined) {
            continue;
          }
          try {
            await this.withTimeout(Promise.resolve(this.sink.append(entry)));
          } catch {
            this.sinkFailuresByUnit.add(entry.unitId);
            this.addGap(entry.unitId, entry.chunk.length, entry.runId, 'STORAGE_BUFFER_OVERFLOW');
          } finally {
            this.bytes -= entry.chunk.length;
            this.decrementPending(entry.unitId);
            this.resolveCloseIfReady(entry.unitId);
          }
        }
      } finally {
        this.pumping = false;
      }
    })().finally(() => {
      this.pumpPromise = undefined;
      if (this.entries.length > 0) {
        this.schedulePump();
      }
    });
    return this.pumpPromise;
  }

  private addGap(
    unitId: string,
    bytes: number | null,
    runId?: string,
    reason?: LogGapReason,
  ): void {
    if (bytes !== null) {
      this.gapBytesByUnit.set(unitId, (this.gapBytesByUnit.get(unitId) ?? 0) + bytes);
    }
    if (runId !== undefined && reason !== undefined) {
      this.sink.recordGap?.({ lostBytes: bytes, reason, runId, unitId });
    }
  }

  private incrementPending(unitId: string): void {
    this.pendingByUnit.set(unitId, (this.pendingByUnit.get(unitId) ?? 0) + 1);
  }

  private decrementPending(unitId: string): void {
    const pending = (this.pendingByUnit.get(unitId) ?? 0) - 1;
    if (pending <= 0) {
      this.pendingByUnit.delete(unitId);
      return;
    }
    this.pendingByUnit.set(unitId, pending);
  }

  private resolveCloseIfReady(unitId: string): void {
    if (
      !this.closedUnits.has(unitId) ||
      (this.pendingByUnit.get(unitId) ?? 0) !== 0 ||
      this.closeFinalizing.has(unitId)
    ) {
      return;
    }
    this.closeFinalizing.add(unitId);
    const runId = this.runIdByUnit.get(unitId);
    const finish = (result: RuntimeLogCloseResult): void => {
      this.closeFinalizing.delete(unitId);
      this.closeResolvers.get(unitId)?.({
        recoveryState: result.recoveryState,
        storageState: this.sinkFailuresByUnit.has(unitId) ? 'DEGRADED' : result.storageState,
      });
      this.closeResolvers.delete(unitId);
      this.runIdByUnit.delete(unitId);
    };
    void this.withTimeout(
      Promise.resolve(this.sink.close({ unitId, ...(runId === undefined ? {} : { runId }) })),
    )
      .then((result) =>
        finish(
          result ?? {
            recoveryState: 'NONE',
            storageState: 'HEALTHY',
          },
        ),
      )
      .catch(() =>
        finish({
          recoveryState: 'MEMORY_PENDING',
          storageState: 'DEGRADED',
        }),
      );
  }

  private async withTimeout<T>(operation: Promise<T>): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error('RUNTIME_LOG_SINK_TIMEOUT')),
        this.sinkTimeoutMilliseconds,
      );
      timer.unref();
    });
    try {
      return await Promise.race([operation, timeout]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    }
  }
}

function assertPositiveInteger(value: number, code: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(code);
  }
}

function assertSafePath(value: string, code: string): void {
  if (
    !path.isAbsolute(value) ||
    value.includes('\u0000') ||
    value.includes('\r') ||
    value.includes('\n')
  ) {
    throw new Error(code);
  }
}

function assertChildPath(root: string, candidate: string, code: string): void {
  assertSafePath(candidate, code);
  const relative = path.relative(root, candidate);
  if (relative.length === 0 || relative === '..' || relative.startsWith(`..${path.sep}`)) {
    throw new Error(code);
  }
}

function validateSpec(spec: RuntimeSpawnSpec): void {
  assertSafePath(spec.executablePath, 'RUNTIME_EXECUTABLE_PATH_INVALID');
  assertSafePath(spec.cwd, 'RUNTIME_WORKING_DIRECTORY_INVALID');
  assertSafePath(spec.runtimeDirectory, 'RUNTIME_DIRECTORY_INVALID');
  assertChildPath(spec.runtimeDirectory, spec.iniPath, 'RUNTIME_INI_PATH_INVALID');
  assertChildPath(spec.runtimeDirectory, spec.log4rsPath, 'RUNTIME_LOG_CONFIG_PATH_INVALID');
  if (spec.unitId.trim().length === 0 || spec.unitId.includes('\u0000')) {
    throw new Error('RUNTIME_UNIT_ID_INVALID');
  }
  if (spec.args.some((argument) => argument.includes('\u0000'))) {
    throw new Error('RUNTIME_ARGUMENT_INVALID');
  }
  for (const [key, value] of Object.entries(spec.env)) {
    if (
      key.length === 0 ||
      key.includes('=') ||
      key.includes('\u0000') ||
      value.includes('\u0000')
    ) {
      throw new Error('RUNTIME_ENVIRONMENT_INVALID');
    }
  }
  if (spec.iniContent.includes('\u0000') || spec.log4rsContent.includes('\u0000')) {
    throw new Error('RUNTIME_CONFIGURATION_CONTENT_INVALID');
  }
  if (spec.sensitiveValues?.some((value) => value.length === 0 || value.includes('\u0000'))) {
    throw new Error('RUNTIME_SENSITIVE_VALUE_INVALID');
  }
}

function validateOptions(
  options: RuntimeProcessSupervisorOptions,
): Required<
  Pick<
    RuntimeProcessSupervisorOptions,
    'logSinkTimeoutMilliseconds' | 'maxBufferedLogBytes' | 'stopPolicy'
  >
> {
  const maxBufferedLogBytes = options.maxBufferedLogBytes ?? RUNTIME_DEFAULT_MAX_BUFFERED_LOG_BYTES;
  assertPositiveInteger(maxBufferedLogBytes, 'RUNTIME_LOG_BUFFER_LIMIT_INVALID');
  const logSinkTimeoutMilliseconds =
    options.logSinkTimeoutMilliseconds ?? RUNTIME_DEFAULT_LOG_SINK_TIMEOUT_MILLISECONDS;
  assertPositiveInteger(logSinkTimeoutMilliseconds, 'RUNTIME_LOG_SINK_TIMEOUT_INVALID');
  const stopPolicy = options.stopPolicy ?? RUNTIME_DEFAULT_STOP_POLICY;
  assertPositiveInteger(stopPolicy.interruptGraceMilliseconds, 'RUNTIME_STOP_POLICY_INVALID');
  assertPositiveInteger(stopPolicy.terminateGraceMilliseconds, 'RUNTIME_STOP_POLICY_INVALID');
  return { logSinkTimeoutMilliseconds, maxBufferedLogBytes, stopPolicy };
}

function readStream(stream: NodeJS.ReadableStream | null): Readable | null {
  return stream instanceof Readable ? stream : null;
}

export class RuntimeProcessSupervisor {
  private readonly factory: RuntimeProcessFactory;
  private readonly logSinkTimeoutMilliseconds: number;
  private readonly logBuffer: BoundedLogBuffer;
  private readonly maxBufferedLogBytes: number;
  private readonly awaitSpawn: boolean;
  private readonly readinessProbe: RuntimeReadinessProbe | undefined;
  private readonly stopPolicy: RuntimeStopPolicy;
  private readonly rawHandlers = new Map<
    string,
    { readonly drop: (entry: RawLogEntry) => void; readonly process: (entry: RawLogEntry) => void }
  >();
  private readonly rawIngress: FairRawLogIngress;

  constructor(private readonly options: RuntimeProcessSupervisorOptions) {
    const validated = validateOptions(options);
    this.factory = options.factory ?? new NodeRuntimeProcessFactory();
    this.maxBufferedLogBytes = validated.maxBufferedLogBytes;
    this.logSinkTimeoutMilliseconds = validated.logSinkTimeoutMilliseconds;
    this.awaitSpawn = options.awaitSpawn ?? false;
    this.readinessProbe = options.readinessProbe;
    this.stopPolicy = validated.stopPolicy;
    this.logBuffer = new BoundedLogBuffer(
      this.maxBufferedLogBytes,
      options.logSink,
      this.logSinkTimeoutMilliseconds,
    );
    const maxBlockBytes = options.maxRawLogBlockBytes ?? RUNTIME_DEFAULT_MAX_RAW_LOG_BLOCK_BYTES;
    const maxQueuedBytes = options.maxQueuedRawLogBytes ?? RUNTIME_DEFAULT_MAX_QUEUED_RAW_LOG_BYTES;
    const rateBytesPerSecond =
      options.rawLogRateBytesPerSecond ?? RUNTIME_DEFAULT_RAW_LOG_RATE_BYTES_PER_SECOND;
    const burstBytes = options.rawLogBurstBytes ?? RUNTIME_DEFAULT_RAW_LOG_BURST_BYTES;
    assertPositiveInteger(maxBlockBytes, 'RUNTIME_RAW_LOG_BLOCK_LIMIT_INVALID');
    assertPositiveInteger(maxQueuedBytes, 'RUNTIME_RAW_LOG_QUEUE_LIMIT_INVALID');
    assertPositiveInteger(rateBytesPerSecond, 'RUNTIME_RAW_LOG_RATE_LIMIT_INVALID');
    assertPositiveInteger(burstBytes, 'RUNTIME_RAW_LOG_BURST_LIMIT_INVALID');
    this.rawIngress = new FairRawLogIngress(
      {
        burstBytes,
        maxBlockBytes,
        maxQueuedBytes,
        now: options.monotonicNowMilliseconds ?? (() => performance.now()),
        rateBytesPerSecond,
      },
      (entry) => this.rawHandlers.get(entry.unitId)?.process(entry),
      (entry) => this.rawHandlers.get(entry.unitId)?.drop(entry),
    );
  }

  finalizeRun(runId: string): void {
    this.logBuffer.finalizeRun(runId);
  }

  async settleRunLogs(runId: string): Promise<RuntimeLogCloseResult | undefined> {
    return await this.logBuffer.settleRun(runId);
  }

  async start(spec: RuntimeSpawnSpec): Promise<ManagedRuntimeProcess> {
    validateSpec(spec);
    if (this.options.beforeStart !== undefined) {
      if (spec.runId === undefined || spec.runId.trim().length === 0) {
        throw new Error('LOG_RUN_ID_REQUIRED');
      }
      await this.options.beforeStart(spec.runId);
    }
    await mkdir(spec.runtimeDirectory, { mode: 0o700, recursive: true });
    try {
      await writeFile(spec.iniPath, spec.iniContent, { encoding: 'utf8', flag: 'wx', mode: 0o600 });
      if (spec.caCertificate !== undefined)
        await writeFile(path.join(spec.runtimeDirectory, 'mysql-ca.pem'), spec.caCertificate, {
          encoding: 'utf8',
          flag: 'wx',
          mode: 0o600,
        });
      if (spec.postgresCaCertificate !== undefined)
        await writeFile(
          path.join(spec.runtimeDirectory, 'postgres-ca.pem'),
          spec.postgresCaCertificate,
          {
            encoding: 'utf8',
            flag: 'wx',
            mode: 0o600,
          },
        );
      await writeFile(spec.log4rsPath, spec.log4rsContent, {
        encoding: 'utf8',
        flag: 'wx',
        mode: 0o600,
      });
      const child = this.factory.spawn(spec);
      const managed = this.manage(spec, child);
      if (this.awaitSpawn || this.readinessProbe !== undefined) {
        try {
          await managed.waitForSpawn();
          if (this.readinessProbe !== undefined) {
            await this.readinessProbe(spec, managed);
          }
        } catch (error) {
          await managed.stop().catch(() => undefined);
          throw error;
        }
      }
      return managed;
    } catch (error) {
      try {
        await this.cleanupFiles(spec);
      } catch (cleanupError) {
        throw new RuntimeConfigurationCleanupError(error, { cause: cleanupError });
      }
      throw error;
    }
  }

  private manage(spec: RuntimeSpawnSpec, child: RuntimeChildProcess): ManagedRuntimeProcess {
    const stdout = readStream(child.stdout);
    const stderr = readStream(child.stderr);
    const redactors = new Map<'STDOUT' | 'STDERR', StreamingRuntimeRedactor>([
      [
        'STDOUT',
        new StreamingRuntimeRedactor(this.options.redact, undefined, spec.sensitiveValues),
      ],
      [
        'STDERR',
        new StreamingRuntimeRedactor(this.options.redact, undefined, spec.sensitiveValues),
      ],
    ]);
    const failedStreams = new Set<'STDOUT' | 'STDERR'>();
    this.rawHandlers.set(spec.unitId, {
      drop: (entry) => {
        const pending = redactors.get(entry.stream)?.discard() ?? 0;
        this.logBuffer.enqueueGap(
          spec.unitId,
          entry.chunk.length + pending,
          spec.runId,
          'RAW_QUEUE_OVERFLOW',
        );
      },
      process: (entry) =>
        failedStreams.has(entry.stream)
          ? this.logBuffer.enqueueGap(
              spec.unitId,
              entry.chunk.length,
              spec.runId,
              'RAW_QUEUE_OVERFLOW',
            )
          : this.enqueueRedacted(
              entry.stream,
              spec.unitId,
              entry.chunk,
              redactors.get(entry.stream)!,
              spec.runId,
            ),
    });
    stdout?.on('data', (chunk: Buffer | Uint8Array | string) => {
      this.rawIngress.enqueue({
        chunk: Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
        stream: 'STDOUT',
        unitId: spec.unitId,
      });
    });
    stderr?.on('data', (chunk: Buffer | Uint8Array | string) => {
      this.rawIngress.enqueue({
        chunk: Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
        stream: 'STDERR',
        unitId: spec.unitId,
      });
    });

    let settled = false;
    let logStreamError = false;
    const handleStreamError = (stream: 'STDOUT' | 'STDERR'): void => {
      logStreamError = true;
      failedStreams.add(stream);
      const discardedBytes = redactors.get(stream)?.discard() ?? 0;
      this.logBuffer.enqueueGap(spec.unitId, discardedBytes);
      this.logBuffer.enqueueGap(spec.unitId, null, spec.runId, 'STREAM_ERROR');
    };
    stdout?.on('error', () => handleStreamError('STDOUT'));
    stderr?.on('error', () => handleStreamError('STDERR'));
    let terminationTimedOut = false;
    let rawCloseResult: { code: number | null; signal: NodeJS.Signals | null } = {
      code: null,
      signal: null,
    };
    let resolveClosed: (() => void) | undefined;
    const closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    let resolveExit: ((result: RuntimeProcessExit) => void) | undefined;
    const exit = new Promise<RuntimeProcessExit>((resolve) => {
      resolveExit = resolve;
    });
    let spawned = false;
    let resolveSpawned: (() => void) | undefined;
    let rejectSpawned: ((error: unknown) => void) | undefined;
    const spawnReady = new Promise<void>((resolve, reject) => {
      resolveSpawned = resolve;
      rejectSpawned = reject;
    });
    // The normal production path may not await spawn readiness.  Attach a
    // rejection handler nevertheless so an early close/error cannot become an
    // unhandled rejection; callers that do await waitForSpawn still observe
    // the original rejection.
    void spawnReady.catch(() => undefined);
    const signals: NodeJS.Signals[] = [];
    const finish = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (settled) {
        return;
      }
      settled = true;
      rawCloseResult = { code, signal };
      if (!spawned) {
        rejectSpawned?.(new Error('RUNTIME_PROCESS_SPAWN_FAILED'));
      }
      resolveClosed?.();
      void (async () => {
        await this.rawIngress.settleUnit(spec.unitId);
        for (const stream of ['STDOUT', 'STDERR'] as const) {
          this.flushRedacted(stream, spec.unitId, redactors.get(stream)!, spec.runId);
        }
        const logClose = await this.logBuffer.close(spec.unitId, spec.runId);
        let configurationCleanupFailed = false;
        try {
          await this.cleanupFiles(spec);
        } catch {
          configurationCleanupFailed = true;
        }
        const logGapBytes = this.logBuffer.releaseUnit(spec.unitId);
        this.rawHandlers.delete(spec.unitId);
        this.rawIngress.releaseUnit(spec.unitId);
        resolveExit?.({
          confirmed: true,
          configurationCleanupFailed,
          code: rawCloseResult.code,
          forcedKill: false,
          logGapBytes,
          logRecoveryState: logClose.recoveryState,
          logStorageState: logClose.storageState,
          logStreamError,
          signal: rawCloseResult.signal,
          signals: [...signals],
          terminationTimedOut,
        });
      })();
    };
    child.once('spawn', () => {
      spawned = true;
      resolveSpawned?.();
    });
    child.once('error', (error: unknown) => {
      if (!spawned) {
        rejectSpawned?.(error);
      }
      finish(null, null);
    });
    child.once('close', (code: unknown, signal: unknown) =>
      finish(
        typeof code === 'number' ? code : null,
        typeof signal === 'string' ? (signal as NodeJS.Signals) : null,
      ),
    );

    const send = (signal: NodeJS.Signals): void => {
      signals.push(signal);
      try {
        if (process.platform !== 'win32' && child.pid !== undefined) {
          try {
            process.kill(-child.pid, signal);
          } catch {
            child.kill(signal);
          }
        } else {
          child.kill(signal);
        }
      } catch {
        // The process may have exited between the state check and kill.
      }
    };
    const timeoutExit = (): RuntimeProcessExit => ({
      confirmed: false,
      code: null,
      forcedKill: true,
      logGapBytes: this.logBuffer.getLogGapBytes(spec.unitId),
      logRecoveryState: 'NONE',
      logStorageState: 'HEALTHY',
      logStreamError,
      signal: 'SIGKILL',
      signals: [...signals],
      terminationTimedOut: true,
    });
    let forceKillPromise: Promise<RuntimeProcessExit> | undefined;
    const forceKill = async (): Promise<RuntimeProcessExit> => {
      if (forceKillPromise !== undefined) {
        return await forceKillPromise;
      }
      forceKillPromise = (async () => {
        if (!settled) {
          send('SIGKILL');
          if (!(await this.closedWithin(closed, this.stopPolicy.terminateGraceMilliseconds))) {
            if (!settled) {
              terminationTimedOut = true;
              return timeoutExit();
            }
          }
        }
        const result = await exit;
        return { ...result, forcedKill: signals.includes('SIGKILL'), signals: [...signals] };
      })();
      return await forceKillPromise;
    };

    let stopPromise: Promise<RuntimeProcessExit> | undefined;
    const stop = async (): Promise<RuntimeProcessExit> => {
      if (stopPromise !== undefined) {
        return await stopPromise;
      }
      stopPromise = (async () => {
        if (settled) {
          return await exit;
        }
        send('SIGINT');
        if (!(await this.closedWithin(closed, this.stopPolicy.interruptGraceMilliseconds))) {
          send('SIGTERM');
          if (!(await this.closedWithin(closed, this.stopPolicy.terminateGraceMilliseconds))) {
            send('SIGKILL');
            const closedAfterKill = await this.closedWithin(
              closed,
              this.stopPolicy.terminateGraceMilliseconds,
            );
            if (!closedAfterKill && !settled) {
              terminationTimedOut = true;
              // Do not synthesize a terminal close event.  The OS process may
              // still be alive, so callers must retain its identity/handle and
              // keep capacity reserved until waitForExit() confirms closure.
              return timeoutExit();
            }
          }
        }
        const result = await exit;
        return {
          ...result,
          forcedKill: signals.includes('SIGKILL'),
          signals: [...signals],
        };
      })();
      return await stopPromise;
    };

    return {
      forceKill,
      pid: child.pid ?? undefined,
      stop,
      waitForExit: async () => await exit,
      waitForSpawn: async () => await spawnReady,
    };
  }

  private enqueueRedacted(
    stream: 'STDOUT' | 'STDERR',
    unitId: string,
    rawChunk: Buffer | Uint8Array | string,
    redactor: StreamingRuntimeRedactor,
    runId?: string,
  ): void {
    const raw = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    try {
      const redacted = redactor.appendBytes(raw);
      if (redacted.length > 0) {
        this.logBuffer.enqueue({
          chunk: Buffer.from(redacted, 'utf8'),
          ...(runId === undefined ? {} : { runId }),
          stream,
          unitId,
        });
      }
    } catch {
      const discardedBytes = redactor.discard();
      this.logBuffer.enqueue({
        chunk: Buffer.from('[REDACTION_FAILED]', 'utf8'),
        ...(runId === undefined ? {} : { runId }),
        stream,
        unitId,
      });
      // The placeholder is safe to persist; the queue's gap counter records that raw bytes were discarded.
      this.logBuffer.enqueueGap(unitId, raw.length + discardedBytes, runId, 'REDACTION_FAILURE');
    }
  }

  private flushRedacted(
    stream: 'STDOUT' | 'STDERR',
    unitId: string,
    redactor: StreamingRuntimeRedactor,
    runId?: string,
  ): void {
    try {
      const redacted = redactor.flush();
      if (redacted.length > 0) {
        this.logBuffer.enqueue({
          chunk: Buffer.from(redacted, 'utf8'),
          ...(runId === undefined ? {} : { runId }),
          stream,
          unitId,
        });
      }
    } catch {
      const discardedBytes = redactor.discard();
      this.logBuffer.enqueue({
        chunk: Buffer.from('[REDACTION_FAILED]', 'utf8'),
        ...(runId === undefined ? {} : { runId }),
        stream,
        unitId,
      });
      this.logBuffer.enqueueGap(unitId, discardedBytes, runId, 'REDACTION_FAILURE');
    }
  }

  private async closedWithin(closed: Promise<void>, milliseconds: number): Promise<boolean> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), milliseconds);
      timer.unref();
    });
    const result = await Promise.race([closed.then(() => true), timeout]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    return result;
  }

  private async cleanupFiles(spec: RuntimeSpawnSpec): Promise<void> {
    const removeFile =
      this.options.removeFile ?? (async (filePath: string) => await rm(filePath, { force: true }));
    const results = await Promise.allSettled([
      removeFile(spec.iniPath),
      removeFile(spec.log4rsPath),
      ...(spec.caCertificate === undefined
        ? []
        : [removeFile(path.join(spec.runtimeDirectory, 'mysql-ca.pem'))]),
      ...(spec.postgresCaCertificate === undefined
        ? []
        : [removeFile(path.join(spec.runtimeDirectory, 'postgres-ca.pem'))]),
    ]);
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') {
      throw failure.reason;
    }
  }
}
