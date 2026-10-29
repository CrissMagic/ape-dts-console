export type LogSegmentStream = 'STDOUT' | 'STDERR';

export type LogSegmentState =
  'OPEN' | 'SEALED' | 'AVAILABLE' | 'DELETED' | 'MISSING' | 'INTEGRITY_FAILED';

export type LogGapReason =
  | 'REDACTION_FAILURE'
  | 'RAW_QUEUE_OVERFLOW'
  | 'STORAGE_BUFFER_OVERFLOW'
  | 'CONTAINER_RESTART'
  | 'STREAM_ERROR'
  | 'LEGACY_FORMAT';

export type LogStorageState = 'HEALTHY' | 'DEGRADED' | 'UNKNOWN';

export type LogRecoveryState = 'NONE' | 'MEMORY_PENDING' | 'DURABLE_PENDING' | 'MIXED_PENDING';

export type LogPendingRecoveryState = Extract<
  LogRecoveryState,
  'MEMORY_PENDING' | 'DURABLE_PENDING'
>;

export class LogSegmentPersistenceError extends Error {
  readonly code: string;

  constructor(
    readonly recoveryState: LogPendingRecoveryState,
    message = 'LOG_SEGMENT_PERSIST_FAILED',
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.code = message;
    this.name = 'LogSegmentPersistenceError';
  }
}

export interface LogSegmentRecord {
  readonly byteSize: number;
  readonly createdAt: Date;
  readonly deletedAt: Date | null;
  readonly deletedReason: 'RETENTION' | 'CAPACITY_ROLLOVER' | null;
  readonly endedAt: Date | null;
  readonly id: string;
  readonly runId: string;
  readonly runUnitId: string;
  readonly sequence: number;
  readonly sha256: string | null;
  readonly startedAt: Date;
  readonly state: LogSegmentState;
  readonly stream: LogSegmentStream;
}

interface LogGapRecordIdentity {
  readonly id: string;
  readonly occurredAt: Date;
  readonly runId: string;
  readonly runUnitId: string;
}

export type LogGapRecord = LogGapRecordIdentity &
  (
    | {
        readonly containerRestart: false;
        readonly endedAt: null;
        readonly lostBytes: null;
        readonly reason: 'STREAM_ERROR';
        readonly startedAt: null;
      }
    | {
        readonly containerRestart: boolean;
        readonly endedAt: Date | null;
        readonly lostBytes: number | null;
        readonly reason: Exclude<LogGapReason, 'STREAM_ERROR'>;
        readonly startedAt: Date | null;
      }
  );

export interface LogSegmentStore {
  /** CAPACITY_ROLLOVER completion also persists run has_gap/truncated flags atomically. */
  deleteSegment(input: {
    readonly reason: 'RETENTION' | 'CAPACITY_ROLLOVER';
    readonly segment: LogSegmentRecord;
  }): Promise<void>;
  /**
   * Optional durable capacity boundary. Implementations backed by persistent
   * metadata use this to include segments created by an earlier process.
   */
  enforceRunCapacity?(input: { readonly maxBytes: number; readonly runId: string }): Promise<void>;
  persistSegment(input: {
    readonly content: Buffer;
    readonly segment: LogSegmentRecord;
  }): Promise<void>;
  recordGap(gap: LogGapRecord): Promise<void>;
  updateRunLogState(input: {
    readonly recoveryState: LogRecoveryState;
    readonly runId: string;
    readonly storageState: LogStorageState;
  }): Promise<void>;
}
