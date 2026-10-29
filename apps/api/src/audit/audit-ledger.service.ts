import { timingSafeEqual, randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { PoolClient, QueryResultRow } from 'pg';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { calculateAuditEventHash, type AuditHashInput } from './audit-hash.js';

const AUDIT_LOCK_NAMESPACE = 1_096_048_964;
const AUDIT_LOCK_KEY = 1_096_104_660;
const FORBIDDEN_DETAIL_KEY = /password|secret|token|code|credential|cookie|authorization/iu;

export interface AuditChainRow extends AuditHashInput {
  readonly eventHash: Buffer;
  readonly previousHash: Buffer | null;
  readonly sequence: string;
}

export interface AuditCheckpoint {
  readonly firstRetainedSequence: string;
  readonly lastDeletedEventHash: Buffer;
}

export type AuditVerificationResult =
  | { readonly checkedEvents: number; readonly valid: true }
  | {
      readonly checkedEvents: number;
      readonly reason:
        | 'EVENT_HASH_MISMATCH'
        | 'MISSING_CHECKPOINT'
        | 'NON_MONOTONIC_SEQUENCE'
        | 'PREVIOUS_HASH_MISMATCH';
      readonly sequence: string;
      readonly valid: false;
    };

export interface AppendAuditEventInput {
  readonly action: string;
  readonly actorType: AuditHashInput['actorType'];
  readonly actorUserId: string | null;
  readonly details: Readonly<Record<string, unknown>>;
  readonly subjectId: string | null;
  readonly subjectType: string;
}

interface PreviousHashRow extends QueryResultRow {
  event_hash: Buffer;
}

interface InsertedAuditRow extends QueryResultRow {
  sequence: string;
}

interface DatabaseAuditRow extends QueryResultRow {
  action: string;
  actor_type: AuditHashInput['actorType'];
  actor_user_id: string | null;
  canonical_details: Record<string, unknown>;
  event_hash: Buffer;
  event_id: string;
  occurred_at: Date;
  previous_hash: Buffer | null;
  sequence: string;
  subject_id: string | null;
  subject_type: string;
}

function equalHash(left: Uint8Array | null, right: Uint8Array | null): boolean {
  if (left === null || right === null) {
    return left === right;
  }
  return left.byteLength === right.byteLength && timingSafeEqual(left, right);
}

function assertSafeAuditValue(value: unknown, path: readonly string[] = []): void {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertSafeAuditValue(entry, [...path, String(index)]));
    return;
  }
  if (typeof value !== 'object') {
    throw new Error('AUDIT_DETAILS_INVALID');
  }
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (FORBIDDEN_DETAIL_KEY.test(key)) {
      throw new Error(`AUDIT_DETAILS_SENSITIVE_KEY:${[...path, key].join('.')}`);
    }
    assertSafeAuditValue(entry, [...path, key]);
  }
}

export function verifyAuditChainRows(
  rows: readonly AuditChainRow[],
  checkpoint: AuditCheckpoint | null,
): AuditVerificationResult {
  if (rows.length === 0) {
    return { checkedEvents: 0, valid: true };
  }
  const first = rows[0]!;
  let expectedPreviousHash: Buffer | null = null;
  if (first.previousHash !== null) {
    if (
      checkpoint === null ||
      checkpoint.firstRetainedSequence !== first.sequence ||
      !equalHash(checkpoint.lastDeletedEventHash, first.previousHash)
    ) {
      return {
        checkedEvents: 0,
        reason: 'MISSING_CHECKPOINT',
        sequence: first.sequence,
        valid: false,
      };
    }
    expectedPreviousHash = checkpoint.lastDeletedEventHash;
  }

  let previousSequence: bigint | null = null;
  for (const [index, row] of rows.entries()) {
    const sequence = BigInt(row.sequence);
    if (previousSequence !== null && sequence <= previousSequence) {
      return {
        checkedEvents: index,
        reason: 'NON_MONOTONIC_SEQUENCE',
        sequence: row.sequence,
        valid: false,
      };
    }
    if (!equalHash(row.previousHash, expectedPreviousHash)) {
      return {
        checkedEvents: index,
        reason: 'PREVIOUS_HASH_MISMATCH',
        sequence: row.sequence,
        valid: false,
      };
    }
    const calculated = calculateAuditEventHash(expectedPreviousHash, {
      action: row.action,
      actorType: row.actorType,
      actorUserId: row.actorUserId,
      details: row.details,
      eventId: row.eventId,
      occurredAt: row.occurredAt,
      subjectId: row.subjectId,
      subjectType: row.subjectType,
    });
    if (!equalHash(row.eventHash, calculated)) {
      return {
        checkedEvents: index + 1,
        reason: 'EVENT_HASH_MISMATCH',
        sequence: row.sequence,
        valid: false,
      };
    }
    expectedPreviousHash = row.eventHash;
    previousSequence = sequence;
  }
  return { checkedEvents: rows.length, valid: true };
}

@Injectable()
export class AuditLedgerService {
  constructor(private readonly database: PlatformDatabaseService) {}

  async append(client: PoolClient, input: AppendAuditEventInput): Promise<string> {
    assertSafeAuditValue(input.details);
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      AUDIT_LOCK_NAMESPACE,
      AUDIT_LOCK_KEY,
    ]);
    const previous = await client.query<PreviousHashRow>(
      'SELECT event_hash FROM ape_dts_platform.audit_events ORDER BY sequence DESC LIMIT 1',
    );
    const previousHash = previous.rows[0]?.event_hash ?? null;
    const event: AuditHashInput = {
      ...input,
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
    };
    const inserted = await client.query<InsertedAuditRow>(
      `INSERT INTO ape_dts_platform.audit_events (
        event_id,
        actor_type,
        actor_user_id,
        action,
        subject_type,
        subject_id,
        canonical_details,
        previous_hash,
        event_hash,
        occurred_at
      ) VALUES ($1, $2, $3::uuid, $4, $5, $6::uuid, $7::jsonb, $8, $9, $10::timestamptz)
      RETURNING sequence::text`,
      [
        event.eventId,
        event.actorType,
        event.actorUserId,
        event.action,
        event.subjectType,
        event.subjectId,
        JSON.stringify(event.details),
        previousHash,
        calculateAuditEventHash(previousHash, event),
        event.occurredAt,
      ],
    );
    const sequence = inserted.rows[0]?.sequence;
    if (sequence === undefined) {
      throw new Error('AUDIT_APPEND_FAILED');
    }
    return sequence;
  }

  async verify(): Promise<AuditVerificationResult> {
    const pool = this.database.requirePool();
    const result = await pool.query<DatabaseAuditRow>(
      `SELECT
        sequence::text,
        event_id,
        actor_type,
        actor_user_id,
        action,
        subject_type,
        subject_id,
        canonical_details,
        previous_hash,
        event_hash,
        occurred_at
       FROM ape_dts_platform.audit_events
       ORDER BY ape_dts_platform.audit_events.sequence`,
    );
    const rows: AuditChainRow[] = result.rows.map((row) => ({
      action: row.action,
      actorType: row.actor_type,
      actorUserId: row.actor_user_id,
      details: row.canonical_details,
      eventHash: row.event_hash,
      eventId: row.event_id,
      occurredAt: row.occurred_at.toISOString(),
      previousHash: row.previous_hash,
      sequence: row.sequence,
      subjectId: row.subject_id,
      subjectType: row.subject_type,
    }));
    const first = rows[0];
    let checkpoint: AuditCheckpoint | null = null;
    if (first?.previousHash !== null && first !== undefined) {
      const checkpointResult = await pool.query<
        QueryResultRow & {
          first_retained_sequence: string;
          last_deleted_event_hash: Buffer;
        }
      >(
        `SELECT first_retained_sequence::text, last_deleted_event_hash
         FROM ape_dts_platform.audit_chain_checkpoints
         WHERE first_retained_sequence = $1::bigint`,
        [first.sequence],
      );
      const row = checkpointResult.rows[0];
      if (row !== undefined) {
        checkpoint = {
          firstRetainedSequence: row.first_retained_sequence,
          lastDeletedEventHash: row.last_deleted_event_hash,
        };
      }
    }
    return verifyAuditChainRows(rows, checkpoint);
  }
}
