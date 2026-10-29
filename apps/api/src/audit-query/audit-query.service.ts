import { Injectable } from '@nestjs/common';
import type {
  AuditActorType,
  AuditEvent,
  AuditEventListQuery,
  AuditEventListResponse,
} from '@scs/contracts';
import type { QueryResultRow } from 'pg';
import { z } from 'zod';

import { PlatformDatabaseService } from '../database/platform-database.service.js';

interface AuditEventRow extends QueryResultRow {
  action: string;
  actor_type: AuditActorType;
  actor_user_id: string | null;
  canonical_details: Record<string, unknown>;
  event_id: string;
  occurred_at: Date;
  sequence: string;
  subject_id: string | null;
  subject_type: string;
}

interface AuditCursor {
  readonly occurredAt: string;
  readonly sequence: string;
}

function hasValidCalendarDate(value: string): boolean {
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(5, 7));
  const day = Number(value.slice(8, 10));
  if (year < 1) return false;
  const endOfMonth = new Date(0);
  endOfMonth.setUTCHours(0, 0, 0, 0);
  endOfMonth.setUTCFullYear(year, month, 0);
  return day <= endOfMonth.getUTCDate();
}

const auditCursorSchema = z
  .object({
    occurredAt: z.iso
      .datetime({ offset: true })
      .refine(hasValidCalendarDate, 'occurredAt must be a real calendar date'),
    sequence: z.string().regex(/^[1-9]\d*$/u),
  })
  .strict();

export class AuditQueryError extends Error {
  readonly code = 'AUDIT_CURSOR_INVALID';

  constructor(options?: ErrorOptions) {
    super('AUDIT_CURSOR_INVALID', options);
    this.name = 'AuditQueryError';
  }
}

function encodeCursor(cursor: AuditCursor): string {
  return Buffer.from(JSON.stringify(cursor), 'utf8').toString('base64url');
}

function decodeCursor(value: string): AuditCursor {
  try {
    const decoded = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as unknown;
    return auditCursorSchema.parse(decoded);
  } catch (cause) {
    throw new AuditQueryError({ cause });
  }
}

function toAuditEvent(row: AuditEventRow): AuditEvent {
  return {
    action: row.action,
    actorType: row.actor_type,
    actorUserId: row.actor_user_id,
    details: row.canonical_details,
    eventId: row.event_id,
    occurredAt: row.occurred_at.toISOString(),
    sequence: row.sequence,
    subjectId: row.subject_id,
    subjectType: row.subject_type,
  };
}

@Injectable()
export class AuditQueryService {
  constructor(private readonly database: PlatformDatabaseService) {}

  async list(query: AuditEventListQuery): Promise<AuditEventListResponse> {
    const clauses: string[] = [];
    const values: unknown[] = [];
    const add = (clause: (parameter: number) => string, value: unknown): void => {
      values.push(value);
      clauses.push(clause(values.length));
    };

    if (query.actorType !== undefined) {
      add((parameter) => `actor_type = $${parameter}`, query.actorType);
    }
    if (query.action !== undefined) {
      add((parameter) => `action = $${parameter}`, query.action);
    }
    if (query.subjectType !== undefined) {
      add((parameter) => `subject_type = $${parameter}`, query.subjectType);
    }
    if (query.subjectId !== undefined) {
      add((parameter) => `subject_id = $${parameter}::uuid`, query.subjectId);
    }
    if (query.cursor !== undefined) {
      const cursor = decodeCursor(query.cursor);
      values.push(cursor.occurredAt, cursor.sequence);
      clauses.push(
        `(occurred_at, sequence) < ($${values.length - 1}::timestamptz, $${values.length}::bigint)`,
      );
    }

    values.push(query.limit + 1);
    const where = clauses.length === 0 ? '' : `WHERE ${clauses.join('\n         AND ')}`;
    const result = await this.database.requirePool().query<AuditEventRow>(
      `SELECT
         sequence::text,
         event_id,
         actor_type,
         actor_user_id,
         action,
         subject_type,
         subject_id,
         canonical_details,
         occurred_at
       FROM ape_dts_platform.audit_events
       ${where}
       ORDER BY occurred_at DESC, sequence DESC
       LIMIT $${values.length}`,
      values,
    );
    const hasMore = result.rows.length > query.limit;
    const events = result.rows.slice(0, query.limit).map(toAuditEvent);
    const last = events.at(-1);
    return {
      events,
      nextCursor:
        hasMore && last !== undefined
          ? encodeCursor({ occurredAt: last.occurredAt, sequence: last.sequence })
          : null,
    };
  }
}
