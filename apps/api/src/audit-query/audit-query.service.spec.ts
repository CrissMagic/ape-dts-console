import { describe, expect, it, vi } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { AuditQueryError, AuditQueryService } from './audit-query.service.js';

const userId = '11111111-1111-4111-8111-111111111111';
const subjectId = '22222222-2222-4222-8222-222222222222';

function row(sequence: string, occurredAt: string, actor: 'SYSTEM' | 'USER' = 'SYSTEM') {
  return {
    action: 'TASK_RUNTIME_STARTED',
    actor_type: actor,
    actor_user_id: actor === 'USER' ? userId : null,
    canonical_details: { origin: actor },
    event_id: `00000000-0000-4000-8000-${sequence.padStart(12, '0')}`,
    occurred_at: new Date(occurredAt),
    sequence,
    subject_id: subjectId,
    subject_type: 'TASK',
  };
}

function setup(rows: readonly ReturnType<typeof row>[] = []) {
  const query = vi.fn(async () => ({ rows }));
  const service = new AuditQueryService({
    requirePool: () => ({ query }),
  } as unknown as PlatformDatabaseService);
  return { query, service };
}

describe('AuditQueryService', () => {
  it('returns newest facts first with exact actor identity and a stable continuation cursor', async () => {
    const test = setup([
      row('3', '2026-09-18T08:03:00.000Z', 'USER'),
      row('2', '2026-09-18T08:02:00.000Z'),
      row('1', '2026-09-18T08:01:00.000Z'),
    ]);

    const first = await test.service.list({
      action: 'TASK_RUNTIME_STARTED',
      actorType: 'USER',
      limit: 2,
      subjectId,
      subjectType: 'TASK',
    });

    expect(first.events).toHaveLength(2);
    expect(first.events[0]).toMatchObject({
      actorType: 'USER',
      actorUserId: userId,
      sequence: '3',
    });
    expect(first.events[1]).toMatchObject({
      actorType: 'SYSTEM',
      actorUserId: null,
      sequence: '2',
    });
    expect(first.nextCursor).toEqual(expect.any(String));
    expect(test.query).toHaveBeenCalledWith(
      expect.stringContaining('ORDER BY occurred_at DESC, sequence DESC'),
      ['USER', 'TASK_RUNTIME_STARTED', 'TASK', subjectId, 3],
    );

    test.query.mockResolvedValueOnce({ rows: [row('1', '2026-09-18T08:01:00.000Z')] });
    await test.service.list({ cursor: first.nextCursor!, limit: 2 });

    expect(test.query).toHaveBeenLastCalledWith(
      expect.stringContaining('(occurred_at, sequence) < ($1::timestamptz, $2::bigint)'),
      ['2026-09-18T08:02:00.000Z', '2', 3],
    );
  });

  it('omits a continuation cursor when the page is complete', async () => {
    const test = setup([row('1', '2026-09-18T08:01:00.000Z')]);

    await expect(test.service.list({ limit: 50 })).resolves.toMatchObject({
      events: [expect.objectContaining({ sequence: '1' })],
      nextCursor: null,
    });
  });

  it('rejects malformed cursors without querying the ledger', async () => {
    const test = setup();

    await expect(test.service.list({ cursor: 'not-a-cursor', limit: 50 })).rejects.toBeInstanceOf(
      AuditQueryError,
    );
    expect(test.query).not.toHaveBeenCalled();
  });

  it.each(['2026-02-30T00:00:00.000Z', '2026-04-31T00:00:00.000+08:00', '2026-09-18T08:00:00.000'])(
    'rejects an invalid cursor timestamp %s before querying the ledger',
    async (occurredAt) => {
      const test = setup();
      const cursor = Buffer.from(JSON.stringify({ occurredAt, sequence: '1' }), 'utf8').toString(
        'base64url',
      );

      await expect(test.service.list({ cursor, limit: 50 })).rejects.toMatchObject({
        code: 'AUDIT_CURSOR_INVALID',
      });
      expect(test.query).not.toHaveBeenCalled();
    },
  );
});
