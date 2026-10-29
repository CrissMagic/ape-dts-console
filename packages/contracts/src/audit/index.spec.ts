import { describe, expect, it } from 'vitest';

import {
  auditEventListQuerySchema,
  auditEventListResponseSchema,
  auditEventSchema,
} from './index.js';

const userId = '11111111-1111-4111-8111-111111111111';

describe('audit contracts', () => {
  it('coerces a bounded list request and preserves supported filters', () => {
    expect(
      auditEventListQuerySchema.parse({
        action: ' TASK_STARTED ',
        actorType: 'USER',
        cursor: 'opaque-cursor',
        limit: '25',
        subjectId: userId,
        subjectType: ' TASK ',
      }),
    ).toEqual({
      action: 'TASK_STARTED',
      actorType: 'USER',
      cursor: 'opaque-cursor',
      limit: 25,
      subjectId: userId,
      subjectType: 'TASK',
    });
  });

  it('rejects unknown query fields and unbounded limits', () => {
    expect(() => auditEventListQuerySchema.parse({ limit: '201' })).toThrow();
    expect(() => auditEventListQuerySchema.parse({ order: 'ASC' })).toThrow();
  });

  it('keeps user and system actor facts explicit', () => {
    const base = {
      action: 'TASK_STARTED',
      details: {},
      eventId: '22222222-2222-4222-8222-222222222222',
      occurredAt: '2026-09-18T08:00:00.000Z',
      sequence: '2',
      subjectId: null,
      subjectType: 'TASK',
    };
    expect(
      auditEventSchema.parse({ ...base, actorType: 'SYSTEM', actorUserId: null }),
    ).toMatchObject({ actorType: 'SYSTEM', actorUserId: null });
    expect(() =>
      auditEventSchema.parse({ ...base, actorType: 'USER', actorUserId: null }),
    ).toThrow();
    expect(() =>
      auditEventSchema.parse({ ...base, actorType: 'SYSTEM', actorUserId: userId }),
    ).toThrow();
  });

  it('accepts a deployment administrator without inventing a user identity', () => {
    expect(
      auditEventListResponseSchema.parse({
        events: [
          {
            action: 'PLATFORM_INITIALIZED',
            actorType: 'DEPLOYMENT_ADMIN',
            actorUserId: null,
            details: {},
            eventId: '22222222-2222-4222-8222-222222222222',
            occurredAt: '2026-09-18T08:00:00.000Z',
            sequence: '1',
            subjectId: null,
            subjectType: 'PLATFORM',
          },
        ],
        nextCursor: null,
      }).events[0],
    ).toMatchObject({ actorType: 'DEPLOYMENT_ADMIN', actorUserId: null });
  });
});
