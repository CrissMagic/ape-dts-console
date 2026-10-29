import { describe, expect, it } from 'vitest';

import {
  runtimeLogQuerySchema,
  startTaskRuntimeRequestSchema,
  stopTaskRuntimeRequestSchema,
} from './index.js';
import { runtimePerformanceWindowSchema } from './performance.js';

const revisionId = '22222222-2222-4222-8222-222222222222';

describe('runtime request schemas', () => {
  it('accepts a start request without supervisor-owned runtime units', () => {
    expect(
      startTaskRuntimeRequestSchema.parse({
        expectedTaskRowVersion: '7',
        idempotencyKey: 'start-request-1',
        revisionId,
      }),
    ).toMatchObject({ expectedTaskRowVersion: '7', revisionId });
  });

  it('rejects client-supplied runtime unit identities or ports', () => {
    const base = {
      expectedTaskRowVersion: '7',
      idempotencyKey: 'start-request-1',
      revisionId,
    };
    expect(
      startTaskRuntimeRequestSchema.safeParse({
        ...base,
        units: [{ metricsPort: 19_001, unitKey: 'partition:0' }],
      }).success,
    ).toBe(false);
  });

  it('rejects a blank idempotency key instead of normalizing it to a default', () => {
    expect(
      stopTaskRuntimeRequestSchema.safeParse({
        expectedTaskRowVersion: '7',
        idempotencyKey: '   ',
      }).success,
    ).toBe(false);
  });

  it('accepts open log time bounds and rejects an empty or reversed range', () => {
    expect(runtimeLogQuerySchema.parse({ from: '2026-09-08T00:00:00.000Z' })).toMatchObject({
      from: '2026-09-08T00:00:00.000Z',
      limit: 500,
    });
    expect(runtimeLogQuerySchema.safeParse({ from: 'not-a-time' }).success).toBe(false);
    expect(
      runtimeLogQuerySchema.safeParse({
        from: '2026-09-08T01:00:00.000Z',
        to: '2026-09-08T01:00:00.000Z',
      }).success,
    ).toBe(false);
    expect(
      runtimeLogQuerySchema.safeParse({
        from: '2026-09-08T02:00:00.000Z',
        to: '2026-09-08T01:00:00.000Z',
      }).success,
    ).toBe(false);
  });

  it('limits performance history queries to the seven-day retention envelope', () => {
    expect(runtimePerformanceWindowSchema.options).toEqual(['1h', '24h', '7d']);
    expect(runtimePerformanceWindowSchema.safeParse('30d').success).toBe(false);
    expect(runtimePerformanceWindowSchema.safeParse('365d').success).toBe(false);
  });
});
