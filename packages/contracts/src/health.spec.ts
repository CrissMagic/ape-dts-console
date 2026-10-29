import { describe, expect, it } from 'vitest';

import {
  liveHealthResponseSchema,
  readyHealthResponseSchema,
  unavailableHealthResponseSchema,
} from './health.js';

describe('health contracts', () => {
  it('accepts the explicit live and ready states', () => {
    expect(liveHealthResponseSchema.parse({ status: 'ok' })).toEqual({ status: 'ok' });
    expect(readyHealthResponseSchema.parse({ phase: 'READY', status: 'ready' })).toEqual({
      phase: 'READY',
      status: 'ready',
    });
  });

  it('does not allow READY to be represented as unavailable', () => {
    const result = unavailableHealthResponseSchema.safeParse({
      code: 'PLATFORM_NOT_READY',
      phase: 'READY',
      status: 'not_ready',
    });

    expect(result.success).toBe(false);
  });
});
