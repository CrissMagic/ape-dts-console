import { expect, it } from 'vitest';
import { recoveryFailureReason } from './recovery-failure.js';

it('keeps safe driver and wrapped failure reasons without exposing exception text', () => {
  expect(
    recoveryFailureReason(Object.assign(new Error('database unavailable'), { code: '53300' })),
  ).toBe('53300');
  expect(
    recoveryFailureReason(
      Object.assign(new Error('RUNTIME_CONTINUITY_BLOCKED'), { details: { reason: 'ETIMEDOUT' } }),
    ),
  ).toBe('ETIMEDOUT');
  expect(recoveryFailureReason(new Error('timeout exceeded when trying to connect'))).toBe(
    'METADATA_POOL_ACQUIRE_TIMEOUT',
  );
  expect(recoveryFailureReason(new Error('Query read timeout'))).toBe('METADATA_QUERY_TIMEOUT');
  expect(
    recoveryFailureReason(
      new Error('connection postgres://user:private@localhost failed', {
        cause: Object.assign(new Error('failed'), { code: 'ECONNRESET' }),
      }),
    ),
  ).toBe('ECONNRESET');
  expect(recoveryFailureReason(new Error('password=private'))).toBe('RUNTIME_RECOVERY_FAILED');
});
