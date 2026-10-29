import { describe, expect, it } from 'vitest';

import {
  BootstrapAccessError,
  BootstrapAccessService,
  type BootstrapAccessPolicy,
} from './bootstrap-access.service.js';

const policy: BootstrapAccessPolicy = {
  failureWindowMs: 60_000,
  lockoutMs: 30_000,
  maxFailures: 3,
  sessionTtlMs: 300_000,
};

describe('BootstrapAccessService', () => {
  it('invalidates the one-time initialization code after issuing one memory-only session', () => {
    let now = 1_000;
    const service = new BootstrapAccessService('ABCD-EF12-3456', policy, () => now);

    const session = service.verifyInitializationCode('abcd-ef12-3456');
    expect(service.authorizeSession(session.sessionToken)).toBe(true);
    expect(() => service.verifyInitializationCode('ABCD-EF12-3456')).toThrowError(
      expect.objectContaining({ code: 'INITIALIZATION_CODE_ALREADY_USED' }),
    );

    now = session.expiresAtMs + 1;
    expect(service.authorizeSession(session.sessionToken)).toBe(false);
  });

  it('rate limits failures without changing the valid code', () => {
    let now = 10_000;
    const service = new BootstrapAccessService('ABCD-EF12-3456', policy, () => now);

    for (const attempt of ['wrong-1', 'wrong-2']) {
      expect(() => service.verifyInitializationCode(attempt)).toThrowError(
        expect.objectContaining({ code: 'INVALID_INITIALIZATION_CODE' }),
      );
    }
    expect(() => service.verifyInitializationCode('wrong-3')).toThrowError(
      expect.objectContaining<Partial<BootstrapAccessError>>({ code: 'BOOTSTRAP_RATE_LIMITED' }),
    );
    expect(() => service.verifyInitializationCode('ABCD-EF12-3456')).toThrowError(
      expect.objectContaining({ code: 'BOOTSTRAP_RATE_LIMITED' }),
    );

    now += policy.lockoutMs + 1;
    expect(service.verifyInitializationCode('ABCD-EF12-3456').sessionToken).toHaveLength(43);
  });

  it('explicitly invalidates the bootstrap session when initialization completes', () => {
    const service = new BootstrapAccessService('ABCD-EF12-3456', policy, () => 1_000);
    const { sessionToken } = service.verifyInitializationCode('ABCD-EF12-3456');

    service.invalidateSession();

    expect(service.authorizeSession(sessionToken)).toBe(false);
  });
});
