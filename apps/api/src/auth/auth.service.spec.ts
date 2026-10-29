import { describe, expect, it } from 'vitest';

import {
  authenticationFingerprint,
  authorizationSubject,
  hasPlatformCapability,
  nextAuthenticationFailure,
  sessionDeadlines,
} from './auth-policy.js';

describe('authentication policy', () => {
  it('defaults to an 8 hour idle and 7 day absolute session window', () => {
    const now = new Date('2026-09-01T00:00:00.000Z');

    expect(sessionDeadlines(now)).toEqual({
      absoluteExpiresAt: new Date('2026-09-08T00:00:00.000Z'),
      idleExpiresAt: new Date('2026-09-01T08:00:00.000Z'),
    });
  });

  it('creates deadlines from the policy captured for a new login', () => {
    const now = new Date('2026-09-01T00:00:00.000Z');
    expect(sessionDeadlines(now, { idleTimeoutMinutes: 90, absoluteTimeoutHours: 48 })).toEqual({
      idleExpiresAt: new Date('2026-09-01T01:30:00.000Z'),
      absoluteExpiresAt: new Date('2026-09-03T00:00:00.000Z'),
    });
  });

  it('keeps platform capabilities explicit without inventing task authorization', () => {
    const administrator = authorizationSubject({
      displayName: 'Administrator',
      id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3',
      role: 'ADMIN',
      status: 'ACTIVE',
      username: 'admin',
    });
    const developer = { ...administrator, role: 'DEVELOPER' as const };

    expect(hasPlatformCapability(administrator, 'MANAGE_USERS')).toBe(true);
    expect(hasPlatformCapability(developer, 'VIEW_DASHBOARD')).toBe(true);
    expect(hasPlatformCapability(developer, 'MANAGE_USERS')).toBe(false);
  });

  it('rate limits an account and source together with bounded backoff', () => {
    const now = new Date('2026-09-01T00:00:00.000Z');
    const fifth = nextAuthenticationFailure(4, now, now);
    const muchLater = nextAuthenticationFailure(20, now, now);

    expect(authenticationFingerprint('Admin')).toHaveLength(32);
    expect(authenticationFingerprint('Admin')).not.toEqual(
      authenticationFingerprint('other-source'),
    );
    expect(fifth).toMatchObject({
      blockedUntil: new Date('2026-09-01T00:00:30.000Z'),
      failureCount: 5,
    });
    expect(muchLater.blockedUntil).toEqual(new Date('2026-09-01T00:05:00.000Z'));
  });
});
