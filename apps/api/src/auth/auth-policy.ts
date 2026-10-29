import { createHash } from 'node:crypto';
import type { UserSummary } from '@scs/contracts';

export const AUTH_SESSION_IDLE_MILLISECONDS = 8 * 60 * 60 * 1_000;
export const AUTH_SESSION_ABSOLUTE_MILLISECONDS = 7 * 24 * 60 * 60 * 1_000;
export const AUTH_CHALLENGE_MILLISECONDS = 5 * 60 * 1_000;
export const AUTH_ACTIVATION_MILLISECONDS = 24 * 60 * 60 * 1_000;
export const AUTH_FAILURE_WINDOW_MILLISECONDS = 15 * 60 * 1_000;
export const AUTH_MAX_FAILURES = 5;
export const AUTH_MAX_BACKOFF_MILLISECONDS = 5 * 60 * 1_000;
export const AUTH_RECOVERY_CODE_COUNT = 10;

export interface AuthenticationFailureState {
  readonly blockedUntil: Date | null;
  readonly failureCount: number;
  readonly windowStartedAt: Date;
}

export type PlatformCapability =
  'MANAGE_PLATFORM' | 'MANAGE_USERS' | 'VIEW_AUDIT' | 'VIEW_DASHBOARD';

export interface AuthorizationSubject {
  readonly role: UserSummary['role'];
  readonly status: UserSummary['status'];
  readonly userId: string;
}

export function authorizationSubject(user: UserSummary): AuthorizationSubject {
  return { role: user.role, status: user.status, userId: user.id };
}

export function hasPlatformCapability(
  subject: AuthorizationSubject,
  capability: PlatformCapability,
): boolean {
  if (subject.status !== 'ACTIVE') {
    return false;
  }
  if (capability === 'VIEW_DASHBOARD') {
    return true;
  }
  return subject.role === 'ADMIN';
}

export function authenticationFingerprint(value: string): Buffer {
  if (value.length === 0) {
    throw new Error('AUTHENTICATION_FINGERPRINT_INPUT_INVALID');
  }
  return createHash('sha256').update(value, 'utf8').digest();
}

export function sessionDeadlines(
  now: Date,
  policy = {
    idleTimeoutMinutes: AUTH_SESSION_IDLE_MILLISECONDS / 60_000,
    absoluteTimeoutHours: AUTH_SESSION_ABSOLUTE_MILLISECONDS / 3_600_000,
  },
): {
  readonly absoluteExpiresAt: Date;
  readonly idleExpiresAt: Date;
} {
  return {
    absoluteExpiresAt: new Date(now.getTime() + policy.absoluteTimeoutHours * 60 * 60 * 1_000),
    idleExpiresAt: new Date(now.getTime() + policy.idleTimeoutMinutes * 60 * 1_000),
  };
}

export function nextAuthenticationFailure(
  previousFailureCount: number,
  previousWindowStartedAt: Date,
  now: Date,
): AuthenticationFailureState {
  const withinWindow =
    now.getTime() - previousWindowStartedAt.getTime() < AUTH_FAILURE_WINDOW_MILLISECONDS;
  const failureCount = withinWindow ? previousFailureCount + 1 : 1;
  const windowStartedAt = withinWindow ? previousWindowStartedAt : now;
  if (failureCount < AUTH_MAX_FAILURES) {
    return { blockedUntil: null, failureCount, windowStartedAt };
  }
  const exponent = Math.min(failureCount - AUTH_MAX_FAILURES, 20);
  const backoff = Math.min(30_000 * 2 ** exponent, AUTH_MAX_BACKOFF_MILLISECONDS);
  return {
    blockedUntil: new Date(now.getTime() + backoff),
    failureCount,
    windowStartedAt,
  };
}
