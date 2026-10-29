import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export interface BootstrapAccessPolicy {
  readonly failureWindowMs: number;
  readonly lockoutMs: number;
  readonly maxFailures: number;
  readonly sessionTtlMs: number;
}

export const PLATFORM_BOOTSTRAP_ACCESS_POLICY: BootstrapAccessPolicy = {
  failureWindowMs: 60_000,
  lockoutMs: 60_000,
  maxFailures: 5,
  sessionTtlMs: 30 * 60_000,
};

export type BootstrapAccessErrorCode =
  'BOOTSTRAP_RATE_LIMITED' | 'INITIALIZATION_CODE_ALREADY_USED' | 'INVALID_INITIALIZATION_CODE';

export class BootstrapAccessError extends Error {
  constructor(
    readonly code: BootstrapAccessErrorCode,
    readonly retryAfterMs?: number,
  ) {
    super(code);
    this.name = 'BootstrapAccessError';
  }
}

export interface BootstrapSession {
  readonly expiresAtMs: number;
  readonly sessionToken: string;
}

function normalizeInitializationCode(code: string): string {
  return code.trim().toUpperCase();
}

function validatePolicy(policy: BootstrapAccessPolicy): void {
  for (const [name, value] of Object.entries(policy)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`invalid bootstrap access policy: ${name}`);
    }
  }
}

export function generateInitializationCode(): string {
  const raw = randomBytes(10).toString('hex').toUpperCase();
  return raw.match(/.{1,4}/gu)?.join('-') ?? raw;
}

export class BootstrapAccessService {
  private readonly digestKey = randomBytes(32);
  private readonly failureTimes: number[] = [];
  private initializationCodeDigest: Buffer | undefined;
  private lockoutUntilMs = 0;
  private sessionDigest: Buffer | undefined;
  private sessionExpiresAtMs = 0;

  constructor(
    initializationCode: string,
    private readonly policy: BootstrapAccessPolicy,
    private readonly now: () => number = Date.now,
  ) {
    validatePolicy(policy);
    this.initializationCodeDigest = this.digest(normalizeInitializationCode(initializationCode));
  }

  private digest(value: string): Buffer {
    return createHmac('sha256', this.digestKey).update(value, 'utf8').digest();
  }

  private pruneFailures(nowMs: number): void {
    while (
      this.failureTimes[0] !== undefined &&
      this.failureTimes[0] <= nowMs - this.policy.failureWindowMs
    ) {
      this.failureTimes.shift();
    }
  }

  verifyInitializationCode(initializationCode: string): BootstrapSession {
    const nowMs = this.now();
    if (nowMs < this.lockoutUntilMs) {
      throw new BootstrapAccessError('BOOTSTRAP_RATE_LIMITED', this.lockoutUntilMs - nowMs);
    }
    if (this.initializationCodeDigest === undefined) {
      throw new BootstrapAccessError('INITIALIZATION_CODE_ALREADY_USED');
    }
    this.pruneFailures(nowMs);
    const submittedDigest = this.digest(normalizeInitializationCode(initializationCode));
    if (!timingSafeEqual(submittedDigest, this.initializationCodeDigest)) {
      this.failureTimes.push(nowMs);
      if (this.failureTimes.length >= this.policy.maxFailures) {
        this.lockoutUntilMs = nowMs + this.policy.lockoutMs;
        throw new BootstrapAccessError('BOOTSTRAP_RATE_LIMITED', this.policy.lockoutMs);
      }
      throw new BootstrapAccessError('INVALID_INITIALIZATION_CODE');
    }

    this.initializationCodeDigest.fill(0);
    this.initializationCodeDigest = undefined;
    this.failureTimes.length = 0;
    const sessionToken = randomBytes(32).toString('base64url');
    this.sessionDigest = this.digest(sessionToken);
    this.sessionExpiresAtMs = nowMs + this.policy.sessionTtlMs;
    return { expiresAtMs: this.sessionExpiresAtMs, sessionToken };
  }

  authorizeSession(sessionToken: string): boolean {
    if (
      this.sessionDigest === undefined ||
      this.now() > this.sessionExpiresAtMs ||
      sessionToken.length === 0
    ) {
      return false;
    }
    const submittedDigest = this.digest(sessionToken);
    return timingSafeEqual(submittedDigest, this.sessionDigest);
  }

  invalidateSession(): void {
    this.sessionDigest?.fill(0);
    this.sessionDigest = undefined;
    this.sessionExpiresAtMs = 0;
  }
}
