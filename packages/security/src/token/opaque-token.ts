import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

export interface RecoveryCodeHashRecord {
  readonly hash: Buffer;
  readonly salt: Buffer;
}

export function generateOpaqueToken(prefix: string, byteLength: number): string {
  if (
    prefix.length === 0 ||
    !/^[A-Za-z0-9_-]+$/u.test(prefix) ||
    !Number.isSafeInteger(byteLength) ||
    byteLength < 16
  ) {
    throw new Error('OPAQUE_TOKEN_POLICY_INVALID');
  }
  return `${prefix}${randomBytes(byteLength).toString('base64url')}`;
}

export function hashOpaqueToken(token: string): Buffer {
  if (token.length === 0) {
    throw new Error('OPAQUE_TOKEN_INVALID');
  }
  return createHash('sha256').update(token, 'utf8').digest();
}

export function verifyOpaqueToken(token: string, expectedHash: Uint8Array): boolean {
  if (expectedHash.byteLength !== 32) {
    return false;
  }
  const actual = hashOpaqueToken(token);
  return timingSafeEqual(actual, expectedHash);
}

export function generateRecoveryCodes(count: number): readonly string[] {
  if (!Number.isSafeInteger(count) || count <= 0 || count > 32) {
    throw new Error('RECOVERY_CODE_COUNT_INVALID');
  }
  return Array.from({ length: count }, () => generateOpaqueToken('scs-rc-v1_', 16));
}

export function hashRecoveryCode(code: string): RecoveryCodeHashRecord {
  if (!/^scs-rc-v1_[A-Za-z0-9_-]{22}$/u.test(code)) {
    throw new Error('RECOVERY_CODE_INVALID');
  }
  const salt = randomBytes(16);
  return {
    hash: createHash('sha256').update(salt).update(code, 'utf8').digest(),
    salt,
  };
}

export function verifyRecoveryCode(code: string, record: RecoveryCodeHashRecord): boolean {
  if (
    record.salt.byteLength < 16 ||
    record.hash.byteLength !== 32 ||
    !/^scs-rc-v1_[A-Za-z0-9_-]{22}$/u.test(code)
  ) {
    return false;
  }
  const actual = createHash('sha256').update(record.salt).update(code, 'utf8').digest();
  return timingSafeEqual(actual, record.hash);
}
