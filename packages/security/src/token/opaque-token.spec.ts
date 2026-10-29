import { describe, expect, it } from 'vitest';

import {
  generateOpaqueToken,
  generateRecoveryCodes,
  hashOpaqueToken,
  hashRecoveryCode,
  verifyOpaqueToken,
  verifyRecoveryCode,
} from './opaque-token.js';

describe('opaque security tokens', () => {
  it('stores only a one-way digest for high-entropy activation and session tokens', () => {
    const token = generateOpaqueToken('scs-at-v1_', 32);
    const digest = hashOpaqueToken(token);

    expect(token).toMatch(/^scs-at-v1_[A-Za-z0-9_-]{43}$/u);
    expect(digest).toHaveLength(32);
    expect(digest.toString('utf8')).not.toContain(token);
    expect(verifyOpaqueToken(token, digest)).toBe(true);
    expect(verifyOpaqueToken(`${token}x`, digest)).toBe(false);
  });

  it('creates unique recovery codes whose independently salted hashes cannot be reused', () => {
    const codes = generateRecoveryCodes(10);
    const first = hashRecoveryCode(codes[0]!);
    const sameCodeAgain = hashRecoveryCode(codes[0]!);

    expect(new Set(codes)).toHaveLength(10);
    expect(codes.every((code) => /^scs-rc-v1_[A-Za-z0-9_-]{22}$/u.test(code))).toBe(true);
    expect(first.salt).not.toEqual(sameCodeAgain.salt);
    expect(first.hash).not.toEqual(sameCodeAgain.hash);
    expect(verifyRecoveryCode(codes[0]!, first)).toBe(true);
    expect(verifyRecoveryCode(codes[1]!, first)).toBe(false);
  });
});
