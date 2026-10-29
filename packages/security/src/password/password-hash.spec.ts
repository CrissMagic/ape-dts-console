import { describe, expect, it } from 'vitest';

import { hashPassword, verifyPassword, type Argon2idParameters } from '../index.js';

const TEST_KDF_PARAMETERS: Argon2idParameters = {
  memoryKiB: 1_024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};

describe('password hashing', () => {
  it('stores the explicit policy and verifies without retaining the plaintext', async () => {
    const record = await hashPassword('a long administrator password', TEST_KDF_PARAMETERS);

    expect(record).toMatchObject({ algorithm: 'ARGON2ID', parameters: TEST_KDF_PARAMETERS });
    expect(JSON.stringify(record)).not.toContain('administrator password');
    await expect(verifyPassword('a long administrator password', record)).resolves.toBe(true);
    await expect(verifyPassword('different password', record)).resolves.toBe(false);
  });
});
