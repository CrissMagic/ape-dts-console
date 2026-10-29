import { describe, expect, it } from 'vitest';

import { deriveArgon2idKey, InvalidKdfParametersError } from '../index.js';

describe('Argon2id KDF', () => {
  it('requires an explicit valid policy and derives deterministic bytes for the same salt', async () => {
    const parameters = {
      memoryKiB: 1_024,
      parallelism: 2,
      passes: 2,
      tagLength: 32,
      version: 1 as const,
    };
    const salt = Buffer.alloc(16, 7);

    const first = await deriveArgon2idKey('passphrase', salt, parameters);
    const second = await deriveArgon2idKey('passphrase', salt, parameters);

    expect(first).toEqual(second);
    expect(first).toHaveLength(32);
  });

  it('rejects unsafe or malformed parameters instead of applying a fallback', async () => {
    await expect(
      deriveArgon2idKey('passphrase', Buffer.alloc(16), {
        memoryKiB: 16,
        parallelism: 2,
        passes: 1,
        tagLength: 16,
        version: 1,
      }),
    ).rejects.toBeInstanceOf(InvalidKdfParametersError);
  });
});
