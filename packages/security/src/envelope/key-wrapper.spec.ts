import { describe, expect, it } from 'vitest';

import {
  createInitialKeyMaterial,
  createPassphraseWrapper,
  decryptSecret,
  encryptSecret,
  unwrapKey,
  type Argon2idParameters,
} from '../index.js';

const TEST_KDF_PARAMETERS: Argon2idParameters = {
  memoryKiB: 1_024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};

describe('DEK key wrappers', () => {
  it('unwraps the same DEK through the passphrase and offline recovery key', async () => {
    const material = await createInitialKeyMaterial(
      'correct horse battery staple',
      TEST_KDF_PARAMETERS,
    );

    const fromPassphrase = await unwrapKey(material.passphraseWrapper, {
      kind: 'PASSPHRASE',
      value: 'correct horse battery staple',
    });
    const fromRecovery = await unwrapKey(material.recoveryWrapper, {
      kind: 'RECOVERY',
      value: material.recoveryKey,
    });

    expect(fromPassphrase).toEqual(material.dek);
    expect(fromRecovery).toEqual(material.dek);
  });

  it('rejects an incorrect passphrase without returning partial key material', async () => {
    const material = await createInitialKeyMaterial('right passphrase', TEST_KDF_PARAMETERS);

    await expect(
      unwrapKey(material.passphraseWrapper, {
        kind: 'PASSPHRASE',
        value: 'wrong passphrase',
      }),
    ).rejects.toMatchObject({ code: 'INVALID_KEY_MATERIAL' });
  });

  it('rewraps the DEK without changing an existing business-secret ciphertext', async () => {
    const material = await createInitialKeyMaterial('old passphrase', TEST_KDF_PARAMETERS);
    const secret = encryptSecret(material.dek, Buffer.from('database-password'), {
      aadVersion: 1,
      ownerId: '018f6d9f-f4a7-7c34-bd42-3bc7f3324db2',
      ownerType: 'DATA_SOURCE',
      purpose: 'PASSWORD',
      secretId: '018f6da0-2bb9-7904-a5ec-6bd17f0ffac6',
    });
    const serializedBefore = JSON.stringify(secret);

    const replacement = await createPassphraseWrapper(
      material.dek,
      'new passphrase',
      TEST_KDF_PARAMETERS,
    );
    const unwrapped = await unwrapKey(replacement, {
      kind: 'PASSPHRASE',
      value: 'new passphrase',
    });

    expect(JSON.stringify(secret)).toBe(serializedBefore);
    expect(
      decryptSecret(unwrapped, secret, {
        aadVersion: 1,
        ownerId: '018f6d9f-f4a7-7c34-bd42-3bc7f3324db2',
        ownerType: 'DATA_SOURCE',
        purpose: 'PASSWORD',
        secretId: '018f6da0-2bb9-7904-a5ec-6bd17f0ffac6',
      }).toString('utf8'),
    ).toBe('database-password');
  });

  it('binds secret ciphertext to its owner and purpose through AAD', async () => {
    const material = await createInitialKeyMaterial('passphrase', TEST_KDF_PARAMETERS);
    const context = {
      aadVersion: 1 as const,
      ownerId: '018f6d9f-f4a7-7c34-bd42-3bc7f3324db2',
      ownerType: 'DATA_SOURCE',
      purpose: 'PASSWORD',
      secretId: '018f6da0-2bb9-7904-a5ec-6bd17f0ffac6',
    };
    const secret = encryptSecret(material.dek, Buffer.from('secret'), context);

    expect(() =>
      decryptSecret(material.dek, secret, { ...context, purpose: 'ACCESS_KEY' }),
    ).toThrow('INVALID_SECRET_CIPHERTEXT');
  });
});
