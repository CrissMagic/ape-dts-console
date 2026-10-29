import { createCipheriv, createDecipheriv, hkdfSync, randomBytes } from 'node:crypto';

import { deriveArgon2idKey, type Argon2idParameters } from '../kdf/argon2id.js';

const DEK_LENGTH = 32;
const KEK_LENGTH = 32;
const GCM_NONCE_LENGTH = 12;
const GCM_TAG_LENGTH = 16;
const RECOVERY_KEY_LENGTH = 32;
const RECOVERY_KEY_PREFIX = 'scs-rk-v1_';
const RECOVERY_HKDF_INFO = 'scs-data-sync-platform:recovery-wrapper:v1';

export type KeyWrapperType = 'PASSPHRASE' | 'RECOVERY';

export type KeyDerivationDescriptor =
  | {
      readonly algorithm: 'ARGON2ID';
      readonly parameters: Argon2idParameters;
      readonly salt: string;
    }
  | {
      readonly algorithm: 'HKDF_SHA256';
      readonly info: typeof RECOVERY_HKDF_INFO;
      readonly salt: string;
    };

export interface WrappedKey {
  readonly cipherAlgorithm: 'AES_256_GCM';
  readonly ciphertext: string;
  readonly kdf: KeyDerivationDescriptor;
  readonly nonce: string;
  readonly tag: string;
  readonly version: 1;
  readonly wrapperType: KeyWrapperType;
}

export type KeyMaterial =
  | { readonly kind: 'PASSPHRASE'; readonly value: string }
  | { readonly kind: 'RECOVERY'; readonly value: string };

export interface InitialKeyMaterial {
  readonly dek: Buffer;
  readonly passphraseWrapper: WrappedKey;
  readonly recoveryKey: string;
  readonly recoveryWrapper: WrappedKey;
}

export class KeyUnwrapError extends Error {
  readonly code = 'INVALID_KEY_MATERIAL';

  constructor() {
    super('INVALID_KEY_MATERIAL');
    this.name = 'KeyUnwrapError';
  }
}

function wrapperAad(wrapperType: KeyWrapperType): Buffer {
  return Buffer.from(`scs-data-sync-platform:key-wrapper:v1:${wrapperType}`, 'utf8');
}

function encryptDek(dek: Uint8Array, kek: Uint8Array, wrapperType: KeyWrapperType) {
  if (dek.byteLength !== DEK_LENGTH || kek.byteLength !== KEK_LENGTH) {
    throw new Error('INVALID_KEY_LENGTH');
  }

  const nonce = randomBytes(GCM_NONCE_LENGTH);
  const cipher = createCipheriv('aes-256-gcm', kek, nonce, { authTagLength: GCM_TAG_LENGTH });
  cipher.setAAD(wrapperAad(wrapperType), { plaintextLength: DEK_LENGTH });
  const ciphertext = Buffer.concat([cipher.update(dek), cipher.final()]);

  return {
    ciphertext: ciphertext.toString('base64'),
    nonce: nonce.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
  };
}

function decryptDek(wrapper: WrappedKey, kek: Uint8Array): Buffer {
  const nonce = Buffer.from(wrapper.nonce, 'base64');
  const ciphertext = Buffer.from(wrapper.ciphertext, 'base64');
  const tag = Buffer.from(wrapper.tag, 'base64');
  const decipher = createDecipheriv('aes-256-gcm', kek, nonce, {
    authTagLength: GCM_TAG_LENGTH,
  });
  decipher.setAAD(wrapperAad(wrapper.wrapperType), { plaintextLength: DEK_LENGTH });
  decipher.setAuthTag(tag);
  const dek = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  if (dek.length !== DEK_LENGTH) {
    throw new Error('INVALID_DEK_LENGTH');
  }
  return dek;
}

function encodeRecoveryKey(rawKey: Uint8Array): string {
  return `${RECOVERY_KEY_PREFIX}${Buffer.from(rawKey).toString('base64url')}`;
}

function decodeRecoveryKey(encodedKey: string): Buffer {
  if (!encodedKey.startsWith(RECOVERY_KEY_PREFIX)) {
    throw new KeyUnwrapError();
  }
  const rawKey = Buffer.from(encodedKey.slice(RECOVERY_KEY_PREFIX.length), 'base64url');
  if (rawKey.length !== RECOVERY_KEY_LENGTH) {
    throw new KeyUnwrapError();
  }
  return rawKey;
}

function deriveRecoveryKek(rawKey: Uint8Array, salt: Uint8Array): Buffer {
  return Buffer.from(
    hkdfSync('sha256', rawKey, salt, Buffer.from(RECOVERY_HKDF_INFO, 'utf8'), KEK_LENGTH),
  );
}

export async function createPassphraseWrapper(
  dek: Uint8Array,
  passphrase: string,
  parameters: Argon2idParameters,
): Promise<WrappedKey> {
  const salt = randomBytes(16);
  const kek = await deriveArgon2idKey(passphrase, salt, parameters);
  try {
    return {
      cipherAlgorithm: 'AES_256_GCM',
      ...encryptDek(dek, kek, 'PASSPHRASE'),
      kdf: {
        algorithm: 'ARGON2ID',
        parameters: { ...parameters },
        salt: salt.toString('base64'),
      },
      version: 1,
      wrapperType: 'PASSPHRASE',
    };
  } finally {
    kek.fill(0);
  }
}

export function createRecoveryWrapper(dek: Uint8Array, recoveryKey: string): WrappedKey {
  const rawKey = decodeRecoveryKey(recoveryKey);
  const salt = randomBytes(32);
  const kek = deriveRecoveryKek(rawKey, salt);
  try {
    return {
      cipherAlgorithm: 'AES_256_GCM',
      ...encryptDek(dek, kek, 'RECOVERY'),
      kdf: {
        algorithm: 'HKDF_SHA256',
        info: RECOVERY_HKDF_INFO,
        salt: salt.toString('base64'),
      },
      version: 1,
      wrapperType: 'RECOVERY',
    };
  } finally {
    rawKey.fill(0);
    kek.fill(0);
  }
}

export async function createInitialKeyMaterial(
  passphrase: string,
  parameters: Argon2idParameters,
): Promise<InitialKeyMaterial> {
  const dek = randomBytes(DEK_LENGTH);
  const recoveryKey = encodeRecoveryKey(randomBytes(RECOVERY_KEY_LENGTH));
  const passphraseWrapper = await createPassphraseWrapper(dek, passphrase, parameters);
  const recoveryWrapper = createRecoveryWrapper(dek, recoveryKey);
  return { dek, passphraseWrapper, recoveryKey, recoveryWrapper };
}

export async function unwrapKey(wrapper: WrappedKey, material: KeyMaterial): Promise<Buffer> {
  if (wrapper.version !== 1 || wrapper.cipherAlgorithm !== 'AES_256_GCM') {
    throw new KeyUnwrapError();
  }
  if (wrapper.wrapperType !== material.kind) {
    throw new KeyUnwrapError();
  }

  let kek: Buffer | undefined;
  try {
    if (wrapper.kdf.algorithm === 'ARGON2ID' && material.kind === 'PASSPHRASE') {
      kek = await deriveArgon2idKey(
        material.value,
        Buffer.from(wrapper.kdf.salt, 'base64'),
        wrapper.kdf.parameters,
      );
    } else if (wrapper.kdf.algorithm === 'HKDF_SHA256' && material.kind === 'RECOVERY') {
      const rawKey = decodeRecoveryKey(material.value);
      try {
        kek = deriveRecoveryKek(rawKey, Buffer.from(wrapper.kdf.salt, 'base64'));
      } finally {
        rawKey.fill(0);
      }
    } else {
      throw new KeyUnwrapError();
    }
    return decryptDek(wrapper, kek);
  } catch {
    throw new KeyUnwrapError();
  } finally {
    kek?.fill(0);
  }
}

export function generateRecoveryKey(): string {
  return encodeRecoveryKey(randomBytes(RECOVERY_KEY_LENGTH));
}
