import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

const GCM_NONCE_LENGTH = 12;
const GCM_TAG_LENGTH = 16;

export interface SecretAadContext {
  readonly aadVersion: 1;
  readonly ownerId: string;
  readonly ownerType: string;
  readonly purpose: string;
  readonly secretId: string;
}

export interface EncryptedSecret {
  readonly aadVersion: 1;
  readonly algorithm: 'AES_256_GCM';
  readonly ciphertext: string;
  readonly nonce: string;
  readonly tag: string;
}

export class SecretCiphertextError extends Error {
  readonly code = 'INVALID_SECRET_CIPHERTEXT';

  constructor() {
    super('INVALID_SECRET_CIPHERTEXT');
    this.name = 'SecretCiphertextError';
  }
}

function buildAad(context: SecretAadContext): Buffer {
  const values = [
    'ape_dts_platform',
    context.aadVersion,
    context.ownerType,
    context.ownerId,
    context.purpose,
    context.secretId,
  ];
  if (values.some((value) => String(value).length === 0)) {
    throw new Error('INVALID_SECRET_AAD');
  }
  return Buffer.from(JSON.stringify(values), 'utf8');
}

export function encryptSecret(
  dek: Uint8Array,
  plaintext: Uint8Array,
  context: SecretAadContext,
): EncryptedSecret {
  if (dek.byteLength !== 32) {
    throw new Error('INVALID_DEK_LENGTH');
  }
  const nonce = randomBytes(GCM_NONCE_LENGTH);
  const cipher = createCipheriv('aes-256-gcm', dek, nonce, { authTagLength: GCM_TAG_LENGTH });
  cipher.setAAD(buildAad(context), { plaintextLength: plaintext.byteLength });
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  return {
    aadVersion: 1,
    algorithm: 'AES_256_GCM',
    ciphertext: ciphertext.toString('base64'),
    nonce: nonce.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
  };
}

export function decryptSecret(
  dek: Uint8Array,
  encrypted: EncryptedSecret,
  context: SecretAadContext,
): Buffer {
  try {
    if (
      dek.byteLength !== 32 ||
      encrypted.algorithm !== 'AES_256_GCM' ||
      encrypted.aadVersion !== context.aadVersion
    ) {
      throw new Error('invalid metadata');
    }
    const ciphertext = Buffer.from(encrypted.ciphertext, 'base64');
    const decipher = createDecipheriv('aes-256-gcm', dek, Buffer.from(encrypted.nonce, 'base64'), {
      authTagLength: GCM_TAG_LENGTH,
    });
    decipher.setAAD(buildAad(context), { plaintextLength: ciphertext.length });
    decipher.setAuthTag(Buffer.from(encrypted.tag, 'base64'));
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch {
    throw new SecretCiphertextError();
  }
}
