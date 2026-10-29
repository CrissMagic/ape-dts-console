import { randomBytes, timingSafeEqual } from 'node:crypto';

import { deriveArgon2idKey, type Argon2idParameters } from '../kdf/argon2id.js';

export interface PasswordHashRecord {
  readonly algorithm: 'ARGON2ID';
  readonly hash: string;
  readonly parameters: Argon2idParameters;
  readonly salt: string;
}

export async function hashPassword(
  password: string,
  parameters: Argon2idParameters,
): Promise<PasswordHashRecord> {
  const salt = randomBytes(16);
  const hash = await deriveArgon2idKey(password, salt, parameters);
  return {
    algorithm: 'ARGON2ID',
    hash: hash.toString('base64'),
    parameters: { ...parameters },
    salt: salt.toString('base64'),
  };
}

export async function verifyPassword(
  password: string,
  record: PasswordHashRecord,
): Promise<boolean> {
  const expected = Buffer.from(record.hash, 'base64');
  const actual = await deriveArgon2idKey(
    password,
    Buffer.from(record.salt, 'base64'),
    record.parameters,
  );
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}
