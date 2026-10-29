import { type Argon2idParameters, validateArgon2idParameters } from '@scs/security';

export const ARGON2ID_POLICY = Symbol('ARGON2ID_POLICY');

const ARGON2_ENV_KEYS = [
  'SECURITY_ARGON2_MEMORY_KIB',
  'SECURITY_ARGON2_PARALLELISM',
  'SECURITY_ARGON2_PASSES',
  'SECURITY_ARGON2_TAG_LENGTH',
] as const;

function parsePositiveInteger(environment: NodeJS.ProcessEnv, key: string): number {
  const raw = environment[key];
  if (raw === undefined || !/^\d+$/u.test(raw)) {
    throw new Error(`${key} must be a positive integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${key} must be a positive integer`);
  }
  return value;
}

export function readArgon2idPolicy(
  environment: NodeJS.ProcessEnv = process.env,
): Argon2idParameters | null {
  const configuredKeys = ARGON2_ENV_KEYS.filter((key) => environment[key] !== undefined);
  if (configuredKeys.length === 0) {
    return null;
  }
  if (configuredKeys.length !== ARGON2_ENV_KEYS.length) {
    throw new Error(`Argon2id policy is partially configured: ${configuredKeys.join(', ')}`);
  }
  const policy: Argon2idParameters = {
    memoryKiB: parsePositiveInteger(environment, 'SECURITY_ARGON2_MEMORY_KIB'),
    parallelism: parsePositiveInteger(environment, 'SECURITY_ARGON2_PARALLELISM'),
    passes: parsePositiveInteger(environment, 'SECURITY_ARGON2_PASSES'),
    tagLength: parsePositiveInteger(environment, 'SECURITY_ARGON2_TAG_LENGTH'),
    version: 1,
  };
  validateArgon2idParameters(policy);
  return policy;
}
