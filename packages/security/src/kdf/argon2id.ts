import { argon2 } from 'node:crypto';

export interface Argon2idParameters {
  readonly memoryKiB: number;
  readonly parallelism: number;
  readonly passes: number;
  readonly tagLength: number;
  readonly version: 1;
}

export class InvalidKdfParametersError extends Error {
  readonly code = 'INVALID_KDF_PARAMETERS';

  constructor(message: string) {
    super(message);
    this.name = 'InvalidKdfParametersError';
  }
}

function assertInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value)) {
    throw new InvalidKdfParametersError(`${name} must be a safe integer`);
  }
}

export function validateArgon2idParameters(parameters: Argon2idParameters): void {
  assertInteger('memoryKiB', parameters.memoryKiB);
  assertInteger('parallelism', parameters.parallelism);
  assertInteger('passes', parameters.passes);
  assertInteger('tagLength', parameters.tagLength);

  if (parameters.version !== 1) {
    throw new InvalidKdfParametersError('unsupported Argon2id policy version');
  }
  if (parameters.parallelism < 2 || parameters.parallelism >= 2 ** 24 - 1) {
    throw new InvalidKdfParametersError('parallelism is outside the supported range');
  }
  if (parameters.memoryKiB <= 8 * parameters.parallelism) {
    throw new InvalidKdfParametersError('memoryKiB must exceed 8 times parallelism');
  }
  if (parameters.passes < 2) {
    throw new InvalidKdfParametersError('passes must be at least 2');
  }
  if (parameters.tagLength < 32) {
    throw new InvalidKdfParametersError('tagLength must be at least 32 bytes');
  }
}

export async function deriveArgon2idKey(
  message: string | Uint8Array,
  salt: Uint8Array,
  parameters: Argon2idParameters,
): Promise<Buffer> {
  validateArgon2idParameters(parameters);

  const messageBytes =
    typeof message === 'string' ? Buffer.from(message, 'utf8') : Buffer.from(message);
  const saltBytes = Buffer.from(salt);
  if (messageBytes.length === 0) {
    throw new InvalidKdfParametersError('message must not be empty');
  }
  if (saltBytes.length < 16) {
    throw new InvalidKdfParametersError('salt must be at least 16 bytes');
  }

  return await new Promise<Buffer>((resolve, reject) => {
    argon2(
      'argon2id',
      {
        memory: parameters.memoryKiB,
        message: messageBytes,
        nonce: saltBytes,
        parallelism: parameters.parallelism,
        passes: parameters.passes,
        tagLength: parameters.tagLength,
      },
      (error, derivedKey) => {
        if (error !== null) {
          reject(error);
          return;
        }
        resolve(Buffer.from(derivedKey));
      },
    );
  });
}
