import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { Injectable } from '@nestjs/common';

export type EmergencyStopCodeErrorCode =
  'EMERGENCY_CODE_ALREADY_ACTIVE' | 'EMERGENCY_CODE_NOT_ACTIVE';

export class EmergencyStopCodeError extends Error {
  constructor(readonly code: EmergencyStopCodeErrorCode) {
    super(code);
    this.name = 'EmergencyStopCodeError';
  }
}

function normalize(value: string): string {
  return value.trim().toUpperCase();
}

@Injectable()
export class EmergencyStopCodeService {
  private readonly digestKey = randomBytes(32);
  private digestValue: Buffer | undefined;
  private plaintext: string | undefined;

  /** Called only after Bootstrap has succeeded. */
  activate(): void {
    if (this.digestValue !== undefined) {
      throw new EmergencyStopCodeError('EMERGENCY_CODE_ALREADY_ACTIVE');
    }
    const raw = randomBytes(15).toString('hex').toUpperCase();
    const code = raw.match(/.{1,5}/gu)?.join('-') ?? raw;
    this.digestValue = this.digest(normalize(code));
    this.plaintext = code;
  }

  /** The startup logger is the sole caller. Subsequent reads return undefined. */
  consumePlaintextOnce(): string | undefined {
    const code = this.plaintext;
    this.plaintext = undefined;
    return code;
  }

  verify(candidate: string): boolean {
    if (this.digestValue === undefined || candidate.length === 0) {
      return false;
    }
    const submitted = this.digest(normalize(candidate));
    return timingSafeEqual(submitted, this.digestValue);
  }

  clear(): void {
    this.digestValue?.fill(0);
    this.digestValue = undefined;
    this.plaintext = undefined;
  }

  private digest(value: string): Buffer {
    return createHmac('sha256', this.digestKey).update(value, 'utf8').digest();
  }
}
