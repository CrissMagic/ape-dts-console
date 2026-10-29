import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export interface TotpGenerationOptions {
  readonly algorithm: 'sha1' | 'sha256' | 'sha512';
  readonly digits: 6 | 8;
  readonly periodSeconds: number;
}

export interface TotpVerificationOptions extends TotpGenerationOptions {
  readonly window: number;
}

export const PLATFORM_TOTP_OPTIONS: TotpGenerationOptions = {
  algorithm: 'sha1',
  digits: 6,
  periodSeconds: 30,
};

const BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

function encodeBase32(bytes: Uint8Array): string {
  let buffer = 0;
  let bits = 0;
  let output = '';
  for (const byte of bytes) {
    buffer = (buffer << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      output += BASE32_ALPHABET[(buffer >>> bits) & 31];
    }
  }
  if (bits > 0) {
    output += BASE32_ALPHABET[(buffer << (5 - bits)) & 31];
  }
  return output;
}

function assertOptions(options: TotpGenerationOptions): void {
  if (!Number.isSafeInteger(options.periodSeconds) || options.periodSeconds <= 0) {
    throw new Error('INVALID_TOTP_PERIOD');
  }
}

function codeForCounter(
  secret: Uint8Array,
  counter: number,
  options: TotpGenerationOptions,
): string {
  if (!Number.isSafeInteger(counter) || counter < 0) {
    throw new Error('INVALID_TOTP_COUNTER');
  }
  const counterBytes = Buffer.alloc(8);
  counterBytes.writeBigUInt64BE(BigInt(counter));
  const digest = createHmac(options.algorithm, secret).update(counterBytes).digest();
  const offset = (digest.at(-1) ?? 0) & 0x0f;
  const binary = (digest.readUInt32BE(offset) & 0x7fff_ffff) % 10 ** options.digits;
  return binary.toString().padStart(options.digits, '0');
}

export function generateTotp(
  secret: Uint8Array,
  unixTimeMs: number,
  options: TotpGenerationOptions = PLATFORM_TOTP_OPTIONS,
): string {
  assertOptions(options);
  const counter = Math.floor(unixTimeMs / 1_000 / options.periodSeconds);
  return codeForCounter(secret, counter, options);
}

export function verifyTotp(
  secret: Uint8Array,
  submittedCode: string,
  unixTimeMs: number,
  options: TotpVerificationOptions,
): number | null {
  assertOptions(options);
  if (!Number.isSafeInteger(options.window) || options.window < 0) {
    throw new Error('INVALID_TOTP_WINDOW');
  }
  if (!new RegExp(`^\\d{${options.digits}}$`, 'u').test(submittedCode)) {
    return null;
  }
  const submitted = Buffer.from(submittedCode, 'ascii');
  const currentCounter = Math.floor(unixTimeMs / 1_000 / options.periodSeconds);
  for (let delta = -options.window; delta <= options.window; delta += 1) {
    const counter = currentCounter + delta;
    if (counter < 0) {
      continue;
    }
    const expected = Buffer.from(codeForCounter(secret, counter, options), 'ascii');
    if (timingSafeEqual(submitted, expected)) {
      return counter;
    }
  }
  return null;
}

export function generateTotpSecret(): Buffer {
  return randomBytes(20);
}

export function createTotpUri(account: string, issuer: string, secret: Uint8Array): string {
  if (account.length === 0 || issuer.length === 0 || secret.byteLength < 20) {
    throw new Error('INVALID_TOTP_ENROLLMENT');
  }
  const label = encodeURIComponent(`${issuer}:${account}`);
  const parameters = new URLSearchParams({
    algorithm: PLATFORM_TOTP_OPTIONS.algorithm.toUpperCase(),
    digits: String(PLATFORM_TOTP_OPTIONS.digits),
    issuer,
    period: String(PLATFORM_TOTP_OPTIONS.periodSeconds),
    secret: encodeBase32(secret),
  });
  return `otpauth://totp/${label}?${parameters.toString()}`;
}
