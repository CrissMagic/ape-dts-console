import { describe, expect, it } from 'vitest';

import { createTotpUri, generateTotp, verifyTotp } from '../index.js';

const RFC_6238_SECRET = Buffer.from('12345678901234567890', 'ascii');

describe('TOTP', () => {
  it('matches the RFC 6238 SHA-1 vector and reports the accepted counter', () => {
    expect(
      generateTotp(RFC_6238_SECRET, 59_000, {
        algorithm: 'sha1',
        digits: 8,
        periodSeconds: 30,
      }),
    ).toBe('94287082');
    expect(
      verifyTotp(RFC_6238_SECRET, '94287082', 59_000, {
        algorithm: 'sha1',
        digits: 8,
        periodSeconds: 30,
        window: 0,
      }),
    ).toBe(1);
  });

  it('encodes an otpauth URI without exposing the secret outside the URI result', () => {
    const uri = createTotpUri('admin@example', 'SCS DTS', RFC_6238_SECRET);

    expect(uri).toContain('otpauth://totp/SCS%20DTS%3Aadmin%40example');
    expect(uri).toContain('issuer=SCS+DTS');
    expect(uri).toContain('algorithm=SHA1');
    expect(uri).toContain('digits=6');
    expect(uri).toContain('period=30');
  });
});
