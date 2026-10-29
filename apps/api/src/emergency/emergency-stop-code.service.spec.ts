import { describe, expect, it } from 'vitest';

import { EmergencyStopCodeService } from './emergency-stop-code.service.js';

describe('EmergencyStopCodeService', () => {
  it('keeps only a verifier after exposing the process code once', () => {
    const service = new EmergencyStopCodeService();
    service.activate();

    const code = service.consumePlaintextOnce();
    expect(code).toMatch(/^[0-9A-F]{5}(?:-[0-9A-F]{5}){5}$/u);
    expect(service.consumePlaintextOnce()).toBeUndefined();
    expect(service.verify(code!)).toBe(true);
    expect(service.verify('wrong-code')).toBe(false);
  });

  it('invalidates the verifier when the process lifecycle clears it', () => {
    const service = new EmergencyStopCodeService();
    service.activate();
    const code = service.consumePlaintextOnce()!;

    service.clear();

    expect(service.verify(code)).toBe(false);
  });
});
