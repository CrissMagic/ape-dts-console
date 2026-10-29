import { Logger } from '@nestjs/common';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { EmergencyStopCodeLifecycleService } from './emergency-stop-code-lifecycle.service.js';
import { EmergencyStopCodeService } from './emergency-stop-code.service.js';

describe('EmergencyStopCodeLifecycleService', () => {
  afterEach(() => vi.restoreAllMocks());

  it('logs the process-lifetime code exactly once and retains only its verifier', () => {
    const stopCode = new EmergencyStopCodeService();
    const warning = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
    const lifecycle = new EmergencyStopCodeLifecycleService(stopCode);

    lifecycle.activateForReadyPlatform();

    expect(warning).toHaveBeenCalledOnce();
    const payload = JSON.parse(String(warning.mock.calls[0]?.[0])) as {
      emergencyStopCode: string;
      event: string;
    };
    expect(payload.event).toBe('emergency.stop_code_activated');
    expect(stopCode.consumePlaintextOnce()).toBeUndefined();
    expect(stopCode.verify(payload.emergencyStopCode)).toBe(true);
  });

  it('invalidates the verifier during application shutdown', () => {
    const stopCode = new EmergencyStopCodeService();
    const warning = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
    const lifecycle = new EmergencyStopCodeLifecycleService(stopCode);
    lifecycle.activateForReadyPlatform();
    const payload = JSON.parse(String(warning.mock.calls[0]?.[0])) as {
      emergencyStopCode: string;
    };

    lifecycle.onApplicationShutdown();

    expect(stopCode.verify(payload.emergencyStopCode)).toBe(false);
  });
});
