import { ServiceUnavailableException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';

import { HealthController } from './health.controller.js';
import { PlatformReadinessService } from './platform-readiness.service.js';

describe('HealthController', () => {
  it('reports liveness without claiming the sealed platform is ready', () => {
    const readiness = new PlatformReadinessService();
    const controller = new HealthController(readiness);

    expect(controller.live()).toEqual({ status: 'ok' });
    expect(() => controller.ready()).toThrow(ServiceUnavailableException);

    try {
      controller.ready();
    } catch (error) {
      expect(error).toBeInstanceOf(ServiceUnavailableException);
      expect((error as ServiceUnavailableException).getResponse()).toEqual({
        code: 'PLATFORM_NOT_READY',
        phase: 'SEALED',
        status: 'not_ready',
      });
    }
  });

  it('reports readiness only after the platform enters READY', () => {
    const readiness = new PlatformReadinessService();
    readiness.beginBootstrap();
    readiness.markReady();

    expect(new HealthController(readiness).ready()).toEqual({
      phase: 'READY',
      status: 'ready',
    });
  });

  it('removes readiness when metadata becomes unavailable', () => {
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    readiness.markMetadataDegraded();

    expect(() => new HealthController(readiness).ready()).toThrow(ServiceUnavailableException);
  });

  it('keeps readiness unavailable throughout reconciliation and a concurrent-controller block', () => {
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    readiness.markMetadataDegraded();
    readiness.markReconciling();

    expect(() => new HealthController(readiness).ready()).toThrow(ServiceUnavailableException);

    readiness.markConcurrentInstanceBlocked();
    try {
      new HealthController(readiness).ready();
    } catch (error) {
      expect(error).toBeInstanceOf(ServiceUnavailableException);
      expect((error as ServiceUnavailableException).getResponse()).toEqual({
        code: 'PLATFORM_NOT_READY',
        phase: 'BLOCKED_CONCURRENT_INSTANCE',
        status: 'not_ready',
      });
    }
  });
});
