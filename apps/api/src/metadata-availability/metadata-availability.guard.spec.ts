import { ServiceUnavailableException, type ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { describe, expect, it, vi } from 'vitest';

import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import { AuthController } from '../auth/auth.controller.js';
import { RecoveryController } from '../recovery/recovery.controller.js';
import { METADATA_AVAILABILITY_RECOVERY_ACCESS } from './metadata-availability.decorator.js';
import { MetadataAvailabilityGuard } from './metadata-availability.guard.js';
import {
  MetadataAvailabilityError,
  type MetadataAvailabilityService,
} from './metadata-availability.service.js';

function context(): ExecutionContext {
  return {
    getClass: () => class TestController {},
    getHandler: () => () => undefined,
  } as unknown as ExecutionContext;
}

function bootstrapState(phase: 'READY' | 'RECOVERING') {
  return {
    getStatus: vi.fn(() => ({ phase, platformInstanceId: 'instance-1' })),
  } as unknown as BootstrapStateService;
}

describe('MetadataAvailabilityGuard', () => {
  it('marks only authentication and recovery controllers for bootstrap recovery access', () => {
    expect(Reflect.getMetadata(METADATA_AVAILABILITY_RECOVERY_ACCESS, AuthController)).toBe(true);
    expect(Reflect.getMetadata(METADATA_AVAILABILITY_RECOVERY_ACCESS, RecoveryController)).toBe(
      true,
    );
  });

  it('runs before route guards and returns one metadata 503 shape', async () => {
    const availability = {
      ensureManagementAvailable: vi.fn(async () => {
        throw new MetadataAvailabilityError('METADATA_DATABASE_UNAVAILABLE', 'METADATA_DEGRADED');
      }),
    } as unknown as MetadataAvailabilityService;
    const reflector = {
      getAllAndOverride: vi.fn(() => false),
    } as unknown as Reflector;
    const guard = new MetadataAvailabilityGuard(reflector, availability, bootstrapState('READY'));

    await expect(guard.canActivate(context())).rejects.toBeInstanceOf(ServiceUnavailableException);
    try {
      await guard.canActivate(context());
    } catch (error) {
      expect((error as ServiceUnavailableException).getResponse()).toEqual({
        code: 'METADATA_DATABASE_UNAVAILABLE',
        phase: 'METADATA_DEGRADED',
        status: 'unavailable',
      });
    }
  });

  it('does not probe controllers explicitly allowed during an outage', async () => {
    const ensureManagementAvailable = vi.fn(async () => undefined);
    const availability = {
      ensureManagementAvailable,
    } as unknown as MetadataAvailabilityService;
    const reflector = {
      getAllAndOverride: vi.fn(() => true),
    } as unknown as Reflector;

    await expect(
      new MetadataAvailabilityGuard(reflector, availability, bootstrapState('READY')).canActivate(
        context(),
      ),
    ).resolves.toBe(true);
    expect(ensureManagementAvailable).not.toHaveBeenCalled();
  });

  it('allows an explicitly scoped auth or recovery route only while bootstrap is RECOVERING', async () => {
    const ensureManagementAvailable = vi.fn(async () => {
      throw new MetadataAvailabilityError('PLATFORM_NOT_READY', 'BOOTSTRAPPING');
    });
    const availability = { ensureManagementAvailable } as unknown as MetadataAvailabilityService;
    const reflector = {
      getAllAndOverride: vi.fn().mockReturnValueOnce(false).mockReturnValueOnce(true),
    } as unknown as Reflector;

    await expect(
      new MetadataAvailabilityGuard(
        reflector,
        availability,
        bootstrapState('RECOVERING'),
      ).canActivate(context()),
    ).resolves.toBe(true);
    expect(ensureManagementAvailable).not.toHaveBeenCalled();
  });

  it('does not broaden recovery access before or after the RECOVERING phase', async () => {
    const ensureManagementAvailable = vi.fn(async () => {
      throw new MetadataAvailabilityError('PLATFORM_NOT_READY', 'BOOTSTRAPPING');
    });
    const availability = { ensureManagementAvailable } as unknown as MetadataAvailabilityService;
    const reflector = {
      getAllAndOverride: vi.fn((key: symbol) => String(key).includes('RECOVERY_ACCESS')),
    } as unknown as Reflector;

    await expect(
      new MetadataAvailabilityGuard(reflector, availability, bootstrapState('READY')).canActivate(
        context(),
      ),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
    expect(ensureManagementAvailable).toHaveBeenCalledOnce();
  });
});
