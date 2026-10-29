import { describe, expect, it, vi } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import {
  isMetadataConnectivityError,
  MetadataAvailabilityError,
  MetadataAvailabilityService,
} from './metadata-availability.service.js';

function database(overrides: Partial<PlatformDatabaseService> = {}): PlatformDatabaseService {
  return {
    abandonControllerLock: vi.fn(),
    acquireControllerLock: vi.fn(async () => true),
    hasPool: vi.fn(() => true),
    probe: vi.fn(async () => undefined),
    registerConnectionErrorListener: vi.fn(() => () => undefined),
    ...overrides,
  } as unknown as PlatformDatabaseService;
}

describe('metadata connectivity classification', () => {
  it.each([
    [{ code: '08006' }],
    [{ code: '57P01' }],
    [{ code: 'ECONNRESET' }],
    [new Error('Connection terminated unexpectedly')],
    [new Error('read ETIMEDOUT')],
  ])('recognizes a metadata outage without classifying business errors: %o', (error) => {
    expect(isMetadataConnectivityError(error)).toBe(true);
  });

  it.each([[{ code: '23505' }], [{ code: '57014' }], [new Error('permission denied')], [null]])(
    'does not turn a non-connectivity failure into an outage: %o',
    (error) => {
      expect(isMetadataConnectivityError(error)).toBe(false);
    },
  );
});

describe('MetadataAvailabilityService', () => {
  it('single-flights concurrent probes while READY', async () => {
    let releaseProbe!: () => void;
    const blockedProbe = new Promise<void>((resolve) => {
      releaseProbe = resolve;
    });
    const probe = vi.fn(() => blockedProbe);
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    const service = new MetadataAvailabilityService(database({ probe }), readiness);

    const first = service.ensureManagementAvailable();
    const second = service.ensureManagementAvailable();
    expect(probe).toHaveBeenCalledTimes(1);
    releaseProbe();

    await expect(Promise.all([first, second])).resolves.toEqual([undefined, undefined]);
  });

  it('abandons the old controller lease and freezes management on a connection failure', async () => {
    const abandonControllerLock = vi.fn();
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    const service = new MetadataAvailabilityService(
      database({
        abandonControllerLock,
        probe: vi.fn(async () => {
          throw Object.assign(new Error('socket closed'), { code: 'ECONNRESET' });
        }),
      }),
      readiness,
    );

    await expect(service.ensureManagementAvailable()).rejects.toMatchObject({
      code: 'METADATA_DATABASE_UNAVAILABLE',
    });
    expect(readiness.getPhase()).toBe('METADATA_DEGRADED');
    expect(abandonControllerLock).toHaveBeenCalledTimes(1);
    expect(service.pendingOutageFact()).toMatchObject({ source: 'management_probe' });
  });

  it('recovers only after reacquiring the controller lock and completing reconciliation', async () => {
    const calls: string[] = [];
    const acquireControllerLock = vi.fn(async () => {
      calls.push('lock');
      return true;
    });
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    readiness.markMetadataDegraded();
    const service = new MetadataAvailabilityService(database({ acquireControllerLock }), readiness);
    service.registerReconciler(async () => {
      calls.push('reconcile');
      expect(readiness.getPhase()).toBe('RECONCILING');
    });

    await service.ensureManagementAvailable();

    expect(calls).toEqual(['lock', 'reconcile']);
    expect(readiness.getPhase()).toBe('READY');
  });

  it('enters a terminal management block when another controller owns the lock', async () => {
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    readiness.markMetadataDegraded();
    const service = new MetadataAvailabilityService(
      database({ acquireControllerLock: vi.fn(async () => false) }),
      readiness,
    );
    service.registerReconciler(async () => undefined);

    await expect(service.ensureManagementAvailable()).rejects.toBeInstanceOf(
      MetadataAvailabilityError,
    );
    expect(readiness.getPhase()).toBe('BLOCKED_CONCURRENT_INSTANCE');
  });

  it('returns to degraded after a reconciliation error so an explicit later request can retry', async () => {
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    readiness.markMetadataDegraded();
    const service = new MetadataAvailabilityService(database(), readiness);
    const reconcile = vi
      .fn<() => Promise<void>>()
      .mockRejectedValueOnce(new Error('reconciliation write rejected'))
      .mockResolvedValueOnce(undefined);
    service.registerReconciler(reconcile);

    await expect(service.ensureManagementAvailable()).rejects.toMatchObject({
      code: 'METADATA_RECONCILIATION_FAILED',
      phase: 'METADATA_DEGRADED',
    });
    expect(readiness.getPhase()).toBe('METADATA_DEGRADED');

    await expect(service.ensureManagementAvailable()).resolves.toBeUndefined();
    expect(reconcile).toHaveBeenCalledTimes(2);
    expect(readiness.getPhase()).toBe('READY');
  });
});
