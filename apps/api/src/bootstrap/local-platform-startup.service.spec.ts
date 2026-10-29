import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';
import type { WrappedKey } from '@scs/security';
import { Reflector } from '@nestjs/core';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { MetadataAvailabilityGuard } from '../metadata-availability/metadata-availability.guard.js';
import { MetadataAvailabilityService } from '../metadata-availability/metadata-availability.service.js';
import { AllowDuringBootstrapRecovery } from '../metadata-availability/metadata-availability.decorator.js';

import { BootstrapStateService } from './bootstrap-state.service.js';
import { LocalPlatformStartupService } from './local-platform-startup.service.js';
import { makeLocalPlatformConfig, writeLocalPlatformConfig } from './local-platform-config.js';

const dirs: string[] = [];
const connection = {
  host: 'localhost',
  port: 5432,
  username: 'owner',
  password: 'db-secret',
  database: 'platform',
  tls: { mode: 'DISABLE' as const },
};
async function fixture(schema: 'CURRENT' | 'OLDER' = 'CURRENT') {
  const directory = await mkdtemp(join(tmpdir(), 'scs-startup-'));
  dirs.push(directory);
  vi.stubEnv('SCS_PLATFORM_CONFIG_PATH', join(directory, 'private', 'config.json'));
  delete process.env.SCS_PLATFORM_DATABASE_CONFIG;
  const wrapper = { ciphertext: randomBytes(32).toString('base64') } as WrappedKey;
  const state = new BootstrapStateService();
  const database = {
    inspectAndUse: vi.fn(async () => ({ state: schema })),
    requirePool: vi.fn(() => ({})),
    acquireControllerLock: vi.fn(async () => true),
    releaseControllerLock: vi.fn(),
  };
  const persistence = {
    ensureDefaultLocalStorage: vi.fn(async () => undefined),
    loadActivePassphraseWrapper: vi.fn(async () => wrapper),
    startExistingRecovery: vi.fn(async () => 'session'),
  };
  const key = { set: vi.fn(), clear: vi.fn() };
  const recovery = {
    run: vi.fn(async (_session: string) => {
      void _session;
      return { state: 'READY' };
    }),
  };
  const recoveryPort = {
    ...recovery,
    startForPlatform: vi.fn((session: string) => {
      void recovery.run(session).catch(() => undefined);
    }),
  };
  const completion = {
    complete: vi.fn(async () => {
      state.markReady();
      return true;
    }),
  };
  const readiness = new PlatformReadinessService();
  const service = new LocalPlatformStartupService(
    state,
    database as never,
    { get: async () => ({ manifest: {} }) } as never,
    persistence as never,
    key as never,
    readiness,
    recoveryPort as never,
    completion as never,
    null,
  );
  return { state, database, persistence, key, recovery, service, wrapper, readiness, completion };
}
afterEach(async () => {
  vi.unstubAllEnvs();
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

describe('automatic platform startup', () => {
  it('does not wait for task recovery before opening management', async () => {
    const context = await fixture();
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), context.wrapper),
    );
    let release!: (value: { state: string }) => void;
    context.recovery.run.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    await context.service.start();
    expect(context.state.getStatus().phase).toBe('READY');
    expect(context.database.releaseControllerLock).not.toHaveBeenCalled();
    release({ state: 'FAILED' });
  });

  it.each(['FAILED', 'PAUSED'])('opens management when task recovery is %s', async (state) => {
    const context = await fixture();
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), context.wrapper),
    );
    context.recovery.run.mockResolvedValueOnce({ state });
    await context.service.start();
    expect(context.completion.complete).toHaveBeenCalledWith('session');
    expect(context.state.getStatus().phase).toBe('READY');
    expect(context.key.clear).not.toHaveBeenCalled();
    expect(context.database.releaseControllerLock).not.toHaveBeenCalled();
  });

  it('seals HTTP recovery access when startup metadata fails and releases its key and lease', async () => {
    const context = await fixture();
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), context.wrapper),
    );
    context.persistence.startExistingRecovery.mockRejectedValueOnce(
      new Error('recovery metadata unavailable'),
    );
    await context.service.onApplicationBootstrap();
    expect(context.state.getStatus()).toMatchObject({
      phase: 'SEALED',
      startupIssue: 'LOCAL_STARTUP_FAILED',
    });
    expect(context.readiness.getPhase()).toBe('SEALED');
    expect(context.key.clear).toHaveBeenCalledOnce();
    expect(context.database.releaseControllerLock).toHaveBeenCalledOnce();
    expect(context.completion.complete).not.toHaveBeenCalled();
    @AllowDuringBootstrapRecovery()
    class RecoveryEndpoint {}
    const guard = new MetadataAvailabilityGuard(
      new Reflector(),
      new MetadataAvailabilityService(context.database as never, context.readiness),
      context.state,
    );
    await expect(
      guard.canActivate({ getClass: () => RecoveryEndpoint, getHandler: () => () => {} } as never),
    ).rejects.toMatchObject({ response: { code: 'PLATFORM_NOT_READY', phase: 'SEALED' } });
  });

  it('blocks management even when releasing the failed startup lease also throws', async () => {
    const context = await fixture();
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), context.wrapper),
    );
    context.completion.complete.mockRejectedValueOnce(new Error('completion failed'));
    context.database.releaseControllerLock.mockRejectedValueOnce(
      new Error('lease connection lost'),
    );
    await expect(context.service.onApplicationBootstrap()).rejects.toThrow('lease connection lost');
    expect(context.state.getStatus().phase).toBe('SEALED');
    expect(context.readiness.getPhase()).toBe('SEALED');
    expect(context.key.clear).toHaveBeenCalledOnce();
  });
  it('exposes setup guidance without a database or key file', async () => {
    const context = await fixture();
    await context.service.start();
    expect(context.state.getStatus().startupIssue).toBe('DATABASE_CONFIGURATION_REQUIRED');
    expect(context.database.inspectAndUse).not.toHaveBeenCalled();
  });
  it('reuses the persisted key and restores an existing platform without changing users', async () => {
    const context = await fixture();
    const dek = randomBytes(32);
    let restoredKey: Buffer | undefined;
    context.key.set.mockImplementation((value: Buffer) => {
      restoredKey = Buffer.from(value);
    });
    await writeLocalPlatformConfig(makeLocalPlatformConfig(connection, dek, context.wrapper));
    // A migrated storage directory supplies the original connection and encryption key.
    vi.stubEnv(
      'SCS_PLATFORM_DATABASE_CONFIG',
      JSON.stringify({ ...connection, database: 'unused' }),
    );
    await context.service.start();
    expect(context.database.inspectAndUse).toHaveBeenCalledWith(connection, {});
    expect(restoredKey).toEqual(dek);
    expect(context.state.getStatus().phase).toBe('READY');
    expect(context.persistence.startExistingRecovery).toHaveBeenCalledOnce();
    expect(context.recovery.run).toHaveBeenCalledWith('session');
  });
  it('does not silently upgrade an older database', async () => {
    const context = await fixture('OLDER');
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), context.wrapper),
    );
    await context.service.start();
    expect(context.state.getStatus().startupIssue).toBe('DATABASE_UPGRADE_REQUIRED');
    expect(context.persistence.startExistingRecovery).not.toHaveBeenCalled();
  });
  it('refuses a key file belonging to a different database', async () => {
    const context = await fixture();
    await writeLocalPlatformConfig(
      makeLocalPlatformConfig(connection, randomBytes(32), {
        ciphertext: randomBytes(32).toString('base64'),
      } as WrappedKey),
    );
    await expect(context.service.start()).rejects.toThrow('LOCAL_KEY_DATABASE_MISMATCH');
    expect(context.key.set).not.toHaveBeenCalled();
  });
});
