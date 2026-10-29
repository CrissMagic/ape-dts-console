import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { Logger } from '@nestjs/common';
import { encryptSecret } from '@scs/security';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { MetadataAvailabilityService } from '../metadata-availability/metadata-availability.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { AlertsService } from './alerts.service.js';
import { PlatformHealthMonitorService } from './platform-health-monitor.service.js';

function fixture(configured = true) {
  const transport = vi
    .spyOn(globalThis, 'fetch')
    .mockResolvedValue(new Response('{"errcode":0}', { status: 200 }));
  const keys = new UnlockedKeyService();
  keys.set(Buffer.alloc(32, 9));
  const secret = Buffer.from('https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abcdefgh');
  const encrypted = encryptSecret(keys.require(), secret, {
    aadVersion: 1,
    ownerType: 'ALERT_SETTINGS',
    ownerId: '1',
    purpose: 'WECOM_WEBHOOK',
    secretId: '1',
  });
  secret.fill(0);
  const row = {
    enabled: configured,
    encrypted_webhook: configured ? encrypted : null,
    row_version: '1',
    configuration: {
      channel: 'WECOM',
      notifyRecovery: true,
      durationSeconds: 30,
      repeatSeconds: 300,
      slotInactive: true,
      slotMissing: true,
      retainedWalBytes: null,
      unconfirmedWalBytes: null,
      kafkaOffsetLag: null,
      delayMilliseconds: null,
    },
  };
  let offline = false;
  const outage = Object.assign(new Error('connection refused'), { code: 'ECONNREFUSED' });
  const query = vi.fn(async (sql: string) => {
    if (offline) throw outage;
    if (sql.startsWith('SELECT enabled')) return { rows: [row] };
    return { rows: [], rowCount: 1 };
  });
  const database = {
    requirePool: () => ({ query }),
    probe: vi.fn(async () => {
      if (offline) throw outage;
    }),
    abandonControllerLock: vi.fn(),
    acquireControllerLock: vi.fn(async () => true),
  };
  const readiness = new PlatformReadinessService();
  readiness.markReady();
  const availability = new MetadataAvailabilityService(database as never, readiness);
  const reconcile = vi.fn(async () => undefined);
  availability.registerReconciler(reconcile);
  const alerts = new AlertsService(database as never, keys, {} as never);
  const monitor = new PlatformHealthMonitorService(availability, readiness, alerts);
  return {
    monitor,
    readiness,
    database,
    query,
    row,
    alerts,
    reconcile,
    transport,
    offline: (value: boolean) => {
      offline = value;
    },
  };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-10-26T00:00:00Z'));
  vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
  vi.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
  vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

it('probes without HTTP traffic and sends cached encrypted notifications while the metadata database is down', async () => {
  const f = fixture();
  await f.monitor.tick();
  f.monitor.onModuleInit();
  await vi.advanceTimersByTimeAsync(1);
  f.offline(true);
  await vi.advanceTimersByTimeAsync(5000);
  expect(f.readiness.getPhase()).toBe('METADATA_DEGRADED');
  expect(f.transport).toHaveBeenCalledOnce();
  expect(Logger.prototype.error).toHaveBeenCalledWith(
    expect.objectContaining({ code: 'PLATFORM_METADATA_UNAVAILABLE' }),
  );
  const notice = JSON.parse(String(f.transport.mock.calls[0]![1]!.body));
  expect(notice.text.content).toContain('2026年10月26日');
  expect(notice.text.content).not.toContain('abcdefgh');
  await vi.advanceTimersByTimeAsync(55000);
  expect(f.transport).toHaveBeenCalledOnce();
  f.offline(false);
  f.transport.mockResolvedValue(new Response('{"errcode":0}', { status: 200 }));
  await vi.advanceTimersByTimeAsync(5000);
  expect(f.reconcile).toHaveBeenCalledOnce();
  expect(f.readiness.getPhase()).toBe('READY');
  expect(f.transport).toHaveBeenCalledTimes(2);
  const inserts = f.query.mock.calls.filter(([sql]) =>
    sql.startsWith('INSERT INTO ape_dts_platform.alert_deliveries'),
  );
  expect(inserts).toHaveLength(2);
  await f.monitor.tick();
  expect(
    f.query.mock.calls.filter(([sql]) =>
      sql.startsWith('INSERT INTO ape_dts_platform.alert_deliveries'),
    ),
  ).toHaveLength(2);
  f.monitor.onApplicationShutdown();
});

it('logs an outage even without a configured notification channel', async () => {
  const f = fixture(false);
  await f.monitor.tick();
  f.offline(true);
  await f.monitor.tick();
  expect(f.transport).not.toHaveBeenCalled();
  expect(Logger.prototype.error).toHaveBeenCalledWith(
    expect.objectContaining({ code: 'PLATFORM_METADATA_UNAVAILABLE' }),
  );
  expect(Logger.prototype.warn).toHaveBeenCalledWith(
    expect.objectContaining({ code: 'PLATFORM_NOTIFICATION_UNAVAILABLE' }),
  );
});

it('keeps failed platform delivery visible in logs and flushes its failed history after recovery', async () => {
  const f = fixture();
  await f.monitor.tick();
  f.transport.mockRejectedValue(new Error('secret transport error'));
  f.offline(true);
  await f.monitor.tick();
  expect(Logger.prototype.error).toHaveBeenCalledWith(
    expect.objectContaining({
      code: 'PLATFORM_NOTIFICATION_RESULT',
      status: 'FAILED',
      failureCode: 'CHANNEL_UNREACHABLE',
    }),
  );
  expect(JSON.stringify(vi.mocked(Logger.prototype.error).mock.calls)).not.toContain(
    'secret transport error',
  );
  f.offline(false);
  await f.monitor.tick();
  expect(
    f.query.mock.calls.filter(([sql]) =>
      sql.startsWith('INSERT INTO ape_dts_platform.alert_deliveries'),
    ),
  ).toHaveLength(1);
});

it('does not claim a database outage for an unrelated monitor error', async () => {
  const f = fixture();
  vi.spyOn(f.alerts, 'get').mockRejectedValue(new Error('bad configuration'));
  await f.monitor.tick();
  expect(f.readiness.getPhase()).toBe('READY');
  expect(f.transport).not.toHaveBeenCalled();
  expect(Logger.prototype.error).toHaveBeenCalledWith({ code: 'PLATFORM_HEALTH_MONITOR_FAILED' });
});

it('runs one probe at a time and stops sending notifications after shutdown', async () => {
  const f = fixture();
  let finish!: () => void;
  f.database.probe.mockImplementation(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      }),
  );
  const pending = f.monitor.tick();
  await f.monitor.tick();
  expect(f.database.probe).toHaveBeenCalledOnce();
  f.monitor.onApplicationShutdown();
  finish();
  await pending;
  await f.monitor.tick();
  expect(f.query).not.toHaveBeenCalled();
  expect(f.transport).not.toHaveBeenCalled();
});

it('reports controller lock conflicts without stealing the lock or reconciling', async () => {
  const f = fixture();
  await f.monitor.tick();
  f.readiness.markConcurrentInstanceBlocked();
  await f.monitor.tick();
  expect(f.reconcile).not.toHaveBeenCalled();
  expect(f.database.acquireControllerLock).not.toHaveBeenCalled();
  expect(f.transport).toHaveBeenCalledOnce();
  expect(Logger.prototype.error).toHaveBeenCalledWith(
    expect.objectContaining({ code: 'PLATFORM_CONTROL_UNAVAILABLE' }),
  );
});
