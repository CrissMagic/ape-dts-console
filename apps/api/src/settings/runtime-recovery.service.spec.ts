import { expect, it, vi } from 'vitest';
import { RuntimeRecoverySettingsService } from './runtime-recovery.service.js';

function fixture() {
  const row = { enabled: true, maxAttempts: 3, retryDelaySeconds: 30, rowVersion: '1' };
  const query = vi.fn(async (sql: string, values?: unknown[]) => {
    if (sql.startsWith('SELECT enabled')) return { rows: [row] };
    if (sql.startsWith('UPDATE ape_dts_platform.runtime_recovery_settings')) {
      return {
        rows:
          values?.[3] === '1'
            ? [
                {
                  enabled: values[0],
                  maxAttempts: values[1],
                  retryDelaySeconds: values[2],
                  rowVersion: '2',
                },
              ]
            : [],
      };
    }
    return { rows: [] };
  });
  const release = vi.fn();
  const audit = { append: vi.fn() };
  const service = new RuntimeRecoverySettingsService(
    { requirePool: () => ({ query, connect: async () => ({ query, release }) }) } as never,
    audit as never,
  );
  return { service, query, release, audit };
}
it('uses the last committed policy during a database outage', async () => {
  const f = fixture();
  await f.service.get();
  await f.service.update('actor', {
    enabled: true,
    maxAttempts: 5,
    retryDelaySeconds: 60,
    expectedRowVersion: '1',
  });
  f.query.mockRejectedValue(new Error('offline'));
  await expect(f.service.policy()).resolves.toMatchObject({
    enabled: true,
    maxAttempts: 5,
    retryDelaySeconds: 60,
  });
});
it('does not enable recovery without ever having loaded a policy', async () => {
  const f = fixture();
  f.query.mockRejectedValue(new Error('offline'));
  await expect(f.service.policy()).resolves.toMatchObject({ enabled: false });
});
it('does not cache or publish an update if the transaction fails', async () => {
  const f = fixture();
  await f.service.get();
  f.audit.append.mockRejectedValue(new Error('audit failed'));
  await expect(
    f.service.update('actor', {
      enabled: true,
      maxAttempts: 5,
      retryDelaySeconds: 60,
      expectedRowVersion: '1',
    }),
  ).rejects.toThrow('audit failed');
  expect(f.query).toHaveBeenCalledWith('ROLLBACK');
  expect(f.release).toHaveBeenCalledOnce();
  f.query.mockRejectedValue(new Error('offline'));
  await expect(f.service.policy()).resolves.toMatchObject({ maxAttempts: 3 });
});
it('rejects stale updates without writing an audit event', async () => {
  const f = fixture();
  await expect(
    f.service.update('actor', {
      enabled: false,
      maxAttempts: 5,
      retryDelaySeconds: 60,
      expectedRowVersion: '2',
    }),
  ).rejects.toMatchObject({ response: { code: 'RUNTIME_RECOVERY_SETTINGS_CONFLICT' } });
  expect(f.audit.append).not.toHaveBeenCalled();
  expect(f.query).toHaveBeenCalledWith('ROLLBACK');
});
