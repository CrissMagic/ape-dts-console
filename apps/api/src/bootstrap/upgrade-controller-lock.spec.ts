import { afterEach, expect, it, vi } from 'vitest';
import { waitForUpgradeControllerLock } from './upgrade-controller-lock.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function fixture() {
  const acquireControllerLock = vi.fn(async () => false);
  const query = vi
    .fn<
      (
        sql: string,
      ) => Promise<{ rows: { pid: number; application_name: string; client_addr: string }[] }>
    >()
    .mockResolvedValue({
      rows: [{ pid: 30242, application_name: 'scs-data-sync-platform', client_addr: '192.0.2.10' }],
    });
  const output = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
  const database = { acquireControllerLock, requirePool: () => ({ query }) };
  return { database, acquireControllerLock, query, output };
}

it('continues immediately when the old controller lock is already released', async () => {
  const f = fixture();
  f.acquireControllerLock.mockResolvedValueOnce(true);
  await waitForUpgradeControllerLock(f.database as never);
  expect(f.acquireControllerLock).toHaveBeenCalledOnce();
  expect(f.query).not.toHaveBeenCalled();
  expect(f.output).not.toHaveBeenCalledWith(
    expect.stringContaining('PLATFORM_CONTROLLER_LOCK_TIMEOUT'),
  );
});

it('does not mislabel a database connection failure as a proven pre-migration lock conflict', async () => {
  const f = fixture();
  f.acquireControllerLock.mockRejectedValueOnce(new Error('CONNECTION_TIMEOUT'));
  await expect(waitForUpgradeControllerLock(f.database as never)).rejects.toThrow(
    'CONNECTION_TIMEOUT',
  );
  expect(f.query).not.toHaveBeenCalled();
  expect(f.output).not.toHaveBeenCalledWith(
    expect.stringContaining('PLATFORM_CONTROLLER_LOCK_TIMEOUT'),
  );
});

it('waits for a delayed old session release instead of failing the upgrade immediately', async () => {
  vi.useFakeTimers();
  const f = fixture();
  f.acquireControllerLock.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const operation = waitForUpgradeControllerLock(f.database as never).catch((error) => error);
  await vi.advanceTimersByTimeAsync(1000);
  expect(await operation).toBeUndefined();
  expect(f.acquireControllerLock).toHaveBeenCalledTimes(2);
  expect(f.query).not.toHaveBeenCalled();
});

it('times out with a safe holder diagnostic without killing any database connection', async () => {
  vi.useFakeTimers();
  const f = fixture();
  const outcome = waitForUpgradeControllerLock(f.database as never).catch((error) => error);
  await vi.advanceTimersByTimeAsync(60000);
  expect((await outcome).message).toBe('STOP_PLATFORM_BEFORE_SECURITY_MIGRATION');
  const diagnostic = JSON.parse(
    String(
      f.output.mock.calls.find(([line]) =>
        String(line).includes('PLATFORM_CONTROLLER_LOCK_TIMEOUT'),
      )![0],
    ),
  );
  expect(diagnostic).toMatchObject({
    event: 'PLATFORM_CONTROLLER_LOCK_TIMEOUT',
    waitSeconds: 60,
    diagnosticsAvailable: true,
    holders: [expect.objectContaining({ pid: 30242 })],
  });
  expect(f.query).toHaveBeenCalledOnce();
  expect(f.query.mock.calls[0]?.[0]).not.toMatch(
    /pg_terminate_backend|pg_cancel_backend|DELETE|UPDATE/iu,
  );
});

it('preserves the pre-migration error when holder statistics cannot be read and hides exception secrets', async () => {
  vi.useFakeTimers();
  const f = fixture();
  f.query.mockRejectedValueOnce(new Error('postgres://owner:secret@host'));
  const outcome = waitForUpgradeControllerLock(f.database as never).catch((error) => error);
  await vi.advanceTimersByTimeAsync(60000);
  expect((await outcome).message).toBe('STOP_PLATFORM_BEFORE_SECURITY_MIGRATION');
  const diagnostic = String(
    f.output.mock.calls.find(([line]) =>
      String(line).includes('PLATFORM_CONTROLLER_LOCK_TIMEOUT'),
    )![0],
  );
  expect(diagnostic).not.toContain('secret');
  expect(JSON.parse(diagnostic).diagnosticsAvailable).toBe(false);
});
