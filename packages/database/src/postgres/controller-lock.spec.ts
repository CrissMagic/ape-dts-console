import { EventEmitter } from 'node:events';

import type { PoolClient } from 'pg';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ControllerLockLease } from './controller-lock.js';

const leases: ControllerLockLease[] = [];

function fixture() {
  const client = Object.assign(new EventEmitter(), {
    query: vi.fn(async () => ({ rows: [{ released: true }] })),
    release: vi.fn(),
  });
  const onLost = vi.fn();
  const lease = new ControllerLockLease(client as unknown as PoolClient, onLost);
  leases.push(lease);
  return { client, lease, onLost };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  for (const lease of leases.splice(0)) lease.abandon();
  vi.useRealTimers();
});

describe('controller connection lifecycle', () => {
  it('probes the held connection even without management requests', async () => {
    const f = fixture();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.client.query).toHaveBeenCalledOnce();
    expect(f.lease.isReleased()).toBe(false);
    expect(f.onLost).not.toHaveBeenCalled();
  });

  it('destroys the held connection and reports a failed heartbeat only once', async () => {
    const f = fixture();
    const failure = new Error('Query read timeout');
    f.client.query.mockRejectedValueOnce(failure);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.lease.isReleased()).toBe(true);
    expect(f.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(f.onLost).toHaveBeenCalledExactlyOnceWith(failure);
    expect(f.client.query).toHaveBeenCalledOnce();
  });

  it('handles an error on the checked-out connection without an uncaught error', () => {
    const f = fixture();
    const failure = new Error('Connection terminated unexpectedly');
    expect(() => f.client.emit('error', failure)).not.toThrow();
    expect(f.lease.isReleased()).toBe(true);
    expect(f.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(f.onLost).toHaveBeenCalledExactlyOnceWith(failure);
  });

  it('treats the held connection ending as loss of ownership', () => {
    const f = fixture();
    f.client.emit('end');
    expect(f.lease.isReleased()).toBe(true);
    expect(f.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(f.onLost).toHaveBeenCalledOnce();
  });

  it('destroys rather than reuses a connection when unlocking fails', async () => {
    const f = fixture();
    const failure = new Error('Query read timeout');
    f.client.query.mockRejectedValueOnce(failure);
    await expect(f.lease.release()).rejects.toBe(failure);
    expect(f.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(f.onLost).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.client.query).toHaveBeenCalledOnce();
  });

  it('does not reuse a session when PostgreSQL cannot confirm that its lock was released', async () => {
    const f = fixture();
    f.client.query.mockResolvedValueOnce({ rows: [{ released: false }] });
    await expect(f.lease.release()).rejects.toThrow('CONTROLLER_LOCK_NOT_HELD');
    expect(f.client.release).toHaveBeenCalledExactlyOnceWith(true);
  });

  it('does not report a late heartbeat failure as a new outage after shutdown', async () => {
    const f = fixture();
    let fail!: (error: Error) => void;
    f.client.query.mockImplementationOnce(() => new Promise((_resolve, reject) => (fail = reject)));
    await vi.advanceTimersByTimeAsync(10_000);
    await f.lease.release();
    fail(new Error('Connection terminated'));
    await vi.advanceTimersByTimeAsync(20_000);
    expect(f.client.release).toHaveBeenCalledOnce();
    expect(f.onLost).not.toHaveBeenCalled();
  });

  it('single-flights management probes with the heartbeat on the actual held connection', async () => {
    const f = fixture();
    let complete!: (result: { rows: { released: boolean }[] }) => void;
    f.client.query.mockImplementationOnce(() => new Promise((resolve) => (complete = resolve)));
    const first = f.lease.probe();
    const second = f.lease.probe();
    expect(first).toBe(second);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.client.query).toHaveBeenCalledOnce();
    complete({ rows: [] });
    await Promise.all([first, second]);
  });

  it('stops probing after normal release or abandonment without reporting an outage', async () => {
    const released = fixture();
    await released.lease.release();
    await released.lease.release();
    const abandoned = fixture();
    abandoned.lease.abandon();
    abandoned.lease.abandon();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(released.client.query).toHaveBeenCalledOnce();
    expect(released.client.release).toHaveBeenCalledOnce();
    expect(abandoned.client.query).not.toHaveBeenCalled();
    expect(abandoned.client.release).toHaveBeenCalledExactlyOnceWith(true);
    expect(released.onLost).not.toHaveBeenCalled();
    expect(abandoned.onLost).not.toHaveBeenCalled();
  });

  it('does not queue further heartbeats while the previous probe is pending', async () => {
    const f = fixture();
    let complete!: (result: { rows: { released: boolean }[] }) => void;
    f.client.query.mockImplementationOnce(() => new Promise((resolve) => (complete = resolve)));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.client.query).toHaveBeenCalledOnce();
    complete({ rows: [] });
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.client.query).toHaveBeenCalledTimes(2);
  });
});
