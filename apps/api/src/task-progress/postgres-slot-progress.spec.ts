import { beforeEach, describe, expect, it, vi } from 'vitest';
import { PostgresDataSourceAdapter } from '../data-sources/adapters/postgres-data-source.adapter.js';

const { query, end } = vi.hoisted(() => ({
  query: vi.fn(),
  end: vi.fn().mockResolvedValue(undefined),
}));
vi.mock('pg', () => ({
  Pool: class {
    query = query;
    end = end;
  },
}));
const input = {
  configuration: {
    type: 'POSTGRES' as const,
    host: 'localhost',
    port: 5432,
    database: 'demo',
    username: 'reader',
    tls: { mode: 'DISABLE' as const, riskAcknowledged: true as const },
  },
  password: 'password',
};
beforeEach(() => {
  query.mockReset();
  end.mockClear();
});
describe('PostgreSQL slot progress', () => {
  it('reads replication acknowledgement delay from the sender attached to this slot, not reply age', async () => {
    query.mockResolvedValue({
      rows: [{ active: true, unconfirmed: '1024', retained: '2048', replication_lag_ms: 1234.5 }],
    });
    const value = await new PostgresDataSourceAdapter().readProgress(input, { slotName: 'demo' });
    expect(value.replicationLagMilliseconds).toBe(1234.5);
    expect(query.mock.calls[0]?.[0]).toContain('replication.pid=slot.active_pid');
    expect(query.mock.calls[0]?.[1]).toEqual(['demo']);
    expect(query.mock.calls[0]?.[0]).not.toContain('reply_time');
  });
  it.each([true, false])(
    'reports active=%s without rejecting a slot used by this task',
    async (active) => {
      query.mockResolvedValue({
        rows: [{ active, unconfirmed: '90071992547409999', retained: null }],
      });
      const value = await new PostgresDataSourceAdapter().readProgress(input, { slotName: 'demo' });
      expect(value).toEqual({
        slotStatus: active ? 'ACTIVE' : 'INACTIVE',
        unconfirmedWalBytes: '90071992547409999',
        retainedWalBytes: null,
        replicationLagMilliseconds: null,
        offsetLag: null,
      });
      expect(end).toHaveBeenCalled();
      expect(query.mock.calls[0]?.[0]).not.toMatch(/create|drop|alter/iu);
    },
  );
  it('distinguishes a missing slot from zero WAL backlog', async () => {
    query.mockResolvedValue({ rows: [] });
    expect(await new PostgresDataSourceAdapter().readProgress(input, { slotName: 'demo' })).toEqual(
      {
        slotStatus: 'MISSING',
        unconfirmedWalBytes: null,
        retainedWalBytes: null,
        replicationLagMilliseconds: null,
        offsetLag: null,
      },
    );
  });
  it('preserves null LSNs and closes the connection when a read fails', async () => {
    query.mockResolvedValueOnce({ rows: [{ active: true, unconfirmed: null, retained: null }] });
    expect(
      (await new PostgresDataSourceAdapter().readProgress(input, { slotName: 'demo' }))
        .unconfirmedWalBytes,
    ).toBeNull();
    query.mockRejectedValueOnce(new Error('permission denied'));
    await expect(
      new PostgresDataSourceAdapter().readProgress(input, { slotName: 'demo' }),
    ).rejects.toMatchObject({ code: 'PERMISSION_DENIED' });
    expect(end).toHaveBeenCalledTimes(2);
  });
});
