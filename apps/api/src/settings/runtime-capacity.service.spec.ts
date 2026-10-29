import { describe, expect, it, vi } from 'vitest';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { RuntimeCapacityService } from './runtime-capacity.service.js';

function setup(limit: number | null, used: number) {
  const query = vi.fn(async (sql: string, values?: unknown[]) => {
    if (sql.includes('SELECT max_runtime_units'))
      return { rows: [{ max_runtime_units: limit, row_version: '5' }] };
    if (sql.includes('count(*)')) return { rows: [{ used: String(used) }] };
    if (sql.includes('UPDATE ape_dts_platform.platform_settings'))
      return { rows: [{ row_version: '6' }] };
    return { rows: [], values };
  });
  const release = vi.fn();
  const database = new PlatformDatabaseService();
  vi.spyOn(database, 'requirePool').mockReturnValue({
    connect: async () => ({ query, release }),
  } as unknown as ReturnType<PlatformDatabaseService['requirePool']>);
  const audit = new AuditLedgerService(database);
  const append = vi.spyOn(audit, 'append').mockResolvedValue('1');
  return { service: new RuntimeCapacityService(database, audit), query, release, append };
}

describe('runtime capacity configuration', () => {
  it('keeps unconfigured capacity unset and reports actual occupied units', async () => {
    const { service } = setup(null, 2);
    expect(await service.get()).toEqual({ maxRuntimeUnits: null, rowVersion: '5', used: 2 });
  });

  it('rejects a stale administrator edit without writing or auditing', async () => {
    const { service, query, append, release } = setup(8, 3);
    await expect(
      service.update('admin', { expectedRowVersion: '4', maxRuntimeUnits: 6 }),
    ).rejects.toMatchObject({ response: { code: 'CAPACITY_VERSION_CONFLICT' } });
    expect(query.mock.calls.some(([sql]) => sql.startsWith('UPDATE'))).toBe(false);
    expect(append).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledWith('ROLLBACK');
    expect(release).toHaveBeenCalled();
  });

  it('rejects reducing capacity below occupied units', async () => {
    const { service, query, append } = setup(8, 3);
    await expect(
      service.update('admin', { expectedRowVersion: '5', maxRuntimeUnits: 2 }),
    ).rejects.toMatchObject({ response: { code: 'CAPACITY_BELOW_USAGE', used: 3 } });
    expect(query.mock.calls.some(([sql]) => sql.startsWith('UPDATE'))).toBe(false);
    expect(append).not.toHaveBeenCalled();
  });

  it('allows the occupied count as the new limit and audits the change', async () => {
    const { service, append } = setup(8, 3);
    expect(await service.update('admin', { expectedRowVersion: '5', maxRuntimeUnits: 3 })).toEqual({
      maxRuntimeUnits: 3,
      rowVersion: '6',
      used: 3,
    });
    expect(append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        actorType: 'USER',
        actorUserId: 'admin',
        action: 'RUNTIME_CAPACITY_UPDATED',
        details: { previousLimit: 8, limit: 3, used: 3 },
      }),
    );
  });

  it('rolls back the capacity change when auditing fails', async () => {
    const { service, append, query } = setup(null, 0);
    append.mockRejectedValue(new Error('audit unavailable'));
    await expect(
      service.update('admin', { expectedRowVersion: '5', maxRuntimeUnits: 2 }),
    ).rejects.toThrow('audit unavailable');
    expect(query).toHaveBeenCalledWith('ROLLBACK');
    expect(query).not.toHaveBeenCalledWith('COMMIT');
  });
});
