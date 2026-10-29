import { describe, expect, it, vi } from 'vitest';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { UnlockedKeyService } from '../security/unlocked-key.service.js';
import type { DataSourceAdapterRegistry } from '../data-sources/adapters/types.js';
import { DataSourcesService } from '../data-sources/data-sources.service.js';

describe('progress source identity', () => {
  it('does not decrypt or connect after a running task source is replaced', async () => {
    const requireAdapter = vi.fn();
    const requireKey = vi.fn();
    const service = new DataSourcesService(
      {
        requirePool: () => ({
          query: vi.fn().mockResolvedValue({
            rows: [
              {
                id: '11111111-1111-4111-8111-111111111111',
                row_version: '2',
                state: 'ACTIVE',
                deleted_at: null,
                source_type: 'POSTGRES',
                configuration: { host: 'new-database' },
                connection_secret_id: 'secret',
              },
            ],
          }),
        }),
      } as unknown as PlatformDatabaseService,
      {} as AuditLedgerService,
      { require: requireKey } as unknown as UnlockedKeyService,
      { require: requireAdapter } as unknown as DataSourceAdapterRegistry,
    );
    await expect(
      service.readProgressForSystem(
        '11111111-1111-4111-8111-111111111111',
        { slotName: 'same_name' },
        '1',
      ),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_VERSION_CONFLICT' });
    expect(requireAdapter).not.toHaveBeenCalled();
    expect(requireKey).not.toHaveBeenCalled();
  });
});
