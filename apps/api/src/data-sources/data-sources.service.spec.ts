import { describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { DataSourcesService } from './data-sources.service.js';
import type { DataSourceAdapterRegistry } from './adapters/types.js';

const administrator = {
  displayName: 'Administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
  role: 'ADMIN' as const,
  status: 'ACTIVE' as const,
  username: 'admin',
};

describe('DataSourcesService system checks', () => {
  it('uses the read-only adapter path when checking PostgreSQL resources', async () => {
    const sourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5';
    const source = {
      configuration: {
        database: 'demo',
        host: '127.0.0.1',
        port: 5432,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
        type: 'POSTGRES',
        username: 'reader',
      },
      connection_secret_id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f6',
      created_at: new Date(),
      created_by_user_id: administrator.id,
      deleted_at: null,
      id: sourceId,
      normalized_name: 'postgres-source',
      row_version: '1',
      source_type: 'POSTGRES',
      state: 'ACTIVE',
      updated_at: new Date(),
      updated_by_user_id: administrator.id,
    };
    const query = vi.fn(async () => ({ rows: [source], rowCount: 1 }));
    const readPostgresResources = vi.fn(async () => ({
      databaseName: 'demo',
      slot: { name: 'demo_slot', created: false, stableIdentity: 'slot-identity' },
      publication: { name: 'demo_pub', created: false, stableIdentity: 'pub-identity' },
    }));
    const preparePostgresResources = vi.fn();
    const service = new DataSourcesService(
      { requirePool: () => ({ query }) } as unknown as PlatformDatabaseService,
      { append: vi.fn() } as unknown as AuditLedgerService,
      { require: vi.fn() } as unknown as UnlockedKeyService,
      {
        require: vi.fn(() => ({ readPostgresResources, preparePostgresResources })),
      } as unknown as DataSourceAdapterRegistry,
    );
    Object.assign(service, {
      withConnectionPassword: async (
        _source: unknown,
        use: (password: string) => Promise<unknown>,
      ) => await use('secret'),
    });
    const request = {
      publicationName: 'demo_pub',
      schema: 'public',
      slotName: 'demo_slot',
      table: 'events',
    };

    await expect(service.readPostgresResources(administrator, sourceId, request)).resolves.toEqual(
      expect.objectContaining({ databaseName: 'demo' }),
    );
    expect(readPostgresResources).toHaveBeenCalledWith(
      expect.objectContaining({ password: 'secret' }),
      request,
    );
    expect(preparePostgresResources).not.toHaveBeenCalled();
  });

  it('records a real system connection test without a user identity', async () => {
    const sourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5';
    const source = {
      configuration: {
        bootstrapServers: ['127.0.0.1:9092'],
        plaintextRiskAcknowledged: true,
        type: 'KAFKA',
      },
      connection_secret_id: null,
      created_at: new Date(),
      created_by_user_id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
      deleted_at: null,
      id: sourceId,
      normalized_name: 'source',
      row_version: '1',
      source_type: 'KAFKA',
      state: 'ACTIVE',
      updated_at: new Date(),
      updated_by_user_id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
    };
    const queries: { sql: string; values: readonly unknown[] }[] = [];
    const query = vi.fn(async (sql: string, values: readonly unknown[] = []) => {
      queries.push({ sql, values });
      if (sql.includes('FROM ape_dts_platform.data_sources'))
        return { rows: [source], rowCount: 1 };
      if (sql.includes('INSERT INTO ape_dts_platform.connection_test_runs')) {
        return {
          rows: [{ completed_at: new Date(), id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f9' }],
          rowCount: 1,
        };
      }
      if (sql.includes('INSERT INTO ape_dts_platform.data_source_identity_observations')) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });
    const pool = { connect: vi.fn(async () => ({ query, release: vi.fn() })), query };
    const audit = { append: vi.fn(async () => '1') };
    const adapters = {
      require: vi.fn(() => ({
        test: vi.fn(async () => ({
          kind: 'KAFKA_CLUSTER',
          serverVersion: null,
          value: 'cluster-1',
        })),
      })),
    };
    const service = new DataSourcesService(
      { requirePool: () => pool } as unknown as PlatformDatabaseService,
      audit as unknown as AuditLedgerService,
      { require: vi.fn() } as unknown as UnlockedKeyService,
      adapters as unknown as DataSourceAdapterRegistry,
    );

    const response = await service.testForSystem(sourceId);

    expect(response.state).toBe('SUCCEEDED');
    const insert = queries.find(({ sql }) => sql.includes('connection_test_runs'));
    expect(insert?.values[2]).toBeNull();
    expect(audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actorType: 'SYSTEM', actorUserId: null }),
    );
  });
});
