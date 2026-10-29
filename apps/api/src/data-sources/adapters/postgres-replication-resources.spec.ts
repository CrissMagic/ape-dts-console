import { describe, expect, it, vi } from 'vitest';
import type { Pool } from 'pg';

import {
  preparePostgresReplicationResources,
  readPostgresReplicationResources,
} from './postgres-replication-resources.js';

describe('readPostgresReplicationResources', () => {
  function multiTableFixture(
    options: { invalidSecondTable?: boolean; publicationMissingTable?: boolean } = {},
  ) {
    let publicationExists = options.publicationMissingTable ?? false;
    let slotExists = false;
    const query = vi.fn(async (sql: string, values?: unknown[]) => {
      if (sql.includes('pg_advisory_lock')) return { rows: [] };
      if (sql.includes('pg_control_system'))
        return {
          rows: [{ database_name: 'demo', database_oid: '1', system_identifier: 'system-1' }],
        };
      if (sql.includes('FROM pg_class'))
        return {
          rows: [
            {
              oid: '10',
              primary_key: true,
              relreplident: options.invalidSecondTable && values?.[1] === 'other' ? 'n' : 'd',
            },
          ],
        };
      if (sql.includes('FROM pg_replication_slots'))
        return {
          rows: slotExists
            ? [{ active: false, database: 'demo', plugin: 'pgoutput', slot_type: 'logical' }]
            : [],
        };
      if (sql.includes('FROM pg_publication'))
        return {
          rows: publicationExists
            ? [
                {
                  oid: '20',
                  includes_table: !options.publicationMissingTable,
                  pubinsert: true,
                  pubupdate: true,
                  pubdelete: true,
                  pubtruncate: true,
                },
              ]
            : [],
        };
      if (sql.startsWith('CREATE PUBLICATION')) {
        publicationExists = true;
        return { rows: [] };
      }
      if (sql.includes('pg_create_logical_replication_slot')) {
        slotExists = true;
        return { rows: [{ slot_name: 'demo_slot' }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    });
    const connect = vi.fn(async () => ({ query, release: vi.fn() }));
    const pool = { connect } as unknown as Pool;
    const beforeWrite = vi.fn(async () => undefined);
    const request = {
      publicationName: 'demo_pub',
      resourceMode: 'PLATFORM_MANAGED' as const,
      slotName: 'demo_slot',
      tables: [
        { schema: 'public', name: 'events' },
        { schema: 'public', name: 'other' },
      ],
    };
    return { query, pool, beforeWrite, request, connect };
  }

  it('creates one publication covering all selected tables and one logical slot', async () => {
    const { pool, request, beforeWrite, query } = multiTableFixture();
    const prepared = await preparePostgresReplicationResources(pool, request, beforeWrite);
    expect(prepared).toMatchObject({ slot: { created: true }, publication: { created: true } });
    expect(
      query.mock.calls.filter(([sql]) => sql.startsWith('CREATE PUBLICATION')).map(([sql]) => sql),
    ).toEqual([
      'CREATE PUBLICATION "demo_pub" FOR TABLE "public"."events", "public"."other" WITH (publish = \'insert, update, delete, truncate\')',
    ]);
    expect(
      query.mock.calls.filter(([sql]) => sql.includes('pg_create_logical_replication_slot')),
    ).toHaveLength(1);
  });

  it('rejects an invalid second source table before any external write', async () => {
    const { pool, request, beforeWrite, query } = multiTableFixture({ invalidSecondTable: true });
    await expect(
      preparePostgresReplicationResources(pool, request, beforeWrite),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(beforeWrite).not.toHaveBeenCalled();
    expect(
      query.mock.calls.some(([sql]) =>
        /CREATE PUBLICATION|pg_create_logical_replication_slot/u.test(sql),
      ),
    ).toBe(false);
  });

  it('does not extend an existing publication that omits one selected table', async () => {
    const { pool, request, beforeWrite, query } = multiTableFixture({
      publicationMissingTable: true,
    });
    await expect(
      preparePostgresReplicationResources(pool, request, beforeWrite),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(beforeWrite).not.toHaveBeenCalled();
    expect(
      query.mock.calls.some(([sql]) =>
        /CREATE PUBLICATION|ALTER PUBLICATION|pg_create_logical_replication_slot/u.test(sql),
      ),
    ).toBe(false);
  });

  it('rejects empty, duplicate or ambiguous table requests before connecting', async () => {
    const { pool, request, beforeWrite, connect } = multiTableFixture();
    for (const invalid of [
      { ...request, tables: [] },
      { ...request, tables: [request.tables[0]!, request.tables[0]!] },
      { ...request, schema: 'public', table: 'events' },
    ]) {
      await expect(
        preparePostgresReplicationResources(pool, invalid, beforeWrite),
      ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    }
    expect(connect).not.toHaveBeenCalled();
  });

  it('reports missing resources without implying a self-managed configuration', async () => {
    const query = vi.fn(async (sql: string) => {
      if (sql.includes('pg_control_system')) {
        return {
          rows: [{ database_name: 'demo', database_oid: '1', system_identifier: 'system-1' }],
        };
      }
      if (sql.includes('FROM pg_class')) {
        return { rows: [{ oid: '10', primary_key: true, relreplident: 'd' }] };
      }
      if (sql.includes('FROM pg_replication_slots')) return { rows: [] };
      if (sql.includes('FROM pg_publication')) return { rows: [] };
      throw new Error(`unexpected SQL: ${sql}`);
    });
    const release = vi.fn();
    const pool = {
      connect: vi.fn(async () => ({ query, release })),
    } as unknown as Pool;

    await expect(
      readPostgresReplicationResources(pool, {
        publicationName: 'demo_pub',
        schema: 'public',
        slotName: 'demo_slot',
        table: 'events',
      }),
    ).rejects.toMatchObject({
      code: 'PG_RESOURCE_NOT_FOUND',
      summary: '指定的复制槽或发布不存在，请先准备或恢复资源。',
    });
    expect(
      query.mock.calls.some(([sql]) =>
        /pg_advisory_lock|create\s+publication|pg_create_logical_replication_slot/iu.test(sql),
      ),
    ).toBe(false);
    expect(release).toHaveBeenCalledWith(true);
  });

  it('still creates missing platform-managed resources and returns their identities', async () => {
    let slotExists = false;
    let publicationExists = false;
    const query = vi.fn(async (sql: string) => {
      if (sql.includes('pg_advisory_lock')) return { rows: [] };
      if (sql.includes('pg_control_system')) {
        return {
          rows: [{ database_name: 'demo', database_oid: '1', system_identifier: 'system-1' }],
        };
      }
      if (sql.includes('FROM pg_class')) {
        return { rows: [{ oid: '10', primary_key: true, relreplident: 'd' }] };
      }
      if (sql.includes('FROM pg_replication_slots')) {
        return {
          rows: slotExists
            ? [{ active: false, database: 'demo', plugin: 'pgoutput', slot_type: 'logical' }]
            : [],
        };
      }
      if (sql.includes('FROM pg_publication')) {
        return {
          rows: publicationExists
            ? [
                {
                  includes_table: true,
                  oid: '20',
                  pubdelete: true,
                  pubinsert: true,
                  pubtruncate: true,
                  pubupdate: true,
                },
              ]
            : [],
        };
      }
      if (sql.startsWith('CREATE PUBLICATION')) {
        publicationExists = true;
        return { rows: [] };
      }
      if (sql.includes('pg_create_logical_replication_slot')) {
        slotExists = true;
        return { rows: [{ slot_name: 'demo_slot' }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    });
    const release = vi.fn();
    const pool = {
      connect: vi.fn(async () => ({ query, release })),
    } as unknown as Pool;
    const beforeWrite = vi.fn(async () => undefined);

    await expect(
      preparePostgresReplicationResources(
        pool,
        {
          publicationName: 'demo_pub',
          resourceMode: 'PLATFORM_MANAGED',
          schema: 'public',
          slotName: 'demo_slot',
          table: 'events',
        },
        beforeWrite,
      ),
    ).resolves.toEqual({
      databaseName: 'demo',
      publication: {
        created: true,
        name: 'demo_pub',
        stableIdentity: 'system-1:1:publication:20',
      },
      slot: {
        created: true,
        name: 'demo_slot',
        stableIdentity: 'system-1:1:slot:demo_slot',
      },
    });
    expect(beforeWrite).toHaveBeenCalledWith(
      expect.objectContaining({ publicationIdentity: null, slotIdentity: null }),
    );
    expect(release).toHaveBeenCalledWith(true);
  });
});
