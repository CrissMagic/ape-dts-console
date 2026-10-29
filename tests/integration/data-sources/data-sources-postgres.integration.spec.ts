import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { KafkaDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/kafka-data-source.adapter.js';
import { MysqlCompatibleDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/mysql-compatible-data-source.adapter.js';
import { PostgresDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/postgres-data-source.adapter.js';
import { DataSourceAdapterRegistry } from '../../../apps/api/src/data-sources/adapters/types.js';
import {
  DataSourcesService,
  DataSourcesServiceError,
} from '../../../apps/api/src/data-sources/data-sources.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import type { UserSummary } from '../../../packages/contracts/src/index.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const databaseRole = `scs_u6_${suffix}`;
const metadataDatabase = `scs_u6_meta_${suffix}`;
const sourceDatabase = `scs_u6_source_${suffix}`;
const databasePassword = randomBytes(24).toString('base64url');

let port = 0;
let metadataPool: Pool | undefined;
let sourcePool: Pool | undefined;
let platformDatabase: PlatformDatabaseService | undefined;
let service: DataSourcesService | undefined;
let audit: AuditLedgerService | undefined;

const administrator: UserSummary = {
  displayName: 'Unit 6 administrator',
  id: randomUUID(),
  role: 'ADMIN',
  status: 'ACTIVE',
  username: `unit6-admin-${suffix}`,
};

const developer: UserSummary = {
  displayName: 'Unit 6 developer',
  id: randomUUID(),
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: `unit6-developer-${suffix}`,
};

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function psqlAsContainerAdministrator(database: string, sql: string): void {
  docker(
    [
      'exec',
      '--interactive',
      postgresContainer,
      'psql',
      '--username',
      'postgres',
      '--dbname',
      database,
      '--set',
      'ON_ERROR_STOP=1',
    ],
    sql,
  );
}

function requireService(): DataSourcesService {
  if (service === undefined) {
    throw new Error('data source service is not initialized');
  }
  return service;
}

function requireDatabase(): PlatformDatabaseService {
  if (platformDatabase === undefined) {
    throw new Error('platform database is not initialized');
  }
  return platformDatabase;
}

async function delay(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForGrantToBlockOnDeveloper(): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const result = await requireDatabase()
      .requirePool()
      .query<{ count: string }>(
        `SELECT count(*)::text AS count
       FROM pg_stat_activity
       WHERE datname = current_database()
         AND usename = current_user
         AND wait_event_type = 'Lock'
         AND query LIKE '%FROM ape_dts_platform.users%'`,
      );
    if (Number(result.rows[0]?.count ?? '0') > 0) {
      return;
    }
    await delay(25);
  }
  throw new Error('data source grant did not reach the expected user lock');
}

beforeAll(async () => {
  const running = docker(['inspect', '--format', '{{.State.Running}}', postgresContainer]);
  if (running !== 'true') {
    throw new Error(`PostgreSQL container ${postgresContainer} is not running`);
  }
  const portOutput = docker(['port', postgresContainer, '5432/tcp']);
  port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(port) || port <= 0) {
    throw new Error(`unable to resolve PostgreSQL port: ${portOutput}`);
  }

  psqlAsContainerAdministrator(
    'postgres',
    `CREATE ROLE ${quoteIdentifier(databaseRole)} LOGIN PASSWORD '${databasePassword}';
     CREATE DATABASE ${quoteIdentifier(metadataDatabase)} OWNER ${quoteIdentifier(databaseRole)};
     CREATE DATABASE ${quoteIdentifier(sourceDatabase)} OWNER ${quoteIdentifier(databaseRole)};`,
  );

  metadataPool = new Pool({
    database: metadataDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port,
    user: databaseRole,
  });
  sourcePool = new Pool({
    database: sourceDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port,
    user: databaseRole,
  });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(metadataPool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  await sourcePool.query(
    `CREATE TABLE public.unit6_orders (
       id bigint PRIMARY KEY,
       note text NOT NULL
     );
     INSERT INTO public.unit6_orders (id, note) VALUES (1, 'live metadata row');`,
  );

  platformDatabase = new PlatformDatabaseService();
  await platformDatabase.inspectAndUse(
    {
      database: metadataDatabase,
      host: '127.0.0.1',
      password: databasePassword,
      port,
      tls: { mode: 'DISABLE' },
      username: databaseRole,
    },
    bundle.manifest,
  );
  await requireDatabase()
    .requirePool()
    .query(
      `INSERT INTO ape_dts_platform.users (
       id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
     ) VALUES
       ($1::uuid, $2, $3, 'ADMIN', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6),
       ($7::uuid, $8, $9, 'DEVELOPER', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6)`,
      [
        administrator.id,
        administrator.username,
        administrator.displayName,
        JSON.stringify({ memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 }),
        Buffer.alloc(16, 7),
        Buffer.alloc(32, 8),
        developer.id,
        developer.username,
        developer.displayName,
      ],
    );
  const unlockedKey = new UnlockedKeyService();
  unlockedKey.set(randomBytes(32));
  audit = new AuditLedgerService(platformDatabase);
  service = new DataSourcesService(
    platformDatabase,
    audit,
    unlockedKey,
    new DataSourceAdapterRegistry([
      new PostgresDataSourceAdapter(),
      new KafkaDataSourceAdapter(),
      new MysqlCompatibleDataSourceAdapter('MYSQL'),
      new MysqlCompatibleDataSourceAdapter('DORIS'),
    ]),
  );
}, 120_000);

afterAll(async () => {
  const failures: unknown[] = [];
  async function attempt(cleanup: () => Promise<void> | void): Promise<void> {
    try {
      await cleanup();
    } catch (error) {
      failures.push(error);
    }
  }

  await attempt(async () => await platformDatabase?.onApplicationShutdown());
  await attempt(async () => await metadataPool?.end());
  await attempt(async () => await sourcePool?.end());
  await attempt(() => {
    psqlAsContainerAdministrator(
      'postgres',
      `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname IN ('${metadataDatabase}', '${sourceDatabase}')
           AND pid <> pg_backend_pid();
       DROP DATABASE IF EXISTS ${quoteIdentifier(metadataDatabase)};
       DROP DATABASE IF EXISTS ${quoteIdentifier(sourceDatabase)};
       DROP ROLE IF EXISTS ${quoteIdentifier(databaseRole)};`,
    );
  });
  if (failures.length > 0) {
    throw new AggregateError(failures, 'failed to clean up Unit 6 PostgreSQL test resources');
  }
});

describe('data source catalog against the supplied PostgreSQL 12 container', () => {
  it('copies connection credentials into an independent source without inheriting grants', async () => {
    const catalog = requireService();
    const source = await catalog.create(administrator.id, {
      name: 'Unit 6 copy original',
      configuration: {
        type: 'POSTGRES',
        database: sourceDatabase,
        host: '127.0.0.1',
        port,
        username: databaseRole,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
      },
      credentials: { mode: 'SET', password: databasePassword },
    });
    await catalog.grant(administrator, source.id, { userId: developer.id });
    const copied = await catalog.copy(administrator.id, source.id, {
      name: 'unit 6 copied source',
      expectedRowVersion: source.rowVersion,
    });
    expect(copied.id).not.toBe(source.id);
    expect(copied.configuration).toEqual(source.configuration);
    expect(copied.name).toBe('unit 6 copied source');
    expect(JSON.stringify(copied)).not.toContain(databasePassword);
    await expect(catalog.get(developer, copied.id)).rejects.toMatchObject({
      code: 'DATA_SOURCE_ACCESS_DENIED',
    });
    await expect(catalog.test(administrator, copied.id)).resolves.toMatchObject({
      state: 'SUCCEEDED',
    });
    const secrets = await requireDatabase()
      .requirePool()
      .query<{ id: string; owner_id: string; ciphertext: Buffer }>(
        "SELECT id,owner_id,ciphertext FROM ape_dts_platform.secrets WHERE owner_type='DATA_SOURCE' AND owner_id=ANY($1::uuid[]) AND destroyed_at IS NULL ORDER BY owner_id",
        [[source.id, copied.id]],
      );
    expect(secrets.rows).toHaveLength(2);
    expect(secrets.rows[0]!.id).not.toBe(secrets.rows[1]!.id);
    expect(secrets.rows[0]!.ciphertext.equals(secrets.rows[1]!.ciphertext)).toBe(false);
    await expect(
      catalog.copy(administrator.id, source.id, {
        name: 'Unit 6 stale copy',
        expectedRowVersion: '999',
      }),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_VERSION_CONFLICT' });
    const countSecrets = async () => {
      const result = await requireDatabase()
        .requirePool()
        .query<{ count: string }>(
          "SELECT count(*)::text AS count FROM ape_dts_platform.secrets WHERE owner_type = 'DATA_SOURCE'",
        );
      return result.rows[0]!.count;
    };
    const secretCountBeforeConflict = await countSecrets();
    await expect(
      catalog.copy(administrator.id, source.id, {
        name: copied.name,
        expectedRowVersion: source.rowVersion,
      }),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_NAME_ALREADY_EXISTS' });
    expect(await countSecrets()).toBe(secretCountBeforeConflict);
    await catalog.delete(administrator.id, source.id, source.rowVersion);
    await expect(
      catalog.copy(administrator.id, source.id, {
        name: 'unit 6 deleted copy',
        expectedRowVersion: source.rowVersion,
      }),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_NOT_FOUND' });
    await expect(catalog.test(administrator, copied.id)).resolves.toMatchObject({
      state: 'SUCCEEDED',
    });
  });

  it('reads slot and sender lag statistics without inventing a zero delay for a missing slot', async () => {
    const progress = await new PostgresDataSourceAdapter().readProgress(
      {
        configuration: {
          type: 'POSTGRES',
          database: sourceDatabase,
          host: '127.0.0.1',
          port,
          username: databaseRole,
          tls: { mode: 'DISABLE', riskAcknowledged: true },
        },
        password: databasePassword,
      },
      { slotName: `scs_missing_${suffix}` },
    );
    expect(progress).toMatchObject({
      slotStatus: 'MISSING',
      replicationLagMilliseconds: null,
      unconfirmedWalBytes: null,
      retainedWalBytes: null,
    });
  });

  it('encrypts connection passwords, enforces grants, reads live metadata, and destroys the secret on delete', async () => {
    const catalog = requireService();
    const source = await catalog.create(administrator.id, {
      configuration: {
        database: sourceDatabase,
        host: '127.0.0.1',
        port,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
        type: 'POSTGRES',
        username: databaseRole,
      },
      credentials: { mode: 'SET', password: databasePassword },
      name: 'Unit 6 live PostgreSQL',
    });

    const secret = await requireDatabase()
      .requirePool()
      .query<{ ciphertext: Buffer }>(
        `SELECT secrets.ciphertext
       FROM ape_dts_platform.data_sources AS sources
       JOIN ape_dts_platform.secrets ON secrets.id = sources.connection_secret_id
       WHERE sources.id = $1::uuid`,
        [source.id],
      );
    expect(secret.rows[0]?.ciphertext.toString('utf8')).not.toContain(databasePassword);
    await expect(catalog.list(developer)).resolves.toEqual([]);

    await catalog.grant(administrator, source.id, { userId: developer.id });
    await expect(catalog.list(developer)).resolves.toHaveLength(1);

    const test = await catalog.test(developer, source.id);
    expect(test).toMatchObject({ dataSourceId: source.id, purpose: 'MANUAL', state: 'SUCCEEDED' });

    const tables = await catalog.metadata(developer, source.id, {
      database: sourceDatabase,
      resource: 'TABLES',
      schema: 'public',
    });
    expect(tables).toMatchObject({ resource: 'TABLES', state: 'SUCCESS' });
    expect('items' in tables && tables.items.map((item) => item.name)).toContain('unit6_orders');

    const columns = await catalog.metadata(developer, source.id, {
      database: sourceDatabase,
      resource: 'COLUMNS',
      schema: 'public',
      table: 'unit6_orders',
    });
    expect('items' in columns && columns.items).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          attributes: expect.objectContaining({ isPrimaryKey: 'true' }),
          name: 'id',
        }),
      ]),
    );

    await catalog.revoke(administrator.id, source.id, developer.id);
    await expect(
      catalog.metadata(developer, source.id, { resource: 'DATABASES' }),
    ).rejects.toMatchObject(new DataSourcesServiceError('DATA_SOURCE_ACCESS_DENIED'));

    const retained = await catalog.update(administrator.id, source.id, {
      configuration: source.configuration,
      credentials: { mode: 'RETAIN' },
      expectedRowVersion: source.rowVersion,
      name: source.name,
    });
    expect(retained.rowVersion).toBe('2');

    const replacementPassword = randomBytes(24).toString('base64url');
    const replaced = await (async () => {
      psqlAsContainerAdministrator(
        'postgres',
        `ALTER ROLE ${quoteIdentifier(databaseRole)} PASSWORD '${replacementPassword}';`,
      );
      try {
        const updated = await catalog.update(administrator.id, source.id, {
          configuration: source.configuration,
          credentials: { mode: 'REPLACE', password: replacementPassword },
          expectedRowVersion: retained.rowVersion,
          name: source.name,
        });
        expect(updated.rowVersion).toBe('3');
        await expect(catalog.test(administrator, updated.id)).resolves.toMatchObject({
          state: 'SUCCEEDED',
        });
        return updated;
      } finally {
        psqlAsContainerAdministrator(
          'postgres',
          `ALTER ROLE ${quoteIdentifier(databaseRole)} PASSWORD '${databasePassword}';`,
        );
      }
    })();

    const replacementSecrets = await requireDatabase()
      .requirePool()
      .query<{
        ciphertext: Buffer;
        destroyed_at: Date | null;
      }>(
        `SELECT ciphertext, destroyed_at
       FROM ape_dts_platform.secrets
       WHERE owner_type = 'DATA_SOURCE'
         AND owner_id = $1::uuid
         AND purpose = 'CONNECTION_PASSWORD'
       ORDER BY created_at`,
        [source.id],
      );
    expect(replacementSecrets.rows).toHaveLength(2);
    expect(replacementSecrets.rows.filter((secret) => secret.destroyed_at !== null)).toHaveLength(
      1,
    );
    expect(replacementSecrets.rows.filter((secret) => secret.destroyed_at === null)).toHaveLength(
      1,
    );
    expect(
      replacementSecrets.rows
        .find((secret) => secret.destroyed_at === null)
        ?.ciphertext.toString('utf8'),
    ).not.toContain(replacementPassword);

    const deleted = await catalog.delete(administrator.id, replaced.id, replaced.rowVersion);
    expect(deleted).toMatchObject({ deletedAt: expect.any(String), state: 'DELETED' });

    const destroyed = await requireDatabase()
      .requirePool()
      .query<{ destroyed_at: Date | null }>(
        `SELECT destroyed_at
       FROM ape_dts_platform.secrets
       WHERE owner_type = 'DATA_SOURCE'
         AND owner_id = $1::uuid
         AND purpose = 'CONNECTION_PASSWORD'`,
        [source.id],
      );
    expect(destroyed.rows).toHaveLength(2);
    expect(destroyed.rows.every((secret) => secret.destroyed_at instanceof Date)).toBe(true);
    await expect(audit?.verify()).resolves.toMatchObject({ valid: true });
  });

  it('persists sanitized failed connection-test evidence and its audit event', async () => {
    const catalog = requireService();
    const incorrectPassword = randomBytes(24).toString('base64url');
    const source = await catalog.create(administrator.id, {
      configuration: {
        database: sourceDatabase,
        host: '127.0.0.1',
        port,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
        type: 'POSTGRES',
        username: databaseRole,
      },
      credentials: { mode: 'SET', password: incorrectPassword },
      name: 'Unit 6 failed PostgreSQL connection test',
    });

    const failed = await catalog.test(administrator, source.id);
    expect(failed).toMatchObject({
      dataSourceId: source.id,
      errorCode: 'CONNECTION_FAILED',
      identity: null,
      state: 'FAILED',
    });
    expect(failed.errorSummary).not.toContain(incorrectPassword);

    const recordedRun = await requireDatabase()
      .requirePool()
      .query<{
        error_code: string | null;
        error_summary: string | null;
        state: string;
      }>(
        `SELECT state, error_code, error_summary
       FROM ape_dts_platform.connection_test_runs
       WHERE id = $1::uuid`,
        [failed.id],
      );
    expect(recordedRun.rows).toEqual([
      expect.objectContaining({
        error_code: 'CONNECTION_FAILED',
        state: 'FAILED',
      }),
    ]);
    expect(recordedRun.rows[0]?.error_summary).not.toContain(incorrectPassword);

    const failureAudit = await requireDatabase()
      .requirePool()
      .query<{
        action: string;
        canonical_details: unknown;
      }>(
        `SELECT action, canonical_details
       FROM ape_dts_platform.audit_events
       WHERE subject_type = 'DATA_SOURCE'
         AND subject_id = $1::uuid
         AND action = 'DATA_SOURCE_CONNECTION_TEST_FAILED'`,
        [source.id],
      );
    expect(failureAudit.rows).toHaveLength(1);
    expect(JSON.stringify(failureAudit.rows[0]?.canonical_details)).not.toContain(
      incorrectPassword,
    );
    await expect(audit?.verify()).resolves.toMatchObject({ valid: true });
  });

  it('serializes a grant that is in progress before deleting the data source and revokes it before commit', async () => {
    const catalog = requireService();
    const source = await catalog.create(administrator.id, {
      configuration: {
        database: sourceDatabase,
        host: '127.0.0.1',
        port,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
        type: 'POSTGRES',
        username: databaseRole,
      },
      credentials: { mode: 'SET', password: databasePassword },
      name: 'Unit 6 concurrent grant PostgreSQL',
    });
    const blocker = await requireDatabase().requirePool().connect();
    let grant: Promise<unknown> | undefined;
    let deletion: Promise<unknown> | undefined;

    try {
      await blocker.query('BEGIN');
      await blocker.query(
        `SELECT id
         FROM ape_dts_platform.users
         WHERE id = $1::uuid
         FOR UPDATE`,
        [developer.id],
      );

      grant = catalog.grant(administrator, source.id, { userId: developer.id });
      void grant.catch(() => undefined);
      await waitForGrantToBlockOnDeveloper();

      let deletionSettled = false;
      deletion = catalog.delete(administrator.id, source.id, source.rowVersion).finally(() => {
        deletionSettled = true;
      });
      void deletion.catch(() => undefined);
      await delay(100);
      expect(deletionSettled).toBe(false);

      await blocker.query('COMMIT');
      await expect(grant).resolves.toMatchObject({ dataSourceId: source.id, user: developer });
      await expect(deletion).resolves.toMatchObject({ id: source.id, state: 'DELETED' });

      const activeGrants = await requireDatabase()
        .requirePool()
        .query<{ count: string }>(
          `SELECT count(*)::text AS count
         FROM ape_dts_platform.data_source_grants
         WHERE data_source_id = $1::uuid
           AND revoked_at IS NULL`,
          [source.id],
        );
      expect(activeGrants.rows[0]?.count).toBe('0');
    } finally {
      await blocker.query('ROLLBACK').catch(() => undefined);
      blocker.release();
      await Promise.all([grant?.catch(() => undefined), deletion?.catch(() => undefined)]);
    }
  });
});
