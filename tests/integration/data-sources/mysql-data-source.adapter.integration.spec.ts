import { execFileSync } from 'node:child_process';
import { createServer } from 'node:net';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { MysqlCompatibleDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/mysql-compatible-data-source.adapter.js';
import { DataSourceAdapterRegistry } from '../../../apps/api/src/data-sources/adapters/types.js';
import { DataSourcesService } from '../../../apps/api/src/data-sources/data-sources.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const containerName = `scs-unit6-mysql-${process.pid}-${Date.now()}`;
const databaseName = `unit6_${process.pid}_${Date.now()}`;
const mysqlImage = process.env.SCS_TEST_MYSQL_IMAGE ?? 'mysql:8.0.36';
const rootPassword = 'unit6-temporary-root-password';
const username = 'unit6_user';
const userPassword = 'unit6-temporary-user-password';

let containerCreated = false;
let hostPort = 0;

function docker(arguments_: readonly string[], timeout = 120_000): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

async function availablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        server.close(() => reject(new Error('unable to allocate MySQL test port')));
        return;
      }
      const { port } = address;
      server.close((error) => (error === undefined ? resolve(port) : reject(error)));
    });
  });
}

function input() {
  return {
    configuration: {
      database: databaseName,
      host: '127.0.0.1',
      port: hostPort,
      tls: { mode: 'DISABLE' as const, riskAcknowledged: true as const },
      type: 'MYSQL' as const,
      username,
    },
    password: userPassword,
  };
}

async function waitForMysql(): Promise<void> {
  const deadline = Date.now() + 90_000;
  let lastError: unknown;
  const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');
  while (Date.now() < deadline) {
    try {
      await adapter.test(input());
      return;
    } catch (error) {
      lastError = error;
      await new Promise<void>((resolve) => setTimeout(resolve, 500));
    }
  }
  throw lastError instanceof Error ? lastError : new Error('MySQL did not become ready');
}

beforeAll(async () => {
  hostPort = await availablePort();
  docker([
    'run',
    '--pull',
    'never',
    '--detach',
    '--name',
    containerName,
    '--publish',
    `127.0.0.1:${hostPort}:3306`,
    '--env',
    `MYSQL_DATABASE=${databaseName}`,
    '--env',
    `MYSQL_PASSWORD=${userPassword}`,
    '--env',
    `MYSQL_ROOT_PASSWORD=${rootPassword}`,
    '--env',
    `MYSQL_USER=${username}`,
    mysqlImage,
    '--server-id=17',
    '--log-bin=mysql-bin',
    '--binlog-format=ROW',
    '--binlog-row-image=FULL',
  ]);
  containerCreated = true;
  await waitForMysql();
  docker([
    'exec',
    '--env',
    `MYSQL_PWD=${userPassword}`,
    containerName,
    'mysql',
    '--protocol=TCP',
    '--host',
    '127.0.0.1',
    '--user',
    username,
    '--database',
    databaseName,
    '--execute',
    `CREATE TABLE source_rows (
       id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
       display_name VARCHAR(64) NOT NULL,
       optional_note TEXT NULL
     );
     CREATE TABLE unique_only (id BIGINT NOT NULL UNIQUE);
     CREATE TABLE composite_primary (first_id BIGINT NOT NULL, second_id BIGINT NOT NULL, PRIMARY KEY(first_id, second_id));`,
  ]);
}, 120_000);

afterAll(() => {
  if (!containerCreated) {
    return;
  }
  docker(['rm', '--force', '--volumes', containerName]);
});

describe('MySQL data source adapter', () => {
  it('reads a live server plus existing databases, tables, and columns without managing them', async () => {
    const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');

    await expect(adapter.test(input())).resolves.toMatchObject({
      kind: 'MYSQL_DATABASE',
      serverVersion: expect.stringMatching(/^8\.0\./u),
      value: expect.stringContaining(`:${databaseName}`),
    });
    await expect(adapter.metadata(input(), { resource: 'DATABASES' })).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ name: databaseName })]),
    );
    await expect(
      adapter.metadata(input(), { database: databaseName, resource: 'TABLES' }),
    ).resolves.toEqual(expect.arrayContaining([expect.objectContaining({ name: 'source_rows' })]));
    await expect(
      adapter.metadata(input(), {
        database: databaseName,
        resource: 'COLUMNS',
        table: 'source_rows',
      }),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          identity: '1',
          name: 'id',
          attributes: expect.objectContaining({ isPrimaryKey: 'true' }),
        }),
        expect.objectContaining({
          identity: '2',
          name: 'display_name',
          attributes: expect.objectContaining({ isPrimaryKey: 'false' }),
        }),
        expect.objectContaining({ identity: '3', name: 'optional_note' }),
      ]),
    );
  }, 120_000);

  it('distinguishes unique-only tables from real composite primary keys', async () => {
    const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');
    const unique = await adapter.metadata(input(), {
      database: databaseName,
      resource: 'COLUMNS',
      table: 'unique_only',
    });
    expect(unique.map((column) => column.attributes.isPrimaryKey)).toEqual(['false']);
    const composite = await adapter.metadata(input(), {
      database: databaseName,
      resource: 'COLUMNS',
      table: 'composite_primary',
    });
    expect(composite.map((column) => column.attributes.isPrimaryKey)).toEqual(['true', 'true']);
  });

  it('requires replication-client access for inspecting the Binlog, without blocking normal connections', async () => {
    const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');
    await expect(adapter.test(input())).resolves.toMatchObject({ kind: 'MYSQL_DATABASE' });
    await expect(adapter.inspectMysqlCdc(input())).rejects.toMatchObject({
      code: 'PERMISSION_DENIED',
    });
    docker([
      'exec',
      '--env',
      `MYSQL_PWD=${rootPassword}`,
      containerName,
      'mysql',
      '--user',
      'root',
      '--execute',
      `GRANT REPLICATION CLIENT ON *.* TO '${username}'@'%'`,
    ]);
    const inspection = await adapter.inspectMysqlCdc(input());
    expect(inspection.checks.every((check) => check.state === 'PASSED')).toBe(true);
    expect(inspection.binlog).toMatchObject({
      enabled: true,
      format: 'ROW',
      rowImage: 'FULL',
      serverId: 17,
    });
    expect(inspection.position).toMatchObject({
      filename: expect.stringMatching(/^mysql-bin\.\d+$/u),
      position: expect.any(Number),
    });
    expect((await adapter.test(input())).value).toBe(inspection.identity.value);

    docker([
      'exec',
      '--env',
      `MYSQL_PWD=${rootPassword}`,
      containerName,
      'mysql',
      '--user',
      'root',
      '--execute',
      "SET GLOBAL binlog_row_image = 'MINIMAL'",
    ]);
    try {
      const incompatible = await adapter.inspectMysqlCdc(input());
      expect(incompatible.checks.find((check) => check.key === 'BINLOG_ROW_IMAGE')?.state).toBe(
        'FAILED',
      );
    } finally {
      docker([
        'exec',
        '--env',
        `MYSQL_PWD=${rootPassword}`,
        containerName,
        'mysql',
        '--user',
        'root',
        '--execute',
        "SET GLOBAL binlog_row_image = 'FULL'",
      ]);
    }
  });

  it('persists MySQL identities through encrypted credentials, enforces grants and rejects stale inspections', async () => {
    const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
    const metadataName = `scs_mysql_${process.pid}_${Date.now()}`;
    const metadataPassword = randomBytes(24).toString('base64url');
    const postgresPort = Number(docker(['port', postgresContainer, '5432/tcp']).split(':').at(-1));
    const platform = new PlatformDatabaseService();
    const key = new UnlockedKeyService();
    let pool: Pool | undefined;
    const psql = (sql: string) =>
      execFileSync(
        'docker',
        [
          'exec',
          '--interactive',
          postgresContainer,
          'psql',
          '--username',
          'postgres',
          '--dbname',
          'postgres',
          '--set',
          'ON_ERROR_STOP=1',
        ],
        { input: sql, stdio: ['pipe', 'pipe', 'pipe'], timeout: 120_000 },
      );
    try {
      psql(`CREATE ROLE ${metadataName} LOGIN PASSWORD '${metadataPassword}';
        CREATE DATABASE ${metadataName} OWNER ${metadataName};`);
      pool = new Pool({
        host: '127.0.0.1',
        port: postgresPort,
        database: metadataName,
        user: metadataName,
        password: metadataPassword,
      });
      const bundle = await loadMigrationBundle();
      await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
        expectedState: 'ABSENT',
      });
      await platform.inspectAndUse(
        {
          host: '127.0.0.1',
          port: postgresPort,
          database: metadataName,
          username: metadataName,
          password: metadataPassword,
          tls: { mode: 'DISABLE' },
        },
        bundle.manifest,
      );
      const administrator = {
        id: randomUUID(),
        username: 'admin',
        displayName: 'MySQL integration admin',
        role: 'ADMIN' as const,
        status: 'ACTIVE' as const,
      };
      const developer = {
        id: randomUUID(),
        username: 'mysql_developer',
        displayName: 'MySQL integration developer',
        role: 'DEVELOPER' as const,
        status: 'ACTIVE' as const,
      };
      for (const user of [administrator, developer]) {
        await pool.query(
          `INSERT INTO ape_dts_platform.users (
          id, normalized_username, display_name, role, status,
          password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
        ) VALUES ($1, $2, $3, $4, 'ACTIVE', 'ARGON2ID', $5, $6, $7)`,
          [
            user.id,
            user.username,
            user.displayName,
            user.role,
            { memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 },
            Buffer.alloc(16, 7),
            Buffer.alloc(32, 8),
          ],
        );
      }
      key.set(randomBytes(32));
      const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');
      const service = new DataSourcesService(
        platform,
        new AuditLedgerService(platform),
        key,
        new DataSourceAdapterRegistry([adapter]),
      );
      const source = await service.create(administrator.id, {
        configuration: { ...input().configuration, username: 'root' },
        credentials: { mode: 'SET', password: rootPassword },
        name: 'Isolated MySQL CDC source',
      });
      await expect(
        service.inspectMysqlCdc(developer, source.id, source.rowVersion),
      ).rejects.toMatchObject({ code: 'DATA_SOURCE_ACCESS_DENIED' });
      await service.grant(administrator, source.id, { userId: developer.id });
      const tested = await service.test(developer, source.id);
      expect(tested.state).toBe('SUCCEEDED');
      expect(tested.identity?.kind).toBe('MYSQL_DATABASE');
      const persisted = await pool.query(
        `SELECT identity_kind, identity_value
        FROM ape_dts_platform.data_source_identity_observations WHERE connection_test_run_id = $1`,
        [tested.id],
      );
      expect(persisted.rows).toEqual([
        { identity_kind: 'MYSQL_DATABASE', identity_value: tested.identity?.value },
      ]);
      const inspect = adapter.inspectMysqlCdc.bind(adapter);
      await expect(service.inspectMysqlCdc(developer, source.id, '2')).rejects.toMatchObject({
        code: 'DATA_SOURCE_VERSION_CONFLICT',
      });
      expect(
        (await service.inspectMysqlCdc(developer, source.id, source.rowVersion)).position,
      ).not.toBeNull();
      const systemInspection = await service.inspectMysqlCdcForSystem(source.id, source.rowVersion);
      expect(systemInspection.identity.value).toBe(tested.identity?.value);
      const binlogs = await service.readMysqlBinlogsForSystem(source.id, source.rowVersion);
      expect(binlogs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            filename: systemInspection.position!.filename,
            sizeBytes: expect.stringMatching(/^\d+$/u),
          }),
        ]),
      );
      await expect(service.readMysqlBinlogsForSystem(source.id, '2')).rejects.toMatchObject({
        code: 'DATA_SOURCE_VERSION_CONFLICT',
      });
      adapter.inspectMysqlCdc = async (connection) => {
        const inspection = await inspect(connection);
        await service.update(administrator.id, source.id, {
          configuration: source.configuration,
          credentials: { mode: 'RETAIN' },
          expectedRowVersion: source.rowVersion,
          name: source.name,
        });
        return inspection;
      };
      await expect(
        service.inspectMysqlCdc(developer, source.id, source.rowVersion),
      ).rejects.toMatchObject({ code: 'DATA_SOURCE_CHANGED_DURING_OPERATION' });
      adapter.inspectMysqlCdc = inspect;
      await expect(
        service.inspectMysqlCdcForSystem(source.id, source.rowVersion),
      ).rejects.toMatchObject({ code: 'DATA_SOURCE_VERSION_CONFLICT' });
      await service.revoke(administrator.id, source.id, developer.id);
      await expect(
        service.inspectMysqlCdc(developer, source.id, source.rowVersion),
      ).rejects.toMatchObject({ code: 'DATA_SOURCE_ACCESS_DENIED' });
    } finally {
      await platform.onApplicationShutdown();
      await pool?.end();
      key.clear();
      psql(`DROP DATABASE IF EXISTS ${metadataName}; DROP ROLE IF EXISTS ${metadataName};`);
    }
  }, 120_000);
});
