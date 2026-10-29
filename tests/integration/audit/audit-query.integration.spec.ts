import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AuditQueryService } from '../../../apps/api/src/audit-query/audit-query.service.js';
import {
  AuditLedgerService,
  type AppendAuditEventInput,
} from '../../../apps/api/src/audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const databaseRole = `scs_audit_query_${suffix}`;
const metadataDatabase = `scs_audit_query_${suffix}`;
const databasePassword = randomBytes(24).toString('base64url');

let pool: Pool | undefined;
let platformDatabase: PlatformDatabaseService | undefined;
let auditLedger: AuditLedgerService | undefined;
let auditQuery: AuditQueryService | undefined;

const administratorId = randomUUID();
const primaryTaskId = randomUUID();
const secondaryTaskId = randomUUID();

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
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

function requirePool(): Pool {
  if (pool === undefined) {
    throw new Error('audit query integration pool is not initialized');
  }
  return pool;
}

function requireAuditLedger(): AuditLedgerService {
  if (auditLedger === undefined) {
    throw new Error('audit ledger service is not initialized');
  }
  return auditLedger;
}

function requireAuditQuery(): AuditQueryService {
  if (auditQuery === undefined) {
    throw new Error('audit query service is not initialized');
  }
  return auditQuery;
}

async function inTransaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await requirePool().connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function append(input: AppendAuditEventInput): Promise<string> {
  return inTransaction((client) => requireAuditLedger().append(client, input));
}

beforeAll(async () => {
  const running = docker(['inspect', '--format', '{{.State.Running}}', postgresContainer]);
  if (running !== 'true') {
    throw new Error(`PostgreSQL container ${postgresContainer} is not running`);
  }
  const portOutput = docker(['port', postgresContainer, '5432/tcp']);
  const port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(port) || port <= 0) {
    throw new Error(`unable to resolve PostgreSQL port: ${portOutput}`);
  }

  psqlAsContainerAdministrator(
    'postgres',
    `CREATE ROLE "${databaseRole}" LOGIN PASSWORD '${databasePassword}';
     CREATE DATABASE "${metadataDatabase}" OWNER "${databaseRole}";`,
  );

  pool = new Pool({
    database: metadataDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port,
    user: databaseRole,
  });
  const bundle = await loadMigrationBundle();
  const migration = await new MigrationRunner(new PgMigrationStore(pool)).migrate(
    bundle,
    randomUUID(),
    { expectedState: 'ABSENT' },
  );
  expect(migration.appliedVersions).toEqual(bundle.migrations.map(({ version }) => version));

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
  await requirePool().query(
    `INSERT INTO ape_dts_platform.users (
       id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
     ) VALUES ($1::uuid, $2, $3, 'ADMIN', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6)`,
    [
      administratorId,
      `audit-query-admin-${suffix}`,
      'Audit query integration administrator',
      JSON.stringify({ memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 }),
      Buffer.alloc(16, 7),
      Buffer.alloc(32, 8),
    ],
  );

  auditLedger = new AuditLedgerService(platformDatabase);
  auditQuery = new AuditQueryService(platformDatabase);

  await append({
    action: 'TASK_CREATED',
    actorType: 'USER',
    actorUserId: administratorId,
    details: { case: 'primary-user-created' },
    subjectId: primaryTaskId,
    subjectType: 'TASK',
  });
  await append({
    action: 'TASK_CREATED',
    actorType: 'SYSTEM',
    actorUserId: null,
    details: { case: 'primary-system-created' },
    subjectId: primaryTaskId,
    subjectType: 'TASK',
  });
  await append({
    action: 'TASK_STARTED',
    actorType: 'USER',
    actorUserId: administratorId,
    details: { case: 'primary-user-started' },
    subjectId: primaryTaskId,
    subjectType: 'TASK',
  });
  await append({
    action: 'TASK_CREATED',
    actorType: 'USER',
    actorUserId: administratorId,
    details: { case: 'secondary-user-created' },
    subjectId: secondaryTaskId,
    subjectType: 'TASK',
  });
  await append({
    action: 'TASK_CREATED',
    actorType: 'USER',
    actorUserId: administratorId,
    details: { case: 'primary-data-source-created' },
    subjectId: primaryTaskId,
    subjectType: 'DATA_SOURCE',
  });
  await append({
    action: 'TASK_CREATED',
    actorType: 'DEPLOYMENT_ADMIN',
    actorUserId: null,
    details: { case: 'primary-deployment-admin-created' },
    subjectId: primaryTaskId,
    subjectType: 'TASK',
  });
}, 120_000);

afterAll(async () => {
  await platformDatabase?.onApplicationShutdown();
  await pool?.end();
  try {
    psqlAsContainerAdministrator(
      'postgres',
      `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
        WHERE datname = '${metadataDatabase}'
          AND pid <> pg_backend_pid();
       DROP DATABASE IF EXISTS "${metadataDatabase}";
       DROP ROLE IF EXISTS "${databaseRole}";`,
    );
  } catch (error) {
    process.stderr.write(`failed to clean up audit query PostgreSQL resources: ${String(error)}\n`);
  }
});

describe('audit queries against PostgreSQL', () => {
  it('applies actor, action, subject type, and subject ID as exact filters', async () => {
    const actor = await requireAuditQuery().list({ actorType: 'SYSTEM', limit: 20 });
    expect(actor.events.map((event) => event.details.case)).toEqual(['primary-system-created']);

    const action = await requireAuditQuery().list({ action: 'TASK_STARTED', limit: 20 });
    expect(action.events.map((event) => event.details.case)).toEqual(['primary-user-started']);

    const subjectType = await requireAuditQuery().list({
      limit: 20,
      subjectType: 'DATA_SOURCE',
    });
    expect(subjectType.events.map((event) => event.details.case)).toEqual([
      'primary-data-source-created',
    ]);

    const subjectId = await requireAuditQuery().list({ limit: 20, subjectId: secondaryTaskId });
    expect(subjectId.events.map((event) => event.details.case)).toEqual(['secondary-user-created']);

    const combined = await requireAuditQuery().list({
      action: 'TASK_CREATED',
      actorType: 'USER',
      limit: 20,
      subjectId: primaryTaskId,
      subjectType: 'TASK',
    });
    expect(combined.events.map((event) => event.details.case)).toEqual(['primary-user-created']);
  });

  it('pages newest-first without duplicates or omissions', async () => {
    const expected = await requirePool().query<{ sequence: string }>(
      `SELECT sequence::text
         FROM ape_dts_platform.audit_events
        ORDER BY occurred_at DESC, sequence DESC`,
    );

    const first = await requireAuditQuery().list({ limit: 3 });
    expect(first.events).toHaveLength(3);
    expect(first.nextCursor).not.toBeNull();

    const second = await requireAuditQuery().list({ cursor: first.nextCursor!, limit: 3 });
    expect(second.events).toHaveLength(3);
    expect(second.nextCursor).toBeNull();

    const sequences = [...first.events, ...second.events].map((event) => event.sequence);
    expect(new Set(sequences).size).toBe(sequences.length);
    expect(sequences).toEqual(expected.rows.map((row) => row.sequence));
  });

  it('returns null actorUserId for system-produced facts', async () => {
    const result = await requireAuditQuery().list({ actorType: 'SYSTEM', limit: 20 });

    expect(result.events).toEqual([
      expect.objectContaining({
        actorType: 'SYSTEM',
        actorUserId: null,
        details: { case: 'primary-system-created' },
      }),
    ]);
  });
});
