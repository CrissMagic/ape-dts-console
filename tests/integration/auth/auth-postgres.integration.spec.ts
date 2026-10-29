import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const uniqueSuffix = `${process.pid}-${Date.now()}`;
const containerName = `scs-platform-auth-postgres-test-${uniqueSuffix}`;
const postgresPassword = randomBytes(24).toString('base64url');
const postgresImage = process.env.SCS_TEST_POSTGRES_IMAGE ?? 'postgres:12';

let containerCreated = false;
let pool: Pool | undefined;

function docker(arguments_: string[], timeout = 120_000): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

async function waitForPostgres(): Promise<void> {
  const deadline = Date.now() + 30_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      docker([
        'exec',
        containerName,
        'pg_isready',
        '--username',
        'postgres',
        '--dbname',
        'postgres',
      ]);
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  throw lastError instanceof Error ? lastError : new Error('PostgreSQL did not become ready');
}

function requirePool(): Pool {
  if (pool === undefined) {
    throw new Error('test pool is not initialized');
  }
  return pool;
}

async function insertActiveAdministrator(username: string): Promise<string> {
  const id = randomUUID();
  await requirePool().query(
    `INSERT INTO ape_dts_platform.users (
      id,
      normalized_username,
      display_name,
      role,
      status,
      password_kdf_algorithm,
      password_kdf_parameters,
      password_salt,
      password_hash
    ) VALUES ($1, $2, $3, 'ADMIN', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6)`,
    [
      id,
      username,
      username,
      JSON.stringify({ memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 }),
      Buffer.alloc(16, 1),
      Buffer.alloc(32, 2),
    ],
  );
  return id;
}

beforeAll(async () => {
  docker([
    'run',
    '--pull',
    'never',
    '--detach',
    '--name',
    containerName,
    '--env',
    `POSTGRES_PASSWORD=${postgresPassword}`,
    '--publish',
    '127.0.0.1::5432',
    postgresImage,
  ]);
  containerCreated = true;
  await waitForPostgres();
  const portOutput = docker(['port', containerName, '5432/tcp']);
  const port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  pool = new Pool({
    database: 'postgres',
    host: '127.0.0.1',
    password: postgresPassword,
    port,
    user: 'postgres',
  });
  const bundle = await loadMigrationBundle();
  await expect(
    new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
      expectedState: 'ABSENT',
    }),
  ).resolves.toMatchObject({ appliedVersions: [1, 2, 3, 4, 5, 6, 7, 8] });
});

afterAll(async () => {
  await pool?.end();
  if (containerCreated) {
    try {
      docker(['rm', '--force', containerName]);
    } catch (error) {
      process.stderr.write(`failed to remove PostgreSQL test container: ${String(error)}\n`);
    }
  }
});

describe('PostgreSQL identity and audit invariants', () => {
  it('revokes sessions for security changes and protects the final active administrator', async () => {
    const firstAdministratorId = await insertActiveAdministrator('unit5-admin-one');
    const before = await requirePool().query<{ session_generation: string }>(
      'SELECT session_generation::text FROM ape_dts_platform.users WHERE id = $1',
      [firstAdministratorId],
    );

    await requirePool().query(
      `UPDATE ape_dts_platform.users
       SET password_hash = $2, updated_at = statement_timestamp()
       WHERE id = $1`,
      [firstAdministratorId, Buffer.alloc(32, 3)],
    );
    const after = await requirePool().query<{ session_generation: string }>(
      'SELECT session_generation::text FROM ape_dts_platform.users WHERE id = $1',
      [firstAdministratorId],
    );
    expect(Number(after.rows[0]?.session_generation)).toBe(
      Number(before.rows[0]?.session_generation) + 1,
    );

    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.users
         SET status = 'DISABLED', disabled_at = statement_timestamp()
         WHERE id = $1`,
        [firstAdministratorId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await insertActiveAdministrator('unit5-admin-two');
    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.users
         SET status = 'DISABLED', disabled_at = statement_timestamp()
         WHERE id = $1
         RETURNING status`,
        [firstAdministratorId],
      ),
    ).resolves.toMatchObject({ rows: [{ status: 'DISABLED' }] });
    await expect(
      requirePool().query('DELETE FROM ape_dts_platform.users WHERE id = $1', [
        firstAdministratorId,
      ]),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('accepts only an append-only audit chain whose previous hash matches the current tail', async () => {
    const administratorId = await insertActiveAdministrator('unit5-audit-admin');
    const firstEventId = randomUUID();
    const firstHash = Buffer.alloc(32, 4);
    await requirePool().query(
      `INSERT INTO ape_dts_platform.audit_events (
        event_id,
        actor_type,
        actor_user_id,
        action,
        subject_type,
        subject_id,
        canonical_details,
        previous_hash,
        event_hash
      ) VALUES ($1, 'USER', $2, 'UNIT5_FIRST', 'USER', $2, '{}'::jsonb, NULL, $3)`,
      [firstEventId, administratorId, firstHash],
    );
    await expect(
      requirePool().query(
        `INSERT INTO ape_dts_platform.audit_events (
          event_id,
          actor_type,
          actor_user_id,
          action,
          subject_type,
          subject_id,
          canonical_details,
          previous_hash,
          event_hash
        ) VALUES ($1, 'USER', $2, 'UNIT5_WRONG_TAIL', 'USER', $2, '{}'::jsonb, $3, $4)`,
        [randomUUID(), administratorId, Buffer.alloc(32, 9), Buffer.alloc(32, 5)],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      requirePool().query(
        `INSERT INTO ape_dts_platform.audit_events (
          event_id,
          actor_type,
          actor_user_id,
          action,
          subject_type,
          subject_id,
          canonical_details,
          previous_hash,
          event_hash
        ) VALUES ($1, 'USER', $2, 'UNIT5_SECOND', 'USER', $2, '{}'::jsonb, $3, $4)`,
        [randomUUID(), administratorId, firstHash, Buffer.alloc(32, 5)],
      ),
    ).resolves.toMatchObject({ rowCount: 1 });
    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.audit_events
         SET canonical_details = '{"changed":true}'::jsonb
         WHERE event_id = $1`,
        [firstEventId],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      requirePool().query('DELETE FROM ape_dts_platform.audit_events WHERE event_id = $1', [
        firstEventId,
      ]),
    ).rejects.toMatchObject({ code: '23514' });
  });
});
