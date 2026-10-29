import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { PlatformReadinessService } from '../../../apps/api/src/health/platform-readiness.service.js';
import { MetadataAvailabilityService } from '../../../apps/api/src/metadata-availability/metadata-availability.service.js';

import {
  loadMigrationBundle,
  MigrationLockUnavailableError,
  MigrationRunner,
  PgMigrationStore,
  PgSchemaInspector,
  inspectControllerLockHolders,
  tryAcquireControllerLock,
  type MigrationBundle,
} from '../../../packages/database/src/index.js';

const uniqueSuffix = `${process.pid}-${Date.now()}`;
const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const databaseRole = `scs_bootstrap_${uniqueSuffix.replaceAll('-', '_')}`;
const metadataDatabase = `scs_bootstrap_meta_${uniqueSuffix.replaceAll('-', '_')}`;
const postgresPassword = randomBytes(24).toString('base64url');

let pool: Pool | undefined;
let postgresPort: number | undefined;
const auxiliaryPools: Pool[] = [];
const auxiliaryDatabases: string[] = [];
const controllers = new Set<PlatformDatabaseService>();

function docker(arguments_: string[], timeout = 120_000, input?: string): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout,
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
    120_000,
    sql,
  );
}

async function inspectCurrentDatabase() {
  if (pool === undefined) {
    throw new Error('test pool is not initialized');
  }
  const client = await pool.connect();
  try {
    const bundle = await loadMigrationBundle();
    return await new PgSchemaInspector().inspect(client, bundle.manifest);
  } finally {
    client.release();
  }
}

function throughVersion(bundle: MigrationBundle, version: number): MigrationBundle {
  return {
    manifest: {
      ...bundle.manifest,
      migrations: bundle.manifest.migrations.filter((migration) => migration.version <= version),
    },
    migrations: bundle.migrations.filter((migration) => migration.version <= version),
  };
}

async function createAuxiliaryDatabase(): Promise<Pool> {
  if (pool === undefined || postgresPort === undefined) {
    throw new Error('test PostgreSQL is not initialized');
  }
  const database = `scs_upgrade_${randomUUID().replaceAll('-', '')}`;
  await pool.query(`CREATE DATABASE "${database}"`);
  auxiliaryDatabases.push(database);
  const auxiliary = new Pool({
    database,
    host: '127.0.0.1',
    password: postgresPassword,
    port: postgresPort,
    user: databaseRole,
  });
  auxiliaryPools.push(auxiliary);
  return auxiliary;
}

async function createControllerDatabase() {
  const observer = await createAuxiliaryDatabase();
  const name = await observer.query<{ name: string }>('SELECT current_database() AS name');
  const connection = {
    database: name.rows[0]!.name,
    host: '127.0.0.1',
    port: postgresPort!,
    username: databaseRole,
    password: postgresPassword,
    tls: { mode: 'DISABLE' as const },
  };
  const database = new PlatformDatabaseService();
  controllers.add(database);
  const bundle = await loadMigrationBundle();
  await database.inspectAndUse(connection, bundle.manifest);
  return { database, observer, connection, bundle };
}

async function insertVersion7RunFixture(database: Pool): Promise<{
  readonly profileId: string;
  readonly revisionId: string;
  readonly runId: string;
  readonly runWithoutObjectsId: string;
  readonly taskId: string;
  readonly userId: string;
}> {
  const userId = randomUUID();
  const sourceId = randomUUID();
  const targetId = randomUUID();
  const taskId = randomUUID();
  const revisionId = randomUUID();
  const runId = randomUUID();
  const runWithoutObjectsId = randomUUID();
  const profileId = randomUUID();
  const objectId = randomUUID();
  await database.query(
    `INSERT INTO ape_dts_platform.users (
       id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
     ) VALUES ($1::uuid, $2, 'Upgrade user', 'ADMIN', 'ACTIVE',
               'ARGON2ID', '{"memoryKiB":1024}'::jsonb, $3, $4)`,
    [userId, `upgrade-user-${userId}`, Buffer.alloc(16, 1), Buffer.alloc(32, 2)],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.data_sources (
       id, normalized_name, source_type, configuration, state,
       created_by_user_id, updated_by_user_id
     ) VALUES
       ($1::uuid, $2, 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', $5::uuid, $5::uuid),
       ($3::uuid, $4, 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', $5::uuid, $5::uuid)`,
    [sourceId, `upgrade-source-${sourceId}`, targetId, `upgrade-target-${targetId}`, userId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.tasks (
       id, normalized_name, display_name, task_type,
       source_data_source_id, target_data_source_id,
       desired_state, observed_state, observability_health, recovery_disposition,
       created_by_user_id, updated_by_user_id
     ) VALUES ($1::uuid, $2, 'Upgrade task', 'KAFKA_KAFKA',
               $3::uuid, $4::uuid, 'STOPPED', 'STOPPED', 'HEALTHY', 'NOT_APPLICABLE',
               $5::uuid, $5::uuid)`,
    [taskId, `upgrade-task-${taskId}`, sourceId, targetId, userId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.task_revisions (
       id, task_id, revision_number, state, configuration,
       created_by_user_id, updated_by_user_id
     ) VALUES ($1::uuid, $2::uuid, 1, 'READY_DRAFT', '{}'::jsonb, $3::uuid, $3::uuid)`,
    [revisionId, taskId, userId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.task_runs (
       id, task_id, task_revision_id, status, current_stage
     ) VALUES
       ($1::uuid, $2::uuid, $3::uuid, 'RUNNING', 'RUNNING'),
       ($4::uuid, $2::uuid, $3::uuid, 'RUNNING', 'RUNNING')`,
    [runId, taskId, revisionId, runWithoutObjectsId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.storage_profiles (
       id, profile_key, display_name, version, driver, config, state, created_by_user_id
     ) VALUES ($1::uuid, $2, 'Upgrade profile', 1, 'LOCAL',
               '{"relative_prefix":"upgrade"}'::jsonb, 'DRAFT', $3::uuid)`,
    [profileId, `upgrade-profile-${profileId}`, userId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.stored_objects (
       id, storage_profile_id, kind, object_key, size_bytes, sha256,
       content_type, state, created_by_user_id, available_at
     ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 3,
               decode(repeat('01', 32), 'hex'), 'text/plain', 'AVAILABLE',
               $4::uuid, statement_timestamp())`,
    [objectId, profileId, `runs/${runId}/historical.log`, userId],
  );
  await database.query(
    `INSERT INTO ape_dts_platform.log_segments (
       id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
       stored_object_id, byte_size, sha256, state
     ) VALUES ($1::uuid, $2::uuid, NULL, 'STDOUT', 1,
               statement_timestamp(), statement_timestamp(), $3::uuid, 3,
               decode(repeat('01', 32), 'hex'), 'AVAILABLE')`,
    [randomUUID(), runId, objectId],
  );
  return { profileId, revisionId, runId, runWithoutObjectsId, taskId, userId };
}

beforeAll(async () => {
  const running = docker(['inspect', '--format', '{{.State.Running}}', postgresContainer]);
  if (running !== 'true') {
    throw new Error(`PostgreSQL container ${postgresContainer} is not running`);
  }
  const portOutput = docker(['port', postgresContainer, '5432/tcp']);
  const port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  postgresPort = port;
  psqlAsContainerAdministrator(
    'postgres',
    `CREATE ROLE "${databaseRole}" LOGIN CREATEDB PASSWORD '${postgresPassword}';
     CREATE DATABASE "${metadataDatabase}" OWNER "${databaseRole}";`,
  );
  pool = new Pool({
    database: metadataDatabase,
    host: '127.0.0.1',
    max: 4,
    password: postgresPassword,
    port,
    user: databaseRole,
  });
});

afterAll(async () => {
  await Promise.all([...controllers].map((controller) => controller.onApplicationShutdown()));
  await Promise.all(auxiliaryPools.map(async (auxiliary) => await auxiliary.end()));
  await pool?.end();
  try {
    for (const database of auxiliaryDatabases) {
      psqlAsContainerAdministrator(
        'postgres',
        `SELECT pg_terminate_backend(pid)
           FROM pg_stat_activity
          WHERE datname = '${database}'
            AND pid <> pg_backend_pid();
         DROP DATABASE IF EXISTS "${database}";`,
      );
    }
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
    process.stderr.write(`failed to clean up bootstrap PostgreSQL resources: ${String(error)}\n`);
  }
});

describe('PostgreSQL bootstrap migrations', () => {
  it('rolls back interrupted initialization, rejects a concurrent migrator and reaches CURRENT once', async () => {
    if (pool === undefined) {
      throw new Error('test pool is not initialized');
    }
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(pool));

    expect(await inspectCurrentDatabase()).toMatchObject({ state: 'ABSENT' });
    await expect(
      runner.migrate(bundle, randomUUID(), {
        afterMigrations: () => {
          throw new Error('simulated bootstrap interruption');
        },
        expectedState: 'ABSENT',
      }),
    ).rejects.toThrow('simulated bootstrap interruption');
    expect(await inspectCurrentDatabase()).toMatchObject({ state: 'ABSENT' });

    let releaseFirstMigration: (() => void) | undefined;
    const firstMigrationMayFinish = new Promise<void>((resolve) => {
      releaseFirstMigration = resolve;
    });
    let firstMigrationLocked: (() => void) | undefined;
    const firstMigrationHasLock = new Promise<void>((resolve) => {
      firstMigrationLocked = resolve;
    });
    const firstMigration = runner.migrate(bundle, randomUUID(), {
      afterMigrations: async () => {
        firstMigrationLocked?.();
        await firstMigrationMayFinish;
      },
      expectedState: 'ABSENT',
    });
    await firstMigrationHasLock;

    await expect(
      runner.migrate(bundle, randomUUID(), { expectedState: 'ABSENT' }),
    ).rejects.toBeInstanceOf(MigrationLockUnavailableError);
    releaseFirstMigration?.();
    await expect(firstMigration).resolves.toMatchObject({
      appliedVersions: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    });

    expect(await inspectCurrentDatabase()).toMatchObject({
      appliedVersion: 16,
      pendingVersions: [],
      state: 'CURRENT',
    });
  });

  it('enforces globally unique metrics ports until their release is recorded', async () => {
    if (pool === undefined) {
      throw new Error('test pool is not initialized');
    }
    const administratorId = randomUUID();
    const sourceId = randomUUID();
    const targetId = randomUUID();
    const firstTaskId = randomUUID();
    const secondTaskId = randomUUID();
    const firstRevisionId = randomUUID();
    const secondRevisionId = randomUUID();
    const firstRunId = randomUUID();
    const secondRunId = randomUUID();
    const occupiedPort = 36_101;
    const reusablePort = 36_102;
    const runtimeStatuses = [
      'STARTING',
      'RUNNING',
      'STOPPING',
      'STOPPED',
      'FAILED',
      'BLOCKED',
    ] as const;

    await pool.query(
      `INSERT INTO ape_dts_platform.users (
         id, normalized_username, display_name, role, status,
         password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
       ) VALUES ($1::uuid, $2, 'Metrics port administrator', 'ADMIN', 'ACTIVE',
                 'ARGON2ID', '{"memoryKiB":1024}'::jsonb, $3, $4)`,
      [
        administratorId,
        `metrics-port-admin-${administratorId}`,
        Buffer.alloc(16, 1),
        Buffer.alloc(32, 2),
      ],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.data_sources (
         id, normalized_name, source_type, configuration, state,
         created_by_user_id, updated_by_user_id
       ) VALUES
         ($1::uuid, $2, 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', $5::uuid, $5::uuid),
         ($3::uuid, $4, 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', $5::uuid, $5::uuid)`,
      [
        sourceId,
        `metrics-port-source-${sourceId}`,
        targetId,
        `metrics-port-target-${targetId}`,
        administratorId,
      ],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.tasks (
         id, normalized_name, display_name, task_type,
         source_data_source_id, target_data_source_id,
         desired_state, observed_state, observability_health, recovery_disposition,
         created_by_user_id, updated_by_user_id
       ) VALUES
         ($1::uuid, $2, 'First metrics port task', 'KAFKA_KAFKA',
          $5::uuid, $6::uuid, 'RUNNING', 'RUNNING', 'HEALTHY', 'NOT_APPLICABLE',
          $7::uuid, $7::uuid),
         ($3::uuid, $4, 'Second metrics port task', 'KAFKA_KAFKA',
          $5::uuid, $6::uuid, 'RUNNING', 'RUNNING', 'HEALTHY', 'NOT_APPLICABLE',
          $7::uuid, $7::uuid)`,
      [
        firstTaskId,
        `metrics-port-task-${firstTaskId}`,
        secondTaskId,
        `metrics-port-task-${secondTaskId}`,
        sourceId,
        targetId,
        administratorId,
      ],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.task_revisions (
         id, task_id, revision_number, state, configuration,
         created_by_user_id, updated_by_user_id
       ) VALUES
         ($1::uuid, $3::uuid, 1, 'APPLIED', '{}'::jsonb, $5::uuid, $5::uuid),
         ($2::uuid, $4::uuid, 1, 'APPLIED', '{}'::jsonb, $5::uuid, $5::uuid)`,
      [firstRevisionId, secondRevisionId, firstTaskId, secondTaskId, administratorId],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.task_runs (
         id, task_id, task_revision_id, status, current_stage
       ) VALUES
         ($1::uuid, $3::uuid, $5::uuid, 'RUNNING', 'RUNNING'),
         ($2::uuid, $4::uuid, $6::uuid, 'RUNNING', 'RUNNING')`,
      [firstRunId, secondRunId, firstTaskId, secondTaskId, firstRevisionId, secondRevisionId],
    );

    await expect(
      pool.query<{ metrics_port_released_at: Date | null }>(
        `INSERT INTO ape_dts_platform.task_run_units (
           id, task_id, run_id, unit_key, status, metrics_port
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'occupied-unreleased', 'RUNNING', $4)
         RETURNING metrics_port_released_at`,
        [randomUUID(), firstTaskId, firstRunId, occupiedPort],
      ),
    ).resolves.toMatchObject({ rows: [{ metrics_port_released_at: null }] });
    for (const [index, status] of runtimeStatuses.entries()) {
      await expect(
        pool.query(
          `INSERT INTO ape_dts_platform.task_run_units (
             id, task_id, run_id, unit_key, status, metrics_port
           ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6)`,
          [
            randomUUID(),
            secondTaskId,
            secondRunId,
            `conflicting-${index}-${status.toLowerCase()}`,
            status,
            occupiedPort,
          ],
        ),
      ).rejects.toMatchObject({
        code: '23505',
        constraint: 'task_run_units_one_active_metrics_port',
      });
    }

    const historicalUnitId = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.task_run_units (
         id, task_id, run_id, unit_key, status, metrics_port
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'historical-unreleased', 'STOPPED', $4)`,
      [historicalUnitId, firstTaskId, firstRunId, reusablePort],
    );
    await expect(
      pool.query(
        `INSERT INTO ape_dts_platform.task_run_units (
           id, task_id, run_id, unit_key, status, metrics_port
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'premature-reuse', 'STARTING', $4)`,
        [randomUUID(), secondTaskId, secondRunId, reusablePort],
      ),
    ).rejects.toMatchObject({
      code: '23505',
      constraint: 'task_run_units_one_active_metrics_port',
    });
    await expect(
      pool.query<{ metrics_port_released_at: Date }>(
        `UPDATE ape_dts_platform.task_run_units
            SET metrics_port_released_at = statement_timestamp()
          WHERE id = $1::uuid
          RETURNING metrics_port_released_at`,
        [historicalUnitId],
      ),
    ).resolves.toMatchObject({
      rowCount: 1,
      rows: [{ metrics_port_released_at: expect.any(Date) }],
    });
    await expect(
      pool.query<{ metrics_port_released_at: Date | null }>(
        `INSERT INTO ape_dts_platform.task_run_units (
           id, task_id, run_id, unit_key, status, metrics_port
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'released-port-reuse', 'STOPPING', $4)
         RETURNING metrics_port_released_at`,
        [randomUUID(), secondTaskId, secondRunId, reusablePort],
      ),
    ).resolves.toMatchObject({
      rowCount: 1,
      rows: [{ metrics_port_released_at: null }],
    });

    await expect(
      pool.query(
        `INSERT INTO ape_dts_platform.task_run_units (
           id, task_id, run_id, unit_key, status, metrics_port
         ) VALUES
           ($1::uuid, $4::uuid, $6::uuid, 'distinct-port', 'BLOCKED', $8),
           ($2::uuid, $4::uuid, $6::uuid, 'first-null-port', 'STARTING', NULL),
           ($3::uuid, $5::uuid, $7::uuid, 'second-null-port', 'BLOCKED', NULL)`,
        [
          randomUUID(),
          randomUUID(),
          randomUUID(),
          firstTaskId,
          secondTaskId,
          firstRunId,
          secondRunId,
          occupiedPort + 2,
        ],
      ),
    ).resolves.toMatchObject({ rowCount: 3 });

    await expect(inspectCurrentDatabase()).resolves.toMatchObject({ state: 'CURRENT' });
    await pool.query('DROP INDEX ape_dts_platform.task_run_units_one_active_metrics_port');
    try {
      await expect(inspectCurrentDatabase()).resolves.toMatchObject({
        issues: ['MISSING_INDEX:task_run_units.task_run_units_one_active_metrics_port'],
        state: 'CORRUPT',
      });
    } finally {
      await pool.query(
        `CREATE UNIQUE INDEX task_run_units_one_active_metrics_port
           ON ape_dts_platform.task_run_units (metrics_port)
           WHERE metrics_port IS NOT NULL AND metrics_port_released_at IS NULL`,
      );
    }
    await expect(inspectCurrentDatabase()).resolves.toMatchObject({ state: 'CURRENT' });
  });

  it('upgrades populated version 7 data, binds one historical profile, and preserves USER defaults', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const version7 = throughVersion(bundle, 7);
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await expect(
      runner.migrate(version7, randomUUID(), { expectedState: 'ABSENT' }),
    ).resolves.toMatchObject({ appliedVersions: [1, 2, 3, 4, 5, 6, 7] });
    const fixture = await insertVersion7RunFixture(database);

    await expect(
      runner.migrate(bundle, randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [8, 9, 10, 11, 12, 13, 14, 15, 16] });

    const runs = await database.query<{
      id: string;
      log_recovery_state: string;
      log_storage_state: string;
      log_storage_profile_id: string | null;
    }>(
      `SELECT id, log_storage_profile_id, log_storage_state, log_recovery_state
         FROM ape_dts_platform.task_runs
        WHERE id = ANY($1::uuid[])
        ORDER BY id`,
      [[fixture.runId, fixture.runWithoutObjectsId]],
    );
    expect(runs.rows).toEqual(
      [
        {
          id: fixture.runId,
          log_recovery_state: 'NONE',
          log_storage_profile_id: fixture.profileId,
          log_storage_state: 'UNKNOWN',
        },
        {
          id: fixture.runWithoutObjectsId,
          log_recovery_state: 'NONE',
          log_storage_profile_id: null,
          log_storage_state: 'UNKNOWN',
        },
      ].sort((left, right) => left.id.localeCompare(right.id)),
    );
    await expect(
      database.query(
        `SELECT created_by_actor_type, created_by_user_id
           FROM ape_dts_platform.stored_objects
          WHERE object_key = $1`,
        [`runs/${fixture.runId}/historical.log`],
      ),
    ).resolves.toMatchObject({
      rows: [{ created_by_actor_type: 'USER', created_by_user_id: fixture.userId }],
    });

    await expect(
      database.query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'ARTIFACT', $3, 0,
                   decode(repeat('02', 32), 'hex'), 'application/octet-stream',
                   'PENDING', $4::uuid)
         RETURNING created_by_actor_type`,
        [randomUUID(), fixture.profileId, `artifacts/${randomUUID()}`, fixture.userId],
      ),
    ).resolves.toMatchObject({ rows: [{ created_by_actor_type: 'USER' }] });
    await expect(
      database.query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 0,
                   decode(repeat('03', 32), 'hex'), 'text/plain', 'PENDING',
                   'SYSTEM', NULL)`,
        [randomUUID(), fixture.profileId, `runs/${fixture.runId}/${randomUUID()}.log`],
      ),
    ).resolves.toMatchObject({ rowCount: 1 });
    await expect(
      database.query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 0,
                   decode(repeat('04', 32), 'hex'), 'text/plain', 'PENDING',
                   'SYSTEM', $4::uuid)`,
        [
          randomUUID(),
          fixture.profileId,
          `runs/${fixture.runId}/${randomUUID()}.log`,
          fixture.userId,
        ],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      database.query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'ARTIFACT', $3, 0,
                   decode(repeat('05', 32), 'hex'), 'application/octet-stream',
                   'PENDING', 'USER', NULL)`,
        [randomUUID(), fixture.profileId, `artifacts/${randomUUID()}`],
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('marks version 8 durable log intents pending without claiming historical storage health', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const fixture = await insertVersion7RunFixture(database);
    await expect(
      runner.migrate(throughVersion(bundle, 8), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [8] });
    await database.query(
      `UPDATE ape_dts_platform.stored_objects AS objects
          SET state = 'PENDING', available_at = NULL
         FROM ape_dts_platform.log_segments AS segments
        WHERE segments.run_id = $1::uuid
          AND segments.stored_object_id = objects.id`,
      [fixture.runId],
    );
    await database.query(
      `UPDATE ape_dts_platform.log_segments
          SET state = 'SEALED'
        WHERE run_id = $1::uuid`,
      [fixture.runId],
    );

    await expect(
      runner.migrate(throughVersion(bundle, 9), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [9] });
    await expect(
      database.query(
        `SELECT log_storage_state, log_recovery_state
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid`,
        [fixture.runId],
      ),
    ).resolves.toMatchObject({
      rows: [{ log_recovery_state: 'DURABLE_PENDING', log_storage_state: 'UNKNOWN' }],
    });
  });

  it('renumbers version 9 log segments across units and enforces one sequence per run stream', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const fixture = await insertVersion7RunFixture(database);
    await expect(
      runner.migrate(throughVersion(bundle, 9), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [8, 9] });

    const firstUnitId = randomUUID();
    const secondUnitId = randomUUID();
    await database.query(
      `INSERT INTO ape_dts_platform.task_run_units (
         id, task_id, unit_key, status, run_id
       ) VALUES
         ($1::uuid, $3::uuid, 'partition:0', 'STOPPED', $4::uuid),
         ($2::uuid, $3::uuid, 'partition:1', 'STOPPED', $4::uuid)`,
      [firstUnitId, secondUnitId, fixture.taskId, fixture.runId],
    );

    const firstSegmentId = '00000000-0000-0000-0000-000000000001';
    const secondSegmentId = '00000000-0000-0000-0000-000000000002';
    const thirdSegmentId = '00000000-0000-0000-0000-000000000003';
    const earlierCreatedAt = new Date('2025-01-01T00:00:00.000Z');
    const laterCreatedAt = new Date('2025-01-01T00:01:00.000Z');
    await database.query(
      `INSERT INTO ape_dts_platform.log_segments (
         id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
         byte_size, state, created_at
       ) VALUES
         ($1::uuid, $4::uuid, $5::uuid, 'STDERR', 2, $7::timestamptz, $7::timestamptz,
          0, 'SEALED', $7::timestamptz),
         ($2::uuid, $4::uuid, $6::uuid, 'STDERR', 1, $7::timestamptz, $7::timestamptz,
          0, 'SEALED', $7::timestamptz),
         ($3::uuid, $4::uuid, $5::uuid, 'STDERR', 1, $8::timestamptz, $8::timestamptz,
          0, 'SEALED', $8::timestamptz)`,
      [
        firstSegmentId,
        secondSegmentId,
        thirdSegmentId,
        fixture.runId,
        firstUnitId,
        secondUnitId,
        earlierCreatedAt,
        laterCreatedAt,
      ],
    );

    await expect(
      runner.migrate(throughVersion(bundle, 10), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [10] });

    await expect(
      database.query<{
        created_at: Date;
        id: string;
        run_unit_id: string;
        sequence: number;
      }>(
        `SELECT id, run_unit_id, sequence::integer, created_at
           FROM ape_dts_platform.log_segments
          WHERE id = ANY($1::uuid[])
          ORDER BY sequence`,
        [[firstSegmentId, secondSegmentId, thirdSegmentId]],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          created_at: earlierCreatedAt,
          id: firstSegmentId,
          run_unit_id: firstUnitId,
          sequence: 1,
        },
        {
          created_at: earlierCreatedAt,
          id: secondSegmentId,
          run_unit_id: secondUnitId,
          sequence: 2,
        },
        {
          created_at: laterCreatedAt,
          id: thirdSegmentId,
          run_unit_id: firstUnitId,
          sequence: 3,
        },
      ],
    });
    await expect(
      database.query<{ sequence: number; stream: string }>(
        `SELECT stream, sequence::integer
           FROM ape_dts_platform.log_segments
          WHERE run_id = $1::uuid
          ORDER BY stream, sequence`,
        [fixture.runId],
      ),
    ).resolves.toMatchObject({
      rows: [
        { sequence: 1, stream: 'STDERR' },
        { sequence: 2, stream: 'STDERR' },
        { sequence: 3, stream: 'STDERR' },
        { sequence: 1, stream: 'STDOUT' },
      ],
    });
    await expect(
      database.query<{ conname: string }>(
        `SELECT conname
           FROM pg_constraint
          WHERE conrelid = 'ape_dts_platform.log_segments'::regclass
            AND conname = ANY($1::text[])
          ORDER BY conname`,
        [['log_segments_run_stream_sequence_key', 'log_segments_run_unit_stream_sequence_key']],
      ),
    ).resolves.toMatchObject({
      rows: [
        { conname: 'log_segments_run_stream_sequence_key' },
        { conname: 'log_segments_run_unit_stream_sequence_key' },
      ],
    });
    await expect(
      database.query<{ indexname: string }>(
        `SELECT indexname
           FROM pg_indexes
          WHERE schemaname = 'ape_dts_platform'
            AND tablename = 'log_segments'
            AND indexname = 'log_segments_run_sequence_idx'`,
      ),
    ).resolves.toMatchObject({ rows: [] });
    await expect(
      database.query(
        `INSERT INTO ape_dts_platform.log_segments (
           id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
           byte_size, state
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STDERR', 2,
                   statement_timestamp(), statement_timestamp(), 0, 'SEALED')`,
        [randomUUID(), fixture.runId, firstUnitId],
      ),
    ).rejects.toMatchObject({
      code: '23505',
      constraint: 'log_segments_run_stream_sequence_key',
    });
  });

  it('adds STREAM_ERROR gaps in version 11 without inventing loss metadata', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const fixture = await insertVersion7RunFixture(database);
    await expect(
      runner.migrate(throughVersion(bundle, 10), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [8, 9, 10] });

    const unitId = randomUUID();
    const gapId = randomUUID();
    const occurredAt = new Date('2026-09-07T08:30:00.000Z');
    await database.query(
      `INSERT INTO ape_dts_platform.task_run_units (
         id, task_id, unit_key, status, run_id
       ) VALUES ($1::uuid, $2::uuid, 'stream-error-unit', 'STOPPED', $3::uuid)`,
      [unitId, fixture.taskId, fixture.runId],
    );
    const insertGap = (input: {
      readonly containerRestart: boolean;
      readonly endedAt: Date | null;
      readonly id: string;
      readonly lostBytes: number | null;
      readonly runUnitId: string | null;
      readonly startedAt: Date | null;
    }) =>
      database.query(
        `INSERT INTO ape_dts_platform.log_gaps (
           id, run_id, run_unit_id, reason, started_at, ended_at,
           lost_bytes, occurred_at, container_restart
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STREAM_ERROR', $4::timestamptz,
                   $5::timestamptz, $6::bigint, $7::timestamptz, $8::boolean)`,
        [
          input.id,
          fixture.runId,
          input.runUnitId,
          input.startedAt,
          input.endedAt,
          input.lostBytes,
          occurredAt,
          input.containerRestart,
        ],
      );
    const validGap = {
      containerRestart: false,
      endedAt: null,
      id: gapId,
      lostBytes: null,
      runUnitId: unitId,
      startedAt: null,
    } as const;

    await expect(insertGap(validGap)).rejects.toMatchObject({
      code: '23514',
      constraint: 'log_gaps_reason_check',
    });
    await expect(
      runner.migrate(throughVersion(bundle, 11), randomUUID(), { expectedState: 'OLDER' }),
    ).resolves.toMatchObject({ appliedVersions: [11] });
    for (const invalidGap of [
      { ...validGap, id: randomUUID(), lostBytes: 1 },
      { ...validGap, id: randomUUID(), startedAt: occurredAt },
      { ...validGap, endedAt: occurredAt, id: randomUUID() },
      { ...validGap, containerRestart: true, id: randomUUID() },
      { ...validGap, id: randomUUID(), runUnitId: null },
    ]) {
      await expect(insertGap(invalidGap)).rejects.toMatchObject({
        code: '23514',
        constraint: 'log_gaps_reason_check',
      });
    }
    await expect(insertGap(validGap)).resolves.toMatchObject({ rowCount: 1 });

    await expect(
      database.query<{
        container_restart: boolean;
        ended_at: Date | null;
        lost_bytes: string | null;
        occurred_at: Date;
        reason: string;
        run_id: string;
        run_unit_id: string;
        started_at: Date | null;
      }>(
        `SELECT run_id, run_unit_id, reason, started_at, ended_at,
                lost_bytes::text, occurred_at, container_restart
           FROM ape_dts_platform.log_gaps
          WHERE id = $1::uuid`,
        [gapId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          container_restart: false,
          ended_at: null,
          lost_bytes: null,
          occurred_at: occurredAt,
          reason: 'STREAM_ERROR',
          run_id: fixture.runId,
          run_unit_id: unitId,
          started_at: null,
        },
      ],
    });
  });

  it('refuses version 16 when a continuity baseline points at another task revision', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const task = await insertVersion7RunFixture(database);
    const unrelated = await insertVersion7RunFixture(database);
    await runner.migrate(throughVersion(bundle, 15), randomUUID(), { expectedState: 'OLDER' });
    await database.query(
      `INSERT INTO ape_dts_platform.task_continuity_baselines
         (id, task_id, task_revision_id, baseline_kind, source_cluster_id, source_topic_id,
          source_partition_ids, source_partition_offsets, target_cluster_id, target_topic_id,
          target_partition_count)
       VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA', 'source-cluster', 'source-topic',
               ARRAY[0]::integer[], '{"0":0}'::jsonb, 'target-cluster', 'target-topic', 1)`,
      [randomUUID(), task.taskId, unrelated.revisionId],
    );

    await expect(
      runner.migrate(bundle, randomUUID(), { expectedState: 'OLDER' }),
    ).rejects.toMatchObject({
      code: '23503',
      constraint: 'task_continuity_baseline_revision_task_fk',
    });
    await expect(
      database.query<{ version: number }>(
        'SELECT max(version)::integer AS version FROM ape_dts_platform.schema_migrations',
      ),
    ).resolves.toMatchObject({ rows: [{ version: 15 }] });
  });

  it('refuses version 16 when an existing STREAM_ERROR gap contains loss metadata', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const fixture = await insertVersion7RunFixture(database);
    await runner.migrate(throughVersion(bundle, 15), randomUUID(), { expectedState: 'OLDER' });
    const unitId = randomUUID();
    await database.query(
      `INSERT INTO ape_dts_platform.task_run_units (id, task_id, unit_key, status, run_id)
       VALUES ($1::uuid, $2::uuid, 'invalid-stream-gap', 'STOPPED', $3::uuid)`,
      [unitId, fixture.taskId, fixture.runId],
    );
    await database.query(
      `INSERT INTO ape_dts_platform.log_gaps
         (id, run_id, run_unit_id, reason, lost_bytes, occurred_at)
       VALUES ($1::uuid, $2::uuid, $3::uuid, 'STREAM_ERROR', 1, statement_timestamp())`,
      [randomUUID(), fixture.runId, unitId],
    );

    await expect(
      runner.migrate(bundle, randomUUID(), { expectedState: 'OLDER' }),
    ).rejects.toMatchObject({ code: '23514', constraint: 'log_gaps_reason_check' });
    await expect(
      database.query<{ version: number }>(
        'SELECT max(version)::integer AS version FROM ape_dts_platform.schema_migrations',
      ),
    ).resolves.toMatchObject({ rows: [{ version: 15 }] });
  });

  it('enforces repaired continuity and STREAM_ERROR invariants after version 16', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(throughVersion(bundle, 7), randomUUID(), { expectedState: 'ABSENT' });
    const task = await insertVersion7RunFixture(database);
    const unrelated = await insertVersion7RunFixture(database);
    await runner.migrate(bundle, randomUUID(), { expectedState: 'OLDER' });
    const insertBaseline = (revisionId: string) =>
      database.query(
        `INSERT INTO ape_dts_platform.task_continuity_baselines
           (id, task_id, task_revision_id, baseline_kind, source_cluster_id, source_topic_id,
            source_partition_ids, source_partition_offsets, target_cluster_id, target_topic_id,
            target_partition_count)
         VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA', 'source-cluster', 'source-topic',
                 ARRAY[0]::integer[], '{"0":0}'::jsonb, 'target-cluster', 'target-topic', 1)`,
        [randomUUID(), task.taskId, revisionId],
      );
    await expect(insertBaseline(unrelated.revisionId)).rejects.toMatchObject({
      code: '23503',
      constraint: 'task_continuity_baseline_revision_task_fk',
    });
    await expect(insertBaseline(task.revisionId)).resolves.toMatchObject({ rowCount: 1 });

    const unitId = randomUUID();
    await database.query(
      `INSERT INTO ape_dts_platform.task_run_units (id, task_id, unit_key, status, run_id)
       VALUES ($1::uuid, $2::uuid, 'valid-stream-gap', 'STOPPED', $3::uuid)`,
      [unitId, task.taskId, task.runId],
    );
    const insertStreamGap = (lostBytes: number | null) =>
      database.query(
        `INSERT INTO ape_dts_platform.log_gaps
           (id, run_id, run_unit_id, reason, lost_bytes, occurred_at)
         VALUES ($1::uuid, $2::uuid, $3::uuid, 'STREAM_ERROR', $4::bigint, statement_timestamp())`,
        [randomUUID(), task.runId, unitId, lostBytes],
      );
    await expect(insertStreamGap(1)).rejects.toMatchObject({
      code: '23514',
      constraint: 'log_gaps_reason_check',
    });
    await expect(insertStreamGap(null)).resolves.toMatchObject({ rowCount: 1 });
  });

  it('rejects a version 7 upgrade when one run already spans storage profiles', async () => {
    const database = await createAuxiliaryDatabase();
    const bundle = await loadMigrationBundle();
    const version7 = throughVersion(bundle, 7);
    const runner = new MigrationRunner(new PgMigrationStore(database));
    await runner.migrate(version7, randomUUID(), { expectedState: 'ABSENT' });
    const fixture = await insertVersion7RunFixture(database);
    const secondProfileId = randomUUID();
    const secondObjectId = randomUUID();
    await database.query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state, created_by_user_id
       ) VALUES ($1::uuid, $2, 'Second upgrade profile', 1, 'LOCAL',
                 '{"relative_prefix":"upgrade-second"}'::jsonb, 'DRAFT', $3::uuid)`,
      [secondProfileId, `upgrade-second-${secondProfileId}`, fixture.userId],
    );
    await database.query(
      `INSERT INTO ape_dts_platform.stored_objects (
         id, storage_profile_id, kind, object_key, size_bytes, sha256,
         content_type, state, created_by_user_id, available_at
       ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 3,
                 decode(repeat('06', 32), 'hex'), 'text/plain', 'AVAILABLE',
                 $4::uuid, statement_timestamp())`,
      [secondObjectId, secondProfileId, `runs/${fixture.runId}/second.log`, fixture.userId],
    );
    await database.query(
      `INSERT INTO ape_dts_platform.log_segments (
         id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
         stored_object_id, byte_size, sha256, state
       ) VALUES ($1::uuid, $2::uuid, NULL, 'STDERR', 1,
                 statement_timestamp(), statement_timestamp(), $3::uuid, 3,
                 decode(repeat('06', 32), 'hex'), 'AVAILABLE')`,
      [randomUUID(), fixture.runId, secondObjectId],
    );

    await expect(
      runner.migrate(bundle, randomUUID(), { expectedState: 'OLDER' }),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      database.query(
        `SELECT max(version)::integer AS version FROM ape_dts_platform.schema_migrations`,
      ),
    ).resolves.toMatchObject({ rows: [{ version: 7 }] });
    await expect(
      database.query(
        `SELECT column_name
           FROM information_schema.columns
          WHERE table_schema = 'ape_dts_platform'
            AND table_name = 'task_runs'
            AND column_name = 'log_storage_profile_id'`,
      ),
    ).resolves.toMatchObject({ rows: [] });
  });

  it('holds exactly one session-level controller lease', async () => {
    if (pool === undefined) {
      throw new Error('test pool is not initialized');
    }
    const first = await tryAcquireControllerLock(pool);
    expect(first).not.toBeNull();
    await expect(tryAcquireControllerLock(pool)).resolves.toBeNull();
    await first?.release();

    const replacement = await tryAcquireControllerLock(pool);
    expect(replacement).not.toBeNull();
    await replacement?.release();
  });

  it('keeps an idle controller identifiable and alive, rejects a second instance, and releases on shutdown', async () => {
    const f = await createControllerDatabase();
    const other = new PlatformDatabaseService();
    controllers.add(other);
    await other.inspectAndUse(f.connection, f.bundle.manifest);
    try {
      expect(await f.database.acquireControllerLock()).toBe(true);
      expect(await other.acquireControllerLock()).toBe(false);
      const [holder] = await inspectControllerLockHolders(f.observer);
      expect(holder?.application_name).toMatch(/^scs-data-sync-platform\/[^/]+\/\d+$/u);
      const settings = await f.database.requirePool().query(
        `SELECT current_setting('tcp_keepalives_idle') AS idle,
                current_setting('tcp_keepalives_interval') AS interval,
                current_setting('tcp_keepalives_count') AS count,
                current_setting('tcp_user_timeout') AS user_timeout`,
      );
      expect(settings.rows).toEqual([
        { idle: '10', interval: '5', count: '3', user_timeout: '15000' },
      ]);
      await vi.waitFor(
        async () => {
          const activity = await f.observer.query(
            'SELECT query FROM pg_catalog.pg_stat_activity WHERE pid=$1',
            [holder!.pid],
          );
          expect(activity.rows[0]?.query).toBe('SELECT 1');
        },
        { timeout: 15_000, interval: 100 },
      );
      expect(await other.acquireControllerLock()).toBe(false);
      await f.database.onApplicationShutdown();
      controllers.delete(f.database);
      expect(await other.acquireControllerLock()).toBe(true);
    } finally {
      await f.database.releaseControllerLock();
      await other.releaseControllerLock();
    }
  });

  it('detects a killed controller connection despite a healthy pool and reacquires only during recovery', async () => {
    const f = await createControllerDatabase();
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    const availability = new MetadataAvailabilityService(f.database, readiness);
    const reconcile = vi.fn(async () => undefined);
    availability.onModuleInit();
    availability.registerReconciler(reconcile);
    try {
      expect(await f.database.acquireControllerLock()).toBe(true);
      const [holder] = await inspectControllerLockHolders(f.observer);
      expect(holder).toBeDefined();
      await f.observer.query('SELECT pg_terminate_backend($1)', [holder!.pid]);
      await vi.waitFor(() => expect(readiness.getPhase()).toBe('METADATA_DEGRADED'));
      await expect(f.database.requirePool().query('SELECT 1')).resolves.toMatchObject({
        rowCount: 1,
      });
      await expect(availability.ensureManagementAvailable()).resolves.toBeUndefined();
      expect(readiness.getPhase()).toBe('READY');
      expect(reconcile).toHaveBeenCalledOnce();
      const holders = await inspectControllerLockHolders(f.observer);
      expect(holders).toHaveLength(1);
      expect(holders[0]!.pid).not.toBe(holder!.pid);
    } finally {
      availability.onApplicationShutdown();
      await f.database.releaseControllerLock();
    }
  });

  it('enforces immutable storage versions and evidence-gated artifact activation', async () => {
    if (pool === undefined) {
      throw new Error('test pool is not initialized');
    }
    const administratorId = randomUUID();
    const storageProfileId = randomUUID();
    const storedObjectId = randomUUID();
    const artifactId = randomUUID();
    const probeRunId = randomUUID();
    const sha256 = Buffer.alloc(32, 7);
    await pool.query(
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
        administratorId,
        `unit3-admin-${process.pid}`,
        'Unit 3 administrator',
        JSON.stringify({ memoryKiB: 65_536, parallelism: 1, passes: 3, tagLength: 32 }),
        Buffer.alloc(16, 1),
        Buffer.alloc(32, 2),
      ],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.storage_profiles (
        id,
        profile_key,
        display_name,
        version,
        driver,
        config,
        state,
        created_by_user_id,
        activated_at
      ) VALUES ($1, 'local-primary', 'Local primary', 1, 'LOCAL', $2::jsonb, 'ACTIVE_WRITE', $3, statement_timestamp())`,
      [storageProfileId, JSON.stringify({ relative_prefix: '' }), administratorId],
    );

    await expect(
      pool.query(
        `INSERT INTO ape_dts_platform.storage_profiles (
          id,
          profile_key,
          display_name,
          version,
          driver,
          config,
          state,
          created_by_user_id,
          activated_at
        ) VALUES ($1, 'other-local', 'Other local', 1, 'LOCAL', $2::jsonb, 'ACTIVE_WRITE', $3, statement_timestamp())`,
        [randomUUID(), JSON.stringify({ relative_prefix: 'other' }), administratorId],
      ),
    ).rejects.toMatchObject({ code: '23505' });
    await expect(
      pool.query('UPDATE ape_dts_platform.storage_profiles SET config = $2::jsonb WHERE id = $1', [
        storageProfileId,
        JSON.stringify({ relative_prefix: 'changed' }),
      ]),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.storage_profiles
         SET state = 'DRAFT', activated_at = NULL
         WHERE id = $1`,
        [storageProfileId],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    for (const invalidPrefix of ['nested//path', String.raw`nested\path`, 'nested/../path']) {
      await expect(
        pool.query(
          `INSERT INTO ape_dts_platform.storage_profiles (
            id,
            profile_key,
            display_name,
            version,
            driver,
            config,
            state,
            created_by_user_id
          ) VALUES ($1, $2, 'Invalid local prefix', 1, 'LOCAL', $3::jsonb, 'DRAFT', $4)`,
          [
            randomUUID(),
            `invalid-local-${randomUUID()}`,
            JSON.stringify({ relative_prefix: invalidPrefix }),
            administratorId,
          ],
        ),
      ).rejects.toMatchObject({ code: '23514' });
    }

    await pool.query(
      `INSERT INTO ape_dts_platform.stored_objects (
        id,
        storage_profile_id,
        kind,
        object_key,
        size_bytes,
        sha256,
        content_type,
        state,
        created_by_actor_type,
        created_by_user_id,
        available_at
      ) VALUES ($1, $2, 'ARTIFACT', 'artifacts/dt-main', 1024, $3, 'application/octet-stream', 'AVAILABLE', 'USER', $4, statement_timestamp())`,
      [storedObjectId, storageProfileId, sha256, administratorId],
    );
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.storage_profiles
         SET state = 'RETIRED', retired_at = statement_timestamp()
         WHERE id = $1`,
        [storageProfileId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await pool.query(
      `INSERT INTO ape_dts_platform.artifacts (
        id,
        version_identifier,
        declared_source,
        stored_object_id,
        declared_sha256,
        size_bytes,
        created_by_user_id
      ) VALUES ($1, 'v1.0.0', 'customer supplied object', $2, $3, 1024, $4)`,
      [artifactId, storedObjectId, sha256, administratorId],
    );
    await expect(
      pool.query(
        `INSERT INTO ape_dts_platform.artifacts (
          id,
          version_identifier,
          declared_source,
          stored_object_id,
          declared_sha256,
          size_bytes,
          created_by_user_id
        ) VALUES ($1, 'v1.0.0', 'different object', $2, $3, 1024, $4)`,
        [randomUUID(), storedObjectId, Buffer.alloc(32, 8), administratorId],
      ),
    ).rejects.toMatchObject({ code: '23505' });
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.artifacts
         SET state = 'ACTIVE', actual_sha256 = declared_sha256, activated_at = statement_timestamp()
         WHERE id = $1`,
        [artifactId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await pool.query(
      `INSERT INTO ape_dts_platform.artifact_probe_runs (
        id,
        artifact_id,
        probe_kind,
        probe_version,
        status,
        started_by_user_id,
        ended_at
      ) VALUES ($1, $2, 'BEHAVIOR', 'probe-v1', 'PASSED', $3, statement_timestamp())`,
      [probeRunId, artifactId, administratorId],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.artifact_certifications (
        id,
        artifact_id,
        platform_version,
        task_scenario,
        adapter_id,
        adapter_version,
        state,
        evidence_probe_run_id
      ) VALUES ($1, $2, '0.1.0', 'PG_CDC_TO_KAFKA', 'ape-dts-ini', '1', 'PASSED', $3)`,
      [randomUUID(), artifactId, probeRunId],
    );
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.artifacts
         SET state = 'ACTIVE', actual_sha256 = declared_sha256, activated_at = statement_timestamp()
         WHERE id = $1
         RETURNING state`,
        [artifactId],
      ),
    ).resolves.toMatchObject({ rows: [{ state: 'ACTIVE' }] });
    await pool.query(
      `UPDATE ape_dts_platform.artifacts
       SET state = 'QUARANTINED', state_reason = 'security recall'
       WHERE id = $1`,
      [artifactId],
    );
    await expect(
      pool.query(
        `SELECT state, revoked_at
         FROM ape_dts_platform.artifact_certifications
         WHERE artifact_id = $1`,
        [artifactId],
      ),
    ).resolves.toMatchObject({
      rows: [{ revoked_at: expect.any(Date), state: 'REVOKED' }],
    });
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.artifacts
         SET state = 'ACTIVE'
         WHERE id = $1`,
        [artifactId],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    await pool.query(
      `UPDATE ape_dts_platform.artifacts
       SET state = 'INACTIVE', activated_at = NULL
       WHERE id = $1`,
      [artifactId],
    );
    await expect(
      pool.query(
        `UPDATE ape_dts_platform.artifacts
         SET state = 'ACTIVE', activated_at = statement_timestamp()
         WHERE id = $1`,
        [artifactId],
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });
});
