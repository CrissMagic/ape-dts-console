import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Logger } from '../../../apps/api/node_modules/@nestjs/common/index.js';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';

import { BootstrapFlowModule } from '../../../apps/api/src/bootstrap/bootstrap-flow.module.js';
import { BootstrapPersistenceService } from '../../../apps/api/src/bootstrap/bootstrap-persistence.service.js';
import { BootstrapStateService } from '../../../apps/api/src/bootstrap/bootstrap-state.service.js';
import { BootstrapRecoveryCompletionService } from '../../../apps/api/src/bootstrap/bootstrap-recovery-completion.service.js';
import { RecoveryCoordinatorService } from '../../../apps/api/src/recovery/recovery-coordinator.service.js';
import { RuntimeSupervisorService } from '../../../apps/api/src/runtime/runtime-supervisor.service.js';
import { PlatformReadinessService } from '../../../apps/api/src/health/platform-readiness.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import { TasksService } from '../../../apps/api/src/tasks/tasks.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { readLocalPlatformConfig } from '../../../apps/api/src/bootstrap/local-platform-config.js';
import { runSecurityCli } from '../../../apps/api/src/bootstrap/security-cli.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import { resetAdmin } from '../../../apps/api/src/auth/reset-admin-cli.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import {
  createInitialKeyMaterial,
  decryptSecret,
  encryptSecret,
  hashPassword,
  type Argon2idParameters,
} from '../../../packages/security/src/index.js';

const name = `scs_startup_${process.pid}_${Date.now()}`;
const password = randomBytes(24).toString('base64url');
const policy: Argon2idParameters = {
  memoryKiB: 1024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};
const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
let port: number;
let root: string;
const pools: Pool[] = [];
const databases: string[] = [];
function docker(args: string[], input?: string) {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}
function sql(query: string) {
  docker(
    ['exec', '-i', container, 'psql', '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1'],
    query,
  );
}
async function database(suffix: string) {
  const databaseName = `${name}_${suffix}`;
  sql(`CREATE DATABASE "${databaseName}" OWNER "${name}";`);
  databases.push(databaseName);
  const connection = {
    database: databaseName,
    host: '127.0.0.1',
    port,
    username: name,
    password,
    tls: { mode: 'DISABLE' as const },
  };
  const pool = new Pool({ database: databaseName, host: '127.0.0.1', port, user: name, password });
  pools.push(pool);
  vi.stubEnv('SCS_PLATFORM_CONFIG_PATH', join(root, suffix, 'config.json'));
  vi.stubEnv('SCS_PLATFORM_DATABASE_CONFIG', JSON.stringify(connection));
  return { pool, connection };
}
async function boot() {
  const module = await Test.createTestingModule({ imports: [BootstrapFlowModule] })
    .overrideProvider(ARGON2ID_POLICY)
    .useValue(policy)
    .compile();
  await module.init();
  return module;
}
beforeAll(async () => {
  vi.stubEnv('SECURITY_ARGON2_MEMORY_KIB', '1024');
  vi.stubEnv('SECURITY_ARGON2_PARALLELISM', '2');
  vi.stubEnv('SECURITY_ARGON2_PASSES', '2');
  vi.stubEnv('SECURITY_ARGON2_TAG_LENGTH', '32');
  const portText = docker(['port', container, '5432/tcp']);
  port = Number(portText.slice(portText.lastIndexOf(':') + 1));
  sql(`CREATE ROLE "${name}" LOGIN PASSWORD '${password}';`);
  root = await mkdtemp(join(tmpdir(), 'scs-startup-integration-'));
});
afterAll(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  await Promise.all(pools.map((pool) => pool.end()));
  for (const item of databases) sql(`DROP DATABASE "${item}";`);
  sql(`DROP ROLE "${name}";`);
  if (root) await rm(root, { recursive: true, force: true });
});

it('initializes admin once, reaches READY and reuses the same database/key/password after restart', async () => {
  const { pool } = await database('fresh');
  const log = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
  const app = await boot();
  try {
    expect(app.get(BootstrapStateService).getStatus()).toMatchObject({ phase: 'READY' });
    const storage = await pool.query('SELECT * FROM ape_dts_platform.storage_profiles');
    expect(storage.rows).toHaveLength(1);
    expect(storage.rows[0]).toMatchObject({
      profile_key: 'default-local',
      driver: 'LOCAL',
      state: 'ACTIVE_WRITE',
      config: { relative_prefix: '' },
      created_by_actor_type: 'SYSTEM',
      created_by_user_id: null,
      last_test_status: 'NOT_TESTED',
    });
    await Promise.all(
      Array.from({ length: 3 }, () =>
        new BootstrapPersistenceService().ensureDefaultLocalStorage(pool),
      ),
    );
    expect((await pool.query('SELECT * FROM ape_dts_platform.storage_profiles')).rows).toEqual(
      storage.rows,
    );
    expect(
      (
        await pool.query(
          `SELECT actor_type, actor_user_id FROM ape_dts_platform.audit_events WHERE action = 'DEFAULT_LOCAL_STORAGE_CREATED'`,
        )
      ).rows,
    ).toEqual([{ actor_type: 'SYSTEM', actor_user_id: null }]);
    const users = await pool.query(
      'SELECT normalized_username, status, must_change_password, password_hash FROM ape_dts_platform.users',
    );
    expect(users.rows).toHaveLength(1);
    expect(users.rows[0]).toMatchObject({
      normalized_username: 'admin',
      status: 'ACTIVE',
      must_change_password: true,
    });
    const passwordHash = users.rows[0].password_hash;
    const config = await readLocalPlatformConfig();
    expect(config).not.toBeNull();
    expect(
      log.mock.calls.filter((call) => String(call[0]).includes('bootstrap.administrator_created')),
    ).toHaveLength(1);
    await app.close();
    await pool.query(
      `UPDATE ape_dts_platform.users SET normalized_username = 'existing-admin' WHERE normalized_username = 'admin'`,
    );
    await expect(runSecurityCli(['upgrade', '--backup-confirmed'])).rejects.toThrow(
      'ADMIN_IDENTITY_MIGRATION_REQUIRED',
    );
    await runSecurityCli(['upgrade', '--backup-confirmed', '--administrator', 'existing-admin']);
    expect(
      (
        await pool.query(
          `SELECT password_hash FROM ape_dts_platform.users WHERE normalized_username = 'admin'`,
        )
      ).rows[0].password_hash,
    ).toEqual(passwordHash);
    expect(await readLocalPlatformConfig()).toEqual(config);
    const output = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    try {
      await resetAdmin();
      expect(output.mock.calls.some((call) => String(call[0]).includes('admin 临时密码'))).toBe(
        true,
      );
    } finally {
      output.mockRestore();
    }
    const resetHash = (await pool.query('SELECT password_hash FROM ape_dts_platform.users')).rows[0]
      .password_hash;
    expect(resetHash).not.toEqual(passwordHash);
    await writeFile(
      process.env.SCS_PLATFORM_CONFIG_PATH!,
      JSON.stringify({ ...config, wrapperFingerprint: '0'.repeat(64) }),
    );
    await expect(resetAdmin()).rejects.toThrow('LOCAL_KEY_DATABASE_MISMATCH');
    expect(
      (await pool.query('SELECT password_hash FROM ape_dts_platform.users')).rows[0].password_hash,
    ).toEqual(resetHash);
    await writeFile(process.env.SCS_PLATFORM_CONFIG_PATH!, JSON.stringify(config));
    const restarted = await boot();
    try {
      expect(restarted.get(BootstrapStateService).getStatus()).toMatchObject({ phase: 'READY' });
      expect((await pool.query('SELECT * FROM ape_dts_platform.storage_profiles')).rows).toEqual(
        storage.rows,
      );
      expect(await readLocalPlatformConfig()).toEqual(config);
      expect(
        (await pool.query('SELECT password_hash FROM ape_dts_platform.users')).rows[0]
          .password_hash,
      ).toEqual(resetHash);
      expect(
        log.mock.calls.filter((call) =>
          String(call[0]).includes('bootstrap.administrator_created'),
        ),
      ).toHaveLength(1);
    } finally {
      await restarted.close();
    }
  } finally {
    await app.close();
    log.mockRestore();
  }
});

it('boots the real control plane despite a broken old task and manages it without resetting the database', async () => {
  const { pool } = await database('broken_task');
  const log = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
  const first = await boot();
  const oldInstance = first.get(BootstrapStateService).getStatus().platformInstanceId;
  await first.close();
  const user = (
    await pool.query("SELECT id FROM ape_dts_platform.users WHERE normalized_username='admin'")
  ).rows[0].id;
  const source = randomUUID();
  const target = randomUUID();
  const task = randomUUID();
  const revision = randomUUID();
  const configuration = {
    schemaVersion: 1,
    kind: 'KAFKA_RAW_FORWARD',
    source: { dataSourceId: source, topic: 'source', groupId: 'test-recovery' },
    target: { dataSourceId: target, topic: 'target' },
    tuning: {
      ackIntervalSeconds: 5,
      batchSize: 100,
      bufferSize: 16000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all',
    },
  };
  await pool.query(
    `INSERT INTO ape_dts_platform.data_sources
    (id,normalized_name,source_type,configuration,state,created_by_user_id,updated_by_user_id)
    VALUES ($1::uuid,'source','KAFKA','{"type":"KAFKA","bootstrapServers":["not-used:9092"],"plaintextRiskAcknowledged":true}','ACTIVE',$3::uuid,$3::uuid),
           ($2::uuid,'target','KAFKA','{"type":"KAFKA","bootstrapServers":["not-used:9092"],"plaintextRiskAcknowledged":true}','ACTIVE',$3::uuid,$3::uuid)`,
    [source, target, user],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.tasks
    (id,normalized_name,display_name,task_type,source_data_source_id,target_data_source_id,
     desired_state,observed_state,observability_health,recovery_disposition,created_by_user_id,updated_by_user_id)
    VALUES ($1::uuid,'broken-task','Broken old task','KAFKA_KAFKA',$2::uuid,$3::uuid,'RUNNING','RUNNING','UNAVAILABLE','AUTO_ELIGIBLE',$4::uuid,$4::uuid)`,
    [task, source, target, user],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.task_revisions
    (id,task_id,revision_number,state,configuration_schema_version,configuration,created_by_user_id,updated_by_user_id)
    VALUES ($1::uuid,$2::uuid,1,'APPLIED',1,$3::jsonb,$4::uuid,$4::uuid)`,
    [revision, task, JSON.stringify(configuration), user],
  );
  await pool.query(
    'UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid WHERE id=$1::uuid',
    [task, revision],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.task_runs (id,task_id,task_revision_id,platform_instance_id,status,current_stage)
    VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'RUNNING','RUNNING')`,
    [randomUUID(), task, revision, oldInstance],
  );
  const restarted = await boot();
  try {
    const state = restarted.get(BootstrapStateService).getStatus();
    expect(state.phase).toBe('READY');
    expect(restarted.get(PlatformReadinessService).getPhase()).toBe('READY');
    const recovery = restarted.get(RecoveryCoordinatorService);
    await vi.waitFor(async () =>
      expect((await recovery.getCurrent(state.platformInstanceId)).state).toBe('READY'),
    );
    expect(restarted.get(UnlockedKeyService).require().length).toBe(32);
    const database = restarted.get(PlatformDatabaseService);
    const tasks = new TasksService(database, new AuditLedgerService(database));
    const actor = {
      id: user,
      username: 'admin',
      displayName: '管理员',
      role: 'ADMIN' as const,
      status: 'ACTIVE' as const,
    };
    const blocked = await tasks.get(actor, task);
    expect(blocked).toMatchObject({
      observedState: 'BLOCKED',
      recoveryDisposition: 'MANUAL_REQUIRED',
    });
    await restarted.get(RuntimeSupervisorService).discardRecovery(actor, task, blocked.rowVersion);
    expect(await tasks.get(actor, task)).toMatchObject({
      desiredState: 'STOPPED',
      observedState: 'STOPPED',
      draft: { state: 'READY_DRAFT', configuration },
    });
    expect(
      (await pool.query('SELECT count(*)::text AS count FROM ape_dts_platform.users')).rows[0]
        .count,
    ).toBe('1');
  } finally {
    await restarted.close();
    log.mockRestore();
  }
});

it.each(['FAILED', 'PAUSED'])(
  'opens the control plane for %s task recovery, not for unavailable keys',
  async (outcome) => {
    const { pool } = await database(`recovery_${outcome.toLowerCase()}`);
    vi.spyOn(RecoveryCoordinatorService.prototype, 'startForPlatform').mockImplementation(
      () => undefined,
    );
    const app = await boot();
    try {
      const state = app.get(BootstrapStateService);
      const status = state.getStatus();
      const session = (
        await pool.query(
          'SELECT current_recovery_session_id AS id FROM ape_dts_platform.platform_instances WHERE id=$1::uuid',
          [status.platformInstanceId],
        )
      ).rows[0].id;
      await pool.query('UPDATE ape_dts_platform.recovery_sessions SET state=$2 WHERE id=$1::uuid', [
        session,
        outcome,
      ]);
      const restartedState = new BootstrapStateService(status.platformInstanceId);
      restartedState.markCodeVerified();
      restartedState.recordSchemaInspection('CURRENT');
      restartedState.beginUnlock();
      restartedState.beginRecovery();
      const complete = new BootstrapRecoveryCompletionService(
        { invalidateSession() {} } as never,
        restartedState,
        app.get(PlatformDatabaseService),
        { markReady() {} } as never,
        { activateForReadyPlatform() {} } as never,
      );
      await expect(complete.complete(session)).resolves.toBe(true);
      restartedState.markStartupFailed('test-reset');
      restartedState.markCodeVerified();
      restartedState.recordSchemaInspection('CURRENT');
      restartedState.beginUnlock();
      restartedState.beginRecovery();
      await pool.query(
        "UPDATE ape_dts_platform.recovery_steps SET status='FAILED' WHERE recovery_session_id=$1::uuid AND step_type='UNLOCK_KEYS'",
        [session],
      );
      await expect(complete.complete(session)).resolves.toBe(false);
      await expect(complete.complete(randomUUID())).resolves.toBe(false);
    } finally {
      vi.restoreAllMocks();
      await app.close();
    }
  },
);

it('migrates a legacy database using its original passphrase and preserves encrypted data', async () => {
  const { pool } = await database('legacy');
  const bundle = await loadMigrationBundle();
  const legacy = {
    manifest: {
      ...bundle.manifest,
      migrations: bundle.manifest.migrations.filter((item) => item.version <= 17),
    },
    migrations: bundle.migrations.filter((item) => item.version <= 17),
  };
  const passphrase = randomBytes(32).toString('base64url');
  const material = await createInitialKeyMaterial(passphrase, policy);
  const adminId = randomUUID();
  const instanceId = randomUUID();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(legacy, instanceId, {
    expectedState: 'ABSENT',
    afterMigrations: async (transaction) => {
      await new BootstrapPersistenceService().insertInitialPlatformRows(transaction, {
        administrator: {
          id: adminId,
          displayName: '旧管理员',
          normalizedUsername: 'preview-admin',
          password: await hashPassword('legacy-password-value', policy),
        },
        instanceId,
        passphraseWrapper: material.passphraseWrapper,
        recoveryWrapper: material.recoveryWrapper,
        recoverySessionId: randomUUID(),
        startedAt: new Date().toISOString(),
      });
    },
  });
  const context = {
    aadVersion: 1 as const,
    ownerId: adminId,
    ownerType: 'USER',
    purpose: 'TOTP_SEED',
    secretId: randomUUID(),
  };
  const encrypted = encryptSecret(
    material.dek,
    Buffer.from('preserved-data-source-password'),
    context,
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.secrets (id, owner_type, owner_id, purpose, algorithm, dek_version, aad_version, nonce, ciphertext, tag)
    VALUES ($1, 'USER', $2, 'TOTP_SEED', 'AES_256_GCM', 1, 1, $3, $4, $5)`,
    [
      context.secretId,
      adminId,
      Buffer.from(encrypted.nonce, 'base64'),
      Buffer.from(encrypted.ciphertext, 'base64'),
      Buffer.from(encrypted.tag, 'base64'),
    ],
  );
  await pool.query(
    `UPDATE ape_dts_platform.users SET totp_secret_id = $1, status = 'ACTIVE' WHERE id = $2`,
    [context.secretId, adminId],
  );
  const oldAdmin = (
    await pool.query(
      'SELECT id, password_hash, session_generation, totp_secret_id FROM ape_dts_platform.users WHERE id = $1',
      [adminId],
    )
  ).rows[0];
  await pool.query(
    `INSERT INTO ape_dts_platform.auth_sessions (id, user_id, token_hash, csrf_token_hash, source_fingerprint, session_generation, idle_expires_at, absolute_expires_at, reauthenticated_at)
    VALUES ($1, $2, $3, $4, $5, $6, now() + interval '1 hour', now() + interval '2 hours', now())`,
    [
      randomUUID(),
      adminId,
      randomBytes(32),
      randomBytes(32),
      randomBytes(32),
      oldAdmin.session_generation,
    ],
  );
  const conflictId = randomUUID();
  await pool.query(
    `INSERT INTO ape_dts_platform.users (id, normalized_username, display_name, role, status, password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash)
    SELECT $1, 'admin', 'Conflict fixture', 'DEVELOPER', 'TOTP_REQUIRED', password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash FROM ape_dts_platform.users WHERE id = $2`,
    [conflictId, adminId],
  );
  const migrationArgs = [
    'migrate-security',
    '--backup-confirmed',
    '--administrator',
    'preview-admin',
  ];
  await expect(runSecurityCli(migrationArgs, async () => passphrase)).rejects.toThrow(
    'ADMIN_USERNAME_CONFLICT',
  );
  expect(await readLocalPlatformConfig()).toBeNull();
  expect(
    (
      await pool.query(
        `SELECT count(*)::int AS count FROM information_schema.columns WHERE table_schema = 'ape_dts_platform' AND table_name = 'users' AND column_name = 'must_change_password'`,
      )
    ).rows[0].count,
  ).toBe(0);
  expect(
    (
      await pool.query('SELECT normalized_username FROM ape_dts_platform.users WHERE id = $1', [
        adminId,
      ])
    ).rows[0].normalized_username,
  ).toBe('preview-admin');
  await pool.query(
    `UPDATE ape_dts_platform.users SET normalized_username = 'existing-developer' WHERE id = $1`,
    [conflictId],
  );
  await expect(
    runSecurityCli(['migrate-security', '--backup-confirmed'], async () => passphrase),
  ).rejects.toThrow('ADMIN_IDENTITY_MIGRATION_REQUIRED');
  await expect(runSecurityCli(migrationArgs, async () => 'wrong-password')).rejects.toThrow();
  expect(await readLocalPlatformConfig()).toBeNull();
  await runSecurityCli(migrationArgs, async () => passphrase);
  const renamed = (
    await pool.query(
      'SELECT id, password_hash, session_generation, totp_secret_id, normalized_username FROM ape_dts_platform.users WHERE id = $1',
      [adminId],
    )
  ).rows[0];
  expect(renamed).toMatchObject({
    id: oldAdmin.id,
    password_hash: oldAdmin.password_hash,
    totp_secret_id: oldAdmin.totp_secret_id,
    normalized_username: 'admin',
  });
  expect(BigInt(renamed.session_generation)).toBeGreaterThan(BigInt(oldAdmin.session_generation));
  expect(
    (
      await pool.query(
        'SELECT revocation_reason FROM ape_dts_platform.auth_sessions WHERE user_id = $1',
        [adminId],
      )
    ).rows[0].revocation_reason,
  ).toBe('ADMINISTRATOR_IDENTITY_MIGRATED');
  expect(
    (
      await pool.query(
        `SELECT actor_type, subject_id FROM ape_dts_platform.audit_events WHERE action = 'ADMINISTRATOR_IDENTITY_MIGRATED'`,
      )
    ).rows,
  ).toEqual([{ actor_type: 'DEPLOYMENT_ADMIN', subject_id: adminId }]);
  expect(
    (
      await pool.query('SELECT ciphertext FROM ape_dts_platform.secrets WHERE id = $1', [
        context.secretId,
      ])
    ).rows[0].ciphertext,
  ).toEqual(Buffer.from(encrypted.ciphertext, 'base64'));
  const config = await readLocalPlatformConfig();
  expect(config?.dek).toBe(material.dek.toString('base64'));
  expect(decryptSecret(Buffer.from(config!.dek, 'base64'), encrypted, context).toString()).toBe(
    'preserved-data-source-password',
  );
  const log = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
  const app = await boot();
  try {
    expect(app.get(BootstrapStateService).getStatus()).toMatchObject({ phase: 'READY' });
    // Legacy empty storage is repaired on startup, not during the security migration.
    expect(
      (await pool.query('SELECT created_by_actor_type FROM ape_dts_platform.storage_profiles'))
        .rows,
    ).toEqual([{ created_by_actor_type: 'SYSTEM' }]);
    expect(
      (
        await app
          .get(PlatformDatabaseService)
          .requirePool()
          .query('SELECT count(*)::int AS count FROM ape_dts_platform.users')
      ).rows[0].count,
    ).toBe(2);
    expect(
      log.mock.calls.some((call) => String(call[0]).includes('bootstrap.administrator_created')),
    ).toBe(false);
  } finally {
    await app.close();
    log.mockRestore();
    material.dek.fill(0);
  }
});

it('preserves existing retired user profiles without creating or reactivating a default', async () => {
  const { pool } = await database('configured');
  const log = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);
  const app = await boot();
  await app.close();
  try {
    // Replace only this isolated test database's generated fixture with a user-created profile.
    await pool.query('DELETE FROM ape_dts_platform.storage_profiles');
    await pool.query(
      `INSERT INTO ape_dts_platform.storage_profiles
      (id, profile_key, display_name, version, driver, config, state, created_by_user_id, retired_at)
      SELECT $1, 'custom', 'Custom', 1, 'LOCAL', '{"relative_prefix":"custom"}', 'RETIRED', id, now()
      FROM ape_dts_platform.users WHERE normalized_username = 'admin'`,
      [randomUUID()],
    );
    const before = await pool.query('SELECT * FROM ape_dts_platform.storage_profiles');
    const restarted = await boot();
    try {
      expect(restarted.get(BootstrapStateService).getStatus().phase).toBe('READY');
      expect((await pool.query('SELECT * FROM ape_dts_platform.storage_profiles')).rows).toEqual(
        before.rows,
      );
    } finally {
      await restarted.close();
    }
  } finally {
    log.mockRestore();
  }
});

it('creates exactly one default and audit entry when empty-table initialization races', async () => {
  const { pool } = await database('race');
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(
    await loadMigrationBundle(),
    randomUUID(),
    { expectedState: 'ABSENT' },
  );
  await Promise.all(
    Array.from({ length: 4 }, () =>
      new BootstrapPersistenceService().ensureDefaultLocalStorage(pool),
    ),
  );
  expect((await pool.query('SELECT * FROM ape_dts_platform.storage_profiles')).rows).toHaveLength(
    1,
  );
  expect(
    (
      await pool.query(
        `SELECT * FROM ape_dts_platform.audit_events WHERE action = 'DEFAULT_LOCAL_STORAGE_CREATED'`,
      )
    ).rows,
  ).toHaveLength(1);
});
