import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it } from 'vitest';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import { ConfigTransferService } from '../../../apps/api/src/config-transfer/config-transfer.service.js';
import { DataSourcesService } from '../../../apps/api/src/data-sources/data-sources.service.js';
import { DataSourceAdapterRegistry } from '../../../apps/api/src/data-sources/adapters/types.js';
import { TasksService } from '../../../apps/api/src/tasks/tasks.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import type { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AlertsService } from '../../../apps/api/src/alerts/alerts.service.js';
import { RuntimeRecoverySettingsService } from '../../../apps/api/src/settings/runtime-recovery.service.js';
import type { ConfigBundle, UserSummary } from '@scs/contracts';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const testName = `scs_transfer_${process.pid}_${Date.now()}`;
const password = randomBytes(24).toString('base64url');
const actor: UserSummary = {
  id: randomUUID(),
  username: 'transfer-admin',
  displayName: 'Transfer admin',
  role: 'ADMIN',
  status: 'ACTIVE',
};
const keys = new UnlockedKeyService();
let pool: Pool;
let transfer: ConfigTransferService;
let exported: ConfigBundle;
function docker(args: string[], input?: string) {
  return execFileSync('docker', args, {
    input,
    encoding: 'utf8',
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}
function admin(sql: string) {
  return docker(
    ['exec', '-i', container, 'psql', '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1'],
    sql,
  );
}
beforeAll(async () => {
  if (!/^scs_transfer_\d+_\d+$/u.test(testName)) throw new Error('Unsafe test database name');
  const port = Number(docker(['port', container, '5432/tcp']).split(':').at(-1));
  admin(
    `CREATE ROLE "${testName}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${testName}" OWNER "${testName}";`,
  );
  pool = new Pool({
    host: '127.0.0.1',
    port,
    database: testName,
    user: testName,
    password,
    max: 4,
  });
  const migrations = await loadMigrationBundle();
  const oldBundle = {
    manifest: {
      ...migrations.manifest,
      migrations: migrations.manifest.migrations.filter((entry) => entry.version <= 28),
    },
    migrations: migrations.migrations.filter((entry) => entry.version <= 28),
  };
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(oldBundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  await pool.query(
    'INSERT INTO ape_dts_platform.platform_settings(singleton_id,max_runtime_units) VALUES(1,4)',
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.users(id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash)
    VALUES($1,'transfer-admin','Transfer admin','ADMIN','ACTIVE','ARGON2ID','{}',$2,$3)`,
    [actor.id, Buffer.alloc(16, 1), Buffer.alloc(32, 2)],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.data_sources(id,normalized_name,source_type,configuration,state,created_by_user_id,updated_by_user_id) VALUES($1,'legacy kafka','KAFKA',$2::jsonb,'ACTIVE',$3,$3)`,
    [
      randomUUID(),
      JSON.stringify({
        type: 'KAFKA',
        bootstrapServers: ['legacy.test:9092'],
        plaintextRiskAcknowledged: true,
      }),
      actor.id,
    ],
  );
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(migrations, randomUUID(), {
    expectedState: 'OLDER',
  });
  const db = { requirePool: () => pool } as PlatformDatabaseService;
  keys.set(randomBytes(32));
  const audit = new AuditLedgerService(db);
  const sources = new DataSourcesService(db, audit, keys, new DataSourceAdapterRegistry([]));
  const tasks = new TasksService(db, audit);
  const source = await sources.create(actor.id, {
    name: 'source',
    configuration: {
      type: 'POSTGRES',
      database: 'demo',
      host: 'pg.test',
      port: 5432,
      username: 'reader',
      tls: { mode: 'REQUIRE' },
    },
    credentials: { mode: 'SET', password: 'not-in-export-credential' },
  });
  const target = await sources.create(actor.id, {
    name: 'sink',
    configuration: {
      type: 'KAFKA',
      bootstrapServers: ['kafka.test:9092'],
      plaintextRiskAcknowledged: true,
    },
    credentials: { mode: 'NONE' },
  });
  const group = await tasks.createGroup(actor, { name: '业务组' });
  await tasks.create(actor, {
    name: 'source task',
    taskType: 'PG_KAFKA',
    sourceDataSourceId: source.id,
    targetDataSourceId: target.id,
    groupId: group.id,
    artifactId: null,
    configuration: {
      kind: 'PG_TO_KAFKA',
      schemaVersion: 2,
      source: {
        dataSourceId: source.id,
        includeSnapshot: false,
        resourceMode: 'PLATFORM_MANAGED',
        slotName: 'test_slot',
        publicationName: 'test_pub',
      },
      target: { dataSourceId: target.id, messageFormat: 'json', jsonTemplate: 'cloudcanal' },
      mappings: [{ source: { schema: 'public', name: 'orders' }, targets: [{ topic: 'orders' }] }],
      tuning: {
        batchSize: 100,
        bufferSize: 100,
        checkpointIntervalSeconds: 5,
        requiredAcks: 'all',
      },
    },
  });
  const alerts = new AlertsService(db, keys, audit);
  const previous = await alerts.get();
  await alerts.update(actor.id, {
    ...previous,
    expectedRowVersion: previous.rowVersion,
    enabled: true,
    webhookUrl: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=not-in-export-webhook',
  });
  transfer = new ConfigTransferService(db, sources, tasks, audit);
  exported = await transfer.export(actor, { dataSources: false, tasks: true, system: true });
});
afterAll(async () => {
  keys.clear();
  await pool?.end();
  // Only the generated, validated test database and role are removed; no platform DB is touched.
  if (/^scs_transfer_\d+_\d+$/u.test(testName)) {
    admin(`DROP DATABASE IF EXISTS "${testName}"; DROP ROLE IF EXISTS "${testName}";`);
  }
});
function renamed(name: string): ConfigBundle {
  return {
    ...structuredClone(exported),
    dataSources: exported.dataSources.map((source) => ({
      ...source,
      name: `${name} ${source.name}`,
    })),
    tasks: exported.tasks.map((task) => ({ ...task, name })),
  };
}
it('exports task dependencies and settings without credentials or runtime state', async () => {
  expect(exported.dataSources).toHaveLength(2);
  expect(exported.tasks).toHaveLength(1);
  const text = JSON.stringify(exported);
  expect(text).not.toContain('not-in-export');
  expect(text).not.toMatch(/"(?:ciphertext|password|webhookUrl|checkpoint|desiredState)"/iu);
  const systemOnly = await transfer.export(actor, {
    dataSources: false,
    tasks: false,
    system: true,
  });
  expect(systemOnly.dataSources).toHaveLength(0);
  expect(systemOnly.tasks).toHaveLength(0);
  expect(systemOnly.system?.sessions).toEqual(exported.system?.sessions);
  expect(systemOnly.system?.recovery).toEqual({
    enabled: true,
    maxAttempts: 3,
    retryDelaySeconds: 30,
  });
});

it('persists recovery policy with optimistic concurrency and migrates it with system settings', async () => {
  const db = { requirePool: () => pool } as PlatformDatabaseService;
  const settings = new RuntimeRecoverySettingsService(db, new AuditLedgerService(db));
  const initial = await settings.get();
  const changed = await settings.update(actor.id, {
    enabled: false,
    maxAttempts: 5,
    retryDelaySeconds: 60,
    expectedRowVersion: initial.rowVersion,
  });
  expect(changed).toMatchObject({ enabled: false, maxAttempts: 5, retryDelaySeconds: 60 });
  await expect(
    settings.update(actor.id, {
      enabled: true,
      maxAttempts: 1,
      retryDelaySeconds: 5,
      expectedRowVersion: initial.rowVersion,
    }),
  ).rejects.toMatchObject({ response: { code: 'RUNTIME_RECOVERY_SETTINGS_CONFLICT' } });
  const bundle = await transfer.export(actor, { dataSources: false, tasks: false, system: true });
  expect(bundle.system?.recovery).toEqual({
    enabled: false,
    maxAttempts: 5,
    retryDelaySeconds: 60,
  });
  const preview = await transfer.preview(actor, bundle);
  await transfer.import(actor, bundle, preview.previewHash);
  expect(await settings.get()).toMatchObject(bundle.system!.recovery!);
  // Old v1 bundles omit the new field and must preserve the destination recovery policy.
  delete bundle.system!.recovery;
  const legacyPreview = await transfer.preview(actor, bundle);
  await transfer.import(actor, bundle, legacyPreview.previewHash);
  expect(await settings.get()).toMatchObject({
    enabled: false,
    maxAttempts: 5,
    retryDelaySeconds: 60,
  });
});
it('upgrades existing sources without weakening the ordinary secret constraint', async () => {
  expect(
    (
      await pool.query(
        "SELECT credentials_required FROM ape_dts_platform.data_sources WHERE normalized_name='legacy kafka'",
      )
    ).rows[0].credentials_required,
  ).toBe(false);
  await expect(
    pool.query(
      `INSERT INTO ape_dts_platform.data_sources(id,normalized_name,source_type,configuration,state,created_by_user_id,updated_by_user_id) VALUES($1,'invalid pg','POSTGRES','{"type":"POSTGRES"}','ACTIVE',$2,$2)`,
      [randomUUID(), actor.id],
    ),
  ).rejects.toMatchObject({ constraint: 'data_sources_secret_consistency' });
});
it('previews conflicts, invalidates stale previews, remaps identities and imports only stopped drafts', async () => {
  const conflicted = await transfer.preview(actor, exported);
  expect(conflicted.conflicts).toHaveLength(3);
  const packet = renamed('imported task');
  const preview = await transfer.preview(actor, packet);
  expect(preview.conflicts).toHaveLength(0);
  await pool.query(
    'UPDATE ape_dts_platform.platform_settings SET row_version=row_version+1 WHERE singleton_id=1',
  );
  await expect(transfer.import(actor, packet, preview.previewHash)).rejects.toThrow();
  const refreshed = await transfer.preview(actor, packet);
  await transfer.import(actor, packet, refreshed.previewHash);
  const sources = (
    await pool.query(
      "SELECT id,connection_secret_id FROM ape_dts_platform.data_sources WHERE normalized_name LIKE 'imported task%'",
    )
  ).rows;
  expect(sources).toHaveLength(2);
  expect(sources.every((source) => source.connection_secret_id === null)).toBe(true);
  const importedSource = (
    await pool.query(
      "SELECT id FROM ape_dts_platform.data_sources WHERE normalized_name='imported task source'",
    )
  ).rows[0].id;
  const db = { requirePool: () => pool } as PlatformDatabaseService;
  const sourceService = new DataSourcesService(
    db,
    new AuditLedgerService(db),
    keys,
    new DataSourceAdapterRegistry([]),
  );
  const missing = await sourceService.get(actor, importedSource);
  expect(missing.credentialsRequired).toBe(true);
  const configured = await sourceService.update(actor.id, importedSource, {
    name: missing.name,
    configuration: missing.configuration as Extract<
      typeof missing.configuration,
      { type: 'POSTGRES' }
    >,
    expectedRowVersion: missing.rowVersion,
    credentials: { mode: 'REPLACE', password: 'replacement-for-imported-source' },
  });
  expect(configured.credentialsRequired).not.toBe(true);
  const task = (
    await pool.query(
      "SELECT desired_state,observed_state,current_applied_revision_id,r.configuration FROM ape_dts_platform.tasks t JOIN ape_dts_platform.task_revisions r ON r.id=t.current_draft_revision_id WHERE t.display_name='imported task'",
    )
  ).rows[0];
  expect(task.desired_state).toBe('STOPPED');
  expect(task.observed_state).toBe('STOPPED');
  expect(task.current_applied_revision_id).toBeNull();
  expect(sources.map((source) => source.id)).toContain(task.configuration.source.dataSourceId);
  expect(exported.dataSources.map((source) => source.id)).not.toContain(
    task.configuration.source.dataSourceId,
  );
  expect(
    (await pool.query('SELECT count(*)::int AS count FROM ape_dts_platform.task_runs')).rows[0]
      .count,
  ).toBe(0);
  expect(
    (await pool.query('SELECT count(*)::int AS count FROM ape_dts_platform.external_resources'))
      .rows[0].count,
  ).toBe(0);
  expect(
    (await pool.query('SELECT count(*)::int AS count FROM ape_dts_platform.task_groups')).rows[0]
      .count,
  ).toBe(1);
  expect(
    (
      await pool.query(
        'SELECT enabled,encrypted_webhook FROM ape_dts_platform.alert_settings WHERE singleton_id=1',
      )
    ).rows[0],
  ).toMatchObject({ enabled: false, encrypted_webhook: expect.any(Object) });
});
it('rolls back imported data sources and their audits if task creation fails inside the transaction', async () => {
  await pool.query(`CREATE FUNCTION ape_dts_platform.reject_test_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.display_name='rollback task' THEN RAISE EXCEPTION 'test import failure'; END IF; RETURN NEW; END $$;
    CREATE TRIGGER reject_test_import BEFORE INSERT ON ape_dts_platform.tasks FOR EACH ROW EXECUTE PROCEDURE ape_dts_platform.reject_test_import();`);
  const count = async () =>
    (await pool.query('SELECT count(*)::int AS count FROM ape_dts_platform.audit_events')).rows[0]
      .count;
  const before = await count();
  const packet = renamed('rollback task');
  const preview = await transfer.preview(actor, packet);
  await expect(transfer.import(actor, packet, preview.previewHash)).rejects.toThrow();
  expect(
    (
      await pool.query(
        "SELECT count(*)::int AS count FROM ape_dts_platform.data_sources WHERE normalized_name LIKE 'rollback task%'",
      )
    ).rows[0].count,
  ).toBe(0);
  expect(await count()).toBe(before);
});

it('persists folder order and renames without changing membership, rejecting stale mutations', async () => {
  const db = { requirePool: () => pool } as PlatformDatabaseService;
  const tasks = new TasksService(db, new AuditLedgerService(db));
  const first = await tasks.createGroup(actor, { name: '排序一' });
  const second = await tasks.createGroup(actor, { name: '排序二' });
  const before = (await tasks.listGroups()).map((group) => group.id);
  const reordered = [second.id, ...before.filter((id) => id !== second.id)];
  await tasks.reorderGroups(actor, { groupIds: reordered, expectedGroupIds: before });
  const reloaded = new TasksService(db, new AuditLedgerService(db));
  expect((await reloaded.listGroups()).map((group) => group.id)).toEqual(reordered);
  await expect(
    tasks.reorderGroups(actor, { groupIds: before, expectedGroupIds: before }),
  ).rejects.toMatchObject({ code: 'TASK_GROUP_CHANGED' });
  await expect(
    tasks.reorderGroups(actor, { groupIds: [first.id], expectedGroupIds: reordered }),
  ).rejects.toMatchObject({ code: 'TASK_GROUP_CHANGED' });
  await expect(
    tasks.renameGroup(actor, first.id, { expectedName: first.name, name: second.name }),
  ).rejects.toMatchObject({ code: 'TASK_GROUP_NAME_ALREADY_EXISTS' });
  const renamed = await tasks.renameGroup(actor, first.id, {
    expectedName: first.name,
    name: '新组名',
  });
  expect(renamed.name).toBe('新组名');
  await expect(
    tasks.deleteGroup(actor, first.id, { expectedName: first.name, detachTasks: true }),
  ).rejects.toMatchObject({ code: 'TASK_GROUP_CHANGED' });
  await tasks.deleteGroup(actor, renamed.id, { expectedName: renamed.name, detachTasks: false });
  expect((await tasks.listGroups()).some((group) => group.id === first.id)).toBe(false);
});

it('requires confirmation for occupied folders and only detaches tasks, preserving drafts and runtime state', async () => {
  const db = { requirePool: () => pool } as PlatformDatabaseService;
  const tasks = new TasksService(db, new AuditLedgerService(db));
  const group = (await tasks.listGroups()).find((item) => item.name === '业务组')!;
  const id = (
    await pool.query("SELECT id FROM ape_dts_platform.tasks WHERE display_name='source task'")
  ).rows[0].id;
  const before = await tasks.get(actor, id);
  const memberships = (
    await pool.query('SELECT * FROM ape_dts_platform.task_group_memberships WHERE group_id=$1', [
      group.id,
    ])
  ).rows;
  expect(memberships.length).toBeGreaterThan(0);
  await expect(
    tasks.deleteGroup(actor, group.id, { expectedName: group.name, detachTasks: false }),
  ).rejects.toMatchObject({ code: 'TASK_GROUP_NOT_EMPTY' });
  expect(await tasks.get(actor, id)).toEqual(before);
  expect(await tasks.listGroups()).toContainEqual(group);
  await tasks.deleteGroup(actor, group.id, { expectedName: group.name, detachTasks: true });
  expect(await tasks.get(actor, id)).toEqual({ ...before, groupId: null });
  expect(
    (
      await pool.query('SELECT * FROM ape_dts_platform.task_group_memberships WHERE group_id=$1', [
        group.id,
      ])
    ).rows,
  ).toHaveLength(0);
  const all = await tasks.list(actor);
  for (const member of memberships)
    expect(all.some((task) => task.id === member.task_id && task.groupId === null)).toBe(true);
});
