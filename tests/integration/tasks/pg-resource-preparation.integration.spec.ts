import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import type { UserSummary, TaskSummary } from '../../../packages/contracts/src/index.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import { DataSourcesService } from '../../../apps/api/src/data-sources/data-sources.service.js';
import { DataSourceAdapterRegistry } from '../../../apps/api/src/data-sources/adapters/types.js';
import { PostgresDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/postgres-data-source.adapter.js';
import { TasksService } from '../../../apps/api/src/tasks/tasks.service.js';
import { PgResourceManager } from '../../../apps/api/src/pg-resources/pg-resource-manager.js';
import { PgResourcePreparationService } from '../../../apps/api/src/pg-resources/pg-resource-preparation.service.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const role = `scs_pgres_${suffix}`;
const metadata = `scs_pgres_meta_${suffix}`;
const source = `scs_pgres_source_${suffix}`;
const password = randomBytes(24).toString('base64url');
const actor: UserSummary = {
  id: randomUUID(),
  username: 'pg-resource-admin',
  displayName: 'PG test',
  role: 'ADMIN',
  status: 'ACTIVE',
};
let pool: Pool;
let external: Pool;
let database: PlatformDatabaseService;
let dataSources: DataSourcesService;
let tasks: TasksService;
let preparations: PgResourcePreparationService;
let sourceId: string;
let targetId: string;
const key = new UnlockedKeyService();
const adapter = new PostgresDataSourceAdapter();
let input: Parameters<PostgresDataSourceAdapter['preparePostgresResources']>[0];

function docker(args: string[], sql?: string) {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    input: sql,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120000,
  }).trim();
}
function admin(db: string, sql: string) {
  return docker(
    [
      'exec',
      '--interactive',
      container,
      'psql',
      '-U',
      'postgres',
      '-d',
      db,
      '-v',
      'ON_ERROR_STOP=1',
    ],
    sql,
  );
}
function names() {
  const tail = randomUUID().replaceAll('-', '');
  const slotName = `pgres_${tail}`;
  return { slotName, publicationName: `pgres_${tail}`, schema: 'public', table: 'sample' };
}
async function task(
  mode: 'SELF_MANAGED' | 'PLATFORM_MANAGED' | 'APE_DTS_MANAGED',
  n = names(),
  tables?: readonly { schema: string; name: string }[],
) {
  const created = await tasks.create(actor, {
    name: randomUUID(),
    taskType: 'PG_KAFKA',
    sourceDataSourceId: sourceId,
    targetDataSourceId: targetId,
    configuration: {
      kind: 'PG_TO_KAFKA',
      schemaVersion: tables ? 2 : 1,
      source: {
        dataSourceId: sourceId,
        resourceMode: mode,
        includeSnapshot: false,
        slotName: n.slotName,
        publicationName: n.publicationName,
        ...(tables ? {} : { table: { schema: n.schema, name: n.table } }),
      },
      target: { dataSourceId: targetId, ...(tables ? {} : { topic: 'test.target' }) },
      ...(tables
        ? { mappings: tables.map((source) => ({ source, targets: [{ topic: 'test.target' }] })) }
        : {}),
      tuning: {
        batchSize: 100,
        bufferSize: 16000,
        checkpointIntervalSeconds: 15,
        requiredAcks: 'all',
      },
    },
  });
  return { created, n };
}
function request(t: TaskSummary) {
  return {
    revisionId: t.draft!.id,
    expectedTaskRowVersion: t.rowVersion,
    expectedDraftVersion: t.draft!.draftVersion,
    acknowledgeReplicationRisk: true as const,
  };
}

async function historicalExpansion(mode: 'SELF_MANAGED' | 'PLATFORM_MANAGED' = 'PLATFORM_MANAGED') {
  const p = await task(mode);
  if (mode === 'SELF_MANAGED')
    await adapter.preparePostgresResources(
      input,
      { ...p.n, resourceMode: 'PLATFORM_MANAGED' },
      async () => undefined,
    );
  await preparations.prepare(actor, p.created.id, request(p.created));
  await pool.query("UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1", [
    p.created.draft!.id,
  ]);
  await pool.query(
    'UPDATE ape_dts_platform.tasks SET current_applied_revision_id=current_draft_revision_id,current_draft_revision_id=NULL WHERE id=$1',
    [p.created.id],
  );
  const applied = await tasks.get(actor, p.created.id);
  const old = applied.applied!.configuration;
  const configuration = {
    ...old,
    schemaVersion: 2,
    source: {
      dataSourceId: sourceId,
      resourceMode: mode,
      includeSnapshot: false,
      slotName: p.n.slotName,
      publicationName: p.n.publicationName,
    },
    target: { dataSourceId: targetId },
    mappings: [
      { source: { schema: 'public', name: 'sample' }, targets: [{ topic: 'test.target' }] },
      { source: { schema: 'public', name: 'other' }, targets: [{ topic: 'test.other' }] },
    ],
  };
  const saved = await tasks.saveDraft(actor, applied.id, {
    taskType: applied.taskType,
    revisionId: applied.applied!.id,
    expectedTaskRowVersion: applied.rowVersion,
    expectedDraftVersion: applied.applied!.draftVersion,
    acknowledgePgTableAddition: true,
    configuration,
  });
  return { ...p, applied, saved };
}

beforeAll(async () => {
  const output = docker(['port', container, '5432/tcp']);
  const port = Number(output.slice(output.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(port) || port <= 0) throw new Error('Invalid test PostgreSQL port');
  admin(
    'postgres',
    `CREATE ROLE "${role}" LOGIN REPLICATION PASSWORD '${password}'; CREATE DATABASE "${metadata}" OWNER "${role}"; CREATE DATABASE "${source}" OWNER "${role}";`,
  );
  admin(source, `GRANT EXECUTE ON FUNCTION pg_control_system() TO "${role}";`);
  const connection = { host: '127.0.0.1', port, user: role, password };
  pool = new Pool({ ...connection, database: metadata });
  external = new Pool({ ...connection, database: source });
  await external.query(
    'CREATE TABLE public.sample(id bigint PRIMARY KEY, value text); CREATE TABLE public.other(id bigint PRIMARY KEY)',
  );
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  database = new PlatformDatabaseService();
  await database.inspectAndUse(
    {
      host: connection.host,
      port,
      username: role,
      password,
      database: metadata,
      tls: { mode: 'DISABLE' },
    },
    bundle.manifest,
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.users (id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash)
    VALUES ($1,'pg-resource-admin','PG test','ADMIN','ACTIVE','ARGON2ID',$2,$3,$4)`,
    [
      actor.id,
      { memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 },
      Buffer.alloc(16, 1),
      Buffer.alloc(32, 2),
    ],
  );
  key.set(randomBytes(32));
  const audit = new AuditLedgerService(database);
  dataSources = new DataSourcesService(
    database,
    audit,
    key,
    new DataSourceAdapterRegistry([adapter]),
  );
  const pg = await dataSources.create(actor.id, {
    name: 'PG source',
    configuration: {
      type: 'POSTGRES',
      host: connection.host,
      port,
      username: role,
      database: source,
      tls: { mode: 'DISABLE', riskAcknowledged: true },
    },
    credentials: { mode: 'SET', password },
  });
  sourceId = pg.id;
  targetId = (
    await dataSources.create(actor.id, {
      name: 'Kafka fixture only',
      configuration: {
        type: 'KAFKA',
        bootstrapServers: ['127.0.0.1:9092'],
        plaintextRiskAcknowledged: true,
      },
      credentials: { mode: 'NONE' },
    })
  ).id;
  tasks = new TasksService(database, audit);
  preparations = new PgResourcePreparationService(
    database,
    dataSources,
    new PgResourceManager(database, audit),
  );
  input = { configuration: pg.configuration, password };
}, 120000);

afterAll(async () => {
  vi.restoreAllMocks();
  key.clear();
  await database?.onApplicationShutdown();
  await pool?.end();
  await external?.end();
  if (![role, metadata, source].every((n) => /^scs_pgres_(?:(?:meta|source)_)?\d+_\d+$/u.test(n)))
    throw new Error('Unsafe test cleanup');
  // Remove only generated slots from this isolated source database.
  admin(
    'postgres',
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('${metadata}','${source}') AND pid<>pg_backend_pid();
    SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database='${source}' AND slot_name LIKE 'pgres_%';
    DROP DATABASE IF EXISTS "${source}"; DROP DATABASE IF EXISTS "${metadata}"; DROP ROLE IF EXISTS "${role}";`,
  );
});

describe('explicit PostgreSQL resource preparation in isolated databases', () => {
  it('prepares a confirmed stopped historical expansion while retaining applied history and slot LSN', async () => {
    const p = await historicalExpansion();
    const before = (
      await external.query(
        'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
        [p.n.slotName],
      )
    ).rows[0];
    expect(await preparations.prepare(actor, p.saved.id, request(p.saved))).toMatchObject({
      slot: { created: false },
      publication: { created: false, extendedTables: [{ schema: 'public', name: 'other' }] },
    });
    expect(await preparations.prepare(actor, p.saved.id, request(p.saved))).toMatchObject({
      publication: { extendedTables: [] },
    });
    expect(
      (
        await external.query(
          'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
          [p.n.slotName],
        )
      ).rows[0],
    ).toEqual(before);
    const after = await tasks.get(actor, p.saved.id);
    expect(after.applied).toEqual(p.applied.applied);
    expect(after.draft!.id).toBe(p.saved.draft!.id);
    expect(
      (
        await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
          p.saved.id,
        ])
      ).rows,
    ).toHaveLength(2);
  });

  it('refuses expansion when an old table was removed from the publication without restoring or adding tables', async () => {
    const p = await historicalExpansion();
    await external.query(`ALTER PUBLICATION "${p.n.publicationName}" DROP TABLE public.sample`);
    const before = (
      await external.query(
        'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
        [p.n.slotName],
      )
    ).rows[0];
    await expect(preparations.prepare(actor, p.saved.id, request(p.saved))).rejects.toMatchObject({
      code: 'PG_RESOURCE_CONFLICT',
    });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([]);
    expect(
      (
        await external.query(
          'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
          [p.n.slotName],
        )
      ).rows[0],
    ).toEqual(before);
    const after = await tasks.get(actor, p.saved.id);
    expect(after.applied).toEqual(p.applied.applied);
    expect(after.draft).toEqual(p.saved.draft);
  });

  it('refuses stale historical confirmation and incompatible bindings without adding tables', async () => {
    const p = await historicalExpansion();
    await pool.query(
      'UPDATE ape_dts_platform.task_revisions SET draft_version=draft_version+1 WHERE id=$1',
      [p.saved.draft!.id],
    );
    const edited = await tasks.get(actor, p.saved.id);
    await expect(preparations.prepare(actor, edited.id, request(edited))).rejects.toMatchObject({
      code: 'PG_RESOURCE_PREPARATION_NOT_ALLOWED',
    });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
    const conflict = await historicalExpansion();
    await pool.query(
      "UPDATE ape_dts_platform.external_resources SET lifecycle_state='TRACKED_AFTER_STOP' WHERE task_id=$1 AND resource_type='PG_PUBLICATION'",
      [conflict.saved.id],
    );
    await expect(
      preparations.prepare(actor, conflict.saved.id, request(conflict.saved)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          conflict.n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
  });

  it('only verifies self-managed historical expansions and refuses preparation while running', async () => {
    const p = await historicalExpansion('SELF_MANAGED');
    await expect(preparations.prepare(actor, p.saved.id, request(p.saved))).rejects.toMatchObject({
      code: 'PG_RESOURCE_CONFLICT',
    });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
    await external.query(`ALTER PUBLICATION "${p.n.publicationName}" ADD TABLE public.other`);
    expect(await preparations.prepare(actor, p.saved.id, request(p.saved))).toMatchObject({
      slot: { created: false },
      publication: { created: false, extendedTables: [] },
    });
    await pool.query("UPDATE ape_dts_platform.tasks SET observed_state='RUNNING' WHERE id=$1", [
      p.saved.id,
    ]);
    await expect(preparations.prepare(actor, p.saved.id, request(p.saved))).rejects.toMatchObject({
      code: 'PG_RESOURCE_PREPARATION_NOT_ALLOWED',
    });
  });

  it('reports a completed publication extension if metadata validation fails afterward and retries idempotently', async () => {
    const p = await historicalExpansion();
    const original = adapter.preparePostgresResources.bind(adapter);
    const spy = vi
      .spyOn(adapter, 'preparePostgresResources')
      .mockImplementationOnce(async (...args) => {
        const result = await original(...args);
        await pool.query(
          'UPDATE ape_dts_platform.tasks SET row_version=row_version+1 WHERE id=$1',
          [p.saved.id],
        );
        return result;
      });
    try {
      await expect(preparations.prepare(actor, p.saved.id, request(p.saved))).rejects.toMatchObject(
        {
          code: 'PG_RESOURCE_PREPARATION_FAILED',
          progress: {
            created: { slot: false, publication: false },
            publicationExtendedTables: [{ schema: 'public', name: 'other' }],
            requiresInspection: true,
          },
        },
      );
    } finally {
      spy.mockRestore();
    }
    const refreshed = await tasks.get(actor, p.saved.id);
    expect(await preparations.prepare(actor, refreshed.id, request(refreshed))).toMatchObject({
      publication: { extendedTables: [] },
    });
    expect(refreshed.applied).toEqual(p.applied.applied);
  });

  it('extends a confirmed existing publication additively without recreating or advancing its slot', async () => {
    const n = names();
    const before = await adapter.preparePostgresResources(
      input,
      { ...n, resourceMode: 'PLATFORM_MANAGED' },
      async () => undefined,
    );
    const slot = (
      await external.query(
        'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
        [n.slotName],
      )
    ).rows[0];
    const extending = {
      slotName: n.slotName,
      publicationName: n.publicationName,
      resourceMode: 'PLATFORM_MANAGED' as const,
      extendPublication: { addedTables: [{ schema: 'public', name: 'other' }] },
      tables: [
        { schema: 'public', name: 'sample' },
        { schema: 'public', name: 'other' },
      ],
    };
    const expanded = await adapter.preparePostgresResources(
      input,
      extending,
      async () => undefined,
    );
    expect(expanded).toMatchObject({
      slot: { created: false, stableIdentity: before.slot.stableIdentity },
      publication: {
        created: false,
        stableIdentity: before.publication.stableIdentity,
        extendedTables: [{ schema: 'public', name: 'other' }],
      },
    });
    expect(
      (
        await external.query(
          'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1',
          [n.slotName],
        )
      ).rows[0],
    ).toEqual(slot);
    expect(
      (
        await external.query(
          'SELECT tablename FROM pg_publication_tables WHERE pubname=$1 ORDER BY tablename',
          [n.publicationName],
        )
      ).rows,
    ).toEqual([{ tablename: 'other' }, { tablename: 'sample' }]);
    expect(
      await adapter.preparePostgresResources(input, extending, async () => undefined),
    ).toMatchObject({ publication: { extendedTables: [] } });
  });

  it('does not create a missing slot or publication while extending historical resources', async () => {
    const n = names();
    await external.query(`CREATE PUBLICATION "${n.publicationName}" FOR TABLE public.sample`);
    await expect(
      adapter.preparePostgresResources(
        input,
        {
          slotName: n.slotName,
          publicationName: n.publicationName,
          resourceMode: 'PLATFORM_MANAGED',
          extendPublication: { addedTables: [{ schema: 'public', name: 'other' }] },
          tables: [
            { schema: 'public', name: 'sample' },
            { schema: 'public', name: 'other' },
          ],
        },
        async () => undefined,
      ),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_NOT_FOUND' });
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1', [
          n.slotName,
        ])
      ).rows,
    ).toEqual([]);
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
  });

  it('never extends a self-managed publication even with internal expansion intent', async () => {
    const n = names();
    await adapter.preparePostgresResources(
      input,
      { ...n, resourceMode: 'PLATFORM_MANAGED' },
      async () => undefined,
    );
    await expect(
      adapter.preparePostgresResources(
        input,
        {
          slotName: n.slotName,
          publicationName: n.publicationName,
          resourceMode: 'SELF_MANAGED',
          extendPublication: { addedTables: [{ schema: 'public', name: 'other' }] },
          tables: [
            { schema: 'public', name: 'sample' },
            { schema: 'public', name: 'other' },
          ],
        },
        async () => undefined,
      ),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
  });

  it('prepares one slot and one publication for every table in a saved v2 task', async () => {
    const tables = [
      { schema: 'public', name: 'sample' },
      { schema: 'public', name: 'other' },
    ];
    const p = await task('PLATFORM_MANAGED', names(), tables);
    expect(p.created.draft?.state).toBe('READY_DRAFT');
    const prepared = await preparations.prepare(actor, p.created.id, request(p.created));
    expect(prepared).toMatchObject({ slot: { created: true }, publication: { created: true } });
    expect(
      (
        await external.query(
          'SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=$1 ORDER BY tablename',
          [p.n.publicationName],
        )
      ).rows,
    ).toEqual([
      { schemaname: 'public', tablename: 'other' },
      { schemaname: 'public', tablename: 'sample' },
    ]);
    expect(
      await adapter.readPostgresResources(input, {
        slotName: p.n.slotName,
        publicationName: p.n.publicationName,
        tables,
      }),
    ).toMatchObject({
      slot: { created: false, stableIdentity: prepared.slot.stableIdentity },
      publication: { created: false, stableIdentity: prepared.publication.stableIdentity },
    });
  });

  it('does not modify an existing publication missing the second table of a saved v2 task', async () => {
    const p = await task('PLATFORM_MANAGED', names(), [
      { schema: 'public', name: 'sample' },
      { schema: 'public', name: 'other' },
    ]);
    await external.query(`CREATE PUBLICATION "${p.n.publicationName}" FOR TABLE public.sample`);
    await expect(
      preparations.prepare(actor, p.created.id, request(p.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'sample' }]);
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1', [
          p.n.slotName,
        ])
      ).rows,
    ).toEqual([]);
    expect(
      (
        await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
          p.created.id,
        ])
      ).rows,
    ).toEqual([]);
  });

  it('rejects a later table with no replica identity without leaving a publication or slot', async () => {
    await external.query(
      'CREATE TABLE public.invalid_later(id bigint PRIMARY KEY); ALTER TABLE public.invalid_later REPLICA IDENTITY NOTHING',
    );
    const p = await task('PLATFORM_MANAGED', names(), [
      { schema: 'public', name: 'sample' },
      { schema: 'public', name: 'invalid_later' },
    ]);
    await expect(
      preparations.prepare(actor, p.created.id, request(p.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT pubname FROM pg_publication WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([]);
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1', [
          p.n.slotName,
        ])
      ).rows,
    ).toEqual([]);
  });

  it('serializes concurrent same-task preparations without retaining an XID during slot creation', async () => {
    const p = await task('PLATFORM_MANAGED');
    const results = await Promise.all([
      preparations.prepare(actor, p.created.id, request(p.created)),
      preparations.prepare(actor, p.created.id, request(p.created)),
    ]);
    expect(results.filter((result) => result.slot.created)).toHaveLength(1);
    expect(
      (
        await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
          p.created.id,
        ])
      ).rows,
    ).toHaveLength(2);
  });
  it('reports created objects and refuses binding when the draft changes during external I/O', async () => {
    const p = await task('PLATFORM_MANAGED');
    const original = adapter.preparePostgresResources.bind(adapter);
    const spy = vi
      .spyOn(adapter, 'preparePostgresResources')
      .mockImplementationOnce(async (...args) => {
        const result = await original(...args);
        await pool.query(
          'UPDATE ape_dts_platform.tasks SET row_version=row_version+1 WHERE id=$1',
          [p.created.id],
        );
        return result;
      });
    try {
      await expect(
        preparations.prepare(actor, p.created.id, request(p.created)),
      ).rejects.toMatchObject({
        code: 'PG_RESOURCE_PREPARATION_FAILED',
        progress: { created: { slot: true, publication: true }, requiresInspection: true },
      });
      expect(
        (
          await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
            p.created.id,
          ])
        ).rows,
      ).toEqual([]);
    } finally {
      spy.mockRestore();
    }
  });
  it('does not accept a primary key when replica identity is explicitly NOTHING', async () => {
    const n = names();
    await external.query(
      'CREATE TABLE public.no_identity(id bigint PRIMARY KEY); ALTER TABLE public.no_identity REPLICA IDENTITY NOTHING',
    );
    await expect(
      adapter.preparePostgresResources(
        input,
        { ...n, table: 'no_identity', resourceMode: 'PLATFORM_MANAGED' },
        async () => undefined,
      ),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT pubname FROM pg_publication WHERE pubname=$1', [
          n.publicationName,
        ])
      ).rows,
    ).toEqual([]);
  });
  it('creates missing resources, binds actual creation evidence and is idempotent', async () => {
    const { created, n } = await task('PLATFORM_MANAGED');
    const first = await preparations.prepare(actor, created.id, request(created));
    expect(first).toMatchObject({
      slot: { name: n.slotName, created: true },
      publication: { created: true },
      requiresPreflight: true,
    });
    const second = await preparations.prepare(actor, created.id, request(created));
    expect(second).toMatchObject({ slot: { created: false }, publication: { created: false } });
    expect(second.slot.stableIdentity).toBe(first.slot.stableIdentity);
    expect(
      (
        await pool.query(
          'SELECT platform_created FROM ape_dts_platform.external_resources WHERE task_id=$1',
          [created.id],
        )
      ).rows,
    ).toEqual([{ platform_created: true }, { platform_created: true }]);
    expect((await tasks.get(actor, created.id)).rowVersion).toBe(created.rowVersion);
    const other = await task('PLATFORM_MANAGED', n);
    await expect(
      preparations.prepare(actor, other.created.id, request(other.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
  });
  it('binds existing SELF resources without claiming the platform created them', async () => {
    const n = names();
    await adapter.preparePostgresResources(
      input,
      { ...n, resourceMode: 'PLATFORM_MANAGED' },
      async () => undefined,
    );
    const { created } = await task('SELF_MANAGED', n);
    expect(await preparations.prepare(actor, created.id, request(created))).toMatchObject({
      slot: { created: false },
      publication: { created: false },
    });
    expect(
      (
        await pool.query(
          'SELECT platform_created FROM ape_dts_platform.external_resources WHERE task_id=$1',
          [created.id],
        )
      ).rows,
    ).toEqual([{ platform_created: false }, { platform_created: false }]);
  });
  it('rejects an existing non-pgoutput slot without creating its publication', async () => {
    const p = await task('PLATFORM_MANAGED');
    await external.query("SELECT pg_create_logical_replication_slot($1, 'test_decoding')", [
      p.n.slotName,
    ]);
    await expect(
      preparations.prepare(actor, p.created.id, request(p.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_CONFLICT' });
    expect(
      (
        await external.query('SELECT plugin FROM pg_replication_slots WHERE slot_name=$1', [
          p.n.slotName,
        ])
      ).rows,
    ).toEqual([{ plugin: 'test_decoding' }]);
    expect(
      (
        await external.query('SELECT pubname FROM pg_publication WHERE pubname=$1', [
          p.n.publicationName,
        ])
      ).rows,
    ).toEqual([]);
  });
  it('refuses binding after a source version changes during creation', async () => {
    const p = await task('PLATFORM_MANAGED');
    const original = adapter.preparePostgresResources.bind(adapter);
    const spy = vi
      .spyOn(adapter, 'preparePostgresResources')
      .mockImplementationOnce(async (...args) => {
        const result = await original(...args);
        await pool.query(
          'UPDATE ape_dts_platform.data_sources SET row_version=row_version+1 WHERE id=$1',
          [sourceId],
        );
        return result;
      });
    try {
      await expect(
        preparations.prepare(actor, p.created.id, request(p.created)),
      ).rejects.toMatchObject({
        code: 'PG_RESOURCE_PREPARATION_FAILED',
        progress: { created: { slot: true, publication: true } },
      });
      expect(
        (
          await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
            p.created.id,
          ])
        ).rows,
      ).toEqual([]);
    } finally {
      spy.mockRestore();
    }
  });
  it('rejects incompatible existing publication before creating the missing slot', async () => {
    const { created, n } = await task('PLATFORM_MANAGED');
    await external.query(`CREATE PUBLICATION "${n.publicationName}" FOR TABLE public.other`);
    await expect(preparations.prepare(actor, created.id, request(created))).rejects.toMatchObject({
      code: 'PG_RESOURCE_CONFLICT',
    });
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1', [
          n.slotName,
        ])
      ).rows,
    ).toEqual([]);
    expect(
      (
        await external.query('SELECT tablename FROM pg_publication_tables WHERE pubname=$1', [
          n.publicationName,
        ])
      ).rows,
    ).toEqual([{ tablename: 'other' }]);
  });
  it('rejects missing SELF resources, invalid versions, non-admin, engine mode and running tasks without external writes', async () => {
    const self = await task('SELF_MANAGED');
    await expect(
      preparations.prepare(actor, self.created.id, request(self.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_NOT_FOUND' });
    const p = await task('PLATFORM_MANAGED');
    await expect(
      preparations.prepare({ ...actor, role: 'DEVELOPER' }, p.created.id, request(p.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_ACCESS_DENIED' });
    await expect(
      preparations.prepare(actor, p.created.id, {
        ...request(p.created),
        expectedDraftVersion: '99',
      }),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_VERSION_CONFLICT' });
    const engine = await task('APE_DTS_MANAGED');
    await expect(
      preparations.prepare(actor, engine.created.id, request(engine.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_ENGINE_MANAGED' });
    await pool.query("UPDATE ape_dts_platform.tasks SET observed_state='STARTING' WHERE id=$1", [
      p.created.id,
    ]);
    await expect(
      preparations.prepare(actor, p.created.id, request(p.created)),
    ).rejects.toMatchObject({ code: 'PG_RESOURCE_PREPARATION_NOT_ALLOWED' });
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=ANY($1)', [
          [self.n.slotName, p.n.slotName, engine.n.slotName],
        ])
      ).rows,
    ).toEqual([]);
  });
  it('preserves external resources and reports them when platform binding fails', async () => {
    const p = await task('PLATFORM_MANAGED');
    const failing = new PgResourceManager(database, new AuditLedgerService(database));
    vi.spyOn(failing, 'bind').mockRejectedValue(new Error('metadata audit unavailable'));
    const service = new PgResourcePreparationService(database, dataSources, failing);
    await expect(service.prepare(actor, p.created.id, request(p.created))).rejects.toMatchObject({
      code: 'PG_RESOURCE_PREPARATION_FAILED',
      progress: { created: { slot: true, publication: true }, requiresInspection: true },
    });
    expect(
      (
        await external.query('SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1', [
          p.n.slotName,
        ])
      ).rows,
    ).toHaveLength(1);
    expect(
      (
        await pool.query('SELECT id FROM ape_dts_platform.external_resources WHERE task_id=$1', [
          p.created.id,
        ])
      ).rows,
    ).toEqual([]);
    expect(await preparations.prepare(actor, p.created.id, request(p.created))).toMatchObject({
      slot: { created: false },
      publication: { created: false },
    });
  });
  it('reports partial publication creation when slot creation is denied, without deleting the publication', async () => {
    const n = names();
    admin('postgres', `ALTER ROLE "${role}" NOREPLICATION;`);
    try {
      await expect(
        adapter.preparePostgresResources(
          input,
          { ...n, resourceMode: 'PLATFORM_MANAGED' },
          async () => undefined,
        ),
      ).rejects.toMatchObject({
        code: 'PG_RESOURCE_PREPARATION_FAILED',
        progress: { created: { publication: true, slot: false } },
      });
      expect(
        (
          await external.query('SELECT pubname FROM pg_publication WHERE pubname=$1', [
            n.publicationName,
          ])
        ).rows,
      ).toHaveLength(1);
    } finally {
      admin('postgres', `ALTER ROLE "${role}" REPLICATION;`);
    }
  });
});
