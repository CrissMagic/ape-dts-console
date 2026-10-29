import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  ApeDtsV1Adapter,
  CertifiedAdapterRegistry,
  parsePlatformTaskConfig,
  runtimeText,
} from '../../../packages/config-model/src/index.js';

import { PreflightService } from '../../../apps/api/src/preflight/preflight.service.js';
import {
  RuntimeEnvironmentInspector,
  type RuntimeEnvironmentCheck,
} from '../../../apps/api/src/preflight/runtime-environment-check.js';
import { ContinuityService } from '../../../apps/api/src/preflight/continuity.service.js';
import {
  applyPgTableExpansionContinuity,
  confirmedPgTableExpansion,
} from '../../../apps/api/src/tasks/pg-table-expansion.js';
import { TaskProgressService } from '../../../apps/api/src/task-progress/task-progress.service.js';
import { MysqlRuntimeStateStore } from '../../../apps/api/src/runtime/mysql-runtime-state.js';
import { TasksService, TasksServiceError } from '../../../apps/api/src/tasks/tasks.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { CapacityReservationService } from '../../../apps/api/src/pg-resources/capacity-reservation.service.js';
import {
  PgResourceManager,
  PgResourceManagerError,
} from '../../../apps/api/src/pg-resources/pg-resource-manager.js';
import type {
  ConnectionTestResponse,
  DataSourceSummary,
  MetadataItem,
  MetadataResponse,
  UserSummary,
} from '../../../packages/contracts/src/index.js';
import type { DataSourcesService } from '../../../apps/api/src/data-sources/data-sources.service.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const databaseRole = `scs_u7_${suffix}`;
const metadataDatabase = `scs_u7_meta_${suffix}`;
const databasePassword = randomBytes(24).toString('base64url');

let port = 0;
let pool: Pool | undefined;
let platformDatabase: PlatformDatabaseService | undefined;
let tasks: TasksService | undefined;
let resources: PgResourceManager | undefined;
let capacity: CapacityReservationService | undefined;
let preflight: PreflightService | undefined;

const administrator: UserSummary = {
  displayName: 'Unit 7 administrator',
  id: randomUUID(),
  role: 'ADMIN',
  status: 'ACTIVE',
  username: `unit7-admin-${suffix}`,
};

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
    throw new Error('Unit 7 test pool is not initialized');
  }
  return pool;
}

function requireTasks(): TasksService {
  if (tasks === undefined) {
    throw new Error('Unit 7 task service is not initialized');
  }
  return tasks;
}

function requireResources(): PgResourceManager {
  if (resources === undefined) {
    throw new Error('Unit 7 resource manager is not initialized');
  }
  return resources;
}

function requireCapacity(): CapacityReservationService {
  if (capacity === undefined) {
    throw new Error('Unit 7 capacity service is not initialized');
  }
  return capacity;
}

function requirePreflight(): PreflightService {
  if (preflight === undefined) {
    throw new Error('Unit 7 preflight service is not initialized');
  }
  return preflight;
}

function requirePlatformDatabase(): PlatformDatabaseService {
  if (platformDatabase === undefined) {
    throw new Error('Unit 7 platform database is not initialized');
  }
  return platformDatabase;
}

function kafkaDataSourceSummary(id: string): DataSourceSummary {
  return {
    configuration: {
      bootstrapServers: ['127.0.0.1:9092'],
      plaintextRiskAcknowledged: true,
      type: 'KAFKA',
    },
    createdAt: '2026-09-03T00:00:00.000Z',
    deletedAt: null,
    id,
    name: `unit7-kafka-${id}`,
    rowVersion: '1',
    state: 'ACTIVE',
    updatedAt: '2026-09-03T00:00:00.000Z',
  };
}

function successfulMetadata(
  dataSourceId: string,
  resource: 'TOPICS' | 'PARTITIONS',
  items: readonly MetadataItem[],
): MetadataResponse {
  return {
    attemptedAt: new Date().toISOString(),
    dataSourceId,
    items: [...items],
    refreshedAt: new Date().toISOString(),
    resource,
    state: 'SUCCESS',
  };
}

function buildKafkaPreflightService(
  sourceId: string,
  targetId: string,
  environment: RuntimeEnvironmentCheck = {
    inspect: async () => ({
      key: 'RUNTIME_ENVIRONMENT',
      state: 'PASSED',
      code: 'RUNTIME_ENVIRONMENT_READY',
      summary: 'Test environment ready',
      details: {},
    }),
  },
): PreflightService {
  const source = kafkaDataSourceSummary(sourceId);
  const target = kafkaDataSourceSummary(targetId);
  const dataSources = {
    get: async (_actor: UserSummary, id: string): Promise<DataSourceSummary> =>
      id === sourceId ? source : target,
    metadata: async (
      _actor: UserSummary,
      id: string,
      query: { readonly resource: string; readonly topic?: string },
    ): Promise<MetadataResponse> => {
      if (query.resource === 'PARTITIONS') {
        return successfulMetadata(id, 'PARTITIONS', [
          { attributes: {}, identity: null, name: '0' },
          { attributes: {}, identity: null, name: '1' },
        ]);
      }
      return successfulMetadata(id, 'TOPICS', [
        id === sourceId
          ? {
              attributes: { partitionCount: '2' },
              identity: 'unit7-source-topic-id',
              name: 'unit7.source',
            }
          : {
              attributes: { partitionCount: '1' },
              identity: 'unit7-target-topic-id',
              name: 'unit7.target',
            },
      ]);
    },
    test: async (
      _actor: UserSummary,
      id: string,
      purpose: 'MANUAL' | 'PREFLIGHT',
    ): Promise<ConnectionTestResponse> => ({
      completedAt: new Date().toISOString(),
      dataSourceId: id,
      durationMs: 1,
      errorCode: null,
      errorSummary: null,
      id: randomUUID(),
      identity: {
        kind: 'KAFKA_CLUSTER',
        serverVersion: null,
        value: id === sourceId ? 'unit7-source-cluster' : 'unit7-target-cluster',
      },
      purpose,
      startedAt: new Date().toISOString(),
      state: 'SUCCEEDED',
    }),
  } as unknown as DataSourcesService;
  dataSources.getForSystem = async (id) => dataSources.get(administrator, id);
  dataSources.testForSystem = async (id, purpose) => dataSources.test(administrator, id, purpose);
  dataSources.metadataForSystem = async (id, query) =>
    dataSources.metadata(administrator, id, query);
  const registry = new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]);
  return new PreflightService(
    requirePlatformDatabase(),
    new AuditLedgerService(requirePlatformDatabase()),
    dataSources,
    requireCapacity(),
    registry,
    new ContinuityService(
      requirePlatformDatabase(),
      new AuditLedgerService(requirePlatformDatabase()),
    ),
    '0.0.0',
    environment,
  );
}

function kafkaConfiguration(sourceDataSourceId: string, targetDataSourceId: string) {
  return {
    kind: 'KAFKA_RAW_FORWARD' as const,
    schemaVersion: 1 as const,
    source: {
      dataSourceId: sourceDataSourceId,
      groupId: 'will-be-derived-by-service',
      topic: 'unit7.source',
    },
    target: { dataSourceId: targetDataSourceId, topic: 'unit7.target' },
    tuning: {
      ackIntervalSeconds: 5,
      batchSize: 100,
      bufferSize: 16_000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all' as const,
    },
  };
}

async function createKafkaDataSources(): Promise<{
  sourceId: string;
  targetId: string;
}> {
  const sourceId = randomUUID();
  const targetId = randomUUID();
  await requirePool().query(
    `INSERT INTO ape_dts_platform.data_sources (
       id, normalized_name, source_type, configuration, connection_secret_id,
       state, created_by_user_id, updated_by_user_id
     ) VALUES
       ($1::uuid, $2, 'KAFKA', $3::jsonb, NULL, 'ACTIVE', $5::uuid, $5::uuid),
       ($4::uuid, $6, 'KAFKA', $7::jsonb, NULL, 'ACTIVE', $5::uuid, $5::uuid)`,
    [
      sourceId,
      `unit7-source-${sourceId}`,
      JSON.stringify({
        bootstrapServers: ['127.0.0.1:9092'],
        plaintextRiskAcknowledged: true,
        type: 'KAFKA',
      }),
      targetId,
      administrator.id,
      `unit7-target-${targetId}`,
      JSON.stringify({
        bootstrapServers: ['127.0.0.1:9093'],
        plaintextRiskAcknowledged: true,
        type: 'KAFKA',
      }),
    ],
  );
  return { sourceId, targetId };
}

async function createPostgresDataSource(): Promise<string> {
  const dataSourceId = randomUUID();
  const secretId = randomUUID();
  const database = `unit7_source_${dataSourceId.replaceAll('-', '').slice(0, 12)}`;
  await requirePool().query(
    `INSERT INTO ape_dts_platform.secrets (
       id, owner_type, owner_id, purpose, algorithm, dek_version, aad_version,
       nonce, ciphertext, tag
     ) VALUES ($1::uuid, 'DATA_SOURCE', $2::uuid, 'CONNECTION_PASSWORD', 'AES_256_GCM', 1, 1,
               $3, $4, $5)`,
    [secretId, dataSourceId, Buffer.alloc(12, 1), Buffer.alloc(32, 2), Buffer.alloc(16, 3)],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.data_sources (
       id, normalized_name, source_type, configuration, connection_secret_id,
       state, created_by_user_id, updated_by_user_id
     ) VALUES ($1::uuid, $2, 'POSTGRES', $3::jsonb, $4::uuid, 'ACTIVE', $5::uuid, $5::uuid)`,
    [
      dataSourceId,
      `unit7-postgres-${dataSourceId}`,
      JSON.stringify({
        database,
        host: '127.0.0.1',
        port: 5432,
        tls: { mode: 'DISABLE', riskAcknowledged: true },
        type: 'POSTGRES',
        username: 'unit7',
      }),
      secretId,
      administrator.id,
    ],
  );
  return dataSourceId;
}

async function attachCertifiedKafkaArtifact(taskRevisionId: string): Promise<string> {
  const storageProfileId = randomUUID();
  const storedObjectId = randomUUID();
  const artifactId = randomUUID();
  const probeRunId = randomUUID();
  const now = new Date();
  const sha256 = Buffer.alloc(32, 0xaa);
  await requirePool().query(
    `INSERT INTO ape_dts_platform.storage_profiles (
       id, profile_key, display_name, version, driver, config, state,
       created_by_user_id
     ) VALUES ($1::uuid, $2, 'Unit 7 preflight profile', 1, 'LOCAL',
               '{"relative_prefix":""}'::jsonb, 'DRAFT', $3::uuid)`,
    [storageProfileId, `unit7-preflight-${artifactId}`, administrator.id],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.stored_objects (
       id, storage_profile_id, kind, object_key, size_bytes, sha256,
       content_type, state, created_by_actor_type, created_by_user_id, available_at
     ) VALUES ($1::uuid, $2::uuid, 'ARTIFACT', $3, 1, $4,
               'application/octet-stream', 'AVAILABLE', 'USER', $5::uuid, $6::timestamptz)`,
    [storedObjectId, storageProfileId, `artifacts/${artifactId}`, sha256, administrator.id, now],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifacts (
       id, version_identifier, declared_source, stored_object_id,
       declared_sha256, actual_sha256, size_bytes, state, activated_at,
       created_by_user_id
     ) VALUES ($1::uuid, $2, 'unit-test', $3::uuid, $4, $4, 1,
               'ACTIVE', $5::timestamptz, $6::uuid)`,
    [artifactId, `unit7-artifact-${artifactId}`, storedObjectId, sha256, now, administrator.id],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifact_probe_runs (
       id, artifact_id, probe_kind, probe_version, status,
       started_by_user_id, ended_at, exit_code
     ) VALUES ($1::uuid, $2::uuid, 'BEHAVIOR', 'unit7', 'PASSED',
               $3::uuid, $4::timestamptz, 0)`,
    [probeRunId, artifactId, administrator.id, now],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifact_certifications (
       id, artifact_id, platform_version, task_scenario,
       adapter_id, adapter_version, state, evidence_probe_run_id
     ) VALUES ($1::uuid, $2::uuid, '0.0.0', 'KAFKA_RAW_FORWARD',
               'unit7-adapter', '1', 'PASSED', $3::uuid)`,
    [randomUUID(), artifactId, probeRunId],
  );
  await requirePool().query(
    `UPDATE ape_dts_platform.task_revisions
        SET artifact_id = $2::uuid
      WHERE id = $1::uuid`,
    [taskRevisionId, artifactId],
  );
  return artifactId;
}

function pgConfiguration(sourceDataSourceId: string, targetDataSourceId: string) {
  return {
    kind: 'PG_TO_KAFKA' as const,
    schemaVersion: 1 as const,
    source: {
      dataSourceId: sourceDataSourceId,
      includeSnapshot: false,
      publicationName: 'unit7_publication',
      resourceMode: 'PLATFORM_MANAGED' as const,
      slotName: 'unit7_slot',
      table: { name: 'orders', schema: 'public' },
    },
    target: { dataSourceId: targetDataSourceId, topic: 'unit7.target' },
    tuning: {
      batchSize: 100,
      bufferSize: 16_000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all' as const,
    },
  };
}

async function mysqlTaskFixture() {
  const kafka = await createKafkaDataSources();
  const sourceId = await createPostgresDataSource();
  await requirePool().query(
    `UPDATE ape_dts_platform.data_sources SET source_type='MYSQL',
    configuration=configuration || '{"type":"MYSQL","port":3306,"database":"data_demo"}'::jsonb WHERE id=$1::uuid`,
    [sourceId],
  );
  const configuration = {
    kind: 'MYSQL_TO_KAFKA' as const,
    schemaVersion: 2 as const,
    source: { dataSourceId: sourceId, includeSnapshot: true },
    target: {
      dataSourceId: kafka.targetId,
      messageFormat: 'json' as const,
      jsonTemplate: 'cloudcanal' as const,
    },
    mappings: [
      {
        source: { schema: 'data_demo', name: 'orders' },
        targets: [{ topic: 'orders' }, { topic: 'archive' }],
      },
    ],
    tuning: {
      batchSize: 100,
      bufferSize: 1000,
      checkpointIntervalSeconds: 5,
      requiredAcks: 'all' as const,
    },
  };
  const created = await requireTasks().create(administrator, {
    configuration,
    name: `MySQL mappings ${randomUUID()}`,
    sourceDataSourceId: sourceId,
    targetDataSourceId: kafka.targetId,
    taskType: 'MYSQL_KAFKA',
  });
  return { created, configuration, kafka, sourceId };
}

async function mysqlRunningTaskFixture() {
  const fixture = await mysqlTaskFixture();
  const { created, sourceId } = fixture;
  const revisionId = created.draft!.id;
  const runId = randomUUID();
  const oldRunId = randomUUID();
  const sourceIdentity = `${sourceId}:data_demo`;
  await requirePool().query(
    `UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid`,
    [revisionId],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.preflight_runs (id,task_revision_id,state,result,completed_at)
    VALUES ($1::uuid,$2::uuid,'PASSED','{"dataSourceVersions":{"source":"1","target":"1"}}'::jsonb,statement_timestamp())`,
    [randomUUID(), revisionId],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.task_runs (id,task_id,task_revision_id,status,current_stage,ended_at)
    VALUES ($1::uuid,$3::uuid,$4::uuid,'STOPPED','CDC',statement_timestamp()),($2::uuid,$3::uuid,$4::uuid,'RUNNING','CDC',NULL)`,
    [oldRunId, runId, created.id, revisionId],
  );
  await requirePool().query(
    `UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid,current_draft_revision_id=NULL,
    desired_state='RUNNING',observed_state='RUNNING',row_version=row_version+1 WHERE id=$1::uuid`,
    [created.id, revisionId],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.mysql_task_checkpoints
    (task_id,task_revision_id,active_run_id,configuration_sha256,source_identity,source_row_version,client_server_id,phase,binlog_filename,binlog_position,checkpoint_at)
    VALUES ($1::uuid,$2::uuid,$3::uuid,$4,$5,1,251,'CDC','mysql-bin.000003',12345,statement_timestamp())`,
    [created.id, revisionId, runId, 'a'.repeat(64), sourceIdentity],
  );
  return { ...fixture, revisionId, runId, oldRunId, sourceIdentity };
}

function mysqlCommittedLog(binlogPosition: number): string {
  return `checkpoint_position | ${JSON.stringify({ type: 'MysqlCdc', server_id: '251', binlog_filename: 'mysql-bin.000003', next_event_position: binlogPosition })}\n`;
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
      administrator.id,
      administrator.username,
      administrator.displayName,
      JSON.stringify({ memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 }),
      Buffer.alloc(16, 7),
      Buffer.alloc(32, 8),
    ],
  );
  const audit = new AuditLedgerService(platformDatabase);
  tasks = new TasksService(platformDatabase, audit);
  resources = new PgResourceManager(platformDatabase, audit);
  capacity = new CapacityReservationService(platformDatabase);
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
    process.stderr.write(`failed to clean up Unit 7 PostgreSQL resources: ${String(error)}\n`);
  }
});

describe('task drafts and revision invariants against PostgreSQL', () => {
  it('creates shared task groups with normalized unique names and atomic initial assignment', async () => {
    const service = requireTasks();
    const name = `Group-${randomUUID()}`;
    const group = await service.createGroup(administrator, { name: `  ${name}  ` });
    expect(group).toEqual({ id: expect.any(String), name });
    await expect(
      service.createGroup(administrator, { name: name.toUpperCase() }),
    ).rejects.toMatchObject({ code: 'TASK_GROUP_NAME_ALREADY_EXISTS' });
    const unicode = `Ｆｏｌｄｅｒ-${randomUUID()}`;
    await service.createGroup(administrator, { name: unicode });
    await expect(
      service.createGroup(administrator, { name: unicode.normalize('NFKC') }),
    ).rejects.toMatchObject({ code: 'TASK_GROUP_NAME_ALREADY_EXISTS' });
    expect(await service.listGroups()).toContainEqual(group);
    const { sourceId, targetId } = await createKafkaDataSources();
    const request = {
      configuration: kafkaConfiguration(sourceId, targetId),
      name: `Grouped ${randomUUID()}`,
      sourceDataSourceId: sourceId,
      targetDataSourceId: targetId,
      taskType: 'KAFKA_KAFKA' as const,
      groupId: group.id,
    };
    const created = await service.create(administrator, request);
    expect(created.groupId).toBe(group.id);
    const invalidName = `Invalid group ${randomUUID()}`;
    await expect(
      service.create(administrator, { ...request, name: invalidName, groupId: randomUUID() }),
    ).rejects.toMatchObject({ code: 'TASK_GROUP_NOT_FOUND' });
    expect((await service.list(administrator)).some((task) => task.name === invalidName)).toBe(
      false,
    );
  });

  it('moves tasks between shared groups without mutating runtime, drafts or row versions, and rejects stale moves', async () => {
    const service = requireTasks();
    const first = await service.createGroup(administrator, { name: `First ${randomUUID()}` });
    const second = await service.createGroup(administrator, { name: `Second ${randomUUID()}` });
    const { sourceId, targetId } = await createKafkaDataSources();
    const task = await service.create(administrator, {
      configuration: kafkaConfiguration(sourceId, targetId),
      name: `Moving ${randomUUID()}`,
      sourceDataSourceId: sourceId,
      targetDataSourceId: targetId,
      taskType: 'KAFKA_KAFKA',
    });
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET desired_state='RUNNING', observed_state='RUNNING' WHERE id=$1::uuid`,
      [task.id],
    );
    const before = await service.get(administrator, task.id);
    const moved = await service.assignGroup(administrator, task.id, {
      groupId: first.id,
      expectedGroupId: null,
    });
    expect(moved).toEqual({ ...before, groupId: first.id });
    const competing = await Promise.allSettled([
      service.assignGroup(administrator, task.id, {
        groupId: second.id,
        expectedGroupId: first.id,
      }),
      service.assignGroup(administrator, task.id, { groupId: null, expectedGroupId: first.id }),
    ]);
    expect(competing.filter((item) => item.status === 'fulfilled')).toHaveLength(1);
    expect(competing.find((item) => item.status === 'rejected')).toMatchObject({
      reason: { code: 'TASK_GROUP_ASSIGNMENT_CONFLICT' },
    });
    const current = await service.get(administrator, task.id);
    const ungrouped = await service.assignGroup(administrator, task.id, {
      groupId: null,
      expectedGroupId: current.groupId ?? null,
    });
    expect(ungrouped).toEqual({ ...before, groupId: null });
    const audits = await requirePool().query(
      `SELECT canonical_details FROM ape_dts_platform.audit_events WHERE subject_id=$1::uuid AND action='TASK_GROUP_ASSIGNED' ORDER BY sequence`,
      [task.id],
    );
    expect(audits.rows.length).toBeGreaterThanOrEqual(2);
    expect(audits.rows[0]?.canonical_details).toEqual({ previousGroupId: null, groupId: first.id });
  });

  it('does not grant task access through shared groups and refuses deleted task assignment', async () => {
    const service = requireTasks();
    const group = await service.createGroup(administrator, { name: `Shared ${randomUUID()}` });
    const { sourceId, targetId } = await createKafkaDataSources();
    const task = await service.create(administrator, {
      configuration: kafkaConfiguration(sourceId, targetId),
      groupId: group.id,
      name: `Private ${randomUUID()}`,
      sourceDataSourceId: sourceId,
      targetDataSourceId: targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const developer: UserSummary = {
      ...administrator,
      id: randomUUID(),
      role: 'DEVELOPER',
      username: `dev-${randomUUID()}`,
    };
    await requirePool().query(
      `INSERT INTO ape_dts_platform.users (id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash) SELECT $1::uuid,$2,'Group developer','DEVELOPER','ACTIVE',password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash FROM ape_dts_platform.users WHERE id=$3::uuid`,
      [developer.id, developer.username, administrator.id],
    );
    expect(
      await service.createGroup(developer, { name: `Developer ${randomUUID()}` }),
    ).toMatchObject({ id: expect.any(String) });
    expect(await service.listGroups()).toContainEqual(group);
    expect(await service.list(developer)).not.toContainEqual(
      expect.objectContaining({ id: task.id }),
    );
    await expect(
      service.assignGroup(developer, task.id, { groupId: null, expectedGroupId: group.id }),
    ).rejects.toMatchObject({ code: 'TASK_ACCESS_DENIED' });
    const deniedName = `Denied ${randomUUID()}`;
    await expect(
      service.create(developer, {
        configuration: kafkaConfiguration(sourceId, targetId),
        groupId: group.id,
        name: deniedName,
        sourceDataSourceId: sourceId,
        targetDataSourceId: targetId,
        taskType: 'KAFKA_KAFKA',
      }),
    ).rejects.toMatchObject({ code: 'TASK_ACCESS_DENIED' });
    expect((await service.list(administrator)).some((item) => item.name === deniedName)).toBe(
      false,
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.data_source_grants (id,data_source_id,user_id,granted_by_user_id)
      VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid),($5::uuid,$6::uuid,$3::uuid,$4::uuid)`,
      [randomUUID(), sourceId, developer.id, administrator.id, randomUUID(), targetId],
    );
    expect(await service.list(developer)).toContainEqual(
      expect.objectContaining({ id: task.id, groupId: group.id }),
    );
    const moved = await service.assignGroup(developer, task.id, {
      groupId: null,
      expectedGroupId: group.id,
    });
    expect(moved).toMatchObject({ groupId: null, rowVersion: task.rowVersion });
    await service.assignGroup(developer, task.id, { groupId: group.id, expectedGroupId: null });
    await expect(
      service.assignGroup(developer, task.id, { groupId: randomUUID(), expectedGroupId: group.id }),
    ).rejects.toMatchObject({ code: 'TASK_GROUP_NOT_FOUND' });
    expect((await service.get(developer, task.id)).groupId).toBe(group.id);
    await service.delete(administrator, task.id, task.rowVersion);
    await expect(
      service.assignGroup(administrator, task.id, { groupId: null, expectedGroupId: group.id }),
    ).rejects.toMatchObject({ code: 'TASK_NOT_FOUND' });
    expect((await service.get(administrator, task.id)).groupId).toBe(group.id);
  });

  it('requires version-bound confirmation for PG added tables and preserves the applied configuration', async () => {
    const kafka = await createKafkaDataSources();
    const sourceId = await createPostgresDataSource();
    const configuration = pgConfiguration(sourceId, kafka.targetId);
    configuration.source.includeSnapshot = false;
    const created = await requireTasks().create(administrator, {
      name: `PG addition ${randomUUID()}`,
      taskType: 'PG_KAFKA',
      sourceDataSourceId: sourceId,
      targetDataSourceId: kafka.targetId,
      configuration,
    });
    const continuity = new ContinuityService(
      platformDatabase!,
      new AuditLedgerService(platformDatabase!),
    );
    const observation = {
      kind: 'POSTGRES' as const,
      source: {
        confirmedFlushLsn: '0/100',
        databaseName: 'data_demo',
        databaseOid: '123',
        systemIdentifier: '456',
        slotName: configuration.source.slotName,
        slotPlugin: 'pgoutput',
        slotActive: false as const,
      },
      target: { clusterId: 'cluster', partitionCount: 1, partitionIds: [], topicId: 'old-id' },
      topics: [
        {
          topic: configuration.target.topic,
          resourceType: 'KAFKA_TARGET_TOPIC' as const,
          clusterId: 'cluster',
          partitionCount: 1,
          partitionIds: [],
          topicId: 'old-id',
        },
      ],
    };
    const client = await requirePool().connect();
    try {
      await client.query('BEGIN');
      await continuity.decideWithin(
        client,
        { type: 'USER', userId: administrator.id },
        created.id,
        created.draft!.id,
        created.rowVersion,
        'STOPPED',
        observation,
      );
      await client.query('COMMIT');
    } finally {
      client.release();
    }
    const originalBaseline = (
      await requirePool().query(
        'SELECT * FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
        [created.id],
      )
    ).rows[0];
    await requirePool().query(
      "UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid",
      [created.draft!.id],
    );
    await requirePool().query(
      'UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid,current_draft_revision_id=NULL WHERE id=$1::uuid',
      [created.id, created.draft!.id],
    );
    const applied = await requireTasks().get(administrator, created.id);
    const { table, ...source } = configuration.source;
    const { topic, ...target } = configuration.target;
    const candidate = {
      ...configuration,
      schemaVersion: 2,
      source,
      target,
      mappings: [
        { source: table, targets: [{ topic }] },
        { source: { schema: 'public', name: 'added_table' }, targets: [{ topic: 'new.table' }] },
      ],
    };
    const request = {
      taskType: 'PG_KAFKA' as const,
      revisionId: applied.applied!.id,
      expectedTaskRowVersion: applied.rowVersion,
      expectedDraftVersion: applied.applied!.draftVersion,
      configuration: candidate,
    };
    await expect(
      requireTasks().saveDraft(administrator, created.id, request),
    ).rejects.toMatchObject({ code: 'TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED' });
    const saved = await requireTasks().saveDraft(administrator, created.id, {
      ...request,
      acknowledgePgTableAddition: true,
    });
    expect(saved.applied).toEqual(applied.applied);
    const audit = await requirePool().query(
      'SELECT canonical_details FROM ape_dts_platform.audit_events WHERE subject_id=$1::uuid AND action=$2 ORDER BY sequence DESC LIMIT 1',
      [created.id, 'TASK_DRAFT_SAVED'],
    );
    expect(audit.rows[0]?.canonical_details).toMatchObject({
      pgTableAddition: {
        appliedRevisionId: applied.applied!.id,
        revisionId: saved.draft!.id,
        draftVersion: saved.draft!.draftVersion,
      },
    });
    const addedTopic = { ...observation.topics[0]!, topic: 'new.table', topicId: 'new-id' };
    await requirePool().query(
      `INSERT INTO ape_dts_platform.preflight_runs(id,task_revision_id,state,result,completed_at)
       VALUES ($1::uuid,$2::uuid,'PASSED',$3::jsonb,statement_timestamp())`,
      [
        randomUUID(),
        saved.draft!.id,
        JSON.stringify({
          taskRevision: {
            draftVersion: saved.draft!.draftVersion,
            taskRowVersion: saved.rowVersion,
          },
          checks: [
            {
              key: 'TOPIC_IDENTITIES',
              state: 'PASSED',
              details: { topics: [...observation.topics, addedTopic] },
            },
          ],
        }),
      ],
    );
    const check = await requirePool().connect();
    try {
      await check.query('BEGIN');
      expect(await confirmedPgTableExpansion(check, saved.id, saved.draft!.id)).toBe(true);
      const recreated = await continuity.decideWithin(
        check,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        { ...observation, topics: [{ ...observation.topics[0]!, topicId: 'recreated-topic' }] },
      );
      expect(recreated).toMatchObject({
        state: 'FAILED',
        code: 'SYSTEM_KAFKA_TARGET_TOPIC_RECREATED',
      });
      await check.query('ROLLBACK');
      await check.query('BEGIN');
      const newTopicRecreated = await continuity.decideWithin(
        check,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        {
          ...observation,
          topics: [
            ...observation.topics,
            { ...addedTopic, topicId: 'new-topic-recreated-after-preflight' },
          ],
        },
      );
      expect(newTopicRecreated).toMatchObject({
        state: 'FAILED',
        code: 'SYSTEM_KAFKA_TARGET_TOPIC_RECREATED',
      });
      await check.query('ROLLBACK');
      await check.query('BEGIN');
      const partitionsChanged = await continuity.decideWithin(
        check,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        {
          ...observation,
          topics: [...observation.topics, { ...addedTopic, partitionCount: 2 }],
        },
      );
      expect(partitionsChanged).toMatchObject({
        state: 'FAILED',
        code: 'SYSTEM_KAFKA_TARGET_PARTITION_COUNT_CHANGED',
      });
      await check.query('ROLLBACK');
      await check.query('BEGIN');
      const regressed = await continuity.decideWithin(
        check,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        { ...observation, source: { ...observation.source, confirmedFlushLsn: '0/50' } },
      );
      expect(regressed).toMatchObject({
        state: 'FAILED',
        code: 'PG_CONFIRMED_FLUSH_LSN_REGRESSED',
      });
      await check.query('ROLLBACK');
      await check.query('BEGIN');
      const prospective = await continuity.decideWithin(
        check,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        {
          ...observation,
          source: { ...observation.source, confirmedFlushLsn: '0/200' },
          topics: [
            ...observation.topics,
            { ...observation.topics[0]!, topic: 'new.table', topicId: 'new-id' },
          ],
        },
      );
      expect(prospective).toMatchObject({ state: 'PASSED', code: 'PG_TABLE_ADDITION_READY' });
      await check.query('COMMIT');
      expect(
        (
          await requirePool().query(
            'SELECT * FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
            [created.id],
          )
        ).rows[0],
      ).toEqual(originalBaseline);
      await check.query('BEGIN');
      await applyPgTableExpansionContinuity(check, saved.id, saved.draft!.id);
      await check.query('ROLLBACK');
      expect(
        (
          await requirePool().query(
            'SELECT * FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
            [created.id],
          )
        ).rows[0],
      ).toEqual(originalBaseline);
      await check.query('BEGIN');
      await applyPgTableExpansionContinuity(check, saved.id, saved.draft!.id);
      await check.query('COMMIT');
      expect(
        (
          await requirePool().query(
            'SELECT task_revision_id,pg_confirmed_flush_lsn::text,topic_snapshots FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
            [created.id],
          )
        ).rows[0],
      ).toMatchObject({
        task_revision_id: saved.draft!.id,
        pg_confirmed_flush_lsn: '0/200',
        topic_snapshots: expect.arrayContaining([
          expect.objectContaining({ topic: 'new.table', topicId: 'new-id' }),
        ]),
      });
    } finally {
      await check.query('ROLLBACK');
      check.release();
    }
    await expect(
      requireTasks().saveDraft(administrator, created.id, {
        ...request,
        revisionId: saved.draft!.id,
        expectedDraftVersion: saved.draft!.draftVersion,
        expectedTaskRowVersion: saved.rowVersion,
      }),
    ).rejects.toMatchObject({ code: 'TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED' });
    // A draft written by an older platform must not borrow the previous version's consent.
    await requirePool().query(
      'UPDATE ape_dts_platform.task_revisions SET draft_version=draft_version+1 WHERE id=$1::uuid',
      [saved.draft!.id],
    );
    const stale = await requirePool().connect();
    try {
      await stale.query('BEGIN');
      expect(await confirmedPgTableExpansion(stale, saved.id, saved.draft!.id)).toBe(false);
      const rejected = await continuity.decideWithin(
        stale,
        { type: 'SYSTEM' },
        saved.id,
        saved.draft!.id,
        saved.rowVersion,
        'STOPPED',
        observation,
      );
      expect(rejected).toMatchObject({
        state: 'FAILED',
        code: 'PG_TABLE_ADDITION_CONFIRMATION_REQUIRED',
      });
      await stale.query('ROLLBACK');
    } finally {
      stale.release();
    }
  });

  it.each(['PG_KAFKA', 'KAFKA_KAFKA'] as const)(
    'derives an editable %s draft from its applied configuration without changing the applied revision',
    async (taskType) => {
      const kafka = await createKafkaDataSources();
      const sourceId = taskType === 'PG_KAFKA' ? await createPostgresDataSource() : kafka.sourceId;
      const configuration =
        taskType === 'PG_KAFKA'
          ? pgConfiguration(sourceId, kafka.targetId)
          : kafkaConfiguration(sourceId, kafka.targetId);
      const created = await requireTasks().create(administrator, {
        name: `applied edit ${taskType} ${randomUUID()}`,
        taskType,
        sourceDataSourceId: sourceId,
        targetDataSourceId: kafka.targetId,
        configuration,
      });
      await requirePool().query(
        "UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid AND task_id=$2::uuid",
        [created.draft!.id, created.id],
      );
      await requirePool().query(
        "UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid,current_draft_revision_id=NULL,desired_state='RUNNING',observed_state='RUNNING',row_version=row_version+1 WHERE id=$1::uuid",
        [created.id, created.draft!.id],
      );
      const running = await requireTasks().get(administrator, created.id);
      const change = { ...configuration, tuning: { ...configuration.tuning, batchSize: 300 } };
      const request = {
        taskType,
        configuration: change,
        revisionId: running.applied!.id,
        expectedTaskRowVersion: running.rowVersion,
        expectedDraftVersion: running.applied!.draftVersion,
      };
      await expect(
        requireTasks().saveDraft(administrator, created.id, request),
      ).rejects.toMatchObject({ code: 'TASK_NOT_STOPPED' });
      await requirePool().query(
        "UPDATE ape_dts_platform.tasks SET desired_state='STOPPED',observed_state='STOPPED',row_version=row_version+1 WHERE id=$1::uuid",
        [created.id],
      );
      const stopped = await requireTasks().get(administrator, created.id);
      const outcomes = await Promise.allSettled(
        [1, 2].map(() =>
          requireTasks().saveDraft(administrator, created.id, {
            ...request,
            expectedTaskRowVersion: stopped.rowVersion,
          }),
        ),
      );
      expect(outcomes.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
      expect(outcomes.filter((result) => result.status === 'rejected')).toHaveLength(1);
      const reopened = await requireTasks().get(administrator, created.id);
      expect(reopened.applied).toEqual(stopped.applied);
      expect(reopened.draft?.state).toBe('READY_DRAFT');
      expect(reopened.draft?.id).not.toBe(stopped.applied!.id);
      expect(reopened.draft?.configuration.tuning).toMatchObject({ batchSize: 300 });
      expect(reopened.draft?.configuration.source).toEqual(stopped.applied!.configuration.source);
      expect(reopened.draft?.configuration.target).toEqual(stopped.applied!.configuration.target);
      await expect(
        requireTasks().saveDraft(administrator, created.id, {
          ...request,
          expectedTaskRowVersion: reopened.rowVersion,
        }),
      ).rejects.toMatchObject({ code: 'TASK_REVISION_IMMUTABLE' });
    },
  );

  it('creates and saves MySQL mapped drafts while rejecting cross-database bindings', async () => {
    const { created, configuration, kafka } = await mysqlTaskFixture();
    expect(created.draft?.state).toBe('READY_DRAFT');
    expect(
      (
        await requirePool().query(
          'SELECT source_database, mappings FROM ape_dts_platform.mysql_kafka_revision_configs WHERE task_revision_id=$1::uuid',
          [created.draft!.id],
        )
      ).rows[0],
    ).toEqual({ source_database: 'data_demo', mappings: configuration.mappings });
    const changed = {
      ...configuration,
      source: { ...configuration.source, includeSnapshot: false },
    };
    const saved = await requireTasks().saveDraft(administrator, created.id, {
      configuration: changed,
      taskType: 'MYSQL_KAFKA',
      expectedTaskRowVersion: created.rowVersion,
      expectedDraftVersion: created.draft!.draftVersion,
      revisionId: created.draft!.id,
    });
    expect(saved.draft?.state).toBe('READY_DRAFT');
    expect(
      (
        await requirePool().query(
          'SELECT include_snapshot FROM ape_dts_platform.mysql_kafka_revision_configs WHERE task_revision_id=$1::uuid',
          [created.draft!.id],
        )
      ).rows[0]?.include_snapshot,
    ).toBe(false);
    await expect(
      requireTasks().saveDraft(administrator, created.id, {
        configuration: {
          ...changed,
          mappings: [{ ...configuration.mappings[0], source: { schema: 'other', name: 'orders' } }],
        },
        taskType: 'MYSQL_KAFKA',
        expectedTaskRowVersion: saved.rowVersion,
        expectedDraftVersion: saved.draft!.draftVersion,
        revisionId: saved.draft!.id,
      }),
    ).rejects.toMatchObject({ code: 'TASK_CONFIGURATION_DATA_SOURCE_MISMATCH' });
    expect((await requireTasks().get(administrator, created.id)).draft?.configuration).toEqual(
      changed,
    );
    const continuity = new ContinuityService(
      requirePlatformDatabase(),
      new AuditLedgerService(requirePlatformDatabase()),
    );
    const client = await requirePool().connect();
    const observation = {
      kind: 'MYSQL' as const,
      sourceIdentity: 'mysql-uuid:data_demo',
      target: { clusterId: 'target-cluster', partitionCount: 1, partitionIds: [], topicId: null },
    };
    try {
      await client.query('BEGIN');
      expect(
        await continuity.decideWithin(
          client,
          { type: 'USER', userId: administrator.id },
          created.id,
          created.draft!.id,
          saved.rowVersion,
          'STOPPED',
          observation,
        ),
      ).toMatchObject({ state: 'PASSED' });
      await client.query('COMMIT');
      await client.query('BEGIN');
      expect(
        await continuity.reinitializeMysqlWithin(
          client,
          administrator.id,
          created.id,
          created.draft!.id,
          { ...observation, sourceIdentity: 'replacement-uuid:data_demo' },
        ),
      ).toMatchObject({ state: 'PASSED' });
      await client.query('COMMIT');
      expect(
        (
          await client.query(
            'SELECT mysql_source_identity FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
            [created.id],
          )
        ).rows[0]?.mysql_source_identity,
      ).toBe('replacement-uuid:data_demo');
      const audit = await client.query(
        `SELECT canonical_details AS details FROM ape_dts_platform.audit_events WHERE subject_id=$1 AND action='TASK_MYSQL_SNAPSHOT_REINITIALIZATION_PREFLIGHT'`,
        [created.id],
      );
      expect(audit.rows[0]?.details.previousBaseline.mysql_source_identity).toBe(
        'mysql-uuid:data_demo',
      );
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid`,
      [saved.draft!.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid,current_draft_revision_id=NULL,
      desired_state='RUNNING',observed_state='RUNNING',row_version=row_version+1 WHERE id=$1::uuid`,
      [created.id, saved.draft!.id],
    );
    const applied = await requireTasks().get(administrator, created.id);
    const forkRequest = {
      configuration: changed,
      taskType: 'MYSQL_KAFKA' as const,
      expectedTaskRowVersion: applied.rowVersion,
      expectedDraftVersion: applied.applied!.draftVersion,
      revisionId: applied.applied!.id,
    };
    await expect(
      requireTasks().saveDraft(administrator, created.id, forkRequest),
    ).rejects.toMatchObject({ code: 'TASK_NOT_STOPPED' });
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED',observed_state='STOPPED',row_version=row_version+1 WHERE id=$1::uuid`,
      [created.id],
    );
    const stopped = await requireTasks().get(administrator, created.id);
    const activeRunId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (id,task_id,task_revision_id,status,current_stage)
      VALUES ($1::uuid,$2::uuid,$3::uuid,'RUNNING','CDC')`,
      [activeRunId, created.id, stopped.applied!.id],
    );
    await expect(
      requireTasks().saveDraft(administrator, created.id, {
        ...forkRequest,
        expectedTaskRowVersion: stopped.rowVersion,
      }),
    ).rejects.toMatchObject({ code: 'TASK_ACTIVE_RUN_UNITS' });
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs SET status='STOPPED',ended_at=statement_timestamp() WHERE id=$1::uuid`,
      [activeRunId],
    );
    const concurrent = await Promise.allSettled(
      [1, 2].map(
        async () =>
          await requireTasks().saveDraft(administrator, created.id, {
            ...forkRequest,
            expectedTaskRowVersion: stopped.rowVersion,
          }),
      ),
    );
    expect(concurrent.filter((item) => item.status === 'fulfilled')).toHaveLength(1);
    expect(concurrent.filter((item) => item.status === 'rejected')).toHaveLength(1);
    const reopened = await requireTasks().get(administrator, created.id);
    expect(reopened.draft?.state).toBe('READY_DRAFT');
    expect(reopened.draft?.id).not.toBe(applied.applied?.id);
    expect(reopened.applied).toEqual(stopped.applied);
    expect(reopened.draft?.configuration).toEqual(changed);
    await expect(
      requireTasks().saveDraft(administrator, created.id, {
        ...forkRequest,
        expectedTaskRowVersion: stopped.rowVersion,
      }),
    ).rejects.toMatchObject({ code: 'TASK_REVISION_IMMUTABLE' });
    await expect(
      requireTasks().create(administrator, {
        configuration: {
          ...configuration,
          source: { ...configuration.source, dataSourceId: kafka.sourceId },
        },
        name: `MySQL wrong source ${randomUUID()}`,
        sourceDataSourceId: kafka.sourceId,
        targetDataSourceId: kafka.targetId,
        taskType: 'MYSQL_KAFKA',
      }),
    ).rejects.toMatchObject({ code: 'TASK_DATA_SOURCE_TYPE_INVALID' });
  });

  it('reads MySQL progress only from the current applied revision, running unit and source configuration', async () => {
    const { created, runId, oldRunId } = await mysqlRunningTaskFixture();
    const progress = new TaskProgressService(requirePlatformDatabase(), {} as DataSourcesService);
    await progress.collect();
    const sample = (await progress.listForMonitoring()).find((item) => item.taskId === created.id);
    expect(sample).toMatchObject({
      status: 'AVAILABLE',
      sourceType: 'MYSQL',
      mysqlBinlogFilename: 'mysql-bin.000003',
      mysqlBinlogPosition: '12345',
      offsetLag: null,
      replicationLagMilliseconds: null,
      timeDelayMilliseconds: null,
    });
    expect(sample?.mysqlCheckpointAt).toMatch(/^\d{4}-/u);
    await requirePool().query(
      'UPDATE ape_dts_platform.mysql_task_checkpoints SET active_run_id=$2::uuid WHERE task_id=$1::uuid',
      [created.id, oldRunId],
    );
    await progress.collect();
    expect(
      (await progress.listForMonitoring()).find((item) => item.taskId === created.id),
    ).toMatchObject({
      status: 'UNAVAILABLE',
      mysqlBinlogFilename: null,
      mysqlBinlogPosition: null,
    });
    await requirePool().query(
      'UPDATE ape_dts_platform.mysql_task_checkpoints SET active_run_id=$2::uuid,source_row_version=2 WHERE task_id=$1::uuid',
      [created.id, runId],
    );
    await progress.collect();
    expect(
      (await progress.listForMonitoring()).find((item) => item.taskId === created.id),
    ).toMatchObject({
      status: 'UNAVAILABLE',
      mysqlBinlogFilename: null,
      mysqlBinlogPosition: null,
    });
    progress.onModuleDestroy();
  });

  it('keeps monitoring WAL from a stopped PostgreSQL run and rejects changed source provenance', async () => {
    const kafka = await createKafkaDataSources();
    const sourceId = await createPostgresDataSource();
    const created = await requireTasks().create(administrator, {
      configuration: pgConfiguration(sourceId, kafka.targetId),
      name: `Stopped PG WAL ${randomUUID()}`,
      taskType: 'PG_KAFKA',
      sourceDataSourceId: sourceId,
      targetDataSourceId: kafka.targetId,
    });
    const revisionId = created.draft!.id;
    await requirePool().query(
      "UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid",
      [revisionId],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.preflight_runs(id,task_revision_id,state,result,completed_at)
       VALUES($1,$2,'PASSED','{"dataSourceVersions":{"source":"1","target":"1"}}',statement_timestamp())`,
      [randomUUID(), revisionId],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs(id,task_id,task_revision_id,status,current_stage,ended_at)
       VALUES($1,$2,$3,'STOPPED','SHUTDOWN',statement_timestamp())`,
      [randomUUID(), created.id, revisionId],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2,current_draft_revision_id=NULL
       WHERE id=$1`,
      [created.id, revisionId],
    );
    // A later preflight is not evidence of the source used by the stopped run.
    await requirePool().query(
      `INSERT INTO ape_dts_platform.preflight_runs(id,task_revision_id,state,result,completed_at)
       VALUES($1,$2,'PASSED','{"dataSourceVersions":{"source":"999"}}',statement_timestamp())`,
      [randomUUID(), revisionId],
    );
    const reads: unknown[][] = [];
    const progress = new TaskProgressService(requirePlatformDatabase(), {
      readProgressForSystem: async (...args: unknown[]) => {
        reads.push(args);
        return {
          slotStatus: 'INACTIVE',
          unconfirmedWalBytes: '200',
          retainedWalBytes: '300',
          offsetLag: null,
          replicationLagMilliseconds: null,
        };
      },
    } as unknown as DataSourcesService);
    await progress.collect();
    expect(reads).toContainEqual([sourceId, { slotName: 'unit7_slot' }, '1']);
    expect(
      (await progress.listForMonitoring()).find((item) => item.taskId === created.id),
    ).toMatchObject({
      status: 'AVAILABLE',
      sourceType: 'POSTGRES',
      slotStatus: 'INACTIVE',
      retainedWalBytes: '300',
    });
    expect((await progress.list(administrator)).some((item) => item.taskId === created.id)).toBe(
      false,
    );
    await requirePool().query(
      'UPDATE ape_dts_platform.data_sources SET row_version=row_version+1 WHERE id=$1',
      [sourceId],
    );
    expect(
      (await progress.listForMonitoring()).find((item) => item.taskId === created.id),
    ).toMatchObject({ status: 'UNAVAILABLE' });
    progress.onModuleDestroy();
  });

  it('persists committed MySQL checkpoints and allows a final flush after shutting down the polling timer', async () => {
    const { created, revisionId, runId, sourceIdentity } = await mysqlRunningTaskFixture();
    const directory = await mkdtemp(path.join(tmpdir(), 'scs-mysql-checkpoint-'));
    const store = new MysqlRuntimeStateStore(
      requirePlatformDatabase(),
      {} as DataSourcesService,
      new AuditLedgerService(requirePlatformDatabase()),
    );
    const checkpoint = async () =>
      (
        await requirePool().query(
          `SELECT task_revision_id,active_run_id,binlog_filename,binlog_position::text,checkpoint_at
      FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1::uuid`,
          [created.id],
        )
      ).rows[0];
    try {
      await writeFile(
        path.join(directory, 'mysql-checkpoint.log'),
        mysqlCommittedLog(12360),
        'utf8',
      );
      store.watch({
        taskId: created.id,
        revisionId,
        runId,
        directory,
        position: {
          phase: 'CDC',
          sourceIdentity,
          sourceRowVersion: '1',
          serverId: 251,
          binlogFilename: 'mysql-bin.000003',
          binlogPosition: 12345,
        },
      });
      await store.flush(runId);
      expect(await checkpoint()).toMatchObject({
        task_revision_id: revisionId,
        active_run_id: runId,
        binlog_filename: 'mysql-bin.000003',
        binlog_position: '12360',
      });
      expect((await checkpoint()).checkpoint_at).toBeInstanceOf(Date);
      store.onModuleDestroy();
      await writeFile(
        path.join(directory, 'mysql-checkpoint.log'),
        mysqlCommittedLog(12380),
        'utf8',
      );
      await store.flush(runId);
      expect((await checkpoint()).binlog_position).toBe('12380');
      const history = await requirePool().query(
        `SELECT status FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid ORDER BY status`,
        [created.id],
      );
      expect(history.rows).toEqual([{ status: 'RUNNING' }, { status: 'STOPPED' }]);
    } finally {
      store.onModuleDestroy();
      store.unwatch(runId);
      await rm(directory, { recursive: true, force: true });
    }
  });

  it.each(['RUN_BINDING', 'REVISION_BINDING', 'TERMINAL_RUN'] as const)(
    'ignores a later MySQL checkpoint from an obsolete watch after %s changes',
    async (mode) => {
      const { created, revisionId, runId, sourceIdentity } = await mysqlRunningTaskFixture();
      const directory = await mkdtemp(path.join(tmpdir(), 'scs-mysql-stale-checkpoint-'));
      const store = new MysqlRuntimeStateStore(
        requirePlatformDatabase(),
        {} as DataSourcesService,
        new AuditLedgerService(requirePlatformDatabase()),
      );
      const checkpoint = async () =>
        (
          await requirePool().query(
            `SELECT task_revision_id,active_run_id,binlog_position::text FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1::uuid`,
            [created.id],
          )
        ).rows[0];
      try {
        await writeFile(
          path.join(directory, 'mysql-checkpoint.log'),
          mysqlCommittedLog(12360),
          'utf8',
        );
        store.watch({
          taskId: created.id,
          revisionId,
          runId,
          directory,
          position: {
            phase: 'CDC',
            sourceIdentity,
            sourceRowVersion: '1',
            serverId: 251,
            binlogFilename: 'mysql-bin.000003',
            binlogPosition: 12345,
          },
        });
        await store.flush(runId);
        store.onModuleDestroy();
        if (mode === 'TERMINAL_RUN') {
          await requirePool().query(
            `UPDATE ape_dts_platform.task_runs SET status='STOPPED',ended_at=statement_timestamp() WHERE id=$1::uuid`,
            [runId],
          );
        } else {
          const nextRevisionId = mode === 'REVISION_BINDING' ? randomUUID() : revisionId;
          if (mode === 'REVISION_BINDING')
            await requirePool().query(
              `INSERT INTO ape_dts_platform.task_revisions
            (id,task_id,revision_number,state,configuration_schema_version,configuration,artifact_id,created_by_user_id,updated_by_user_id)
            SELECT $1::uuid,task_id,revision_number+1,'READY_DRAFT',configuration_schema_version,configuration,artifact_id,created_by_user_id,updated_by_user_id
            FROM ape_dts_platform.task_revisions WHERE id=$2::uuid`,
              [nextRevisionId, revisionId],
            );
          const nextRunId = randomUUID();
          await requirePool().query(
            `INSERT INTO ape_dts_platform.task_runs (id,task_id,task_revision_id,status,current_stage)
          VALUES ($1::uuid,$2::uuid,$3::uuid,'STARTING','CDC')`,
            [nextRunId, created.id, nextRevisionId],
          );
          await requirePool().query(
            `UPDATE ape_dts_platform.mysql_task_checkpoints SET active_run_id=$2::uuid,task_revision_id=$3::uuid,binlog_position=20000 WHERE task_id=$1::uuid`,
            [created.id, nextRunId, nextRevisionId],
          );
        }
        const before = await checkpoint();
        const historyBefore = await requirePool().query(
          `SELECT id,status,task_revision_id FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid ORDER BY id`,
          [created.id],
        );
        await writeFile(
          path.join(directory, 'mysql-checkpoint.log'),
          mysqlCommittedLog(30000),
          'utf8',
        );
        await store.flush(runId);
        expect(await checkpoint()).toEqual(before);
        expect(
          (
            await requirePool().query(
              `SELECT id,status,task_revision_id FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid ORDER BY id`,
              [created.id],
            )
          ).rows,
        ).toEqual(historyBefore.rows);
      } finally {
        store.onModuleDestroy();
        store.unwatch(runId);
        await rm(directory, { recursive: true, force: true });
      }
    },
  );

  it('persists every version-2 mapping without pretending that its first source or target is the whole task', async () => {
    const kafka = await createKafkaDataSources();
    const pgId = await createPostgresDataSource();
    const pgLegacy = pgConfiguration(pgId, kafka.targetId);
    const { table: pgTable, ...pgSource } = pgLegacy.source;
    const configurations = [
      {
        ...pgLegacy,
        schemaVersion: 2,
        source: pgSource,
        target: { dataSourceId: kafka.targetId },
        mappings: [
          {
            source: pgTable,
            targets: [{ topic: 'orders' }, { topic: 'archive' }],
          },
          {
            source: { schema: 'sales', name: 'items' },
            targets: [{ topic: 'items' }, { topic: 'archive' }],
          },
        ],
      },
      {
        ...kafkaConfiguration(kafka.sourceId, kafka.targetId),
        schemaVersion: 2,
        source: { dataSourceId: kafka.sourceId },
        target: { dataSourceId: kafka.targetId },
        mappings: [
          { source: 'source.one', targets: [{ topic: 'target.one' }, { topic: 'archive' }] },
          { source: 'source.two', targets: [{ topic: 'target.two' }, { topic: 'archive' }] },
        ],
      },
    ];
    for (const configuration of configurations) {
      const pg = configuration.kind === 'PG_TO_KAFKA';
      const created = await requireTasks().create(administrator, {
        configuration,
        name: `Version 2 complete mappings ${configuration.kind}`,
        sourceDataSourceId: pg ? pgId : kafka.sourceId,
        targetDataSourceId: kafka.targetId,
        taskType: pg ? 'PG_KAFKA' : 'KAFKA_KAFKA',
      });
      expect(created.draft?.state).toBe('READY_DRAFT');
      expect(created.draft?.configuration.mappings).toEqual(configuration.mappings);
      const table = pg ? 'pg_kafka_revision_configs' : 'kafka_kafka_revision_configs';
      const columns = pg
        ? 'source_schema, source_table, target_topic'
        : 'source_topic, target_topic';
      const persisted = await requirePool().query<Record<string, unknown>>(
        `SELECT ${columns}, mappings FROM ape_dts_platform.${table} WHERE task_revision_id = $1::uuid`,
        [created.draft!.id],
      );
      expect(persisted.rows[0]?.mappings).toEqual(configuration.mappings);
      for (const column of columns.split(', ')) expect(persisted.rows[0]?.[column]).toBeNull();
      if (!pg) {
        const source = created.draft?.configuration.source as { groupId: string };
        expect(source.groupId).toBe(`ape-dts-task-${created.id}`);
      }
      const revised = { ...configuration, mappings: [configuration.mappings[1]!] };
      const saved = await requireTasks().saveDraft(administrator, created.id, {
        configuration: revised,
        expectedDraftVersion: created.draft!.draftVersion,
        expectedTaskRowVersion: created.rowVersion,
        revisionId: created.draft!.id,
        taskType: pg ? 'PG_KAFKA' : 'KAFKA_KAFKA',
      });
      expect(saved.draft?.state).toBe('READY_DRAFT');
      const changed = await requirePool().query<{ mappings: unknown }>(
        `SELECT mappings FROM ape_dts_platform.${table} WHERE task_revision_id = $1::uuid`,
        [created.draft!.id],
      );
      expect(changed.rows[0]?.mappings).toEqual(revised.mappings);
    }
  });

  it('preserves legacy scalar storage and refuses a hybrid or empty mapping record', async () => {
    const kafka = await createKafkaDataSources();
    const pgId = await createPostgresDataSource();
    for (const configuration of [
      pgConfiguration(pgId, kafka.targetId),
      kafkaConfiguration(kafka.sourceId, kafka.targetId),
    ]) {
      const pg = configuration.kind === 'PG_TO_KAFKA';
      const created = await requireTasks().create(administrator, {
        configuration,
        name: `Legacy scalar storage ${configuration.kind}`,
        sourceDataSourceId: pg ? pgId : kafka.sourceId,
        targetDataSourceId: kafka.targetId,
        taskType: pg ? 'PG_KAFKA' : 'KAFKA_KAFKA',
      });
      const table = pg ? 'pg_kafka_revision_configs' : 'kafka_kafka_revision_configs';
      const sourceColumns = pg
        ? 'source_schema = NULL, source_table = NULL'
        : 'source_topic = NULL';
      for (const mappings of ['[]', '{}', 'null']) {
        await expect(
          requirePool().query(
            `UPDATE ape_dts_platform.${table} SET ${sourceColumns}, target_topic = NULL, mappings = $2::jsonb WHERE task_revision_id = $1::uuid`,
            [created.draft!.id, mappings],
          ),
        ).rejects.toMatchObject({ code: '23514' });
      }
      await expect(
        requirePool().query(
          `UPDATE ape_dts_platform.${table} SET mappings = '[{}]'::jsonb WHERE task_revision_id = $1::uuid`,
          [created.draft!.id],
        ),
      ).rejects.toMatchObject({ code: '23514' });
      const preserved = await requirePool().query<{ target_topic: string; mappings: null }>(
        `SELECT target_topic, mappings FROM ape_dts_platform.${table} WHERE task_revision_id = $1::uuid`,
        [created.draft!.id],
      );
      expect(preserved.rows[0]).toEqual({
        target_topic: configuration.target.topic,
        mappings: null,
      });
    }
  });

  it('persists Kafka encoding settings through draft edits and renders the saved configuration', async () => {
    const sourceId = await createPostgresDataSource();
    const kafka = await createKafkaDataSources();
    const configuration = pgConfiguration(sourceId, kafka.targetId);
    const create = (name: string) =>
      requireTasks().create(administrator, {
        configuration,
        name,
        sourceDataSourceId: sourceId,
        targetDataSourceId: kafka.targetId,
        taskType: 'PG_KAFKA',
      });
    const created = await create('JSON sink configuration');
    const untouched = await create('Legacy sink configuration');
    const settings = { messageFormat: 'json', jsonTemplate: 'cloudcanal', withFieldDefs: true };
    const saved = await requireTasks().saveDraft(administrator, created.id, {
      configuration: { ...configuration, target: { ...configuration.target, ...settings } },
      expectedDraftVersion: created.draft!.draftVersion,
      expectedTaskRowVersion: created.rowVersion,
      revisionId: created.draft!.id,
      taskType: 'PG_KAFKA',
    });
    expect(saved.draft?.state).toBe('READY_DRAFT');
    const persisted = await requirePool().query<{ configuration: unknown }>(
      'SELECT configuration FROM ape_dts_platform.task_revisions WHERE id = $1::uuid',
      [created.draft!.id],
    );
    const config = parsePlatformTaskConfig(persisted.rows[0]?.configuration);
    expect(config.target).toMatchObject(settings);
    const rendered = new ApeDtsV1Adapter().render({
      config,
      scenario: 'PG_CDC_TO_KAFKA',
      runtime: {
        log4rsPath: '/run/ape-dts/task/run/unit/log4rs.yaml',
        logDirectory: '/run/ape-dts/task/run/unit',
        logLevel: 'info',
        metricsPort: 36001,
        sourceConnection: runtimeText('postgresql://pg/app', 'postgresql://pg/app'),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
    });
    expect(rendered.runtimeIni).toContain('message_format=json\n');
    expect(rendered.runtimeIni).toContain('json_template=cloudcanal\n');
    expect(rendered.runtimeIni).toContain('with_field_defs=true\n');
    const avro = { ...configuration.target, messageFormat: 'avro', withFieldDefs: false };
    const switched = await requireTasks().saveDraft(administrator, created.id, {
      configuration: { ...configuration, target: avro },
      expectedDraftVersion: saved.draft!.draftVersion,
      expectedTaskRowVersion: saved.rowVersion,
      revisionId: saved.draft!.id,
      taskType: 'PG_KAFKA',
    });
    expect(switched.draft?.state).toBe('READY_DRAFT');
    expect(switched.draft?.configuration.target).toEqual(avro);
    const legacy = await requireTasks().get(administrator, untouched.id);
    expect(legacy.draft?.configuration.target).toEqual(configuration.target);
  });

  it('persists environment failure and never marks an otherwise valid draft preflight-passed', async () => {
    const sources = await createKafkaDataSources();
    const task = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(sources.sourceId, sources.targetId),
      name: 'Environment blocked task',
      sourceDataSourceId: sources.sourceId,
      targetDataSourceId: sources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const revisionId = task.draft!.id;
    await attachCertifiedKafkaArtifact(revisionId);
    const service = buildKafkaPreflightService(
      sources.sourceId,
      sources.targetId,
      new RuntimeEnvironmentInspector({}),
    );
    const run = await service.run(administrator, task.id, {
      expectedTaskRowVersion: task.rowVersion,
      revisionId,
    });
    expect(run.state).toBe('FAILED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({ key: 'RUNTIME_ENVIRONMENT', state: 'FAILED' }),
    );
    const current = await requireTasks().get(administrator, task.id);
    expect(current.draft?.state).not.toBe('PREFLIGHT_PASSED');
    const persisted = await requirePool().query<{ result: { checks: unknown[] } }>(
      'SELECT result FROM ape_dts_platform.preflight_runs WHERE id = $1::uuid AND task_revision_id = $2::uuid',
      [run.id, revisionId],
    );
    expect(persisted.rows[0]?.result.checks).toContainEqual(
      expect.objectContaining({ key: 'RUNTIME_ENVIRONMENT', state: 'FAILED' }),
    );
  });

  it('persists and removes the explicit target topic creation policy without changing other tasks', async () => {
    const dataSources = await createKafkaDataSources();
    const configuration = kafkaConfiguration(dataSources.sourceId, dataSources.targetId);
    const create = (name: string) =>
      requireTasks().create(administrator, {
        configuration,
        name,
        sourceDataSourceId: dataSources.sourceId,
        targetDataSourceId: dataSources.targetId,
        taskType: 'KAFKA_KAFKA',
      });
    const created = await create('Topic policy enabled');
    const untouched = await create('Topic policy unchanged');
    const topicCreation = { numPartitions: 3, replicationFactor: 1 };
    const saved = await requireTasks().saveDraft(administrator, created.id, {
      configuration: { ...configuration, target: { ...configuration.target, topicCreation } },
      expectedDraftVersion: created.draft!.draftVersion,
      expectedTaskRowVersion: created.rowVersion,
      revisionId: created.draft!.id,
      taskType: 'KAFKA_KAFKA',
    });
    const persisted = await requirePool().query<{
      configuration: { target: Record<string, unknown> };
    }>('SELECT configuration FROM ape_dts_platform.task_revisions WHERE id = $1::uuid', [
      created.draft!.id,
    ]);
    expect(persisted.rows[0]?.configuration.target.topicCreation).toEqual(topicCreation);
    await requireTasks().saveDraft(administrator, created.id, {
      configuration,
      expectedDraftVersion: saved.draft!.draftVersion,
      expectedTaskRowVersion: saved.rowVersion,
      revisionId: saved.draft!.id,
      taskType: 'KAFKA_KAFKA',
    });
    const cleared = await requirePool().query<{
      configuration: { target: Record<string, unknown> };
    }>('SELECT configuration FROM ape_dts_platform.task_revisions WHERE id = ANY($1::uuid[])', [
      [created.draft!.id, untouched.draft!.id],
    ]);
    expect(cleared.rows).toHaveLength(2);
    for (const row of cleared.rows)
      expect(row.configuration.target).not.toHaveProperty('topicCreation');
  });

  it('derives the Kafka Group ID from the task instead of accepting a user value', async () => {
    const dataSources = await createKafkaDataSources();
    const configuration = kafkaConfiguration(dataSources.sourceId, dataSources.targetId);
    const created = await requireTasks().create(administrator, {
      configuration: {
        ...configuration,
        source: {
          dataSourceId: configuration.source.dataSourceId,
          topic: configuration.source.topic,
        },
      },
      name: 'Unit 7 system Group ID task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });

    expect(created.draft).toMatchObject({
      configuration: {
        source: {
          dataSourceId: dataSources.sourceId,
          groupId: `ape-dts-task-${created.id}`,
          topic: configuration.source.topic,
        },
      },
      state: 'READY_DRAFT',
    });
  });

  it('increments draft_version and rejects a stale save without changing the server row', async () => {
    const dataSources = await createKafkaDataSources();
    const created = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 concurrent draft',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });

    const saved = await requireTasks().saveDraft(administrator, created.id, {
      configuration: {
        ...kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
        target: { dataSourceId: dataSources.targetId, topic: 'unit7.target.updated' },
      },
      expectedDraftVersion: created.draft?.draftVersion ?? '1',
      expectedTaskRowVersion: created.rowVersion,
      revisionId: created.draft?.id ?? '',
      taskType: 'KAFKA_KAFKA',
    });
    expect(saved.draft).toMatchObject({ draftVersion: '2', state: 'READY_DRAFT' });

    await expect(
      requireTasks().saveDraft(administrator, created.id, {
        configuration: {
          ...kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
          target: { dataSourceId: dataSources.targetId, topic: 'unit7.target.stale' },
        },
        expectedDraftVersion: '1',
        expectedTaskRowVersion: created.rowVersion,
        revisionId: created.draft?.id ?? '',
        taskType: 'KAFKA_KAFKA',
      }),
    ).rejects.toMatchObject(new TasksServiceError('TASK_DRAFT_VERSION_CONFLICT'));

    const unchanged = await requirePool().query<{
      configuration: Record<string, unknown>;
      draft_version: string;
      row_version: string;
    }>(
      `SELECT configuration, draft_version::text, row_version::text
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid`,
      [created.draft?.id],
    );
    expect(unchanged.rows[0]).toMatchObject({
      configuration: expect.objectContaining({
        target: expect.objectContaining({ topic: 'unit7.target.updated' }),
      }),
      draft_version: '2',
    });
    expect(Number(unchanged.rows[0]?.row_version)).toBeGreaterThan(1);
  });

  it('keeps an applied revision immutable at the database seam', async () => {
    const dataSources = await createKafkaDataSources();
    const created = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 immutable revision',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const revisionId = created.draft?.id;
    if (revisionId === undefined) {
      throw new Error('created task did not return its draft revision');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.task_revisions
          SET state = 'APPLIED'
        WHERE id = $1::uuid`,
      [revisionId],
    );
    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.task_revisions
            SET configuration = $2::jsonb
          WHERE id = $1::uuid`,
        [
          revisionId,
          JSON.stringify(kafkaConfiguration(dataSources.sourceId, dataSources.targetId)),
        ],
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('soft-deletes only a stopped task with no active run unit and retains revisions/resources', async () => {
    const dataSources = await createKafkaDataSources();
    const deletable = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 deletable task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const blocked = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 running task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const resourceId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.external_resources (
         id, task_id, data_source_id, resource_type, normalized_name,
         database_name, ownership_mode, stable_identity, lifecycle_state, created_by_user_id
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA_GROUP', 'unit7-group', NULL,
                 'SELF_MANAGED', 'unit7-group-identity', 'BOUND', $4::uuid)`,
      [resourceId, deletable.id, dataSources.sourceId, administrator.id],
    );
    const blockedRevisionId = blocked.draft?.id;
    if (blockedRevisionId === undefined) {
      throw new Error('blocked task did not return its draft revision');
    }
    const blockedRunId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (
         id, task_id, task_revision_id, status, current_stage
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'RUNNING', 'RUNNING')`,
      [blockedRunId, blocked.id, blockedRevisionId],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_run_units (id, task_id, run_id, unit_key, status)
       VALUES ($1::uuid, $2::uuid, $3::uuid, 'partition:0', 'RUNNING')`,
      [randomUUID(), blocked.id, blockedRunId],
    );

    await expect(
      requireTasks().delete(administrator, blocked.id, blocked.rowVersion),
    ).rejects.toMatchObject(new TasksServiceError('TASK_ACTIVE_RUN_UNITS'));

    const deleted = await requireTasks().delete(administrator, deletable.id, deletable.rowVersion);
    expect(deleted).toMatchObject({ id: deletable.id, deletedAt: expect.any(String) });

    const retained = await requirePool().query<{
      deleted_at: Date | null;
      revision_count: string;
      resource_count: string;
      state: string;
    }>(
      `SELECT
         tasks.desired_state,
         tasks.deleted_at,
         (SELECT count(*)::text FROM ape_dts_platform.task_revisions WHERE task_id = tasks.id) AS revision_count,
         (SELECT count(*)::text FROM ape_dts_platform.external_resources WHERE task_id = tasks.id) AS resource_count
       FROM ape_dts_platform.tasks
      WHERE tasks.id = $1::uuid`,
      [deletable.id],
    );
    expect(retained.rows[0]).toMatchObject({
      resource_count: '1',
      revision_count: '1',
      desired_state: 'DELETED',
      deleted_at: expect.any(Date),
    });

    const blockedStillActive = await requirePool().query<{
      deleted_at: Date | null;
      desired_state: string;
    }>(
      `SELECT desired_state, deleted_at
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid`,
      [blocked.id],
    );
    expect(blockedStillActive.rows[0]).toEqual({ deleted_at: null, desired_state: 'STOPPED' });
  });

  it('rejects a PG Slot or Publication already bound to another task', async () => {
    const dataSources = await createKafkaDataSources();
    const postgresSourceId = await createPostgresDataSource();
    const first = await requireTasks().create(administrator, {
      configuration: pgConfiguration(postgresSourceId, dataSources.targetId),
      name: 'Unit 7 first PG resource task',
      sourceDataSourceId: postgresSourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'PG_KAFKA',
    });
    const second = await requireTasks().create(administrator, {
      configuration: pgConfiguration(postgresSourceId, dataSources.targetId),
      name: 'Unit 7 second PG resource task',
      sourceDataSourceId: postgresSourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'PG_KAFKA',
    });
    const request = {
      dataSourceId: postgresSourceId,
      databaseName: `unit7_source_${postgresSourceId.replaceAll('-', '').slice(0, 12)}`,
      name: 'unit7_slot',
      ownershipMode: 'PLATFORM_MANAGED' as const,
      resourceType: 'PG_SLOT' as const,
      stableIdentity: 'slot-identity-1',
      taskId: first.id,
    };
    await expect(requireResources().bind(administrator, request)).resolves.toMatchObject({
      taskId: first.id,
      resourceType: 'PG_SLOT',
    });
    await expect(
      requireResources().bind(administrator, { ...request, taskId: second.id }),
    ).rejects.toMatchObject(new PgResourceManagerError('PG_RESOURCE_CONFLICT'));
    const resourceRows = await requirePool().query<{ count: string }>(
      `SELECT count(*)::text AS count
         FROM ape_dts_platform.external_resources
        WHERE data_source_id = $1::uuid
          AND resource_type = 'PG_SLOT'`,
      [postgresSourceId],
    );
    expect(resourceRows.rows[0]?.count).toBe('1');
  });

  it('reserves all Kafka partitions atomically against the configured capacity', async () => {
    const dataSources = await createKafkaDataSources();
    const task = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 capacity task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const revisionId = task.draft?.id;
    if (revisionId === undefined) {
      throw new Error('capacity task did not return its draft revision');
    }
    await requirePool().query(
      `INSERT INTO ape_dts_platform.platform_settings (singleton_id, max_runtime_units)
       VALUES (1, 2)
       ON CONFLICT (singleton_id) DO UPDATE SET max_runtime_units = EXCLUDED.max_runtime_units`,
    );
    const runId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (
         id, task_id, task_revision_id, status, current_stage
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STARTING', 'CAPACITY_TEST')`,
      [runId, task.id, revisionId],
    );
    await expect(
      requireCapacity().reserve(task.id, revisionId, ['partition:0', 'partition:1'], runId),
    ).resolves.toHaveLength(2);

    const second = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 capacity blocked task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const secondRevisionId = second.draft?.id;
    if (secondRevisionId === undefined) {
      throw new Error('second capacity task did not return its draft revision');
    }
    const secondRunId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (
         id, task_id, task_revision_id, status, current_stage
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STARTING', 'CAPACITY_TEST')`,
      [secondRunId, second.id, secondRevisionId],
    );
    await expect(
      requireCapacity().reserve(
        second.id,
        secondRevisionId,
        ['partition:0', 'partition:1'],
        secondRunId,
      ),
    ).rejects.toMatchObject({
      code: 'CAPACITY_INSUFFICIENT',
      details: { limit: 2, requested: 2, used: 2 },
    });
    const secondRows = await requirePool().query<{ count: string }>(
      `SELECT count(*)::text AS count
         FROM ape_dts_platform.capacity_reservations
        WHERE task_id = $1::uuid`,
      [second.id],
    );
    expect(secondRows.rows[0]?.count).toBe('0');
  });

  it('rejects cross-task runtime units and capacity reservations', async () => {
    const dataSources = await createKafkaDataSources();
    const first = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 first association task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const second = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 second association task',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const firstRevisionId = first.draft?.id;
    const secondRevisionId = second.draft?.id;
    if (firstRevisionId === undefined || secondRevisionId === undefined) {
      throw new Error('association tasks did not return draft revisions');
    }
    const runId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (
         id, task_id, task_revision_id, status, current_stage
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STARTING', 'ASSOCIATION_TEST')`,
      [runId, first.id, firstRevisionId],
    );

    await expect(
      requirePool().query(
        `INSERT INTO ape_dts_platform.task_run_units (
           id, task_id, run_id, unit_key, status
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'partition:0', 'STARTING')`,
        [randomUUID(), second.id, runId],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      requireCapacity().reserve(second.id, secondRevisionId, ['partition:0'], runId),
    ).rejects.toMatchObject({ code: 'CAPACITY_RUN_NOT_FOUND' });
  });

  it('persists a passed Kafka preflight and establishes topic identity baselines', async () => {
    const dataSources = await createKafkaDataSources();
    const task = await requireTasks().create(administrator, {
      configuration: kafkaConfiguration(dataSources.sourceId, dataSources.targetId),
      name: 'Unit 7 persisted preflight',
      sourceDataSourceId: dataSources.sourceId,
      targetDataSourceId: dataSources.targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const taskRevisionId = task.draft?.id;
    if (taskRevisionId === undefined) {
      throw new Error('preflight task did not return its draft revision');
    }
    await attachCertifiedKafkaArtifact(taskRevisionId);
    await requirePool().query(
      `INSERT INTO ape_dts_platform.platform_settings (singleton_id, max_runtime_units)
       VALUES (1, 10)
       ON CONFLICT (singleton_id) DO UPDATE SET max_runtime_units = EXCLUDED.max_runtime_units`,
    );

    preflight = buildKafkaPreflightService(dataSources.sourceId, dataSources.targetId);
    const run = await requirePreflight().run(administrator, task.id, {
      expectedTaskRowVersion: task.rowVersion,
      revisionId: taskRevisionId,
    });

    expect(run.state, JSON.stringify(run)).toBe('PASSED');
    expect(run.failureCode).toBeNull();
    const persisted = await requirePool().query<{
      resource_count: string;
      revision_state: string;
      run_state: string;
    }>(
      `SELECT
         (SELECT count(*)::text
            FROM ape_dts_platform.external_resources
           WHERE task_id = $1::uuid
             AND resource_type IN ('KAFKA_SOURCE_TOPIC', 'KAFKA_TARGET_TOPIC')) AS resource_count,
         (SELECT state FROM ape_dts_platform.task_revisions WHERE id = $2::uuid) AS revision_state,
         (SELECT state FROM ape_dts_platform.preflight_runs WHERE id = $3::uuid) AS run_state`,
      [task.id, taskRevisionId, run.id],
    );
    expect(persisted.rows[0]).toEqual({
      resource_count: '2',
      revision_state: 'PREFLIGHT_PASSED',
      run_state: 'PASSED',
    });

    // Exercise recovery through real PostgreSQL bindings and persistence, not
    // only the in-memory query dispatcher used by the focused service tests.
    const oldRunId = randomUUID();
    await requirePool().query(
      `UPDATE ape_dts_platform.task_revisions SET state = 'APPLIED' WHERE id = $1::uuid`,
      [taskRevisionId],
    );
    const runningTask = await requirePool().query<{ row_version: string }>(
      `UPDATE ape_dts_platform.tasks
          SET desired_state = 'RUNNING', current_applied_revision_id = $2::uuid
        WHERE id = $1::uuid RETURNING row_version::text`,
      [task.id, taskRevisionId],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs (id, task_id, task_revision_id, status, current_stage)
       VALUES ($1::uuid, $2::uuid, $3::uuid, 'RUNNING', 'RUNNING')`,
      [oldRunId, task.id, taskRevisionId],
    );
    const recoveryRequest = {
      taskId: task.id,
      revisionId: taskRevisionId,
      expectedOldRunId: oldRunId,
      expectedTaskRowVersion: runningTask.rows[0]!.row_version,
    };
    const withoutCapacity = await requirePreflight().runForSystemRecovery(recoveryRequest);
    expect(withoutCapacity.state).toBe('FAILED');
    expect(withoutCapacity.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'CAPACITY',
        code: 'CAPACITY_RESERVATION_MISMATCH',
        state: 'FAILED',
      }),
    );
    await requireCapacity().reserve(
      task.id,
      taskRevisionId,
      ['partition:0', 'partition:1'],
      oldRunId,
    );
    const recovered = await requirePreflight().runForSystemRecovery(recoveryRequest);
    expect(recovered.state, JSON.stringify(recovered.result.checks)).toBe('PASSED');
    expect(recovered.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'CONFIGURATION',
        code: 'CONFIGURATION_VALID',
        state: 'PASSED',
      }),
    );
    const revisionAfter = await requirePool().query<{ state: string }>(
      'SELECT state FROM ape_dts_platform.task_revisions WHERE id = $1::uuid',
      [taskRevisionId],
    );
    expect(revisionAfter.rows[0]?.state).toBe('APPLIED');
    await expect(
      requirePreflight().runForSystemRecovery({
        ...recoveryRequest,
        expectedOldRunId: randomUUID(),
      }),
    ).rejects.toMatchObject({ code: 'PREFLIGHT_REVISION_NOT_CURRENT' });
  });
});
