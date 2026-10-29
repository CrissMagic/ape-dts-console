import { randomUUID } from 'node:crypto';

import { CertifiedAdapterRegistry, type ApeDtsConfigAdapter } from '@scs/config-model';
import type {
  DataSourceSummary,
  MetadataResponse,
  MetadataItem,
  TaskPreflightResult,
  UserSummary,
} from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import {
  KafkaTopicAuditError,
  type DataSourcesService,
} from '../data-sources/data-sources.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { CapacityReservationService } from '../pg-resources/capacity-reservation.service.js';
import type { ContinuityService } from './continuity.service.js';
import { PreflightService, PreflightServiceError } from './preflight.service.js';
import { DataSourceAdapterError } from '../data-sources/adapters/types.js';

const taskId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3';
const revisionId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4';
const sourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5';
const targetId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f6';
const sourceTopicId = 'source-topic-id';
const targetTopicId = 'target-topic-id';
const sourceClusterId = 'source-cluster-id';
const targetClusterId = 'target-cluster-id';
const artifactId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f7';
const artifactSha256 = 'a'.repeat(64);

const administrator: UserSummary = {
  displayName: 'Preflight administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'preflight-admin',
};

function dataSource(id: string, rowVersion = '1'): DataSourceSummary {
  return {
    configuration: {
      bootstrapServers: ['127.0.0.1:9092'],
      plaintextRiskAcknowledged: true,
      type: 'KAFKA',
    },
    createdAt: '2026-09-03T00:00:00.000Z',
    deletedAt: null,
    id,
    name: `kafka-${id}`,
    rowVersion,
    state: 'ACTIVE',
    updatedAt: '2026-09-03T00:00:00.000Z',
  };
}

function metadata(
  dataSourceId: string,
  resource: 'COLUMNS' | 'TOPICS' | 'PARTITIONS',
  items: readonly MetadataItem[],
): MetadataResponse {
  return {
    attemptedAt: '2026-09-03T00:00:00.000Z',
    dataSourceId,
    items: [...items],
    refreshedAt: '2026-09-03T00:00:00.000Z',
    resource,
    state: 'SUCCESS',
  };
}

interface FakePool {
  connect: ReturnType<typeof vi.fn>;
  query: ReturnType<
    typeof vi.fn<
      (
        sql: string,
        parameters?: readonly unknown[],
      ) => Promise<{ rows: unknown[]; rowCount: number }>
    >
  >;
  currentTaskRowVersion: string;
  insertedResources: number;
  queries: { readonly parameters: readonly unknown[]; readonly sql: string }[];
  runResult: TaskPreflightResult;
  runState: 'RUNNING' | 'PASSED' | 'FAILED' | 'STALE';
}

function fakeDatabase(
  options: {
    readonly heldCapacity?: number;
    readonly configuration?: Record<string, unknown>;
    readonly artifactState?: 'ACTIVE' | 'INACTIVE' | 'QUARANTINED';
    readonly autoTopic?: boolean;
    readonly history?: boolean;
    readonly activeRun?: boolean;
    readonly pgTask?: boolean;
    readonly mysqlTask?: boolean;
    readonly runningRevisionId?: string;
    readonly staleAtFinalize?: boolean;
    readonly systemRecovery?: boolean;
  } = {},
): FakePool {
  const pool = {} as FakePool;
  const task = {
    current_applied_revision_id: options.systemRecovery ? revisionId : null,
    current_draft_revision_id: revisionId,
    deleted_at: null,
    desired_state: options.systemRecovery ? 'RUNNING' : 'STOPPED',
    observed_state: 'STOPPED',
    id: taskId,
    row_version: '1',
    source_data_source_id: sourceId,
    target_data_source_id: targetId,
    task_type: options.mysqlTask ? 'MYSQL_KAFKA' : options.pgTask ? 'PG_KAFKA' : 'KAFKA_KAFKA',
  };
  const revision = {
    artifact_id: artifactId,
    configuration:
      options.configuration ??
      (options.pgTask
        ? {
            kind: 'PG_TO_KAFKA',
            schemaVersion: 1,
            source: {
              dataSourceId: sourceId,
              includeSnapshot: false,
              publicationName: 'demo_pub',
              resourceMode: 'PLATFORM_MANAGED',
              slotName: 'demo_slot',
              table: { name: 'events', schema: 'public' },
            },
            target: { dataSourceId: targetId, topic: 'target.events' },
            tuning: {
              batchSize: 100,
              bufferSize: 100,
              checkpointIntervalSeconds: 5,
              requiredAcks: 'all',
            },
          }
        : {
            kind: 'KAFKA_RAW_FORWARD',
            schemaVersion: 1,
            source: {
              dataSourceId: sourceId,
              groupId: `ape-dts-task-${taskId}`,
              topic: 'source.events',
            },
            target: {
              dataSourceId: targetId,
              topic: 'target.events',
              ...(options.autoTopic
                ? { topicCreation: { numPartitions: 2, replicationFactor: 1 } }
                : {}),
            },
            tuning: {
              ackIntervalSeconds: 5,
              batchSize: 100,
              bufferSize: 16_000,
              checkpointIntervalSeconds: 15,
              requiredAcks: 'all',
            },
          }),
    draft_version: '1',
    id: revisionId,
    state: options.systemRecovery ? 'APPLIED' : 'READY_DRAFT',
    task_id: taskId,
  };
  const dispatch = async (sql: string, parameters: readonly unknown[] = []) => {
    pool.queries.push({ parameters, sql });
    const normalized = sql.replace(/\s+/gu, ' ').trim().toLowerCase();
    if (normalized === 'begin' || normalized === 'commit' || normalized === 'rollback') {
      return { rows: [], rowCount: 0 };
    }
    if (normalized.startsWith('select exists')) {
      if (normalized.includes('task_run_units'))
        return { rows: [{ exists: options.activeRun === true }], rowCount: 1 };
      return { rows: [{ exists: options.history === true }], rowCount: 1 };
    }
    if (normalized.startsWith('select state, result, completed_at')) {
      return {
        rows: [{ state: pool.runState, result: pool.runResult, completed_at: new Date() }],
        rowCount: 1,
      };
    }
    if (
      normalized.includes('from ape_dts_platform.preflight_runs') &&
      normalized.includes("state = 'running'")
    ) {
      return options.runningRevisionId === parameters[0]
        ? { rows: [{ id: randomUUID() }], rowCount: 1 }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('insert into ape_dts_platform.preflight_runs')) {
      pool.runState = 'RUNNING';
      return { rows: [], rowCount: 1 };
    }
    if (normalized.includes('select id, task_revision_id as revision_id')) {
      return {
        rows: [
          {
            completed_at: pool.runState === 'RUNNING' ? null : new Date('2026-09-03T00:00:01.000Z'),
            failure_code:
              pool.runState === 'PASSED'
                ? null
                : pool.runState === 'RUNNING'
                  ? null
                  : 'PREFLIGHT_STALE',
            failure_summary:
              pool.runState === 'PASSED' || pool.runState === 'RUNNING'
                ? null
                : '任务或修订在预检期间发生变化，结果不能用于启动。',
            id: parameters[0],
            result: pool.runResult,
            revision_id: revisionId,
            started_at: new Date('2026-09-03T00:00:00.000Z'),
            state: pool.runState,
            task_id: taskId,
          },
        ],
        rowCount: 1,
      };
    }
    if (normalized.includes('select id, state, encode(actual_sha256')) {
      return {
        rows: [
          {
            actual_sha256: artifactSha256,
            id: artifactId,
            state: options.artifactState ?? 'ACTIVE',
          },
        ],
        rowCount: 1,
      };
    }
    if (normalized.includes('from ape_dts_platform.external_resources')) {
      if (
        options.pgTask &&
        normalized.includes('resource_type = $2') &&
        ['KAFKA_SOURCE_TOPIC', 'KAFKA_TARGET_TOPIC'].includes(String(parameters[1]))
      ) {
        return { rows: [], rowCount: 0 };
      }
      if (options.pgTask) {
        return {
          rows: [
            {
              data_source_id: sourceId,
              resource_type: 'PG_SLOT',
              normalized_name: 'demo_slot',
              database_name: 'demo',
              ownership_mode: 'PLATFORM_MANAGED',
              stable_identity: 'slot-identity',
              lifecycle_state: 'BOUND',
            },
            {
              data_source_id: sourceId,
              resource_type: 'PG_PUBLICATION',
              normalized_name: 'demo_pub',
              database_name: 'demo',
              ownership_mode: 'PLATFORM_MANAGED',
              stable_identity: 'pub-identity',
              lifecycle_state: 'BOUND',
            },
          ],
          rowCount: 2,
        };
      }
      return options.systemRecovery
        ? {
            rows: [
              {
                data_source_id: sourceId,
                resource_type: 'KAFKA_SOURCE_TOPIC',
                normalized_name: 'source.events',
                database_name: null,
                ownership_mode: 'SELF_MANAGED',
                stable_identity: `${sourceClusterId}:${sourceTopicId}`,
                lifecycle_state: 'BOUND',
              },
              {
                data_source_id: targetId,
                resource_type: 'KAFKA_TARGET_TOPIC',
                normalized_name: 'target.events',
                database_name: null,
                ownership_mode: 'SELF_MANAGED',
                stable_identity: `${targetClusterId}:${targetTopicId}`,
                lifecycle_state: 'BOUND',
              },
            ],
            rowCount: 2,
          }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.capacity_reservations')) {
      return {
        rows: [{ count: String(options.heldCapacity ?? (options.systemRecovery ? 2 : 0)) }],
        rowCount: 1,
      };
    }
    if (normalized.includes('insert into ape_dts_platform.external_resources')) {
      pool.insertedResources += 1;
      return { rows: [], rowCount: 1 };
    }
    if (
      normalized.includes('select id, row_version::text') &&
      normalized.includes('data_sources')
    ) {
      return {
        rows: [
          { id: sourceId, row_version: '1' },
          { id: targetId, row_version: '1' },
        ],
        rowCount: 2,
      };
    }
    if (normalized.startsWith('select revision.configuration')) {
      return { rows: [{ configuration: revision.configuration }], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.tasks')) {
      return {
        rows: [
          {
            ...task,
            row_version:
              options.staleAtFinalize && normalized.includes('for update')
                ? pool.currentTaskRowVersion
                : '1',
          },
        ],
        rowCount: 1,
      };
    }
    if (normalized.includes('from ape_dts_platform.task_runs')) {
      return { rows: [{ id: parameters[0] }], rowCount: 1 };
    }
    if (normalized.includes('join ape_dts_platform.task_runs as runs')) {
      return { rows: [{ id: taskId }], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.task_revisions')) {
      return { rows: [revision], rowCount: 1 };
    }
    if (
      normalized.includes("update ape_dts_platform.task_revisions set state = 'prefight_passed'")
    ) {
      return { rows: [], rowCount: 1 };
    }
    if (normalized.includes('update ape_dts_platform.task_revisions')) {
      return { rows: [], rowCount: 1 };
    }
    if (normalized.includes('delete from ape_dts_platform.risk_acceptances')) {
      return { rows: [], rowCount: 0 };
    }
    if (normalized.includes('update ape_dts_platform.preflight_runs')) {
      pool.runState = normalized.includes("state = 'stale'")
        ? 'STALE'
        : parameters[1] === 'FAILED'
          ? 'FAILED'
          : 'PASSED';
      const result = parameters[2] ?? parameters[1];
      if (typeof result === 'string') {
        pool.runResult = JSON.parse(result) as TaskPreflightResult;
      }
      return { rows: [], rowCount: 1 };
    }
    if (normalized.includes('update ape_dts_platform.pg_kafka_revision_configs')) {
      return { rows: [], rowCount: 1 };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  };
  pool.currentTaskRowVersion = options.staleAtFinalize ? '2' : '1';
  pool.insertedResources = 0;
  pool.queries = [];
  pool.runResult = {
    capacity: { available: null, limit: null, requested: null, used: null },
    checks: [],
    dataSourceVersions: { source: '1', target: '1' },
    identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
    taskRevision: { draftVersion: '1', taskRowVersion: '1' },
  };
  pool.runState = 'RUNNING';
  pool.query = vi.fn(dispatch);
  pool.connect = vi.fn(async () => ({ query: pool.query, release: vi.fn() }));
  return pool;
}

function adapterRegistry(): CertifiedAdapterRegistry {
  const adapter: ApeDtsConfigAdapter = {
    id: 'ape-dts-ini',
    render: () => ({ log4rsYaml: '', previewIni: '', runtimeIni: '' }),
    version: '1',
  };
  const registry = new CertifiedAdapterRegistry([adapter]);
  registry.register({
    adapter,
    artifactSha256,
    platformVersion: '0.0.0',
    scenario: 'KAFKA_RAW_FORWARD',
  });
  registry.register({
    adapter,
    artifactSha256,
    platformVersion: '0.0.0',
    scenario: 'PG_CDC_TO_KAFKA',
  });
  return registry;
}

function service(
  options: {
    readonly heldCapacity?: number;
    readonly configuration?: Record<string, unknown>;
    readonly artifactState?: 'ACTIVE' | 'INACTIVE' | 'QUARANTINED';
    readonly autoTopic?: boolean;
    readonly history?: boolean;
    readonly activeRun?: boolean;
    readonly pgTask?: boolean;
    readonly mysqlTask?: boolean;
    readonly runningRevisionId?: string;
    readonly staleAtFinalize?: boolean;
    readonly systemRecovery?: boolean;
  } = {},
) {
  const pool = fakeDatabase(options);
  const source: DataSourceSummary =
    options.pgTask || options.mysqlTask
      ? {
          ...dataSource(sourceId),
          configuration: {
            type: options.mysqlTask ? 'MYSQL' : 'POSTGRES',
            host: 'localhost',
            port: options.mysqlTask ? 3306 : 5432,
            database: 'demo',
            username: 'demo',
            tls: { mode: 'DISABLE', riskAcknowledged: true },
          },
        }
      : dataSource(sourceId);
  const target = dataSource(targetId);
  const metadataMock = vi.fn(
    async (_actor: UserSummary, id: string, query: { resource: string }) => {
      if (query.resource === 'COLUMNS') {
        return metadata(id, 'COLUMNS', [
          {
            attributes: { isNullable: 'false', isPrimaryKey: 'true' },
            identity: null,
            name: 'id',
          },
        ]);
      }
      if (query.resource === 'PARTITIONS') {
        return metadata(id, 'PARTITIONS', [
          { attributes: {}, identity: null, name: '0' },
          { attributes: {}, identity: null, name: '1' },
        ]);
      }
      return metadata(id, 'TOPICS', [
        id === sourceId
          ? { attributes: { partitionCount: '2' }, identity: sourceTopicId, name: 'source.events' }
          : { attributes: { partitionCount: '1' }, identity: targetTopicId, name: 'target.events' },
      ]);
    },
  );
  const testMock = vi.fn(async (_actor: UserSummary, id: string) => ({
    completedAt: '2026-09-03T00:00:00.000Z',
    dataSourceId: id,
    durationMs: 2,
    errorCode: null,
    errorSummary: null,
    id: randomUUID(),
    identity:
      (options.pgTask || options.mysqlTask) && id === sourceId
        ? {
            kind: options.mysqlTask ? ('MYSQL_DATABASE' as const) : ('POSTGRES_DATABASE' as const),
            serverVersion: null,
            value: options.mysqlTask ? 'uuid:demo' : 'postgres-database-identity',
          }
        : {
            kind: 'KAFKA_CLUSTER' as const,
            serverVersion: null,
            value: id === sourceId ? sourceClusterId : targetClusterId,
          },
    purpose: 'PREFLIGHT' as const,
    startedAt: '2026-09-03T00:00:00.000Z',
    state: 'SUCCEEDED' as const,
  }));
  const dataSources = {
    createKafkaTopic: vi.fn(async () => ({ topic: 'target.events', created: true })),
    get: vi.fn(async (_actor: UserSummary, id: string) => (id === sourceId ? source : target)),
    getForSystem: vi.fn(async (id: string) => (id === sourceId ? source : target)),
    metadata: metadataMock,
    test: testMock,
  } as unknown as DataSourcesService;
  Object.assign(dataSources, {
    inspectMysqlCdc: vi.fn(async () => ({
      identity: { kind: 'MYSQL_DATABASE', value: 'uuid:demo', serverVersion: '8.0.36' },
      binlog: { enabled: true, format: 'ROW', rowImage: 'FULL', serverId: 1, gtidMode: 'OFF' },
      position: { filename: 'mysql-bin.000001', position: 157 },
      checks: [{ key: 'BINLOG_POSITION', state: 'PASSED', summary: 'ready' }],
    })),
    metadataForSystem: vi.fn(
      async (id: string, query: { resource: string }) =>
        await metadataMock(administrator, id, query),
    ),
    testForSystem: vi.fn(async (id: string) => await testMock(administrator, id)),
    readPostgresResources: vi.fn(async () => ({
      databaseName: 'demo',
      slot: { name: 'demo_slot', created: false, stableIdentity: 'slot-identity' },
      publication: { name: 'demo_pub', created: false, stableIdentity: 'pub-identity' },
    })),
    readPostgresContinuity: vi.fn(async () => ({
      confirmedFlushLsn: '0/16B6C50',
      databaseName: 'demo',
      databaseOid: '1',
      slotName: 'demo_slot',
      slotPlugin: 'pgoutput',
      slotActive: false as const,
      systemIdentifier: 'system-1',
    })),
  });
  const capacity = {
    snapshot: vi.fn(async (requested: number) =>
      options.configuration !== undefined
        ? { available: 8, limit: 8, requested, used: 0 }
        : { available: 2, limit: 4, requested: 2, used: 2 },
    ),
  } as unknown as CapacityReservationService;
  const audit = { append: vi.fn(async () => '1') } as unknown as AuditLedgerService;
  const database = { requirePool: () => pool } as unknown as PlatformDatabaseService;
  const continuity = {
    reinitializeMysqlWithin: vi.fn<ContinuityService['reinitializeMysqlWithin']>(async () => ({
      code: 'MYSQL_SNAPSHOT_REINITIALIZATION_READY',
      details: {},
      key: 'TOPIC_IDENTITIES',
      state: 'PASSED',
      summary: 'ready',
    })),
    decideWithin: vi.fn<ContinuityService['decideWithin']>(async () => ({
      code: 'CONTINUITY_MATCHED',
      details: {},
      key: 'TOPIC_IDENTITIES',
      state: 'PASSED',
      summary: 'matched',
    })),
  };
  const preflight = new PreflightService(
    database,
    audit,
    dataSources,
    capacity,
    adapterRegistry(),
    continuity as unknown as ContinuityService,
    '0.0.0',
    {
      inspect: async () => ({
        key: 'RUNTIME_ENVIRONMENT',
        state: 'PASSED',
        code: 'RUNTIME_ENVIRONMENT_READY',
        summary: 'Test environment ready',
        details: {},
      }),
    },
  );
  return { audit, continuity, dataSources, pool, preflight };
}

describe('PreflightService', () => {
  function pgConfiguration(includeSnapshot: boolean, tables = ['events']): Record<string, unknown> {
    return {
      kind: 'PG_TO_KAFKA',
      schemaVersion: 2,
      source: {
        dataSourceId: sourceId,
        includeSnapshot,
        resourceMode: 'PLATFORM_MANAGED',
        slotName: 'demo_slot',
        publicationName: 'demo_pub',
      },
      target: { dataSourceId: targetId },
      mappings: tables.map((name) => ({
        source: { schema: 'public', name },
        targets: [{ topic: 'target.events' }],
      })),
      tuning: {
        batchSize: 100,
        bufferSize: 100,
        checkpointIntervalSeconds: 5,
        requiredAcks: 'all',
      },
    };
  }

  function mysqlConfiguration(schema = 'demo'): Record<string, unknown> {
    return {
      kind: 'MYSQL_TO_KAFKA',
      schemaVersion: 2,
      source: { dataSourceId: sourceId, includeSnapshot: true },
      target: { dataSourceId: targetId, messageFormat: 'json', jsonTemplate: 'cloudcanal' },
      mappings: [{ source: { schema, name: 'events' }, targets: [{ topic: 'target.events' }] }],
      tuning: {
        batchSize: 100,
        bufferSize: 1000,
        checkpointIntervalSeconds: 5,
        requiredAcks: 'all',
      },
    };
  }

  it('preflights MySQL tables and Binlog independently from PostgreSQL resources', async () => {
    const context = service({ mysqlTask: true, configuration: mysqlConfiguration() });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(result.state).toBe('PASSED');
    expect(result.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'MYSQL_CDC', state: 'PASSED' }),
        expect.objectContaining({ key: 'PG_RESOURCES', state: 'SKIPPED' }),
      ]),
    );
    expect(context.continuity.decideWithin).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      taskId,
      revisionId,
      '1',
      'STOPPED',
      expect.objectContaining({ kind: 'MYSQL', sourceIdentity: 'uuid:demo' }),
    );
    expect(context.dataSources.readPostgresResources).not.toHaveBeenCalled();
    expect(context.pool.queries.some(({ sql }) => sql.includes('mysql_checkpoints'))).toBe(false);
  });

  it('rejects a MySQL mapping outside the selected database', async () => {
    const context = service({ mysqlTask: true, configuration: mysqlConfiguration('other') });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(result.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          key: 'SOURCE_METADATA',
          state: 'FAILED',
          code: 'MYSQL_SOURCE_DATABASE_MISMATCH',
        }),
      ]),
    );
  });

  it('marks explicit MySQL snapshot reinitialization and never uses ordinary continuity acceptance', async () => {
    const context = service({
      mysqlTask: true,
      configuration: mysqlConfiguration(),
      history: true,
    });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
      reinitializeMysqlSnapshot: true,
    });
    expect(result.state).toBe('PASSED');
    expect(
      result.result.checks.find((item) => item.key === 'CONFIGURATION')?.details
        .reinitializeMysqlSnapshot,
    ).toBe(true);
    expect(context.continuity.reinitializeMysqlWithin).toHaveBeenCalledOnce();
    expect(context.continuity.decideWithin).not.toHaveBeenCalled();
  });

  it('does not reinitialize MySQL continuity while another preflight check is failed', async () => {
    const context = service({ mysqlTask: true, configuration: mysqlConfiguration('other') });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
      reinitializeMysqlSnapshot: true,
    });
    expect(result.state).toBe('FAILED');
    expect(context.continuity.reinitializeMysqlWithin).not.toHaveBeenCalled();
    expect(context.continuity.decideWithin).not.toHaveBeenCalled();
  });

  it('rejects reinitialization for an active run, an incremental-only MySQL task or a Kafka task', async () => {
    for (const context of [
      service({ mysqlTask: true, configuration: mysqlConfiguration(), activeRun: true }),
      service({
        mysqlTask: true,
        configuration: {
          ...mysqlConfiguration(),
          source: { dataSourceId: sourceId, includeSnapshot: false },
        },
      }),
      service(),
    ]) {
      await expect(
        context.preflight.run(administrator, taskId, {
          expectedTaskRowVersion: '1',
          revisionId,
          reinitializeMysqlSnapshot: true,
        }),
      ).rejects.toMatchObject({ code: 'PREFLIGHT_TASK_VERSION_CONFLICT' });
      expect(context.continuity.reinitializeMysqlWithin).not.toHaveBeenCalled();
    }
  });

  it.each([false, true])(
    'allows a PG composite primary key with snapshot=%s and preserves every key column',
    async (includeSnapshot) => {
      const context = service({
        pgTask: true,
        configuration: pgConfiguration(includeSnapshot),
      });
      const originalMetadata = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
      vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
        query.resource === 'COLUMNS'
          ? metadata(
              id,
              'COLUMNS',
              ['order_id', 'line_no'].map((name) => ({
                name,
                identity: null,
                attributes: { isPrimaryKey: 'true', isNullable: 'false' },
              })),
            )
          : originalMetadata(actor, id, query),
      );
      // Exercise the same preflight finalization used by task starts; no live sources are mutated.
      const result = await context.preflight.run(administrator, taskId, {
        expectedTaskRowVersion: '1',
        revisionId,
      });
      expect(result.result.checks.find((item) => item.key === 'PRIMARY_KEY')).toMatchObject({
        state: 'PASSED',
        code: 'SOURCE_PRIMARY_KEY_OK',
        details: { primaryKey: null, primaryKeys: ['order_id', 'line_no'] },
      });
      expect(result.state).toBe('PASSED');
      expect(
        context.pool.queries.filter((item) => item.sql.includes('SET primary_key_column')),
      ).toEqual([expect.objectContaining({ parameters: [revisionId, null] })]);
    },
  );

  it('keeps the legacy single-column PG key snapshot for existing tasks', async () => {
    const context = service({ pgTask: true });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(result.state).toBe('PASSED');
    expect(result.result.checks.find((item) => item.key === 'PRIMARY_KEY')).toMatchObject({
      state: 'PASSED',
      details: { primaryKey: 'id', primaryKeys: ['id'] },
    });
    expect(
      context.pool.queries.filter((item) => item.sql.includes('SET primary_key_column')),
    ).toEqual([expect.objectContaining({ parameters: [revisionId, 'id'] })]);
  });

  it.each([false, true])(
    'checks every PG mapping when the second table is missing a primary key=%s',
    async (missing) => {
      const context = service({
        pgTask: true,
        configuration: pgConfiguration(false, ['events', 'items']),
      });
      const originalMetadata = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
      vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
        query.resource === 'COLUMNS'
          ? metadata(
              id,
              'COLUMNS',
              ['tenant_id', 'id'].map((name) => ({
                name,
                identity: null,
                attributes: {
                  isPrimaryKey: missing && query.table === 'items' ? 'false' : 'true',
                  isNullable: 'false',
                },
              })),
            )
          : originalMetadata(actor, id, query),
      );
      const result = await context.preflight.run(administrator, taskId, {
        expectedTaskRowVersion: '1',
        revisionId,
      });
      expect(result.state).toBe(missing ? 'FAILED' : 'PASSED');
      const primaryKeyCheck = result.result.checks.find((item) => item.key === 'PRIMARY_KEY');
      if (missing) {
        expect(primaryKeyCheck).toMatchObject({
          state: 'FAILED',
          code: 'SOURCE_PRIMARY_KEY_MISSING',
          details: { table: { schema: 'public', name: 'items' } },
        });
      } else {
        expect(primaryKeyCheck).toMatchObject({
          state: 'PASSED',
          details: {
            tables: ['events', 'items'].map((name) => ({
              schema: 'public',
              name,
              primaryKey: null,
              primaryKeys: ['tenant_id', 'id'],
            })),
          },
        });
      }
    },
  );

  it.each([
    { columns: [], code: 'SOURCE_TABLE_OR_COLUMNS_UNAVAILABLE' },
    {
      columns: [
        {
          name: 'business_id',
          identity: null,
          attributes: { isPrimaryKey: 'false', isNullable: 'false' },
        },
      ],
      code: 'SOURCE_PRIMARY_KEY_MISSING',
    },
    {
      columns: ['order_id', 'line_no'].map((name, index) => ({
        name,
        identity: null,
        attributes: { isPrimaryKey: 'true', isNullable: index === 1 ? 'true' : 'false' },
      })),
      code: 'SOURCE_PRIMARY_KEY_NULLABLE',
    },
  ])('rejects unsafe PG key metadata: $code', async ({ columns, code }) => {
    const context = service({ pgTask: true });
    const originalMetadata = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      query.resource === 'COLUMNS'
        ? metadata(id, 'COLUMNS', columns)
        : originalMetadata(actor, id, query),
    );
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(result.result.checks.find((item) => item.key === 'PRIMARY_KEY')).toMatchObject({
      state: 'FAILED',
      code,
    });
    expect(result.state).toBe('FAILED');
  });

  it('allows a MySQL composite primary key and reports failed Binlog checks', async () => {
    const context = service({ mysqlTask: true, configuration: mysqlConfiguration() });
    const oldMetadata = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      query.resource === 'COLUMNS'
        ? metadata(
            id,
            'COLUMNS',
            ['id', 'tenant'].map((name) => ({
              name,
              identity: null,
              attributes: { isPrimaryKey: 'true', isNullable: 'false' },
            })),
          )
        : await oldMetadata(actor, id, query),
    );
    vi.mocked(context.dataSources.inspectMysqlCdc).mockResolvedValue({
      identity: { kind: 'MYSQL_DATABASE', value: 'uuid:demo', serverVersion: '8.0.36' },
      binlog: {
        enabled: true,
        format: 'STATEMENT',
        rowImage: 'FULL',
        serverId: 1,
        gtidMode: 'OFF',
      },
      position: { filename: 'mysql-bin.000001', position: 157 },
      checks: [{ key: 'BINLOG_FORMAT', state: 'FAILED', summary: 'requires ROW' }],
    });
    const result = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(result.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'PRIMARY_KEY', state: 'PASSED' }),
        expect.objectContaining({ key: 'MYSQL_CDC', state: 'FAILED' }),
      ]),
    );
  });

  function multiKafkaConfiguration(): Record<string, unknown> {
    return {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 2,
      source: { dataSourceId: sourceId, groupId: `ape-dts-task-${taskId}` },
      target: { dataSourceId: targetId },
      mappings: [
        {
          source: 'source.events',
          targets: [{ topic: 'target.events' }, { topic: 'target.audit' }],
        },
        {
          source: 'source.orders',
          targets: [{ topic: 'target.orders' }, { topic: 'target.audit' }],
        },
      ],
      tuning: {
        ackIntervalSeconds: 5,
        batchSize: 100,
        bufferSize: 16000,
        checkpointIntervalSeconds: 15,
        requiredAcks: 'all',
      },
    };
  }

  function multiMetadata(context: ReturnType<typeof service>, missingPartitions = false) {
    vi.mocked(context.dataSources.metadata).mockImplementation(async (_actor, id, query) => {
      if (query.resource === 'PARTITIONS')
        return metadata(
          id,
          'PARTITIONS',
          (missingPartitions && query.topic === 'source.orders' ? [0] : [0, 1]).map(
            (partition) => ({ name: String(partition), identity: null, attributes: {} }),
          ),
        );
      return metadata(
        id,
        'TOPICS',
        (id === sourceId
          ? ['source.events', 'source.orders']
          : ['target.events', 'target.orders', 'target.audit']
        ).map((name) => ({ name, identity: null, attributes: { partitionCount: '2' } })),
      );
    });
  }

  it('preflights every source/target and sums source partitions, not fanout targets', async () => {
    const context = service({ configuration: multiKafkaConfiguration() });
    multiMetadata(context);
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('PASSED');
    const topics = run.result.checks.find((item) => item.key === 'TOPIC_IDENTITIES')?.details
      .topics;
    expect(topics).toHaveLength(5);
    expect(topics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ topic: 'source.events', partitionIds: [0, 1] }),
        expect.objectContaining({ topic: 'source.orders', partitionIds: [0, 1] }),
        expect.objectContaining({ topic: 'target.audit' }),
      ]),
    );
    expect(context.continuity.decideWithin.mock.calls[0]?.[6]).toMatchObject({ topics });
    const capacityCheck = run.result.checks.find((item) => item.key === 'CAPACITY');
    expect(capacityCheck?.details.requested).toBe(4);
  });

  it('rejects incomplete partition evidence on any source instead of starting a partial task', async () => {
    const context = service({ configuration: multiKafkaConfiguration() });
    multiMetadata(context, true);
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('FAILED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'TOPIC_IDENTITIES',
        state: 'FAILED',
        code: 'KAFKA_TOPIC_METADATA_INVALID',
      }),
    );
    expect(context.continuity.decideWithin).not.toHaveBeenCalled();
  });

  it('prepares every missing target once even when several source rows share it', async () => {
    const configuration = multiKafkaConfiguration();
    configuration.mappings = [
      {
        source: 'source.events',
        targets: [
          { topic: 'target.events', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
          { topic: 'target.audit', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
        ],
      },
      {
        source: 'source.orders',
        targets: [
          { topic: 'target.audit', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
        ],
      },
    ];
    const context = service({ configuration });
    multiMetadata(context);
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    const created = new Set<string>();
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) => {
      if (id === targetId && query.resource === 'TOPICS')
        return metadata(
          id,
          'TOPICS',
          [...created].map((name) => ({
            name,
            identity: null,
            attributes: { partitionCount: '2' },
          })),
        );
      return original(actor, id, query);
    });
    vi.mocked(context.dataSources.createKafkaTopic).mockImplementation(
      async (_actor, _id, request) => {
        created.add(request.topic);
        return { topic: request.topic, created: true };
      },
    );
    const request = { expectedTaskRowVersion: '1', revisionId };
    const before = await context.preflight.run(administrator, taskId, request);
    expect(before.result.checks).toContainEqual(
      expect.objectContaining({ code: 'TARGET_TOPIC_CREATION_PENDING' }),
    );
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).resolves.toBe(true);
    expect(created).toEqual(new Set(['target.events', 'target.audit']));
    expect(context.dataSources.createKafkaTopic).toHaveBeenCalledTimes(2);
    expect(context.pool.runState).toBe('PASSED');
  });

  it('retains both completed targets when a later target was created but its audit failed', async () => {
    const configuration = multiKafkaConfiguration();
    configuration.mappings = [
      {
        source: 'source.events',
        targets: ['target.events', 'target.audit'].map((topic) => ({
          topic,
          topicCreation: { numPartitions: 2, replicationFactor: 1 },
        })),
      },
    ];
    const context = service({ configuration });
    multiMetadata(context);
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      id === targetId ? metadata(id, 'TOPICS', []) : original(actor, id, query),
    );
    const first = { topic: 'target.events', created: true };
    const second = { topic: 'target.audit', created: true };
    vi.mocked(context.dataSources.createKafkaTopic)
      .mockResolvedValueOnce(first)
      .mockRejectedValueOnce(
        new KafkaTopicAuditError(second, { cause: new Error('audit failed') }),
      );
    const request = { expectedTaskRowVersion: '1', revisionId };
    await context.preflight.run(administrator, taskId, request);
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).rejects.toMatchObject({
      code: 'KAFKA_TOPIC_AUDIT_FAILED',
      result: second,
      completedResults: [first, second],
    });
    expect(context.pool.runState).toBe('FAILED');
  });

  it('reports all three externally completed targets when the owning transaction cannot commit', async () => {
    const configuration = multiKafkaConfiguration();
    const names = ['target.events', 'target.orders', 'target.audit'];
    configuration.mappings = [
      {
        source: 'source.events',
        targets: names.map((topic) => ({
          topic,
          topicCreation: { numPartitions: 2, replicationFactor: 1 },
        })),
      },
    ];
    const context = service({ configuration });
    multiMetadata(context);
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      id === targetId ? metadata(id, 'TOPICS', []) : original(actor, id, query),
    );
    vi.mocked(context.dataSources.createKafkaTopic).mockImplementation(
      async (_actor, _id, request) => ({ topic: request.topic, created: true }),
    );
    const request = { expectedTaskRowVersion: '1', revisionId };
    await context.preflight.run(administrator, taskId, request);
    const dispatch = context.pool.query.getMockImplementation()!;
    context.pool.query.mockImplementation(async (sql, parameters) => {
      if (sql === 'COMMIT') throw new Error('commit failed');
      return dispatch(sql, parameters);
    });
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).rejects.toMatchObject({
      code: 'KAFKA_TOPIC_AUDIT_FAILED',
      completedResults: names.map((topic) => ({ topic, created: true })),
    });
    expect(context.pool.query).toHaveBeenLastCalledWith('ROLLBACK');
    expect(context.pool.runState).toBe('FAILED');
  });

  it('rejects a target that is another source on the same cluster', async () => {
    const configuration = multiKafkaConfiguration();
    configuration.mappings = [
      { source: 'source.events', targets: [{ topic: 'source.orders' }] },
      { source: 'source.orders', targets: [{ topic: 'target.events' }] },
    ];
    const context = service({ configuration });
    multiMetadata(context);
    vi.mocked(context.dataSources.test).mockImplementation(async (_actor, id) => ({
      completedAt: new Date().toISOString(),
      dataSourceId: id,
      durationMs: 1,
      errorCode: null,
      errorSummary: null,
      id: randomUUID(),
      identity: { kind: 'KAFKA_CLUSTER', serverVersion: null, value: sourceClusterId },
      purpose: 'PREFLIGHT',
      startedAt: new Date().toISOString(),
      state: 'SUCCEEDED',
    }));
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) => {
      if (id === targetId && query.resource === 'TOPICS')
        return metadata(
          id,
          'TOPICS',
          ['source.orders', 'target.events'].map((name) => ({
            name,
            identity: null,
            attributes: { partitionCount: '2' },
          })),
        );
      return original(actor, id, query);
    });
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'TOPIC_IDENTITIES',
        state: 'FAILED',
        code: 'KAFKA_TOPIC_LOOP',
      }),
    );
    expect(context.continuity.decideWithin).not.toHaveBeenCalled();
  });

  it('rejects a loop before creating any other missing target', async () => {
    const configuration = multiKafkaConfiguration();
    configuration.mappings = [
      {
        source: 'source.events',
        targets: [
          { topic: 'source.events' },
          { topic: 'missing.target', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
        ],
      },
    ];
    const context = service({ configuration });
    multiMetadata(context);
    const originalTest = vi.mocked(context.dataSources.test).getMockImplementation()!;
    vi.mocked(context.dataSources.test).mockImplementation(async (actor, id, purpose) => {
      const result = await originalTest(actor, id, purpose);
      return {
        ...result,
        identity: { kind: 'KAFKA_CLUSTER', serverVersion: null, value: sourceClusterId },
      };
    });
    const request = { expectedTaskRowVersion: '1', revisionId };
    const run = await context.preflight.run(administrator, taskId, request);
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({ code: 'KAFKA_TOPIC_LOOP', state: 'FAILED' }),
    );
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).resolves.toBe(false);
    expect(context.dataSources.createKafkaTopic).not.toHaveBeenCalled();
  });

  it('reports one platform-managed PG resource failure and skips continuity when resources are missing', async () => {
    const context = service({ pgTask: true });
    vi.mocked(context.dataSources.readPostgresResources).mockRejectedValue(
      new DataSourceAdapterError(
        'PG_RESOURCE_NOT_FOUND',
        '指定的复制槽或发布不存在，请先准备或恢复资源。',
      ),
    );
    vi.mocked(context.dataSources.readPostgresContinuity).mockRejectedValue(
      new DataSourceAdapterError('POSTGRES_SLOT_MISSING', '指定复制槽不存在。'),
    );

    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });

    const pgFailures = run.result.checks.filter(
      (item) => item.key === 'PG_RESOURCES' && item.state === 'FAILED',
    );
    expect(pgFailures).toHaveLength(1);
    expect(pgFailures[0]).toMatchObject({
      code: 'PG_RESOURCE_NOT_FOUND',
      details: { resourceMode: 'PLATFORM_MANAGED' },
    });
    expect(JSON.stringify(pgFailures)).not.toContain('自行管理模式');
    expect(context.dataSources.readPostgresContinuity).not.toHaveBeenCalled();
  });

  it('replaces the successful PG resource check when continuity cannot be read', async () => {
    const context = service({ pgTask: true });
    vi.mocked(context.dataSources.readPostgresContinuity).mockRejectedValue(
      new DataSourceAdapterError('POSTGRES_SLOT_MISSING', '指定复制槽不存在。'),
    );

    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });

    expect(run.result.checks.filter((item) => item.key === 'PG_RESOURCES')).toEqual([
      expect.objectContaining({
        code: 'POSTGRES_SLOT_MISSING',
        details: { resourceMode: 'PLATFORM_MANAGED' },
        state: 'FAILED',
      }),
    ]);
  });

  it.each(['matching', 'replaced', 'missing'] as const)(
    'checks live PG resources after binding: %s',
    async (state) => {
      const context = service();
      const source: DataSourceSummary = {
        ...dataSource(sourceId),
        configuration: {
          type: 'POSTGRES',
          host: 'localhost',
          port: 5432,
          database: 'demo',
          username: 'demo',
          tls: { mode: 'DISABLE', riskAcknowledged: true },
        },
      };
      context.pool.query.mockResolvedValueOnce({
        rows: [
          {
            resource_type: 'PG_SLOT',
            normalized_name: 'demo_slot',
            database_name: 'demo',
            ownership_mode: 'SELF_MANAGED',
            stable_identity: 'slot-identity',
          },
          {
            resource_type: 'PG_PUBLICATION',
            normalized_name: 'demo_pub',
            database_name: 'demo',
            ownership_mode: 'SELF_MANAGED',
            stable_identity: 'pub-identity',
          },
        ],
        rowCount: 2,
      });
      Object.assign(context.dataSources, {
        readPostgresResources: vi.fn(async () => {
          if (state === 'missing')
            throw new DataSourceAdapterError('PG_RESOURCE_NOT_FOUND', '复制资源不存在。');
          return {
            databaseName: 'demo',
            slot: { name: 'demo_slot', created: false, stableIdentity: 'slot-identity' },
            publication: {
              name: 'demo_pub',
              created: false,
              stableIdentity: state === 'replaced' ? 'replacement' : 'pub-identity',
            },
          };
        }),
      });
      const result = await context.preflight['evaluatePgResources'](
        { type: 'USER', user: administrator },
        taskId,
        source,
        {
          kind: 'PG_TO_KAFKA',
          schemaVersion: 1,
          source: {
            dataSourceId: sourceId,
            includeSnapshot: false,
            slotName: 'demo_slot',
            publicationName: 'demo_pub',
            resourceMode: 'SELF_MANAGED',
            table: { schema: 'public', name: 'events' },
          },
          target: { dataSourceId: targetId, topic: 'events' },
          tuning: {
            batchSize: 100,
            bufferSize: 100,
            checkpointIntervalSeconds: 5,
            requiredAcks: 'all',
          },
        },
      );
      expect(result.state).toBe(state === 'matching' ? 'PASSED' : 'FAILED');
      expect(result.code).toBe(
        state === 'matching'
          ? 'PG_RESOURCES_BOUND'
          : state === 'replaced'
            ? 'PG_RESOURCE_CONFLICT'
            : 'PG_RESOURCE_NOT_FOUND',
      );
    },
  );

  function missingTarget(context: ReturnType<typeof service>) {
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      id === targetId ? metadata(id, 'TOPICS', []) : original(actor, id, query),
    );
    return original;
  }
  it('does not create while preflighting; explicit first-start preparation creates then reruns checks', async () => {
    const context = service({ autoTopic: true });
    const original = missingTarget(context);
    const request = { expectedTaskRowVersion: '1', revisionId };
    const before = await context.preflight.run(administrator, taskId, request);
    expect(before.state).toBe('FAILED');
    expect(before.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: 'TARGET_TOPIC_CREATION_PENDING', state: 'FAILED' }),
      ]),
    );
    expect(context.dataSources.createKafkaTopic).not.toHaveBeenCalled();
    expect(context.pool.insertedResources).toBe(0);
    vi.mocked(context.dataSources.createKafkaTopic).mockImplementation(async () => {
      vi.mocked(context.dataSources.metadata).mockImplementation(original);
      return { topic: 'target.events', created: true };
    });
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).resolves.toBe(true);
    expect(context.dataSources.createKafkaTopic).toHaveBeenCalledWith(
      administrator,
      targetId,
      {
        topic: 'target.events',
        numPartitions: 2,
        replicationFactor: 1,
        expectedRowVersion: '1',
      },
      expect.objectContaining({ query: expect.any(Function) }),
    );
    expect(context.pool.runState).toBe('PASSED');
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).resolves.toBe(false);
    expect(context.dataSources.createKafkaTopic).toHaveBeenCalledTimes(1);
  });
  it('prepares concurrently with only one connection per request while all remaining pool slots are occupied', async () => {
    const contexts = await Promise.all(
      Array.from({ length: 3 }, async () => {
        const context = service({ autoTopic: true });
        missingTarget(context);
        const run = await context.preflight.run(administrator, taskId, {
          expectedTaskRowVersion: '1',
          revisionId,
        });
        const query = context.pool.query;
        const client = { query, release: vi.fn() };
        // Three requests plus the controller lease consume max=4. Any pool query
        // or nested connect while holding these clients must fail this test.
        context.pool.query = vi.fn(async () => {
          throw new Error('pool exhausted');
        });
        context.pool.connect
          .mockReset()
          .mockResolvedValueOnce(client)
          .mockRejectedValue(new Error('nested connection'));
        vi.spyOn(context.preflight, 'run').mockImplementation(async () => {
          expect(client.release).toHaveBeenCalledOnce();
          return run;
        });
        vi.mocked(context.dataSources.get).mockImplementation(async (_actor, id, executor) => {
          expect(executor).toBe(client);
          return dataSource(id);
        });
        vi.mocked(context.dataSources.createKafkaTopic).mockImplementation(
          async (_actor, _id, _request, executor) => {
            expect(executor).toBe(client);
            return { topic: 'target.events', created: true };
          },
        );
        return context;
      }),
    );
    await expect(
      Promise.all(
        contexts.map(({ preflight }) =>
          preflight.prepareFirstStartTopic(administrator, taskId, {
            expectedTaskRowVersion: '1',
            revisionId,
          }),
        ),
      ),
    ).resolves.toEqual([true, true, true]);
    for (const context of contexts) {
      expect(context.pool.connect).toHaveBeenCalledOnce();
      expect(context.pool.query).not.toHaveBeenCalled();
    }
  });
  it('preserves external completion when the owning preparation transaction cannot commit', async () => {
    const context = service({ autoTopic: true });
    missingTarget(context);
    const request = { expectedTaskRowVersion: '1', revisionId };
    await context.preflight.run(administrator, taskId, request);
    const dispatch = context.pool.query.getMockImplementation()!;
    context.pool.query.mockImplementation(
      async (sql: string, parameters: readonly unknown[] = []) => {
        if (sql === 'COMMIT') throw new Error('commit failed');
        return dispatch(sql, parameters);
      },
    );
    const rerun = vi.spyOn(context.preflight, 'run');
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).rejects.toMatchObject({
      code: 'KAFKA_TOPIC_AUDIT_FAILED',
      result: { topic: 'target.events', created: true },
    });
    expect(rerun).not.toHaveBeenCalled();
    expect(context.pool.query).toHaveBeenLastCalledWith('ROLLBACK');
  });
  it.each(['unchecked', 'permission', 'version', 'history', 'source-failure'] as const)(
    'never creates for %s',
    async (reason) => {
      const context = service({ autoTopic: reason !== 'unchecked', history: reason === 'history' });
      missingTarget(context);
      const request = { expectedTaskRowVersion: '1', revisionId };
      await context.preflight.run(administrator, taskId, request);
      if (reason === 'source-failure')
        context.pool.runResult.checks.push({
          key: 'SOURCE_CONNECTION',
          state: 'FAILED',
          code: 'FAILED',
          summary: 'failed',
          details: {},
        });
      const actor =
        reason === 'permission' ? { ...administrator, role: 'DEVELOPER' as const } : administrator;
      const result = context.preflight.prepareFirstStartTopic(
        actor,
        taskId,
        reason === 'version' ? { ...request, expectedTaskRowVersion: '99' } : request,
      );
      if (['permission', 'version', 'history'].includes(reason))
        await expect(result).rejects.toBeInstanceOf(PreflightServiceError);
      else await expect(result).resolves.toBe(false);
      expect(context.dataSources.createKafkaTopic).not.toHaveBeenCalled();
    },
  );
  it('retains failed preparation and retries without assuming an externally created Topic was rolled back', async () => {
    const context = service({ autoTopic: true });
    const original = missingTarget(context);
    const request = { expectedTaskRowVersion: '1', revisionId };
    await context.preflight.run(administrator, taskId, request);
    vi.mocked(context.dataSources.createKafkaTopic).mockRejectedValueOnce(
      new Error('audit failed after external create'),
    );
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).rejects.toThrow('audit failed');
    expect(context.pool.runState).toBe('FAILED');
    vi.mocked(context.dataSources.createKafkaTopic).mockImplementation(async () => {
      vi.mocked(context.dataSources.metadata).mockImplementation(original);
      return { topic: 'target.events', created: false };
    });
    await expect(
      context.preflight.prepareFirstStartTopic(administrator, taskId, request),
    ).resolves.toBe(true);
    expect(context.pool.runState).toBe('PASSED');
  });
  it('preflights legacy Kafka with actual partitions and no Topic UUIDs', async () => {
    const context = service();
    vi.mocked(context.dataSources.metadata).mockImplementation(async (_actor, id, query) =>
      metadata(
        id,
        query.resource === 'PARTITIONS' ? 'PARTITIONS' : 'TOPICS',
        query.resource === 'PARTITIONS'
          ? [
              { attributes: {}, identity: null, name: '0' },
              { attributes: {}, identity: null, name: '1' },
            ]
          : [
              {
                attributes: { partitionCount: id === sourceId ? '2' : '1' },
                identity: null,
                name: id === sourceId ? 'source.events' : 'target.events',
              },
            ],
      ),
    );
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('PASSED');
    expect(run.result.identities).toMatchObject({ sourceTopic: null, targetTopic: null });
    expect(run.result.checks).toEqual(
      expect.arrayContaining([expect.objectContaining({ code: 'KAFKA_TOPIC_ID_UNAVAILABLE' })]),
    );
    expect(context.pool.insertedResources).toBe(0);
    await expect(
      context.preflight.revalidateContinuityBeforeRuntime({
        id: taskId,
        revisionId,
        rowVersion: '1',
        desiredState: 'RUNNING',
      }),
    ).resolves.toBeUndefined();
  });

  it('allows manual target topic when enumeration is unsupported and preserves real source capacity', async () => {
    const context = service();
    const original = vi.mocked(context.dataSources.metadata).getMockImplementation()!;
    vi.mocked(context.dataSources.metadata).mockImplementation(async (actor, id, query) =>
      id === targetId
        ? {
            attemptedAt: new Date().toISOString(),
            dataSourceId: id,
            errorCode: 'METADATA_UNSUPPORTED',
            errorSummary: 'Metadata unavailable',
            resource: 'TOPICS',
            state: 'ERROR',
          }
        : await original(actor, id, query),
    );
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('PASSED');
    expect(run.result.capacity.requested).toBe(2);
    expect(run.result.identities.targetTopic).toBeNull();
    expect(run.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'TARGET_METADATA', state: 'SKIPPED' }),
      ]),
    );
  });

  it('runs recovery preflight as SYSTEM while preserving the applied revision', async () => {
    const context = service({ systemRecovery: true });
    const oldRunId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a399';

    const run = await context.preflight.runForSystemRecovery({
      expectedOldRunId: oldRunId,
      expectedPlatformInstanceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a398',
      expectedTaskRowVersion: '1',
      revisionId,
      taskId,
    });

    expect(run.state).toBe('PASSED');
    expect(
      run.result.checks.find((item) => item.key === 'TOPIC_IDENTITIES')?.details,
    ).toMatchObject({
      topics: [
        { resourceType: 'KAFKA_SOURCE_TOPIC', topic: 'source.events', partitionIds: [0, 1] },
        { resourceType: 'KAFKA_TARGET_TOPIC', topic: 'target.events', partitionCount: 1 },
      ],
    });
    expect(context.dataSources.getForSystem).toHaveBeenCalledTimes(2);
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'CONFIGURATION',
        code: 'CONFIGURATION_VALID',
        state: 'PASSED',
      }),
    );
    expect(context.dataSources.testForSystem).toHaveBeenCalledTimes(2);
    expect(context.audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actorType: 'SYSTEM', actorUserId: null }),
    );
    expect(
      context.pool.queries.some(({ sql }) => sql.includes("SET state = 'PREFLIGHT_PASSED'")),
    ).toBe(false);
    const oldRunBindings = context.pool.queries.filter(({ sql }) =>
      sql.includes('ape_dts_platform.task_runs'),
    );
    expect(oldRunBindings).toHaveLength(3);
    expect(
      oldRunBindings.every(({ sql }) =>
        sql.includes("runs.status = 'BLOCKED' AND runs.current_stage = 'RECONCILIATION'"),
      ),
    ).toBe(true);
  });

  it.each([
    { fresh: true, held: 0, passed: true },
    { fresh: true, held: 1, passed: false },
    { fresh: true, held: 2, passed: false },
    { fresh: false, held: 0, passed: false },
    { fresh: false, held: 1, passed: false },
    { fresh: false, held: 2, passed: true },
  ])(
    'checks fresh and retained recovery capacity independently: %j',
    async ({ fresh, held, passed }) => {
      const context = service({ systemRecovery: true, heldCapacity: held });
      const run = await context.preflight.runForSystemRecovery({
        expectedOldRunId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a399',
        expectedTaskRowVersion: '1',
        revisionId,
        taskId,
        reserveFreshCapacity: fresh,
      });
      expect(run.state).toBe(passed ? 'PASSED' : 'FAILED');
      expect(run.result.checks.find((item) => item.key === 'CAPACITY')).toMatchObject({
        state: passed ? 'PASSED' : 'FAILED',
        code: passed ? 'CAPACITY_AVAILABLE' : 'CAPACITY_RESERVATION_MISMATCH',
      });
    },
  );
  it('keeps applied revisions unavailable to ordinary user preflight', async () => {
    const context = service({ systemRecovery: true });
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('FAILED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'CONFIGURATION',
        code: 'REVISION_NOT_READY',
        state: 'FAILED',
      }),
    );
    expect(
      context.pool.queries.some(({ sql }) => sql.includes("SET state = 'PREFLIGHT_PASSED'")),
    ).toBe(false);
  });

  it('still blocks system recovery when an external connection fails', async () => {
    const context = service({ systemRecovery: true });
    vi.mocked(context.dataSources.testForSystem).mockRejectedValue(new Error('connection failed'));
    const run = await context.preflight.runForSystemRecovery({
      expectedOldRunId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a399',
      expectedTaskRowVersion: '1',
      revisionId,
      taskId,
    });
    expect(run.state).toBe('FAILED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'CONFIGURATION',
        code: 'CONFIGURATION_VALID',
        state: 'PASSED',
      }),
    );
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({ key: 'SOURCE_CONNECTION', state: 'FAILED' }),
    );
  });
  it('preflights an existing uploaded binary without activation or certification', async () => {
    const context = service({ artifactState: 'INACTIVE' });
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('PASSED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'ARTIFACT',
        code: 'ARTIFACT_AVAILABLE',
        state: 'PASSED',
      }),
    );
  });

  it('still blocks quarantined uploaded binaries', async () => {
    const context = service({ artifactState: 'QUARANTINED' });
    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });
    expect(run.state).toBe('FAILED');
    expect(run.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'ARTIFACT',
        code: 'ARTIFACT_NOT_ACTIVE',
        state: 'FAILED',
      }),
    );
  });

  it('checks Kafka identities and partitions, records continuity, and advances the revision', async () => {
    const context = service();

    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });

    expect(run.state).toBe('PASSED');
    expect(
      run.result.checks.find((item) => item.key === 'TOPIC_IDENTITIES')?.details,
    ).toMatchObject({
      topics: [
        { resourceType: 'KAFKA_SOURCE_TOPIC', topic: 'source.events', partitionIds: [0, 1] },
        { resourceType: 'KAFKA_TARGET_TOPIC', topic: 'target.events', partitionCount: 1 },
      ],
    });
    expect(run.result.capacity).toMatchObject({ limit: 4, requested: 2, used: 2 });
    expect(run.result.identities).toMatchObject({
      source: sourceClusterId,
      sourceTopic: sourceTopicId,
      target: targetClusterId,
      targetTopic: targetTopicId,
    });
    expect(run.result.checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: 'ARTIFACT_AVAILABLE', key: 'ARTIFACT', state: 'PASSED' }),
        expect.objectContaining({
          code: 'CONTINUITY_MATCHED',
          key: 'TOPIC_IDENTITIES',
          state: 'PASSED',
        }),
      ]),
    );
    expect(context.pool.insertedResources).toBe(2);
  });

  it.each([false, true])(
    'preserves a blocking continuity decision and its evidence (SYSTEM: %s)',
    async (systemRecovery) => {
      const context = service({ systemRecovery });
      const details = {
        baselineRowVersion: '7',
        observationId: randomUUID(),
        risks: ['KAFKA_TARGET_TOPIC_RECREATED'],
      };
      context.continuity.decideWithin.mockResolvedValue({
        code: 'KAFKA_TARGET_TOPIC_RECREATED',
        state: 'FAILED',
        key: 'TOPIC_IDENTITIES',
        summary: 'Topic continuity requires confirmation',
        details,
      });
      const run = systemRecovery
        ? await context.preflight.runForSystemRecovery({
            expectedOldRunId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a399',
            expectedPlatformInstanceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a398',
            expectedTaskRowVersion: '1',
            revisionId,
            taskId,
          })
        : await context.preflight.run(administrator, taskId, {
            expectedTaskRowVersion: '1',
            revisionId,
          });
      expect(run.state).toBe('FAILED');
      expect(run.result.checks.find((item) => item.key === 'TOPIC_IDENTITIES')).toMatchObject({
        code: 'KAFKA_TARGET_TOPIC_RECREATED',
        state: 'FAILED',
        details: {
          ...details,
          topics: [
            { resourceType: 'KAFKA_SOURCE_TOPIC', topic: 'source.events', partitionIds: [0, 1] },
            { resourceType: 'KAFKA_TARGET_TOPIC', topic: 'target.events', partitionCount: 1 },
          ],
        },
      });
      expect(context.pool.insertedResources).toBe(0);
    },
  );

  it('marks a run STALE when the task row changes while external checks are running', async () => {
    const context = service({ staleAtFinalize: true });

    const run = await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });

    expect(run.state).toBe('STALE');
    expect(run.failureCode).toBe('PREFLIGHT_STALE');
    expect(run.result.checks.length).toBeGreaterThan(0);
    expect(context.pool.insertedResources).toBe(0);
  });

  it('rejects a second RUNNING preflight for the exact revision before external checks', async () => {
    const context = service({ runningRevisionId: revisionId });

    await expect(
      context.preflight.run(administrator, taskId, {
        expectedTaskRowVersion: '1',
        revisionId,
      }),
    ).rejects.toEqual(new PreflightServiceError('PREFLIGHT_ALREADY_RUNNING'));

    expect(context.dataSources.test).not.toHaveBeenCalled();
    expect(context.pool.queries.some(({ sql }) => sql.includes('INSERT INTO'))).toBe(false);
  });

  it('does not let a RUNNING preflight for another revision block this revision', async () => {
    const otherRevisionId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a399';
    const context = service({ runningRevisionId: otherRevisionId });

    await expect(
      context.preflight.run(administrator, taskId, {
        expectedTaskRowVersion: '1',
        revisionId,
      }),
    ).resolves.toMatchObject({ state: 'PASSED' });

    const runningQuery = context.pool.queries.find(({ sql }) =>
      sql.includes('FROM ape_dts_platform.preflight_runs'),
    );
    expect(runningQuery?.parameters).toEqual([revisionId]);
  });

  it('uses PostgreSQL defaults for start time and statement time for completion', async () => {
    const context = service();

    await context.preflight.run(administrator, taskId, {
      expectedTaskRowVersion: '1',
      revisionId,
    });

    const insert = context.pool.queries.find(({ sql }) =>
      sql.includes('INSERT INTO ape_dts_platform.preflight_runs'),
    );
    expect(insert?.sql).not.toContain('started_at');
    expect(insert?.parameters.some((value) => value instanceof Date)).toBe(false);
    const completion = context.pool.queries.find(({ sql }) =>
      sql.includes('UPDATE ape_dts_platform.preflight_runs'),
    );
    expect(completion?.sql).toContain('completed_at = statement_timestamp()');
    expect(completion?.parameters.some((value) => value instanceof Date)).toBe(false);
  });
});
