import { describe, expect, it, vi } from 'vitest';
import type { PoolClient } from 'pg';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { DataSourcesService } from '../data-sources/data-sources.service.js';
import type { CapacityReservationService } from '../pg-resources/capacity-reservation.service.js';
import type { CertifiedAdapterRegistry } from '@scs/config-model';
import { ContinuityService, type ContinuityObservation } from './continuity.service.js';
import { PreflightService, PreflightServiceError } from './preflight.service.js';

const baseline = {
  baseline_kind: 'KAFKA',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f1',
  pg_confirmed_flush_lsn: null,
  pg_database_name: null,
  pg_database_oid: null,
  pg_slot_name: null,
  pg_slot_plugin: null,
  pg_system_identifier: null,
  row_version: '4',
  source_cluster_id: 'source-cluster',
  source_partition_ids: [0, 1],
  source_topic_id: 'source-topic-id',
  target_cluster_id: 'target-cluster',
  target_partition_count: 2,
  target_topic_id: 'target-topic-id',
  task_revision_id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f2',
};

function kafka(
  partitions: number[],
  targetTopicId = 'target-topic-id',
): Extract<ContinuityObservation, { kind: 'KAFKA' }> {
  return {
    kind: 'KAFKA',
    source: {
      clusterId: 'source-cluster',
      partitionCount: partitions.length,
      partitionIds: partitions,
      topicId: 'source-topic-id',
    },
    target: {
      clusterId: 'target-cluster',
      partitionCount: 2,
      partitionIds: [],
      topicId: targetTopicId,
    },
  };
}

function clientFor(
  row: object | null,
  options: { history?: boolean; monotonic?: boolean; priorTopics?: unknown } = {},
) {
  const queries: string[] = [];
  const query = vi.fn(async (sql: string) => {
    queries.push(sql.replace(/\s+/gu, ' ').trim());
    if (sql.trimStart().startsWith('UPDATE ape_dts_platform.tasks'))
      return { rows: [], rowCount: 1 };
    if (sql.includes('FROM ape_dts_platform.task_continuity_baselines'))
      return { rows: row === null ? [] : [row], rowCount: row === null ? 0 : 1 };
    if (sql.includes('FROM ape_dts_platform.preflight_runs'))
      return { rows: [{ topics: options.priorTopics }], rowCount: 1 };
    if (sql.includes('SELECT EXISTS'))
      return { rows: [{ exists: options.history === true }], rowCount: 1 };
    if (sql.includes('::pg_lsn >='))
      return { rows: [{ monotonic: options.monotonic === true }], rowCount: 1 };
    return { rows: [], rowCount: 1 };
  });
  return { client: { query } as unknown as PoolClient, queries };
}

function service(): ContinuityService {
  return new ContinuityService(
    {} as PlatformDatabaseService,
    { append: vi.fn() } as unknown as AuditLedgerService,
  );
}

describe('ContinuityService decision gate', () => {
  it('keeps MySQL instance and database identity separate from PostgreSQL slots', async () => {
    const observed = {
      kind: 'MYSQL' as const,
      sourceIdentity: 'mysql-uuid:demo',
      target: kafka([0]).target,
    };
    const fake = clientFor({
      ...baseline,
      baseline_kind: 'MYSQL',
      mysql_source_identity: 'mysql-uuid:demo',
      source_cluster_id: null,
      source_partition_ids: null,
    });
    expect(
      await service().decideWithin(
        fake.client,
        { type: 'SYSTEM' },
        crypto.randomUUID(),
        crypto.randomUUID(),
        '7',
        'RUNNING',
        observed,
      ),
    ).toMatchObject({ state: 'PASSED' });
    expect(
      await service().decideWithin(
        fake.client,
        { type: 'SYSTEM' },
        crypto.randomUUID(),
        crypto.randomUUID(),
        '7',
        'RUNNING',
        { ...observed, sourceIdentity: 'other:demo' },
      ),
    ).toMatchObject({ state: 'FAILED', code: 'MYSQL_SOURCE_IDENTITY_CHANGED' });
    expect(fake.queries.some((sql) => sql.includes('::pg_lsn >='))).toBe(false);
  });

  it('creates a MySQL baseline on the first user preflight without a Binlog checkpoint', async () => {
    const fake = clientFor(null);
    await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '1',
      'STOPPED',
      { kind: 'MYSQL', sourceIdentity: 'uuid:demo', target: kafka([0]).target },
    );
    expect(
      fake.queries.some((sql) => sql.includes("'MYSQL'") && sql.includes('mysql_source_identity')),
    ).toBe(true);
    expect(fake.queries.some((sql) => sql.includes('mysql_checkpoints'))).toBe(false);
  });
  function snapshots() {
    return [
      { ...kafka([0]).source, topic: 'first', resourceType: 'KAFKA_SOURCE_TOPIC' as const },
      {
        ...kafka([0]).source,
        topic: 'second',
        topicId: 'second-id',
        resourceType: 'KAFKA_SOURCE_TOPIC' as const,
      },
      { ...kafka([0]).target, topic: 'sink', resourceType: 'KAFKA_TARGET_TOPIC' as const },
    ];
  }

  it('compares every source snapshot and blocks recreation of a non-first source', async () => {
    const topics = snapshots();
    const fake = clientFor({ ...baseline, source_partition_ids: [0], topic_snapshots: topics });
    const changed = topics.map((topic) =>
      topic.topic === 'second' ? { ...topic, topicId: 'rebuilt' } : topic,
    );
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      { ...kafka([0]), topics: changed },
    );
    expect(result).toMatchObject({ state: 'FAILED', code: 'KAFKA_SOURCE_TOPIC_ID_CHANGED' });
  });

  it('treats reordered topic mappings as the same continuity baseline', async () => {
    const topics = snapshots();
    const fake = clientFor({ ...baseline, source_partition_ids: [0], topic_snapshots: topics });
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      { ...kafka([0]), topics: [...topics].reverse() },
    );
    expect(result.state).toBe('PASSED');
  });

  it('requires explicit acceptance when an existing mapping set changes', async () => {
    const topics = snapshots();
    const fake = clientFor({ ...baseline, source_partition_ids: [0], topic_snapshots: topics });
    const result = await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'STOPPED',
      { ...kafka([0]), topics: topics.slice(1) },
    );
    expect(result).toMatchObject({ state: 'FAILED', code: 'KAFKA_TOPIC_MAPPING_CHANGED' });
  });

  it('upgrades a legacy draft baseline from its saved preflight even after that draft becomes v2', async () => {
    const all = snapshots();
    const topics = [all[0]!, all[2]!];
    const fake = clientFor(
      { ...baseline, source_partition_ids: [0], topic_snapshots: null },
      { priorTopics: topics },
    );
    const result = await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'STOPPED',
      { ...kafka([0]), topics },
    );
    expect(result.state).toBe('PASSED');
    expect(fake.queries.some((sql) => sql.includes('topic_snapshots = COALESCE'))).toBe(true);
    expect(fake.queries.some((sql) => sql.includes('SELECT configuration FROM'))).toBe(false);
  });

  it('keeps known topic identity when accepting another topic partition change with incomplete IDs', async () => {
    const topics = snapshots();
    const taskId = crypto.randomUUID();
    const observationId = crypto.randomUUID();
    const observation = {
      ...kafka([0, 1]),
      topics: topics.map((item) =>
        item.topic === 'second'
          ? { ...item, partitionIds: [0, 1], partitionCount: 2 }
          : { ...item, topicId: null, partitionCount: null },
      ),
    };
    const query = vi.fn(async (sql: string, _parameters: readonly unknown[] = []) => {
      void _parameters;
      if (sql.includes('SELECT id, task_id, task_revision_id, baseline_id, observation, decision'))
        return {
          rows: [
            {
              id: observationId,
              task_id: taskId,
              task_revision_id: baseline.task_revision_id,
              baseline_id: baseline.id,
              decision: 'MANUAL_REQUIRED',
              observation,
            },
          ],
          rowCount: 1,
        };
      if (sql.includes('FROM ape_dts_platform.task_continuity_baselines'))
        return { rows: [{ ...baseline, topic_snapshots: topics }], rowCount: 1 };
      if (sql.includes('SELECT desired_state, row_version'))
        return { rows: [{ desired_state: 'STOPPED', row_version: '7' }], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    });
    const client = { query, release: vi.fn() };
    const continuity = new ContinuityService(
      {
        requirePool: () => ({ connect: async () => client }),
      } as unknown as PlatformDatabaseService,
      { append: vi.fn() } as unknown as AuditLedgerService,
    );
    await continuity.accept(
      {
        id: crypto.randomUUID(),
        username: 'admin',
        displayName: 'admin',
        role: 'ADMIN',
        status: 'ACTIVE',
      },
      taskId,
      {
        expectedBaselineRowVersion: '4',
        expectedTaskRowVersion: '7',
        observationId,
        reason: 'Accept partition growth',
      },
    );
    const update = query.mock.calls.find(([sql]) =>
      sql.includes('SET task_revision_id=$2::uuid, source_partition_ids='),
    );
    const parameters = update?.[1];
    const saved = JSON.parse(String(parameters?.[7])) as {
      topic: string;
      topicId: string | null;
      partitionCount: number | null;
      partitionIds: number[];
    }[];
    expect(saved.find((item) => item.topic === 'first')).toMatchObject({
      topicId: 'source-topic-id',
      partitionCount: 1,
    });
    expect(saved.find((item) => item.topic === 'second')).toMatchObject({
      topicId: 'second-id',
      partitionIds: [0, 1],
    });
    expect(saved.find((item) => item.topic === 'sink')).toMatchObject({
      topicId: 'target-topic-id',
      partitionCount: 2,
    });
    const next = clientFor({ ...baseline, topic_snapshots: saved });
    const result = await service().decideWithin(
      next.client,
      { type: 'SYSTEM' },
      taskId,
      baseline.task_revision_id,
      '8',
      'RUNNING',
      {
        ...kafka([0]),
        topics: topics.map((item) =>
          item.topic === 'first' ? { ...item, topicId: 'rebuilt' } : item,
        ),
      },
    );
    expect(result).toMatchObject({ state: 'FAILED', code: 'KAFKA_SOURCE_TOPIC_ID_CHANGED' });
  });

  it('allows legacy Kafka with unavailable Topic UUIDs without fabricating or comparing identities', async () => {
    const fake = clientFor({ ...baseline, source_topic_id: null, target_topic_id: null });
    const observation = kafka([0, 1]);
    if (observation.kind !== 'KAFKA') throw new Error('expected Kafka');
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      {
        ...observation,
        source: { ...observation.source, topicId: null },
        target: { ...observation.target, topicId: null },
      },
    );
    expect(result.state).toBe('PASSED');
  });

  it('still blocks source partition loss when Topic UUIDs are unavailable', async () => {
    const fake = clientFor({ ...baseline, source_topic_id: null, target_topic_id: null });
    const observation = kafka([0]);
    if (observation.kind !== 'KAFKA') throw new Error('expected Kafka');
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      {
        ...observation,
        source: { ...observation.source, topicId: null },
        target: { ...observation.target, topicId: null },
      },
    );
    expect(result).toMatchObject({ state: 'FAILED', code: 'KAFKA_SOURCE_PARTITION_SET_CHANGED' });
  });

  it('requires manual acceptance only for a strict source partition superset', async () => {
    const fake = clientFor(baseline);
    const result = await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'STOPPED',
      kafka([0, 1, 2]),
    );
    expect(result).toMatchObject({ code: 'KAFKA_SOURCE_PARTITIONS_ADDED', state: 'FAILED' });
    expect(fake.queries.some((sql) => sql.includes('recovery_disposition = $5'))).toBe(true);
  });

  it('permanently blocks a non-superset source partition change', async () => {
    const fake = clientFor(baseline);
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      kafka([0, 2]),
    );
    expect(result).toMatchObject({ code: 'KAFKA_SOURCE_PARTITION_SET_CHANGED', state: 'FAILED' });
  });

  it('requires manual acceptance for a same-cluster target recreation', async () => {
    const fake = clientFor(baseline);
    const result = await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'STOPPED',
      kafka([0, 1], 'rebuilt-topic-id'),
    );
    expect(result).toMatchObject({ code: 'KAFKA_TARGET_TOPIC_RECREATED', state: 'FAILED' });
  });

  it('binds permanent gate updates to task revision, row versions and desired state', async () => {
    const fake = clientFor({ ...baseline, source_cluster_id: 'old-cluster' });
    await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      kafka([0, 1]),
    );
    const update = fake.queries.find((sql) => sql.startsWith('UPDATE ape_dts_platform.tasks'));
    expect(update).toContain('task.row_version = $3::bigint');
    expect(update).toContain('baseline.row_version = $4::bigint');
    expect(update).toContain('task.desired_state = $7');
  });

  it('permanently blocks a historical task whose baseline is unavailable', async () => {
    const fake = clientFor(null, { history: true });
    const result = await service().decideWithin(
      fake.client,
      { type: 'USER', userId: crypto.randomUUID() },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'STOPPED',
      kafka([0, 1]),
    );
    expect(result).toMatchObject({ code: 'BASELINE_UNAVAILABLE', state: 'FAILED' });
  });

  it('permanently blocks PostgreSQL confirmed LSN regression', async () => {
    const pgBaseline = {
      ...baseline,
      baseline_kind: 'POSTGRES',
      source_cluster_id: null,
      source_partition_ids: null,
      source_topic_id: null,
      pg_system_identifier: 'sys',
      pg_database_name: 'db',
      pg_database_oid: '42',
      pg_slot_name: 'slot',
      pg_slot_plugin: 'pgoutput',
      pg_confirmed_flush_lsn: '0/20',
    };
    const fake = clientFor(pgBaseline, { monotonic: false });
    const result = await service().decideWithin(
      fake.client,
      { type: 'SYSTEM' },
      crypto.randomUUID(),
      crypto.randomUUID(),
      '7',
      'RUNNING',
      {
        kind: 'POSTGRES',
        source: {
          confirmedFlushLsn: '0/10',
          databaseName: 'db',
          databaseOid: '42',
          slotActive: false,
          slotName: 'slot',
          slotPlugin: 'pgoutput',
          systemIdentifier: 'sys',
        },
        target: {
          clusterId: 'target-cluster',
          partitionCount: 2,
          partitionIds: [],
          topicId: 'target-topic-id',
        },
      },
    );
    expect(result).toMatchObject({ code: 'PG_CONFIRMED_FLUSH_LSN_REGRESSED', state: 'FAILED' });
  });
});

describe('runtime continuity transaction boundary', () => {
  it('commits a blocked continuity decision before reporting runtime rejection', async () => {
    const statements: string[] = [];
    const transactionClient = {
      query: vi.fn(async (sql: string) => {
        statements.push(sql);
        return { rows: [], rowCount: null };
      }),
      release: vi.fn(),
    } as unknown as PoolClient;
    const pool = {
      connect: vi.fn(async () => transactionClient),
      query: vi.fn(async () => ({
        rows: [
          {
            configuration: {
              kind: 'KAFKA_RAW_FORWARD',
              schemaVersion: 1,
              source: { dataSourceId: crypto.randomUUID(), topic: 'source', groupId: 'group' },
              target: { dataSourceId: crypto.randomUUID(), topic: 'target' },
              tuning: {
                ackIntervalSeconds: 5,
                batchSize: 100,
                bufferSize: 16_000,
                checkpointIntervalSeconds: 15,
                requiredAcks: 'all',
              },
            },
          },
        ],
      })),
    };
    const database = { requirePool: () => pool } as unknown as PlatformDatabaseService;
    const preflight = new PreflightService(
      database,
      {} as AuditLedgerService,
      {
        testForSystem: vi.fn(async () => ({
          identity: { kind: 'KAFKA_CLUSTER', value: 'cluster' },
        })),
        metadataForSystem: vi.fn(async (_id, query) => ({
          state: 'SUCCESS',
          items:
            query.resource === 'PARTITIONS'
              ? [{ name: '0', identity: null, attributes: {} }]
              : ['source', 'target'].map((name) => ({
                  name,
                  identity: null,
                  attributes: { partitionCount: '1' },
                })),
        })),
      } as unknown as DataSourcesService,
      {} as CapacityReservationService,
      {} as CertifiedAdapterRegistry,
      { decideWithin: vi.fn(async () => ({ state: 'FAILED' })) } as unknown as ContinuityService,
      '0.0.0',
      {
        inspect: async () => {
          throw new Error('Not used by continuity revalidation');
        },
      },
    );
    await expect(
      preflight.revalidateContinuityBeforeRuntime({
        desiredState: 'STOPPED',
        id: crypto.randomUUID(),
        revisionId: crypto.randomUUID(),
        rowVersion: '9',
      }),
    ).rejects.toBeInstanceOf(PreflightServiceError);
    expect(statements).toEqual(['BEGIN', 'COMMIT']);
  });
});
