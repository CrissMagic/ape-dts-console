import type { UserSummary } from '@scs/contracts';
import type { PoolClient } from 'pg';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { KafkaDataSourceAdapter } from './adapters/kafka-data-source.adapter.js';
import { DataSourceAdapterRegistry } from './adapters/types.js';
import { DataSourcesService } from './data-sources.service.js';

const admin = vi.hoisted(() => ({
  connect: vi.fn(),
  disconnect: vi.fn(),
  createTopics: vi.fn(),
}));
vi.mock('kafkajs', () => ({
  Kafka: class {
    admin() {
      return admin;
    }
  },
}));

const actor: UserSummary = {
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
  username: 'admin',
  displayName: '管理员',
  role: 'ADMIN',
  status: 'ACTIVE',
};
const configuration = {
  type: 'KAFKA' as const,
  bootstrapServers: ['localhost:9092'],
  plaintextRiskAcknowledged: true as const,
};
const request = {
  topic: 'cdc.samples',
  numPartitions: 3,
  replicationFactor: 1,
  expectedRowVersion: '2',
};
const sourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5';

function fixture() {
  const source = {
    id: sourceId,
    source_type: 'KAFKA',
    configuration,
    connection_secret_id: null,
    row_version: '2',
    created_at: new Date('2026-09-29T00:00:00Z'),
    updated_at: new Date('2026-09-29T00:00:00Z'),
    deleted_at: null,
    normalized_name: 'Kafka source',
    state: 'ACTIVE',
  };
  const query = vi.fn(async (sql: string) => ({
    rows: sql.includes('FROM ape_dts_platform.data_sources') ? [source] : [],
    rowCount: 1,
  }));
  const release = vi.fn();
  const pool = { connect: vi.fn(async () => ({ query, release })) };
  const append = vi.fn(async () => '1');
  const service = new DataSourcesService(
    { requirePool: () => pool } as unknown as PlatformDatabaseService,
    { append } as unknown as AuditLedgerService,
    {} as UnlockedKeyService,
    new DataSourceAdapterRegistry([new KafkaDataSourceAdapter()]),
  );
  return { service, query, release, append, pool };
}

beforeEach(() => {
  vi.clearAllMocks();
  admin.connect.mockResolvedValue(undefined);
  admin.disconnect.mockResolvedValue(undefined);
  admin.createTopics.mockResolvedValue(true);
});

describe('Kafka topic creation business operation', () => {
  it.each(['ADMIN', 'DEVELOPER'] as const)(
    'reads source access on the existing connection for %s',
    async (role) => {
      const f = fixture();
      f.pool.connect.mockRejectedValue(new Error('all other connections are occupied'));
      const client = { query: f.query, release: f.release } as unknown as PoolClient;
      await expect(f.service.get({ ...actor, role }, sourceId, client)).resolves.toMatchObject({
        id: sourceId,
        rowVersion: '2',
        state: 'ACTIVE',
      });
      expect(f.pool.connect).not.toHaveBeenCalled();
      expect(f.query).toHaveBeenCalledOnce();
      expect(f.release).not.toHaveBeenCalled();
    },
  );
  it('reuses the caller transaction without acquiring or committing another connection', async () => {
    const f = fixture();
    f.pool.connect.mockRejectedValue(new Error('all other connections are occupied'));
    const client = { query: f.query, release: f.release } as unknown as PoolClient;
    await expect(f.service.createKafkaTopic(actor, sourceId, request, client)).resolves.toEqual({
      topic: request.topic,
      created: true,
    });
    expect(f.pool.connect).not.toHaveBeenCalled();
    expect(f.append).toHaveBeenCalledWith(
      client,
      expect.objectContaining({ action: 'KAFKA_TOPIC_CREATED' }),
    );
    expect(f.query.mock.calls.some(([sql]) => ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql))).toBe(
      false,
    );
    expect(f.release).not.toHaveBeenCalled();
  });
  it('creates on the selected locked source and records a USER audit', async () => {
    const f = fixture();
    await expect(f.service.createKafkaTopic(actor, sourceId, request)).resolves.toEqual({
      topic: request.topic,
      created: true,
    });
    expect(f.query).toHaveBeenCalledWith(
      expect.stringMatching(/state = 'ACTIVE'[\s\S]*deleted_at IS NULL FOR UPDATE/u),
      [sourceId],
    );
    expect(admin.createTopics).toHaveBeenCalledWith({
      topics: [{ topic: request.topic, numPartitions: 3, replicationFactor: 1 }],
      waitForLeaders: false,
      timeout: 10000,
    });
    expect(f.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        actorType: 'USER',
        actorUserId: actor.id,
        action: 'KAFKA_TOPIC_CREATED',
      }),
    );
    expect(f.query).toHaveBeenLastCalledWith('COMMIT');
    expect(f.release).toHaveBeenCalled();
  });

  it('does not alter existing topics and records the no-op outcome', async () => {
    admin.createTopics.mockResolvedValue(false);
    const f = fixture();
    await expect(f.service.createKafkaTopic(actor, sourceId, request)).resolves.toEqual({
      topic: request.topic,
      created: false,
    });
    expect(f.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ action: 'KAFKA_TOPIC_ALREADY_EXISTS' }),
    );
  });

  it('refuses stale source versions before any external mutation', async () => {
    const f = fixture();
    await expect(
      f.service.createKafkaTopic(actor, sourceId, { ...request, expectedRowVersion: '1' }),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_VERSION_CONFLICT' });
    expect(admin.connect).not.toHaveBeenCalled();
  });

  it('refuses non-administrators before acquiring a source', async () => {
    const f = fixture();
    await expect(
      f.service.createKafkaTopic({ ...actor, role: 'DEVELOPER' }, sourceId, request),
    ).rejects.toMatchObject({ code: 'DATA_SOURCE_ACCESS_DENIED' });
    expect(f.query).not.toHaveBeenCalled();
  });

  it.each([
    { topic: '.' },
    { topic: '..' },
    { topic: 'bad/topic' },
    { numPartitions: 0 },
    { numPartitions: 1.5 },
    { replicationFactor: -1 },
  ])('rejects invalid parameters before connecting: %j', async (invalid) => {
    const f = fixture();
    await expect(
      f.service.createKafkaTopic(actor, sourceId, { ...request, ...invalid }),
    ).rejects.toThrow();
    expect(admin.connect).not.toHaveBeenCalled();
  });

  it('reports external completion explicitly when audit persistence fails', async () => {
    const f = fixture();
    f.append.mockRejectedValue(new Error('audit failed'));
    await expect(f.service.createKafkaTopic(actor, sourceId, request)).rejects.toMatchObject({
      code: 'KAFKA_TOPIC_AUDIT_FAILED',
      result: { topic: request.topic, created: true },
    });
    expect(f.query).toHaveBeenLastCalledWith('ROLLBACK');
  });

  it('handles a concurrent create without metadata or topic identity', async () => {
    admin.createTopics.mockRejectedValue({ errors: [{ type: 'TOPIC_ALREADY_EXISTS' }] });
    await expect(fixture().service.createKafkaTopic(actor, sourceId, request)).resolves.toEqual({
      topic: request.topic,
      created: false,
    });
  });

  it.each([
    ['TOPIC_AUTHORIZATION_FAILED', 'PERMISSION_DENIED'],
    ['INVALID_REPLICATION_FACTOR', 'KAFKA_TOPIC_PARAMETERS_INVALID'],
    ['REQUEST_TIMED_OUT', 'TIMEOUT'],
  ])('reports broker error %s without interpreting it as missing', async (type, code) => {
    admin.createTopics.mockRejectedValue({ errors: [{ type }] });
    const f = fixture();
    await expect(f.service.createKafkaTopic(actor, sourceId, request)).rejects.toMatchObject({
      code,
    });
    expect(f.append).not.toHaveBeenCalled();
    expect(admin.disconnect).toHaveBeenCalled();
  });
});
