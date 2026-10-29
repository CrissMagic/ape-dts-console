import { describe, expect, it, vi } from 'vitest';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import {
  DataSourcesServiceError,
  type DataSourcesService,
} from '../data-sources/data-sources.service.js';
import { TaskProgressService } from './task-progress.service.js';

const applied = {
  id: '11111111-1111-4111-8111-111111111111',
  current_applied_revision_id: '22222222-2222-4222-8222-222222222222',
  source_version: '1',
  source_row_version: '1',
  run_id: '55555555-5555-4555-8555-555555555555',
  current_stage: 'CDC',
  configuration: {
    kind: 'PG_TO_KAFKA',
    schemaVersion: 1,
    source: {
      dataSourceId: '33333333-3333-4333-8333-333333333333',
      slotName: 'actual_slot',
      publicationName: 'actual_pub',
      resourceMode: 'SELF_MANAGED',
      includeSnapshot: false,
      table: { name: 'events', schema: 'public' },
    },
    target: { dataSourceId: '44444444-4444-4444-8444-444444444444', topic: 'events' },
    tuning: {
      batchSize: 200,
      bufferSize: 1000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all',
    },
  },
};
function fixture() {
  const query = vi.fn().mockResolvedValue({ rows: [applied] });
  const readProgressForSystem = vi.fn().mockResolvedValue({
    slotStatus: 'INACTIVE',
    unconfirmedWalBytes: '9007199254740999',
    retainedWalBytes: null,
    offsetLag: null,
    replicationLagMilliseconds: 1234,
  });
  const service = new TaskProgressService(
    { requirePool: () => ({ query }) } as unknown as PlatformDatabaseService,
    { readProgressForSystem } as unknown as DataSourcesService,
  );
  return { service, query, readProgressForSystem };
}
function kafkaTask() {
  return {
    ...applied,
    configuration: {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 2,
      source: { dataSourceId: applied.configuration.source.dataSourceId, groupId: 'task-group' },
      target: { dataSourceId: applied.configuration.target.dataSourceId },
      mappings: [
        { source: 'events', targets: [{ topic: 'events-copy' }, { topic: 'archive' }] },
        { source: 'orders', targets: [{ topic: 'orders-copy' }] },
      ],
      tuning: { ...applied.configuration.tuning, ackIntervalSeconds: 1 },
    },
  };
}
describe('task progress collection', () => {
  it('reports only the bound MySQL Binlog checkpoint and keeps source lag unknown', async () => {
    const { service, query, readProgressForSystem } = fixture();
    const task = {
      ...applied,
      configuration: {
        kind: 'MYSQL_TO_KAFKA',
        schemaVersion: 2,
        source: { dataSourceId: applied.configuration.source.dataSourceId, includeSnapshot: true },
        target: { dataSourceId: applied.configuration.target.dataSourceId },
        mappings: [{ source: { schema: 'demo', name: 'orders' }, targets: [{ topic: 'orders' }] }],
        tuning: applied.configuration.tuning,
      },
    };
    const checkpointAt = new Date('2026-10-16T01:00:00.000Z');
    query.mockImplementation(async (sql: string) => ({
      rows: sql.includes('mysql_task_checkpoints')
        ? [
            {
              binlog_filename: 'mysql-bin.000003',
              binlog_position: '12345',
              checkpoint_at: checkpointAt,
            },
          ]
        : [task],
    }));
    await service.collect();
    expect(readProgressForSystem).not.toHaveBeenCalled();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'AVAILABLE',
        sourceType: 'MYSQL',
        mysqlBinlogFilename: 'mysql-bin.000003',
        mysqlBinlogPosition: '12345',
        mysqlCheckpointAt: checkpointAt.toISOString(),
        offsetLag: null,
        replicationLagMilliseconds: null,
        timeDelayMilliseconds: null,
        slotStatus: null,
      }),
    ]);
    const call = query.mock.calls.find(([sql]) => String(sql).includes('mysql_task_checkpoints'));
    expect(call?.[1]).toEqual([task.id, task.current_applied_revision_id, task.run_id, '1']);
    query.mockImplementation(async (sql: string) => ({
      rows: sql.includes('mysql_task_checkpoints') ? [] : [task],
    }));
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'UNAVAILABLE',
        mysqlBinlogFilename: null,
        mysqlBinlogPosition: null,
      }),
    ]);
  });
  it('sums lag from all source topics exactly without counting fanout destinations', async () => {
    const { service, query, readProgressForSystem } = fixture();
    query.mockResolvedValue({ rows: [kafkaTask()] });
    readProgressForSystem.mockResolvedValueOnce({
      slotStatus: null,
      unconfirmedWalBytes: null,
      retainedWalBytes: null,
      offsetLag: '9007199254740999',
    });
    readProgressForSystem.mockResolvedValueOnce({
      slotStatus: null,
      unconfirmedWalBytes: null,
      retainedWalBytes: null,
      offsetLag: '8',
    });
    await service.collect();
    expect(readProgressForSystem.mock.calls.map(([, request]) => request)).toEqual([
      { topic: 'events', groupId: 'task-group' },
      { topic: 'orders', groupId: 'task-group' },
    ]);
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'AVAILABLE',
        sourceType: 'KAFKA',
        offsetLag: '9007199254741007',
      }),
    ]);
  });

  it('does not report partial topic lag as a complete healthy sample after a failed read', async () => {
    const { service, query, readProgressForSystem } = fixture();
    query.mockResolvedValue({ rows: [kafkaTask()] });
    readProgressForSystem.mockResolvedValueOnce({ offsetLag: '0' });
    readProgressForSystem.mockRejectedValueOnce(new Error('second topic unavailable'));
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'READ_FAILED', offsetLag: null }),
    ]);
  });

  it('keeps legacy single-topic Kafka collection compatible', async () => {
    const { service, query, readProgressForSystem } = fixture();
    const task = kafkaTask();
    const { mappings, ...configuration } = task.configuration;
    query.mockResolvedValue({
      rows: [
        {
          ...task,
          configuration: {
            ...configuration,
            schemaVersion: 1,
            source: { ...configuration.source, topic: mappings[0]!.source },
            target: { ...configuration.target, topic: mappings[0]!.targets[0]!.topic },
          },
        },
      ],
    });
    readProgressForSystem.mockResolvedValueOnce({ offsetLag: '17' });
    await service.collect();
    expect(readProgressForSystem).toHaveBeenCalledExactlyOnceWith(
      applied.configuration.source.dataSourceId,
      { topic: 'events', groupId: 'task-group' },
      '1',
    );
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'AVAILABLE', offsetLag: '17' }),
    ]);
  });

  it('makes a multi-topic sample unavailable when the source changes before a later read', async () => {
    const { service, query, readProgressForSystem } = fixture();
    query.mockResolvedValue({ rows: [kafkaTask()] });
    readProgressForSystem.mockResolvedValueOnce({ offsetLag: '0' });
    readProgressForSystem.mockRejectedValueOnce(
      new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT'),
    );
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE', offsetLag: null }),
    ]);
  });

  it('keeps total lag unknown when any source topic has no committed offset', async () => {
    const { service, query, readProgressForSystem } = fixture();
    query.mockResolvedValue({ rows: [kafkaTask()] });
    readProgressForSystem.mockResolvedValueOnce({ offsetLag: '8' });
    readProgressForSystem.mockResolvedValueOnce({ offsetLag: null });
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'AVAILABLE', offsetLag: null }),
    ]);
  });

  it('reads the applied slot, keeps exact WAL bytes and does not invent time delay', async () => {
    const { service, query, readProgressForSystem } = fixture();
    await service.collect();
    expect(query.mock.calls[0]?.[0]).toContain("task.observed_state='RUNNING'");
    expect(query.mock.calls[0]?.[0]).toContain('revision.id=task.current_applied_revision_id');
    expect(readProgressForSystem).toHaveBeenCalledWith(
      applied.configuration.source.dataSourceId,
      {
        slotName: 'actual_slot',
      },
      '1',
    );
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        slotStatus: 'INACTIVE',
        status: 'AVAILABLE',
        unconfirmedWalBytes: '9007199254740999',
        retainedWalBytes: null,
        timeDelayMilliseconds: null,
        timeDelayStatus: 'UNSUPPORTED',
        currentStage: 'CDC',
        replicationLagMilliseconds: 1234,
      }),
    ]);
  });
  it('replaces successful values with unknown on read failure, and clears removed task samples', async () => {
    const { service, query, readProgressForSystem } = fixture();
    await service.collect();
    readProgressForSystem.mockRejectedValueOnce(new Error('permission denied'));
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'READ_FAILED',
        slotStatus: null,
        unconfirmedWalBytes: null,
      }),
    ]);
    query.mockResolvedValueOnce({ rows: [] });
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([]);
  });

  it('continues querying the applied PostgreSQL slot using its last run binding after stopping', async () => {
    const { service, query, readProgressForSystem } = fixture();
    const stopped = { ...applied, observed_state: 'STOPPED', current_stage: 'SHUTDOWN' };
    query.mockResolvedValue({ rows: [stopped] });
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'AVAILABLE',
        sourceType: 'POSTGRES',
        slotStatus: 'INACTIVE',
        unconfirmedWalBytes: '9007199254740999',
      }),
    ]);
    expect(readProgressForSystem).toHaveBeenCalledExactlyOnceWith(
      applied.configuration.source.dataSourceId,
      { slotName: 'actual_slot' },
      '1',
    );
    query.mockResolvedValueOnce({ rows: [{ ...stopped, source_row_version: '2' }] });
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE' }),
    ]);
  });
  it('does not expose samples for another applied revision or an unauthorized task', async () => {
    const { service, query } = fixture();
    await service.collect();
    query.mockResolvedValueOnce({ rows: [] });
    const actor: Parameters<TaskProgressService['list']>[0] = {
      id: applied.id,
      role: 'DEVELOPER',
      status: 'ACTIVE',
      displayName: 'dev',
      username: 'dev',
    };
    expect(await service.list(actor)).toEqual([]);
    query.mockResolvedValueOnce({ rows: [{ ...applied, current_applied_revision_id: 'other' }] });
    expect(await service.list(actor)).toEqual([]);
    expect(query.mock.calls[1]?.[0]).toContain('grant_row.revoked_at IS NULL');
  });
  it('makes stale samples unavailable to monitoring without reconnecting to sources', async () => {
    const { service, readProgressForSystem } = fixture();
    vi.useFakeTimers();
    try {
      await service.collect();
      vi.advanceTimersByTime(60_001);
      expect(await service.listForMonitoring()).toEqual([
        expect.objectContaining({ status: 'UNAVAILABLE' }),
      ]);
      expect(readProgressForSystem).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
  it('does not use later preflight or a changed source to recover a cached alarm', async () => {
    const { service, query, readProgressForSystem } = fixture();
    await service.collect();
    query.mockResolvedValueOnce({ rows: [{ ...applied, source_row_version: '2' }] });
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE' }),
    ]);
    expect(readProgressForSystem).toHaveBeenCalledTimes(1);
  });
  it('marks missing startup source-version evidence unavailable without reading any source', async () => {
    const { service, query, readProgressForSystem } = fixture();
    query.mockResolvedValueOnce({ rows: [{ ...applied, source_version: null }] });
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE' }),
    ]);
    expect(readProgressForSystem).not.toHaveBeenCalled();
  });
  it('clears prior source facts after the same-read version guard rejects collection', async () => {
    const { service, readProgressForSystem } = fixture();
    await service.collect();
    readProgressForSystem.mockRejectedValueOnce(
      new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT'),
    );
    await service.collect();
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({
        status: 'UNAVAILABLE',
        slotStatus: null,
        unconfirmedWalBytes: null,
      }),
    ]);
  });
  it('does not reuse a healthy cache after the same revision restarts in another run', async () => {
    const { service, query } = fixture();
    await service.collect();
    query.mockResolvedValueOnce({ rows: [{ ...applied, run_id: 'new-run' }] });
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE' }),
    ]);
  });
  it('keeps an old in-flight read bound to its original run after completion', async () => {
    const { service, query, readProgressForSystem } = fixture();
    let complete!: (value: {
      slotStatus: string;
      unconfirmedWalBytes: null;
      retainedWalBytes: null;
      offsetLag: null;
    }) => void;
    readProgressForSystem.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const collection = service.collect();
    await vi.waitFor(() => expect(readProgressForSystem).toHaveBeenCalledOnce());
    complete({
      slotStatus: 'ACTIVE',
      unconfirmedWalBytes: null,
      retainedWalBytes: null,
      offsetLag: null,
    });
    await collection;
    query.mockResolvedValueOnce({ rows: [{ ...applied, run_id: 'new-run' }] });
    expect(await service.listForMonitoring()).toEqual([
      expect.objectContaining({ status: 'UNAVAILABLE' }),
    ]);
  });
});
