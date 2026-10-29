import { describe, expect, it } from 'vitest';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  mysqlConfigurationFingerprint,
  parseMysqlCheckpointLog,
  compareMysqlPositions,
  mysqlSnapshotCompleted,
  readMysqlSnapshotCompleted,
} from './mysql-runtime-state.js';

describe('MySQL durable runtime positions', () => {
  it('proves all tables completed even when finish evidence exceeds the checkpoint tail window', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'mysql-snapshot-evidence-'));
    try {
      const config = {
        mappings: Array.from({ length: 16000 }, (_, index) => ({
          source: { schema: 'data_demo', name: `table_${index}` },
        })),
      };
      const lines = config.mappings.map(({ source }) =>
        JSON.stringify({
          type: 'RdbSnapshotFinished',
          db_type: 'mysql',
          schema: source.schema,
          tb: source.name,
        }),
      );
      expect(Buffer.byteLength(lines.join('\n'))).toBeGreaterThan(1024 * 1024);
      await writeFile(
        path.join(directory, 'mysql-finished.log'),
        `${lines.join('\n')}\ntask finished\n`,
      );
      expect(await readMysqlSnapshotCompleted(directory, config)).toBe(true);
      await writeFile(
        path.join(directory, 'mysql-finished.log'),
        `${lines.slice(1).join('\n')}\ntask finished\n`,
      );
      expect(await readMysqlSnapshotCompleted(directory, config)).toBe(false);
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });
  const position = { binlogFilename: 'mysql-bin.000009', binlogPosition: 120, serverId: 2000 };
  const log = (value: object) => `checkpoint_position | ${JSON.stringify(value)}\n`;
  const commit = {
    type: 'MysqlCdc',
    server_id: '2000',
    binlog_filename: 'mysql-bin.000009',
    next_event_position: 120,
    gtid_set: '',
    timestamp: '2026-10-16 00:00:00.000',
  };

  it('resumes only from acknowledged transaction checkpoints, never receive watermarks', () => {
    expect(
      parseMysqlCheckpointLog(`current_position | ${JSON.stringify(commit)}\n`, 2000),
    ).toBeNull();
    expect(parseMysqlCheckpointLog(log(commit), 2000)).toEqual(position);
    expect(parseMysqlCheckpointLog(log({ type: 'RdbSnapshotFinished' }), 2000)).toBeNull();
    expect(
      parseMysqlCheckpointLog(`checkpoint_position | ${JSON.stringify(commit)}`, 2000),
    ).toBeNull();
    expect(
      parseMysqlCheckpointLog(
        `${log(commit)}current_position | ${JSON.stringify({ ...commit, next_event_position: 200 })}\n`,
        2000,
      ),
    ).toEqual(position);
  });

  it('rejects corrupt, foreign-client and invalid positions instead of fabricating defaults', () => {
    for (const change of [
      { server_id: '2001' },
      { next_event_position: 0 },
      { next_event_position: 4_294_967_296 },
      { binlog_filename: '../mysql-bin.000009' },
    ])
      expect(() => parseMysqlCheckpointLog(log({ ...commit, ...change }), 2000)).toThrow(
        'MYSQL_CHECKPOINT_INVALID',
      );
    expect(() => parseMysqlCheckpointLog('checkpoint_position | not-json\n', 2000)).toThrow(
      'MYSQL_CHECKPOINT_INVALID',
    );
  });

  it('orders file rotations numerically and refuses incomparable file families', () => {
    expect(compareMysqlPositions(position, { ...position, binlogPosition: 121 })).toBe(-1);
    expect(
      compareMysqlPositions(position, {
        ...position,
        binlogFilename: 'mysql-bin.000010',
        binlogPosition: 4,
      }),
    ).toBe(-1);
    expect(compareMysqlPositions(position, position)).toBe(0);
    expect(() =>
      compareMysqlPositions(position, { ...position, binlogFilename: 'other.000010' }),
    ).toThrow('MYSQL_BINLOG_FAMILY_CHANGED');
  });

  it('preserves checkpoints across tuning edits but does not reuse them for changed data routing', () => {
    const config = {
      kind: 'MYSQL_TO_KAFKA' as const,
      schemaVersion: 2 as const,
      source: { dataSourceId: '11111111-1111-4111-8111-111111111111', includeSnapshot: true },
      target: {
        dataSourceId: '22222222-2222-4222-8222-222222222222',
        messageFormat: 'json' as const,
        jsonTemplate: 'cloudcanal' as const,
      },
      mappings: [
        { source: { schema: 'data_demo', name: 'orders' }, targets: [{ topic: 'orders' }] },
      ],
      tuning: {
        batchSize: 200,
        bufferSize: 16000,
        checkpointIntervalSeconds: 1,
        requiredAcks: 'all' as const,
      },
    };
    const fingerprint = mysqlConfigurationFingerprint(config);
    expect(
      mysqlConfigurationFingerprint({ ...config, tuning: { ...config.tuning, batchSize: 400 } }),
    ).toBe(fingerprint);
    expect(
      mysqlConfigurationFingerprint({
        ...config,
        mappings: [{ ...config.mappings[0]!, targets: [{ topic: 'another' }] }],
      }),
    ).not.toBe(fingerprint);
    expect(
      mysqlConfigurationFingerprint({
        ...config,
        source: { ...config.source, includeSnapshot: false },
      }),
    ).toBe(fingerprint);
    expect(
      mysqlConfigurationFingerprint({
        ...config,
        target: { dataSourceId: config.target.dataSourceId, messageFormat: 'avro' },
      }),
    ).toBe(fingerprint);
  });

  it('requires all mapped tables and the normal completion marker before switching to CDC', () => {
    const config = {
      mappings: [
        { source: { schema: 'data_demo', name: 'orders' } },
        { source: { schema: 'data_demo', name: 'items' } },
      ],
    };
    const finished = (table: string) =>
      `${JSON.stringify({ type: 'RdbSnapshotFinished', db_type: 'mysql', schema: 'data_demo', tb: table })}\n`;
    expect(mysqlSnapshotCompleted(finished('orders') + 'task finished\n', config)).toBe(false);
    expect(mysqlSnapshotCompleted(finished('orders') + finished('items'), config)).toBe(false);
    expect(
      mysqlSnapshotCompleted(finished('orders') + finished('items') + 'task finished\n', config),
    ).toBe(true);
  });
});
