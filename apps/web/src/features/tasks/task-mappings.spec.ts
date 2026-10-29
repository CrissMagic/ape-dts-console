import { describe, expect, it } from 'vitest';
import { emptyTaskMapping, readTaskMappings, serializeTaskMappings } from './task-mappings';

describe('task mapping editor', () => {
  it('reads old one-to-one configurations without losing topic creation options', () => {
    const rows = readTaskMappings({
      kind: 'PG_TO_KAFKA',
      schemaVersion: 1,
      source: { table: { schema: 'public', name: 'orders' } },
      target: { topic: 'orders', topicCreation: { numPartitions: 3, replicationFactor: 1 } },
    });
    expect(serializeTaskMappings(rows, 'PG_KAFKA', true)).toEqual([
      {
        source: { schema: 'public', name: 'orders' },
        targets: [{ topic: 'orders', topicCreation: { numPartitions: 3, replicationFactor: 1 } }],
      },
    ]);
  });

  it.each(['PG_KAFKA', 'MYSQL_KAFKA', 'KAFKA_KAFKA'] as const)(
    'round trips all sources and destinations for %s',
    (type) => {
      const mappings = ['orders', 'customers'].map((name) => ({
        source:
          type !== 'KAFKA_KAFKA'
            ? { schema: type === 'MYSQL_KAFKA' ? 'data_demo' : 'public', name }
            : name,
        targets: [
          { topic: `${name}.primary` },
          { topic: 'audit', topicCreation: { numPartitions: 2, replicationFactor: 1 } },
        ],
      }));
      expect(
        serializeTaskMappings(
          readTaskMappings({
            schemaVersion: 2,
            kind:
              type === 'PG_KAFKA'
                ? 'PG_TO_KAFKA'
                : type === 'MYSQL_KAFKA'
                  ? 'MYSQL_TO_KAFKA'
                  : 'KAFKA_RAW_FORWARD',
            mappings,
          }),
          type,
          true,
        ),
      ).toEqual(mappings);
    },
  );

  it('accepts MySQL table identifiers up to 64 characters without changing the PG limit', () => {
    const row = {
      ...emptyTaskMapping(),
      sourceSchema: 'data_demo',
      sourceTable: 'x'.repeat(64),
      targets: [
        { topic: 'orders', autoCreateTopic: false, numPartitions: null, replicationFactor: null },
      ],
    };
    expect(serializeTaskMappings([row], 'MYSQL_KAFKA', true)[0]?.source).toEqual({
      schema: 'data_demo',
      name: 'x'.repeat(64),
    });
    expect(() => serializeTaskMappings([row], 'PG_KAFKA', true)).toThrow('格式不正确');
  });

  it('permits an incomplete saved draft but blocks leaving mappings without a source and targets', () => {
    expect(serializeTaskMappings([], 'KAFKA_KAFKA')).toEqual([]);
    expect(() => serializeTaskMappings([], 'KAFKA_KAFKA', true)).toThrow('至少添加一个来源');
    expect(() =>
      serializeTaskMappings(
        [{ ...emptyTaskMapping(), sourceTopic: 'orders' }],
        'KAFKA_KAFKA',
        true,
      ),
    ).toThrow('目标 Topic');
  });

  it('rejects duplicate sources, invalid topics and inconsistent shared target policies', () => {
    const row = {
      ...emptyTaskMapping(),
      sourceTopic: 'orders',
      targets: [
        { topic: 'audit', autoCreateTopic: false, numPartitions: null, replicationFactor: null },
      ],
    };
    expect(() => serializeTaskMappings([row, row], 'KAFKA_KAFKA', true)).toThrow('来源不能重复');
    expect(() =>
      serializeTaskMappings(
        [{ ...row, targets: [{ ...row.targets[0]!, topic: '..' }] }],
        'KAFKA_KAFKA',
      ),
    ).toThrow('格式不正确');
    expect(() =>
      serializeTaskMappings(
        [
          row,
          {
            ...row,
            sourceTopic: 'customers',
            targets: [
              { ...row.targets[0]!, autoCreateTopic: true, numPartitions: 2, replicationFactor: 1 },
            ],
          },
        ],
        'KAFKA_KAFKA',
      ),
    ).toThrow('创建策略必须一致');
  });
});
