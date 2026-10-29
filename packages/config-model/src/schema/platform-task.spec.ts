import { describe, expect, it } from 'vitest';

import {
  kafkaTaskMappings,
  parsePlatformTaskConfig,
  pgTaskMappings,
  pgTableAddition,
  taskTargetTopics,
} from './platform-task.js';

const sourceDataSourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3';
const targetDataSourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4';

function pgTask() {
  return {
    kind: 'PG_TO_KAFKA',
    schemaVersion: 1,
    source: {
      dataSourceId: sourceDataSourceId,
      includeSnapshot: true,
      publicationName: 'orders_publication',
      resourceMode: 'PLATFORM_MANAGED',
      slotName: 'orders_slot',
      table: { name: 'orders', schema: 'public' },
    },
    target: { dataSourceId: targetDataSourceId, topic: 'cdc.orders' },
    tuning: {
      batchSize: 100,
      bufferSize: 16_000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all',
    },
  };
}

function pgMappingsTask() {
  const legacy = pgTask();
  const { table, ...source } = legacy.source;
  return {
    ...legacy,
    schemaVersion: 2,
    source,
    target: { dataSourceId: targetDataSourceId },
    mappings: [
      {
        source: table,
        targets: [{ topic: 'cdc.orders' }, { topic: 'cdc.archive' }],
      },
      {
        source: { schema: 'public', name: 'items' },
        targets: [{ topic: 'cdc.items' }, { topic: 'cdc.archive' }],
      },
    ],
  };
}

describe('parsePlatformTaskConfig', () => {
  it('detects added PG tables against applied config without mistaking format, order or fanout changes', () => {
    const old = { ...pgTask(), source: { ...pgTask().source, includeSnapshot: false } };
    const next = pgMappingsTask();
    next.source.includeSnapshot = false;
    // Keep the old route unchanged for a pure addition.
    next.mappings[0]!.targets = [{ topic: 'cdc.orders' }];
    expect(pgTableAddition(old, next)).toEqual({
      addedTables: [{ schema: 'public', name: 'items' }],
      eligible: true,
    });
    const same = { ...next, mappings: [next.mappings[0]!] };
    expect(pgTableAddition(old, same).addedTables).toEqual([]);
    same.mappings[0]!.targets.push({ topic: 'second.target' });
    expect(pgTableAddition(old, same).addedTables).toEqual([]);
    expect(pgTableAddition(old, next).eligible).toBe(false);
    expect(pgTableAddition(null, next).addedTables).toEqual([]);
  });

  it('does not authorize changing slots, deleting old tables or abandoning an unfinished snapshot', () => {
    const old = { ...pgTask(), source: { ...pgTask().source, includeSnapshot: false } };
    const next = pgMappingsTask();
    next.source.includeSnapshot = false;
    next.mappings[0]!.targets = [{ topic: 'cdc.orders' }];
    expect(
      pgTableAddition(old, { ...next, source: { ...next.source, slotName: 'other_slot' } })
        .eligible,
    ).toBe(false);
    expect(pgTableAddition(old, { ...next, mappings: [next.mappings[1]!] }).eligible).toBe(false);
    expect(pgTableAddition(pgTask(), next).eligible).toBe(false);
  });
  it('keeps every PG mapping and reads old tasks as one-row mappings', () => {
    const input = pgMappingsTask();
    const parsed = parsePlatformTaskConfig(input);
    if (parsed.kind !== 'PG_TO_KAFKA') throw new Error('Unexpected task kind');
    expect(parsed).toEqual(input);
    expect(pgTaskMappings(parsed)).toEqual(input.mappings);
    expect(taskTargetTopics(parsed)).toEqual([
      { topic: 'cdc.orders' },
      { topic: 'cdc.archive' },
      { topic: 'cdc.items' },
    ]);
    const old = parsePlatformTaskConfig(pgTask());
    if (old.kind !== 'PG_TO_KAFKA') throw new Error('Unexpected task kind');
    expect(pgTaskMappings(old)).toEqual([
      { source: pgTask().source.table, targets: [{ topic: pgTask().target.topic }] },
    ]);
  });

  it('keeps multiple Kafka sources and all per-source targets', () => {
    const input = {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 2,
      source: { dataSourceId: sourceDataSourceId, groupId: 'ape-dts-task-test' },
      target: { dataSourceId: targetDataSourceId },
      mappings: [
        { source: 'source.one', targets: [{ topic: 'target.one' }, { topic: 'archive' }] },
        { source: 'source.two', targets: [{ topic: 'target.two' }, { topic: 'archive' }] },
      ],
      tuning: { ...pgTask().tuning, ackIntervalSeconds: 5 },
    };
    const parsed = parsePlatformTaskConfig(input);
    if (parsed.kind !== 'KAFKA_RAW_FORWARD') throw new Error('Unexpected task kind');
    expect(kafkaTaskMappings(parsed)).toEqual(input.mappings);
    expect(parsed).toEqual(input);
    const topicCreation = { numPartitions: 2, replicationFactor: 1 };
    const { mappings, ...common } = input;
    const old = parsePlatformTaskConfig({
      ...common,
      schemaVersion: 1,
      source: { ...input.source, topic: mappings[0]!.source },
      target: { ...input.target, ...mappings[0]!.targets[0]!, topicCreation },
    });
    if (old.kind !== 'KAFKA_RAW_FORWARD') throw new Error('Unexpected task kind');
    expect(kafkaTaskMappings(old)).toEqual([
      { source: 'source.one', targets: [{ topic: 'target.one', topicCreation }] },
    ]);
    expect(taskTargetTopics(old)).toEqual([{ topic: 'target.one', topicCreation }]);
  });

  it('applies duplicate, topic-name and creation-policy validation equally to Kafka mapping rows', () => {
    const input = {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 2,
      source: { dataSourceId: sourceDataSourceId, groupId: 'ape-dts-task-test' },
      target: { dataSourceId: targetDataSourceId },
      tuning: { ...pgTask().tuning, ackIntervalSeconds: 5 },
    };
    const policy = { numPartitions: 2, replicationFactor: 1 };
    for (const mappings of [
      [{ source: '.', targets: [{ topic: 'valid' }] }],
      [{ source: 'source:injected', targets: [{ topic: 'valid' }] }],
      [{ source: 'source.one', targets: [{ topic: '..' }] }],
      [{ source: 'source.one', targets: [{ topic: 'target' }, { topic: 'target' }] }],
      [
        { source: 'source.one', targets: [{ topic: 'target' }] },
        { source: 'source.one', targets: [{ topic: 'other' }] },
      ],
      [
        { source: 'source.one', targets: [{ topic: 'shared', topicCreation: policy }] },
        {
          source: 'source.two',
          targets: [{ topic: 'shared', topicCreation: { ...policy, numPartitions: 3 } }],
        },
      ],
    ]) {
      expect(() => parsePlatformTaskConfig({ ...input, mappings })).toThrow();
    }
    const parsed = parsePlatformTaskConfig({
      ...input,
      mappings: [
        { source: 'source.one', targets: [{ topic: 'shared', topicCreation: policy }] },
        { source: 'source.two', targets: [{ topic: 'shared', topicCreation: policy }] },
      ],
    });
    expect(taskTargetTopics(parsed)).toEqual([{ topic: 'shared', topicCreation: policy }]);
  });

  it('retains PG snapshot and JSON-format validation for version-2 mappings', () => {
    const input = pgMappingsTask();
    expect(() =>
      parsePlatformTaskConfig({
        ...input,
        source: { ...input.source, resourceMode: 'APE_DTS_MANAGED' },
      }),
    ).toThrow('APE_DTS_MANAGED does not support snapshot');
    expect(() =>
      parsePlatformTaskConfig({
        ...input,
        target: { ...input.target, messageFormat: 'avro', jsonTemplate: 'cloudcanal' },
      }),
    ).toThrow('JSON template requires JSON message format');
  });

  it('rejects empty mappings, duplicate sources, duplicate row targets and conflicting creation policies', () => {
    const input = pgMappingsTask();
    for (const mappings of [
      [],
      [{ source: input.mappings[0]!.source, targets: [] }],
      [input.mappings[0], input.mappings[0]],
      [{ source: input.mappings[0]!.source, targets: [{ topic: 'same' }, { topic: 'same' }] }],
      [
        { source: input.mappings[0]!.source, targets: [{ topic: 'shared' }] },
        {
          source: input.mappings[1]!.source,
          targets: [{ topic: 'shared', topicCreation: { numPartitions: 1, replicationFactor: 1 } }],
        },
      ],
    ]) {
      expect(() => parsePlatformTaskConfig({ ...input, mappings })).toThrow();
    }
  });

  it('rejects ambiguous old scalar bindings on version 2 instead of silently choosing a first row', () => {
    const input = pgMappingsTask();
    expect(() => parsePlatformTaskConfig({ ...input, source: pgTask().source })).toThrow();
    expect(() => parsePlatformTaskConfig({ ...input, target: pgTask().target })).toThrow();
    const topicCreation = { numPartitions: 2, replicationFactor: 1 };
    const mappings = input.mappings.map((mapping) => ({
      ...mapping,
      targets: mapping.targets.map((target) => ({ ...target, topicCreation })),
    }));
    const parsed = parsePlatformTaskConfig({ ...input, mappings });
    expect(taskTargetTopics(parsed).every((target) => target.topicCreation === undefined)).toBe(
      false,
    );
    expect(taskTargetTopics(parsed)).toHaveLength(3);
  });

  it('accepts the stable first-version PG and Kafka task models without adding defaults', () => {
    expect(parsePlatformTaskConfig(pgTask())).toEqual(pgTask());

    const kafka = {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 1,
      source: {
        dataSourceId: sourceDataSourceId,
        groupId: 'ape-dts-task-018f6da0',
        topic: 'source.cdc',
      },
      target: { dataSourceId: targetDataSourceId, topic: 'target.cdc' },
      tuning: {
        ackIntervalSeconds: 5,
        batchSize: 100,
        bufferSize: 16_000,
        checkpointIntervalSeconds: 15,
        requiredAcks: 'all',
      },
    };
    expect(parsePlatformTaskConfig(kafka)).toEqual(kafka);
  });

  it('rejects snapshot with APE_DTS_MANAGED and rejects missing tuning instead of filling values', () => {
    expect(() =>
      parsePlatformTaskConfig({
        ...pgTask(),
        source: { ...pgTask().source, resourceMode: 'APE_DTS_MANAGED' },
      }),
    ).toThrow('APE_DTS_MANAGED does not support snapshot');

    expect(() => parsePlatformTaskConfig({ ...pgTask(), tuning: undefined })).toThrow();
  });

  it('rejects names that cannot be represented by the certified first adapter', () => {
    expect(() =>
      parsePlatformTaskConfig({
        ...pgTask(),
        target: { ...pgTask().target, topic: 'topic:injected' },
      }),
    ).toThrow();
  });
});
