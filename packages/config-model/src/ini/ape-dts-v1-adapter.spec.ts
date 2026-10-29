import { describe, expect, it } from 'vitest';

import { ApeDtsV1Adapter } from '../adapters/ape-dts-v1-adapter.js';
import { parsePlatformTaskConfig } from '../schema/platform-task.js';
import { renderIni } from './ini-renderer.js';
import { runtimeText } from './runtime-text.js';

const sourceDataSourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3';
const targetDataSourceId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4';

const pgTask = parsePlatformTaskConfig({
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
});

const runtime = {
  log4rsPath: '/run/ape-dts/task/run/unit/log4rs.yaml',
  logDirectory: '/run/ape-dts/task/run/unit/logs',
  logLevel: 'info' as const,
  metricsPort: 19_090,
  sourceConnection: runtimeText(
    'postgresql://replicator:very-secret@pg.internal:5432/app',
    'postgresql://replicator:[REDACTED]@pg.internal:5432/app',
  ),
  targetConnection: runtimeText('kafka.internal:9092', 'kafka.internal:9092'),
};

describe('ApeDtsV1Adapter', () => {
  it('renders all selected PG tables with all fanout targets for both stages', () => {
    if (pgTask.kind !== 'PG_TO_KAFKA' || pgTask.schemaVersion !== 1)
      throw new Error('Unexpected task kind');
    const { table, ...source } = pgTask.source;
    const config = parsePlatformTaskConfig({
      ...pgTask,
      schemaVersion: 2,
      source,
      target: { dataSourceId: targetDataSourceId },
      mappings: [
        {
          source: table,
          targets: [{ topic: 'orders' }, { topic: 'archive' }],
        },
        {
          source: { schema: 'public', name: 'items' },
          targets: [{ topic: 'items' }, { topic: 'archive' }],
        },
      ],
    });
    for (const scenario of ['PG_SNAPSHOT_TO_KAFKA', 'PG_CDC_TO_KAFKA'] as const) {
      const rendered = new ApeDtsV1Adapter().render({ config, runtime, scenario });
      expect(rendered.runtimeIni).toContain('do_tbs=public.orders,public.items\n');
      expect(rendered.runtimeIni).toContain(
        'topic_fanout_map=public.orders:orders|archive,public.items:items|archive\n',
      );
      expect(rendered.runtimeIni).not.toMatch(/\ntopic_map=/u);
    }
  });

  it('renders only the explicitly bound Kafka source and refuses missing or unknown bindings', () => {
    const config = parsePlatformTaskConfig({
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 2,
      source: { dataSourceId: sourceDataSourceId, groupId: 'ape-dts-task-test' },
      target: { dataSourceId: targetDataSourceId },
      mappings: [
        { source: 'source.one', targets: [{ topic: 'target.one' }] },
        { source: 'source.two', targets: [{ topic: 'target.two' }, { topic: 'archive' }] },
      ],
      tuning: { ...pgTask.tuning, ackIntervalSeconds: 5 },
    });
    const adapter = new ApeDtsV1Adapter();
    const request = {
      config,
      runtime: { ...runtime, sourcePartition: 0, sourceTopic: 'source.two' },
      scenario: 'KAFKA_RAW_FORWARD' as const,
    };
    const rendered = adapter.render(request);
    expect(rendered.runtimeIni).toContain('topic=source.two\n');
    expect(rendered.runtimeIni).toContain('topic_fanout_map=source.two:target.two|archive\n');
    expect(rendered.runtimeIni).not.toContain('source.one');
    expect(() =>
      adapter.render({ ...request, runtime: { ...runtime, sourcePartition: 0 } }),
    ).toThrow('KAFKA_SOURCE_TOPIC_REQUIRED');
    expect(() =>
      adapter.render({ ...request, runtime: { ...request.runtime, sourceTopic: 'unknown' } }),
    ).toThrow('KAFKA_SOURCE_TOPIC_INVALID');
    for (const sourcePartition of [-1, 0.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        adapter.render({ ...request, runtime: { ...request.runtime, sourcePartition } }),
      ).toThrow('KAFKA_SOURCE_PARTITION_REQUIRED');
    }
  });

  it('passes explicit CloudCanal JSON settings to both snapshot and CDC sinkers', () => {
    const config = parsePlatformTaskConfig({
      ...pgTask,
      target: {
        ...pgTask.target,
        messageFormat: 'json',
        jsonTemplate: 'cloudcanal',
        withFieldDefs: true,
      },
    });
    for (const scenario of ['PG_SNAPSHOT_TO_KAFKA', 'PG_CDC_TO_KAFKA'] as const) {
      const rendered = new ApeDtsV1Adapter().render({ config, runtime, scenario });
      for (const ini of [rendered.runtimeIni, rendered.previewIni]) {
        const sinker = ini.split('[sinker]\n')[1]?.split('\n[router]')[0];
        expect(sinker).toContain('message_format=json\n');
        expect(sinker).toContain('json_template=cloudcanal\n');
        expect(sinker).toContain('with_field_defs=true\n');
      }
    }
  });

  it('renders an explicit Avro field-definition choice without changing legacy engine defaults', () => {
    const adapter = new ApeDtsV1Adapter();
    const config = parsePlatformTaskConfig({
      ...pgTask,
      target: { ...pgTask.target, messageFormat: 'avro', withFieldDefs: false },
    });
    const rendered = adapter.render({ config, runtime, scenario: 'PG_CDC_TO_KAFKA' });
    expect(rendered.runtimeIni).toContain('message_format=avro\n');
    expect(rendered.runtimeIni).toContain('with_field_defs=false\n');
    expect(rendered.runtimeIni).not.toContain('json_template=');
    const legacy = adapter.render({ config: pgTask, runtime, scenario: 'PG_CDC_TO_KAFKA' });
    expect(legacy.runtimeIni).not.toMatch(/message_format=|json_template=|with_field_defs=/u);
  });

  it('renders the standard JSON template independently from CloudCanal', () => {
    const config = parsePlatformTaskConfig({
      ...pgTask,
      target: { ...pgTask.target, messageFormat: 'json', jsonTemplate: 'standard' },
    });
    const rendered = new ApeDtsV1Adapter().render({ config, runtime, scenario: 'PG_CDC_TO_KAFKA' });
    expect(rendered.runtimeIni).toContain('message_format=json\n');
    expect(rendered.runtimeIni).toContain('json_template=standard\n');
    expect(rendered.runtimeIni).not.toContain('with_field_defs=');
  });

  it('keeps platform topic provisioning policy out of engine configuration', () => {
    const adapter = new ApeDtsV1Adapter();
    const config = parsePlatformTaskConfig({
      ...pgTask,
      target: {
        ...pgTask.target,
        topicCreation: { numPartitions: 3, replicationFactor: 1 },
      },
    });
    for (const scenario of ['PG_SNAPSHOT_TO_KAFKA', 'PG_CDC_TO_KAFKA'] as const) {
      expect(adapter.render({ config, runtime, scenario })).toEqual(
        adapter.render({ config: pgTask, runtime, scenario }),
      );
    }
  });

  it('renders separate PG snapshot and CDC INI while forcing safe CDC resource behavior', () => {
    const adapter = new ApeDtsV1Adapter();
    const snapshot = adapter.render({ config: pgTask, runtime, scenario: 'PG_SNAPSHOT_TO_KAFKA' });
    const cdc = adapter.render({ config: pgTask, runtime, scenario: 'PG_CDC_TO_KAFKA' });

    expect(snapshot.runtimeIni).toContain('extract_type=snapshot');
    expect(snapshot.runtimeIni).toContain('do_tbs=public.orders');
    expect(snapshot.runtimeIni).toContain('topic_map=public.orders:cdc.orders');
    expect(snapshot.runtimeIni).not.toContain('slot_name=');
    expect(cdc.runtimeIni).toContain('extract_type=cdc');
    expect(cdc.runtimeIni).toContain('slot_name=orders_slot');
    expect(cdc.runtimeIni).toContain('pub_name=orders_publication');
    expect(cdc.runtimeIni).toContain('recreate_slot_if_exists=false');
    expect(cdc.runtimeIni).toContain('http_host=127.0.0.1');
    expect(cdc.runtimeIni).toContain('http_port=19090');
    expect(cdc.previewIni).not.toContain('very-secret');
    expect(cdc.previewIni).toContain('[REDACTED]');
    expect(cdc.log4rsYaml).toContain('kind: console');
    expect(cdc.log4rsYaml).not.toMatch(/kind:\s*file/u);
  });

  it('renders one Kafka partition with raw forwarding invariants and no resumer section', () => {
    const config = parsePlatformTaskConfig({
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
    });
    const rendered = new ApeDtsV1Adapter().render({
      config,
      runtime: {
        ...runtime,
        sourceConnection: runtimeText('source-kafka:9092', 'source-kafka:9092'),
        sourcePartition: 3,
        targetConnection: runtimeText('target-kafka:9092', 'target-kafka:9092'),
      },
      scenario: 'KAFKA_RAW_FORWARD',
    });

    expect(rendered.runtimeIni).toContain('partition=3');
    expect(rendered.runtimeIni).toContain('offset=-1');
    expect(rendered.runtimeIni).toContain('ack_interval_secs=5');
    expect(rendered.runtimeIni).toContain('required_acks=all');
    expect(rendered.runtimeIni).toContain('topic_fanout_map=source.cdc:target.cdc');
    expect(rendered.runtimeIni).toContain('parallel_type=serial');
    expect(rendered.runtimeIni).not.toContain('[resumer]');
    expect(rendered.runtimeIni).not.toMatch(/message_format=|json_template=|with_field_defs=/u);
  });

  it('rejects a newline in any INI value instead of allowing section or key injection', () => {
    expect(() => renderIni([{ name: 'runtime', values: { log_level: 'info\n[sinker]' } }])).toThrow(
      'INI_VALUE_INVALID',
    );
  });
});
