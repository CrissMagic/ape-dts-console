import { describe, expect, it } from 'vitest';

import { ApeDtsV1Adapter } from '../adapters/ape-dts-v1-adapter.js';
import { runtimeText } from './runtime-text.js';

const config = {
  kind: 'MYSQL_TO_KAFKA' as const,
  schemaVersion: 2 as const,
  source: { dataSourceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', includeSnapshot: true },
  target: {
    dataSourceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4',
    messageFormat: 'json' as const,
    jsonTemplate: 'cloudcanal' as const,
    withFieldDefs: true,
  },
  mappings: [
    {
      source: { schema: 'data_demo', name: 'orders' },
      targets: [{ topic: 'orders' }, { topic: 'archive' }],
    },
    {
      source: { schema: 'data_demo', name: 'items' },
      targets: [{ topic: 'items', topicCreation: { numPartitions: 3, replicationFactor: 1 } }],
    },
  ],
  tuning: {
    batchSize: 200,
    bufferSize: 16_000,
    checkpointIntervalSeconds: 10,
    requiredAcks: 'all' as const,
  },
};
const runtime = {
  log4rsPath: '/run/ape-dts/task/run/unit/log4rs.yaml',
  logDirectory: '/run/ape-dts/task/run/unit/logs',
  logLevel: 'info' as const,
  metricsPort: 19_090,
  sourceConnection: runtimeText(
    'mysql://reader:private-password@mysql.test:3306/data_demo?ssl-mode=disabled',
    'mysql://reader:[REDACTED]@mysql.test:3306/data_demo?ssl-mode=disabled',
  ),
  targetConnection: runtimeText('kafka.test:9092', 'kafka.test:9092'),
  mysqlCdc: { serverId: 2000, binlogFilename: 'mysql-bin.000003', binlogPosition: 1234 },
};

describe('MySQL to Kafka configuration rendering', () => {
  it('renders snapshot tables, fanout and encoding without PostgreSQL resources or leaked secrets', () => {
    const rendered = new ApeDtsV1Adapter().render({
      config,
      runtime,
      scenario: 'MYSQL_SNAPSHOT_TO_KAFKA',
    });
    expect(rendered.runtimeIni).toContain('db_type=mysql\nextract_type=snapshot\n');
    expect(rendered.log4rsYaml).toContain('kind: rolling_file');
    expect(rendered.log4rsYaml).toContain('mysql-finished.log');
    expect(rendered.log4rsYaml).toContain('position_logger:');
    expect(rendered.runtimeIni).toContain('do_tbs=data_demo.orders,data_demo.items\n');
    expect(rendered.runtimeIni).toContain(
      'topic_fanout_map=data_demo.orders:orders|archive,data_demo.items:items\n',
    );
    expect(rendered.runtimeIni).toContain('parallel_type=snapshot\nparallel_size=1\n');
    expect(rendered.runtimeIni).toContain(
      'message_format=json\njson_template=cloudcanal\nwith_field_defs=true\n',
    );
    expect(rendered.runtimeIni).toContain('private-password');
    expect(rendered.previewIni).toContain('[REDACTED]');
    expect(rendered.previewIni).not.toContain('private-password');
    expect(rendered.log4rsYaml).not.toContain('private-password');
    expect(rendered.runtimeIni).not.toMatch(
      /slot_name|pub_name|start_lsn|topicCreation|numPartitions|replicationFactor/u,
    );
  });

  it('renders an explicit CDC position and stable replication client ID instead of latest', () => {
    const rendered = new ApeDtsV1Adapter().render({
      config,
      runtime,
      scenario: 'MYSQL_CDC_TO_KAFKA',
    });
    expect(rendered.runtimeIni).toContain('db_type=mysql\nextract_type=cdc\n');
    expect(rendered.runtimeIni).toContain('binlog_filename=mysql-bin.000003\n');
    expect(rendered.runtimeIni).toContain('binlog_position=1234\n');
    expect(rendered.runtimeIni).toContain('server_id=2000\n');
    expect(rendered.runtimeIni).toContain('do_events=insert,update,delete\n');
    expect(rendered.runtimeIni).toContain('parallel_type=serial\nparallel_size=1\n');
    expect(rendered.previewIni).toContain('binlog_position=1234\n');
  });

  it.each([
    { messageFormat: 'avro' as const },
    { messageFormat: 'json' as const, jsonTemplate: 'standard' as const },
    { messageFormat: 'json' as const, jsonTemplate: 'cloudcanal' as const },
  ])('preserves the selected $messageFormat/$jsonTemplate encoding', (encoding) => {
    const target = { dataSourceId: config.target.dataSourceId, withFieldDefs: false, ...encoding };
    const rendered = new ApeDtsV1Adapter().render({
      config: { ...config, target },
      runtime,
      scenario: 'MYSQL_CDC_TO_KAFKA',
    });
    expect(rendered.runtimeIni).toContain(`message_format=${encoding.messageFormat}\n`);
    expect(rendered.runtimeIni).toContain('with_field_defs=false\n');
    if ('jsonTemplate' in encoding)
      expect(rendered.runtimeIni).toContain(`json_template=${encoding.jsonTemplate}\n`);
    else expect(rendered.runtimeIni).not.toContain('json_template=');
  });

  it('keeps defaults absent and does not manufacture snapshot or CDC settings', () => {
    const rendered = new ApeDtsV1Adapter().render({
      config: { ...config, target: { dataSourceId: config.target.dataSourceId } },
      runtime,
      scenario: 'MYSQL_SNAPSHOT_TO_KAFKA',
    });
    expect(rendered.runtimeIni).not.toMatch(
      /message_format|json_template|with_field_defs|binlog_filename|binlog_position|server_id/u,
    );
  });

  it('refuses CDC when no usable position or client ID was supplied', () => {
    const adapter = new ApeDtsV1Adapter();
    const missingPosition = {
      log4rsPath: runtime.log4rsPath,
      logDirectory: runtime.logDirectory,
      logLevel: runtime.logLevel,
      metricsPort: runtime.metricsPort,
      sourceConnection: runtime.sourceConnection,
      targetConnection: runtime.targetConnection,
    };
    expect(() =>
      adapter.render({ config, runtime: missingPosition, scenario: 'MYSQL_CDC_TO_KAFKA' }),
    ).toThrow('MYSQL_CDC_POSITION_REQUIRED');
    for (const binlogFilename of ['', '../mysql-bin.000003', 'mysql-bin\nserver_id=1']) {
      expect(() =>
        adapter.render({
          config,
          runtime: { ...runtime, mysqlCdc: { ...runtime.mysqlCdc, binlogFilename } },
          scenario: 'MYSQL_CDC_TO_KAFKA',
        }),
      ).toThrow('MYSQL_CDC_POSITION_INVALID');
    }
    for (const binlogPosition of [0, 3, -1, 1.5, NaN, 4_294_967_296]) {
      expect(() =>
        adapter.render({
          config,
          runtime: { ...runtime, mysqlCdc: { ...runtime.mysqlCdc, binlogPosition } },
          scenario: 'MYSQL_CDC_TO_KAFKA',
        }),
      ).toThrow('MYSQL_CDC_POSITION_INVALID');
    }
    for (const serverId of [0, -1, 1.5, NaN, 4_294_967_296]) {
      expect(() =>
        adapter.render({
          config,
          runtime: { ...runtime, mysqlCdc: { ...runtime.mysqlCdc, serverId } },
          scenario: 'MYSQL_CDC_TO_KAFKA',
        }),
      ).toThrow('MYSQL_CDC_SERVER_ID_INVALID');
    }
  });

  it('does not run snapshot for a CDC-only configuration', () => {
    expect(() =>
      new ApeDtsV1Adapter().render({
        config: { ...config, source: { ...config.source, includeSnapshot: false } },
        runtime,
        scenario: 'MYSQL_SNAPSHOT_TO_KAFKA',
      }),
    ).toThrow('MYSQL_SNAPSHOT_NOT_ENABLED');
  });

  it('rejects ambiguous mappings and invalid encoding before writing INI', () => {
    const adapter = new ApeDtsV1Adapter();
    for (const mappings of [
      [],
      [config.mappings[0]!, config.mappings[0]!],
      [{ ...config.mappings[0]!, targets: [{ topic: 'orders' }, { topic: 'orders' }] }],
      [{ source: { schema: 'data_demo', name: 'orders,other' }, targets: [{ topic: 'orders' }] }],
      [
        {
          ...config.mappings[0]!,
          targets: [
            { topic: 'archive', topicCreation: { numPartitions: 1, replicationFactor: 1 } },
          ],
        },
        {
          ...config.mappings[1]!,
          targets: [
            { topic: 'archive', topicCreation: { numPartitions: 3, replicationFactor: 1 } },
          ],
        },
      ],
    ]) {
      expect(() =>
        adapter.render({
          config: { ...config, mappings },
          runtime,
          scenario: 'MYSQL_CDC_TO_KAFKA',
        }),
      ).toThrow();
    }
    expect(() =>
      adapter.render({
        config: { ...config, target: { ...config.target, messageFormat: 'avro' } },
        runtime,
        scenario: 'MYSQL_CDC_TO_KAFKA',
      }),
    ).toThrow();
  });

  it('does not let MySQL requests fall through the PostgreSQL or Kafka adapter paths', () => {
    const adapter = new ApeDtsV1Adapter();
    for (const scenario of ['PG_CDC_TO_KAFKA', 'KAFKA_RAW_FORWARD']) {
      expect(() => Reflect.apply(adapter.render, adapter, [{ config, runtime, scenario }])).toThrow(
        'ADAPTER_SCENARIO_CONFIG_MISMATCH',
      );
    }
  });
});
