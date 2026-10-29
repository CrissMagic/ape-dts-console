import path from 'node:path';

import { renderIni, type IniSection } from '../ini/ini-renderer.js';
import {
  kafkaTaskMappings,
  pgTaskMappings,
  type PlatformTaskConfig,
  type PgToKafkaTaskConfig,
} from '../schema/platform-task.js';
import {
  mysqlToKafkaTaskConfigSchema,
  type MysqlToKafkaTaskConfig,
} from '../schema/mysql-kafka-task.js';
import type {
  AdapterRenderRequest,
  ApeDtsConfigAdapter,
  ApeDtsRuntimeBindings,
  RenderedApeDtsConfig,
  MysqlAdapterRenderRequest,
  MysqlTaskScenario,
  TaskScenario,
} from './types.js';

function assertRuntimePath(value: string): void {
  if (
    !path.posix.isAbsolute(value) ||
    !value.startsWith('/run/ape-dts/') ||
    value.split('/').some((segment) => segment === '..' || segment === '.') ||
    value.includes('\r') ||
    value.includes('\n') ||
    value.includes('\u0000')
  ) {
    throw new Error('APE_DTS_RUNTIME_PATH_INVALID');
  }
}

function validateRuntime(runtime: ApeDtsRuntimeBindings): void {
  assertRuntimePath(runtime.log4rsPath);
  assertRuntimePath(runtime.logDirectory);
  if (
    !Number.isSafeInteger(runtime.metricsPort) ||
    runtime.metricsPort < 1 ||
    runtime.metricsPort > 65_535
  ) {
    throw new Error('APE_DTS_METRICS_PORT_INVALID');
  }
}

function runtimeSections(runtime: ApeDtsRuntimeBindings): readonly IniSection[] {
  return [
    {
      name: 'runtime',
      values: {
        log_dir: runtime.logDirectory,
        log_level: runtime.logLevel,
        log4rs_file: runtime.log4rsPath,
      },
    },
    {
      name: 'metrics',
      values: { http_host: '127.0.0.1', http_port: runtime.metricsPort, workers: 1 },
    },
  ];
}

function mysqlCdcValues(runtime: ApeDtsRuntimeBindings): Record<string, number | string> {
  const position = runtime.mysqlCdc;
  if (position === undefined) throw new Error('MYSQL_CDC_POSITION_REQUIRED');
  if (
    typeof position.binlogFilename !== 'string' ||
    !/^[A-Za-z0-9._-]{1,255}$/u.test(position.binlogFilename) ||
    position.binlogFilename === '.' ||
    position.binlogFilename === '..' ||
    !Number.isSafeInteger(position.binlogPosition) ||
    position.binlogPosition < 4 ||
    position.binlogPosition > 4_294_967_295
  )
    throw new Error('MYSQL_CDC_POSITION_INVALID');
  if (
    !Number.isSafeInteger(position.serverId) ||
    position.serverId < 1 ||
    position.serverId > 4_294_967_295
  )
    throw new Error('MYSQL_CDC_SERVER_ID_INVALID');
  return {
    binlog_filename: position.binlogFilename,
    binlog_position: position.binlogPosition,
    server_id: position.serverId,
  };
}

function rdbSections(
  config: PgToKafkaTaskConfig | MysqlToKafkaTaskConfig,
  runtime: ApeDtsRuntimeBindings,
  scenario: Extract<TaskScenario, 'PG_SNAPSHOT_TO_KAFKA' | 'PG_CDC_TO_KAFKA'> | MysqlTaskScenario,
  preview: boolean,
): readonly IniSection[] {
  const mysql = config.kind === 'MYSQL_TO_KAFKA';
  const snapshot = scenario === 'PG_SNAPSHOT_TO_KAFKA' || scenario === 'MYSQL_SNAPSHOT_TO_KAFKA';
  if (snapshot && !config.source.includeSnapshot) {
    throw new Error(mysql ? 'MYSQL_SNAPSHOT_NOT_ENABLED' : 'PG_SNAPSHOT_NOT_ENABLED');
  }
  const mappings = config.kind === 'PG_TO_KAFKA' ? pgTaskMappings(config) : config.mappings;
  const tableName = (source: { schema: string; name: string }) => `${source.schema}.${source.name}`;
  const extractorValues: Record<string, boolean | number | string> = {
    db_type: mysql ? 'mysql' : 'pg',
    extract_type: snapshot ? 'snapshot' : 'cdc',
    url: preview ? runtime.sourceConnection.preview() : runtime.sourceConnection.reveal(),
  };
  if (mysql && runtime.mysqlTls !== undefined) {
    extractorValues.ssl_mode = runtime.mysqlTls.mode;
    if (runtime.mysqlTls.caPath !== undefined) {
      assertRuntimePath(runtime.mysqlTls.caPath);
      extractorValues.ssl_ca_path = runtime.mysqlTls.caPath;
    }
  }
  if (!snapshot && config.kind === 'PG_TO_KAFKA') {
    extractorValues.start_lsn = '';
    extractorValues.slot_name = config.source.slotName;
    extractorValues.pub_name = config.source.publicationName;
    extractorValues.recreate_slot_if_exists = false;
  }
  if (!snapshot && mysql) Object.assign(extractorValues, mysqlCdcValues(runtime));
  return [
    { name: 'extractor', values: extractorValues },
    {
      name: 'filter',
      values: {
        ignore_dbs: '',
        do_dbs: '',
        do_tbs: mappings.map((mapping) => tableName(mapping.source)).join(','),
        ignore_tbs: '',
        do_events: snapshot ? 'insert' : 'insert,update,delete',
      },
    },
    {
      name: 'sinker',
      values: {
        db_type: 'kafka',
        sink_type: 'write',
        batch_size: config.tuning.batchSize,
        required_acks: config.tuning.requiredAcks,
        url: preview ? runtime.targetConnection.preview() : runtime.targetConnection.reveal(),
        ...(config.target.messageFormat === undefined
          ? {}
          : { message_format: config.target.messageFormat }),
        ...(config.target.jsonTemplate === undefined
          ? {}
          : { json_template: config.target.jsonTemplate }),
        ...(config.target.withFieldDefs === undefined
          ? {}
          : { with_field_defs: config.target.withFieldDefs }),
      },
    },
    {
      name: 'router',
      values: {
        db_map: '',
        tb_map: '',
        col_map: '',
        ...(config.kind === 'PG_TO_KAFKA' && config.schemaVersion === 1
          ? { topic_map: `${tableName(config.source.table)}:${config.target.topic}` }
          : {
              topic_fanout_map: mappings
                .map(
                  (mapping) =>
                    `${tableName(mapping.source)}:${mapping.targets.map((target) => target.topic).join('|')}`,
                )
                .join(','),
            }),
      },
    },
    {
      name: 'parallelizer',
      values: { parallel_type: mysql && snapshot ? 'snapshot' : 'serial', parallel_size: 1 },
    },
    {
      name: 'pipeline',
      values: {
        buffer_size: config.tuning.bufferSize,
        checkpoint_interval_secs: config.tuning.checkpointIntervalSeconds,
      },
    },
    ...runtimeSections(runtime),
  ];
}

function kafkaSections(
  config: Extract<PlatformTaskConfig, { kind: 'KAFKA_RAW_FORWARD' }>,
  runtime: ApeDtsRuntimeBindings,
  preview: boolean,
): readonly IniSection[] {
  if (!Number.isSafeInteger(runtime.sourcePartition) || (runtime.sourcePartition ?? -1) < 0) {
    throw new Error('KAFKA_SOURCE_PARTITION_REQUIRED');
  }
  if (config.schemaVersion === 2 && runtime.sourceTopic === undefined) {
    throw new Error('KAFKA_SOURCE_TOPIC_REQUIRED');
  }
  const sourceTopic =
    runtime.sourceTopic ?? (config.schemaVersion === 1 ? config.source.topic : undefined);
  const mapping = kafkaTaskMappings(config).find((candidate) => candidate.source === sourceTopic);
  if (mapping === undefined) throw new Error('KAFKA_SOURCE_TOPIC_INVALID');
  return [
    {
      name: 'extractor',
      values: {
        db_type: 'kafka',
        extract_type: 'cdc',
        url: preview ? runtime.sourceConnection.preview() : runtime.sourceConnection.reveal(),
        group: config.source.groupId,
        topic: mapping.source,
        partition: runtime.sourcePartition as number,
        offset: -1,
        ack_interval_secs: config.tuning.ackIntervalSeconds,
      },
    },
    {
      name: 'sinker',
      values: {
        db_type: 'kafka',
        sink_type: 'write',
        batch_size: config.tuning.batchSize,
        required_acks: config.tuning.requiredAcks,
        url: preview ? runtime.targetConnection.preview() : runtime.targetConnection.reveal(),
      },
    },
    {
      name: 'router',
      values: {
        topic_fanout_map: `${mapping.source}:${mapping.targets.map((target) => target.topic).join('|')}`,
      },
    },
    { name: 'parallelizer', values: { parallel_type: 'serial', parallel_size: 1 } },
    {
      name: 'pipeline',
      values: {
        buffer_size: config.tuning.bufferSize,
        checkpoint_interval_secs: config.tuning.checkpointIntervalSeconds,
      },
    },
    ...runtimeSections(runtime),
  ];
}

function renderConsoleOnlyLog4rs(logLevel: ApeDtsRuntimeBindings['logLevel']): string {
  return [
    'appenders:',
    '  stdout:',
    '    kind: console',
    '    encoder:',
    '      pattern: "{d(%Y-%m-%dT%H:%M:%S%.3fZ)} {l} {m}{n}"',
    'root:',
    `  level: ${logLevel}`,
    '  appenders:',
    '    - stdout',
    '',
  ].join('\n');
}

function renderMysqlLog4rs(runtime: ApeDtsRuntimeBindings): string {
  const appenders = ['checkpoint', 'finished'].flatMap((kind) => [
    `  mysql_${kind}:`,
    kind === 'checkpoint' ? '    kind: rolling_file' : '    kind: file',
    '    append: true',
    `    path: ${JSON.stringify(path.posix.join(runtime.logDirectory, `mysql-${kind}.log`))}`,
    '    encoder:',
    '      pattern: "{m}{n}"',
    ...(kind === 'finished'
      ? []
      : [
          '    policy:',
          '      kind: compound',
          '      trigger:',
          '        kind: size',
          '        limit: 1mb',
          '      roller:',
          '        kind: fixed_window',
          '        base: 1',
          '        count: 1',
          `        pattern: ${JSON.stringify(path.posix.join(runtime.logDirectory, `mysql-${kind}.{}.log`))}`,
        ]),
  ]);
  const console = renderConsoleOnlyLog4rs(runtime.logLevel).split('root:');
  return (
    `${console[0]}${appenders.join('\n')}\nroot:${console[1]}loggers:\n` +
    ['checkpoint', 'finished']
      .map(
        (kind) =>
          `  ${kind === 'checkpoint' ? 'position' : 'finished'}_logger:\n    level: info\n    appenders: [mysql_${kind}]\n    additive: false\n`,
      )
      .join('')
  );
}

export class ApeDtsV1Adapter implements ApeDtsConfigAdapter {
  readonly id = 'ape-dts-ini';
  readonly version = '1';

  render(request: AdapterRenderRequest | MysqlAdapterRenderRequest): RenderedApeDtsConfig {
    validateRuntime(request.runtime);
    let runtimeSectionsResult: readonly IniSection[];
    let previewSectionsResult: readonly IniSection[];
    if (
      request.scenario === 'MYSQL_SNAPSHOT_TO_KAFKA' ||
      request.scenario === 'MYSQL_CDC_TO_KAFKA'
    ) {
      if (request.config.kind !== 'MYSQL_TO_KAFKA')
        throw new Error('ADAPTER_SCENARIO_CONFIG_MISMATCH');
      const config = mysqlToKafkaTaskConfigSchema.parse(request.config);
      runtimeSectionsResult = rdbSections(config, request.runtime, request.scenario, false);
      previewSectionsResult = rdbSections(config, request.runtime, request.scenario, true);
    } else if (request.scenario === 'KAFKA_RAW_FORWARD') {
      if (request.config.kind !== 'KAFKA_RAW_FORWARD') {
        throw new Error('ADAPTER_SCENARIO_CONFIG_MISMATCH');
      }
      runtimeSectionsResult = kafkaSections(request.config, request.runtime, false);
      previewSectionsResult = kafkaSections(request.config, request.runtime, true);
    } else {
      if (request.config.kind !== 'PG_TO_KAFKA') {
        throw new Error('ADAPTER_SCENARIO_CONFIG_MISMATCH');
      }
      runtimeSectionsResult = rdbSections(request.config, request.runtime, request.scenario, false);
      previewSectionsResult = rdbSections(request.config, request.runtime, request.scenario, true);
    }
    return {
      log4rsYaml:
        request.config.kind === 'MYSQL_TO_KAFKA'
          ? renderMysqlLog4rs(request.runtime)
          : renderConsoleOnlyLog4rs(request.runtime.logLevel),
      previewIni: renderIni(previewSectionsResult),
      runtimeIni: renderIni(runtimeSectionsResult),
    };
  }
}
