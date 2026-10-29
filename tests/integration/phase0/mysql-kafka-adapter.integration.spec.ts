import { execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  ApeDtsV1Adapter,
  mysqlToKafkaTaskConfigSchema,
  runtimeText,
} from '../../../packages/config-model/src/index.js';

const archive = process.env.SCS_PHASE0_APE_DTS_ARCHIVE;
const prefix = `scs-mysql-kafka-${process.pid}-${Date.now()}`;
const network = `${prefix}-network`;
const mysqlContainer = `${prefix}-mysql`;
const kafkaContainer = `${prefix}-kafka`;
const runtimeImage = process.env.SCS_PHASE0_RUNTIME_IMAGE ?? 'node:24.19.0-bookworm-slim';
const mysqlImage = process.env.SCS_TEST_MYSQL_IMAGE ?? 'mysql:8.0.36';
const kafkaImage = process.env.SCS_TEST_KAFKA_IMAGE ?? 'apache/kafka:3.8.0';
const password = 'isolated-mysql-kafka-test-only';
const containers = new Set<string>();
const directories = new Set<string>();
let networkCreated = false;

function safeText(value: string): string {
  return value.replaceAll(password, '[REDACTED]').replace(/:\/\/[^@\s]+@/gu, '://[REDACTED]@');
}

function docker(args: readonly string[], timeout = 30_000): string {
  try {
    return execFileSync('docker', args, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout,
    }).trim();
  } catch (error) {
    const stderr =
      typeof error === 'object' && error !== null && 'stderr' in error
        ? String(error.stderr)
        : 'Docker command failed';
    // eslint-disable-next-line preserve-caught-error -- Docker's original error can expose credentials in command arguments.
    throw new Error(safeText(stderr));
  }
}

function start(name: string, args: readonly string[]): void {
  docker(['run', '--pull', 'never', '--detach', '--name', name, '--network', network, ...args]);
  containers.add(name);
}

function sql(statement: string): string {
  return docker([
    'exec',
    '--env',
    `MYSQL_PWD=${password}`,
    mysqlContainer,
    'mysql',
    '--user=root',
    '--batch',
    '--skip-column-names',
    '--database=data_demo',
    '--execute',
    statement,
  ]);
}

async function untilReady(operation: () => void, label: string): Promise<void> {
  const deadline = Date.now() + 90_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      operation();
      return;
    } catch (error) {
      lastError = error;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `${label} did not become ready: ${lastError instanceof Error ? lastError.message : 'unknown failure'}`,
  );
}

function kafka(args: readonly string[], timeout = 30_000): string {
  return docker(['exec', kafkaContainer, ...args], timeout);
}

function records(
  topic: string,
  count: number,
): { action: string; table: string; data: { id: number; note: string }[] }[] {
  const output = kafka([
    '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--topic',
    topic,
    '--partition',
    '0',
    '--offset',
    'earliest',
    '--max-messages',
    String(count),
    '--timeout-ms',
    '20000',
  ]);
  return output
    .split(/\r?\n/u)
    .filter(Boolean)
    .map(
      (line) =>
        JSON.parse(line) as { action: string; table: string; data: { id: number; note: string }[] },
    );
}

beforeAll(async () => {
  if (archive === undefined || !(await stat(archive)).isFile())
    throw new Error('SCS_PHASE0_APE_DTS_ARCHIVE must reference the ape-dts tar.gz to test');
  for (const image of [mysqlImage, kafkaImage, runtimeImage]) docker(['image', 'inspect', image]);
  docker(['network', 'create', network]);
  networkCreated = true;
  start(mysqlContainer, [
    '--network-alias',
    'mysql',
    '--env',
    'MYSQL_DATABASE=data_demo',
    '--env',
    'MYSQL_ROOT_HOST=%',
    '--env',
    `MYSQL_ROOT_PASSWORD=${password}`,
    mysqlImage,
    '--server-id=17',
    '--log-bin=mysql-bin',
    '--binlog-format=ROW',
    '--binlog-row-image=FULL',
  ]);
  start(kafkaContainer, [
    '--network-alias',
    'kafka',
    ...Object.entries({
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093',
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092',
      KAFKA_NODE_ID: '1',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
    }).flatMap(([key, value]) => ['--env', `${key}=${value}`]),
    kafkaImage,
  ]);
  await untilReady(() => {
    sql('SELECT 1');
  }, 'MySQL');
  await untilReady(() => {
    kafka(['/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--list']);
  }, 'Kafka');
  sql(
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, note VARCHAR(64) NOT NULL); CREATE TABLE items (id BIGINT PRIMARY KEY, note VARCHAR(64) NOT NULL); INSERT INTO orders VALUES (1, 'initial-order'); INSERT INTO items VALUES (1, 'initial-item');",
  );
  for (const topic of ['orders', 'archive', 'items'])
    kafka([
      '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server',
      'kafka:9092',
      '--create',
      '--topic',
      topic,
      '--partitions',
      '1',
      '--replication-factor',
      '1',
    ]);
});

afterAll(async () => {
  const failures: unknown[] = [];
  for (const name of containers) {
    try {
      docker(['rm', '--force', '--volumes', name]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (networkCreated) {
    try {
      docker(['network', 'rm', network]);
    } catch (error) {
      failures.push(error);
    }
  }
  for (const directory of directories) {
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length) throw new AggregateError(failures, 'isolated MySQL/Kafka cleanup failed');
});

describe('rendered MySQL to Kafka engine adapter', () => {
  it('copies two snapshot tables and explicit-position CDC to every mapped topic as CloudCanal JSON', async () => {
    if (archive === undefined) throw new Error('artifact path is required');
    const [binlogFilename, rawPosition] = sql('SHOW MASTER STATUS').split(/\s/u);
    if (!binlogFilename || !rawPosition)
      throw new Error('MySQL did not expose a Binlog start position');
    const config = mysqlToKafkaTaskConfigSchema.parse({
      kind: 'MYSQL_TO_KAFKA',
      schemaVersion: 2,
      source: { dataSourceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', includeSnapshot: true },
      target: {
        dataSourceId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4',
        messageFormat: 'json',
        jsonTemplate: 'cloudcanal',
        withFieldDefs: true,
      },
      mappings: [
        {
          source: { schema: 'data_demo', name: 'orders' },
          targets: [{ topic: 'orders' }, { topic: 'archive' }],
        },
        { source: { schema: 'data_demo', name: 'items' }, targets: [{ topic: 'items' }] },
      ],
      tuning: {
        batchSize: 200,
        bufferSize: 16000,
        checkpointIntervalSeconds: 1,
        requiredAcks: 'all',
      },
    });
    const directory = await mkdtemp(path.join(tmpdir(), 'scs-mysql-kafka-adapter-'));
    directories.add(directory);
    async function run(stage: 'snapshot' | 'cdc'): Promise<string> {
      const stageDirectory = path.join(directory, stage);
      await mkdir(path.join(stageDirectory, 'logs'), { recursive: true });
      const rendered = new ApeDtsV1Adapter().render({
        config,
        runtime: {
          log4rsPath: '/run/ape-dts/log4rs.yaml',
          logDirectory: '/run/ape-dts/logs',
          logLevel: 'info',
          metricsPort: 19090,
          sourceConnection: runtimeText(
            `mysql://root:${password}@mysql:3306/data_demo?ssl-mode=disabled`,
            'mysql://root:[REDACTED]@mysql:3306/data_demo?ssl-mode=disabled',
          ),
          targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
          mysqlCdc: {
            binlogFilename: binlogFilename!,
            binlogPosition: Number(rawPosition),
            serverId: 2000,
          },
        },
        scenario: stage === 'snapshot' ? 'MYSQL_SNAPSHOT_TO_KAFKA' : 'MYSQL_CDC_TO_KAFKA',
      });
      await writeFile(path.join(stageDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
      await writeFile(path.join(stageDirectory, 'log4rs.yaml'), rendered.log4rsYaml, 'utf8');
      const name = `${prefix}-${stage}`;
      start(name, [
        '--cpus',
        '4',
        '--memory',
        '8g',
        '--mount',
        `type=bind,source=${path.resolve(archive!)},target=/artifact/candidate.tar.gz,readonly`,
        '--mount',
        `type=bind,source=${stageDirectory},target=/run/ape-dts`,
        '--entrypoint',
        '/bin/sh',
        runtimeImage,
        '-ec',
        'mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
      ]);
      return name;
    }
    const snapshot = await run('snapshot');
    const exit = docker(['wait', snapshot], 60_000);
    if (exit !== '0')
      throw new Error(`snapshot failed: ${safeText(docker(['logs', snapshot])).slice(-4000)}`);
    const snapshotOrders = records('orders', 1);
    expect(snapshotOrders).toMatchObject([
      { action: 'INSERT', table: 'orders', data: [{ id: 1, note: 'initial-order' }] },
    ]);
    expect(records('archive', 1)).toEqual(snapshotOrders);
    expect(records('items', 1)).toMatchObject([
      { action: 'INSERT', table: 'items', data: [{ id: 1, note: 'initial-item' }] },
    ]);

    sql(
      "INSERT INTO orders VALUES (2, 'after-snapshot'); UPDATE orders SET note='changed-order' WHERE id=1; DELETE FROM orders WHERE id=2; INSERT INTO items VALUES (2, 'new-item');",
    );
    const cdc = await run('cdc');
    await untilReady(() => {
      const running = docker(['inspect', '--format', '{{.State.Running}}', cdc]);
      if (running !== 'true')
        throw new Error(`CDC failed: ${safeText(docker(['logs', cdc])).slice(-4000)}`);
      if (!docker(['logs', cdc]).includes('MysqlCdcExtractor starts'))
        throw new Error('CDC has not started yet');
    }, 'CDC runner');
    const orderRecords = records('orders', 4);
    expect(orderRecords.map((record) => record.action)).toEqual([
      'INSERT',
      'INSERT',
      'UPDATE',
      'DELETE',
    ]);
    expect(orderRecords[1]?.data).toMatchObject([{ id: 2, note: 'after-snapshot' }]);
    expect(orderRecords[2]?.data).toMatchObject([{ id: 1, note: 'changed-order' }]);
    expect(records('archive', 4)).toEqual(orderRecords);
    expect(records('items', 2).at(-1)?.data).toMatchObject([{ id: 2, note: 'new-item' }]);
  }, 180_000);
});
