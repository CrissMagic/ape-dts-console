import { execFileSync } from 'node:child_process';
import { createServer } from 'node:net';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Kafka } from 'kafkajs';

import { KafkaDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/kafka-data-source.adapter.js';

const containerName = `scs-unit6-kafka-${process.pid}-${Date.now()}`;
const kafkaImage = process.env.SCS_TEST_KAFKA_IMAGE ?? 'apache/kafka:3.8.0';
const clusterId = 'MkU3OEVBNTcwNTJENDM2Qk';
const topicName = `unit6-live-${process.pid}-${Date.now()}`;

let containerCreated = false;
let hostPort = 0;

function docker(arguments_: readonly string[], timeout = 120_000): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

function brokerTopicId(name: string): string {
  const description = docker([
    'exec',
    containerName,
    '/opt/kafka/bin/kafka-topics.sh',
    '--bootstrap-server',
    '127.0.0.1:29092',
    '--topic',
    name,
    '--describe',
  ]);
  const match = /\bTopicId:\s+([A-Za-z0-9_-]{22})\b/u.exec(description);
  if (match === null) {
    throw new Error(`Kafka CLI did not return a TopicId for ${name}`);
  }
  return match[1]!;
}

async function availablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        server.close(() => reject(new Error('unable to allocate Kafka test port')));
        return;
      }
      const { port } = address;
      server.close((error) => (error === undefined ? resolve(port) : reject(error)));
    });
  });
}

async function waitForKafka(): Promise<void> {
  const deadline = Date.now() + 90_000;
  let lastError: unknown;
  const adapter = new KafkaDataSourceAdapter();
  const input = {
    configuration: {
      bootstrapServers: [`127.0.0.1:${hostPort}`],
      plaintextRiskAcknowledged: true as const,
      type: 'KAFKA' as const,
    },
    password: null,
  };
  while (Date.now() < deadline) {
    try {
      await adapter.test(input);
      return;
    } catch (error) {
      lastError = error;
      await new Promise<void>((resolve) => setTimeout(resolve, 500));
    }
  }
  throw lastError instanceof Error ? lastError : new Error('Kafka did not become ready');
}

beforeAll(async () => {
  hostPort = await availablePort();
  docker([
    'run',
    '--pull',
    'never',
    '--detach',
    '--name',
    containerName,
    '--publish',
    `127.0.0.1:${hostPort}:9092`,
    '--env',
    `CLUSTER_ID=${clusterId}`,
    '--env',
    'KAFKA_AUTO_CREATE_TOPICS_ENABLE=false',
    '--env',
    'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
    '--env',
    'KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093',
    '--env',
    'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
    '--env',
    'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL',
    '--env',
    'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT',
    '--env',
    `KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:${hostPort},INTERNAL://127.0.0.1:29092`,
    '--env',
    'KAFKA_LISTENERS=EXTERNAL://:9092,INTERNAL://:29092,CONTROLLER://:9093',
    '--env',
    'KAFKA_NODE_ID=1',
    '--env',
    'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
    '--env',
    'KAFKA_PROCESS_ROLES=broker,controller',
    '--env',
    'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
    '--env',
    'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
    kafkaImage,
  ]);
  containerCreated = true;
  await waitForKafka();
  const kafka = new Kafka({ brokers: [`127.0.0.1:${hostPort}`], clientId: 'scs-unit6-fixture' });
  const administrator = kafka.admin();
  try {
    await administrator.connect();
    await administrator.createTopics({
      topics: [{ numPartitions: 3, replicationFactor: 1, topic: topicName }],
      waitForLeaders: true,
    });
  } finally {
    await administrator.disconnect().catch(() => undefined);
  }
}, 120_000);

afterAll(() => {
  if (!containerCreated) {
    return;
  }
  docker(['rm', '--force', containerName]);
});

describe('Kafka data source adapter', () => {
  it('reads a live cluster identity plus existing Topics and Partitions without managing them', async () => {
    const adapter = new KafkaDataSourceAdapter();
    const input = {
      configuration: {
        bootstrapServers: [`127.0.0.1:${hostPort}`],
        plaintextRiskAcknowledged: true as const,
        type: 'KAFKA' as const,
      },
      password: null,
    };

    await expect(adapter.test(input)).resolves.toMatchObject({
      kind: 'KAFKA_CLUSTER',
      value: expect.any(String),
    });
    await expect(adapter.metadata(input, { resource: 'TOPICS' })).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ name: topicName })]),
    );
    await expect(
      adapter.metadata(input, { resource: 'PARTITIONS', topic: topicName }),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: '0' }),
        expect.objectContaining({ name: '1' }),
        expect.objectContaining({ name: '2' }),
      ]),
    );
  }, 120_000);

  it('reads a stable Topic ID and detects same-name Topic recreation', async () => {
    const adapter = new KafkaDataSourceAdapter();
    const input = {
      configuration: {
        bootstrapServers: [`127.0.0.1:${hostPort}`],
        plaintextRiskAcknowledged: true as const,
        type: 'KAFKA' as const,
      },
      password: null,
    };
    const firstTopic = (await adapter.metadata(input, { resource: 'TOPICS' })).find(
      (topic) => topic.name === topicName,
    );
    expect(firstTopic).toMatchObject({
      identity: expect.stringMatching(/^[A-Za-z0-9_-]{22}$/u),
      name: topicName,
    });
    expect(firstTopic?.identity).toBe(brokerTopicId(topicName));
    expect(
      (await adapter.metadata(input, { resource: 'TOPICS' })).find(
        (topic) => topic.name === topicName,
      )?.identity,
    ).toBe(firstTopic?.identity);

    const kafka = new Kafka({
      brokers: [`127.0.0.1:${hostPort}`],
      clientId: 'scs-unit6-topic-recreation',
    });
    const administrator = kafka.admin();
    try {
      await administrator.connect();
      await administrator.deleteTopics({ timeout: 30_000, topics: [topicName] });
      await administrator.createTopics({
        topics: [{ numPartitions: 3, replicationFactor: 1, topic: topicName }],
        waitForLeaders: true,
      });
    } finally {
      await administrator.disconnect().catch(() => undefined);
    }

    const recreatedTopic = (await adapter.metadata(input, { resource: 'TOPICS' })).find(
      (topic) => topic.name === topicName,
    );
    expect(recreatedTopic).toMatchObject({
      identity: expect.stringMatching(/^[A-Za-z0-9_-]{22}$/u),
      name: topicName,
    });
    expect(recreatedTopic?.identity).toBe(brokerTopicId(topicName));
    expect(recreatedTopic?.identity).not.toBe(firstTopic?.identity);
  }, 120_000);
});
