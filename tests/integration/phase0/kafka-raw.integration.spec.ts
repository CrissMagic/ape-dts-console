import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  ApeDtsV1Adapter,
  parsePlatformTaskConfig,
  runtimeText,
} from '../../../packages/config-model/dist/index.js';

const repositoryRoot = fileURLToPath(new URL('../../..', import.meta.url));
const composeFile = fileURLToPath(new URL('../../../docker/compose.phase0.yml', import.meta.url));
const composeProjectName = `scs-phase0-raw-${process.pid}-${Date.now()}`;
const candidateArchive = process.env.SCS_PHASE0_APE_DTS_ARCHIVE;
const runtimeImage = process.env.SCS_PHASE0_RUNTIME_IMAGE ?? 'centos:7';

if (candidateArchive === undefined || candidateArchive.trim().length === 0) {
  throw new Error('SCS_PHASE0_APE_DTS_ARCHIVE is required for Phase 0 certification');
}

let environmentStarted = false;
const candidateContainerNames = new Set<string>();
const runtimeDirectories = new Set<string>();

interface RawCandidate {
  readonly containerName: string;
  readonly metricsPort: number;
  readonly runtimeDirectory: string;
}

interface RawCandidateOptions {
  readonly groupId: string;
  readonly limits?: { readonly cpus: string; readonly memory: string };
  readonly metricsPort: number;
  readonly sourcePartition: number;
  readonly sourceTopic: string;
  readonly targetTopic: string;
}

function docker(arguments_: readonly string[], timeout = 180_000): string {
  return execFileSync('docker', arguments_, {
    cwd: repositoryRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

function compose(arguments_: readonly string[], timeout = 180_000): string {
  return docker(
    ['compose', '--project-name', composeProjectName, '--file', composeFile, ...arguments_],
    timeout,
  );
}

function redactConnectionStrings(value: string): string {
  return value.replace(/:\/\/[^@\s]+@/gu, '://[REDACTED]@');
}

function lastLogLines(value: string): string {
  return redactConnectionStrings(value).split(/\r?\n/gu).filter(Boolean).slice(-10).join('\n');
}

function quoteForShell(value: string): string {
  return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

function startCandidate(
  containerName: string,
  candidateRuntimeDirectory: string,
  limits: { readonly cpus: string; readonly memory: string } = { cpus: '4', memory: '8g' },
): void {
  docker([
    'run',
    '--detach',
    '--name',
    containerName,
    '--network',
    `${composeProjectName}_default`,
    '--cpus',
    limits.cpus,
    '--memory',
    limits.memory,
    '--mount',
    `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
    '--mount',
    `type=bind,source=${candidateRuntimeDirectory},target=/run/ape-dts`,
    '--entrypoint',
    '/bin/bash',
    runtimeImage,
    '-lc',
    'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
  ]);
  candidateContainerNames.add(containerName);
}

function removeCandidate(containerName: string): void {
  docker(['rm', '--force', containerName]);
  candidateContainerNames.delete(containerName);
}

async function removeRuntimeDirectory(directory: string): Promise<void> {
  await rm(directory, { force: true, recursive: true });
  runtimeDirectories.delete(directory);
}

async function waitForCandidateLog(containerName: string, expectedLog: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const inspection = JSON.parse(docker(['inspect', containerName])) as Array<{
      State: { Running: boolean; Status: string };
    }>;
    const state = inspection[0]?.State;
    const consoleOutput = docker(['logs', containerName]);
    if (consoleOutput.includes(expectedLog)) {
      return;
    }
    if (state?.Status === 'exited') {
      throw new Error(
        `candidate raw process exited before readiness: ${lastLogLines(consoleOutput)}`,
      );
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `candidate raw process did not log ${expectedLog}: ${lastLogLines(
      docker(['logs', containerName]),
    )}`,
  );
}

function requestCandidateHttp(containerName: string, port: number, pathname: string): string {
  return docker(
    [
      'exec',
      containerName,
      '/bin/bash',
      '-lc',
      `exec 3<>/dev/tcp/127.0.0.1/${port}; printf 'GET ${pathname} HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3; cat <&3`,
    ],
    10_000,
  );
}

function candidateResourceLimits(containerName: string): { memory: number; nanoCpus: number } {
  const inspection = JSON.parse(docker(['inspect', containerName])) as Array<{
    HostConfig: { Memory: number; NanoCpus: number };
  }>;
  const hostConfig = inspection[0]?.HostConfig;
  if (hostConfig === undefined) {
    throw new Error(`candidate container ${containerName} was not inspectable`);
  }
  return { memory: hostConfig.Memory, nanoCpus: hostConfig.NanoCpus };
}

async function startRawCandidate({
  groupId,
  limits,
  metricsPort,
  sourcePartition,
  sourceTopic,
  targetTopic,
}: RawCandidateOptions): Promise<RawCandidate> {
  const runtimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-raw-unit-'));
  runtimeDirectories.add(runtimeDirectory);
  const rendered = new ApeDtsV1Adapter().render({
    config: parsePlatformTaskConfig({
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 1,
      source: {
        dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d24',
        groupId,
        topic: sourceTopic,
      },
      target: {
        dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d25',
        topic: targetTopic,
      },
      tuning: {
        ackIntervalSeconds: 1,
        batchSize: 1,
        bufferSize: 100,
        checkpointIntervalSeconds: 1,
        requiredAcks: 'all',
      },
    }),
    runtime: {
      log4rsPath: '/run/ape-dts/log4rs.yaml',
      logDirectory: '/run/ape-dts/logs',
      logLevel: 'info',
      metricsPort,
      sourceConnection: runtimeText('kafka:9092', 'kafka:9092'),
      sourcePartition,
      targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
    },
    scenario: 'KAFKA_RAW_FORWARD',
  });
  await mkdir(path.join(runtimeDirectory, 'logs'));
  await writeFile(path.join(runtimeDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
  await writeFile(path.join(runtimeDirectory, 'log4rs.yaml'), rendered.log4rsYaml, 'utf8');

  const containerName = `${composeProjectName}-partition-${sourcePartition}-${metricsPort}`;
  startCandidate(containerName, runtimeDirectory, limits);
  return { containerName, metricsPort, runtimeDirectory };
}

function createTopic(topic: string, partitions = 1): void {
  compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-topics.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--create',
    '--if-not-exists',
    '--topic',
    topic,
    '--partitions',
    String(partitions),
    '--replication-factor',
    '1',
  ]);
}

function alterTopicPartitions(topic: string, partitions: number): void {
  compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-topics.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--alter',
    '--topic',
    topic,
    '--partitions',
    String(partitions),
  ]);
}

function topicEndOffsets(topic: string): ReadonlyMap<number, number> {
  const output = compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-get-offsets.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--topic',
    topic,
  ]);
  const offsets = new Map<number, number>();
  for (const line of output.split(/\r?\n/gu)) {
    const parts = line.trim().split(':');
    if (parts.length !== 3 || parts[0] !== topic) {
      continue;
    }
    const partition = Number(parts[1]);
    const offset = Number(parts[2]);
    if (!Number.isSafeInteger(partition) || !Number.isSafeInteger(offset)) {
      throw new Error(`Kafka reported an invalid topic offset: ${line}`);
    }
    offsets.set(partition, offset);
  }
  return offsets;
}

async function waitForTopicPartitionCount(topic: string, expectedCount: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (topicEndOffsets(topic).size === expectedCount) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Kafka topic ${topic} did not reach ${expectedCount} partitions`);
}

async function waitForTopicPartitionEndOffset(
  topic: string,
  partition: number,
  expectedOffset: number,
): Promise<void> {
  const deadline = Date.now() + 30_000;
  let latestOffset: number | undefined;
  while (Date.now() < deadline) {
    latestOffset = topicEndOffsets(topic).get(partition);
    if (latestOffset === expectedOffset) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `Kafka topic ${topic} partition ${partition} did not reach ${expectedOffset}; latest ${latestOffset}`,
  );
}

function topicEndOffsetTotal(offsets: ReadonlyMap<number, number>): number {
  return [...offsets.values()].reduce((total, offset) => total + offset, 0);
}

async function waitForTopicEndOffsetTotal(
  topic: string,
  expectedOffset: number,
): Promise<ReadonlyMap<number, number>> {
  const deadline = Date.now() + 30_000;
  let latestOffsets = topicEndOffsets(topic);
  while (Date.now() < deadline) {
    latestOffsets = topicEndOffsets(topic);
    if (topicEndOffsetTotal(latestOffsets) === expectedOffset) {
      return latestOffsets;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `Kafka topic ${topic} did not reach total end offset ${expectedOffset}; latest ${topicEndOffsetTotal(
      latestOffsets,
    )}`,
  );
}

function produceRawRecords(topic: string, records: readonly string[], roundRobin = false): void {
  const producerProperties = roundRobin
    ? [
        `--producer-property ${quoteForShell(
          'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner',
        )}`,
        `--producer-property ${quoteForShell('linger.ms=0')}`,
      ]
    : [];
  const producerCommand = [
    `printf '%s\\n' ${records.map(quoteForShell).join(' ')}`,
    '| /opt/kafka/bin/kafka-console-producer.sh',
    '--bootstrap-server kafka:9092',
    `--topic ${quoteForShell(topic)}`,
    '--sync',
    ...producerProperties,
    '--property parse.headers=true',
    '--property parse.key=true',
    "--property 'null.marker=<null>'",
  ].join(' ');
  compose(['exec', '--no-TTY', 'kafka', '/bin/bash', '-lc', producerCommand]);
}

function consumeFormattedRecords(
  topic: string,
  maxMessages: number,
  offset: string,
  partition = 0,
): string {
  return compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--topic',
    topic,
    '--partition',
    String(partition),
    '--offset',
    offset,
    '--max-messages',
    String(maxMessages),
    '--timeout-ms',
    '30000',
    '--formatter',
    'kafka.tools.DefaultMessageFormatter',
    '--property',
    'print.timestamp=false',
    '--property',
    'print.partition=true',
    '--property',
    'print.offset=true',
    '--property',
    'print.headers=true',
    '--property',
    'print.key=true',
    '--property',
    'print.value=true',
    '--property',
    'key.separator=|',
    '--property',
    'headers.separator=,',
    '--property',
    'null.literal=<null>',
  ]);
}

// The extractor's "starts" log precedes consumer creation. A new latest group
// has no fixed start position until Kafka initializes it, so log/HTTP health
// cannot be used as evidence that a subsequent record is live input.
async function establishRawReadiness(sourceTopic: string, targetTopic: string, groupId: string) {
  const deadline = Date.now() + 30_000;
  let probe = 0;
  while (Date.now() < deadline) {
    produceRawRecords(sourceTopic, [
      `readiness:probe\treadiness-${++probe}\treadiness-probe-${probe}`,
    ]);
    const targetOffset = topicEndOffsets(targetTopic).get(0);
    if (targetOffset === undefined) throw new Error('Readiness Target partition is unavailable');
    if (targetOffset > 0) {
      const sourceOffset = topicEndOffsets(sourceTopic).get(0);
      if (sourceOffset === undefined) throw new Error('Readiness Source offset is unavailable');
      // Wait for the probe just produced, not merely any earlier probe, to be
      // acknowledged. No business sample is retried by this startup handshake.
      await waitForGroupOffset(groupId, sourceTopic, sourceOffset);
      const targetBaseline = topicEndOffsets(targetTopic).get(0);
      if (targetBaseline === undefined || targetBaseline <= 0)
        throw new Error('Readiness Target offset is unavailable');
      const records = consumeFormattedRecords(targetTopic, targetBaseline, 'earliest');
      const lines = records.split(/\r?\n/gu).filter(Boolean);
      expect(lines).toHaveLength(targetBaseline);
      expect(lines.every((line) => line.includes('readiness-probe-'))).toBe(true);
      expect(records).not.toContain('payload-historical');
      return { sourceOffset, targetOffset: targetBaseline };
    }
  }
  throw new Error('Kafka end-to-end readiness was not established within 30 seconds');
}

function payloadTargetPartitions(
  topic: string,
  expectedPayloads: readonly string[],
  targetOffsets: ReadonlyMap<number, number>,
): ReadonlyMap<string, number> {
  const result = new Map<string, number>();
  for (const [partition, endOffset] of targetOffsets) {
    if (endOffset === 0) {
      continue;
    }
    const records = consumeFormattedRecords(topic, endOffset, 'earliest', partition);
    for (const payload of expectedPayloads) {
      if (!records.includes(payload)) {
        continue;
      }
      if (result.has(payload)) {
        throw new Error(`Target payload ${payload} appeared in more than one partition`);
      }
      result.set(payload, partition);
    }
  }
  if (result.size !== expectedPayloads.length) {
    const missingPayloads = expectedPayloads.filter((payload) => !result.has(payload));
    throw new Error(`Target payloads were missing: ${missingPayloads.join(', ')}`);
  }
  return result;
}

function consumeAllFormattedRecords(topic: string, maxMessages: number): string {
  return compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-console-consumer.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--topic',
    topic,
    '--from-beginning',
    '--max-messages',
    String(maxMessages),
    '--timeout-ms',
    '30000',
    '--formatter',
    'kafka.tools.DefaultMessageFormatter',
    '--property',
    'print.timestamp=false',
    '--property',
    'print.partition=true',
    '--property',
    'print.offset=true',
    '--property',
    'print.headers=true',
    '--property',
    'print.key=true',
    '--property',
    'print.value=true',
    '--property',
    'key.separator=|',
    '--property',
    'headers.separator=,',
    '--property',
    'null.literal=<null>',
  ]);
}

function hasExpectedGroupOffset(
  description: string,
  topic: string,
  expectedOffset: number,
): boolean {
  return description.split(/\r?\n/gu).some((line) => {
    const fields = line.trim().split(/\s+/u);
    return (
      fields[1] === topic &&
      fields[2] === '0' &&
      fields[3] === String(expectedOffset) &&
      fields[4] === String(expectedOffset)
    );
  });
}

function hasExpectedGroupOffsets(
  description: string,
  topic: string,
  expectedOffsets: ReadonlyMap<number, number>,
): boolean {
  return [...expectedOffsets].every(([partition, expectedOffset]) =>
    description.split(/\r?\n/gu).some((line) => {
      const fields = line.trim().split(/\s+/u);
      return (
        fields[1] === topic &&
        fields[2] === String(partition) &&
        fields[3] === String(expectedOffset) &&
        fields[4] === String(expectedOffset)
      );
    }),
  );
}

async function waitForGroupOffset(
  groupId: string,
  topic: string,
  expectedOffset: number,
): Promise<void> {
  const deadline = Date.now() + 30_000;
  let latestDescription = '';
  while (Date.now() < deadline) {
    try {
      latestDescription = compose([
        'exec',
        '--no-TTY',
        'kafka',
        '/opt/kafka/bin/kafka-consumer-groups.sh',
        '--bootstrap-server',
        'kafka:9092',
        '--group',
        groupId,
        '--describe',
      ]);
    } catch {
      latestDescription = '';
    }
    if (hasExpectedGroupOffset(latestDescription, topic, expectedOffset)) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `consumer group ${groupId} did not commit ${expectedOffset} for ${topic}: ${latestDescription}`,
  );
}

async function waitForGroupOffsets(
  groupId: string,
  topic: string,
  expectedOffsets: ReadonlyMap<number, number>,
): Promise<void> {
  const deadline = Date.now() + 30_000;
  let latestDescription = '';
  while (Date.now() < deadline) {
    try {
      latestDescription = compose([
        'exec',
        '--no-TTY',
        'kafka',
        '/opt/kafka/bin/kafka-consumer-groups.sh',
        '--bootstrap-server',
        'kafka:9092',
        '--group',
        groupId,
        '--describe',
      ]);
    } catch {
      latestDescription = '';
    }
    if (hasExpectedGroupOffsets(latestDescription, topic, expectedOffsets)) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `consumer group ${groupId} did not commit every expected offset for ${topic}: ${latestDescription}`,
  );
}

beforeAll(async () => {
  await stat(candidateArchive);
  compose(['up', '--detach', '--wait', '--wait-timeout', '120']);
  environmentStarted = true;
});

afterAll(async () => {
  const failures: unknown[] = [];
  async function attempt(cleanup: () => Promise<void> | void): Promise<void> {
    try {
      await cleanup();
    } catch (error) {
      failures.push(error);
    }
  }

  for (const containerName of candidateContainerNames) {
    await attempt(() => {
      docker(['rm', '--force', containerName]);
      candidateContainerNames.delete(containerName);
    });
  }
  for (const directory of runtimeDirectories) {
    await attempt(async () => {
      await rm(directory, { force: true, recursive: true });
      runtimeDirectories.delete(directory);
    });
  }
  if (environmentStarted) {
    await attempt(() => {
      compose(['down', '--volumes', '--remove-orphans']);
      environmentStarted = false;
    });
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, 'failed to clean up Phase 0 Kafka Raw test resources');
  }
});

describe('Phase 0 Kafka Raw certification', () => {
  it('forwards live records with headers and null key or payload, then resumes from the committed offset', async () => {
    const sourceTopic = 'phase0rawsource';
    const targetTopic = 'phase0rawtarget';
    const groupId = 'phase0rawgroup';
    createTopic(sourceTopic);
    createTopic(targetTopic);
    produceRawRecords(sourceTopic, ['history:ignored\thistorical-key\tpayload-historical']);

    const rendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'KAFKA_RAW_FORWARD',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d24',
          groupId,
          topic: sourceTopic,
        },
        target: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d25',
          topic: targetTopic,
        },
        tuning: {
          ackIntervalSeconds: 1,
          batchSize: 1,
          bufferSize: 100,
          checkpointIntervalSeconds: 1,
          requiredAcks: 'all',
        },
      }),
      runtime: {
        log4rsPath: '/run/ape-dts/log4rs.yaml',
        logDirectory: '/run/ape-dts/logs',
        logLevel: 'info',
        metricsPort: 19092,
        sourceConnection: runtimeText('kafka:9092', 'kafka:9092'),
        sourcePartition: 0,
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'KAFKA_RAW_FORWARD',
    });

    const rawRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-raw-'));
    runtimeDirectories.add(rawRuntimeDirectory);
    await mkdir(path.join(rawRuntimeDirectory, 'logs'));
    await writeFile(path.join(rawRuntimeDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
    await writeFile(path.join(rawRuntimeDirectory, 'log4rs.yaml'), rendered.log4rsYaml, 'utf8');

    const firstContainerName = `${composeProjectName}-first`;
    startCandidate(firstContainerName, rawRuntimeDirectory);
    await waitForCandidateLog(firstContainerName, 'KafkaCdcExtractor starts');
    expect(requestCandidateHttp(firstContainerName, 19092, '/healthz')).toContain('200');
    expect(requestCandidateHttp(firstContainerName, 19092, '/metrics')).toContain('200');

    const baseline = await establishRawReadiness(sourceTopic, targetTopic, groupId);

    produceRawRecords(sourceTopic, [
      'trace:one,kind:raw\tkey-one\tpayload-one',
      'flag:empty-key\t<null>\tpayload-null-key',
      'flag:empty-value\tkey-null-value\t<null>',
    ]);
    const liveRecords = consumeFormattedRecords(targetTopic, 3, String(baseline.targetOffset));
    const liveRecordLines = liveRecords.split(/\r?\n/gu).filter(Boolean);
    if (liveRecordLines.length !== 3) {
      throw new Error(
        `Kafka live delivery mismatch: sourceOffsets=${JSON.stringify([...topicEndOffsets(sourceTopic)])}; targetOffsets=${JSON.stringify([...topicEndOffsets(targetTopic)])}; candidate=${lastLogLines(docker(['logs', firstContainerName]))}`,
      );
    }
    expect(liveRecordLines).toHaveLength(3);
    expect(liveRecords).not.toContain('payload-historical');
    expect(liveRecords).toContain('trace:one');
    expect(liveRecords).toContain('kind:raw');
    expect(liveRecords).toContain('key-one');
    expect(liveRecords).toContain('payload-one');
    expect(liveRecords).toContain('flag:empty-key');
    expect(liveRecords).toContain('payload-null-key');
    expect(liveRecords).toContain('flag:empty-value');
    expect(liveRecords).toContain('key-null-value');
    expect(liveRecords).toContain('<null>');
    await waitForGroupOffset(groupId, sourceTopic, baseline.sourceOffset + 3);

    docker(['kill', firstContainerName]);
    removeCandidate(firstContainerName);

    const restartedContainerName = `${composeProjectName}-restart`;
    startCandidate(restartedContainerName, rawRuntimeDirectory);
    await waitForCandidateLog(restartedContainerName, 'KafkaCdcExtractor starts');
    produceRawRecords(sourceTopic, ['trace:restart\trestart-key\tpayload-after-restart']);
    const restartedRecord = consumeFormattedRecords(
      targetTopic,
      1,
      String(baseline.targetOffset + 3),
    );
    expect(restartedRecord).toContain('payload-after-restart');
    expect(restartedRecord).not.toContain('payload-one');
    await waitForGroupOffset(groupId, sourceTopic, baseline.sourceOffset + 4);

    const consoleOutput = redactConnectionStrings(docker(['logs', restartedContainerName]));
    expect(consoleOutput).not.toContain('phase0:phase0');
    expect(await readdir(path.join(rawRuntimeDirectory, 'logs'))).toEqual([]);

    removeCandidate(restartedContainerName);
    await removeRuntimeDirectory(rawRuntimeDirectory);
  }, 180_000);

  it('forwards live records from every source partition using one shared group within the total 4C/8G task allocation', async () => {
    const sourceTopic = 'phase0rawmultisource';
    const targetTopic = 'phase0rawmultitarget';
    const groupId = 'phase0rawmultigroup';
    const partitions = [0, 1, 2] as const;
    createTopic(sourceTopic, partitions.length);
    createTopic(targetTopic, partitions.length);
    produceRawRecords(
      sourceTopic,
      partitions.map((partition) => `history:ignored\t<null>\thistorical-${partition}`),
      true,
    );

    const candidates = await Promise.all(
      partitions.map(async (sourcePartition) => {
        return await startRawCandidate({
          groupId,
          limits: { cpus: '1.33', memory: '2730m' },
          metricsPort: 19092 + sourcePartition,
          sourcePartition,
          sourceTopic,
          targetTopic,
        });
      }),
    );
    await Promise.all(
      candidates.map(async ({ containerName, metricsPort }) => {
        await waitForCandidateLog(containerName, 'KafkaCdcExtractor starts');
        expect(requestCandidateHttp(containerName, metricsPort, '/healthz')).toContain('200');
      }),
    );
    const resourceLimits = candidates.map(({ containerName }) =>
      candidateResourceLimits(containerName),
    );
    expect(resourceLimits.every((limits) => limits.nanoCpus > 0 && limits.memory > 0)).toBe(true);
    expect(
      resourceLimits.reduce((total, limits) => total + limits.nanoCpus, 0),
    ).toBeLessThanOrEqual(4 * 1_000_000_000);
    expect(resourceLimits.reduce((total, limits) => total + limits.memory, 0)).toBeLessThanOrEqual(
      8 * 1024 * 1024 * 1024,
    );

    const livePayloads = Array.from({ length: 6 }, (_, index) => `payload-multi-${index}`);
    produceRawRecords(
      sourceTopic,
      livePayloads.map((payload) => `trace:multi\t<null>\t${payload}`),
      true,
    );
    const liveRecords = consumeAllFormattedRecords(targetTopic, livePayloads.length);
    const liveRecordLines = liveRecords.split(/\r?\n/gu).filter(Boolean);
    expect(liveRecordLines).toHaveLength(livePayloads.length);
    expect(liveRecords).not.toContain('historical-');
    for (const payload of livePayloads) {
      expect(liveRecords).toContain(payload);
    }
    await waitForGroupOffsets(
      groupId,
      sourceTopic,
      new Map(partitions.map((partition) => [partition, 3])),
    );

    for (const { containerName, runtimeDirectory: candidateRuntimeDirectory } of candidates) {
      removeCandidate(containerName);
      await removeRuntimeDirectory(candidateRuntimeDirectory);
    }
  }, 180_000);

  it('does not forward a record from a Source partition added after the fixed partition units start', async () => {
    const sourceTopic = 'phase0rawexpandedsource';
    const targetTopic = 'phase0rawexpandedtarget';
    const groupId = 'phase0rawexpandedgroup';
    const initialPartitions = [0, 1, 2] as const;
    createTopic(sourceTopic, initialPartitions.length);
    createTopic(targetTopic, initialPartitions.length);

    const candidates = await Promise.all(
      initialPartitions.map(
        async (sourcePartition) =>
          await startRawCandidate({
            groupId,
            limits: { cpus: '1.33', memory: '2730m' },
            metricsPort: 19102 + sourcePartition,
            sourcePartition,
            sourceTopic,
            targetTopic,
          }),
      ),
    );
    await Promise.all(
      candidates.map(async ({ containerName, metricsPort }) => {
        await waitForCandidateLog(containerName, 'KafkaCdcExtractor starts');
        expect(requestCandidateHttp(containerName, metricsPort, '/healthz')).toContain('200');
      }),
    );

    alterTopicPartitions(sourceTopic, 4);
    await waitForTopicPartitionCount(sourceTopic, 4);
    const sourceOffsetsBefore = topicEndOffsets(sourceTopic);
    expect(sourceOffsetsBefore.get(3)).toBe(0);
    const newPartitionKey = 'phase0-partition-key-1';
    produceRawRecords(sourceTopic, [`trace:expanded\t${newPartitionKey}\tpayload-added-partition`]);
    await waitForTopicPartitionEndOffset(sourceTopic, 3, 1);
    for (const partition of initialPartitions) {
      expect(topicEndOffsets(sourceTopic).get(partition)).toBe(sourceOffsetsBefore.get(partition));
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 3_000));
    expect(
      [...topicEndOffsets(targetTopic).values()].reduce((total, offset) => total + offset, 0),
    ).toBe(0);

    for (const { containerName, runtimeDirectory: candidateRuntimeDirectory } of candidates) {
      removeCandidate(containerName);
      await removeRuntimeDirectory(candidateRuntimeDirectory);
    }
  }, 180_000);

  it('changes at least one keyed Target partition after the Target Topic expands and the candidate restarts', async () => {
    const sourceTopic = 'phase0rawtargetmapsource';
    const targetTopic = 'phase0rawtargetmaptarget';
    const groupId = 'phase0rawtargetmapgroup';
    const mappingKeys = Array.from({ length: 12 }, (_, index) => `target-map-key-${index}`);
    const payloadsBefore = mappingKeys.map(
      (_, index) => `payload-before-${String(index).padStart(2, '0')}`,
    );
    const payloadsAfter = mappingKeys.map(
      (_, index) => `payload-after-${String(index).padStart(2, '0')}`,
    );
    createTopic(sourceTopic);
    createTopic(targetTopic, 3);

    const candidate = await startRawCandidate({
      groupId,
      metricsPort: 19112,
      sourcePartition: 0,
      sourceTopic,
      targetTopic,
    });
    await waitForCandidateLog(candidate.containerName, 'KafkaCdcExtractor starts');
    expect(
      requestCandidateHttp(candidate.containerName, candidate.metricsPort, '/healthz'),
    ).toContain('200');

    produceRawRecords(
      sourceTopic,
      mappingKeys.map((key, index) => `trace:target-before\t${key}\t${payloadsBefore[index]}`),
    );
    await waitForGroupOffset(groupId, sourceTopic, mappingKeys.length);
    let targetOffsets = await waitForTopicEndOffsetTotal(targetTopic, mappingKeys.length);
    const partitionsBefore = payloadTargetPartitions(targetTopic, payloadsBefore, targetOffsets);

    alterTopicPartitions(targetTopic, 4);
    await waitForTopicPartitionCount(targetTopic, 4);
    removeCandidate(candidate.containerName);

    const restartedCandidate = await startRawCandidate({
      groupId,
      metricsPort: 19113,
      sourcePartition: 0,
      sourceTopic,
      targetTopic,
    });
    await waitForCandidateLog(restartedCandidate.containerName, 'KafkaCdcExtractor starts');
    expect(
      requestCandidateHttp(
        restartedCandidate.containerName,
        restartedCandidate.metricsPort,
        '/healthz',
      ),
    ).toContain('200');

    produceRawRecords(
      sourceTopic,
      mappingKeys.map((key, index) => `trace:target-after\t${key}\t${payloadsAfter[index]}`),
    );
    await waitForGroupOffset(groupId, sourceTopic, mappingKeys.length * 2);
    targetOffsets = await waitForTopicEndOffsetTotal(targetTopic, mappingKeys.length * 2);
    const partitionsAfter = payloadTargetPartitions(targetTopic, payloadsAfter, targetOffsets);

    const changedMappings = mappingKeys.filter(
      (_, index) =>
        partitionsBefore.get(payloadsBefore[index]) !== partitionsAfter.get(payloadsAfter[index]),
    );
    expect(changedMappings).not.toHaveLength(0);

    removeCandidate(restartedCandidate.containerName);
    await removeRuntimeDirectory(candidate.runtimeDirectory);
    await removeRuntimeDirectory(restartedCandidate.runtimeDirectory);
  }, 180_000);
});
