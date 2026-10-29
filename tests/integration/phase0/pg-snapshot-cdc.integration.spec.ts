import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
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
const composeProjectName = `scs-phase0-snapshot-${process.pid}-${Date.now()}`;
const candidateArchive = process.env.SCS_PHASE0_APE_DTS_ARCHIVE;
const runtimeImage = process.env.SCS_PHASE0_RUNTIME_IMAGE ?? 'centos:7';

if (candidateArchive === undefined || candidateArchive.trim().length === 0) {
  throw new Error('SCS_PHASE0_APE_DTS_ARCHIVE is required for Phase 0 certification');
}

let environmentStarted = false;
const candidateContainerNames = new Set<string>();
const runtimeDirectories = new Set<string>();

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

function signalDiagnostic(value: string): string {
  const lines = redactConnectionStrings(value).split(/\r?\n/gu).filter(Boolean);
  const signalLine = lines.findIndex((line) => line.includes('SIGTERM received'));
  const relevantLines = signalLine >= 0 ? lines.slice(signalLine, signalLine + 3) : [];
  return [...relevantLines, ...lines.slice(-3)].join('\n');
}

function lastLogLines(value: string): string {
  return redactConnectionStrings(value).split(/\r?\n/gu).filter(Boolean).slice(-10).join('\n');
}

async function waitForCandidateToRun(containerName: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const inspection = JSON.parse(docker(['inspect', containerName])) as Array<{
      State: { Running: boolean; Status: string };
    }>;
    const state = inspection[0]?.State;
    if (state?.Running) {
      return;
    }
    if (state?.Status === 'exited') {
      const consoleOutput = redactConnectionStrings(docker(['logs', containerName]));
      throw new Error(`candidate CDC process exited before DML: ${consoleOutput}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error('candidate CDC process did not become running within 30 seconds');
}

async function waitForCandidateLog(containerName: string, expectedLog: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const inspection = JSON.parse(docker(['inspect', containerName])) as Array<{
      State: { Status: string };
    }>;
    const state = inspection[0]?.State;
    const consoleOutput = docker(['logs', containerName]);
    if (consoleOutput.includes(expectedLog)) {
      return;
    }
    if (state?.Status === 'exited') {
      throw new Error(`candidate exited before ${expectedLog}: ${lastLogLines(consoleOutput)}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `candidate did not log ${expectedLog}: ${lastLogLines(docker(['logs', containerName]))}`,
  );
}

function readTopicEndOffset(topic: string): number {
  const offsets = compose([
    'exec',
    '--no-TTY',
    'kafka',
    '/opt/kafka/bin/kafka-get-offsets.sh',
    '--bootstrap-server',
    'kafka:9092',
    '--topic',
    topic,
    '--partitions',
    '0',
  ]);
  const matchingLine = offsets.split(/\r?\n/gu).find((line) => line.startsWith(`${topic}:0:`));
  if (matchingLine === undefined) {
    throw new Error(`Kafka did not report a partition 0 end offset for ${topic}: ${offsets}`);
  }
  const offset = Number(matchingLine.split(':')[2]);
  if (!Number.isSafeInteger(offset)) {
    throw new Error(`Kafka reported an invalid end offset for ${topic}: ${matchingLine}`);
  }
  return offset;
}

async function waitForTopicEndOffset(topic: string, minimumOffset: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  let latestOffset = 0;
  while (Date.now() < deadline) {
    latestOffset = readTopicEndOffset(topic);
    if (latestOffset >= minimumOffset) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `Kafka target topic ${topic} did not reach offset ${minimumOffset}; latest ${latestOffset}`,
  );
}

function consumeValues(topic: string, offset: number, maxMessages: number): string {
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
    '0',
    '--offset',
    String(offset),
    '--max-messages',
    String(maxMessages),
    '--timeout-ms',
    '30000',
    '--formatter',
    'kafka.tools.DefaultMessageFormatter',
    '--property',
    'print.offset=true',
    '--property',
    'print.value=true',
  ]);
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

  for (const candidateContainerName of candidateContainerNames) {
    await attempt(() => {
      docker(['rm', '--force', candidateContainerName]);
      candidateContainerNames.delete(candidateContainerName);
    });
  }
  for (const runtimeDirectory of runtimeDirectories) {
    await attempt(async () => {
      await rm(runtimeDirectory, { force: true, recursive: true });
      runtimeDirectories.delete(runtimeDirectory);
    });
  }
  if (environmentStarted) {
    await attempt(() => {
      compose(['down', '--volumes', '--remove-orphans']);
      environmentStarted = false;
    });
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, 'failed to clean up Phase 0 PostgreSQL test resources');
  }
});

describe('Phase 0 PostgreSQL snapshot certification', () => {
  it('transfers every seeded PostgreSQL row to Kafka through the candidate binary', async () => {
    const topic = 'phase0snapshot';
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "CREATE TABLE public.phase0_orders (id integer PRIMARY KEY, payload text NOT NULL); INSERT INTO public.phase0_orders (id, payload) VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');",
    ]);
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
      '1',
      '--replication-factor',
      '1',
    ]);

    const rendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'PG_TO_KAFKA',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d20',
          includeSnapshot: true,
          publicationName: 'phase0_pub',
          resourceMode: 'SELF_MANAGED',
          slotName: 'phase0_slot',
          table: { name: 'phase0_orders', schema: 'public' },
        },
        target: { dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d21', topic },
        tuning: {
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
        metricsPort: 19090,
        sourceConnection: runtimeText(
          'postgres://phase0:phase0@postgres:5432/phase0',
          'postgres://[REDACTED]@postgres:5432/phase0',
        ),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'PG_SNAPSHOT_TO_KAFKA',
    });

    const snapshotRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-snapshot-'));
    runtimeDirectories.add(snapshotRuntimeDirectory);
    await mkdir(path.join(snapshotRuntimeDirectory, 'logs'));
    await writeFile(path.join(snapshotRuntimeDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
    await writeFile(
      path.join(snapshotRuntimeDirectory, 'log4rs.yaml'),
      rendered.log4rsYaml,
      'utf8',
    );

    const containerName = `${composeProjectName}-snapshot`;
    candidateContainerNames.add(containerName);
    docker([
      'run',
      '--detach',
      '--name',
      containerName,
      '--network',
      `${composeProjectName}_default`,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--mount',
      `type=bind,source=${snapshotRuntimeDirectory},target=/run/ape-dts`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
    ]);

    const exitCode = docker(['wait', containerName]);
    if (exitCode !== '0') {
      const consoleOutput = redactConnectionStrings(docker(['logs', containerName]));
      throw new Error(`candidate snapshot process exited with ${exitCode}: ${consoleOutput}`);
    }

    const candidateInspection = JSON.parse(docker(['inspect', containerName])) as Array<{
      HostConfig: { Memory: number; NanoCpus: number };
    }>;
    expect(candidateInspection[0]?.HostConfig).toMatchObject({
      Memory: 8 * 1024 * 1024 * 1024,
      NanoCpus: 4 * 1_000_000_000,
    });

    const records = compose([
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
      '3',
      '--timeout-ms',
      '10000',
      '--formatter',
      'kafka.tools.DefaultMessageFormatter',
      '--property',
      'print.offset=true',
      '--property',
      'print.value=false',
    ]);
    const recordLines = records.split(/\r?\n/u).filter(Boolean);
    expect(recordLines).toEqual(['Offset:0', 'Offset:1', 'Offset:2']);

    const consoleOutput = redactConnectionStrings(docker(['logs', containerName]));
    expect(consoleOutput).not.toContain('phase0:phase0');

    docker(['rm', containerName]);
    candidateContainerNames.delete(containerName);
    await rm(snapshotRuntimeDirectory, { force: true, recursive: true });
    runtimeDirectories.delete(snapshotRuntimeDirectory);
  }, 180_000);

  it('captures PostgreSQL insert, update, and delete after a pre-created slot and publication', async () => {
    const topic = 'phase0cdc';
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      'CREATE TABLE public.phase0_cdc_orders (id integer PRIMARY KEY, payload text NOT NULL); CREATE PUBLICATION phase0pub FOR TABLE public.phase0_cdc_orders;',
    ]);
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "SELECT * FROM pg_create_logical_replication_slot('phase0slot', 'pgoutput');",
    ]);
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
      '1',
      '--replication-factor',
      '1',
    ]);

    const rendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'PG_TO_KAFKA',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d22',
          includeSnapshot: false,
          publicationName: 'phase0pub',
          resourceMode: 'SELF_MANAGED',
          slotName: 'phase0slot',
          table: { name: 'phase0_cdc_orders', schema: 'public' },
        },
        target: { dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d23', topic },
        tuning: {
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
        metricsPort: 19091,
        sourceConnection: runtimeText(
          'postgres://phase0:phase0@postgres:5432/phase0',
          'postgres://[REDACTED]@postgres:5432/phase0',
        ),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'PG_CDC_TO_KAFKA',
    });

    const cdcRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-cdc-'));
    runtimeDirectories.add(cdcRuntimeDirectory);
    await mkdir(path.join(cdcRuntimeDirectory, 'logs'));
    await writeFile(path.join(cdcRuntimeDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
    await writeFile(path.join(cdcRuntimeDirectory, 'log4rs.yaml'), rendered.log4rsYaml, 'utf8');

    const containerName = `${composeProjectName}-cdc`;
    candidateContainerNames.add(containerName);
    const candidateArguments = [
      'run',
      '--detach',
      '--name',
      containerName,
      '--network',
      `${composeProjectName}_default`,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--mount',
      `type=bind,source=${cdcRuntimeDirectory},target=/run/ape-dts`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
    ];
    docker(candidateArguments);
    await waitForCandidateToRun(containerName);

    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "INSERT INTO public.phase0_cdc_orders (id, payload) VALUES (1, 'alpha'); UPDATE public.phase0_cdc_orders SET payload = 'bravo' WHERE id = 1; DELETE FROM public.phase0_cdc_orders WHERE id = 1;",
    ]);

    const records = compose([
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
      '3',
      '--timeout-ms',
      '30000',
      '--formatter',
      'kafka.tools.DefaultMessageFormatter',
      '--property',
      'print.offset=true',
      '--property',
      'print.value=false',
    ]);
    expect(records.split(/\r?\n/u).filter(Boolean)).toEqual(['Offset:0', 'Offset:1', 'Offset:2']);

    // Stop without waiting for this tail to reach Kafka. Recovery may replay, but must not omit it.
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "INSERT INTO public.phase0_cdc_orders (id, payload) SELECT n, 'shutdown-tail-' || n FROM generate_series(100, 199) n;",
    ]);
    docker(['stop', '--time', '15', containerName], 30_000);
    const exitCode = docker(['inspect', '--format', '{{.State.ExitCode}}', containerName]);
    if (exitCode !== '0') {
      const consoleOutput = signalDiagnostic(docker(['logs', containerName]));
      throw new Error(
        `candidate CDC process exited with ${exitCode} after SIGTERM: ${consoleOutput}`,
      );
    }

    const consoleOutput = redactConnectionStrings(docker(['logs', containerName]));
    expect(consoleOutput).not.toContain('phase0:phase0');
    const checkpoints = [
      ...consoleOutput.matchAll(/checkpoint_position \|[^\r\n]*"lsn":"([^"]+)"/gu),
    ];
    const checkpoint = checkpoints.at(-1)?.[1];
    expect(checkpoint).toBeDefined();
    const restartIni = rendered.runtimeIni.replace(
      /^start_lsn\s*=.*$/mu,
      `start_lsn=${checkpoint}`,
    );
    expect(restartIni).not.toBe(rendered.runtimeIni);
    await writeFile(path.join(cdcRuntimeDirectory, 'config.ini'), restartIni, 'utf8');
    docker(['rm', containerName]);
    docker(candidateArguments);
    await waitForCandidateToRun(containerName);
    const deadline = Date.now() + 30_000;
    let recoveredIds = new Set<string>();
    while (Date.now() < deadline) {
      const endOffset = readTopicEndOffset(topic);
      if (endOffset > 0) {
        const recovered = consumeValues(topic, 0, endOffset);
        recoveredIds = new Set(
          [...recovered.matchAll(/shutdown-tail-(\d+)/gu)].map((match) => match[1]!),
        );
        if (recoveredIds.size === 100) break;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    expect([...recoveredIds].sort()).toEqual(
      Array.from({ length: 100 }, (_, index) => String(index + 100)),
    );
    docker(['stop', '--time', '15', containerName], 30_000);
    expect(docker(['inspect', '--format', '{{.State.ExitCode}}', containerName])).toBe('0');

    docker(['rm', containerName]);
    candidateContainerNames.delete(containerName);
    await rm(cdcRuntimeDirectory, { force: true, recursive: true });
    runtimeDirectories.delete(cdcRuntimeDirectory);
  }, 180_000);

  it('keeps every change made after snapshot output available to the pre-created CDC slot', async () => {
    const topic = 'phase0window';
    const snapshotRowCount = 10_000;
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "CREATE TABLE public.phase0_window_orders (id integer PRIMARY KEY, payload text NOT NULL); INSERT INTO public.phase0_window_orders (id, payload) SELECT id, repeat('x', 1024) FROM generate_series(1, 10000) AS id; CREATE PUBLICATION phase0windowpub FOR TABLE public.phase0_window_orders;",
    ]);
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "SELECT * FROM pg_create_logical_replication_slot('phase0windowslot', 'pgoutput');",
    ]);
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
      '1',
      '--replication-factor',
      '1',
    ]);

    const snapshotRendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'PG_TO_KAFKA',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d26',
          includeSnapshot: true,
          publicationName: 'phase0windowpub',
          resourceMode: 'SELF_MANAGED',
          slotName: 'phase0windowslot',
          table: { name: 'phase0_window_orders', schema: 'public' },
        },
        target: { dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d27', topic },
        tuning: {
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
        metricsPort: 19093,
        sourceConnection: runtimeText(
          'postgres://phase0:phase0@postgres:5432/phase0',
          'postgres://[REDACTED]@postgres:5432/phase0',
        ),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'PG_SNAPSHOT_TO_KAFKA',
    });

    const windowRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-window-'));
    runtimeDirectories.add(windowRuntimeDirectory);
    await mkdir(path.join(windowRuntimeDirectory, 'logs'));
    await writeFile(
      path.join(windowRuntimeDirectory, 'config.ini'),
      snapshotRendered.runtimeIni,
      'utf8',
    );
    await writeFile(
      path.join(windowRuntimeDirectory, 'log4rs.yaml'),
      snapshotRendered.log4rsYaml,
      'utf8',
    );

    const snapshotContainerName = `${composeProjectName}-window-snapshot`;
    candidateContainerNames.add(snapshotContainerName);
    docker([
      'run',
      '--detach',
      '--name',
      snapshotContainerName,
      '--network',
      `${composeProjectName}_default`,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--mount',
      `type=bind,source=${windowRuntimeDirectory},target=/run/ape-dts`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
    ]);
    await waitForCandidateLog(snapshotContainerName, 'PgSnapshotExtractor starts');
    await waitForTopicEndOffset(topic, 100);

    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "INSERT INTO public.phase0_window_orders (id, payload) VALUES (10001, 'insert-during-snapshot'); UPDATE public.phase0_window_orders SET payload = 'updated-during-snapshot' WHERE id = 1; DELETE FROM public.phase0_window_orders WHERE id = 2;",
    ]);

    const snapshotExitCode = docker(['wait', snapshotContainerName], 120_000);
    if (snapshotExitCode !== '0') {
      throw new Error(
        `candidate window snapshot exited with ${snapshotExitCode}: ${lastLogLines(
          docker(['logs', snapshotContainerName]),
        )}`,
      );
    }
    const snapshotEndOffset = readTopicEndOffset(topic);
    expect(snapshotEndOffset).toBeGreaterThanOrEqual(snapshotRowCount);
    expect(snapshotEndOffset).toBeLessThanOrEqual(snapshotRowCount + 1);

    docker(['rm', snapshotContainerName]);
    candidateContainerNames.delete(snapshotContainerName);

    const cdcRendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'PG_TO_KAFKA',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d26',
          includeSnapshot: false,
          publicationName: 'phase0windowpub',
          resourceMode: 'SELF_MANAGED',
          slotName: 'phase0windowslot',
          table: { name: 'phase0_window_orders', schema: 'public' },
        },
        target: { dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d27', topic },
        tuning: {
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
        metricsPort: 19094,
        sourceConnection: runtimeText(
          'postgres://phase0:phase0@postgres:5432/phase0',
          'postgres://[REDACTED]@postgres:5432/phase0',
        ),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'PG_CDC_TO_KAFKA',
    });
    await writeFile(
      path.join(windowRuntimeDirectory, 'config.ini'),
      cdcRendered.runtimeIni,
      'utf8',
    );
    await writeFile(
      path.join(windowRuntimeDirectory, 'log4rs.yaml'),
      cdcRendered.log4rsYaml,
      'utf8',
    );

    const cdcContainerName = `${composeProjectName}-window-cdc`;
    candidateContainerNames.add(cdcContainerName);
    docker([
      'run',
      '--detach',
      '--name',
      cdcContainerName,
      '--network',
      `${composeProjectName}_default`,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--mount',
      `type=bind,source=${windowRuntimeDirectory},target=/run/ape-dts`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
    ]);
    await waitForCandidateLog(cdcContainerName, 'PgCdcExtractor starts');

    const cdcRecords = consumeValues(topic, snapshotEndOffset, 3);
    expect(cdcRecords.split(/\r?\n/gu).filter(Boolean)).toHaveLength(3);
    expect(cdcRecords).toContain(`Offset:${snapshotEndOffset}`);
    expect(cdcRecords).toContain('insert-during-snapshot');
    expect(cdcRecords).toContain('updated-during-snapshot');
    expect(cdcRecords).toContain('\finsert');
    expect(cdcRecords).toContain('\fupdate');
    expect(cdcRecords).toContain('\fdelete');

    const consoleOutput = redactConnectionStrings(docker(['logs', cdcContainerName]));
    expect(consoleOutput).not.toContain('phase0:phase0');

    docker(['rm', '--force', cdcContainerName]);
    candidateContainerNames.delete(cdcContainerName);
    await rm(windowRuntimeDirectory, { force: true, recursive: true });
    runtimeDirectories.delete(windowRuntimeDirectory);
  }, 180_000);

  it('transfers a PostgreSQL row to a target topic containing dots without panicking', async () => {
    const topic = 'phase0.pg.snapshot';
    compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--command',
      "CREATE TABLE public.phase0_dotted_orders (id integer PRIMARY KEY, payload text NOT NULL); INSERT INTO public.phase0_dotted_orders (id, payload) VALUES (1, 'dotted-topic');",
    ]);
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
      '1',
      '--replication-factor',
      '1',
    ]);

    const rendered = new ApeDtsV1Adapter().render({
      config: parsePlatformTaskConfig({
        kind: 'PG_TO_KAFKA',
        schemaVersion: 1,
        source: {
          dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d28',
          includeSnapshot: true,
          publicationName: 'phase0dottedpub',
          resourceMode: 'SELF_MANAGED',
          slotName: 'phase0dottedslot',
          table: { name: 'phase0_dotted_orders', schema: 'public' },
        },
        target: { dataSourceId: '018f7bf0-21d6-7f12-a741-1d2c34c94d29', topic },
        tuning: {
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
        metricsPort: 19095,
        sourceConnection: runtimeText(
          'postgres://phase0:phase0@postgres:5432/phase0',
          'postgres://[REDACTED]@postgres:5432/phase0',
        ),
        targetConnection: runtimeText('kafka:9092', 'kafka:9092'),
      },
      scenario: 'PG_SNAPSHOT_TO_KAFKA',
    });

    const dottedRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-phase0-dotted-'));
    runtimeDirectories.add(dottedRuntimeDirectory);
    await mkdir(path.join(dottedRuntimeDirectory, 'logs'));
    await writeFile(path.join(dottedRuntimeDirectory, 'config.ini'), rendered.runtimeIni, 'utf8');
    await writeFile(path.join(dottedRuntimeDirectory, 'log4rs.yaml'), rendered.log4rsYaml, 'utf8');

    const containerName = `${composeProjectName}-dotted`;
    candidateContainerNames.add(containerName);
    docker([
      'run',
      '--detach',
      '--name',
      containerName,
      '--network',
      `${composeProjectName}_default`,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--mount',
      `type=bind,source=${dottedRuntimeDirectory},target=/run/ape-dts`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; exec /tmp/ape-dts/ape-dts --config /run/ape-dts/config.ini',
    ]);

    expect(docker(['wait', containerName])).toBe('0');
    const consoleOutput = redactConnectionStrings(docker(['logs', containerName]));
    expect(consoleOutput).not.toContain('panic');
    expect(consoleOutput).not.toContain('phase0:phase0');
    expect(consumeValues(topic, 0, 1)).toContain('dotted-topic');

    docker(['rm', containerName]);
    candidateContainerNames.delete(containerName);
    await rm(dottedRuntimeDirectory, { force: true, recursive: true });
    runtimeDirectories.delete(dottedRuntimeDirectory);
  }, 180_000);
});
