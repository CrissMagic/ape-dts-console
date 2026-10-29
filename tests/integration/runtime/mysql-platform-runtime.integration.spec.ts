import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const artifact = process.env.SCS_TEST_APE_DTS_ARTIFACT;
const suffix = `${process.pid}-${Date.now()}`;
const prefix = `scs-mysql-platform-${suffix}`;
const network = `${prefix}-network`;
const postgres = `${prefix}-postgres`;
const mysql = `${prefix}-mysql`;
const kafka = `${prefix}-kafka`;
const runner = `${prefix}-runner`;
const postgresImage = process.env.SCS_TEST_POSTGRES_IMAGE ?? 'postgres:12';
const mysqlImage = process.env.SCS_TEST_MYSQL_IMAGE ?? 'mysql:8.0.36';
const kafkaImage = process.env.SCS_TEST_KAFKA_IMAGE ?? 'apache/kafka:3.8.0';
const suppliedRunnerImage = process.env.SCS_MYSQL_PLATFORM_RUNTIME_IMAGE;
const runnerImage = suppliedRunnerImage ?? `${prefix}:toolchain`;
const password = randomBytes(24).toString('hex');
const role = 'mysql_runtime_fixture';
const database = 'mysql_runtime_fixture';
const containers = new Set<string>();
let networkCreated = false;
let imageCreated = false;
let outputDirectory: string | undefined;

function safe(value: string): string {
  return value.replaceAll(password, '[REDACTED]').replace(/:\/\/[^@\s]+@/gu, '://[REDACTED]@');
}

function docker(args: readonly string[], input?: string, timeout = 30_000): string {
  try {
    const result = spawnSync('docker', args, {
      encoding: 'utf8',
      input,
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout,
      maxBuffer: 16 * 1024 * 1024,
    });
    if (result.error !== undefined) throw result.error;
    if (result.status !== 0) throw new Error(result.stderr || `Docker exited ${result.status}`);
    return (args[0] === 'logs' ? result.stdout + result.stderr : result.stdout).trim();
  } catch (error) {
    const detail =
      typeof error === 'object' && error !== null && 'stderr' in error
        ? String(error.stderr)
        : String(error);
    // eslint-disable-next-line preserve-caught-error -- Docker errors include temporary database credentials in argv.
    throw new Error(safe(detail));
  }
}

function start(name: string, args: readonly string[]): void {
  docker(['run', '--pull', 'never', '--detach', '--name', name, '--network', network, ...args]);
  containers.add(name);
}

async function ready(probe: () => void, label: string): Promise<void> {
  const deadline = Date.now() + 90_000;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      probe();
      return;
    } catch (error) {
      last = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`${label} is not ready: ${last instanceof Error ? last.message : String(last)}`);
}

function mount(source: string, target: string): string[] {
  return ['--mount', `type=bind,source=${path.resolve(source)},target=${target},readonly`];
}

beforeAll(async () => {
  if (artifact === undefined || !(await stat(artifact)).isFile())
    throw new Error(
      'SCS_TEST_APE_DTS_ARTIFACT must explicitly name the newly built MySQL checkpoint-safe engine artifact',
    );
  for (const image of [postgresImage, mysqlImage, kafkaImage]) docker(['image', 'inspect', image]);
  if (suppliedRunnerImage === undefined) {
    docker(['build', '--target', 'toolchain', '--tag', runnerImage, '.'], undefined, 600_000);
    imageCreated = true;
  } else docker(['image', 'inspect', suppliedRunnerImage]);
  outputDirectory = await mkdtemp(path.join(tmpdir(), 'scs-mysql-platform-runtime-'));
  docker(['network', 'create', network]);
  networkCreated = true;
  start(postgres, [
    '--network-alias',
    'metadata',
    '--env',
    `POSTGRES_PASSWORD=${password}`,
    postgresImage,
  ]);
  start(mysql, [
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
  start(kafka, [
    '--network-alias',
    'kafka',
    ...Object.entries({
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk',
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093',
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_AUTHORIZER_CLASS_NAME: 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: 'true',
    }).flatMap(([key, value]) => ['--env', `${key}=${value}`]),
    kafkaImage,
  ]);
  await ready(() => {
    docker(['exec', postgres, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres']);
  }, 'metadata PostgreSQL');
  docker(
    ['exec', '--interactive', postgres, 'psql', '-U', 'postgres', '-v', 'ON_ERROR_STOP=1'],
    `CREATE ROLE ${role} LOGIN PASSWORD '${password}'; CREATE DATABASE ${database} OWNER ${role};`,
  );
  await ready(() => {
    docker([
      'exec',
      '--env',
      `MYSQL_PWD=${password}`,
      mysql,
      'mysql',
      '--protocol=TCP',
      '--host=127.0.0.1',
      '-uroot',
      '-e',
      'SELECT 1',
    ]);
  }, 'MySQL');
  await ready(() => {
    docker([
      'exec',
      kafka,
      '/opt/kafka/bin/kafka-topics.sh',
      '--bootstrap-server',
      'kafka:9092',
      '--list',
    ]);
  }, 'Kafka');
  const packageNames = [
    'contracts',
    'security',
    'database',
    'storage',
    'observability',
    'config-model',
  ];
  start(runner, [
    '--cpus',
    '4',
    '--memory',
    '8g',
    '--workdir',
    '/workspace/apps/api',
    ...mount('apps/api/dist', '/workspace/apps/api/dist'),
    ...packageNames.flatMap((name) =>
      mount(`packages/${name}/dist`, `/workspace/packages/${name}/dist`),
    ),
    ...mount('packages/database/migrations', '/workspace/packages/database/migrations'),
    ...mount(
      'tests/integration/runtime/fixtures/mysql-platform-runtime.driver.mjs',
      '/workspace/apps/api/mysql-platform-runtime.driver.mjs',
    ),
    ...mount(artifact, '/fixture/engine-package'),
    '--mount',
    `type=bind,source=${outputDirectory},target=/fixture/output`,
    '--env',
    `SCS_FIXTURE_PASSWORD=${password}`,
    '--env',
    `SCS_FIXTURE_DATABASE=${database}`,
    '--env',
    `SCS_FIXTURE_ROLE=${role}`,
    runnerImage,
    'node',
    'mysql-platform-runtime.driver.mjs',
  ]);
}, 600_000);

afterAll(async () => {
  const errors: unknown[] = [];
  for (const name of containers) {
    try {
      docker(['rm', '--force', '--volumes', name]);
    } catch (error) {
      errors.push(error);
    }
  }
  if (networkCreated) {
    try {
      docker(['network', 'rm', network]);
    } catch (error) {
      errors.push(error);
    }
  }
  if (imageCreated) {
    try {
      docker(['image', 'rm', runnerImage]);
    } catch (error) {
      errors.push(error);
    }
  }
  if (outputDirectory !== undefined) {
    try {
      await rm(outputDirectory, { recursive: true, force: true });
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length)
    throw new AggregateError(errors, 'isolated MySQL platform runtime cleanup failed');
});

describe('real MySQL platform runtime', () => {
  it('runs snapshot, fanout, committed-position resume, system recovery and local failures without skipping rejected writes', async () => {
    const deadline = Date.now() + 480_000;
    let brokerWritePolicy: 'ALLOW' | 'REJECT' = 'ALLOW';
    let abruptRestarts = 0;
    let abruptKillSent = false;
    while (Date.now() < deadline) {
      if (outputDirectory === undefined) throw new Error('runtime output is unavailable');
      if (!abruptKillSent) {
        try {
          await stat(path.join(outputDirectory, 'crash-requested.json'));
          docker(['kill', '--signal', 'KILL', runner]);
          abruptKillSent = true;
        } catch (error) {
          if (!(
            typeof error === 'object' &&
            error !== null &&
            'code' in error &&
            error.code === 'ENOENT'
          ))
            throw error;
        }
      }
      if (docker(['inspect', '--format', '{{.State.Running}}', runner]) !== 'true') {
        const exit = docker(['inspect', '--format', '{{.State.ExitCode}}', runner]);
        if (exit !== '137' || abruptRestarts !== 0) break;
        const crashMarker: unknown = JSON.parse(
          await readFile(path.join(outputDirectory, 'crash-requested.json'), 'utf8'),
        );
        expect(crashMarker).toHaveProperty('processBootId');
        expect(docker(['inspect', '--format', '{{.State.OOMKilled}}', runner])).toBe('false');
        docker([
          'exec',
          '--env',
          `MYSQL_PWD=${password}`,
          mysql,
          'mysql',
          '--protocol=TCP',
          '--host=127.0.0.1',
          '-uroot',
          'data_demo',
          '-e',
          "INSERT INTO items VALUES(4,'while-node-killed')",
        ]);
        docker(['start', runner]);
        abruptRestarts += 1;
        continue;
      }
      let requested: string | null = null;
      try {
        requested = await readFile(path.join(outputDirectory, 'broker-request.json'), 'utf8');
      } catch (error) {
        if (!(
          typeof error === 'object' &&
          error !== null &&
          'code' in error &&
          error.code === 'ENOENT'
        ))
          throw error;
      }
      if (requested !== null) {
        const desired: unknown = JSON.parse(requested);
        if (desired !== 'ALLOW' && desired !== 'REJECT')
          throw new Error('Unexpected isolated Broker fault request');
        if (desired !== brokerWritePolicy) {
          const acl = [
            'exec',
            kafka,
            '/opt/kafka/bin/kafka-acls.sh',
            '--bootstrap-server',
            'kafka:9092',
          ];
          if (desired === 'REJECT') {
            docker([
              ...acl,
              '--add',
              '--allow-principal',
              'User:ANONYMOUS',
              '--operation',
              'Read',
              '--operation',
              'Describe',
              '--topic',
              'archive',
            ]);
            docker([
              ...acl,
              '--add',
              '--deny-principal',
              'User:ANONYMOUS',
              '--operation',
              'Write',
              '--topic',
              'archive',
            ]);
          } else docker([...acl, '--remove', '--force', '--topic', 'archive']);
          const observed = docker([...acl, '--list', '--topic', 'archive']);
          if (desired === 'REJECT') {
            expect(observed).toContain('name=archive');
            expect(observed).toMatch(
              /principal=User:ANONYMOUS[^\n]*operation=WRITE[^\n]*permissionType=DENY/u,
            );
          } else expect(observed).not.toContain('permissionType=DENY');
          await writeFile(
            path.join(outputDirectory, `broker-acl-${desired.toLowerCase()}.txt`),
            observed,
          );
          brokerWritePolicy = desired;
          await writeFile(
            path.join(outputDirectory, 'broker-applied.tmp'),
            JSON.stringify(desired),
          );
          await rename(
            path.join(outputDirectory, 'broker-applied.tmp'),
            path.join(outputDirectory, 'broker-applied.json'),
          );
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    if (docker(['inspect', '--format', '{{.State.Running}}', runner]) === 'true')
      throw new Error(
        `MySQL runtime fixture timed out: ${safe(docker(['logs', runner])).slice(-10_000)}`,
      );
    const exit = docker(['inspect', '--format', '{{.State.ExitCode}}', runner]);
    if (exit !== '0')
      throw new Error(
        `MySQL platform runner exited ${exit}: ${safe(docker(['logs', runner])).slice(-15_000)}`,
      );
    if (outputDirectory === undefined) throw new Error('runtime output is unavailable');
    expect(abruptRestarts).toBe(1);
    expect(abruptKillSent).toBe(true);
    const result: unknown = JSON.parse(
      await readFile(path.join(outputDirectory, 'result.json'), 'utf8'),
    );
    expect(result).toEqual({
      snapshotAndFanout: true,
      stoppedResume: true,
      systemRecovery: true,
      abruptNodeRestart: true,
      expiredTaskIsLocal: true,
      brokerRejectedTransactionRetainsCheckpoint: true,
      historicalMetricsRetained: true,
    });
  }, 500_000);
});
