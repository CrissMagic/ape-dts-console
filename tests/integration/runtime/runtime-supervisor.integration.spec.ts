import { execFileSync } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { PassThrough } from 'node:stream';

import { Test } from '@nestjs/testing';
import {
  ApeDtsV1Adapter,
  CertifiedAdapterRegistry,
  parsePlatformTaskConfig,
  type KafkaTaskMapping,
} from '../../../packages/config-model/src/index.js';
import { ArtifactCache, LocalObjectStorage } from '../../../packages/storage/src/index.js';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import type { AlertsService } from '../../../apps/api/src/alerts/alerts.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { BootstrapPersistenceService } from '../../../apps/api/src/bootstrap/bootstrap-persistence.service.js';
import { PostgresRecoveryStore } from '../../../apps/api/src/recovery/postgres-recovery.store.js';
import { TaskRecoveryService } from '../../../apps/api/src/recovery/task-recovery.service.js';
import { CapacityReservationService } from '../../../apps/api/src/pg-resources/capacity-reservation.service.js';
import { PgResourceManager } from '../../../apps/api/src/pg-resources/pg-resource-manager.js';
import { DataSourcesService } from '../../../apps/api/src/data-sources/data-sources.service.js';
import { DataSourceAdapterRegistry } from '../../../apps/api/src/data-sources/adapters/types.js';
import { ContinuityService } from '../../../apps/api/src/preflight/continuity.service.js';
import { PreflightService } from '../../../apps/api/src/preflight/preflight.service.js';
import { RuntimeEnvironmentInspector } from '../../../apps/api/src/preflight/runtime-environment-check.js';
import { PostgresRecoveryExternalActions } from '../../../apps/api/src/recovery/recovery-external-actions.js';
import {
  RuntimeSupervisorError,
  RuntimeSupervisorService,
} from '../../../apps/api/src/runtime/runtime-supervisor.service.js';
import { RuntimeMetricsPortAllocator } from '../../../apps/api/src/runtime/runtime-metrics-port-allocator.js';
import { MysqlRuntimeStateStore } from '../../../apps/api/src/runtime/mysql-runtime-state.js';
import {
  RuntimeReadNotFoundError,
  RuntimeReadService,
} from '../../../apps/api/src/runtime/runtime-read.service.js';
import {
  FixedAutoRecoveryCoordinator,
  type AutoRecoveryScheduler,
} from '../../../apps/api/src/runtime/fixed-auto-recovery.js';
import { RuntimeModule } from '../../../apps/api/src/runtime/runtime.module.js';
import {
  RuntimeProcessSupervisor,
  type RuntimeChildProcess,
  type RuntimeLogSink,
  type RuntimeProcessFactory,
  type RuntimeSpawnSpec,
} from '../../../apps/api/src/runtime/process-supervisor.js';
import {
  ControlledRuntimeLaunchSpecFactory,
  UnconfiguredRuntimeLaunchSpecFactory,
} from '../../../apps/api/src/runtime/runtime-launch-spec.js';
import { PostgresLogSegmentStore } from '../../../apps/api/src/storage/postgres-log-segment-store.js';
import { LOCAL_LOG_STORAGE_ROOT } from '../../../apps/api/src/storage/storage.tokens.js';
import { TasksService } from '../../../apps/api/src/tasks/tasks.service.js';
import {
  AuthenticatedRuntimeLaunchSpecError,
  AuthenticatedRuntimeLaunchSpecFactory,
} from '../../../apps/api/src/runtime/authenticated-runtime-launch-spec.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import type { TaskPreflightResult, UserSummary } from '../../../packages/contracts/src/index.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
  inspectControllerLockHolders,
  tryAcquireControllerLock,
} from '../../../packages/database/src/index.js';
import { waitForUpgradeControllerLock } from '../../../apps/api/src/bootstrap/upgrade-controller-lock.js';
import { LogPipeline, RuntimeLogPipelineSink } from '../../../packages/observability/src/index.js';
import { segmentObjectKey, type LogSegmentRecord } from '../../../packages/storage/src/index.js';

const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const databaseRole = `scs_u8_${suffix}`;
const metadataDatabase = `scs_u8_meta_${suffix}`;
const databasePassword = randomBytes(24).toString('base64url');

let pool: Pool | undefined;
let platformDatabase: PlatformDatabaseService | undefined;
const runtimePlatformInstanceId = randomUUID();
let postgresPort: number | undefined;
let runtime: RuntimeSupervisorService | undefined;
let launchFactoryRoot: string | undefined;
const temporaryDirectories: string[] = [];

const administrator: UserSummary = {
  displayName: 'Unit 8 administrator',
  id: randomUUID(),
  role: 'ADMIN',
  status: 'ACTIVE',
  username: `unit8-admin-${suffix}`,
};

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
}

function psqlAsContainerAdministrator(database: string, sql: string): void {
  docker(
    [
      'exec',
      '--interactive',
      postgresContainer,
      'psql',
      '--username',
      'postgres',
      '--dbname',
      database,
      '--set',
      'ON_ERROR_STOP=1',
    ],
    sql,
  );
}

function requirePool(): Pool {
  if (pool === undefined) {
    throw new Error('Unit 8 test pool is not initialized');
  }
  return pool;
}

function requireRuntime(): RuntimeSupervisorService {
  if (runtime === undefined) {
    throw new Error('Unit 8 runtime service is not initialized');
  }
  return runtime;
}

async function runtimeRowCounts(taskId: string): Promise<{
  readonly operations: string;
  readonly reservations: string;
  readonly runs: string;
}> {
  const result = await requirePool().query<{
    operations: string;
    reservations: string;
    runs: string;
  }>(
    `SELECT
       (SELECT count(*)::text FROM ape_dts_platform.task_operations WHERE task_id = $1::uuid) AS operations,
       (SELECT count(*)::text FROM ape_dts_platform.task_runs WHERE task_id = $1::uuid) AS runs,
       (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE task_id = $1::uuid) AS reservations`,
    [taskId],
  );
  const row = result.rows[0];
  if (row === undefined) {
    throw new Error('Unit 8 runtime row count query returned no row');
  }
  return row;
}

async function expectStartRejection(
  taskId: string,
  before: { readonly operations: string; readonly reservations: string; readonly runs: string },
  failureCode: string,
): Promise<void> {
  const after = await runtimeRowCounts(taskId);
  expect(after).toEqual({
    operations: String(Number(before.operations) + 1),
    reservations: before.reservations,
    runs: before.runs,
  });
  const read = await new RuntimeReadService(platformDatabase as PlatformDatabaseService).get(
    administrator,
    taskId,
  );
  expect(read.run).toBeNull();
  expect(read.operation).toMatchObject({
    action: 'START',
    failureCode,
    runId: null,
    state: 'REJECTED',
    taskId,
  });
  expect(read.operation?.failureSummary).toBe(`启动前检查未通过（${failureCode}）。`);
}

async function runtimeTerminalState(
  taskId: string,
  runId: string,
): Promise<{
  readonly activeReservations: string;
  readonly observedState: string;
  readonly runFailureCode: string | null;
  readonly runStatus: string;
  readonly unitStatuses: string[];
}> {
  const result = await requirePool().query<{
    active_reservations: string;
    observed_state: string;
    run_failure_code: string | null;
    run_status: string;
    unit_statuses: string[];
  }>(
    `SELECT tasks.observed_state,
            (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
            (SELECT failure_code FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_failure_code,
            (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state IN ('ACTIVE', 'RESERVED')) AS active_reservations,
            (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $2::uuid) AS unit_statuses
       FROM ape_dts_platform.tasks
      WHERE tasks.id = $1::uuid`,
    [taskId, runId],
  );
  const row = result.rows[0];
  if (row === undefined) {
    throw new Error('Unit 8 runtime terminal state was not found');
  }
  return {
    activeReservations: row.active_reservations,
    observedState: row.observed_state,
    runFailureCode: row.run_failure_code,
    runStatus: row.run_status,
    unitStatuses: row.unit_statuses,
  };
}

async function runtimePortReleaseStates(runId: string): Promise<readonly boolean[]> {
  const result = await requirePool().query<{ released: boolean }>(
    `SELECT metrics_port_released_at IS NOT NULL AS released
       FROM ape_dts_platform.task_run_units
      WHERE run_id = $1::uuid
      ORDER BY unit_key`,
    [runId],
  );
  return result.rows.map((row) => row.released);
}

class FakeChildProcess extends EventEmitter {
  readonly stdout = new PassThrough();
  readonly stderr = new PassThrough();
  readonly signals: NodeJS.Signals[] = [];
  readonly pid: number | undefined;
  private closed = false;

  constructor(
    pid: number | undefined,
    private readonly closeOn: NodeJS.Signals | null,
  ) {
    super();
    this.pid = pid;
  }

  kill(signal: NodeJS.Signals): boolean {
    this.signals.push(signal);
    if (this.closeOn === signal) {
      this.close(0, signal);
    }
    return true;
  }

  close(code: number | null, signal: NodeJS.Signals | null): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.stdout.end();
    this.stderr.end();
    this.emit('close', code, signal);
  }
}

const spawnedChildren: FakeChildProcess[] = [];

class RecordingSink implements RuntimeLogSink {
  readonly values: string[] = [];

  append(entry: { readonly chunk: Buffer }): void {
    this.values.push(entry.chunk.toString('utf8'));
  }

  close(): void {
    // The integration focuses on process and database state; log storage is covered separately.
  }
}

function testRuntimePreflight(overrides: Record<string, unknown> = {}): never {
  return {
    revalidateContinuityBeforeRuntime: vi.fn(
      async (task: { readonly id: string; readonly revisionId: string }) => {
        const facts = await requirePool().query<{
          source_cluster: string;
          source_topic: string | null;
          target_cluster: string;
          target_topic: string | null;
        }>(
          `SELECT result #>> '{identities,source}' AS source_cluster,
                  result #>> '{identities,sourceTopic}' AS source_topic,
                  result #>> '{identities,target}' AS target_cluster,
                  result #>> '{identities,targetTopic}' AS target_topic
             FROM ape_dts_platform.preflight_runs
            WHERE task_revision_id = $1::uuid
              AND state = 'PASSED'
            ORDER BY completed_at DESC, id DESC
            LIMIT 1`,
          [task.revisionId],
        );
        const fact = facts.rows[0];
        if (fact === undefined || fact.source_cluster === null || fact.target_cluster === null) {
          throw new Error('runtime continuity test fixture facts are unavailable');
        }
        const baseline = await requirePool().query<{ id: string }>(
          `INSERT INTO ape_dts_platform.task_continuity_baselines
             (id, task_id, task_revision_id, baseline_kind, source_cluster_id, source_topic_id,
              source_partition_ids, source_partition_offsets, target_cluster_id, target_topic_id,
              target_partition_count)
           VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA', $4, $5,
                   ARRAY[0, 1]::integer[], '{"0":0,"1":0}'::jsonb, $6, $7, 1)
           ON CONFLICT (task_id) DO UPDATE
             SET task_revision_id = EXCLUDED.task_revision_id,
                 source_cluster_id = EXCLUDED.source_cluster_id,
                 source_topic_id = EXCLUDED.source_topic_id,
                 source_partition_ids = EXCLUDED.source_partition_ids,
                 source_partition_offsets = EXCLUDED.source_partition_offsets,
                 target_cluster_id = EXCLUDED.target_cluster_id,
                 target_topic_id = EXCLUDED.target_topic_id,
                 target_partition_count = EXCLUDED.target_partition_count,
                 row_version = ape_dts_platform.task_continuity_baselines.row_version + 1,
                 updated_at = statement_timestamp()
           RETURNING id`,
          [
            randomUUID(),
            task.id,
            task.revisionId,
            fact.source_cluster,
            fact.source_topic,
            fact.target_cluster,
            fact.target_topic,
          ],
        );
        const baselineId = baseline.rows[0]?.id;
        if (baselineId === undefined) {
          throw new Error('runtime continuity test baseline was not persisted');
        }
        await requirePool().query(
          `INSERT INTO ape_dts_platform.task_continuity_observations
             (id, task_id, task_revision_id, baseline_id, observation, decision,
              decision_code, actor_type, actor_user_id)
           VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::jsonb,
                   'MATCHED', 'CONTINUITY_MATCHED', 'SYSTEM', NULL)`,
          [
            randomUUID(),
            task.id,
            task.revisionId,
            baselineId,
            JSON.stringify({
              kind: 'KAFKA',
              source: {
                clusterId: fact.source_cluster,
                partitionIds: [0, 1],
                topicId: fact.source_topic,
              },
              target: {
                clusterId: fact.target_cluster,
                partitionCount: 1,
                partitionIds: [],
                topicId: fact.target_topic,
              },
            }),
          ],
        );
      },
    ),
    ...overrides,
  } as never;
}

function testRuntimePreflightWithoutContinuityFacts(): never {
  return {
    revalidateContinuityBeforeRuntime: vi.fn(async () => undefined),
  } as never;
}

function testMetricsPortAllocator(): RuntimeMetricsPortAllocator {
  return new RuntimeMetricsPortAllocator({ maxPort: 55_000, minPort: 45_000 }, async () => true);
}

function testLaunchFactory(): ControlledRuntimeLaunchSpecFactory {
  if (launchFactoryRoot === undefined) {
    throw new Error('Unit 8 launch factory root is not initialized');
  }
  return new ControlledRuntimeLaunchSpecFactory({
    environment: { PATH: process.env.PATH ?? '' },
    executablePath: process.execPath,
    render: (input) => ({
      iniContent: `[runtime]\nunit=${input.unitKey}\n`,
      log4rsContent: 'appenders: []\n',
    }),
    runtimeRoot: launchFactoryRoot,
  });
}

function supervisorWithProcesses(
  children: FakeChildProcess[],
  databaseOverride?: PlatformDatabaseService,
  removeFile?: (filePath: string) => Promise<void>,
  preflight: never = testRuntimePreflight(),
  beforeStart?: (runId: string) => Promise<void>,
): RuntimeSupervisorService {
  const database = databaseOverride ?? platformDatabase;
  if (database === undefined) {
    throw new Error('Unit 8 platform database is not initialized');
  }
  return new RuntimeSupervisorService(
    database,
    new AuditLedgerService(database),
    new CapacityReservationService(database),
    new RuntimeProcessSupervisor({
      factory: {
        spawn: () => {
          const child = children.shift();
          if (child === undefined) {
            throw new Error('runtime lifecycle child fixture exhausted');
          }
          queueMicrotask(() => child.emit('spawn'));
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
      ...(removeFile === undefined ? {} : { removeFile }),
      ...(beforeStart === undefined ? {} : { beforeStart }),
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    }),
    testLaunchFactory(),
    undefined,
    preflight,
    testMetricsPortAllocator(),
  );
}

function autoRecoverySupervisor(
  children: FakeChildProcess[],
  preflightState: 'PASSED' | 'FAILED' = 'PASSED',
): {
  readonly advanceRetry: () => Promise<void>;
  readonly preflight: ReturnType<typeof vi.fn>;
  readonly supervisor: RuntimeSupervisorService;
} {
  let retry: (() => void) | undefined;
  let now = Date.parse('2026-09-08T00:00:00.000Z');
  const scheduler: AutoRecoveryScheduler = {
    clear: vi.fn(),
    now: () => now,
    schedule: (callback, delay) => {
      if (delay === 30_000) retry = callback;
      return callback;
    },
  };
  const preflight = vi.fn(async () => ({
    failureCode: preflightState === 'PASSED' ? null : 'SYSTEM_PREFLIGHT_FAILED',
    state: preflightState,
  }));
  const database = platformDatabase as PlatformDatabaseService;
  const supervisor = new RuntimeSupervisorService(
    database,
    new AuditLedgerService(database),
    new CapacityReservationService(database),
    new RuntimeProcessSupervisor({
      factory: {
        spawn: () => {
          const child = children.shift();
          if (child === undefined) throw new Error('auto recovery child fixture exhausted');
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    }),
    testLaunchFactory(),
    undefined,
    testRuntimePreflight({ runForSystemRecovery: preflight }),
    testMetricsPortAllocator(),
  );
  const coordinator = new FixedAutoRecoveryCoordinator(supervisor, scheduler);
  (supervisor as unknown as { autoRecovery: FixedAutoRecoveryCoordinator }).autoRecovery =
    coordinator;
  return {
    advanceRetry: async () => {
      const deadline = Date.now() + 5_000;
      while (retry === undefined && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      if (retry === undefined) {
        throw new Error('auto recovery retry was not scheduled');
      }
      now += 30_000;
      retry();
      await new Promise((resolve) => setTimeout(resolve, 500));
    },
    preflight,
    supervisor,
  };
}

async function waitForTaskState(
  taskId: string,
  expected: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await requirePool().query<{ observed_state: string }>(
      `SELECT observed_state FROM ape_dts_platform.tasks WHERE id=$1::uuid`,
      [taskId],
    );
    if (result.rows[0]?.observed_state === expected) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  const diagnostic = await requirePool().query(
    `SELECT task.observed_state, task.desired_state, task.row_version,
            task.recovery_disposition,
            (SELECT jsonb_agg(jsonb_build_object('state',state,'failure',failure_code,'reason',reason)
                    ORDER BY created_at) FROM ape_dts_platform.task_operations WHERE task_id=task.id) AS operations
       FROM ape_dts_platform.tasks AS task WHERE task.id=$1::uuid`,
    [taskId],
  );
  throw new Error(
    `task ${taskId} did not reach ${expected}: ${JSON.stringify(diagnostic.rows[0])}`,
  );
}

async function recoveryState(
  taskId: string,
  runId: string,
): Promise<{
  disposition: string;
  reservations: string;
  state: string;
}> {
  const result = await requirePool().query<{
    disposition: string;
    reservations: string;
    state: string;
  }>(
    `SELECT recovery_disposition AS disposition, observed_state AS state,
            (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations
              WHERE run_id=$2::uuid AND state IN ('ACTIVE','RESERVED')) AS reservations
       FROM ape_dts_platform.tasks WHERE id=$1::uuid`,
    [taskId, runId],
  );
  return result.rows[0]!;
}

async function allowR32TestCapacity(): Promise<void> {
  await requirePool().query(
    `UPDATE ape_dts_platform.platform_settings SET max_runtime_units=100 WHERE singleton_id=1`,
  );
}

async function createTask(
  legacyRiskConfirmation = true,
  legacyTopics: readonly ('SOURCE' | 'TARGET')[] = [],
  mappings?: readonly KafkaTaskMapping[],
  mysqlKey?: UnlockedKeyService,
  includeSnapshot = true,
): Promise<{
  id: string;
  revisionId: string;
  rowVersion: string;
  sourceId: string;
  targetId: string;
}> {
  const database = platformDatabase;
  if (database === undefined) {
    throw new Error('Unit 8 platform database is not initialized');
  }
  const tasks = new TasksService(database, new AuditLedgerService(database));
  const mysqlSnapshot = mysqlKey !== undefined;
  let sourceId = randomUUID();
  const targetId = randomUUID();
  if (mysqlKey !== undefined) {
    const source = await new DataSourcesService(
      database,
      new AuditLedgerService(database),
      mysqlKey,
      new DataSourceAdapterRegistry([]),
    ).create(administrator.id, {
      name: `MySQL snapshot fixture ${sourceId}`,
      configuration: {
        type: 'MYSQL',
        host: '127.0.0.1',
        port: 3306,
        database: 'demo',
        username: 'fixture',
        tls: { mode: 'DISABLE', riskAcknowledged: true },
      },
      credentials: { mode: 'SET', password: 'isolated-fixture-password' },
    });
    sourceId = source.id;
  }
  await requirePool().query(
    `INSERT INTO ape_dts_platform.data_sources (
       id, normalized_name, source_type, configuration, connection_secret_id,
       state, created_by_user_id, updated_by_user_id
     ) VALUES
       ($1::uuid, $2, $8, $3::jsonb,
        (SELECT connection_secret_id FROM ape_dts_platform.data_sources WHERE id=$1::uuid), 'ACTIVE', $6::uuid, $6::uuid),
       ($4::uuid, $5, 'KAFKA', $7::jsonb, NULL, 'ACTIVE', $6::uuid, $6::uuid)
       ON CONFLICT (id) DO NOTHING`,
    [
      sourceId,
      `unit8-source-${sourceId}`,
      JSON.stringify(
        mysqlSnapshot
          ? {
              type: 'MYSQL',
              host: '127.0.0.1',
              port: 3306,
              database: 'demo',
              username: 'fixture',
              tls: { mode: 'DISABLE', riskAcknowledged: true },
            }
          : {
              bootstrapServers: ['127.0.0.1:9092'],
              plaintextRiskAcknowledged: true,
              type: 'KAFKA',
            },
      ),
      targetId,
      `unit8-target-${targetId}`,
      administrator.id,
      JSON.stringify({
        bootstrapServers: ['127.0.0.1:9093'],
        plaintextRiskAcknowledged: true,
        type: 'KAFKA',
      }),
      mysqlSnapshot ? 'MYSQL' : 'KAFKA',
    ],
  );
  const created = await tasks.create(administrator, {
    configuration: mysqlSnapshot
      ? {
          kind: 'MYSQL_TO_KAFKA',
          schemaVersion: 2,
          source: { dataSourceId: sourceId, includeSnapshot },
          target: { dataSourceId: targetId },
          mappings: ['orders', 'items'].map((name) => ({
            source: { schema: 'demo', name },
            targets: [{ topic: `unit8.${name}` }],
          })),
          tuning: {
            batchSize: 100,
            bufferSize: 16000,
            checkpointIntervalSeconds: 1,
            requiredAcks: 'all',
          },
        }
      : {
          kind: 'KAFKA_RAW_FORWARD',
          schemaVersion: mappings ? 2 : 1,
          source: {
            dataSourceId: sourceId,
            groupId: 'placeholder',
            ...(mappings ? {} : { topic: 'unit8.source' }),
          },
          target: { dataSourceId: targetId, ...(mappings ? {} : { topic: 'unit8.target' }) },
          ...(mappings ? { mappings } : {}),
          tuning: {
            ackIntervalSeconds: 5,
            batchSize: 100,
            bufferSize: 16_000,
            checkpointIntervalSeconds: 15,
            requiredAcks: 'all',
          },
        },
    name: `Unit 8 runtime ${randomUUID()}`,
    sourceDataSourceId: sourceId,
    targetDataSourceId: targetId,
    taskType: mysqlSnapshot ? 'MYSQL_KAFKA' : 'KAFKA_KAFKA',
  });
  const revisionId = created.draft?.id;
  if (revisionId === undefined) {
    throw new Error('Unit 8 task did not return a draft revision');
  }
  const artifactId = randomUUID();
  const storageProfileId = randomUUID();
  const storedObjectId = randomUUID();
  const probeRunId = randomUUID();
  const now = new Date();
  const artifactSha256 = Buffer.alloc(32, 0xbb);
  await requirePool().query(
    `INSERT INTO ape_dts_platform.storage_profiles (
       id, profile_key, display_name, version, driver, config, state,
       created_by_user_id
     ) VALUES ($1::uuid, $2, 'Unit 8 runtime profile', 1, 'LOCAL',
               '{"relative_prefix":""}'::jsonb, 'DRAFT', $3::uuid)`,
    [storageProfileId, `unit8-runtime-${artifactId}`, administrator.id],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.stored_objects (
       id, storage_profile_id, kind, object_key, size_bytes, sha256,
       content_type, state, created_by_actor_type, created_by_user_id, available_at
     ) VALUES ($1::uuid, $2::uuid, 'ARTIFACT', $3, 1, $4,
               'application/octet-stream', 'AVAILABLE', 'USER', $5::uuid, $6::timestamptz)`,
    [
      storedObjectId,
      storageProfileId,
      `artifacts/${artifactId}`,
      artifactSha256,
      administrator.id,
      now,
    ],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifacts (
       id, version_identifier, declared_source, stored_object_id,
       declared_sha256, actual_sha256, size_bytes, state, activated_at,
       created_by_user_id
     ) VALUES ($1::uuid, $2, 'unit-test', $3::uuid, $4, $4, 1,
               'ACTIVE', $5::timestamptz, $6::uuid)`,
    [
      artifactId,
      `unit8-artifact-${artifactId}`,
      storedObjectId,
      artifactSha256,
      now,
      administrator.id,
    ],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifact_probe_runs (
       id, artifact_id, probe_kind, probe_version, status,
       started_by_user_id, ended_at, exit_code
     ) VALUES ($1::uuid, $2::uuid, 'BEHAVIOR', 'unit8', 'PASSED',
               $3::uuid, $4::timestamptz, 0)`,
    [probeRunId, artifactId, administrator.id, now],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.artifact_certifications (
       id, artifact_id, platform_version, task_scenario,
       adapter_id, adapter_version, state, evidence_probe_run_id
     ) VALUES ($1::uuid, $2::uuid, '0.0.0', 'KAFKA_RAW_FORWARD',
               'unit8-adapter', '1', 'PASSED', $3::uuid)`,
    [randomUUID(), artifactId, probeRunId],
  );
  await requirePool().query(
    `UPDATE ape_dts_platform.task_revisions
        SET artifact_id = $2::uuid
      WHERE id = $1::uuid`,
    [revisionId, artifactId],
  );
  // Multi-source cases use the actual preflight and continuity services below.
  if (mappings || mysqlSnapshot)
    return { id: created.id, revisionId, rowVersion: created.rowVersion, sourceId, targetId };
  await requirePool().query(
    `UPDATE ape_dts_platform.task_revisions
        SET state = $2
      WHERE id = $1::uuid`,
    [revisionId, legacyRiskConfirmation ? 'RISK_CONFIRMED' : 'PREFLIGHT_PASSED'],
  );
  const sourceCluster = `unit8-source-cluster-${sourceId}`;
  const targetCluster = `unit8-target-cluster-${targetId}`;
  const sourceTopicId = legacyTopics.includes('SOURCE') ? null : `unit8-source-topic-${sourceId}`;
  const targetTopicId = legacyTopics.includes('TARGET') ? null : `unit8-target-topic-${targetId}`;
  for (const [dataSourceId, identityValue] of [
    [sourceId, sourceCluster],
    [targetId, targetCluster],
  ] as const) {
    const testId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.connection_test_runs (
         id, data_source_id, initiated_by_user_id, purpose, state,
         started_at, completed_at, duration_ms
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'PREFLIGHT', 'SUCCEEDED',
                 $4::timestamptz, $4::timestamptz, 1)`,
      [testId, dataSourceId, administrator.id, now],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.data_source_identity_observations (
         id, data_source_id, connection_test_run_id, identity_kind,
         identity_value, server_version, observed_at
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA_CLUSTER',
                 $4, NULL, $5::timestamptz)`,
      [randomUUID(), dataSourceId, testId, identityValue, now],
    );
  }
  for (const [dataSourceId, resourceType, name, clusterId, topicId] of [
    [sourceId, 'KAFKA_SOURCE_TOPIC', 'unit8.source', sourceCluster, sourceTopicId],
    [targetId, 'KAFKA_TARGET_TOPIC', 'unit8.target', targetCluster, targetTopicId],
  ] as const) {
    if (topicId === null) continue;
    await requirePool().query(
      `INSERT INTO ape_dts_platform.external_resources (
       id, task_id, data_source_id, resource_type, normalized_name,
       ownership_mode, stable_identity, lifecycle_state, platform_created,
       created_by_user_id
     ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5,
        'SELF_MANAGED', $6, 'BOUND', false, $7::uuid)`,
      [
        randomUUID(),
        created.id,
        dataSourceId,
        resourceType,
        name,
        `${clusterId}:${topicId}`,
        administrator.id,
      ],
    );
  }
  const preflightResult = {
    capacity: { available: 4, limit: 4, requested: 2, used: 0 },
    checks: [
      ['CONFIGURATION', 'CONFIGURATION_VALID'],
      ['DATA_SOURCE_ACCESS', 'DATA_SOURCE_ACCESS_OK'],
      ['SOURCE_CONNECTION', 'DATA_SOURCE_CONNECTION_OK'],
      ['TARGET_CONNECTION', 'DATA_SOURCE_CONNECTION_OK'],
      ['SOURCE_METADATA', 'DATA_SOURCE_METADATA_OK'],
      ['TARGET_METADATA', 'DATA_SOURCE_METADATA_OK'],
      [
        'TOPIC_IDENTITIES',
        'KAFKA_TOPIC_IDENTITIES_OK',
        {
          topics: [
            {
              clusterId: sourceCluster,
              partitionCount: 2,
              partitionIds: [0, 1],
              resourceType: 'KAFKA_SOURCE_TOPIC',
              topic: 'unit8.source',
              topicId: sourceTopicId,
            },
            {
              clusterId: targetCluster,
              partitionCount: 1,
              partitionIds: [],
              resourceType: 'KAFKA_TARGET_TOPIC',
              topic: 'unit8.target',
              topicId: targetTopicId,
            },
          ],
        },
      ],
      [
        'ARTIFACT',
        'ARTIFACT_AVAILABLE',
        {
          adapterId: 'ape-dts-ini',
          adapterVersion: '1',
          platformVersion: '0.0.0',
          scenario: 'KAFKA_RAW_FORWARD',
        },
      ],
      ['CAPACITY', 'CAPACITY_AVAILABLE'],
    ].map(([key, code, details = {}]) => ({
      code,
      details,
      key,
      state: 'PASSED',
      summary: 'Unit 8 preflight check passed.',
    })),
  };
  preflightResult.checks.push(
    {
      code: 'NOT_APPLICABLE',
      details: {},
      key: 'PRIMARY_KEY',
      state: 'SKIPPED',
      summary: 'Not applicable.',
    },
    {
      code: 'NOT_APPLICABLE',
      details: {},
      key: 'PG_RESOURCES',
      state: 'SKIPPED',
      summary: 'Not applicable.',
    },
  );
  Object.assign(preflightResult, {
    dataSourceVersions: { source: '1', target: '1' },
    identities: {
      source: sourceCluster,
      sourceTopic: sourceTopicId,
      target: targetCluster,
      targetTopic: targetTopicId,
    },
    taskRevision: { draftVersion: '1', taskRowVersion: '1' },
  });
  await requirePool().query(
    `INSERT INTO ape_dts_platform.preflight_runs (
       id, task_revision_id, state, started_at, completed_at, result
     ) VALUES ($1::uuid, $2::uuid, 'PASSED', $3::timestamptz, $3::timestamptz, $4::jsonb)`,
    [randomUUID(), revisionId, now, JSON.stringify(preflightResult)],
  );
  if (legacyRiskConfirmation)
    await requirePool().query(
      `INSERT INTO ape_dts_platform.risk_acceptances (
       id, task_revision_id, accepted_by_user_id, note
     ) VALUES ($1::uuid, $2::uuid, $3::uuid, NULL)`,
      [randomUUID(), revisionId, administrator.id],
    );
  return {
    id: created.id,
    revisionId,
    rowVersion: created.rowVersion,
    sourceId,
    targetId,
  };
}

const runtimeMappings: readonly KafkaTaskMapping[] = [
  { source: 'unit8.source_a', targets: [{ topic: 'unit8.target_a' }, { topic: 'unit8.shared' }] },
  { source: 'unit8.source_b', targets: [{ topic: 'unit8.target_b' }, { topic: 'unit8.shared' }] },
];
const mappedUnitKeys = runtimeMappings.flatMap(({ source }) =>
  [0, 1].map((partition) => `topic:${source}:partition:${partition}`),
);

async function mappedRuntimeFixture(legacy: boolean, missingSecondPartitions = false) {
  const task = await createTask(false, [], runtimeMappings);
  const database = platformDatabase!;
  const audit = new AuditLedgerService(database);
  const tasks = new TasksService(database, audit);
  const root = await mkdtemp(path.join(tmpdir(), 'scs-mapped-runtime-'));
  temporaryDirectories.push(root);
  const runtimeRoot = `/run/ape-dts/${randomUUID()}`;
  temporaryDirectories.push(path.resolve(runtimeRoot));
  const artifactId = (
    await requirePool().query<{ artifact_id: string }>(
      'SELECT artifact_id FROM ape_dts_platform.task_revisions WHERE id=$1::uuid',
      [task.revisionId],
    )
  ).rows[0]!.artifact_id;
  const bytes = Buffer.from('mapped task executable fixture, never executed');
  const hash = createHash('sha256').update(bytes).digest();
  await mkdir(path.join(root, 'artifacts'));
  await writeFile(path.join(root, 'artifacts', artifactId), bytes);
  await requirePool().query(
    'UPDATE ape_dts_platform.stored_objects SET sha256=$2,size_bytes=$3 WHERE id=(SELECT stored_object_id FROM ape_dts_platform.artifacts WHERE id=$1::uuid)',
    [artifactId, hash, bytes.length],
  );
  await requirePool().query(
    'UPDATE ape_dts_platform.artifacts SET declared_sha256=$2,actual_sha256=$2,size_bytes=$3 WHERE id=$1::uuid',
    [artifactId, hash, bytes.length],
  );
  await requirePool().query(
    "INSERT INTO ape_dts_platform.artifact_signature_checks(id,artifact_id,policy_version,status,checked_by_user_id) VALUES($1::uuid,$2::uuid,'DISABLED','NOT_REQUIRED',$3::uuid)",
    [randomUUID(), artifactId, administrator.id],
  );
  const targetTopics = [
    ...new Set(runtimeMappings.flatMap(({ targets }) => targets.map(({ topic }) => topic))),
  ];
  const preflight = new PreflightService(
    database,
    audit,
    new DataSourcesService(
      database,
      audit,
      new UnlockedKeyService(),
      new DataSourceAdapterRegistry([
        {
          type: 'KAFKA',
          test: async ({ configuration }) => {
            if (configuration.type !== 'KAFKA') throw new Error('Expected Kafka configuration');
            return {
              kind: 'KAFKA_CLUSTER',
              serverVersion: null,
              value: configuration.bootstrapServers[0]?.endsWith(':9092')
                ? `mapped-source-${task.sourceId}`
                : `mapped-target-${task.targetId}`,
            };
          },
          metadata: async ({ configuration }, query) => {
            if (configuration.type !== 'KAFKA') throw new Error('Expected Kafka configuration');
            if (query.resource === 'PARTITIONS') {
              const partitions =
                missingSecondPartitions && query.topic === runtimeMappings[1]!.source
                  ? [0]
                  : [0, 1];
              return partitions.map((id) => ({ name: String(id), identity: null, attributes: {} }));
            }
            const source = configuration.bootstrapServers[0]?.endsWith(':9092');
            return (source ? runtimeMappings.map((mapping) => mapping.source) : targetTopics).map(
              (name) => ({
                name,
                identity: legacy ? null : `mapped-id-${name}`,
                attributes: { partitionCount: source ? '2' : '1' },
              }),
            );
          },
        },
      ]),
    ),
    new CapacityReservationService(database),
    new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
    new ContinuityService(database, audit),
    '0.0.0',
    new RuntimeEnvironmentInspector({
      SCS_RUNTIME_METRICS_PORT_MIN: '45000',
      SCS_RUNTIME_METRICS_PORT_MAX: '55000',
    }),
  );
  const spawns: RuntimeSpawnSpec[] = [];
  const supervisor = async () => {
    const instanceId = randomUUID();
    await requirePool().query(
      "INSERT INTO ape_dts_platform.platform_instances(id,platform_version,started_at,last_heartbeat_at) VALUES($1,'mapped-test',now(),now())",
      [instanceId],
    );
    const service = new RuntimeSupervisorService(
      database,
      audit,
      new CapacityReservationService(database),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: (spec) => {
            spawns.push(spec);
            const child = new FakeChildProcess(51_000 + spawns.length, 'SIGINT');
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      new AuthenticatedRuntimeLaunchSpecFactory({
        adapters: new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
        artifactCache: new ArtifactCache(path.join(root, 'cache')),
        database,
        localObjectStorage: new LocalObjectStorage(root),
        platformVersion: '0.0.0',
        runtimeRoot,
        unlockedKey: new UnlockedKeyService(),
      }),
      { getStatus: () => ({ phase: 'READY', platformInstanceId: instanceId }) } as never,
      preflight,
      testMetricsPortAllocator(),
    );
    return { service, instanceId };
  };
  return { task, tasks, preflight, spawns, supervisor };
}

let nextPersistedMetricsPort = 36_001;

async function pgExpansionRuntimeFixture(failExpansionSpawn: boolean) {
  const seed = await createTask(false, [], runtimeMappings);
  const database = platformDatabase!;
  const audit = new AuditLedgerService(database);
  const tasks = new TasksService(database, audit);
  const key = new UnlockedKeyService();
  key.set(randomBytes(32));
  const resourceSuffix = randomUUID().replaceAll('-', '');
  const slotName = `slot_${resourceSuffix}`;
  const publicationName = `pub_${resourceSuffix}`;
  let confirmedFlushLsn = '0/100';
  let slotStatus: 'ACTIVE' | 'INACTIVE' = 'INACTIVE';
  const targetTopics = ['unit8.pg_orders', 'unit8.pg_added'];
  const readyTopics = new Set(targetTopics);
  const sources = new DataSourcesService(
    database,
    audit,
    key,
    new DataSourceAdapterRegistry([
      {
        type: 'POSTGRES',
        readProgress: async () => ({
          slotStatus,
          retainedWalBytes: '100',
          unconfirmedWalBytes: '0',
          offsetLag: null,
          replicationLagMilliseconds: null,
        }),
        test: async () => ({ kind: 'POSTGRES_DATABASE', value: '456:123', serverVersion: '12' }),
        metadata: async () => [
          { name: 'id', identity: null, attributes: { isPrimaryKey: 'true', isNullable: 'false' } },
        ],
        readPostgresResources: async () => ({
          databaseName: 'data_demo',
          slot: { name: slotName, created: false, stableIdentity: `456:123:${slotName}` },
          publication: {
            name: publicationName,
            created: false,
            stableIdentity: `456:123:${publicationName}`,
          },
        }),
        readPostgresContinuity: async () => ({
          confirmedFlushLsn,
          databaseName: 'data_demo',
          databaseOid: '123',
          systemIdentifier: '456',
          slotName,
          slotPlugin: 'pgoutput',
          slotActive: false,
        }),
      },
      {
        type: 'KAFKA',
        test: async () => ({
          kind: 'KAFKA_CLUSTER',
          value: `pg-target-${seed.targetId}`,
          serverVersion: null,
        }),
        metadata: async () =>
          [...readyTopics].map((name) => ({
            name,
            identity: `id-${name}`,
            attributes: { partitionCount: '1' },
          })),
        createKafkaTopic: async (_input, request) => {
          const created = !readyTopics.has(request.topic);
          readyTopics.add(request.topic);
          return { topic: request.topic, created };
        },
      },
    ]),
  );
  const source = await sources.create(administrator.id, {
    name: `PG expansion ${resourceSuffix}`,
    configuration: {
      type: 'POSTGRES',
      host: '127.0.0.1',
      port: 5432,
      database: 'data_demo',
      username: 'fixture',
      tls: { mode: 'DISABLE', riskAcknowledged: true },
    },
    credentials: { mode: 'SET', password: 'isolated-fixture-password' },
  });
  const artifactId = (await tasks.get(administrator, seed.id)).draft!.artifactId;
  const created = await tasks.create(administrator, {
    name: `PG runtime expansion ${resourceSuffix}`,
    taskType: 'PG_KAFKA',
    sourceDataSourceId: source.id,
    targetDataSourceId: seed.targetId,
    artifactId,
    configuration: {
      kind: 'PG_TO_KAFKA',
      schemaVersion: 2,
      source: {
        dataSourceId: source.id,
        includeSnapshot: false,
        slotName,
        publicationName,
        resourceMode: 'PLATFORM_MANAGED',
      },
      target: { dataSourceId: seed.targetId },
      mappings: [
        { source: { schema: 'public', name: 'orders' }, targets: [{ topic: targetTopics[0]! }] },
      ],
      tuning: {
        batchSize: 100,
        bufferSize: 16000,
        checkpointIntervalSeconds: 15,
        requiredAcks: 'all',
      },
    },
  });
  const resources = new PgResourceManager(database, audit);
  for (const [resourceType, name] of [
    ['PG_SLOT', slotName],
    ['PG_PUBLICATION', publicationName],
  ] as const) {
    await resources.bind(administrator, {
      dataSourceId: source.id,
      databaseName: 'data_demo',
      name,
      ownershipMode: 'PLATFORM_MANAGED',
      resourceType,
      stableIdentity: `456:123:${name}`,
      taskId: created.id,
    });
  }
  const preflight = new PreflightService(
    database,
    audit,
    sources,
    new CapacityReservationService(database),
    new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
    new ContinuityService(database, audit),
    '0.0.0',
    new RuntimeEnvironmentInspector({
      SCS_RUNTIME_METRICS_PORT_MIN: '45000',
      SCS_RUNTIME_METRICS_PORT_MAX: '55000',
    }),
  );
  const launchedConfigurations: unknown[] = [];
  const launch = new ControlledRuntimeLaunchSpecFactory({
    executablePath: process.execPath,
    runtimeRoot: launchFactoryRoot!,
    render: ({ config }) => {
      launchedConfigurations.push(config);
      return { iniContent: '[runtime]\nfixture=pg-expansion\n', log4rsContent: 'appenders: []\n' };
    },
  });
  let spawnCount = 0;
  const notify = vi.fn<AlertsService['observe']>(async () => undefined);
  const service = new RuntimeSupervisorService(
    database,
    audit,
    new CapacityReservationService(database),
    new RuntimeProcessSupervisor({
      factory: {
        spawn: () => {
          spawnCount += 1;
          if (failExpansionSpawn && spawnCount === 2)
            throw new Error('fixture PG expansion spawn rejected');
          const child = new FakeChildProcess(62000 + spawnCount, 'SIGINT');
          queueMicrotask(() => child.emit('spawn'));
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    }),
    launch,
    undefined,
    preflight,
    testMetricsPortAllocator(),
    undefined,
    undefined,
    sources,
    { observe: notify } as unknown as AlertsService,
  );
  return {
    created,
    tasks,
    preflight,
    service,
    launchedConfigurations,
    advanceSlot: () => {
      confirmedFlushLsn = '0/200';
    },
    hideAddedTopic: () => readyTopics.delete('unit8.pg_added'),
    notify,
    setSlotStatus: (value: 'ACTIVE' | 'INACTIVE') => {
      slotStatus = value;
    },
  };
}

async function mysqlSnapshotRuntimeFixture(failCdcSpawn = false, includeSnapshot = true) {
  const key = new UnlockedKeyService();
  key.set(randomBytes(32));
  const task = await createTask(false, [], undefined, key, includeSnapshot);
  const database = platformDatabase!;
  const audit = new AuditLedgerService(database);
  const tasks = new TasksService(database, audit);
  let currentPosition = 157;
  const sources = new DataSourcesService(
    database,
    audit,
    key,
    new DataSourceAdapterRegistry([
      {
        type: 'MYSQL',
        test: async () => ({
          kind: 'MYSQL_DATABASE',
          value: `mysql-${task.sourceId}:demo`,
          serverVersion: '8.0.36',
        }),
        metadata: async (_input, query) =>
          query.resource === 'COLUMNS'
            ? [
                {
                  name: 'id',
                  identity: null,
                  attributes: { isPrimaryKey: 'true', dataType: 'bigint' },
                },
              ]
            : ['orders', 'items'].map((name) => ({
                name,
                identity: null,
                attributes: { schema: 'demo' },
              })),
        inspectMysqlCdc: async () => ({
          identity: {
            kind: 'MYSQL_DATABASE',
            value: `mysql-${task.sourceId}:demo`,
            serverVersion: '8.0.36',
          },
          binlog: { enabled: true, format: 'ROW', rowImage: 'FULL', serverId: 17, gtidMode: 'OFF' },
          position: { filename: 'mysql-bin.000001', position: currentPosition },
          checks: [{ key: 'BINLOG_POSITION', state: 'PASSED', summary: 'Fixture source ready' }],
        }),
        readMysqlBinlogs: async () => [{ filename: 'mysql-bin.000001', sizeBytes: '10000' }],
      },
      {
        type: 'KAFKA',
        test: async () => ({
          kind: 'KAFKA_CLUSTER',
          value: `mysql-target-${task.targetId}`,
          serverVersion: null,
        }),
        metadata: async () =>
          ['orders', 'items'].map((name) => ({
            name: `unit8.${name}`,
            identity: `topic-${name}`,
            attributes: { partitionCount: '1' },
          })),
      },
    ]),
  );
  const preflight = new PreflightService(
    database,
    audit,
    sources,
    new CapacityReservationService(database),
    new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
    new ContinuityService(database, audit),
    '0.0.0',
    new RuntimeEnvironmentInspector({
      SCS_RUNTIME_METRICS_PORT_MIN: '45000',
      SCS_RUNTIME_METRICS_PORT_MAX: '55000',
    }),
  );
  const state = new MysqlRuntimeStateStore(database, sources, audit);
  const children: FakeChildProcess[] = [];
  const spawns: RuntimeSpawnSpec[] = [];
  const launches: { phase: string; binlogPosition: number }[] = [];
  const launch = new ControlledRuntimeLaunchSpecFactory({
    executablePath: process.execPath,
    runtimeRoot: launchFactoryRoot!,
    render: (input) => {
      if (input.mysql === undefined) throw new Error('MySQL runtime binding missing');
      launches.push({ phase: input.mysql.phase, binlogPosition: input.mysql.binlogPosition });
      return {
        iniContent: `[mysql]\nposition=${input.mysql.binlogPosition}\n`,
        log4rsContent: 'appenders: []\n',
      };
    },
  });
  const processes = new RuntimeProcessSupervisor({
    factory: {
      spawn: (spec) => {
        spawns.push(spec);
        if (failCdcSpawn && launches.at(-1)?.phase === 'CDC')
          throw new Error('fixture CDC spawn rejected');
        const child = new FakeChildProcess(61000 + children.length, 'SIGINT');
        children.push(child);
        queueMicrotask(() => child.emit('spawn'));
        return child as unknown as RuntimeChildProcess;
      },
    },
    logSink: new RecordingSink(),
    redact: (value) => value,
    stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
  });
  const supervisor = new RuntimeSupervisorService(
    database,
    audit,
    new CapacityReservationService(database),
    processes,
    launch,
    undefined,
    preflight,
    testMetricsPortAllocator(),
    state,
  );
  const start = async (includeSnapshotOverride?: boolean) => {
    let current = await tasks.get(administrator, task.id);
    if (current.draft === null || includeSnapshotOverride !== undefined) {
      const applied = current.draft ?? current.applied!;
      const config = parsePlatformTaskConfig(applied.configuration);
      if (config.kind !== 'MYSQL_TO_KAFKA') throw new Error('Expected MySQL fixture configuration');
      current = await tasks.saveDraft(administrator, task.id, {
        taskType: 'MYSQL_KAFKA',
        revisionId: applied.id,
        expectedDraftVersion: applied.draftVersion,
        expectedTaskRowVersion: current.rowVersion,
        artifactId: applied.artifactId,
        configuration:
          includeSnapshotOverride === undefined
            ? config
            : { ...config, source: { ...config.source, includeSnapshot: includeSnapshotOverride } },
      });
    }
    const checked = await preflight.run(administrator, task.id, {
      expectedTaskRowVersion: current.rowVersion,
      revisionId: current.draft!.id,
    });
    expect(checked.state, JSON.stringify(checked.result.checks)).toBe('PASSED');
    current = await tasks.get(administrator, task.id);
    const operation = await supervisor.start(administrator, {
      taskId: task.id,
      revisionId: current.draft!.id,
      expectedTaskRowVersion: current.rowVersion,
      idempotencyKey: randomUUID(),
    });
    expect(operation.state).toBe('SUCCEEDED');
    return operation.runId!;
  };
  const checkpoint = async () =>
    (
      await requirePool().query(
        'SELECT phase,binlog_filename,binlog_position::text,active_run_id FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1',
        [task.id],
      )
    ).rows[0];
  const finish = async (tables: readonly string[]) => {
    await writeFile(
      path.join(spawns.at(-1)!.runtimeDirectory, 'mysql-finished.log'),
      tables
        .map((tb) =>
          JSON.stringify({ type: 'RdbSnapshotFinished', db_type: 'mysql', schema: 'demo', tb }),
        )
        .join('\n') + '\ntask finished\n',
    );
    children.at(-1)!.close(0, null);
  };
  return {
    task,
    audit,
    processes,
    children,
    tasks,
    supervisor,
    state,
    start,
    checkpoint,
    finish,
    spawns,
    launches,
    advanceSource: () => {
      currentPosition = 999;
    },
    cleanup: async () => {
      const current = await tasks.get(administrator, task.id);
      if (current.observedState === 'RUNNING') {
        const stopped = await supervisor.stop(administrator, {
          taskId: current.id,
          expectedTaskRowVersion: current.rowVersion,
          idempotencyKey: randomUUID(),
        });
        expect(stopped.state).toBe('SUCCEEDED');
      }
      await supervisor.beforeApplicationShutdown('SIGTERM');
      state.onModuleDestroy();
      key.clear();
      // The shared database's later recovery tests must not select this completed failure fixture.
      await requirePool().query(
        "UPDATE ape_dts_platform.tasks SET desired_state='STOPPED', recovery_disposition='NOT_APPLICABLE' WHERE id=$1::uuid",
        [task.id],
      );
    },
  };
}

async function createPersistedActiveRun(task: {
  readonly id: string;
  readonly revisionId: string;
}): Promise<{ readonly operationId: string; readonly runId: string }> {
  const operationId = randomUUID();
  const runId = randomUUID();
  const metricsPortBase = nextPersistedMetricsPort;
  nextPersistedMetricsPort += 2;
  const artifact = await requirePool().query<{ artifact_id: string }>(
    `SELECT artifact_id
       FROM ape_dts_platform.task_revisions
      WHERE id = $1::uuid
        AND task_id = $2::uuid`,
    [task.revisionId, task.id],
  );
  const artifactId = artifact.rows[0]?.artifact_id;
  if (artifactId === undefined) {
    throw new Error('Unit 8 persisted run artifact was not found');
  }
  await requirePool().query(
    `INSERT INTO ape_dts_platform.task_operations (
       id, task_id, action, idempotency_key, target_revision_id,
       state, initiated_by_user_id, reason, execution_stage, started_at
     ) VALUES ($1::uuid, $2::uuid, 'START', $3, $4::uuid,
               'EXECUTING', $5::uuid, 'persisted runtime fixture',
               'RUNNING', statement_timestamp())`,
    [operationId, task.id, `persisted-${runId}`, task.revisionId, administrator.id],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.task_runs (
       id, task_id, task_revision_id, operation_id, artifact_id,
       status, current_stage, started_at
     ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
               'RUNNING', 'RUNNING', statement_timestamp())`,
    [runId, task.id, task.revisionId, operationId, artifactId],
  );
  await requirePool().query(
    `UPDATE ape_dts_platform.task_operations
        SET run_id = $2::uuid
      WHERE id = $1::uuid
        AND task_id = $3::uuid`,
    [operationId, runId, task.id],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.task_run_stages (
       id, run_id, stage_order, stage_type, status, started_at, heartbeat_at
     ) VALUES ($1::uuid, $2::uuid, 1, 'RUNNING', 'RUNNING',
               statement_timestamp(), statement_timestamp())`,
    [randomUUID(), runId],
  );
  for (const [index, unitKey] of ['partition:0', 'partition:1'].entries()) {
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_run_units (
         id, task_id, run_id, unit_key, status, runtime_directory,
         ini_path, log4rs_path, metrics_port, process_token
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, 'RUNNING',
                 '/persisted-runtime', '/persisted-runtime/runtime.ini',
                 '/persisted-runtime/log4rs.yaml', $5, $6)`,
      [randomUUID(), task.id, runId, unitKey, metricsPortBase + index, randomUUID()],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.capacity_reservations (
         id, task_id, task_revision_id, run_id, unit_key, state
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, 'ACTIVE')`,
      [randomUUID(), task.id, task.revisionId, runId, unitKey],
    );
  }
  await requirePool().query(
    `UPDATE ape_dts_platform.tasks
        SET desired_state = 'RUNNING', observed_state = 'RUNNING',
            row_version = row_version + 1, updated_at = statement_timestamp()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
        AND observed_state = 'STOPPED'`,
    [task.id],
  );
  return { operationId, runId };
}

beforeAll(async () => {
  const running = docker(['inspect', '--format', '{{.State.Running}}', postgresContainer]);
  if (running !== 'true') {
    throw new Error(`PostgreSQL container ${postgresContainer} is not running`);
  }
  launchFactoryRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-launch-factory-'));
  const portOutput = docker(['port', postgresContainer, '5432/tcp']);
  const port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  postgresPort = port;
  psqlAsContainerAdministrator(
    'postgres',
    `CREATE ROLE "${databaseRole}" LOGIN PASSWORD '${databasePassword}';
     CREATE DATABASE "${metadataDatabase}" OWNER "${databaseRole}";`,
  );
  pool = new Pool({
    database: metadataDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port,
    user: databaseRole,
  });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  platformDatabase = new PlatformDatabaseService();
  await platformDatabase.inspectAndUse(
    {
      database: metadataDatabase,
      host: '127.0.0.1',
      password: databasePassword,
      port,
      tls: { mode: 'DISABLE' },
      username: databaseRole,
    },
    bundle.manifest,
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.platform_instances
       (id, platform_version, started_at, last_heartbeat_at, process_id)
     VALUES ($1::uuid, 'integration-test', statement_timestamp(), statement_timestamp(), NULL)`,
    [runtimePlatformInstanceId],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.users (
       id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
     ) VALUES ($1::uuid, $2, $3, 'ADMIN', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6)`,
    [
      administrator.id,
      administrator.username,
      administrator.displayName,
      JSON.stringify({ memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 }),
      Buffer.alloc(16, 7),
      Buffer.alloc(32, 8),
    ],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.platform_settings (singleton_id, max_runtime_units)
     VALUES (1, 4)`,
  );
  const sink = new RecordingSink();
  const factory: RuntimeProcessFactory = {
    spawn: () => {
      const child = new FakeChildProcess(20_000 + spawnedChildren.length, 'SIGINT');
      spawnedChildren.push(child);
      queueMicrotask(() => child.emit('spawn'));
      return child as unknown as RuntimeChildProcess;
    },
  };
  const processSupervisor = new RuntimeProcessSupervisor({
    factory,
    logSink: sink,
    redact: (value) => value.replaceAll('secret', '[REDACTED]'),
    stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
  });

  runtime = new RuntimeSupervisorService(
    platformDatabase,
    new AuditLedgerService(platformDatabase),
    new CapacityReservationService(platformDatabase),
    processSupervisor,
    testLaunchFactory(),
    undefined,
    testRuntimePreflight(),
    testMetricsPortAllocator(),
  );
}, 120_000);

afterEach(async () => {
  // Each case shares only this temporary database; restart reservations must not leak into the next case.
  await requirePool().query(`UPDATE ape_dts_platform.capacity_reservations AS reservation
    SET state='RELEASED', released_at=statement_timestamp()
    FROM ape_dts_platform.task_runs AS run
    WHERE reservation.run_id=run.id AND run.failure_code='RUNTIME_PLATFORM_STOPPED_FOR_RESTART'
      AND reservation.state IN ('RESERVED','ACTIVE')`);
  await requirePool().query(`UPDATE ape_dts_platform.tasks AS task
    SET desired_state='STOPPED', recovery_disposition='NOT_APPLICABLE'
    WHERE EXISTS (SELECT 1 FROM ape_dts_platform.task_runs AS run
      WHERE run.task_id=task.id AND run.failure_code='RUNTIME_PLATFORM_STOPPED_FOR_RESTART')`);
});

afterAll(async () => {
  for (const directory of temporaryDirectories) {
    await rm(directory, { force: true, recursive: true });
  }
  if (launchFactoryRoot !== undefined) {
    await rm(launchFactoryRoot, { force: true, recursive: true });
  }
  await platformDatabase?.onApplicationShutdown();
  await pool?.end();
  try {
    psqlAsContainerAdministrator(
      'postgres',
      `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
        WHERE datname = '${metadataDatabase}'
          AND pid <> pg_backend_pid();
       DROP DATABASE IF EXISTS "${metadataDatabase}";
       DROP ROLE IF EXISTS "${databaseRole}";`,
    );
  } catch (error) {
    process.stderr.write(`failed to clean up Unit 8 PostgreSQL resources: ${String(error)}\n`);
  }
});

describe('runtime supervisor against PostgreSQL', () => {
  it('restarts an interrupted MySQL snapshot from its original Binlog baseline', async () => {
    const fixture = await mysqlSnapshotRuntimeFixture();
    try {
      const firstRun = await fixture.start();
      const first = await fixture.checkpoint();
      const current = await fixture.tasks.get(administrator, fixture.task.id);
      expect(
        (
          await fixture.supervisor.stop(administrator, {
            taskId: current.id,
            expectedTaskRowVersion: current.rowVersion,
            idempotencyKey: randomUUID(),
          })
        ).state,
      ).toBe('SUCCEEDED');
      fixture.advanceSource();
      await expect(fixture.start(false)).rejects.toMatchObject({
        code: 'MYSQL_SNAPSHOT_NOT_ENABLED',
      });
      expect(await fixture.checkpoint()).toMatchObject({
        phase: 'SNAPSHOT',
        binlog_position: '157',
      });
      const secondRun = await fixture.start(true);
      expect(secondRun).not.toBe(firstRun);
      expect(await fixture.checkpoint()).toMatchObject({
        phase: 'SNAPSHOT',
        binlog_filename: first.binlog_filename,
        binlog_position: first.binlog_position,
        active_run_id: secondRun,
      });
      expect(fixture.launches).toEqual([
        { phase: 'SNAPSHOT', binlogPosition: 157 },
        { phase: 'SNAPSHOT', binlogPosition: 157 },
      ]);
      const stages = await requirePool().query(
        "SELECT status FROM ape_dts_platform.task_run_stages WHERE run_id=$1 AND stage_type='SNAPSHOT'",
        [secondRun],
      );
      expect(stages.rows).toEqual([{ status: 'RUNNING' }]);
    } finally {
      await fixture.cleanup();
    }
  });

  it('rejects ordinary MySQL CDC resume when an existing task loses its checkpoint', async () => {
    const fixture = await mysqlSnapshotRuntimeFixture(false, false);
    try {
      const firstRun = await fixture.start();
      expect(await fixture.checkpoint()).toMatchObject({ phase: 'CDC', binlog_position: '157' });
      const current = await fixture.tasks.get(administrator, fixture.task.id);
      expect(
        (
          await fixture.supervisor.stop(administrator, {
            taskId: current.id,
            expectedTaskRowVersion: current.rowVersion,
            idempotencyKey: randomUUID(),
          })
        ).state,
      ).toBe('SUCCEEDED');
      await requirePool().query(
        'DELETE FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1::uuid',
        [fixture.task.id],
      );
      fixture.advanceSource();
      await expect(fixture.start()).rejects.toMatchObject({ code: 'MYSQL_CHECKPOINT_MISSING' });
      expect(fixture.spawns).toHaveLength(1);
      expect(await fixture.checkpoint()).toBeUndefined();
      expect(
        (
          await requirePool().query(
            'SELECT id,status FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid',
            [fixture.task.id],
          )
        ).rows,
      ).toEqual([{ id: firstRun, status: 'STOPPED' }]);
    } finally {
      await fixture.cleanup();
    }
  });

  it('recovers a disconnected PG process and performs concurrent continuity probes without pool starvation', async () => {
    const f = await pgExpansionRuntimeFixture(false);
    const policy = vi
      .spyOn(f.service, 'policy')
      .mockResolvedValue({ enabled: true, maxAttempts: 3, retryDelaySeconds: 5 });
    try {
      let task = await f.tasks.get(administrator, f.created.id);
      const checked = await f.preflight.run(administrator, task.id, {
        expectedTaskRowVersion: task.rowVersion,
        revisionId: task.draft!.id,
      });
      expect(checked.state, JSON.stringify(checked.result.checks)).toBe('PASSED');
      task = await f.tasks.get(administrator, task.id);
      const started = await f.service.start(administrator, {
        taskId: task.id,
        revisionId: task.draft!.id,
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: randomUUID(),
      });
      expect(started.state).toBe('SUCCEEDED');
      task = await f.tasks.get(administrator, task.id);
      const active = f.service.listActiveRuntimeTasks().find((r) => r.taskId === task.id)!;
      const baseline = async () =>
        (
          await requirePool().query(
            'SELECT pg_slot_name,pg_confirmed_flush_lsn::text FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1',
            [task.id],
          )
        ).rows[0];
      const before = await baseline();
      // The metadata pool has four clients; every external probe also reads/writes metadata.
      // Holding a transaction while probing would starve these six concurrent callers.
      await Promise.all(
        Array.from({ length: 6 }, () =>
          f.preflight.revalidateContinuityBeforeRuntime({
            id: task.id,
            revisionId: active.revisionId,
            rowVersion: task.rowVersion,
            desiredState: 'RUNNING',
          }),
        ),
      );
      const request = {
        taskId: task.id,
        runId: active.runId,
        revisionId: active.revisionId,
        collectedAt: new Date().toISOString(),
      };
      expect(
        await f.service.recoverDisconnectedPostgres({
          ...request,
          collectedAt: new Date(Date.now() - 120000).toISOString(),
        }),
      ).toBe(false);
      f.setSlotStatus('ACTIVE');
      expect(await f.service.recoverDisconnectedPostgres(request)).toBe(false);
      f.setSlotStatus('INACTIVE');
      policy.mockResolvedValueOnce({ enabled: false, maxAttempts: 3, retryDelaySeconds: 5 });
      expect(await f.service.recoverDisconnectedPostgres(request)).toBe(false);
      f.notify.mockRejectedValueOnce(new Error('CHANNEL_UNREACHABLE'));
      expect(await f.service.recoverDisconnectedPostgres(request)).toBe(true);
      expect(f.notify).toHaveBeenCalledWith(
        task.id,
        task.name,
        'UNEXPECTED_EXIT',
        true,
        true,
        '故障码：RUNTIME_PG_REPLICATION_DISCONNECTED',
      );
      const failed = (
        await requirePool().query(
          'SELECT failure_code FROM ape_dts_platform.task_runs WHERE id=$1',
          [active.runId],
        )
      ).rows[0];
      expect(failed.failure_code).toBe('RUNTIME_PG_REPLICATION_DISCONNECTED');
      expect((await f.tasks.get(administrator, task.id)).desiredState).toBe('RUNNING');
      expect(await baseline()).toEqual(before);
      await waitForTaskState(task.id, 'RUNNING', 15_000);
      const recovered = f.service.listActiveRuntimeTasks().find((r) => r.taskId === task.id)!;
      expect(recovered.runId).not.toBe(active.runId);
      expect(await f.service.recoverDisconnectedPostgres(request)).toBe(false);
      expect(await baseline()).toEqual(before);
      await requirePool().query(
        'UPDATE ape_dts_platform.data_sources SET row_version=row_version+1 WHERE id=$1',
        [task.sourceDataSourceId],
      );
      expect(
        await f.service.recoverDisconnectedPostgres({
          taskId: task.id,
          runId: recovered.runId,
          revisionId: recovered.revisionId,
          collectedAt: new Date().toISOString(),
        }),
      ).toBe(false);
      expect(f.service.listActiveRuntimeTasks().some((r) => r.runId === recovered.runId)).toBe(
        true,
      );
    } finally {
      await f.service.onApplicationShutdown('SIGTERM');
    }
  });

  it.each([false, true])(
    'fails only the MySQL task when snapshot evidence or CDC launch fails (spawn failure=%s)',
    async (failCdcSpawn) => {
      const fixture = await mysqlSnapshotRuntimeFixture(failCdcSpawn);
      try {
        const runId = await fixture.start();
        await fixture.finish(failCdcSpawn ? ['orders', 'items'] : ['orders']);
        await waitForTaskState(fixture.task.id, 'BLOCKED');
        const terminal = await runtimeTerminalState(fixture.task.id, runId);
        expect(terminal).toMatchObject({
          observedState: 'BLOCKED',
          activeReservations: '0',
          runFailureCode: failCdcSpawn
            ? 'MYSQL_CDC_START_FAILED'
            : 'MYSQL_SNAPSHOT_COMPLETION_UNPROVEN',
        });
        expect(await runtimePortReleaseStates(runId)).toEqual([true]);
        expect(fixture.spawns).toHaveLength(failCdcSpawn ? 2 : 1);
        expect((await fixture.tasks.get(administrator, fixture.task.id)).id).toBe(fixture.task.id);
        expect(await fixture.checkpoint()).toMatchObject({
          phase: failCdcSpawn ? 'CDC' : 'SNAPSHOT',
          binlog_filename: 'mysql-bin.000001',
          binlog_position: '157',
          active_run_id: runId,
        });
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it('retains the MySQL CDC handle and capacity when stage persistence and stop both fail', async () => {
    const fixture = await mysqlSnapshotRuntimeFixture();
    const append = fixture.audit.append.bind(fixture.audit);
    const start = fixture.processes.start.bind(fixture.processes);
    let restoreStop: (() => void) | undefined;
    let runId: string | undefined;
    const auditFailure = vi
      .spyOn(fixture.audit, 'append')
      .mockImplementation(async (client, event) => {
        if (event.action === 'TASK_MYSQL_SNAPSHOT_COMPLETED')
          throw new Error('fixture snapshot stage audit failed');
        return await append(client, event);
      });
    try {
      runId = await fixture.start();
      const cdcStart = vi.spyOn(fixture.processes, 'start').mockImplementation(async (spec) => {
        const handle = await start(spec);
        const stop = vi
          .spyOn(handle, 'stop')
          .mockRejectedValue(new Error('fixture CDC stop failed'));
        restoreStop = () => stop.mockRestore();
        return handle;
      });
      try {
        await fixture.finish(['orders', 'items']);
        await waitForTaskState(fixture.task.id, 'BLOCKED');
        expect(await runtimeTerminalState(fixture.task.id, runId)).toMatchObject({
          observedState: 'BLOCKED',
          runFailureCode: 'RUNTIME_PROCESS_HANDLE_MISSING',
          activeReservations: '1',
        });
        expect(await runtimePortReleaseStates(runId)).toEqual([false]);
        expect(fixture.spawns).toHaveLength(2);
        expect(fixture.children[1]!.signals).toEqual([]);
      } finally {
        cdcStart.mockRestore();
      }
    } finally {
      restoreStop?.();
      auditFailure.mockRestore();
      if (runId !== undefined) {
        const terminated = await fixture.supervisor.terminateActiveTask(
          fixture.task.id,
          'GRACEFUL',
        );
        expect(terminated.allConfirmed).toBe(true);
        expect(fixture.children.at(-1)!.signals).toContain('SIGINT');
        // Termination is now confirmed; release only this fixture's retained database reservations.
        await requirePool().query(
          "UPDATE ape_dts_platform.capacity_reservations SET state='RELEASED',released_at=statement_timestamp() WHERE task_id=$1::uuid AND run_id=$2::uuid AND state IN ('RESERVED','ACTIVE')",
          [fixture.task.id, runId],
        );
        await requirePool().query(
          'UPDATE ape_dts_platform.task_run_units SET metrics_port_released_at=statement_timestamp() WHERE task_id=$1::uuid AND run_id=$2::uuid',
          [fixture.task.id, runId],
        );
      }
      await fixture.cleanup();
    }
  });
  it('reports the exact controller lease and waits for its old session to release before continuing', async () => {
    const blocker = await tryAcquireControllerLock(requirePool());
    expect(blocker).not.toBeNull();
    if (blocker === null) throw new Error('Isolated database controller lock unavailable');
    try {
      const holders = await inspectControllerLockHolders(requirePool());
      expect(holders).toHaveLength(1);
      expect(holders[0]).toMatchObject({ username: databaseRole, state: 'idle' });
      expect(holders[0]!.pid).toBeGreaterThan(0);
      expect(holders[0]!.backend_started_at).toMatch(
        /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$/u,
      );
      expect(holders[0]).not.toHaveProperty('query');
      const waiting = waitForUpgradeControllerLock(platformDatabase!).catch((error) => error);
      await new Promise((resolve) => setTimeout(resolve, 50));
      await blocker.release();
      expect(await waiting).toBeUndefined();
    } finally {
      await blocker.release();
      await platformDatabase!.releaseControllerLock();
    }
    expect(await inspectControllerLockHolders(requirePool())).toEqual([]);
  });

  it('loads persisted recovery bindings and pauses unconfirmed old containers without SQL errors', async () => {
    const database = platformDatabase!;
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    await requirePool().query(
      `UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid AND task_id=$2::uuid`,
      [task.revisionId, task.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks
          SET current_applied_revision_id=$2::uuid, recovery_disposition='AUTO_ELIGIBLE', observed_state='BLOCKED'
        WHERE id=$1::uuid`,
      [task.id, task.revisionId],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET platform_instance_id=$2::uuid, status='BLOCKED', current_stage='RECONCILIATION',
              ended_at=statement_timestamp(), failure_code='RECOVERY_TEST_RESTART',
              failure_summary='Old test container interrupted'
        WHERE id=$1::uuid AND task_id=$3::uuid`,
      [runId, runtimePlatformInstanceId, task.id],
    );
    const audit = new AuditLedgerService(database);
    const preflight = new PreflightService(
      database,
      audit,
      new DataSourcesService(
        database,
        audit,
        new UnlockedKeyService(),
        new DataSourceAdapterRegistry([]),
      ),
      new CapacityReservationService(database),
      new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
      new ContinuityService(database, audit),
      '0.0.0',
      new RuntimeEnvironmentInspector(),
    );
    const actions = new PostgresRecoveryExternalActions(
      database,
      preflight,
      supervisorWithProcesses([]),
    );
    const candidate = {
      taskId: task.id,
      revisionId: task.revisionId,
      taskName: 'Recovery binding regression',
      taskType: 'KAFKA_KAFKA' as const,
      unitKeys: ['partition:0', 'partition:1'],
      units: 2,
    };
    const sessionId = randomUUID();
    try {
      await expect(actions.verifyContinuity(sessionId, [candidate])).resolves.toMatchObject({
        kind: 'PAUSED',
        code: 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED',
        progress: { taskId: task.id },
      });
      await expect(
        actions.verifyContinuity(sessionId, [
          { ...candidate, unitKeys: ['partition:0', 'partition:2'] },
        ]),
      ).resolves.toMatchObject({ kind: 'FAILED', code: 'RECOVERY_OLD_RUN_BINDING_INVALID' });
    } finally {
      // Other tests share this temporary database; release only this fixture's retained capacity.
      await requirePool().query(
        `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED', observed_state='STOPPED', recovery_disposition='NOT_APPLICABLE' WHERE id=$1::uuid`,
        [task.id],
      );
      await requirePool().query(
        `UPDATE ape_dts_platform.task_run_units SET status='STOPPED', ended_at=statement_timestamp(), metrics_port_released_at=statement_timestamp(), failure_code=NULL, failure_summary=NULL WHERE run_id=$1::uuid AND task_id=$2::uuid`,
        [runId, task.id],
      );
      await requirePool().query(
        `UPDATE ape_dts_platform.capacity_reservations
            SET state='RELEASED', released_at=statement_timestamp()
          WHERE task_id=$1::uuid AND run_id=$2::uuid AND state IN ('RESERVED','ACTIVE')`,
        [task.id, runId],
      );
    }
  });

  it('reports recovery log and metrics readiness for the currently managed run without SQL errors', async () => {
    const task = await createTask();
    const supervisor = supervisorWithProcesses([
      new FakeChildProcess(39_001, 'SIGINT'),
      new FakeChildProcess(39_002, 'SIGINT'),
    ]);
    try {
      const started = await supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `recovery-observability-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(started.state).toBe('SUCCEEDED');
      if (started.runId === null) throw new Error('Recovery observability run was not created');
      await requirePool().query(
        `UPDATE ape_dts_platform.task_runs SET log_storage_state='UNKNOWN'
          WHERE id=$1::uuid AND task_id=$2::uuid`,
        [started.runId, task.id],
      );
      await expect(supervisor.recoveryObservability([task.id])).resolves.toEqual([
        { taskId: task.id, runId: started.runId, metricsReady: true, logsReady: false },
      ]);
      await requirePool().query(
        `UPDATE ape_dts_platform.task_runs SET log_storage_state='HEALTHY'
          WHERE id=$1::uuid AND task_id=$2::uuid`,
        [started.runId, task.id],
      );
      await expect(supervisor.recoveryObservability([task.id])).resolves.toEqual([
        { taskId: task.id, runId: started.runId, metricsReady: true, logsReady: true },
      ]);
      await expect(supervisorWithProcesses([]).recoveryObservability([task.id])).resolves.toEqual([
        { taskId: task.id, runId: started.runId, metricsReady: false, logsReady: false },
      ]);
      await expect(supervisor.recoveryObservability([])).resolves.toEqual([]);
    } finally {
      await supervisor.onApplicationShutdown('SIGTERM');
    }
    await expect(supervisor.recoveryObservability([task.id])).resolves.toEqual([]);
  });

  it('renders the uploaded launch configuration without certification from the current stopped preflighted draft', async () => {
    const task = await createTask(false);
    const root = await mkdtemp(path.join(tmpdir(), 'scs-launch-binding-'));
    temporaryDirectories.push(root);
    const row = (
      await requirePool().query(
        'SELECT artifact_id, configuration FROM ape_dts_platform.task_revisions WHERE id = $1::uuid',
        [task.revisionId],
      )
    ).rows[0];
    const bytes = Buffer.from('test executable bytes, never executed');
    const hash = createHash('sha256').update(bytes).digest();
    await mkdir(path.join(root, 'artifacts'));
    await writeFile(path.join(root, 'artifacts', row.artifact_id), bytes);
    // These writes affect only this test's newly created artifact and its object.
    await requirePool().query(
      'UPDATE ape_dts_platform.stored_objects SET sha256=$2, size_bytes=$3 WHERE id=(SELECT stored_object_id FROM ape_dts_platform.artifacts WHERE id=$1::uuid)',
      [row.artifact_id, hash, bytes.length],
    );
    await requirePool().query(
      'UPDATE ape_dts_platform.artifacts SET declared_sha256=$2, actual_sha256=$2, size_bytes=$3 WHERE id=$1::uuid',
      [row.artifact_id, hash, bytes.length],
    );
    await requirePool().query(
      'DELETE FROM ape_dts_platform.artifact_certifications WHERE artifact_id=$1::uuid',
      [row.artifact_id],
    );
    await requirePool().query(
      "INSERT INTO ape_dts_platform.artifact_signature_checks(id,artifact_id,policy_version,status,checked_by_user_id) VALUES($1::uuid,$2::uuid,'DISABLED','NOT_REQUIRED',$3::uuid)",
      [randomUUID(), row.artifact_id, administrator.id],
    );
    const factory = new AuthenticatedRuntimeLaunchSpecFactory({
      adapters: new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
      artifactCache: new ArtifactCache(path.join(root, 'cache')),
      database: platformDatabase!,
      localObjectStorage: new LocalObjectStorage(root),
      platformVersion: '0.0.0',
      runtimeRoot: '/run/ape-dts',
      unlockedKey: new UnlockedKeyService(),
    });
    const launch = await factory.create({
      config: parsePlatformTaskConfig(row.configuration),
      metricsPort: 36001,
      revisionId: task.revisionId,
      taskId: task.id,
      runId: randomUUID(),
      unitId: randomUUID(),
      unitKey: 'partition:0',
    });
    expect(launch.iniContent).toContain('unit8.target');
    expect(launch.iniContent).toContain('36001');
    await expect(
      factory.verifyArtifact({ taskId: task.id, revisionId: task.revisionId }),
    ).rejects.toMatchObject({ code: 'RUNTIME_REVISION_BINDING_CHANGED' });
  });

  it('starts real binary subprocesses without cgroup delegation and releases their capacity on stop', async () => {
    const task = await createTask();
    const database = platformDatabase as PlatformDatabaseService;
    const controlledFactory = testLaunchFactory();
    const specs: RuntimeSpawnSpec[] = [];
    const supervisor = new RuntimeSupervisorService(
      database,
      new AuditLedgerService(database),
      new CapacityReservationService(database),
      new RuntimeProcessSupervisor({
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 2_000, terminateGraceMilliseconds: 2_000 },
      }),
      {
        create: (input) => {
          const spec = {
            ...controlledFactory.create(input),
            args: ['-e', 'setInterval(() => {}, 1000);'],
          };
          specs.push(spec);
          return spec;
        },
        verifyArtifact: (input) => controlledFactory.verifyArtifact(input),
      },
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    try {
      const started = await supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `real-process-start-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(started.state).toBe('SUCCEEDED');
      if (started.runId === null) throw new Error('Real process run was not created');
      const units = await requirePool().query<{ process_pid: number }>(
        `SELECT process_pid FROM ape_dts_platform.task_run_units WHERE run_id = $1::uuid`,
        [started.runId],
      );
      expect(units.rows).toHaveLength(2);
      for (const unit of units.rows) {
        expect(unit.process_pid).toBeGreaterThan(0);
        expect(() => process.kill(unit.process_pid, 0)).not.toThrow();
      }
      expect(new Set(specs.map((spec) => spec.runtimeDirectory)).size).toBe(2);
      for (const spec of specs)
        await expect(readFile(spec.iniPath, 'utf8')).resolves.toContain('[runtime]');
      const current = await requirePool().query<{ row_version: string }>(
        `SELECT row_version::text FROM ape_dts_platform.tasks WHERE id = $1::uuid`,
        [task.id],
      );
      const stopped = await supervisor.stop(administrator, {
        expectedTaskRowVersion: current.rows[0]?.row_version ?? '',
        idempotencyKey: `real-process-stop-${task.id}`,
        taskId: task.id,
      });
      expect(stopped.state).toBe('SUCCEEDED');
      await expect(runtimeTerminalState(task.id, started.runId)).resolves.toMatchObject({
        activeReservations: '0',
        observedState: 'STOPPED',
        runStatus: 'STOPPED',
        unitStatuses: ['STOPPED', 'STOPPED'],
      });
      await expect(runtimePortReleaseStates(started.runId)).resolves.toEqual([true, true]);
      for (const unit of units.rows) {
        expect(() => process.kill(unit.process_pid, 0)).toThrow();
      }
      for (const spec of specs)
        await expect(readFile(spec.iniPath)).rejects.toMatchObject({ code: 'ENOENT' });
    } finally {
      await supervisor.onApplicationShutdown('SIGTERM');
    }
  });

  it.each([undefined, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid process PID %s and stops all started handles before releasing capacity',
    async (pid) => {
      const task = await createTask();
      const children = [
        new FakeChildProcess(pid, 'SIGINT'),
        new FakeChildProcess(35_300, 'SIGINT'),
      ];
      const supervisor = supervisorWithProcesses([...children]);
      const result = await supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `invalid-pid-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(result).toMatchObject({
        failureCode: 'RUNTIME_PROCESS_START_FAILED',
        state: 'FAILED_RETRYABLE',
      });
      if (result.runId === null) throw new Error('Invalid PID did not retain its run id');
      expect(children.map((child) => child.signals)).toEqual([['SIGINT'], ['SIGINT']]);
      await expect(runtimeTerminalState(task.id, result.runId)).resolves.toMatchObject({
        activeReservations: '0',
        observedState: 'FAILED',
        runFailureCode: 'RUNTIME_PROCESS_START_FAILED',
        runStatus: 'FAILED',
        unitStatuses: ['FAILED', 'FAILED'],
      });
      await expect(runtimePortReleaseStates(result.runId)).resolves.toEqual([true, true]);
    },
  );

  it('starts all units behind a barrier, activates capacity, and stops gracefully', async () => {
    const task = await createTask(false);
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);
    const started = await requireRuntime().start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(started.state).toBe('SUCCEEDED');
    const riskRecords = await requirePool().query(
      'SELECT id FROM ape_dts_platform.risk_acceptances WHERE task_revision_id = $1::uuid',
      [task.revisionId],
    );
    expect(riskRecords.rowCount).toBe(0);
    const taskService = new TasksService(
      platformDatabase!,
      new AuditLedgerService(platformDatabase!),
    );
    const configuredTask = await taskService.get(administrator, task.id);
    expect(configuredTask.draft).toBeNull();
    expect(configuredTask.applied).toMatchObject({ id: task.revisionId, state: 'APPLIED' });
    const persistedRevision = (
      await requirePool().query(
        `SELECT artifact_id,configuration FROM ape_dts_platform.task_revisions WHERE id=$1 AND task_id=$2`,
        [task.revisionId, task.id],
      )
    ).rows[0];
    expect(configuredTask.applied?.artifactId).toBe(persistedRevision.artifact_id);
    expect(configuredTask.applied?.configuration).toEqual(persistedRevision.configuration);
    expect(
      (await taskService.list(administrator)).find((item) => item.id === task.id)?.applied,
    ).toEqual(configuredTask.applied);
    if (started.runId === null) throw new Error('Runtime run was not initialized');
    const running = await requirePool().query<{
      observed_state: string;
      revision_state: string;
      run_status: string;
      reservations: string;
      row_version: string;
      unit_statuses: string[];
    }>(
      `SELECT tasks.observed_state,
              tasks.row_version::text AS row_version,
              (SELECT state FROM ape_dts_platform.task_revisions WHERE id = $2::uuid) AS revision_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $3::uuid AND state = 'ACTIVE') AS reservations,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, task.revisionId, started.runId],
    );
    expect(running.rows[0]).toMatchObject({
      observed_state: 'RUNNING',
      revision_state: 'APPLIED',
      run_status: 'RUNNING',
      reservations: '2',
      unit_statuses: ['RUNNING', 'RUNNING'],
    });
    const startStages = await requirePool().query<{
      ended_at: Date | null;
      failure_code: string | null;
      progress_total: string | null;
      stage_type: string;
      started_at: Date | null;
      status: string;
    }>(
      `SELECT stage_type, status, started_at, ended_at, progress_total::text, failure_code
         FROM ape_dts_platform.task_run_stages
        WHERE run_id = $1::uuid
        ORDER BY stage_order`,
      [started.runId],
    );
    expect(startStages.rows.map(({ stage_type, status }) => [stage_type, status])).toEqual([
      ['PREFLIGHT', 'SUCCEEDED'],
      ['ENSURE_ARTIFACTS', 'SUCCEEDED'],
      ['PREPARE_RESOURCES', 'SUCCEEDED'],
      ['GENERATE_INI', 'SUCCEEDED'],
      ['STARTING_BARRIER', 'SUCCEEDED'],
      ['CDC_STARTED', 'SUCCEEDED'],
      ['MONITORING_READY', 'SUCCEEDED'],
    ]);
    expect(startStages.rows.every((stage) => stage.started_at instanceof Date)).toBe(true);
    expect(startStages.rows.every((stage) => stage.ended_at instanceof Date)).toBe(true);
    expect(startStages.rows.every((stage) => stage.progress_total === null)).toBe(true);
    expect(startStages.rows.every((stage) => stage.failure_code === null)).toBe(true);

    const repeated = await requireRuntime().start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(repeated.id).toBe(started.id);

    const stopped = await requireRuntime().stop(administrator, {
      expectedTaskRowVersion: running.rows[0]?.row_version ?? '',
      idempotencyKey: `stop-${task.id}`,
      taskId: task.id,
    });
    expect(stopped.state).toBe('SUCCEEDED');
    const final = await requirePool().query<{
      observed_state: string;
      run_status: string;
      released: string;
      unit_statuses: string[];
    }>(
      `SELECT tasks.observed_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state = 'RELEASED') AS released,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $2::uuid) AS unit_statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, started.runId],
    );
    expect(final.rows[0]).toMatchObject({
      observed_state: 'STOPPED',
      run_status: 'STOPPED',
      released: '2',
      unit_statuses: ['STOPPED', 'STOPPED'],
    });
    await expect(runtimePortReleaseStates(started.runId)).resolves.toEqual([true, true]);
  });

  it.each([
    { legacyTopics: ['SOURCE'] },
    { legacyTopics: ['TARGET'] },
    { legacyTopics: ['SOURCE', 'TARGET'] },
  ] as const)(
    'starts legacy Kafka topics without fabricated identities: %j',
    async ({ legacyTopics }) => {
      const task = await createTask(false, legacyTopics);
      const started = await requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `legacy-start-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(started.state).toBe('SUCCEEDED');
      const read = await new RuntimeReadService(platformDatabase!).get(administrator, task.id);
      expect(read.run?.status).toBe('RUNNING');
      expect(read.units.map((unit) => unit.status)).toEqual(['RUNNING', 'RUNNING']);
      const current = await new TasksService(
        platformDatabase!,
        new AuditLedgerService(platformDatabase!),
      ).get(administrator, task.id);
      const stopped = await requireRuntime().stop(administrator, {
        expectedTaskRowVersion: current.rowVersion,
        idempotencyKey: `legacy-stop-${task.id}`,
        taskId: task.id,
      });
      expect(stopped.state).toBe('SUCCEEDED');
    },
  );

  it.each([false, true])(
    'starts from real persisted preflight and continuity results (legacy Kafka: %s)',
    async (legacy) => {
      const task = await createTask(false, legacy ? ['SOURCE', 'TARGET'] : []);
      const database = platformDatabase!;
      const audit = new AuditLedgerService(database);
      const preflight = new PreflightService(
        database,
        audit,
        new DataSourcesService(
          database,
          audit,
          new UnlockedKeyService(),
          new DataSourceAdapterRegistry([
            {
              type: 'KAFKA',
              test: async ({ configuration }) => {
                if (configuration.type !== 'KAFKA') throw new Error('Expected Kafka configuration');
                return {
                  kind: 'KAFKA_CLUSTER',
                  serverVersion: null,
                  value: configuration.bootstrapServers[0]?.endsWith(':9092')
                    ? `unit8-source-cluster-${task.sourceId}`
                    : `unit8-target-cluster-${task.targetId}`,
                };
              },
              metadata: async (_input, query) =>
                query.resource === 'PARTITIONS'
                  ? [0, 1].map((id) => ({ name: String(id), identity: null, attributes: {} }))
                  : [
                      {
                        name: 'unit8.source',
                        identity: legacy ? null : `unit8-source-topic-${task.sourceId}`,
                        attributes: { partitionCount: '2' },
                      },
                      {
                        name: 'unit8.target',
                        identity: legacy ? null : `unit8-target-topic-${task.targetId}`,
                        attributes: { partitionCount: '1' },
                      },
                    ],
            },
          ]),
        ),
        new CapacityReservationService(database),
        new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
        new ContinuityService(database, audit),
        '0.0.0',
        new RuntimeEnvironmentInspector({
          SCS_RUNTIME_METRICS_PORT_MIN: '45000',
          SCS_RUNTIME_METRICS_PORT_MAX: '55000',
        }),
      );
      const checked = await preflight.run(administrator, task.id, {
        expectedTaskRowVersion: task.rowVersion,
        revisionId: task.revisionId,
      });
      expect(
        checked.state,
        JSON.stringify(checked.result.checks.filter((check) => check.state === 'FAILED')),
      ).toBe('PASSED');
      const persisted = (
        await requirePool().query<{ result: TaskPreflightResult }>(
          'SELECT result FROM ape_dts_platform.preflight_runs WHERE id = $1::uuid',
          [checked.id],
        )
      ).rows[0];
      expect(
        persisted?.result.checks.find((check) => check.key === 'TOPIC_IDENTITIES'),
      ).toMatchObject({
        state: 'PASSED',
        details: {
          topics: [
            { topic: 'unit8.source', partitionIds: [0, 1] },
            { topic: 'unit8.target', partitionCount: 1 },
          ],
        },
      });
      const supervisor = supervisorWithProcesses(
        [new FakeChildProcess(35_501, 'SIGINT'), new FakeChildProcess(35_502, 'SIGINT')],
        undefined,
        undefined,
        preflight as never,
      );
      const started = await supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `real-preflight-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(started.state).toBe('SUCCEEDED');
      const current = await new TasksService(database, audit).get(administrator, task.id);
      expect(
        (
          await supervisor.stop(administrator, {
            expectedTaskRowVersion: current.rowVersion,
            idempotencyKey: `real-preflight-stop-${task.id}`,
            taskId: task.id,
          })
        ).state,
      ).toBe('SUCCEEDED');
    },
  );

  it.each([false, true])(
    'runs all v2 topic partitions with fanout and restores them after platform shutdown (legacy Kafka: %s)',
    async (legacy) => {
      const fixture = await mappedRuntimeFixture(legacy);
      const { task, tasks, preflight, spawns } = fixture;
      expect(await runtimeRowCounts(task.id)).toEqual({
        operations: '0',
        reservations: '0',
        runs: '0',
      });
      expect(
        (
          await requirePool().query(
            'SELECT id FROM ape_dts_platform.preflight_runs WHERE task_revision_id=$1',
            [task.revisionId],
          )
        ).rows,
      ).toEqual([]);
      const checked = await preflight.run(administrator, task.id, {
        expectedTaskRowVersion: task.rowVersion,
        revisionId: task.revisionId,
      });
      expect(checked.state, JSON.stringify(checked.result.checks)).toBe('PASSED');
      expect(checked.result.capacity.requested).toBe(4);
      const persisted = (
        await requirePool().query<{ result: TaskPreflightResult }>(
          'SELECT result FROM ape_dts_platform.preflight_runs WHERE id=$1',
          [checked.id],
        )
      ).rows[0]!.result;
      expect(persisted.checks.find((check) => check.key === 'TOPIC_IDENTITIES')).toMatchObject({
        state: 'PASSED',
        details: {
          topics: expect.arrayContaining(
            runtimeMappings.map(({ source }) => ({
              clusterId: `mapped-source-${task.sourceId}`,
              topic: source,
              topicId: legacy ? null : `mapped-id-${source}`,
              partitionIds: [0, 1],
              partitionCount: 2,
              resourceType: 'KAFKA_SOURCE_TOPIC',
            })),
          ),
        },
      });
      const initial = await fixture.supervisor();
      let active = initial.service;
      const assertRun = async (runId: string) => {
        const units = (
          await requirePool().query<{ id: string; unit_key: string; status: string }>(
            'SELECT id,unit_key,status FROM ape_dts_platform.task_run_units WHERE run_id=$1 ORDER BY unit_key',
            [runId],
          )
        ).rows;
        expect(units.map((unit) => unit.unit_key)).toEqual(mappedUnitKeys);
        expect(units.map((unit) => unit.status)).toEqual([
          'RUNNING',
          'RUNNING',
          'RUNNING',
          'RUNNING',
        ]);
        for (const unit of units) {
          const spec = spawns.find((candidate) => candidate.unitId === unit.id)!;
          const mapping = runtimeMappings.find(({ source }) =>
            unit.unit_key.startsWith(`topic:${source}:`),
          )!;
          const partition = Number(unit.unit_key.slice(unit.unit_key.lastIndexOf(':') + 1));
          const ini = await readFile(spec.iniPath, 'utf8');
          expect(ini).toBe(spec.iniContent);
          expect(ini).toContain(`topic=${mapping.source}\n`);
          expect(ini).toContain(`partition=${partition}\n`);
          expect(ini).toContain(`group=ape-dts-task-${task.id}\n`);
          expect(ini).toContain(
            `topic_fanout_map=${mapping.source}:${mapping.targets.map(({ topic }) => topic).join('|')}\n`,
          );
          expect(ini).not.toContain(
            `topic=${runtimeMappings.find((row) => row !== mapping)!.source}\n`,
          );
        }
        expect(
          (
            await requirePool().query<{ unit_key: string }>(
              "SELECT unit_key FROM ape_dts_platform.capacity_reservations WHERE run_id=$1 AND state='ACTIVE' ORDER BY unit_key",
              [runId],
            )
          ).rows.map((row) => row.unit_key),
        ).toEqual(mappedUnitKeys);
      };
      try {
        const started = await active.start(administrator, {
          expectedTaskRowVersion: task.rowVersion,
          idempotencyKey: `mapped-start-${task.id}`,
          revisionId: task.revisionId,
          taskId: task.id,
        });
        expect(started.state, started.failureCode ?? '').toBe('SUCCEEDED');
        expect(spawns).toHaveLength(4);
        await assertRun(started.runId!);
        const snapshots = (
          await requirePool().query<{ topic_snapshots: unknown[] }>(
            'SELECT topic_snapshots FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1',
            [task.id],
          )
        ).rows[0]!.topic_snapshots;
        expect(snapshots).toHaveLength(5);

        await initial.service.beforeApplicationShutdown('SIGTERM');
        const shutdownTask = await tasks.get(administrator, task.id);
        expect(shutdownTask).toMatchObject({ desiredState: 'RUNNING', observedState: 'BLOCKED' });
        const restoredPreflight = await preflight.runForSystemRecovery({
          expectedOldRunId: started.runId!,
          expectedPlatformInstanceId: initial.instanceId,
          expectedTaskRowVersion: shutdownTask.rowVersion,
          revisionId: task.revisionId,
          taskId: task.id,
        });
        expect(restoredPreflight.state, JSON.stringify(restoredPreflight.result.checks)).toBe(
          'PASSED',
        );
        expect(restoredPreflight.result.capacity.requested).toBe(4);
        const replacement = await fixture.supervisor();
        active = replacement.service;
        const restored = await active.recoverPersistedTask({
          expectedTaskRowVersion: shutdownTask.rowVersion,
          oldRunId: started.runId!,
          platformInstanceId: initial.instanceId,
          revisionId: task.revisionId,
          taskId: task.id,
          unitKeys: mappedUnitKeys,
        });
        expect(restored.runId).not.toBe(started.runId);
        expect(spawns).toHaveLength(8);
        await assertRun(restored.runId);
        expect(
          (
            await requirePool().query<{ count: string }>(
              "SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE task_id=$1 AND state IN ('ACTIVE','RESERVED')",
              [task.id],
            )
          ).rows[0]!.count,
        ).toBe('4');
        const current = await tasks.get(administrator, task.id);
        expect(
          (
            await active.stop(administrator, {
              expectedTaskRowVersion: current.rowVersion,
              idempotencyKey: `mapped-stop-${task.id}`,
              taskId: task.id,
            })
          ).state,
        ).toBe('SUCCEEDED');
        expect(await runtimePortReleaseStates(restored.runId)).toEqual([true, true, true, true]);
      } finally {
        await active.onApplicationShutdown('SIGTERM');
      }
    },
  );

  it('restarts a stopped Kafka task from an applied-derived draft without replacing its history or topic identities', async () => {
    const fixture = await mappedRuntimeFixture(false);
    const { task, tasks, preflight, spawns } = fixture;
    const { service } = await fixture.supervisor();
    try {
      const firstPreflight = await preflight.run(administrator, task.id, {
        expectedTaskRowVersion: task.rowVersion,
        revisionId: task.revisionId,
      });
      expect(firstPreflight.state, JSON.stringify(firstPreflight.result.checks)).toBe('PASSED');
      let current = await tasks.get(administrator, task.id);
      const first = await service.start(administrator, {
        expectedTaskRowVersion: current.rowVersion,
        idempotencyKey: `applied-edit-first-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      });
      expect(first.state, first.failureCode ?? '').toBe('SUCCEEDED');
      current = await tasks.get(administrator, task.id);
      const original = structuredClone(current.applied!);
      const baseline = (
        await requirePool().query<{ topic_snapshots: unknown }>(
          'SELECT topic_snapshots FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1',
          [task.id],
        )
      ).rows[0]!.topic_snapshots;
      expect(current.draft).toBeNull();
      expect(
        (
          await service.stop(administrator, {
            expectedTaskRowVersion: current.rowVersion,
            idempotencyKey: `applied-edit-stop-${task.id}`,
            taskId: task.id,
          })
        ).state,
      ).toBe('SUCCEEDED');
      current = await tasks.get(administrator, task.id);
      const configuration = parsePlatformTaskConfig(original.configuration);
      current = await tasks.saveDraft(administrator, task.id, {
        artifactId: original.artifactId,
        configuration: {
          ...configuration,
          tuning: { ...configuration.tuning, batchSize: configuration.tuning.batchSize + 1 },
        },
        expectedDraftVersion: original.draftVersion,
        expectedTaskRowVersion: current.rowVersion,
        revisionId: original.id,
        taskType: 'KAFKA_KAFKA',
      });
      const nextRevisionId = current.draft!.id;
      expect(nextRevisionId).not.toBe(original.id);
      expect(current.applied).toEqual(original);
      const nextPreflight = await preflight.run(administrator, task.id, {
        expectedTaskRowVersion: current.rowVersion,
        revisionId: nextRevisionId,
      });
      expect(nextPreflight.state, JSON.stringify(nextPreflight.result.checks)).toBe('PASSED');
      current = await tasks.get(administrator, task.id);
      const second = await service.start(administrator, {
        expectedTaskRowVersion: current.rowVersion,
        idempotencyKey: `applied-edit-second-${task.id}`,
        revisionId: nextRevisionId,
        taskId: task.id,
      });
      expect(second.state, second.failureCode ?? '').toBe('SUCCEEDED');
      expect(second.runId).not.toBe(first.runId);
      expect(spawns).toHaveLength(8);
      const revisions = (
        await requirePool().query<{ id: string; state: string; configuration: unknown }>(
          'SELECT id,state,configuration FROM ape_dts_platform.task_revisions WHERE task_id=$1 ORDER BY revision_number',
          [task.id],
        )
      ).rows;
      expect(revisions).toEqual([
        { id: original.id, state: 'SUPERSEDED', configuration: original.configuration },
        {
          id: nextRevisionId,
          state: 'APPLIED',
          configuration: {
            ...configuration,
            tuning: { ...configuration.tuning, batchSize: configuration.tuning.batchSize + 1 },
          },
        },
      ]);
      expect(
        (
          await requirePool().query<{ id: string; status: string }>(
            'SELECT id,status FROM ape_dts_platform.task_runs WHERE task_id=$1 ORDER BY created_at,id',
            [task.id],
          )
        ).rows,
      ).toEqual([
        { id: first.runId, status: 'STOPPED' },
        { id: second.runId, status: 'RUNNING' },
      ]);
      expect(
        (
          await requirePool().query<{ topic_snapshots: unknown }>(
            'SELECT topic_snapshots FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1',
            [task.id],
          )
        ).rows[0]!.topic_snapshots,
      ).toEqual(baseline);
      current = await tasks.get(administrator, task.id);
      expect(current.id).toBe(task.id);
      expect(current.currentAppliedRevisionId).toBe(nextRevisionId);
      expect(
        (
          await service.stop(administrator, {
            expectedTaskRowVersion: current.rowVersion,
            idempotencyKey: `applied-edit-final-stop-${task.id}`,
            taskId: task.id,
          })
        ).state,
      ).toBe('SUCCEEDED');
    } finally {
      await service.onApplicationShutdown('SIGTERM');
    }
  });

  it.each([false, true])(
    'adopts PG table expansion continuity only after successful runtime start (spawn fails: %s)',
    async (spawnFails) => {
      const {
        created,
        tasks,
        preflight,
        service,
        launchedConfigurations,
        advanceSlot,
        hideAddedTopic,
      } = await pgExpansionRuntimeFixture(spawnFails);
      const baseline = async () =>
        (
          await requirePool().query(
            'SELECT * FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid',
            [created.id],
          )
        ).rows[0];
      try {
        let current = await tasks.get(administrator, created.id);
        const firstPreflight = await preflight.run(administrator, current.id, {
          expectedTaskRowVersion: current.rowVersion,
          revisionId: current.draft!.id,
        });
        expect(firstPreflight.state, JSON.stringify(firstPreflight.result.checks)).toBe('PASSED');
        current = await tasks.get(administrator, current.id);
        const first = await service.start(administrator, {
          taskId: current.id,
          revisionId: current.draft!.id,
          expectedTaskRowVersion: current.rowVersion,
          idempotencyKey: randomUUID(),
        });
        expect(first.state, first.failureCode ?? '').toBe('SUCCEEDED');
        current = await tasks.get(administrator, current.id);
        const original = structuredClone(current.applied!);
        expect(
          (
            await service.stop(administrator, {
              taskId: current.id,
              expectedTaskRowVersion: current.rowVersion,
              idempotencyKey: randomUUID(),
            })
          ).state,
        ).toBe('SUCCEEDED');
        const originalBaseline = structuredClone(await baseline());
        current = await tasks.get(administrator, current.id);
        const config = parsePlatformTaskConfig(original.configuration);
        if (config.kind !== 'PG_TO_KAFKA' || config.schemaVersion !== 2)
          throw new Error('Expected PG v2 fixture');
        current = await tasks.saveDraft(administrator, current.id, {
          taskType: 'PG_KAFKA',
          artifactId: original.artifactId,
          revisionId: original.id,
          expectedDraftVersion: original.draftVersion,
          expectedTaskRowVersion: current.rowVersion,
          acknowledgePgTableAddition: true,
          configuration: {
            ...config,
            mappings: [
              ...config.mappings,
              {
                source: { schema: 'public', name: 'added_table' },
                targets: [
                  {
                    topic: 'unit8.pg_added',
                    ...(!spawnFails
                      ? { topicCreation: { numPartitions: 1, replicationFactor: 1 } }
                      : {}),
                  },
                ],
              },
            ],
          },
        });
        const nextRevision = current.draft!.id;
        advanceSlot();
        if (!spawnFails) hideAddedTopic();
        let checked = await preflight.run(administrator, current.id, {
          expectedTaskRowVersion: current.rowVersion,
          revisionId: nextRevision,
        });
        if (!spawnFails) {
          expect(checked.state).toBe('FAILED');
          expect(checked.result.checks).toContainEqual(
            expect.objectContaining({ code: 'TARGET_TOPIC_CREATION_PENDING' }),
          );
          expect(
            await preflight.prepareFirstStartTopic(administrator, current.id, {
              expectedTaskRowVersion: current.rowVersion,
              revisionId: nextRevision,
            }),
          ).toBe(true);
          current = await tasks.get(administrator, current.id);
          checked = await preflight.run(administrator, current.id, {
            expectedTaskRowVersion: current.rowVersion,
            revisionId: nextRevision,
          });
        }
        expect(checked.state, JSON.stringify(checked.result.checks)).toBe('PASSED');
        expect(checked.result.checks).toContainEqual(
          expect.objectContaining({ code: 'PG_TABLE_ADDITION_READY', state: 'PASSED' }),
        );
        expect(await baseline()).toEqual(originalBaseline);
        current = await tasks.get(administrator, current.id);
        expect(current.applied).toEqual(original);
        const started = await service.start(administrator, {
          taskId: current.id,
          revisionId: nextRevision,
          expectedTaskRowVersion: current.rowVersion,
          idempotencyKey: randomUUID(),
        });
        expect(started.state, started.failureCode ?? '').toBe(
          spawnFails ? 'FAILED_RETRYABLE' : 'SUCCEEDED',
        );
        if (spawnFails) expect(started.failureCode).toBe('RUNTIME_PROCESS_START_FAILED');
        expect(launchedConfigurations).toHaveLength(2);
        expect(launchedConfigurations[1]).toMatchObject({
          source: { includeSnapshot: false, slotName: config.source.slotName },
          mappings: expect.arrayContaining([
            expect.objectContaining({ source: { schema: 'public', name: 'added_table' } }),
          ]),
        });
        current = await tasks.get(administrator, current.id);
        const revisions = (
          await requirePool().query<{ id: string; state: string; configuration: unknown }>(
            'SELECT id,state,configuration FROM ape_dts_platform.task_revisions WHERE task_id=$1 ORDER BY revision_number',
            [created.id],
          )
        ).rows;
        expect(revisions[0]).toEqual({
          id: original.id,
          state: spawnFails ? 'APPLIED' : 'SUPERSEDED',
          configuration: original.configuration,
        });
        if (spawnFails) {
          expect(current.applied).toEqual(original);
          expect(await baseline()).toEqual(originalBaseline);
        } else {
          expect(current.currentAppliedRevisionId).toBe(nextRevision);
          expect(revisions[1]?.state).toBe('APPLIED');
          expect(await baseline()).toMatchObject({
            task_revision_id: nextRevision,
            pg_slot_name: config.source.slotName,
            pg_confirmed_flush_lsn: '0/200',
            topic_snapshots: expect.arrayContaining([
              expect.objectContaining({ topic: 'unit8.pg_added', topicId: 'id-unit8.pg_added' }),
            ]),
          });
          expect(
            (
              await service.stop(administrator, {
                taskId: current.id,
                expectedTaskRowVersion: current.rowVersion,
                idempotencyKey: randomUUID(),
              })
            ).state,
          ).toBe('SUCCEEDED');
        }
        expect(
          (
            await requirePool().query<{ task_id: string; id: string }>(
              'SELECT task_id,id FROM ape_dts_platform.task_runs WHERE task_id=$1 ORDER BY created_at,id',
              [created.id],
            )
          ).rows,
        ).toEqual([
          { task_id: created.id, id: first.runId },
          { task_id: created.id, id: started.runId },
        ]);
      } finally {
        await service.onApplicationShutdown('SIGTERM');
      }
    },
  );

  it('does not partially start a v2 task when the second source partition list is incomplete', async () => {
    const { task, preflight, spawns, supervisor } = await mappedRuntimeFixture(true, true);
    const checked = await preflight.run(administrator, task.id, {
      expectedTaskRowVersion: task.rowVersion,
      revisionId: task.revisionId,
    });
    expect(checked.state).toBe('FAILED');
    expect(checked.result.checks).toContainEqual(
      expect.objectContaining({
        key: 'TOPIC_IDENTITIES',
        code: 'KAFKA_TOPIC_METADATA_INVALID',
        state: 'FAILED',
      }),
    );
    const attempt = await supervisor();
    try {
      await expect(
        attempt.service.start(administrator, {
          expectedTaskRowVersion: task.rowVersion,
          idempotencyKey: `mapped-partial-${task.id}`,
          revisionId: task.revisionId,
          taskId: task.id,
        }),
      ).rejects.toMatchObject({ code: 'RUNTIME_CONTINUITY_BLOCKED' });
      expect(spawns).toEqual([]);
      expect(await runtimeRowCounts(task.id)).toMatchObject({ reservations: '0', runs: '0' });
    } finally {
      await attempt.service.onApplicationShutdown('SIGTERM');
    }
  });

  it.each(['KAFKA_SOURCE_TOPIC', 'KAFKA_TARGET_TOPIC'])(
    'still rejects a changed modern Topic binding: %s',
    async (resourceType) => {
      const task = await createTask(false, [
        resourceType === 'KAFKA_SOURCE_TOPIC' ? 'TARGET' : 'SOURCE',
      ]);
      await requirePool().query(
        `UPDATE ape_dts_platform.external_resources SET stable_identity = 'changed-topic'
          WHERE task_id = $1::uuid AND resource_type = $2`,
        [task.id, resourceType],
      );
      const before = await runtimeRowCounts(task.id);
      await expect(
        requireRuntime().start(administrator, {
          expectedTaskRowVersion: task.rowVersion,
          idempotencyKey: `changed-topic-${task.id}`,
          revisionId: task.revisionId,
          taskId: task.id,
        }),
      ).rejects.toMatchObject({
        code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH',
        details: { resourceType },
      });
      await expectStartRejection(task.id, before, 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
    },
  );

  it.each(['CLUSTER', 'PARTITIONS'] as const)(
    'keeps the %s startup barrier for legacy Kafka',
    async (changed) => {
      const task = await createTask(false, ['SOURCE', 'TARGET']);
      if (changed === 'CLUSTER') {
        await requirePool().query(
          `UPDATE ape_dts_platform.data_source_identity_observations
              SET identity_value = 'changed-cluster' WHERE data_source_id = $1::uuid`,
          [task.sourceId],
        );
      } else {
        await requirePool().query(
          `UPDATE ape_dts_platform.preflight_runs
              SET result = jsonb_set(result, '{checks,6,details,topics,0,partitionIds}', '[1,1]'::jsonb)
            WHERE task_revision_id = $1::uuid AND state = 'PASSED'`,
          [task.revisionId],
        );
      }
      const before = await runtimeRowCounts(task.id);
      await expect(
        requireRuntime().start(administrator, {
          expectedTaskRowVersion: task.rowVersion,
          idempotencyKey: `legacy-barrier-${task.id}`,
          revisionId: task.revisionId,
          taskId: task.id,
        }),
      ).rejects.toMatchObject({
        code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH',
        details: {
          reason:
            changed === 'CLUSTER'
              ? 'DATA_SOURCE_IDENTITY_CHANGED'
              : 'KAFKA_PARTITION_BASELINE_MISSING',
        },
      });
      await expectStartRejection(task.id, before, 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
    },
  );

  it('serializes concurrent metrics-port allocation and assigns disjoint ports', async () => {
    const firstTask = await createTask();
    const secondTask = await createTask();
    const firstSupervisor = supervisorWithProcesses([
      new FakeChildProcess(35_191, 'SIGINT'),
      new FakeChildProcess(35_192, 'SIGINT'),
    ]);
    const secondSupervisor = supervisorWithProcesses([
      new FakeChildProcess(35_193, 'SIGINT'),
      new FakeChildProcess(35_194, 'SIGINT'),
    ]);
    const attempts = [
      {
        supervisor: firstSupervisor,
        task: firstTask,
        promise: firstSupervisor.start(administrator, {
          expectedTaskRowVersion: firstTask.rowVersion,
          idempotencyKey: `metrics-port-first-${firstTask.id}`,
          revisionId: firstTask.revisionId,
          taskId: firstTask.id,
        }),
      },
      {
        supervisor: secondSupervisor,
        task: secondTask,
        promise: secondSupervisor.start(administrator, {
          expectedTaskRowVersion: secondTask.rowVersion,
          idempotencyKey: `metrics-port-second-${secondTask.id}`,
          revisionId: secondTask.revisionId,
          taskId: secondTask.id,
        }),
      },
    ] as const;

    const started = await Promise.all(attempts.map((attempt) => attempt.promise));
    const runIds = started.map((result) => result.runId);
    if (runIds.some((runId) => runId === null)) {
      throw new Error('Concurrent metrics-port fixtures did not retain both run ids');
    }
    const ports = await requirePool().query<{ metrics_port: number }>(
      `SELECT metrics_port
         FROM ape_dts_platform.task_run_units
        WHERE run_id = ANY($1::uuid[])
        ORDER BY metrics_port`,
      [runIds],
    );
    expect(ports.rows).toHaveLength(4);
    expect(new Set(ports.rows.map((row) => row.metrics_port)).size).toBe(4);

    for (const attempt of attempts) {
      const current = await requirePool().query<{ row_version: string }>(
        `SELECT row_version::text
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid`,
        [attempt.task.id],
      );
      await attempt.supervisor.stop(administrator, {
        expectedTaskRowVersion: current.rows[0]?.row_version ?? '',
        idempotencyKey: `metrics-port-stop-${attempt.task.id}`,
        taskId: attempt.task.id,
      });
    }
  });

  it('blocks a confirmed stop and releases capacity when plaintext configuration cleanup fails', async () => {
    const task = await createTask();
    const children = [
      new FakeChildProcess(35_231, 'SIGINT'),
      new FakeChildProcess(35_232, 'SIGINT'),
    ];

    const supervisor = supervisorWithProcesses([...children], undefined, async () => {
      throw new Error('injected configuration cleanup failure');
    });
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `configuration-cleanup-start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    if (started.runId === null) throw new Error('Configuration cleanup fixture did not start');
    const running = await requirePool().query<{ row_version: string }>(
      `SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1::uuid`,
      [task.id],
    );

    const stopped = await supervisor.stop(administrator, {
      expectedTaskRowVersion: running.rows[0]?.row_version ?? '',
      idempotencyKey: `configuration-cleanup-stop-${task.id}`,
      taskId: task.id,
    });

    expect(stopped).toMatchObject({
      failureCode: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
      state: 'BLOCKED_MANUAL',
    });
    await expect(runtimeTerminalState(task.id, started.runId)).resolves.toMatchObject({
      activeReservations: '0',
      observedState: 'BLOCKED',
      runFailureCode: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
      runStatus: 'BLOCKED',
      unitStatuses: ['STOPPED', 'STOPPED'],
    });
  });

  it('surfaces an unconfigured launch adapter as the typed 503 service error', async () => {
    const task = await createTask();
    const before = await runtimeRowCounts(task.id);

    const database = platformDatabase as PlatformDatabaseService;
    const supervisor = new RuntimeSupervisorService(
      database,
      new AuditLedgerService(database),
      new CapacityReservationService(database),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            throw new Error('unconfigured launch must not spawn');
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
      }),
      new UnconfiguredRuntimeLaunchSpecFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );

    await expect(
      supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `unconfigured-launch-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED' });
    await expectStartRejection(task.id, before, 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED');
  });

  it('records an authenticated launch binding failure without exposing its private cause', async () => {
    const task = await createTask();
    const before = await runtimeRowCounts(task.id);
    const database = platformDatabase as PlatformDatabaseService;
    const supervisor = new RuntimeSupervisorService(
      database,
      new AuditLedgerService(database),
      new CapacityReservationService(database),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            throw new Error('launch must not spawn');
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
      }),
      {
        create: async () => {
          throw new AuthenticatedRuntimeLaunchSpecError('RUNTIME_REVISION_BINDING_CHANGED', {
            cause: new Error('private artifact metadata'),
          });
        },
      } as never,
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    await expect(
      supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `authenticated-binding-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_REVISION_BINDING_CHANGED' });
    await expectStartRejection(task.id, before, 'RUNTIME_REVISION_BINDING_CHANGED');
    const recorded = await requirePool().query<{ failure_summary: string }>(
      `SELECT failure_summary FROM ape_dts_platform.task_operations
        WHERE task_id = $1::uuid AND idempotency_key = $2`,
      [task.id, `authenticated-binding-${task.id}`],
    );
    expect(recorded.rows[0]?.failure_summary).not.toContain('private artifact metadata');
  });

  it('keeps a successful process exit STOPPED while persisting durable log recovery state', async () => {
    const task = await createTask();
    const finalizedRuns: string[] = [];
    const children = [
      new FakeChildProcess(31_111, 'SIGINT'),
      new FakeChildProcess(31_112, 'SIGINT'),
    ];
    let nextChild = 0;
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            const child = children[nextChild];
            nextChild += 1;
            if (child === undefined) {
              throw new Error('log state integration exhausted fake children');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: {
          append: () => undefined,
          close: () => ({ recoveryState: 'DURABLE_PENDING', storageState: 'DEGRADED' }),
          finalizeRun: (runId) => finalizedRuns.push(runId),
        },
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `log-state-start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    const running = await requirePool().query<{ row_version: string }>(
      `SELECT row_version::text
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid`,
      [task.id],
    );

    const stopped = await supervisor.stop(administrator, {
      expectedTaskRowVersion: running.rows[0]?.row_version ?? '',
      idempotencyKey: `log-state-stop-${task.id}`,
      taskId: task.id,
    });

    expect(stopped.state).toBe('SUCCEEDED');
    expect(finalizedRuns).toEqual([started.runId]);
    await expect(
      requirePool().query(
        `SELECT status, has_gap, log_storage_state, log_recovery_state
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid`,
        [started.runId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          has_gap: false,
          log_recovery_state: 'DURABLE_PENDING',
          log_storage_state: 'DEGRADED',
          status: 'STOPPED',
        },
      ],
    });
  });

  it('uses the platform launch factory for system-planned runtime units', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-controlled-'));
    temporaryDirectories.push(root);
    const children = [
      new FakeChildProcess(32_001, 'SIGINT'),
      new FakeChildProcess(32_002, 'SIGINT'),
    ];
    const captured: RuntimeSpawnSpec[] = [];
    const launchFactory = new ControlledRuntimeLaunchSpecFactory({
      environment: { SCS_RUNTIME_CONFIGURATION: 'controlled' },
      executablePath: process.execPath,
      render: () => ({
        iniContent: '[runtime]\nsource=platform\n',
        log4rsContent: 'appenders: []\n',
      }),
      runtimeRoot: root,
    });
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: (spec) => {
            captured.push(spec);
            const child = children.shift();
            if (child === undefined) {
              throw new Error('controlled child fixture exhausted');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      launchFactory,
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `controlled-launch-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(started.state).toBe('SUCCEEDED');
    expect(captured).toHaveLength(2);
    for (const spec of captured) {
      expect(spec.executablePath).toBe(process.execPath);
      expect(spec.args[0]).toBe('--config');
      expect(spec.args[1]).toBe(spec.iniPath);
      expect(spec.cwd).toBe(spec.runtimeDirectory);
      expect(spec.env).toEqual({ SCS_RUNTIME_CONFIGURATION: 'controlled' });
      expect(spec.iniContent).toBe('[runtime]\nsource=platform\n');
      expect(spec.log4rsContent).toBe('appenders: []\n');
      expect(spec.runtimeDirectory.startsWith(root + path.sep)).toBe(true);
    }
    await supervisor.onApplicationShutdown('TEST');
  });

  it('discards only a confirmed old task recovery, retaining configuration and failure history', async () => {
    const task = await createTask();
    const other = await createTask();
    const persisted = await createPersistedActiveRun(task);
    const supervisor = supervisorWithProcesses([]);
    await requirePool().query(
      "UPDATE ape_dts_platform.task_revisions SET state='APPLIED' WHERE id=$1::uuid",
      [task.revisionId],
    );
    await requirePool().query(
      "UPDATE ape_dts_platform.tasks SET current_draft_revision_id=NULL, current_applied_revision_id=$2::uuid, recovery_disposition='AUTO_ELIGIBLE' WHERE id=$1::uuid",
      [task.id, task.revisionId],
    );
    const configuration = (
      await requirePool().query(
        'SELECT configuration FROM ape_dts_platform.task_revisions WHERE id=$1::uuid',
        [task.revisionId],
      )
    ).rows[0].configuration;
    await supervisor.reconcilePersistedRuns();
    const row = (
      await requirePool().query(
        'SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1::uuid',
        [task.id],
      )
    ).rows[0];
    await expect(
      supervisor.discardRecovery({ ...administrator, role: 'DEVELOPER' }, task.id, row.row_version),
    ).rejects.toMatchObject({ code: 'RUNTIME_ACCESS_DENIED' });
    await expect(
      supervisor.discardRecovery(administrator, task.id, '999999'),
    ).rejects.toMatchObject({ code: 'RUNTIME_TASK_VERSION_CONFLICT' });
    await expect(
      supervisor.discardRecovery(administrator, task.id, row.row_version),
    ).resolves.toEqual({ discarded: true });
    await expect(runtimeTerminalState(task.id, persisted.runId)).resolves.toMatchObject({
      activeReservations: '0',
      observedState: 'STOPPED',
      runStatus: 'BLOCKED',
    });
    await expect(runtimePortReleaseStates(persisted.runId)).resolves.toEqual([true, true]);
    expect(
      (
        await requirePool().query(
          'SELECT configuration FROM ape_dts_platform.task_revisions WHERE id=$1::uuid',
          [task.revisionId],
        )
      ).rows[0].configuration,
    ).toEqual(configuration);
    const editable = await new TasksService(
      platformDatabase!,
      new AuditLedgerService(platformDatabase!),
    ).get(administrator, task.id);
    expect(editable.draft).toMatchObject({ state: 'READY_DRAFT', configuration });
    expect(editable.draft?.id).not.toBe(task.revisionId);
    expect(
      (
        await requirePool().query(
          'SELECT desired_state, recovery_disposition FROM ape_dts_platform.tasks WHERE id=$1::uuid',
          [task.id],
        )
      ).rows[0],
    ).toEqual({ desired_state: 'STOPPED', recovery_disposition: 'NOT_APPLICABLE' });
    expect(
      (
        await requirePool().query(
          'SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1::uuid',
          [other.id],
        )
      ).rows[0].row_version,
    ).toBe(other.rowVersion);
    expect(
      (
        await requirePool().query(
          'SELECT action FROM ape_dts_platform.audit_events WHERE subject_id=$1::uuid ORDER BY sequence DESC LIMIT 1',
          [task.id],
        )
      ).rows[0].action,
    ).toBe('TASK_RUNTIME_RECOVERY_DISCARDED');
  });

  it('records a confirmed retry for only the selected old task while preserving its applied configuration', async () => {
    const task = await createTask();
    const other = await createTask();
    const old = await createPersistedActiveRun(task);
    const instanceId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.platform_instances
      (id,platform_version,started_at,last_heartbeat_at,ended_at,clean_shutdown)
      VALUES ($1,'retry-test',now(),now(),now(),true)`,
      [instanceId],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks
      SET desired_state='RUNNING',observed_state='BLOCKED',current_applied_revision_id=$2,
          recovery_disposition='MANUAL_REQUIRED' WHERE id=$1`,
      [task.id, task.revisionId],
    );
    await supervisorWithProcesses([]).reconcilePersistedRuns();
    const coordinator = {
      runCandidate: vi
        .fn<(sessionId: string, candidate: unknown) => Promise<{ state: string }>>()
        .mockResolvedValue({ state: 'READY' }),
    };
    const service = new TaskRecoveryService(
      platformDatabase!,
      {
        getStatus: () => ({ phase: 'READY', platformInstanceId: instanceId }),
      } as never,
      supervisorWithProcesses([]),
      coordinator as never,
      new AuditLedgerService(platformDatabase!),
    );
    const rowVersion = (
      await requirePool().query(
        `SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1`,
        [task.id],
      )
    ).rows[0].row_version;
    await expect(service.retry(administrator, task.id, rowVersion)).resolves.toEqual({
      state: 'READY',
    });
    expect(coordinator.runCandidate).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        taskId: task.id,
        revisionId: task.revisionId,
        units: 2,
      }),
    );
    const sessionId = coordinator.runCandidate.mock.calls[0]![0];
    const steps = await requirePool().query(
      `SELECT step_type,status,progress
      FROM ape_dts_platform.recovery_steps WHERE recovery_session_id=$1 ORDER BY position`,
      [sessionId],
    );
    expect(steps.rows).toHaveLength(7);
    expect(steps.rows.find((step) => step.step_type === 'VERIFY_CONTINUITY')?.progress).toEqual({
      oldRuntimeStoppedConfirmed: true,
    });
    expect(
      (
        await requirePool().query(
          `SELECT current_applied_revision_id,recovery_disposition
      FROM ape_dts_platform.tasks WHERE id=$1`,
          [task.id],
        )
      ).rows[0],
    ).toMatchObject({
      current_applied_revision_id: task.revisionId,
      recovery_disposition: 'AUTO_ELIGIBLE',
    });
    expect(
      (
        await requirePool().query(
          `SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1`,
          [other.id],
        )
      ).rows[0].row_version,
    ).toBe(other.rowVersion);
    expect(
      (
        await requirePool().query(
          `SELECT action FROM ape_dts_platform.audit_events
      WHERE subject_id=$1 ORDER BY sequence DESC LIMIT 1`,
          [task.id],
        )
      ).rows[0].action,
    ).toBe('TASK_RUNTIME_RECOVERY_RETRY_REQUESTED');
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED',recovery_disposition='NOT_APPLICABLE' WHERE id=$1`,
      [task.id],
    );
    await new CapacityReservationService(platformDatabase!).release(old.runId);
  });

  it('selects only old runs for a recovery session and never marks its current run manual', async () => {
    const oldTask = await createTask();
    const currentTask = await createTask();
    await createPersistedActiveRun(oldTask);
    const current = await createPersistedActiveRun(currentTask);
    const instanceId = randomUUID();
    try {
      for (const task of [oldTask, currentTask]) {
        await requirePool().query(
          "UPDATE ape_dts_platform.tasks SET current_applied_revision_id=$2::uuid,recovery_disposition='AUTO_ELIGIBLE' WHERE id=$1::uuid",
          [task.id, task.revisionId],
        );
      }
      const sessionId = await new BootstrapPersistenceService().startExistingRecovery(
        requirePool(),
        instanceId,
      );
      await requirePool().query(
        'UPDATE ape_dts_platform.task_runs SET platform_instance_id=$2::uuid WHERE id=$1::uuid',
        [current.runId, instanceId],
      );
      const store = new PostgresRecoveryStore(platformDatabase!);
      expect((await store.listCandidates(sessionId)).map((task) => task.taskId)).toEqual([
        oldTask.id,
      ]);
      await requirePool().query(
        "UPDATE ape_dts_platform.recovery_sessions SET state='PAUSED' WHERE id=$1::uuid",
        [sessionId],
      );
      await requirePool().query(
        "UPDATE ape_dts_platform.recovery_steps SET status='PAUSED' WHERE recovery_session_id=$1::uuid AND step_type='PLAN_CAPACITY'",
        [sessionId],
      );
      expect(
        (await store.selectCandidates(sessionId, [oldTask.id], '1')).map((task) => task.taskId),
      ).toEqual([oldTask.id]);
      expect(
        (await store.applyCapacitySelection(sessionId, [oldTask.id], '1', administrator.id)).kind,
      ).toBe('APPLIED');
      expect((await runtimeTerminalState(currentTask.id, current.runId)).activeReservations).toBe(
        '2',
      );
      await expect(runtimePortReleaseStates(current.runId)).resolves.toEqual([false, false]);
      await store.markManualRequired(currentTask.id, 'TEST', sessionId);
      expect(
        (
          await requirePool().query(
            'SELECT recovery_disposition FROM ape_dts_platform.tasks WHERE id=$1::uuid',
            [currentTask.id],
          )
        ).rows[0].recovery_disposition,
      ).toBe('AUTO_ELIGIBLE');
      await store.markManualRequired(oldTask.id, 'RECOVERY_OLD_RUN_BINDING_INVALID', sessionId);
      expect((await store.listCandidates(sessionId)).length).toBe(0);
    } finally {
      await requirePool().query(
        'UPDATE ape_dts_platform.platform_instances SET ended_at=statement_timestamp(),clean_shutdown=true WHERE id=$1::uuid',
        [instanceId],
      );
      await requirePool().query(
        "UPDATE ape_dts_platform.capacity_reservations SET state='RELEASED',released_at=statement_timestamp() WHERE task_id=ANY($1::uuid[]) AND state IN ('ACTIVE','RESERVED')",
        [[oldTask.id, currentTask.id]],
      );
      await requirePool().query(
        'UPDATE ape_dts_platform.task_run_units SET metrics_port_released_at=statement_timestamp() WHERE task_id=ANY($1::uuid[])',
        [[oldTask.id, currentTask.id]],
      );
    }
  });

  it.each(['STARTING', 'RUNNING', 'STOPPING', 'BLOCKED'])(
    'never discards a %s run belonging to the current instance',
    async (status) => {
      const task = await createTask();
      const persisted = await createPersistedActiveRun(task);
      try {
        await requirePool().query(
          `UPDATE ape_dts_platform.task_runs SET platform_instance_id=$2::uuid, status=$3,
                ended_at=CASE WHEN $3='BLOCKED' THEN statement_timestamp() ELSE NULL END,
                failure_code=CASE WHEN $3='BLOCKED' THEN 'TEST_BLOCKED' ELSE NULL END,
                failure_summary=CASE WHEN $3='BLOCKED' THEN 'test' ELSE NULL END WHERE id=$1::uuid`,
          [persisted.runId, runtimePlatformInstanceId, status],
        );
        await requirePool().query(
          "UPDATE ape_dts_platform.tasks SET observed_state='BLOCKED' WHERE id=$1::uuid",
          [task.id],
        );
        const row = (
          await requirePool().query(
            'SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1::uuid',
            [task.id],
          )
        ).rows[0];
        const supervisor = supervisorWithProcesses([]);
        await supervisor.reconcilePersistedRuns();
        expect((await runtimeTerminalState(task.id, persisted.runId)).runStatus).toBe(status);
        await expect(
          supervisor.discardRecovery(administrator, task.id, row.row_version),
        ).rejects.toMatchObject({ code: 'RUNTIME_ALREADY_RUNNING' });
        expect((await runtimeTerminalState(task.id, persisted.runId)).activeReservations).toBe('2');
        await expect(runtimePortReleaseStates(persisted.runId)).resolves.toEqual([false, false]);
      } finally {
        await requirePool().query(
          "UPDATE ape_dts_platform.capacity_reservations SET state='RELEASED',released_at=statement_timestamp() WHERE task_id=$1::uuid AND state IN ('RESERVED','ACTIVE')",
          [task.id],
        );
        await requirePool().query(
          'UPDATE ape_dts_platform.task_run_units SET metrics_port_released_at=statement_timestamp() WHERE run_id=$1::uuid',
          [persisted.runId],
        );
      }
    },
  );

  it('rejects container recovery starts after shutdown has begun', async () => {
    const supervisor = supervisorWithProcesses([]);
    await supervisor.onApplicationShutdown('SIGTERM');
    await expect(
      supervisor.recoverPersistedTask({
        expectedTaskRowVersion: '1',
        oldRunId: randomUUID(),
        platformInstanceId: runtimePlatformInstanceId,
        revisionId: randomUUID(),
        taskId: randomUUID(),
        unitKeys: ['pg'],
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_OPERATION_IN_PROGRESS' });
  });

  it('reconciles a persisted active run as BLOCKED while retaining capacity', async () => {
    const task = await createTask();
    const persisted = await createPersistedActiveRun(task);
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({ logSink: new RecordingSink(), redact: (value) => value }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );

    await expect(supervisor.reconcilePersistedRuns()).resolves.toBe(1);
    await expect(supervisor.reconcilePersistedRuns()).resolves.toBe(0);
    const state = await requirePool().query<{
      desired_state: string;
      observed_state: string;
      operation_state: string;
      run_status: string;
      run_failure_code: string;
      stage_status: string;
      active_reservations: string;
      unit_statuses: string[];
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT state FROM ape_dts_platform.task_operations WHERE id = $2::uuid) AS operation_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_status,
              (SELECT failure_code FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_failure_code,
              (SELECT status FROM ape_dts_platform.task_run_stages WHERE run_id = $3::uuid) AS stage_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $3::uuid AND state IN ('RESERVED', 'ACTIVE')) AS active_reservations,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, persisted.operationId, persisted.runId],
    );
    expect(state.rows[0]).toMatchObject({
      active_reservations: '2',
      desired_state: 'RUNNING',
      observed_state: 'BLOCKED',
      operation_state: 'BLOCKED_MANUAL',
      run_failure_code: 'RUNTIME_CONTAINER_RESTART_REQUIRES_RECONCILIATION',
      run_status: 'BLOCKED',
      stage_status: 'PAUSED',
      unit_statuses: ['BLOCKED', 'BLOCKED'],
    });
    const audit = await requirePool().query<{ action: string }>(
      `SELECT action
         FROM ape_dts_platform.audit_events
        WHERE subject_id = $1::uuid
        ORDER BY sequence DESC
        LIMIT 1`,
      [task.id],
    );
    expect(audit.rows[0]?.action).toBe('TASK_RUNTIME_RECONCILIATION_BLOCKED');
    // The production state deliberately retains this reservation until an
    // operator proves the external process is gone.  Release only the
    // fixture's reservation after the retention assertion so later tests can
    // use the 4-unit envelope.
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      persisted.runId,
    );
  });

  it('waits for an admitted START before shutdown snapshots and stops its processes', async () => {
    const task = await createTask();
    const children = [
      new FakeChildProcess(33_011, 'SIGINT'),
      new FakeChildProcess(33_012, 'SIGINT'),
    ];
    let releasePrepare: (() => void) | undefined;
    const prepareWait = new Promise<void>((resolve) => {
      releasePrepare = resolve;
    });
    let preparing = false;
    const supervisor = supervisorWithProcesses(
      [...children],
      undefined,
      undefined,
      testRuntimePreflight(),
      async () => {
        preparing = true;
        await prepareWait;
      },
    );

    const starting = supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `shutdown-admitted-start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    const prepareDeadline = Date.now() + 2_000;
    while (!preparing && Date.now() < prepareDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    expect(preparing).toBe(true);

    const shutdown = supervisor.onApplicationShutdown('SIGTERM');
    let shutdownCompleted = false;
    void shutdown.then(() => {
      shutdownCompleted = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(shutdownCompleted).toBe(false);
    releasePrepare?.();

    const [started] = await Promise.all([starting, shutdown]);
    if (started.runId === null) {
      throw new Error('Admitted START did not retain its run id');
    }
    expect(children.map((child) => child.signals)).toEqual([['SIGINT'], ['SIGINT']]);
    await expect(runtimeTerminalState(task.id, started.runId)).resolves.toMatchObject({
      activeReservations: '2',
      observedState: 'BLOCKED',
      runStatus: 'BLOCKED',
      unitStatuses: ['BLOCKED', 'BLOCKED'],
    });
    await expect(runtimePortReleaseStates(started.runId)).resolves.toEqual([false, false]);
  });

  it('retains running intent, units and capacity for restart after confirmed application shutdown', async () => {
    const task = await createTask();
    const shutdownInstanceId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.platform_instances
      (id,platform_version,started_at,last_heartbeat_at)
      VALUES ($1,'0.0.0-test',now(),now())`,
      [shutdownInstanceId],
    );
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-shutdown-'));
    temporaryDirectories.push(root);
    const children = [
      new FakeChildProcess(33_001, 'SIGINT'),
      new FakeChildProcess(33_002, 'SIGINT'),
    ];
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            const child = children.shift();
            if (child === undefined) {
              throw new Error('shutdown child fixture exhausted');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      testLaunchFactory(),
      { getStatus: () => ({ phase: 'READY', platformInstanceId: shutdownInstanceId }) } as never,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `shutdown-confirmed-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    if (started.runId === null) {
      throw new Error('Confirmed shutdown fixture did not retain its run id');
    }
    await supervisor.beforeApplicationShutdown('SIGTERM');

    const state = await requirePool().query<{
      desired_state: string;
      observed_state: string;
      operation_state: string;
      run_status: string;
      released_reservations: string;
      unit_statuses: string[];
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT state FROM ape_dts_platform.task_operations WHERE id = $2::uuid) AS operation_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $3::uuid AND state = 'RELEASED') AS released_reservations,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, started.id, started.runId],
    );
    expect(state.rows[0]).toMatchObject({
      desired_state: 'RUNNING',
      observed_state: 'BLOCKED',
      operation_state: 'SUCCEEDED',
      released_reservations: '0',
      run_status: 'BLOCKED',
      unit_statuses: ['BLOCKED', 'BLOCKED'],
    });
    await expect(runtimePortReleaseStates(started.runId)).resolves.toEqual([false, false]);
    expect(
      (
        await requirePool().query(
          `SELECT clean_shutdown FROM ape_dts_platform.platform_instances WHERE id=$1::uuid`,
          [shutdownInstanceId],
        )
      ).rows[0]?.clean_shutdown,
    ).toBe(true);
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      started.runId,
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED', recovery_disposition='NOT_APPLICABLE' WHERE id=$1::uuid`,
      [task.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_run_units SET metrics_port_released_at=now() WHERE run_id=$1::uuid`,
      [started.runId],
    );
  });

  it('marks shutdown BLOCKED and retains capacity when a process never confirms close', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-shutdown-blocked-'));
    temporaryDirectories.push(root);
    const blockedChildren = [
      new FakeChildProcess(34_001, null),
      new FakeChildProcess(34_002, null),
    ];
    const children = [...blockedChildren];
    const settleRun = vi.fn(async () => ({
      recoveryState: 'NONE' as const,
      storageState: 'HEALTHY' as const,
    }));
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            const child = children.shift();
            if (child === undefined) {
              throw new Error('blocked shutdown child fixture exhausted');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: {
          append: () => undefined,
          close: () => undefined,
          settleRun,
        },
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `shutdown-blocked-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    if (started.runId === null) {
      throw new Error('Blocked shutdown fixture did not retain its run id');
    }
    await supervisor.onApplicationShutdown('SIGTERM');

    const state = await requirePool().query<{
      desired_state: string;
      observed_state: string;
      operation_state: string;
      run_status: string;
      active_reservations: string;
      unit_statuses: string[];
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT state FROM ape_dts_platform.task_operations WHERE id = $2::uuid) AS operation_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $3::uuid AND state IN ('RESERVED', 'ACTIVE')) AS active_reservations,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, started.id, started.runId],
    );
    expect(state.rows[0]).toMatchObject({
      active_reservations: '2',
      desired_state: 'RUNNING',
      observed_state: 'BLOCKED',
      // The START operation is already terminal once the run is RUNNING;
      // shutdown only transitions an operation when one is still executing.
      operation_state: 'SUCCEEDED',
      run_status: 'BLOCKED',
      unit_statuses: ['BLOCKED', 'BLOCKED'],
    });
    expect(settleRun).toHaveBeenCalledWith(started.runId);
    await expect(runtimePortReleaseStates(started.runId)).resolves.toEqual([false, false]);
    for (const child of blockedChildren) {
      child.close(0, null);
    }
    // Test-only cleanup after asserting that unconfirmed shutdown retained the
    // reservation; production requires an explicit operator reconciliation.
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      started.runId,
    );
  });

  it('blocks a stop with missing process handles and retains capacity', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-stop-missing-handle-'));
    temporaryDirectories.push(root);
    const childProcesses = [new FakeChildProcess(34_101, null), new FakeChildProcess(34_102, null)];
    const children = [...childProcesses];
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            const child = children.shift();
            if (child === undefined) {
              throw new Error('missing-handle child fixture exhausted');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `stop-missing-handle-start-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(started.state).toBe('SUCCEEDED');
    const running = await requirePool().query<{ row_version: string }>(
      `SELECT row_version::text
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid
          AND observed_state = 'RUNNING'`,
      [task.id],
    );
    const expectedTaskRowVersion = running.rows[0]?.row_version;
    if (expectedTaskRowVersion === undefined) {
      throw new Error('Unit 8 running task row version was not found');
    }

    // Simulate a container/runtime boundary that lost the in-memory handle.
    const handles = (supervisor as unknown as { activeProcesses: Map<string, unknown> })
      .activeProcesses;
    handles.clear();

    const stopped = await supervisor.stop(administrator, {
      expectedTaskRowVersion,
      idempotencyKey: `stop-missing-handle-${task.id}`,
      taskId: task.id,
    });
    expect(stopped.state).toBe('BLOCKED_MANUAL');
    const state = await requirePool().query<{
      desired_state: string;
      observed_state: string;
      operation_state: string;
      run_status: string;
      active_reservations: string;
      unit_statuses: string[];
      unit_endings: boolean[];
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT state FROM ape_dts_platform.task_operations WHERE id = $2::uuid) AS operation_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $3::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $3::uuid AND state IN ('ACTIVE', 'RESERVED')) AS active_reservations,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_statuses,
              (SELECT array_agg(ended_at IS NOT NULL ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = $3::uuid) AS unit_endings
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, stopped.id, started.runId],
    );
    expect(state.rows[0]).toEqual({
      desired_state: 'STOPPED',
      observed_state: 'BLOCKED',
      operation_state: 'BLOCKED_MANUAL',
      run_status: 'BLOCKED',
      active_reservations: '2',
      unit_statuses: ['BLOCKED', 'BLOCKED'],
      unit_endings: [true, true],
    });

    for (const child of childProcesses) {
      child.close(0, null);
    }
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      started.runId,
    );
  });

  it('stops already started units and records a failed run when the barrier is partial', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);
    let spawnCount = 0;
    const children: FakeChildProcess[] = [];
    const factory: RuntimeProcessFactory = {
      spawn: () => {
        spawnCount += 1;
        if (spawnCount === 2) {
          throw new Error('controlled spawn failure');
        }
        const child = new FakeChildProcess(30_000 + spawnCount, 'SIGINT');
        children.push(child);
        return child as unknown as RuntimeChildProcess;
      },
    };
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory,
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const result = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `partial-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(result.state).toBe('FAILED_RETRYABLE');
    const failed = await requirePool().query<{
      observed_state: string;
      run_status: string;
      reservation_count: string;
      statuses: string[];
    }>(
      `SELECT tasks.observed_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE operation_id = $2::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE task_id = $1::uuid AND state IN ('ACTIVE', 'RESERVED')) AS reservation_count,
              (SELECT array_agg(status ORDER BY unit_key) FROM ape_dts_platform.task_run_units WHERE run_id = (SELECT run_id FROM ape_dts_platform.task_operations WHERE id = $2::uuid)) AS statuses
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, result.id],
    );
    expect(failed.rows[0]).toMatchObject({
      observed_state: 'FAILED',
      run_status: 'FAILED',
      reservation_count: '0',
      statuses: ['FAILED', 'FAILED'],
    });
    const failedStages = await requirePool().query<{
      failure_code: string | null;
      failure_summary: string | null;
      stage_type: string;
      status: string;
    }>(
      `SELECT stage_type, status, failure_code, failure_summary
         FROM ape_dts_platform.task_run_stages
        WHERE run_id = $1::uuid
        ORDER BY stage_order`,
      [result.runId],
    );
    expect(failedStages.rows.map(({ stage_type, status }) => [stage_type, status])).toEqual([
      ['PREFLIGHT', 'SUCCEEDED'],
      ['ENSURE_ARTIFACTS', 'SUCCEEDED'],
      ['PREPARE_RESOURCES', 'SUCCEEDED'],
      ['GENERATE_INI', 'SUCCEEDED'],
      ['STARTING_BARRIER', 'FAILED'],
      ['CDC_STARTED', 'PENDING'],
      ['MONITORING_READY', 'PENDING'],
    ]);
    expect(failedStages.rows[4]).toMatchObject({
      failure_code: 'RUNTIME_PROCESS_START_FAILED',
      failure_summary: 'controlled spawn failure',
    });
    if (result.runId === null) {
      throw new Error('Partial start failure did not retain its run id');
    }
    await expect(runtimePortReleaseStates(result.runId)).resolves.toEqual([true, true]);
    expect(children[0]?.signals).toContain('SIGINT');
    await expect(readFile(path.join(root, 'partition-0.ini'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(readFile(path.join(root, 'partition-0.yaml'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(readFile(path.join(root, 'partition-1.ini'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(readFile(path.join(root, 'partition-1.yaml'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('preserves the authoritative log recovery state when a partial start leaves a process unconfirmed', async () => {
    const task = await createTask();
    const child = new FakeChildProcess(30_101, null);
    let spawnCount = 0;
    const settleRun = vi.fn(async () => ({
      recoveryState: 'DURABLE_PENDING' as const,
      statePersisted: false,
      storageState: 'DEGRADED' as const,
    }));
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => {
            spawnCount += 1;
            if (spawnCount === 2) {
              throw new Error('controlled partial start failure');
            }
            queueMicrotask(() => child.emit('spawn'));
            return child as unknown as RuntimeChildProcess;
          },
        },
        logSink: {
          append: () => undefined,
          close: () => ({ recoveryState: 'MEMORY_PENDING', storageState: 'DEGRADED' }),
          settleRun,
        },
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );

    const result = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `partial-blocked-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    if (result.runId === null) {
      throw new Error('Partial blocked start did not retain its run id');
    }

    expect(result.state).toBe('BLOCKED_MANUAL');
    expect(settleRun).toHaveBeenCalledWith(result.runId);
    await expect(
      requirePool().query(
        `SELECT runs.status,
                runs.log_storage_state,
                runs.log_recovery_state,
                (SELECT count(*)::integer
                   FROM ape_dts_platform.capacity_reservations AS reservations
                  WHERE reservations.run_id = runs.id
                    AND reservations.state IN ('RESERVED', 'ACTIVE')) AS active_reservations
           FROM ape_dts_platform.task_runs AS runs
          WHERE runs.id = $1::uuid`,
        [result.runId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          active_reservations: 2,
          log_recovery_state: 'DURABLE_PENDING',
          log_storage_state: 'DEGRADED',
          status: 'BLOCKED',
        },
      ],
    });

    child.close(0, 'SIGKILL');
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      result.runId,
    );
  });

  it('rejects a start when a referenced data source changed after preflight', async () => {
    const task = await createTask();
    const source = await requirePool().query<{ source_id: string }>(
      `SELECT source_data_source_id AS source_id
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid`,
      [task.id],
    );
    const sourceId = source.rows[0]?.source_id;
    if (sourceId === undefined) {
      throw new Error('Unit 8 source data source was not found');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.data_sources
          SET row_version = row_version + 1,
              updated_at = statement_timestamp()
        WHERE id = $1::uuid
          AND state = 'ACTIVE'
          AND deleted_at IS NULL`,
      [sourceId],
    );
    const before = await runtimeRowCounts(task.id);
    const childCount = spawnedChildren.length;
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);

    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `source-version-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT' });
    await expectStartRejection(task.id, before, 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT');
    const replay = await requireRuntime().start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `source-version-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(replay).toMatchObject({
      failureCode: 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT',
      runId: null,
      state: 'REJECTED',
    });
    const afterReplay = await runtimeRowCounts(task.id);
    expect(afterReplay.operations).toBe(String(Number(before.operations) + 1));
    expect(spawnedChildren.length).toBe(childCount);
  });

  it('does not record a rejection for a task the actor cannot access or a missing task', async () => {
    const task = await createTask();
    const before = await runtimeRowCounts(task.id);
    const ungrantedActor: UserSummary = {
      ...administrator,
      id: randomUUID(),
      role: 'DEVELOPER',
    };
    await expect(
      requireRuntime().start(ungrantedActor, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `ungranted-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_ACCESS_DENIED' });
    await expect(runtimeRowCounts(task.id)).resolves.toEqual(before);
    await expect(
      new RuntimeReadService(platformDatabase as PlatformDatabaseService).get(
        ungrantedActor,
        task.id,
      ),
    ).rejects.toBeInstanceOf(RuntimeReadNotFoundError);

    const missingTaskId = randomUUID();
    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `missing-${missingTaskId}`,
        revisionId: task.revisionId,
        taskId: missingTaskId,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_TASK_NOT_FOUND' });
    const missingOperations = await requirePool().query(
      'SELECT id FROM ape_dts_platform.task_operations WHERE task_id = $1::uuid',
      [missingTaskId],
    );
    expect(missingOperations.rowCount).toBe(0);
  });

  it('shows a newer start rejection while retaining an older stopped run', async () => {
    const task = await createTask();
    const oldOperationId = randomUUID();
    const oldRunId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_operations
         (id, task_id, action, idempotency_key, target_revision_id, state,
          initiated_by_user_id, reason, started_at, completed_at)
       VALUES ($1::uuid, $2::uuid, 'START', $3, $4::uuid, 'SUCCEEDED',
               $5::uuid, 'historical start', statement_timestamp(), statement_timestamp())`,
      [oldOperationId, task.id, `historical-${oldOperationId}`, task.revisionId, administrator.id],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.task_runs
         (id, task_id, task_revision_id, operation_id, status, current_stage, ended_at)
       VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'STOPPED', 'MONITORING_READY',
               statement_timestamp())`,
      [oldRunId, task.id, task.revisionId, oldOperationId],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_operations SET run_id = $2::uuid
        WHERE id = $1::uuid AND task_id = $3::uuid`,
      [oldOperationId, oldRunId, task.id],
    );
    const source = await requirePool().query<{ source_id: string }>(
      `SELECT source_data_source_id AS source_id FROM ape_dts_platform.tasks WHERE id = $1::uuid`,
      [task.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.data_sources SET row_version = row_version + 1
        WHERE id = $1::uuid AND state = 'ACTIVE' AND deleted_at IS NULL`,
      [source.rows[0]!.source_id],
    );
    const before = await runtimeRowCounts(task.id);
    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `after-old-run-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT' });
    const after = await runtimeRowCounts(task.id);
    expect(after).toEqual({
      operations: String(Number(before.operations) + 1),
      reservations: before.reservations,
      runs: before.runs,
    });
    const read = await new RuntimeReadService(platformDatabase as PlatformDatabaseService).get(
      administrator,
      task.id,
    );
    expect(read.run?.id).toBe(oldRunId);
    expect(read.operation).toMatchObject({
      failureCode: 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT',
      runId: null,
      state: 'REJECTED',
    });
    expect(read.retryable).toBe(false);
  });

  it('leaves the idempotency key unused when Topic preparation requires a new preflight', async () => {
    const task = await createTask();
    const before = await runtimeRowCounts(task.id);
    const prepareFirstStartTopic = vi.fn(async () => true);
    const supervisor = supervisorWithProcesses(
      [],
      undefined,
      undefined,
      testRuntimePreflight({ prepareFirstStartTopic }),
    );
    await expect(
      supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `topic-recheck-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED' });
    expect(prepareFirstStartTopic).toHaveBeenCalledOnce();
    await expect(runtimeRowCounts(task.id)).resolves.toEqual(before);
    const read = await new RuntimeReadService(platformDatabase as PlatformDatabaseService).get(
      administrator,
      task.id,
    );
    expect(read.run).toBeNull();
    expect(read.operation).toBeNull();
  });

  it('rejects a quarantined artifact even when the revision still has a risk acceptance', async () => {
    const task = await createTask();
    const artifact = await requirePool().query<{ artifact_id: string }>(
      `SELECT artifact_id
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid`,
      [task.revisionId],
    );
    const artifactId = artifact.rows[0]?.artifact_id;
    if (artifactId === undefined) {
      throw new Error('Unit 8 artifact was not bound');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.artifacts
          SET state = 'QUARANTINED',
              state_reason = 'test',
              activated_at = NULL
        WHERE id = $1::uuid
          AND state = 'ACTIVE'`,
      [artifactId],
    );
    const before = await runtimeRowCounts(task.id);
    const childCount = spawnedChildren.length;
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);

    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `inactive-artifact-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_ARTIFACT_NOT_ACTIVE' });
    await expectStartRejection(task.id, before, 'RUNTIME_ARTIFACT_NOT_ACTIVE');
    expect(spawnedChildren.length).toBe(childCount);
  });

  it('rejects a structurally invalid or stale preflight result without creating a run', async () => {
    const task = await createTask();
    await requirePool().query(
      `UPDATE ape_dts_platform.preflight_runs
          SET result = '{}'::jsonb
        WHERE task_revision_id = $1::uuid
          AND state = 'PASSED'`,
      [task.revisionId],
    );
    const before = await runtimeRowCounts(task.id);
    const childCount = spawnedChildren.length;
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);
    const supervisor = supervisorWithProcesses(
      [],
      undefined,
      undefined,
      testRuntimePreflightWithoutContinuityFacts(),
    );

    await expect(
      supervisor.start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `invalid-preflight-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_PREFLIGHT_STALE' });
    await expectStartRejection(task.id, before, 'RUNTIME_PREFLIGHT_STALE');
    expect(spawnedChildren.length).toBe(childCount);
  });

  it('rejects a Kafka start when the preflight partition baseline is invalid', async () => {
    const task = await createTask();
    await requirePool().query(
      `UPDATE ape_dts_platform.preflight_runs
          SET result = jsonb_set(
            result,
            '{checks,6,details,topics,0,partitionIds}',
            '[1,1]'::jsonb
          )
        WHERE task_revision_id = $1::uuid
          AND state = 'PASSED'`,
      [task.revisionId],
    );
    const before = await runtimeRowCounts(task.id);
    const childCount = spawnedChildren.length;
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);

    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `partition-baseline-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH' });
    await expectStartRejection(task.id, before, 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
    expect(spawnedChildren.length).toBe(childCount);
  });

  it('rejects a start when the declared artifact hash differs from the verified object hash', async () => {
    const task = await createTask();
    const artifact = await requirePool().query<{ artifact_id: string }>(
      `SELECT artifact_id
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid`,
      [task.revisionId],
    );
    const artifactId = artifact.rows[0]?.artifact_id;
    if (artifactId === undefined) {
      throw new Error('Unit 8 artifact was not bound');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.artifacts
          SET declared_sha256 = decode(repeat('cc', 32), 'hex')
        WHERE id = $1::uuid`,
      [artifactId],
    );
    const before = await runtimeRowCounts(task.id);
    const childCount = spawnedChildren.length;
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);

    await expect(
      requireRuntime().start(administrator, {
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: `declared-hash-${task.id}`,
        revisionId: task.revisionId,
        taskId: task.id,
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME_ARTIFACT_NOT_ACTIVE' });
    await expectStartRejection(task.id, before, 'RUNTIME_ARTIFACT_NOT_ACTIVE');
    expect(spawnedChildren.length).toBe(childCount);
  });

  it('reconciles an unexpected child exit as FAILED and retains capacity for fixed recovery', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess(31_001, null);
    const survivingChild = new FakeChildProcess(31_002, 'SIGINT');
    const children = [child, survivingChild];
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => children.shift() as unknown as RuntimeChildProcess,
        },
        logSink: new RecordingSink(),
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `unexpected-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(started.state).toBe('SUCCEEDED');
    child.close(42, null);
    let failed = await requirePool().query<{
      desired_state: string;
      observed_state: string;
      run_status: string;
      reservation_count: string;
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state IN ('ACTIVE', 'RESERVED')) AS reservation_count
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, started.runId],
    );
    const deadline = Date.now() + 5_000;
    while (
      Date.now() < deadline &&
      (failed.rows[0]?.observed_state !== 'FAILED' || failed.rows[0]?.run_status !== 'FAILED')
    ) {
      await new Promise((resolve) => setTimeout(resolve, 25));
      failed = await requirePool().query<{
        desired_state: string;
        observed_state: string;
        run_status: string;
        reservation_count: string;
      }>(
        `SELECT tasks.desired_state,
                tasks.observed_state,
                (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
                (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state IN ('ACTIVE', 'RESERVED')) AS reservation_count
           FROM ape_dts_platform.tasks
          WHERE tasks.id = $1::uuid`,
        [task.id, started.runId],
      );
    }
    expect(failed.rows[0]).toEqual({
      desired_state: 'RUNNING',
      observed_state: 'FAILED',
      run_status: 'FAILED',
      reservation_count: '2',
    });
    expect(survivingChild.signals).toContain('SIGINT');
    const units = await requirePool().query<{
      failure_code: string | null;
      status: string;
      unit_key: string;
    }>(
      `SELECT unit_key, status, failure_code
         FROM ape_dts_platform.task_run_units
        WHERE run_id = $1::uuid
        ORDER BY unit_key`,
      [started.runId],
    );
    expect(units.rows).toHaveLength(2);
    expect(units.rows.map((item) => item.status).sort()).toEqual(['FAILED', 'STOPPED']);
    expect(
      units.rows.filter((item) => item.failure_code === 'RUNTIME_UNEXPECTED_EXIT'),
    ).toHaveLength(1);
    expect(units.rows.filter((item) => item.failure_code === null)).toHaveLength(1);
  });

  it('automatically preflights and starts a new SYSTEM run after the controlled 30-second delay', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const first = new FakeChildProcess(32_001, null);
    const survivor = new FakeChildProcess(32_002, 'SIGINT');
    const recoveredFirst = new FakeChildProcess(32_003, null);
    const recoveredSecond = new FakeChildProcess(32_004, null);
    const harness = autoRecoverySupervisor([first, survivor, recoveredFirst, recoveredSecond]);
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-happy-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    first.close(42, null);
    await waitForTaskState(task.id, 'FAILED');
    await harness.advanceRetry();
    expect(harness.preflight).toHaveBeenCalledTimes(1);
    await waitForTaskState(task.id, 'RUNNING');

    const facts = await requirePool().query<{
      active_reservations: string;
      actor_type: string;
      new_run_id: string;
      old_run_status: string;
      task_state: string;
    }>(
      `SELECT task.observed_state AS task_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id=$2::uuid) AS old_run_status,
              (SELECT id::text FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid
                ORDER BY started_at DESC,id DESC LIMIT 1) AS new_run_id,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations
                WHERE task_id=$1::uuid AND state IN ('ACTIVE','RESERVED')) AS active_reservations,
              (SELECT actor_type FROM ape_dts_platform.audit_events
                WHERE subject_id=$1::uuid AND action='TASK_RUNTIME_AUTO_RECOVERY_START_ACCEPTED'
                ORDER BY occurred_at DESC LIMIT 1) AS actor_type
         FROM ape_dts_platform.tasks AS task WHERE task.id=$1::uuid`,
      [task.id, started.runId],
    );
    expect(facts.rows[0]).toMatchObject({
      active_reservations: '2',
      actor_type: 'SYSTEM',
      old_run_status: 'FAILED',
      task_state: 'RUNNING',
    });
    expect(facts.rows[0]?.new_run_id).not.toBe(started.runId);
    const recoveredStages = await requirePool().query<{ stage_type: string; status: string }>(
      `SELECT stage_type, status FROM ape_dts_platform.task_run_stages
        WHERE run_id = $1::uuid ORDER BY stage_order`,
      [facts.rows[0]?.new_run_id],
    );
    expect(recoveredStages.rows.map(({ stage_type, status }) => [stage_type, status])).toEqual([
      ['PREFLIGHT', 'SUCCEEDED'],
      ['ENSURE_ARTIFACTS', 'SUCCEEDED'],
      ['PREPARE_RESOURCES', 'SUCCEEDED'],
      ['GENERATE_INI', 'SUCCEEDED'],
      ['STARTING_BARRIER', 'SUCCEEDED'],
      ['CDC_STARTED', 'SUCCEEDED'],
      ['MONITORING_READY', 'SUCCEEDED'],
    ]);
  });

  it('moves a failed SYSTEM preflight to the manual gate and releases retained capacity', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const failed = new FakeChildProcess(32_101, null);
    const survivor = new FakeChildProcess(32_102, 'SIGINT');
    const harness = autoRecoverySupervisor([failed, survivor], 'FAILED');
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-preflight-fail-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    failed.close(43, null);
    await waitForTaskState(task.id, 'FAILED');
    await harness.advanceRetry();
    const state = await recoveryState(task.id, started.runId);
    expect(state).toEqual({ disposition: 'MANUAL_REQUIRED', reservations: '0', state: 'BLOCKED' });
  });

  it('allocates available metrics ports when another task uses the released ports during retry wait', async () => {
    await allowR32TestCapacity();
    const firstTask = await createTask();
    const failed = new FakeChildProcess(32_091, null);
    const first = autoRecoverySupervisor([
      failed,
      new FakeChildProcess(32_092, 'SIGINT'),
      new FakeChildProcess(32_093, 'SIGINT'),
      new FakeChildProcess(32_094, 'SIGINT'),
    ]);
    const initial = await first.supervisor.start(administrator, {
      taskId: firstTask.id,
      revisionId: firstTask.revisionId,
      expectedTaskRowVersion: firstTask.rowVersion,
      idempotencyKey: `ports-first-${firstTask.id}`,
    });
    failed.close(42, null);
    await waitForTaskState(firstTask.id, 'FAILED');
    const secondTask = await createTask();
    const second = autoRecoverySupervisor([
      new FakeChildProcess(32_095, 'SIGINT'),
      new FakeChildProcess(32_096, 'SIGINT'),
    ]);
    const secondRun = await second.supervisor.start(administrator, {
      taskId: secondTask.id,
      revisionId: secondTask.revisionId,
      expectedTaskRowVersion: secondTask.rowVersion,
      idempotencyKey: `ports-second-${secondTask.id}`,
    });
    const ports = async (runId: string) =>
      (
        await requirePool().query<{ metrics_port: number }>(
          'SELECT metrics_port FROM ape_dts_platform.task_run_units WHERE run_id=$1',
          [runId],
        )
      ).rows.map((item) => item.metrics_port);
    expect(await ports(secondRun.runId!)).toEqual(
      expect.arrayContaining(await ports(initial.runId!)),
    );
    await first.advanceRetry();
    await waitForTaskState(firstTask.id, 'RUNNING');
    const active = (
      await requirePool().query<{ metrics_port: number }>(
        'SELECT metrics_port FROM ape_dts_platform.task_run_units WHERE task_id=ANY($1::uuid[]) AND metrics_port_released_at IS NULL',
        [[firstTask.id, secondTask.id]],
      )
    ).rows;
    expect(active).toHaveLength(4);
    expect(new Set(active.map((item) => item.metrics_port)).size).toBe(4);
    for (const [taskId, supervisor] of [
      [firstTask.id, first.supervisor],
      [secondTask.id, second.supervisor],
    ] as const) {
      const row = (
        await requirePool().query(
          'SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1',
          [taskId],
        )
      ).rows[0];
      await supervisor.stop(administrator, {
        taskId,
        expectedTaskRowVersion: row.row_version,
        idempotencyKey: `ports-stop-${taskId}`,
      });
    }
  });

  it('manually retries the applied run on the same instance after automatic recovery was exhausted', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const failed = new FakeChildProcess(32_111, null);
    const survivor = new FakeChildProcess(32_112, 'SIGINT');
    const harness = autoRecoverySupervisor(
      [
        failed,
        survivor,
        new FakeChildProcess(32_113, 'SIGINT'),
        new FakeChildProcess(32_114, 'SIGINT'),
      ],
      'FAILED',
    );
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `manual-recovery-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    failed.close(43, null);
    await waitForTaskState(task.id, 'FAILED');
    await harness.advanceRetry();
    expect(await recoveryState(task.id, started.runId)).toEqual({
      disposition: 'MANUAL_REQUIRED',
      reservations: '0',
      state: 'BLOCKED',
    });
    const service = new TaskRecoveryService(
      platformDatabase!,
      {
        getStatus: () => ({ phase: 'READY', platformInstanceId: runtimePlatformInstanceId }),
      } as never,
      harness.supervisor,
      {} as never,
      new AuditLedgerService(platformDatabase!),
    );
    let row = (
      await requirePool().query(
        'SELECT row_version::text,current_applied_revision_id FROM ape_dts_platform.tasks WHERE id=$1',
        [task.id],
      )
    ).rows[0];
    await expect(service.retry(administrator, task.id, row.row_version)).rejects.toThrow(
      'RUNTIME_RECOVERY_PREFLIGHT_FAILED',
    );
    expect(await recoveryState(task.id, started.runId)).toEqual({
      disposition: 'MANUAL_REQUIRED',
      reservations: '0',
      state: 'BLOCKED',
    });
    const database = platformDatabase!;
    const audit = new AuditLedgerService(database);
    const realPreflight = new PreflightService(
      database,
      audit,
      new DataSourcesService(
        database,
        audit,
        new UnlockedKeyService(),
        new DataSourceAdapterRegistry([
          {
            type: 'KAFKA',
            test: async ({ configuration }) => {
              if (configuration.type !== 'KAFKA') throw new Error('Expected Kafka');
              return {
                kind: 'KAFKA_CLUSTER',
                serverVersion: null,
                value: configuration.bootstrapServers[0]?.endsWith(':9092')
                  ? `unit8-source-cluster-${task.sourceId}`
                  : `unit8-target-cluster-${task.targetId}`,
              };
            },
            metadata: async (_input, query) =>
              query.resource === 'PARTITIONS'
                ? [0, 1].map((id) => ({ name: String(id), identity: null, attributes: {} }))
                : [
                    {
                      name: 'unit8.source',
                      identity: `unit8-source-topic-${task.sourceId}`,
                      attributes: { partitionCount: '2' },
                    },
                    {
                      name: 'unit8.target',
                      identity: `unit8-target-topic-${task.targetId}`,
                      attributes: { partitionCount: '1' },
                    },
                  ],
          },
        ]),
      ),
      new CapacityReservationService(database),
      new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
      new ContinuityService(database, audit),
      '0.0.0',
      new RuntimeEnvironmentInspector({
        SCS_RUNTIME_METRICS_PORT_MIN: '45000',
        SCS_RUNTIME_METRICS_PORT_MAX: '55000',
      }),
    );
    harness.preflight.mockImplementation((request) => realPreflight.runForSystemRecovery(request));
    row = (
      await requirePool().query(
        'SELECT row_version::text,current_applied_revision_id FROM ape_dts_platform.tasks WHERE id=$1',
        [task.id],
      )
    ).rows[0];
    const result = await service.retry(administrator, task.id, row.row_version);
    expect(result).toMatchObject({ operation: { state: 'SUCCEEDED' } });
    await waitForTaskState(task.id, 'RUNNING');
    const runs = (
      await requirePool().query(
        'SELECT id,status,task_revision_id FROM ape_dts_platform.task_runs WHERE task_id=$1 ORDER BY started_at,id',
        [task.id],
      )
    ).rows;
    expect(runs).toHaveLength(2);
    expect(runs.find((run) => run.id === started.runId)).toMatchObject({
      status: 'FAILED',
      task_revision_id: row.current_applied_revision_id,
    });
    expect(runs.find((run) => run.id !== started.runId)).toMatchObject({
      status: 'RUNNING',
      task_revision_id: row.current_applied_revision_id,
    });
    await expect(service.retry(administrator, task.id, row.row_version)).rejects.toThrow(
      'RECOVERY_STEP_NOT_ACTIONABLE',
    );
    const current = (
      await requirePool().query(
        'SELECT row_version::text FROM ape_dts_platform.tasks WHERE id=$1',
        [task.id],
      )
    ).rows[0];
    await harness.supervisor.stop(administrator, {
      taskId: task.id,
      expectedTaskRowVersion: current.row_version,
      idempotencyKey: `manual-recovery-stop-${task.id}`,
    });
  });

  it('records a failed SYSTEM recovery at the real starting barrier', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const failed = new FakeChildProcess(32_301, null);
    const survivor = new FakeChildProcess(32_302, 'SIGINT');
    const onlyRecoveredUnit = new FakeChildProcess(32_303, 'SIGINT');
    const harness = autoRecoverySupervisor([failed, survivor, onlyRecoveredUnit]);
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-start-fail-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    failed.close(45, null);
    await waitForTaskState(task.id, 'FAILED');
    await harness.advanceRetry();
    const latest = await requirePool().query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.task_runs
        WHERE task_id = $1::uuid AND id <> $2::uuid
        ORDER BY started_at DESC, id DESC LIMIT 1`,
      [task.id, started.runId],
    );
    const stages = await requirePool().query<{
      failure_code: string | null;
      stage_type: string;
      status: string;
    }>(
      `SELECT stage_type, status, failure_code
         FROM ape_dts_platform.task_run_stages
        WHERE run_id = $1::uuid ORDER BY stage_order`,
      [latest.rows[0]?.id],
    );
    expect(stages.rows.map(({ stage_type, status }) => [stage_type, status])).toEqual([
      ['PREFLIGHT', 'SUCCEEDED'],
      ['ENSURE_ARTIFACTS', 'SUCCEEDED'],
      ['PREPARE_RESOURCES', 'SUCCEEDED'],
      ['GENERATE_INI', 'SUCCEEDED'],
      ['STARTING_BARRIER', 'FAILED'],
      ['CDC_STARTED', 'PENDING'],
      ['MONITORING_READY', 'PENDING'],
    ]);
    expect(stages.rows[4]?.failure_code).toBe('RUNTIME_PROCESS_START_FAILED');
  });

  it('does not overwrite a newer user stop intent when the old retry timer fires', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const failed = new FakeChildProcess(32_201, null);
    const survivor = new FakeChildProcess(32_202, 'SIGINT');
    const harness = autoRecoverySupervisor([failed, survivor]);
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-user-intent-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    failed.close(44, null);
    await waitForTaskState(task.id, 'FAILED');
    await requirePool().query(
      `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED', row_version=row_version+1
        WHERE id=$1::uuid AND desired_state='RUNNING' AND observed_state='FAILED'`,
      [task.id],
    );
    await harness.advanceRetry();
    const rows = await requirePool().query<{ desired_state: string; runs: string }>(
      `SELECT desired_state,
              (SELECT count(*)::text FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid) AS runs
         FROM ape_dts_platform.tasks WHERE id=$1::uuid`,
      [task.id],
    );
    expect(rows.rows[0]).toEqual({ desired_state: 'STOPPED', runs: '1' });
    expect(started.runId).toBeTruthy();
  });

  it('rolls back the new run and operation when reservation transfer is incomplete', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const failed = new FakeChildProcess(32_301, null);
    const survivor = new FakeChildProcess(32_302, 'SIGINT');
    const harness = autoRecoverySupervisor([failed, survivor]);
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-transfer-fail-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    failed.close(45, null);
    await waitForTaskState(task.id, 'FAILED');
    await requirePool().query(
      `UPDATE ape_dts_platform.capacity_reservations SET state='RELEASED', released_at=statement_timestamp()
        WHERE run_id=$1::uuid AND unit_key='partition:1' AND state='ACTIVE'`,
      [started.runId],
    );
    await harness.advanceRetry();
    const counts = await runtimeRowCounts(task.id);
    expect(counts.runs).toBe('1');
    expect(counts.operations).toBe('1');
    expect((await recoveryState(task.id, started.runId)).disposition).toBe('MANUAL_REQUIRED');
  });

  it('sends a second abnormal exit inside ten minutes directly to the manual gate', async () => {
    await allowR32TestCapacity();
    const task = await createTask();
    const first = new FakeChildProcess(32_401, null);
    const survivor = new FakeChildProcess(32_402, 'SIGINT');
    const second = new FakeChildProcess(32_403, null);
    const secondSurvivor = new FakeChildProcess(32_404, 'SIGINT');
    const harness = autoRecoverySupervisor([first, survivor, second, secondSurvivor]);
    const started = await harness.supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `auto-second-exit-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    first.close(46, null);
    await waitForTaskState(task.id, 'FAILED');
    await harness.advanceRetry();
    await waitForTaskState(task.id, 'RUNNING');
    second.close(47, null);
    await waitForTaskState(task.id, 'BLOCKED');
    const latest = await requirePool().query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid
        ORDER BY started_at DESC,id DESC LIMIT 1`,
      [task.id],
    );
    expect(await recoveryState(task.id, latest.rows[0]!.id)).toEqual({
      disposition: 'MANUAL_REQUIRED',
      reservations: '0',
      state: 'BLOCKED',
    });
    expect(started.runId).toBeTruthy();
  });

  it('marks the task BLOCKED and retains capacity when an unexpected exit leaves a survivor unconfirmed', async () => {
    const task = await createTask();
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-unexpected-blocked-'));
    temporaryDirectories.push(root);
    const failedChild = new FakeChildProcess(31_101, null);
    const survivor = new FakeChildProcess(31_102, null);
    const children = [failedChild, survivor];
    const settleRun = vi.fn(async (runId: string) => {
      await requirePool().query(
        `UPDATE ape_dts_platform.task_runs
            SET log_storage_state = 'HEALTHY',
                log_recovery_state = 'NONE'
          WHERE id = $1::uuid`,
        [runId],
      );
      return {
        recoveryState: 'NONE' as const,
        statePersisted: true,
        storageState: 'HEALTHY' as const,
      };
    });
    const supervisor = new RuntimeSupervisorService(
      platformDatabase as PlatformDatabaseService,
      new AuditLedgerService(platformDatabase as PlatformDatabaseService),
      new CapacityReservationService(platformDatabase as PlatformDatabaseService),
      new RuntimeProcessSupervisor({
        factory: {
          spawn: () => children.shift() as unknown as RuntimeChildProcess,
        },
        logSink: {
          append: () => undefined,
          close: () => ({ recoveryState: 'MEMORY_PENDING', storageState: 'DEGRADED' }),
          settleRun,
        },
        redact: (value) => value,
        stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
      }),
      testLaunchFactory(),
      undefined,
      testRuntimePreflight(),
      testMetricsPortAllocator(),
    );
    const started = await supervisor.start(administrator, {
      expectedTaskRowVersion: task.rowVersion,
      idempotencyKey: `unexpected-blocked-${task.id}`,
      revisionId: task.revisionId,
      taskId: task.id,
    });
    expect(started.state).toBe('SUCCEEDED');

    failedChild.close(42, null);
    let state = await requirePool().query<{
      desired_state: string;
      log_recovery_state: string;
      log_storage_state: string;
      observed_state: string;
      run_status: string;
      reservation_count: string;
    }>(
      `SELECT tasks.desired_state,
              tasks.observed_state,
              (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
              (SELECT log_storage_state FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS log_storage_state,
              (SELECT log_recovery_state FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS log_recovery_state,
              (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state IN ('ACTIVE', 'RESERVED')) AS reservation_count
         FROM ape_dts_platform.tasks
        WHERE tasks.id = $1::uuid`,
      [task.id, started.runId],
    );
    const deadline = Date.now() + 5_000;
    while (
      Date.now() < deadline &&
      (state.rows[0]?.observed_state !== 'BLOCKED' || state.rows[0]?.run_status !== 'BLOCKED')
    ) {
      await new Promise((resolve) => setTimeout(resolve, 25));
      state = await requirePool().query<{
        desired_state: string;
        log_recovery_state: string;
        log_storage_state: string;
        observed_state: string;
        run_status: string;
        reservation_count: string;
      }>(
        `SELECT tasks.desired_state,
                tasks.observed_state,
                (SELECT status FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS run_status,
                (SELECT log_storage_state FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS log_storage_state,
                (SELECT log_recovery_state FROM ape_dts_platform.task_runs WHERE id = $2::uuid) AS log_recovery_state,
                (SELECT count(*)::text FROM ape_dts_platform.capacity_reservations WHERE run_id = $2::uuid AND state IN ('ACTIVE', 'RESERVED')) AS reservation_count
           FROM ape_dts_platform.tasks
          WHERE tasks.id = $1::uuid`,
        [task.id, started.runId],
      );
    }
    expect(state.rows[0]).toEqual({
      desired_state: 'RUNNING',
      log_recovery_state: 'NONE',
      log_storage_state: 'HEALTHY',
      observed_state: 'BLOCKED',
      run_status: 'BLOCKED',
      reservation_count: '2',
    });
    expect(settleRun).toHaveBeenCalledWith(started.runId);
    expect(survivor.signals).toEqual(['SIGINT', 'SIGTERM', 'SIGKILL']);
    const units = await requirePool().query<{
      ended_at: Date | null;
      failure_code: string | null;
      status: string;
      unit_key: string;
    }>(
      `SELECT unit_key, status, failure_code, ended_at
         FROM ape_dts_platform.task_run_units
        WHERE run_id = $1::uuid
        ORDER BY unit_key`,
      [started.runId],
    );
    expect(units.rows).toHaveLength(2);
    expect(units.rows.map((item) => item.status).sort()).toEqual(['BLOCKED', 'FAILED']);
    expect(units.rows.every((item) => item.ended_at instanceof Date)).toBe(true);
    expect(units.rows.map((item) => item.failure_code).sort()).toEqual([
      'RUNTIME_PROCESS_HANDLE_MISSING',
      'RUNTIME_UNEXPECTED_EXIT',
    ]);

    survivor.close(0, 'SIGKILL');
    // The run is intentionally still BLOCKED after the handle eventually closes;
    // capacity release requires an explicit operator reconciliation.
    await new CapacityReservationService(platformDatabase as PlatformDatabaseService).release(
      started.runId,
    );
  });

  it('requires preflight and never starts a READY draft', async () => {
    const database = platformDatabase;
    if (database === undefined) {
      throw new Error('Unit 8 platform database is not initialized');
    }
    const tasks = new TasksService(database, new AuditLedgerService(database));
    const sourceId = randomUUID();
    const targetId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.data_sources (
         id, normalized_name, source_type, configuration, connection_secret_id,
         state, created_by_user_id, updated_by_user_id
       ) VALUES
         ($1::uuid, $2, 'KAFKA', $3::jsonb, NULL, 'ACTIVE', $5::uuid, $5::uuid),
         ($4::uuid, $6, 'KAFKA', $7::jsonb, NULL, 'ACTIVE', $5::uuid, $5::uuid)`,
      [
        sourceId,
        `unit8-ready-source-${sourceId}`,
        JSON.stringify({
          bootstrapServers: ['127.0.0.1:9092'],
          plaintextRiskAcknowledged: true,
          type: 'KAFKA',
        }),
        targetId,
        administrator.id,
        `unit8-ready-target-${targetId}`,
        JSON.stringify({
          bootstrapServers: ['127.0.0.1:9093'],
          plaintextRiskAcknowledged: true,
          type: 'KAFKA',
        }),
      ],
    );
    const created = await tasks.create(administrator, {
      configuration: {
        kind: 'KAFKA_RAW_FORWARD',
        schemaVersion: 1,
        source: { dataSourceId: sourceId, groupId: 'placeholder', topic: 'unit8.ready.source' },
        target: { dataSourceId: targetId, topic: 'unit8.ready.target' },
        tuning: {
          ackIntervalSeconds: 5,
          batchSize: 1,
          bufferSize: 1,
          checkpointIntervalSeconds: 1,
          requiredAcks: 'all',
        },
      },
      name: `Unit 8 ready ${randomUUID()}`,
      sourceDataSourceId: sourceId,
      targetDataSourceId: targetId,
      taskType: 'KAFKA_KAFKA',
    });
    const revisionId = created.draft?.id;
    if (revisionId === undefined) {
      throw new Error('Unit 8 ready task did not return a revision');
    }
    const root = await mkdtemp(path.join(tmpdir(), 'scs-u8-'));
    temporaryDirectories.push(root);
    const supervisor = supervisorWithProcesses(
      [],
      undefined,
      undefined,
      testRuntimePreflightWithoutContinuityFacts(),
    );
    await expect(
      supervisor.start(administrator, {
        expectedTaskRowVersion: created.rowVersion,
        idempotencyKey: `ready-${created.id}`,
        revisionId,
        taskId: created.id,
      }),
    ).rejects.toMatchObject(new RuntimeSupervisorError('RUNTIME_PREFLIGHT_REQUIRED'));
  });

  it('binds all unit logs in one run to one LOCAL profile and records SYSTEM ownership', async () => {
    const database = platformDatabase;
    if (database === undefined) {
      throw new Error('Unit 8 platform database is not initialized');
    }
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const units = await requirePool().query<{ id: string; unit_key: string }>(
      `SELECT id, unit_key
         FROM ape_dts_platform.task_run_units
        WHERE run_id = $1::uuid
        ORDER BY unit_key`,
      [runId],
    );
    const firstUnitId = units.rows[0]?.id;
    const secondUnitId = units.rows[1]?.id;
    if (firstUnitId === undefined || secondUnitId === undefined) {
      throw new Error('Unit 8 log storage fixture requires two runtime units');
    }

    const localRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-log-store-'));
    temporaryDirectories.push(localRoot);
    const firstProfileId = randomUUID();
    const secondProfileId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Unit 8 log profile A', 1, 'LOCAL',
                 '{"relative_prefix":"profile-a"}'::jsonb, 'ACTIVE_WRITE',
                 $3::uuid, statement_timestamp())`,
      [firstProfileId, `unit8-log-a-${runId}`, administrator.id],
    );

    const store = new PostgresLogSegmentStore(database, localRoot);
    const firstContent = Buffer.from('first unit log\n', 'utf8');
    const firstCreatedAt = new Date();
    const firstSegment: LogSegmentRecord = {
      byteSize: firstContent.length,
      createdAt: firstCreatedAt,
      deletedAt: null,
      deletedReason: null,
      endedAt: firstCreatedAt,
      id: randomUUID(),
      runId,
      runUnitId: firstUnitId,
      sequence: 1,
      sha256: createHash('sha256').update(firstContent).digest('hex'),
      startedAt: firstCreatedAt,
      state: 'SEALED',
      stream: 'STDOUT',
    };
    await store.persistSegment({ content: firstContent, segment: firstSegment });
    await store.persistSegment({ content: firstContent, segment: firstSegment });

    await requirePool().query(
      `UPDATE ape_dts_platform.storage_profiles
          SET state = 'READ_ONLY_REFERENCED'
        WHERE id = $1::uuid`,
      [firstProfileId],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Unit 8 log profile B', 1, 'LOCAL',
                 '{"relative_prefix":"profile-b"}'::jsonb, 'ACTIVE_WRITE',
                 $3::uuid, statement_timestamp())`,
      [secondProfileId, `unit8-log-b-${runId}`, administrator.id],
    );

    const secondContent = Buffer.from('second unit log\n', 'utf8');
    const secondCreatedAt = new Date();
    const secondSegment: LogSegmentRecord = {
      byteSize: secondContent.length,
      createdAt: secondCreatedAt,
      deletedAt: null,
      deletedReason: null,
      endedAt: secondCreatedAt,
      id: randomUUID(),
      runId,
      runUnitId: secondUnitId,
      sequence: 2,
      sha256: createHash('sha256').update(secondContent).digest('hex'),
      startedAt: secondCreatedAt,
      state: 'SEALED',
      stream: 'STDOUT',
    };
    await store.persistSegment({ content: secondContent, segment: secondSegment });

    const persisted = await requirePool().query<{
      created_by_actor_type: string;
      created_by_user_id: string | null;
      log_storage_profile_id: string;
      object_key: string;
      run_unit_id: string;
      segment_state: string;
      storage_profile_id: string;
      stored_object_state: string;
    }>(
      `SELECT runs.log_storage_profile_id,
              segments.run_unit_id,
              segments.state AS segment_state,
              objects.storage_profile_id,
              objects.object_key,
              objects.state AS stored_object_state,
              objects.created_by_actor_type,
              objects.created_by_user_id
         FROM ape_dts_platform.task_runs AS runs
         JOIN ape_dts_platform.log_segments AS segments
           ON segments.run_id = runs.id
         JOIN ape_dts_platform.stored_objects AS objects
           ON objects.id = segments.stored_object_id
        WHERE runs.id = $1::uuid
          AND segments.id = ANY($2::uuid[])
        ORDER BY segments.run_unit_id`,
      [runId, [firstSegment.id, secondSegment.id]],
    );
    expect(persisted.rows).toHaveLength(2);
    expect(persisted.rows.every((row) => row.log_storage_profile_id === firstProfileId)).toBe(true);
    expect(persisted.rows.every((row) => row.storage_profile_id === firstProfileId)).toBe(true);
    expect(persisted.rows.every((row) => row.created_by_actor_type === 'SYSTEM')).toBe(true);
    expect(persisted.rows.every((row) => row.created_by_user_id === null)).toBe(true);
    expect(persisted.rows.every((row) => row.segment_state === 'AVAILABLE')).toBe(true);
    expect(persisted.rows.every((row) => row.stored_object_state === 'AVAILABLE')).toBe(true);

    const mismatchedObjectId = randomUUID();
    const mismatchedSegmentId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.stored_objects (
         id, storage_profile_id, kind, object_key, size_bytes, sha256,
         content_type, state, created_by_actor_type, created_by_user_id
       ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 0,
                 decode(repeat('00', 32), 'hex'), 'text/plain', 'PENDING',
                 'SYSTEM', NULL)`,
      [mismatchedObjectId, secondProfileId, `mismatched/${mismatchedObjectId}`],
    );
    await expect(
      requirePool().query(
        `INSERT INTO ape_dts_platform.log_segments (
           id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
           stored_object_id, byte_size, sha256, state
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STDERR', 99,
                   statement_timestamp(), statement_timestamp(), $4::uuid, 0,
                   decode(repeat('00', 32), 'hex'), 'SEALED')`,
        [mismatchedSegmentId, runId, secondUnitId, mismatchedObjectId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    const secondStoredObject = await requirePool().query<{ stored_object_id: string }>(
      `SELECT stored_object_id
         FROM ape_dts_platform.log_segments
        WHERE id = $1::uuid`,
      [secondSegment.id],
    );
    const secondStoredObjectId = secondStoredObject.rows[0]?.stored_object_id;
    if (secondStoredObjectId === undefined) {
      throw new Error('Unit 8 second stored log object was not found');
    }
    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.stored_objects
            SET storage_profile_id = $2::uuid
          WHERE id = $1::uuid`,
        [secondStoredObjectId, secondProfileId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    const firstObjectKey = persisted.rows.find(
      (row) => row.run_unit_id === firstUnitId,
    )?.object_key;
    const secondObjectKey = persisted.rows.find(
      (row) => row.run_unit_id === secondUnitId,
    )?.object_key;
    if (firstObjectKey === undefined || secondObjectKey === undefined) {
      throw new Error('Unit 8 persisted log object keys were not found');
    }
    await expect(
      readFile(path.join(localRoot, 'profile-a', ...firstObjectKey.split('/')), 'utf8'),
    ).resolves.toBe(firstContent.toString('utf8'));
    await expect(
      readFile(path.join(localRoot, 'profile-a', ...secondObjectKey.split('/')), 'utf8'),
    ).resolves.toBe(secondContent.toString('utf8'));
    await expect(
      readFile(path.join(localRoot, 'profile-b', ...secondObjectKey.split('/')), 'utf8'),
    ).rejects.toMatchObject({ code: 'ENOENT' });

    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.task_runs
            SET log_storage_profile_id = $2::uuid
          WHERE id = $1::uuid`,
        [runId, secondProfileId],
      ),
    ).rejects.toMatchObject({ code: '23514' });
    await expect(
      requirePool().query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, 0,
                   decode(repeat('00', 32), 'hex'), 'text/plain', 'PENDING',
                   'SYSTEM', $4::uuid)`,
        [randomUUID(), secondProfileId, `invalid/${randomUUID()}`, administrator.id],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    const gapId = randomUUID();
    const gapOccurredAt = new Date();
    const gapRecord = {
      containerRestart: true,
      endedAt: null,
      id: gapId,
      lostBytes: null,
      occurredAt: gapOccurredAt,
      reason: 'CONTAINER_RESTART',
      runId,
      runUnitId: firstUnitId,
      startedAt: null,
    } as const;
    await store.recordGap(gapRecord);
    await store.recordGap(gapRecord);
    await expect(store.recordGap({ ...gapRecord, lostBytes: 1 })).rejects.toMatchObject({
      code: 'LOG_GAP_METADATA_CONFLICT',
    });
    const gap = await requirePool().query<{
      has_gap: boolean;
      lost_bytes: string | null;
      truncated: boolean;
    }>(
      `SELECT runs.has_gap, runs.truncated, gaps.lost_bytes::text
         FROM ape_dts_platform.task_runs AS runs
         JOIN ape_dts_platform.log_gaps AS gaps
           ON gaps.run_id = runs.id
        WHERE runs.id = $1::uuid
          AND gaps.id = $2::uuid`,
      [runId, gapId],
    );
    expect(gap.rows).toEqual([{ has_gap: true, lost_bytes: null, truncated: false }]);

    const streamErrorChild = new FakeChildProcess(31_500, null);
    const streamErrorSupervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => streamErrorChild as unknown as RuntimeChildProcess },
      logSink: new RuntimeLogPipelineSink(new LogPipeline({ store })),
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });
    const streamErrorRuntimeDirectory = await mkdtemp(path.join(tmpdir(), 'scs-u8-stream-error-'));
    temporaryDirectories.push(streamErrorRuntimeDirectory);
    const streamErrorProcess = await streamErrorSupervisor.start({
      args: [],
      cwd: streamErrorRuntimeDirectory,
      env: { PATH: process.env.PATH ?? '' },
      executablePath: process.execPath,
      iniContent: '[runtime]\n',
      iniPath: path.join(streamErrorRuntimeDirectory, 'runtime.ini'),
      log4rsContent: 'appenders: []\n',
      log4rsPath: path.join(streamErrorRuntimeDirectory, 'log4rs.yaml'),
      runtimeDirectory: streamErrorRuntimeDirectory,
      runId,
      unitId: firstUnitId,
    });
    streamErrorChild.emit('spawn');
    const earliestOccurredAt = Date.now();
    streamErrorChild.stdout.emit('error', new Error('stdout failed'));
    const latestOccurredAt = Date.now();
    streamErrorChild.close(0, null);
    await expect(streamErrorProcess.waitForExit()).resolves.toMatchObject({
      confirmed: true,
      logGapBytes: 0,
      logStreamError: true,
    });

    const streamError = await requirePool().query<{
      container_restart: boolean;
      ended_at: Date | null;
      lost_bytes: string | null;
      occurred_at: Date;
      reason: string;
      run_unit_id: string;
      started_at: Date | null;
    }>(
      `SELECT run_unit_id, reason, started_at, ended_at,
              lost_bytes::text, occurred_at, container_restart
         FROM ape_dts_platform.log_gaps
        WHERE run_id = $1::uuid
          AND run_unit_id = $2::uuid
          AND reason = 'STREAM_ERROR'`,
      [runId, firstUnitId],
    );
    expect(streamError.rows).toHaveLength(1);
    expect(streamError.rows[0]).toMatchObject({
      container_restart: false,
      ended_at: null,
      lost_bytes: null,
      reason: 'STREAM_ERROR',
      run_unit_id: firstUnitId,
      started_at: null,
    });
    expect(streamError.rows[0]?.occurred_at.getTime()).toBeGreaterThanOrEqual(earliestOccurredAt);
    expect(streamError.rows[0]?.occurred_at.getTime()).toBeLessThanOrEqual(latestOccurredAt);

    const deletedAt = new Date();
    await store.deleteSegment({
      reason: 'CAPACITY_ROLLOVER',
      segment: {
        ...firstSegment,
        deletedAt,
        deletedReason: 'CAPACITY_ROLLOVER',
        state: 'DELETED',
      },
    });
    const deleted = await requirePool().query<{
      has_gap: boolean;
      segment_state: string;
      stored_object_state: string;
      truncated: boolean;
    }>(
      `SELECT runs.has_gap,
              runs.truncated,
              segments.state AS segment_state,
              objects.state AS stored_object_state
         FROM ape_dts_platform.log_segments AS segments
         JOIN ape_dts_platform.stored_objects AS objects
           ON objects.id = segments.stored_object_id
         JOIN ape_dts_platform.task_runs AS runs
           ON runs.id = segments.run_id
        WHERE segments.id = $1::uuid`,
      [firstSegment.id],
    );
    expect(deleted.rows).toEqual([
      {
        has_gap: true,
        segment_state: 'DELETED',
        stored_object_state: 'DELETED',
        truncated: true,
      },
    ]);
    await expect(
      requirePool().query(
        `SELECT reason, completed_at IS NOT NULL AS completed
           FROM ape_dts_platform.log_segment_deletion_requests
          WHERE log_segment_id = $1::uuid`,
        [firstSegment.id],
      ),
    ).resolves.toMatchObject({
      rows: [{ completed: true, reason: 'CAPACITY_ROLLOVER' }],
    });
    await expect(
      readFile(path.join(localRoot, 'profile-a', ...firstObjectKey.split('/')), 'utf8'),
    ).rejects.toMatchObject({ code: 'ENOENT' });

    const profileDirectory = path.join(localRoot, 'profile-a');
    const profileBackup = path.join(localRoot, 'profile-a-backup');
    await rename(profileDirectory, profileBackup);
    await writeFile(profileDirectory, 'temporarily block the storage profile directory');
    const secondDeletedAt = new Date();
    try {
      await expect(
        store.deleteSegment({
          reason: 'CAPACITY_ROLLOVER',
          segment: {
            ...secondSegment,
            deletedAt: secondDeletedAt,
            deletedReason: 'CAPACITY_ROLLOVER',
            state: 'DELETED',
          },
        }),
      ).rejects.toMatchObject({
        code: 'LOG_SEGMENT_DELETE_FAILED',
        recoveryState: 'DURABLE_PENDING',
      });
    } finally {
      await rm(profileDirectory, { force: true });
      await rename(profileBackup, profileDirectory);
    }
    await expect(
      requirePool().query(
        `SELECT completed_at, last_error_code, reason
           FROM ape_dts_platform.log_segment_deletion_requests
          WHERE log_segment_id = $1::uuid`,
        [secondSegment.id],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          completed_at: null,
          last_error_code: 'LOG_SEGMENT_DELETE_FAILED',
          reason: 'CAPACITY_ROLLOVER',
        },
      ],
    });

    await expect(store.recoverPendingOperations()).resolves.toMatchObject({
      completedDeletions: 1,
      failures: 0,
    });
    await expect(
      requirePool().query(
        `SELECT runs.has_gap,
                runs.truncated,
                segments.state AS segment_state,
                objects.state AS stored_object_state,
                requests.completed_at IS NOT NULL AS deletion_completed,
                requests.last_error_code
           FROM ape_dts_platform.log_segments AS segments
           JOIN ape_dts_platform.stored_objects AS objects
             ON objects.id = segments.stored_object_id
           JOIN ape_dts_platform.task_runs AS runs
             ON runs.id = segments.run_id
           JOIN ape_dts_platform.log_segment_deletion_requests AS requests
             ON requests.log_segment_id = segments.id
          WHERE segments.id = $1::uuid`,
        [secondSegment.id],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          deletion_completed: true,
          has_gap: true,
          last_error_code: null,
          segment_state: 'DELETED',
          stored_object_state: 'DELETED',
          truncated: true,
        },
      ],
    });
  });

  it('rejects retiring a profile while an active run remains bound to it', async () => {
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const profileId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Unit 8 active run profile', 1, 'LOCAL',
                 '{"relative_prefix":"active-run"}'::jsonb, 'READ_ONLY_REFERENCED',
                 $3::uuid, statement_timestamp())`,
      [profileId, `unit8-active-run-${profileId}`, administrator.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET log_storage_profile_id = $2::uuid
        WHERE id = $1::uuid`,
      [runId, profileId],
    );

    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.storage_profiles
            SET state = 'RETIRED', retired_at = statement_timestamp()
          WHERE id = $1::uuid`,
        [profileId],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET status = 'STOPPED', current_stage = 'STOPPED', ended_at = statement_timestamp()
        WHERE id = $1::uuid`,
      [runId],
    );
    await expect(
      requirePool().query(
        `UPDATE ape_dts_platform.storage_profiles
            SET state = 'RETIRED', retired_at = statement_timestamp()
          WHERE id = $1::uuid
        RETURNING state`,
        [profileId],
      ),
    ).resolves.toMatchObject({ rows: [{ state: 'RETIRED' }] });
  });

  it('recovers prepared files, records restart gaps, and completes persisted deletion intents', async () => {
    const database = platformDatabase;
    if (database === undefined) {
      throw new Error('Unit 8 platform database is not initialized');
    }
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const unitResult = await requirePool().query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.task_run_units
        WHERE run_id = $1::uuid
        ORDER BY unit_key
        LIMIT 1`,
      [runId],
    );
    const runUnitId = unitResult.rows[0]?.id;
    if (runUnitId === undefined) {
      throw new Error('Unit 8 recovery fixture requires a runtime unit');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.storage_profiles
          SET state = 'READ_ONLY_REFERENCED'
        WHERE state = 'ACTIVE_WRITE'`,
    );
    const profileId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Unit 8 recovery profile', 1, 'LOCAL',
                 '{"relative_prefix":"recovery"}'::jsonb, 'ACTIVE_WRITE',
                 $3::uuid, statement_timestamp())`,
      [profileId, `unit8-recovery-${profileId}`, administrator.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET log_storage_profile_id = $2::uuid
        WHERE id = $1::uuid
          AND log_storage_profile_id IS NULL`,
      [runId, profileId],
    );

    const localRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-recovery-'));
    temporaryDirectories.push(localRoot);
    const createdAt = new Date();
    const fixtures = [
      { content: Buffer.from('complete\n'), expected: 'AVAILABLE', sequence: 1 },
      { content: Buffer.from('missing\n'), expected: 'MISSING', sequence: 2 },
      { content: Buffer.from('expected\n'), expected: 'INTEGRITY_FAILED', sequence: 3 },
    ] as const;
    const segments: Array<{ content: Buffer; id: string; objectId: string; objectKey: string }> =
      [];
    for (const fixture of fixtures) {
      const segmentId = randomUUID();
      const objectId = randomUUID();
      const objectKey = segmentObjectKey({ id: segmentId, runId, stream: 'STDOUT' });
      const sha256 = createHash('sha256').update(fixture.content).digest('hex');
      await requirePool().query(
        `INSERT INTO ape_dts_platform.stored_objects (
           id, storage_profile_id, kind, object_key, size_bytes, sha256,
           content_type, state, created_by_actor_type, created_by_user_id
         ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, $4::bigint,
                   decode($5, 'hex'), 'text/plain; charset=utf-8', 'PENDING',
                   'SYSTEM', NULL)`,
        [objectId, profileId, objectKey, fixture.content.length, sha256],
      );
      await requirePool().query(
        `INSERT INTO ape_dts_platform.log_segments (
           id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
           stored_object_id, byte_size, sha256, state, created_at
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STDOUT', $4::bigint,
                   $5::timestamptz, $5::timestamptz, $6::uuid, $7::bigint,
                   decode($8, 'hex'), 'SEALED', $5::timestamptz)`,
        [
          segmentId,
          runId,
          runUnitId,
          fixture.sequence,
          createdAt,
          objectId,
          fixture.content.length,
          sha256,
        ],
      );
      segments.push({ content: fixture.content, id: segmentId, objectId, objectKey });
    }
    for (const [index, segment] of segments.entries()) {
      if (index === 1) {
        continue;
      }
      const objectPath = path.join(localRoot, 'recovery', ...segment.objectKey.split('/'));
      await mkdir(path.dirname(objectPath), { recursive: true });
      await writeFile(objectPath, index === 2 ? 'corrupt\n' : segment.content);
    }

    const store = new PostgresLogSegmentStore(database, localRoot);
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET log_storage_state = 'DEGRADED',
              log_recovery_state = 'MIXED_PENDING'
        WHERE id = $1::uuid`,
      [runId],
    );
    await requirePool().query(
      `ALTER TABLE ape_dts_platform.log_gaps
         ADD CONSTRAINT log_gaps_reject_memory_pending_test CHECK (
           run_id <> '${runId}'::uuid OR run_unit_id IS NOT NULL
         )`,
    );
    try {
      await expect(store.recoverPendingOperations()).resolves.toEqual({
        availableSegments: 1,
        capacityDeletedSegments: 0,
        completedDeletions: 0,
        failures: 1,
        integrityFailedSegments: 1,
        memoryPendingGaps: 0,
        missingSegments: 1,
      });
      await expect(
        requirePool().query(
          `SELECT log_recovery_state
             FROM ape_dts_platform.task_runs
            WHERE id = $1::uuid`,
          [runId],
        ),
      ).resolves.toMatchObject({ rows: [{ log_recovery_state: 'MEMORY_PENDING' }] });
    } finally {
      await requirePool().query(
        `ALTER TABLE ape_dts_platform.log_gaps
           DROP CONSTRAINT IF EXISTS log_gaps_reject_memory_pending_test`,
      );
    }
    await expect(store.recoverPendingOperations()).resolves.toEqual({
      availableSegments: 0,
      capacityDeletedSegments: 0,
      completedDeletions: 0,
      failures: 0,
      integrityFailedSegments: 0,
      memoryPendingGaps: 1,
      missingSegments: 0,
    });
    const recovered = await requirePool().query<{
      failure_code: string | null;
      object_state: string;
      sequence: string;
      segment_state: string;
      unavailable_reason: string | null;
    }>(
      `SELECT segments.sequence::text,
              segments.state AS segment_state,
              segments.unavailable_reason,
              objects.state AS object_state,
              objects.failure_code
         FROM ape_dts_platform.log_segments AS segments
         JOIN ape_dts_platform.stored_objects AS objects
           ON objects.id = segments.stored_object_id
        WHERE segments.id = ANY($1::uuid[])
        ORDER BY segments.sequence`,
      [segments.map((segment) => segment.id)],
    );
    expect(recovered.rows).toEqual([
      {
        failure_code: null,
        object_state: 'AVAILABLE',
        segment_state: 'AVAILABLE',
        sequence: '1',
        unavailable_reason: null,
      },
      {
        failure_code: 'STORAGE_MISSING',
        object_state: 'MISSING',
        segment_state: 'MISSING',
        sequence: '2',
        unavailable_reason: 'STORAGE_MISSING',
      },
      {
        failure_code: 'INTEGRITY_MISMATCH',
        object_state: 'INTEGRITY_FAILED',
        segment_state: 'INTEGRITY_FAILED',
        sequence: '3',
        unavailable_reason: 'INTEGRITY_MISMATCH',
      },
    ]);
    await expect(
      requirePool().query(
        `SELECT id, reason, container_restart, lost_bytes::text
           FROM ape_dts_platform.log_gaps
          WHERE id = ANY($1::uuid[])
          ORDER BY id`,
        [[segments[1]?.id, segments[2]?.id]],
      ),
    ).resolves.toMatchObject({
      rows: expect.arrayContaining([
        {
          container_restart: true,
          id: segments[1]?.id,
          lost_bytes: String(segments[1]?.content.length),
          reason: 'CONTAINER_RESTART',
        },
        {
          container_restart: true,
          id: segments[2]?.id,
          lost_bytes: String(segments[2]?.content.length),
          reason: 'CONTAINER_RESTART',
        },
      ]),
    });
    await expect(
      requirePool().query(
        `SELECT has_gap, log_storage_state, log_recovery_state
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid`,
        [runId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          has_gap: true,
          log_recovery_state: 'NONE',
          log_storage_state: 'DEGRADED',
        },
      ],
    });
    await expect(
      requirePool().query(
        `SELECT count(*)::integer AS count
           FROM ape_dts_platform.log_gaps
          WHERE run_id = $1::uuid
            AND run_unit_id IS NULL
            AND reason = 'CONTAINER_RESTART'
            AND lost_bytes IS NULL`,
        [runId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });

    const available = segments[0];
    if (available === undefined) {
      throw new Error('Unit 8 recovery fixture did not create an available segment');
    }
    const deletedAt = new Date();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.log_segment_deletion_requests (
         log_segment_id, run_id, stored_object_id, reason, deleted_at
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'RETENTION', $4::timestamptz)`,
      [available.id, runId, available.objectId, deletedAt],
    );
    await rm(path.join(localRoot, 'recovery', ...available.objectKey.split('/')));
    await expect(store.recoverPendingOperations()).resolves.toMatchObject({
      completedDeletions: 1,
      failures: 0,
    });
    await expect(
      requirePool().query(
        `SELECT segments.state AS segment_state,
                objects.state AS object_state,
                requests.completed_at IS NOT NULL AS deletion_completed
           FROM ape_dts_platform.log_segments AS segments
           JOIN ape_dts_platform.stored_objects AS objects
             ON objects.id = segments.stored_object_id
           JOIN ape_dts_platform.log_segment_deletion_requests AS requests
             ON requests.log_segment_id = segments.id
          WHERE segments.id = $1::uuid`,
        [available.id],
      ),
    ).resolves.toMatchObject({
      rows: [{ deletion_completed: true, object_state: 'DELETED', segment_state: 'DELETED' }],
    });
  });

  it('rejects an unsupported active OSS profile before binding the run', async () => {
    const database = platformDatabase;
    if (database === undefined) {
      throw new Error('Unit 8 platform database is not initialized');
    }
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const unitResult = await requirePool().query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.task_run_units WHERE run_id = $1::uuid LIMIT 1`,
      [runId],
    );
    const runUnitId = unitResult.rows[0]?.id;
    if (runUnitId === undefined) {
      throw new Error('Unit 8 OSS fixture requires a runtime unit');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.storage_profiles
          SET state = 'READ_ONLY_REFERENCED'
        WHERE state = 'ACTIVE_WRITE'`,
    );
    const profileId = randomUUID();
    const secretId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.secrets (
         id, owner_type, owner_id, purpose, algorithm, dek_version,
         aad_version, nonce, ciphertext, tag
       ) VALUES ($1::uuid, 'STORAGE_PROFILE', $2::uuid, 'OSS_CREDENTIAL',
                 'AES_256_GCM', 1, 1, $3, $4, $5)`,
      [secretId, profileId, Buffer.alloc(12, 1), Buffer.alloc(32, 2), Buffer.alloc(16, 3)],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, secret_id,
         state, created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Unsupported OSS profile', 1, 'OSS',
                 '{"endpoint":"https://example.invalid","bucket":"logs","prefix":"runtime"}'::jsonb,
                 $3::uuid, 'ACTIVE_WRITE', $4::uuid, statement_timestamp())`,
      [profileId, `unit8-oss-${profileId}`, secretId, administrator.id],
    );
    const content = Buffer.from('must stay buffered\n');
    const createdAt = new Date();
    const segment: LogSegmentRecord = {
      byteSize: content.length,
      createdAt,
      deletedAt: null,
      deletedReason: null,
      endedAt: createdAt,
      id: randomUUID(),
      runId,
      runUnitId,
      sequence: 1,
      sha256: createHash('sha256').update(content).digest('hex'),
      startedAt: createdAt,
      state: 'SEALED',
      stream: 'STDOUT',
    };
    const localRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-oss-'));
    temporaryDirectories.push(localRoot);
    const store = new PostgresLogSegmentStore(database, localRoot);

    await expect(store.persistSegment({ content, segment })).rejects.toMatchObject({
      code: 'LOG_STORAGE_DRIVER_UNSUPPORTED',
      recoveryState: 'MEMORY_PENDING',
    });
    await expect(
      requirePool().query(
        `SELECT log_storage_profile_id,
                (SELECT count(*)::integer
                   FROM ape_dts_platform.log_segments
                  WHERE id = $2::uuid) AS segment_count
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid`,
        [runId, segment.id],
      ),
    ).resolves.toMatchObject({
      rows: [{ log_storage_profile_id: null, segment_count: 0 }],
    });
  });

  it('classifies a failure after durable metadata prepare as startup-recoverable', async () => {
    const database = platformDatabase;
    if (database === undefined) {
      throw new Error('Unit 8 platform database is not initialized');
    }
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const unitResult = await requirePool().query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.task_run_units WHERE run_id = $1::uuid LIMIT 1`,
      [runId],
    );
    const runUnitId = unitResult.rows[0]?.id;
    if (runUnitId === undefined) {
      throw new Error('Unit 8 durable recovery fixture requires a runtime unit');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.storage_profiles
          SET state = 'READ_ONLY_REFERENCED'
        WHERE state = 'ACTIVE_WRITE'`,
    );
    const profileId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Blocked LOCAL profile', 1, 'LOCAL',
                 '{"relative_prefix":"blocked"}'::jsonb, 'ACTIVE_WRITE',
                 $3::uuid, statement_timestamp())`,
      [profileId, `unit8-blocked-${profileId}`, administrator.id],
    );
    const localRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-blocked-'));
    temporaryDirectories.push(localRoot);
    await writeFile(path.join(localRoot, 'blocked'), 'not a directory');
    const pipeline = new LogPipeline({
      retryDelayMilliseconds: 60_000,
      store: new PostgresLogSegmentStore(database, localRoot),
    });

    pipeline.append({ chunk: 'durable pending\n', runId, stream: 'STDOUT', unitId: runUnitId });
    await expect(pipeline.close({ runId, unitId: runUnitId })).resolves.toEqual({
      recoveryState: 'DURABLE_PENDING',
      storageState: 'DEGRADED',
    });
    await expect(
      requirePool().query(
        `SELECT runs.log_storage_profile_id,
                runs.log_storage_state,
                runs.log_recovery_state,
                segments.state AS segment_state,
                objects.state AS object_state
           FROM ape_dts_platform.task_runs AS runs
           JOIN ape_dts_platform.log_segments AS segments ON segments.run_id = runs.id
           JOIN ape_dts_platform.stored_objects AS objects ON objects.id = segments.stored_object_id
          WHERE runs.id = $1::uuid`,
        [runId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          log_recovery_state: 'DURABLE_PENDING',
          log_storage_profile_id: profileId,
          log_storage_state: 'DEGRADED',
          object_state: 'PENDING',
          segment_state: 'SEALED',
        },
      ],
    });
  });

  it('persists real process stdout and stderr through the production module wiring', async () => {
    if (postgresPort === undefined) {
      throw new Error('Unit 8 PostgreSQL port is not initialized');
    }
    const task = await createTask();
    const { runId } = await createPersistedActiveRun(task);
    const unitResult = await requirePool().query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.task_run_units WHERE run_id = $1::uuid LIMIT 1`,
      [runId],
    );
    const runUnitId = unitResult.rows[0]?.id;
    if (runUnitId === undefined) {
      throw new Error('Unit 8 production wiring fixture requires a runtime unit');
    }
    await requirePool().query(
      `UPDATE ape_dts_platform.storage_profiles
          SET state = 'READ_ONLY_REFERENCED'
        WHERE state = 'ACTIVE_WRITE'`,
    );
    const profileId = randomUUID();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.storage_profiles (
         id, profile_key, display_name, version, driver, config, state,
         created_by_user_id, activated_at
       ) VALUES ($1::uuid, $2, 'Module-wired LOCAL profile', 1, 'LOCAL',
                 '{"relative_prefix":"module"}'::jsonb, 'ACTIVE_WRITE',
                 $3::uuid, statement_timestamp())`,
      [profileId, `unit8-module-${profileId}`, administrator.id],
    );
    await requirePool().query(
      `UPDATE ape_dts_platform.task_runs
          SET log_storage_profile_id = $2::uuid
        WHERE id = $1::uuid
          AND log_storage_profile_id IS NULL`,
      [runId, profileId],
    );
    const localRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-module-store-'));
    const runtimeRoot = await mkdtemp(path.join(tmpdir(), 'scs-u8-module-runtime-'));
    temporaryDirectories.push(localRoot, runtimeRoot);
    const startupSegmentId = randomUUID();
    const startupObjectId = randomUUID();
    const startupContent = Buffer.from('startup recovery\n');
    const startupObjectKey = segmentObjectKey({
      id: startupSegmentId,
      runId,
      stream: 'STDOUT',
    });
    const startupSha256 = createHash('sha256').update(startupContent).digest('hex');
    const startupTime = new Date();
    await requirePool().query(
      `INSERT INTO ape_dts_platform.stored_objects (
         id, storage_profile_id, kind, object_key, size_bytes, sha256,
         content_type, state, created_by_actor_type, created_by_user_id
       ) VALUES ($1::uuid, $2::uuid, 'LOG_SEGMENT', $3, $4::bigint,
                 decode($5, 'hex'), 'text/plain; charset=utf-8', 'PENDING',
                 'SYSTEM', NULL)`,
      [startupObjectId, profileId, startupObjectKey, startupContent.length, startupSha256],
    );
    await requirePool().query(
      `INSERT INTO ape_dts_platform.log_segments (
         id, run_id, run_unit_id, stream, sequence, started_at, ended_at,
         stored_object_id, byte_size, sha256, state, created_at
       ) VALUES ($1::uuid, $2::uuid, $3::uuid, 'STDOUT', 99,
                 $4::timestamptz, $4::timestamptz, $5::uuid, $6::bigint,
                 decode($7, 'hex'), 'SEALED', $4::timestamptz)`,
      [
        startupSegmentId,
        runId,
        runUnitId,
        startupTime,
        startupObjectId,
        startupContent.length,
        startupSha256,
      ],
    );
    const startupPath = path.join(localRoot, 'module', ...startupObjectKey.split('/'));
    await mkdir(path.dirname(startupPath), { recursive: true });
    await writeFile(startupPath, startupContent);
    const testingModule = await Test.createTestingModule({ imports: [RuntimeModule] })
      .overrideProvider(LOCAL_LOG_STORAGE_ROOT)
      .useValue(localRoot)
      .compile();
    try {
      const moduleDatabase = testingModule.get(PlatformDatabaseService, { strict: false });
      const bundle = await loadMigrationBundle();
      await moduleDatabase.inspectAndUse(
        {
          database: metadataDatabase,
          host: '127.0.0.1',
          password: databasePassword,
          port: postgresPort,
          tls: { mode: 'DISABLE' },
          username: databaseRole,
        },
        bundle.manifest,
      );
      const recoveryDeadline = Date.now() + 2_000;
      let startupState: string | undefined;
      while (Date.now() < recoveryDeadline) {
        const result = await requirePool().query<{ state: string }>(
          `SELECT state FROM ape_dts_platform.log_segments WHERE id = $1::uuid`,
          [startupSegmentId],
        );
        startupState = result.rows[0]?.state;
        if (startupState === 'AVAILABLE') {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(startupState).toBe('AVAILABLE');
      const supervisor = testingModule.get(RuntimeProcessSupervisor, { strict: false });
      const runtimeDirectory = path.join(runtimeRoot, 'unit');
      const processHandle = await supervisor.start({
        args: [
          '-e',
          "process.stdout.write('module stdout\\n'); process.stderr.write('password=module-secret\\n');",
        ],
        cwd: runtimeDirectory,
        env: { PATH: process.env.PATH ?? '' },
        executablePath: process.execPath,
        iniContent: '[runtime]\n',
        iniPath: path.join(runtimeDirectory, 'runtime.ini'),
        log4rsContent: 'appenders: []\n',
        log4rsPath: path.join(runtimeDirectory, 'log4rs.yaml'),
        runtimeDirectory,
        runId,
        unitId: runUnitId,
      });
      await expect(processHandle.waitForExit()).resolves.toMatchObject({
        confirmed: true,
        code: 0,
      });

      const persisted = await requirePool().query<{
        created_by_actor_type: string;
        created_by_user_id: string | null;
        object_key: string;
        stream: string;
      }>(
        `SELECT segments.stream,
                objects.object_key,
                objects.created_by_actor_type,
                objects.created_by_user_id
           FROM ape_dts_platform.log_segments AS segments
           JOIN ape_dts_platform.stored_objects AS objects
             ON objects.id = segments.stored_object_id
          WHERE segments.run_id = $1::uuid
            AND segments.run_unit_id = $2::uuid
            AND segments.sequence = 1
          ORDER BY segments.stream`,
        [runId, runUnitId],
      );
      expect(persisted.rows).toHaveLength(2);
      expect(persisted.rows.every((row) => row.created_by_actor_type === 'SYSTEM')).toBe(true);
      expect(persisted.rows.every((row) => row.created_by_user_id === null)).toBe(true);
      const contents = await Promise.all(
        persisted.rows.map(async (row) => ({
          content: await readFile(
            path.join(localRoot, 'module', ...row.object_key.split('/')),
            'utf8',
          ),
          stream: row.stream,
        })),
      );
      const records = contents.flatMap(({ content }) =>
        content
          .trimEnd()
          .split('\n')
          .map(
            (line) =>
              JSON.parse(line) as {
                readonly payload: string;
                readonly stream: string;
                readonly unitId: string;
              },
          ),
      );
      expect(records).toContainEqual(
        expect.objectContaining({
          payload: 'module stdout',
          stream: 'STDOUT',
          unitId: runUnitId,
        }),
      );
      expect(records).toContainEqual(
        expect.objectContaining({
          payload: 'password=[REDACTED]',
          stream: 'STDERR',
          unitId: runUnitId,
        }),
      );
    } finally {
      await testingModule.close();
    }
  });
});
