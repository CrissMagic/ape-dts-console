import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { execFileSync } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { chmod, copyFile, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import process from 'node:process';
import { setTimeout as delay } from 'node:timers/promises';

import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import kafkaPackage from 'kafkajs';
import { ApeDtsV1Adapter, CertifiedAdapterRegistry } from '@scs/config-model';
import { loadMigrationBundle, MigrationRunner, PgMigrationStore } from '@scs/database';
import { ArtifactCache, LocalObjectStorage } from '@scs/storage';

import { AuditLedgerService } from './dist/audit/audit-ledger.service.js';
import { PlatformDatabaseService } from './dist/database/platform-database.service.js';
import { DataSourcesService } from './dist/data-sources/data-sources.service.js';
import { DataSourceAdapterRegistry } from './dist/data-sources/adapters/types.js';
import { MysqlCompatibleDataSourceAdapter } from './dist/data-sources/adapters/mysql-compatible-data-source.adapter.js';
import { KafkaDataSourceAdapter } from './dist/data-sources/adapters/kafka-data-source.adapter.js';
import { CapacityReservationService } from './dist/pg-resources/capacity-reservation.service.js';
import { ContinuityService } from './dist/preflight/continuity.service.js';
import { PreflightService } from './dist/preflight/preflight.service.js';
import { RuntimeEnvironmentInspector } from './dist/preflight/runtime-environment-check.js';
import { PostgresRecoveryStore } from './dist/recovery/postgres-recovery.store.js';
import { RecoveryCoordinatorService } from './dist/recovery/recovery-coordinator.service.js';
import { PostgresRecoveryExternalActions } from './dist/recovery/recovery-external-actions.js';
import { TaskRecoveryService } from './dist/recovery/task-recovery.service.js';
import { AuthenticatedRuntimeLaunchSpecFactory } from './dist/runtime/authenticated-runtime-launch-spec.js';
import { MysqlRuntimeStateStore } from './dist/runtime/mysql-runtime-state.js';
import { createRuntimePerformanceCollector } from './dist/runtime/performance-collection.js';
import { RuntimePerformanceReadService } from './dist/runtime/performance-read.service.js';
import { RuntimePerformanceStore } from './dist/runtime/performance-store.js';
import { RuntimeProcessSupervisor, redactRuntimeLog } from './dist/runtime/process-supervisor.js';
import { RuntimeMetricsPortAllocator } from './dist/runtime/runtime-metrics-port-allocator.js';
import { RuntimeSupervisorService } from './dist/runtime/runtime-supervisor.service.js';
import { TasksService } from './dist/tasks/tasks.service.js';
import { UnlockedKeyService } from './dist/security/unlocked-key.service.js';

const password = process.env.SCS_FIXTURE_PASSWORD;
const { Kafka } = kafkaPackage;
assert.ok(password, 'isolated fixture password is required');
let restartState;
try {
  restartState = JSON.parse(await readFile('/fixture/restart-state.json', 'utf8'));
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}
const processBootId = randomUUID();
const connection = {
  host: 'metadata',
  port: 5432,
  database: process.env.SCS_FIXTURE_DATABASE,
  password,
  user: process.env.SCS_FIXTURE_ROLE,
};
const pool = new Pool(connection);
const database = new PlatformDatabaseService();
const key = new UnlockedKeyService();
key.set(restartState ? Buffer.from(restartState.dek, 'hex') : randomBytes(32));
const administrator = restartState?.administrator ?? {
  id: randomUUID(),
  username: 'admin',
  displayName: 'MySQL runtime integration',
  role: 'ADMIN',
  status: 'ACTIVE',
};
const audit = new AuditLedgerService(database);
const capacity = new CapacityReservationService(database);
const sources = new DataSourcesService(
  database,
  audit,
  key,
  new DataSourceAdapterRegistry([
    new MysqlCompatibleDataSourceAdapter('MYSQL'),
    new KafkaDataSourceAdapter(),
  ]),
);
const adapters = new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]);
const preflight = new PreflightService(
  database,
  audit,
  sources,
  capacity,
  adapters,
  new ContinuityService(database, audit),
  'integration-mysql-runtime',
  new RuntimeEnvironmentInspector({
    SCS_RUNTIME_METRICS_PORT_MIN: '40000',
    SCS_RUNTIME_METRICS_PORT_MAX: '41000',
  }),
);
const tasks = new TasksService(database, audit);
const logOutput = new Map();
const supervisors = [];
const stateStores = [];
const topicMessages = new Map();
let sourceConnection;
let kafkaAdmin;
let consumer;
let source;
let target;
let artifactId;
let consumerFailure;

function safe(value) {
  return String(value)
    .replaceAll(password, '[REDACTED]')
    .replace(/:\/\/[^@\s]+@/gu, '://[REDACTED]@');
}

async function until(operation, label, timeout = 60_000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    if (consumerFailure) throw consumerFailure;
    try {
      const value = await operation();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await delay(100);
  }
  throw new Error(`${label} timed out${last ? `: ${safe(last.stack ?? last)}` : ''}`);
}

function messages(topic) {
  return topicMessages.get(topic) ?? [];
}
function matches(topic, action, id, note) {
  return messages(topic).filter(
    (message) =>
      message.action === action &&
      (action === 'DELETE' ? message.before : message.data)?.some(
        (row) => Number(row.id) === id && (note === undefined || row.note === note),
      ),
  );
}

async function checkpoint(taskId) {
  return (
    await pool.query(
      'SELECT task_id,task_revision_id,active_run_id,client_server_id::text,phase,binlog_filename,binlog_position::text,checkpoint_at FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1',
      [taskId],
    )
  ).rows[0];
}

async function waitCheckpoint(taskId, minimum) {
  return await until(async () => {
    const value = await checkpoint(taskId);
    return (
      value?.checkpoint_at &&
      value.phase === 'CDC' &&
      value.binlog_filename === minimum.File &&
      BigInt(value.binlog_position) >= BigInt(minimum.Position) &&
      value
    );
  }, `committed MySQL checkpoint for ${taskId}`);
}

async function masterStatus() {
  return (await sourceConnection.query('SHOW MASTER STATUS'))[0][0];
}

async function setBrokerWritePolicy(value) {
  await writeFile('/fixture/output/broker-request.tmp', JSON.stringify(value));
  await rename('/fixture/output/broker-request.tmp', '/fixture/output/broker-request.json');
  await until(
    async () => JSON.parse(await readFile('/fixture/output/broker-applied.json', 'utf8')) === value,
    `deployment-side Broker write policy=${value}`,
    30_000,
  );
}

async function createSupervisor() {
  const instanceId = randomUUID();
  await pool.query(
    'INSERT INTO ape_dts_platform.platform_instances(id,platform_version,started_at,last_heartbeat_at,process_id) VALUES($1,$2,now(),now(),$3)',
    [instanceId, 'integration-mysql-runtime', process.pid],
  );
  const state = new MysqlRuntimeStateStore(database, sources, audit);
  stateStores.push(state);
  const service = new RuntimeSupervisorService(
    database,
    audit,
    capacity,
    new RuntimeProcessSupervisor({
      logSink: {
        append: ({ runId, chunk }) => {
          logOutput.set(
            runId,
            `${logOutput.get(runId) ?? ''}${chunk.toString('utf8')}`.slice(-64 * 1024),
          );
        },
        close: () => ({ recoveryState: 'NONE', storageState: 'HEALTHY' }),
      },
      redact: redactRuntimeLog,
      stopPolicy: { interruptGraceMilliseconds: 5_000, terminateGraceMilliseconds: 1_000 },
    }),
    new AuthenticatedRuntimeLaunchSpecFactory({
      adapters,
      artifactCache: new ArtifactCache('/fixture/cache'),
      database,
      localObjectStorage: new LocalObjectStorage('/fixture/storage'),
      platformVersion: 'integration-mysql-runtime',
      runtimeRoot: '/run/ape-dts',
      unlockedKey: key,
      environment: { PATH: process.env.PATH, RUST_BACKTRACE: '1' },
    }),
    { getStatus: () => ({ phase: 'READY', platformInstanceId: instanceId }) },
    preflight,
    new RuntimeMetricsPortAllocator({ minPort: 40000, maxPort: 41000 }),
    state,
  );
  supervisors.push(service);
  return { service, instanceId, state };
}

async function registerArtifact() {
  await mkdir('/fixture/storage/artifacts', { recursive: true });
  await mkdir('/fixture/extracted', { recursive: true });
  const header = await readFile('/fixture/engine-package');
  if (header[0] === 0x1f && header[1] === 0x8b) {
    execFileSync('tar', ['-xzf', '/fixture/engine-package', '-C', '/fixture/extracted']);
    await copyFile('/fixture/extracted/ape-dts', '/fixture/storage/artifacts/engine');
  } else await copyFile('/fixture/engine-package', '/fixture/storage/artifacts/engine');
  await chmod('/fixture/storage/artifacts/engine', 0o500);
  const bytes = await readFile('/fixture/storage/artifacts/engine');
  assert.equal(
    bytes.subarray(0, 4).toString('hex'),
    '7f454c46',
    'the registered engine must be a real Linux ELF',
  );
  const hash = createHash('sha256').update(bytes).digest();
  const storageProfileId = randomUUID();
  const storedObjectId = randomUUID();
  artifactId = randomUUID();
  await pool.query(
    "INSERT INTO ape_dts_platform.storage_profiles(id,profile_key,display_name,version,driver,config,state,created_by_user_id) VALUES($1,'mysql-test-artifacts','MySQL isolated artifact',1,'LOCAL','{\"relative_prefix\":\"\"}','DRAFT',$2)",
    [storageProfileId, administrator.id],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.stored_objects(id,storage_profile_id,kind,object_key,size_bytes,sha256,content_type,state,created_by_actor_type,created_by_user_id,available_at) VALUES($1,$2,'ARTIFACT','artifacts/engine',$3,$4,'application/octet-stream','AVAILABLE','USER',$5,now())",
    [storedObjectId, storageProfileId, bytes.length, hash, administrator.id],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.artifacts(id,version_identifier,declared_source,stored_object_id,declared_sha256,actual_sha256,size_bytes,state,activated_at,created_by_user_id) VALUES($1,'isolated-mysql-engine','SCS_TEST_APE_DTS_ARTIFACT',$2,$3,$3,$4,'ACTIVE',now(),$5)",
    [artifactId, storedObjectId, hash, bytes.length, administrator.id],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.artifact_signature_checks(id,artifact_id,policy_version,status,checked_by_user_id) VALUES($1,$2,'DISABLED','NOT_REQUIRED',$3)",
    [randomUUID(), artifactId, administrator.id],
  );
}

async function createTask(name, mappings, includeSnapshot = true) {
  return await tasks.create(administrator, {
    name,
    artifactId,
    sourceDataSourceId: source.id,
    targetDataSourceId: target.id,
    taskType: 'MYSQL_KAFKA',
    configuration: {
      kind: 'MYSQL_TO_KAFKA',
      schemaVersion: 2,
      source: { dataSourceId: source.id, includeSnapshot },
      target: { dataSourceId: target.id, messageFormat: 'json', jsonTemplate: 'cloudcanal' },
      mappings: mappings.map(([table, topics]) => ({
        source: { schema: 'data_demo', name: table },
        targets: topics.map((topic) => ({ topic })),
      })),
      tuning: {
        batchSize: 2,
        bufferSize: 16000,
        checkpointIntervalSeconds: 1,
        requiredAcks: 'all',
      },
    },
  });
}

async function prepare(taskId) {
  let task = await tasks.get(administrator, taskId);
  if (task.draft === null) {
    assert.ok(task.applied, 'stopped MySQL task must retain its applied configuration');
    task = await tasks.saveDraft(administrator, task.id, {
      taskType: 'MYSQL_KAFKA',
      revisionId: task.applied.id,
      expectedDraftVersion: task.applied.draftVersion,
      expectedTaskRowVersion: task.rowVersion,
      artifactId: task.applied.artifactId,
      configuration: task.applied.configuration,
    });
  }
  const checked = await preflight.run(administrator, task.id, {
    expectedTaskRowVersion: task.rowVersion,
    revisionId: task.draft.id,
  });
  assert.equal(checked.state, 'PASSED', JSON.stringify(checked.result.checks));
  return await tasks.get(administrator, taskId);
}

async function startTask(supervisor, taskId) {
  const task = await prepare(taskId);
  const operation = await supervisor.start(administrator, {
    taskId,
    expectedTaskRowVersion: task.rowVersion,
    revisionId: task.draft.id,
    idempotencyKey: randomUUID(),
  });
  assert.equal(operation.state, 'SUCCEEDED', JSON.stringify(operation));
  await until(async () => {
    const row = await checkpoint(taskId);
    return row?.phase === 'CDC' && row;
  }, 'automatic snapshot-to-CDC transition');
  return operation;
}

async function stopTask(supervisor, taskId) {
  const task = await tasks.get(administrator, taskId);
  const result = await supervisor.stop(administrator, {
    taskId,
    expectedTaskRowVersion: task.rowVersion,
    idempotencyKey: randomUUID(),
  });
  assert.equal(result.state, 'SUCCEEDED', JSON.stringify(result));
  assert.equal((await tasks.get(administrator, taskId)).observedState, 'STOPPED');
}

async function run() {
  const bundle = await loadMigrationBundle();
  if (!restartState)
    await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
      expectedState: 'ABSENT',
    });
  await database.inspectAndUse(
    { ...connection, username: connection.user, tls: { mode: 'DISABLE' } },
    bundle.manifest,
  );
  assert.equal(await database.acquireControllerLock(), true);
  if (!restartState) {
    await pool.query(
      "INSERT INTO ape_dts_platform.users(id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash) VALUES($1,'admin',$2,'ADMIN','ACTIVE','ARGON2ID',$3,$4,$5)",
      [
        administrator.id,
        administrator.displayName,
        { memoryKiB: 1024, parallelism: 1, passes: 1, tagLength: 32 },
        Buffer.alloc(16, 7),
        Buffer.alloc(32, 8),
      ],
    );
    await pool.query(
      'INSERT INTO ape_dts_platform.platform_settings(singleton_id,max_runtime_units) VALUES(1,4)',
    );
  }
  sourceConnection = await mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password,
    database: 'data_demo',
    multipleStatements: true,
  });
  if (!restartState) {
    await sourceConnection.query(
      "CREATE TABLE orders(id BIGINT PRIMARY KEY,note VARCHAR(64) NOT NULL); CREATE TABLE items(id BIGINT PRIMARY KEY,note VARCHAR(64) NOT NULL); CREATE TABLE archived_rows(id BIGINT PRIMARY KEY,note VARCHAR(64) NOT NULL); INSERT INTO orders VALUES(1,'initial-order'); INSERT INTO items VALUES(1,'initial-item')",
    );
    source = await sources.create(administrator.id, {
      name: 'Isolated MySQL',
      configuration: {
        type: 'MYSQL',
        host: 'mysql',
        port: 3306,
        database: 'data_demo',
        username: 'root',
        tls: { mode: 'DISABLE', riskAcknowledged: true },
      },
      credentials: { mode: 'SET', password },
    });
    target = await sources.create(administrator.id, {
      name: 'Isolated Kafka',
      configuration: {
        type: 'KAFKA',
        bootstrapServers: ['kafka:9092'],
        plaintextRiskAcknowledged: true,
      },
      credentials: { mode: 'NONE' },
    });
  } else {
    source = restartState.source;
    target = restartState.target;
    artifactId = restartState.artifactId;
  }
  const kafka = new Kafka({
    clientId: 'mysql-platform-fixture',
    brokers: ['kafka:9092'],
    logLevel: 0,
  });
  kafkaAdmin = kafka.admin();
  await kafkaAdmin.connect();
  const topics = ['orders', 'archive', 'items', 'expired'];
  await kafkaAdmin.createTopics({
    waitForLeaders: true,
    topics: topics.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
  });
  consumer = kafka.consumer({ groupId: `mysql-fixture-${randomUUID()}`, retry: { retries: 2 } });
  await consumer.connect();
  await consumer.subscribe({ topics, fromBeginning: true });
  await consumer
    .run({
      eachMessage: async ({ topic, message }) => {
        if (message.value === null) return;
        const parsed = JSON.parse(message.value.toString('utf8'));
        topicMessages.set(topic, [...messages(topic), parsed]);
      },
    })
    .catch((error) => {
      consumerFailure = error;
    });
  const cluster = await kafkaAdmin.describeCluster();
  const archiveMetadata = (await kafkaAdmin.fetchTopicMetadata({ topics: ['archive'] })).topics[0];
  assert.equal(cluster.clusterId, 'MkU3OEVBNTcwNTJENDM2Qk');
  assert.equal(archiveMetadata.name, 'archive');
  assert.equal(archiveMetadata.partitions.length, 1);
  assert.equal(archiveMetadata.partitions[0].replicas.length, 1);
  if (restartState) {
    await resumeAfterAbruptRestart(kafka);
    return;
  }
  await registerArtifact();
  const first = await createSupervisor();
  const task = await createTask('MySQL snapshot fanout', [
    ['orders', ['orders', 'archive']],
    ['items', ['items']],
  ]);
  const initial = await startTask(first.service, task.id);
  await until(
    () =>
      matches('orders', 'INSERT', 1, 'initial-order').length &&
      matches('items', 'INSERT', 1, 'initial-item').length,
    'initial snapshot records',
  );
  await sourceConnection.query(
    "START TRANSACTION;INSERT INTO orders VALUES(2,'after-snapshot');UPDATE orders SET note='changed-order' WHERE id=1;DELETE FROM orders WHERE id=2;INSERT INTO items VALUES(2,'new-item');COMMIT",
  );
  await until(
    () =>
      matches('orders', 'INSERT', 2).length &&
      matches('orders', 'UPDATE', 1, 'changed-order').length &&
      matches('orders', 'DELETE', 2).length &&
      matches('items', 'INSERT', 2).length,
    'CDC transaction records',
  );
  await until(() => messages('orders').length === messages('archive').length, 'fanout delivery');
  assert.deepEqual(messages('archive'), messages('orders'));
  assert.ok(
    messages('orders').every(
      (message) =>
        message.db === 'data_demo' && message.schema === 'data_demo' && message.table === 'orders',
    ),
  );
  const committed = await waitCheckpoint(task.id, await masterStatus());
  process.stdout.write('MYSQL_RUNTIME_FIXTURE snapshot/fanout/checkpoint passed\n');
  await stopTask(first.service, task.id);
  const stopped = await checkpoint(task.id);
  assert.equal(stopped.binlog_filename, committed.binlog_filename);
  assert.ok(BigInt(stopped.binlog_position) >= BigInt(committed.binlog_position));
  await sourceConnection.query("INSERT INTO orders VALUES(3,'while-stopped')");
  const resumed = await startTask(first.service, task.id);
  assert.notEqual(resumed.runId, initial.runId);
  await until(
    () =>
      matches('orders', 'INSERT', 3, 'while-stopped').length &&
      matches('archive', 'INSERT', 3, 'while-stopped').length,
    'stopped-window replay',
  );
  assert.equal(
    matches('orders', 'INSERT', 1, 'initial-order').length,
    1,
    'resuming must not rerun the original snapshot',
  );
  assert.equal(
    (
      await pool.query(
        "SELECT count(*)::int AS count FROM ape_dts_platform.task_run_stages WHERE run_id=$1 AND stage_type='SNAPSHOT'",
        [resumed.runId],
      )
    ).rows[0].count,
    0,
  );
  const beforeRestart = await waitCheckpoint(task.id, await masterStatus());
  process.stdout.write('MYSQL_RUNTIME_FIXTURE normal stop/resume passed\n');
  await first.service.beforeApplicationShutdown('SIGTERM');
  first.state.onModuleDestroy();
  const blocked = await tasks.get(administrator, task.id);
  assert.equal(blocked.desiredState, 'RUNNING');
  assert.equal(blocked.observedState, 'BLOCKED');
  assert.equal(
    (
      await pool.query(
        'SELECT clean_shutdown FROM ape_dts_platform.platform_instances WHERE id=$1',
        [first.instanceId],
      )
    ).rows[0].clean_shutdown,
    true,
  );
  await sourceConnection.query("INSERT INTO items VALUES(3,'while-platform-stopped')");
  const second = await createSupervisor();
  await second.service.reconcilePersistedRuns();
  const systemCheck = await preflight.runForSystemRecovery({
    expectedOldRunId: resumed.runId,
    expectedPlatformInstanceId: first.instanceId,
    expectedTaskRowVersion: blocked.rowVersion,
    revisionId: blocked.currentAppliedRevisionId,
    taskId: task.id,
  });
  assert.equal(systemCheck.state, 'PASSED', JSON.stringify(systemCheck));
  const recoveredTask = await tasks.get(administrator, task.id);
  const recovered = await second.service.recoverPersistedTask({
    expectedTaskRowVersion: recoveredTask.rowVersion,
    oldRunId: resumed.runId,
    platformInstanceId: first.instanceId,
    revisionId: recoveredTask.currentAppliedRevisionId,
    taskId: task.id,
    unitKeys: ['primary'],
  });
  await until(
    () => matches('items', 'INSERT', 3, 'while-platform-stopped').length,
    'platform shutdown window replay',
  );
  const recoveredCheckpoint = await waitCheckpoint(task.id, await masterStatus());
  assert.equal(recoveredCheckpoint.active_run_id, recovered.runId);
  assert.equal(recoveredCheckpoint.client_server_id, beforeRestart.client_server_id);
  assert.equal(matches('orders', 'INSERT', 1, 'initial-order').length, 1);
  process.stdout.write('MYSQL_RUNTIME_FIXTURE system recovery passed\n');

  await writeFile(
    '/fixture/restart-state.json',
    JSON.stringify({
      administrator,
      artifactId,
      checkpoint: recoveredCheckpoint,
      dek: key.require().toString('hex'),
      oldInstanceId: second.instanceId,
      oldRunId: recovered.runId,
      processBootId,
      source,
      target,
      taskId: task.id,
    }),
    { mode: 0o600 },
  );
  await writeFile('/fixture/output/crash-requested.json', JSON.stringify({ processBootId }));
  // The Docker daemon sends SIGKILL from outside the PID namespace; namespace PID 1
  // can ignore signals generated inside its own namespace.
  await new Promise(() => {});
}

async function resumeAfterAbruptRestart(kafka) {
  assert.notEqual(
    processBootId,
    restartState.processBootId,
    'recovery must run in a new Node process',
  );
  assert.notEqual(
    (
      await pool.query(
        'SELECT clean_shutdown FROM ape_dts_platform.platform_instances WHERE id=$1',
        [restartState.oldInstanceId],
      )
    ).rows[0].clean_shutdown,
    true,
    'the killed platform must not be recorded as a graceful shutdown',
  );
  const current = await createSupervisor();
  await current.service.reconcilePersistedRuns();
  const blocked = await tasks.get(administrator, restartState.taskId);
  assert.equal(blocked.desiredState, 'RUNNING');
  assert.equal(blocked.observedState, 'BLOCKED');
  const coordinator = new RecoveryCoordinatorService(
    new PostgresRecoveryStore(database),
    new PostgresRecoveryExternalActions(database, preflight, current.service),
  );
  const recovery = new TaskRecoveryService(
    database,
    { getStatus: () => ({ phase: 'READY', platformInstanceId: current.instanceId }) },
    current.service,
    coordinator,
    audit,
  );
  const outcome = await recovery.retry(administrator, blocked.id, blocked.rowVersion);
  assert.equal(outcome.state, 'READY', JSON.stringify(outcome));
  await until(
    () => matches('items', 'INSERT', 4, 'while-node-killed').length,
    'new Node process replay of the abrupt shutdown window',
  );
  const recoveredCheckpoint = await waitCheckpoint(blocked.id, await masterStatus());
  assert.notEqual(recoveredCheckpoint.active_run_id, restartState.oldRunId);
  assert.equal(recoveredCheckpoint.client_server_id, restartState.checkpoint.client_server_id);
  await until(
    () => matches('orders', 'INSERT', 1, 'initial-order').length,
    'retained message history',
  );
  assert.equal(matches('orders', 'INSERT', 1, 'initial-order').length, 1);
  assert.equal(
    (
      await pool.query(
        "SELECT count(*)::int AS count FROM ape_dts_platform.task_run_stages WHERE run_id=$1 AND stage_type='SNAPSHOT'",
        [recoveredCheckpoint.active_run_id],
      )
    ).rows[0].count,
    0,
    'abrupt restart must resume the committed position, not restart the snapshot',
  );
  process.stdout.write('MYSQL_RUNTIME_FIXTURE abrupt new Node recovery passed\n');
  await verifyFailuresAndBroker(kafka, current, blocked, {
    runId: recoveredCheckpoint.active_run_id,
  });
}

async function verifyFailuresAndBroker(kafka, second, task, recovered) {
  const expired = await createTask(
    'Expired checkpoint remains local',
    [['archived_rows', ['expired']]],
    false,
  );
  await startTask(second.service, expired.id);
  await sourceConnection.query("INSERT INTO archived_rows VALUES(1,'old-binlog')");
  await until(
    () => matches('expired', 'INSERT', 1, 'old-binlog').length,
    'expired-task seed transaction',
  );
  const expiredPosition = await waitCheckpoint(expired.id, await masterStatus());
  await stopTask(second.service, expired.id);
  await sourceConnection.query('FLUSH BINARY LOGS');
  await sourceConnection.query("INSERT INTO orders VALUES(4,'new-binlog')");
  await until(
    () => matches('orders', 'INSERT', 4, 'new-binlog').length,
    'healthy task on new Binlog',
  );
  const current = await waitCheckpoint(task.id, await masterStatus());
  assert.notEqual(current.binlog_filename, expiredPosition.binlog_filename);
  await sourceConnection.query('PURGE BINARY LOGS TO ?', [current.binlog_filename]);
  const expiredDraft = await prepare(expired.id);
  await assert.rejects(
    () =>
      second.service.start(administrator, {
        taskId: expired.id,
        expectedTaskRowVersion: expiredDraft.rowVersion,
        revisionId: expiredDraft.draft.id,
        idempotencyKey: randomUUID(),
      }),
    (error) => error.code === 'MYSQL_BINLOG_POSITION_UNAVAILABLE',
  );
  assert.equal((await tasks.get(administrator, task.id)).observedState, 'RUNNING');
  assert.ok(
    (await tasks.list(administrator)).some((item) => item.id === expired.id),
    'a failed task must remain manageable',
  );
  assert.equal((await checkpoint(expired.id)).binlog_filename, expiredPosition.binlog_filename);

  const beforeDenied = await waitCheckpoint(task.id, await masterStatus());
  await setBrokerWritePolicy('REJECT');
  const rejectionProbe = kafka.producer({ retry: { retries: 0 } });
  await rejectionProbe.connect();
  try {
    await until(
      async () => {
        try {
          await rejectionProbe.send({
            topic: 'archive',
            acks: -1,
            messages: [{ value: JSON.stringify({ action: 'TEST_REJECTION_PROBE', data: [] }) }],
          });
          return false;
        } catch (error) {
          assert.match(
            String(error.type ?? error.message),
            /TOPIC_AUTHORIZATION_FAILED|Not authorized/iu,
          );
          return true;
        }
      },
      'Broker must actually reject writes before the source transaction is committed',
      15_000,
    );
  } finally {
    await rejectionProbe.disconnect();
  }
  await sourceConnection.query(
    "START TRANSACTION;INSERT INTO orders VALUES(10,'broker-reject-first');INSERT INTO orders VALUES(11,'broker-reject-second');COMMIT",
  );
  const deniedPosition = await masterStatus();
  await until(
    async () => {
      const state = await tasks.get(administrator, task.id);
      return (
        ['FAILED', 'BLOCKED'].includes(state.observedState) ||
        /TOPIC_AUTHORIZATION_FAILED|Not authorized|authorization|Failed.*produce/iu.test(
          logOutput.get(recovered.runId) ?? '',
        )
      );
    },
    'real Broker rejection',
    45_000,
  );
  await second.state.flush(recovered.runId);
  const afterDenied = await checkpoint(task.id);
  assert.equal(afterDenied.binlog_filename, beforeDenied.binlog_filename);
  assert.equal(
    afterDenied.binlog_position,
    beforeDenied.binlog_position,
    'a partially delivered/rejected source transaction cannot advance the committed checkpoint',
  );
  assert.ok(BigInt(afterDenied.binlog_position) < BigInt(deniedPosition.Position));
  assert.equal(matches('archive', 'INSERT', 10).length, 0);
  await setBrokerWritePolicy('ALLOW');
  await until(
    () =>
      ['orders', 'archive'].every(
        (topic) =>
          matches(topic, 'INSERT', 10, 'broker-reject-first').length &&
          matches(topic, 'INSERT', 11, 'broker-reject-second').length,
      ),
    'replay to every target after the Broker recovers',
    90_000,
  );
  const finalCheckpoint = await waitCheckpoint(task.id, await masterStatus());
  const performanceRunId = finalCheckpoint.active_run_id;
  const performance = createRuntimePerformanceCollector(
    database,
    second.service,
    new RuntimePerformanceStore(database),
  );
  try {
    await until(
      async () => {
        await performance.runOnce();
        return (
          await pool.query(
            `SELECT count(*)::int AS count FROM ape_dts_platform.metric_samples_high
             WHERE task_id=$1 AND run_id=$2 AND gap_reason IS NULL AND sink_records>0`,
            [task.id, performanceRunId],
          )
        ).rows[0].count;
      },
      'real engine HTTP metrics must be persisted against the owned runtime process',
      30_000,
    );
  } finally {
    await performance.onModuleDestroy();
  }
  await stopTask(second.service, task.id);
  const history = await new RuntimePerformanceReadService(database).detail(
    administrator,
    task.id,
    '1h',
    performanceRunId,
  );
  assert.equal(history.summary.status, 'NOT_RUNNING');
  assert.equal(history.summary.values.recordsPerSecond, null);
  assert.ok(history.runs.some((run) => run.id === performanceRunId && run.endedAt !== null));
  assert.ok(
    history.points.some((point) => !point.gap && point.sinkRecords > 0),
    'stopping the task must not erase real, persisted performance samples',
  );
  process.stdout.write('MYSQL_RUNTIME_FIXTURE stopped historical performance retained passed\n');
  await writeFile(
    '/fixture/output/result.json',
    JSON.stringify({
      snapshotAndFanout: true,
      stoppedResume: true,
      systemRecovery: true,
      abruptNodeRestart: true,
      expiredTaskIsLocal: true,
      brokerRejectedTransactionRetainsCheckpoint: true,
      historicalMetricsRetained: true,
    }),
  );
}

let failure;
try {
  await run();
} catch (error) {
  failure = error;
  for (const [runId, text] of logOutput)
    process.stderr.write(
      `run ${runId}: ${safe(text)
        .split('\n')
        .filter((line) =>
          / ERROR | WARN | starts| started|Binlog|checkpoint|task stopped|filtered|shutdown/iu.test(
            line,
          ),
        )
        .join('\n')
        .slice(-5_000)}\n`,
    );
  try {
    process.stderr.write(
      `checkpoint facts: ${JSON.stringify((await pool.query('SELECT task_id,active_run_id,phase,binlog_filename,binlog_position::text,checkpoint_at FROM ape_dts_platform.mysql_task_checkpoints')).rows)}\n`,
    );
    process.stderr.write(
      `metric facts: ${JSON.stringify((await pool.query('SELECT task_id,run_id,gap_reason,sink_records FROM ape_dts_platform.metric_samples_high ORDER BY collected_at DESC LIMIT 5')).rows)}\n`,
    );
  } catch {
    /* Schema setup may itself have failed. */
  }
  process.stderr.write(
    `consumed facts: ${JSON.stringify([...topicMessages].map(([topic, rows]) => [topic, rows.map((row) => [row.action, (row.action === 'DELETE' ? row.before : row.data)?.map((value) => value.id)])]))}\n`,
  );
  process.stderr.write(`${safe(error.stack ?? error)}\n`);
} finally {
  for (const supervisor of supervisors) {
    try {
      await supervisor.beforeApplicationShutdown('SIGTERM');
    } catch (error) {
      failure ??= error;
    }
  }
  for (const state of stateStores) state.onModuleDestroy();
  await consumer?.disconnect().catch((error) => {
    failure ??= error;
  });
  await kafkaAdmin?.disconnect().catch((error) => {
    failure ??= error;
  });
  await sourceConnection?.end().catch((error) => {
    failure ??= error;
  });
  await database.onApplicationShutdown().catch((error) => {
    failure ??= error;
  });
  await pool.end().catch((error) => {
    failure ??= error;
  });
  key.clear();
}
if (failure) process.exitCode = 1;
