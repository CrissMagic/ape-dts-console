import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { CsrfGuard, SessionAuthGuard } from '../../../apps/api/src/auth/auth.guards.js';
import { RuntimeController } from '../../../apps/api/src/runtime/runtime.controller.js';
import { RuntimeLogReadService } from '../../../apps/api/src/runtime/runtime-log-read.service.js';
import {
  RuntimeReadService,
  RuntimeReadNotFoundError,
} from '../../../apps/api/src/runtime/runtime-read.service.js';
import { RuntimeSupervisorService } from '../../../apps/api/src/runtime/runtime-supervisor.service.js';
import { LogPipelineCoordinatorService } from '../../../apps/api/src/log-lifecycle/log-pipeline-coordinator.service.js';
import { PostgresLogSegmentStore } from '../../../apps/api/src/storage/postgres-log-segment-store.js';
import { LogPipeline } from '../../../packages/observability/src/index.js';
import { LocalLogSegmentObjectStore, segmentSha256 } from '../../../packages/storage/src/index.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { BootstrapStateService } from '../../../apps/api/src/bootstrap/bootstrap-state.service.js';
import { EmergencyPendingEventQueue } from '../../../apps/api/src/emergency/emergency-pending-events.js';
import type { EmergencyStructuredLogEvent } from '../../../apps/api/src/emergency/emergency-ports.js';
import { EmergencyReconciliationService } from '../../../apps/api/src/emergency/emergency-reconciliation.service.js';
import { PlatformReadinessService } from '../../../apps/api/src/health/platform-readiness.service.js';
import { MetadataAvailabilityService } from '../../../apps/api/src/metadata-availability/metadata-availability.service.js';
import { PostgresRecoveryStore } from '../../../apps/api/src/recovery/postgres-recovery.store.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
  PgSchemaInspector,
} from '../../../packages/database/src/index.js';

const suffix = `${process.pid}_${Date.now()}`;
const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const role = `scs_unit8_${suffix}`;
const databaseName = `scs_unit8_${suffix}`;
const password = randomBytes(24).toString('base64url');

let pool: Pool;
let port: number;

class IntegrationRuntimeReconciler {
  calls = 0;

  async reconcilePersistedRuns(): Promise<void> {
    this.calls += 1;
  }
}

interface RuntimeFixture {
  operationId: string;
  platformInstanceId: string;
  revisionId: string;
  runId: string;
  taskId: string;
}

async function createRuntimeFixture(): Promise<RuntimeFixture> {
  const fixture = {
    operationId: randomUUID(),
    platformInstanceId: randomUUID(),
    revisionId: randomUUID(),
    runId: randomUUID(),
    taskId: randomUUID(),
  };
  const userId = randomUUID();
  const sourceId = randomUUID();
  const targetId = randomUUID();
  await pool.query(`
    INSERT INTO ape_dts_platform.platform_instances
      (id, platform_version, started_at, last_heartbeat_at)
    VALUES ('${fixture.platformInstanceId}', '0.1.0', statement_timestamp(), statement_timestamp());
    INSERT INTO ape_dts_platform.users
      (id, normalized_username, display_name, role, status, password_kdf_algorithm,
       password_kdf_parameters, password_salt, password_hash)
    VALUES ('${userId}', 'unit8-${userId}', 'Unit 8 user', 'ADMIN', 'ACTIVE', 'ARGON2ID', '{}',
            decode(repeat('05',16),'hex'), decode(repeat('06',32),'hex'));
    INSERT INTO ape_dts_platform.data_sources
      (id, normalized_name, source_type, configuration, state, created_by_user_id, updated_by_user_id)
    VALUES ('${sourceId}', 'unit8-source-${sourceId}', 'KAFKA', '{"type":"KAFKA"}', 'ACTIVE', '${userId}', '${userId}'),
           ('${targetId}', 'unit8-target-${targetId}', 'KAFKA', '{"type":"KAFKA"}', 'ACTIVE', '${userId}', '${userId}');
    INSERT INTO ape_dts_platform.tasks
      (id, normalized_name, display_name, task_type, source_data_source_id, target_data_source_id,
       desired_state, observed_state, observability_health, recovery_disposition,
       created_by_user_id, updated_by_user_id)
    VALUES ('${fixture.taskId}', 'unit8-task-${fixture.taskId}', 'Unit 8 runtime task', 'KAFKA_KAFKA',
            '${sourceId}', '${targetId}', 'RUNNING', 'RUNNING', 'HEALTHY', 'AUTO_ELIGIBLE', '${userId}', '${userId}');
    INSERT INTO ape_dts_platform.task_revisions
      (id, task_id, revision_number, state, configuration, created_by_user_id, updated_by_user_id)
    VALUES ('${fixture.revisionId}', '${fixture.taskId}', 1, 'APPLIED', '{}', '${userId}', '${userId}');
    UPDATE ape_dts_platform.tasks SET current_applied_revision_id = '${fixture.revisionId}' WHERE id = '${fixture.taskId}';
    INSERT INTO ape_dts_platform.task_operations
      (id, task_id, action, idempotency_key, target_revision_id, state)
    VALUES ('${fixture.operationId}', '${fixture.taskId}', 'EMERGENCY_STOP', 'unit8-${fixture.operationId}',
            '${fixture.revisionId}', 'EXECUTING');
    INSERT INTO ape_dts_platform.task_runs
      (id, task_id, task_revision_id, operation_id, platform_instance_id, process_binding_hash, status, current_stage)
    VALUES ('${fixture.runId}', '${fixture.taskId}', '${fixture.revisionId}', '${fixture.operationId}',
            '${fixture.platformInstanceId}', '${'a'.repeat(64)}', 'RUNNING', 'RUNNING');
    UPDATE ape_dts_platform.task_operations SET run_id = '${fixture.runId}' WHERE id = '${fixture.operationId}';
  `);
  return fixture;
}

function confirmedEvent(
  fixture: RuntimeFixture,
  eventId = randomUUID(),
): EmergencyStructuredLogEvent {
  return {
    action: 'EMERGENCY_STOP_CONFIRMED',
    details: { runId: fixture.runId, unitCount: 1 },
    eventId,
    occurredAt: new Date().toISOString(),
    operationId: fixture.operationId,
    platformInstanceId: fixture.platformInstanceId,
    processBindingHash: 'a'.repeat(64),
    reason: null,
    revisionId: fixture.revisionId,
    taskId: fixture.taskId,
  };
}

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', [...arguments_], {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
}

function administer(sql: string): void {
  docker(
    [
      'exec',
      '--interactive',
      container,
      'psql',
      '--username',
      'postgres',
      '--dbname',
      'postgres',
      '--set',
      'ON_ERROR_STOP=1',
    ],
    sql,
  );
}

async function queryBatch(sql: string, values: readonly unknown[]): Promise<void> {
  for (const statement of sql
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean)) {
    const indexes = [
      ...new Set([...statement.matchAll(/\$(\d+)/gu)].map((match) => Number(match[1]))),
    ];
    const positions = new Map(indexes.map((index, position) => [index, position + 1]));
    const localized = statement.replace(
      /\$(\d+)/gu,
      (_, raw: string) => `$${positions.get(Number(raw))}`,
    );
    await pool.query(
      localized,
      indexes.map((index) => values[index - 1]),
    );
  }
}

beforeAll(async () => {
  expect(docker(['inspect', '--format', '{{.State.Running}}', container])).toBe('true');
  const binding = docker(['port', container, '5432/tcp']);
  port = Number(binding.slice(binding.lastIndexOf(':') + 1));
  administer(
    `CREATE ROLE "${role}" LOGIN PASSWORD '${password}';
     CREATE DATABASE "${databaseName}" OWNER "${role}";`,
  );
  pool = new Pool({ database: databaseName, host: '127.0.0.1', password, port, user: role });
});

afterAll(async () => {
  await pool?.end();
  try {
    administer(
      `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
        WHERE datname = '${databaseName}' AND pid <> pg_backend_pid();
       DROP DATABASE IF EXISTS "${databaseName}";
       DROP ROLE IF EXISTS "${role}";`,
    );
  } catch (error) {
    process.stderr.write(`failed to clean up Unit 8 PostgreSQL resources: ${String(error)}\n`);
  }
});

describe.sequential('Unit 8 PostgreSQL integration', () => {
  it('applies migrations 1 through 16 and enforces the emergency fact constraints', async () => {
    const bundle = await loadMigrationBundle();
    const instanceId = randomUUID();
    await expect(
      new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, instanceId, {
        expectedState: 'ABSENT',
      }),
    ).resolves.toMatchObject({
      appliedVersions: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    });

    const client = await pool.connect();
    try {
      await expect(new PgSchemaInspector().inspect(client, bundle.manifest)).resolves.toMatchObject(
        {
          appliedVersion: 16,
          pendingVersions: [],
          state: 'CURRENT',
        },
      );
    } finally {
      client.release();
    }

    const platformInstanceId = randomUUID();
    const administratorId = randomUUID();
    const sourceId = randomUUID();
    const targetId = randomUUID();
    const taskId = randomUUID();
    const revisionId = randomUUID();
    const runId = randomUUID();
    const operationId = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.platform_instances
         (id, platform_version, started_at, last_heartbeat_at)
       VALUES ('${platformInstanceId}'::uuid, '0.1.0', statement_timestamp(), statement_timestamp());
       INSERT INTO ape_dts_platform.users
         (id, normalized_username, display_name, role, status, password_kdf_algorithm,
          password_kdf_parameters, password_salt, password_hash)
       VALUES ('${administratorId}'::uuid, 'unit8-${administratorId}', 'Unit 8 administrator', 'ADMIN', 'ACTIVE', 'ARGON2ID',
               '{}'::jsonb, decode(repeat('01', 16), 'hex'), decode(repeat('02', 32), 'hex'));
       INSERT INTO ape_dts_platform.data_sources
         (id, normalized_name, source_type, configuration, state, created_by_user_id, updated_by_user_id)
       VALUES ('${sourceId}'::uuid, 'unit8-source-${sourceId}', 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', '${administratorId}'::uuid, '${administratorId}'::uuid),
              ('${targetId}'::uuid, 'unit8-target-${targetId}', 'KAFKA', '{"type":"KAFKA"}'::jsonb, 'ACTIVE', '${administratorId}'::uuid, '${administratorId}'::uuid);
       INSERT INTO ape_dts_platform.tasks
         (id, normalized_name, display_name, task_type, source_data_source_id, target_data_source_id,
          desired_state, observed_state, observability_health, recovery_disposition,
          created_by_user_id, updated_by_user_id)
       VALUES ('${taskId}'::uuid, 'unit8-task-${taskId}', 'Unit 8 task', 'KAFKA_KAFKA', '${sourceId}'::uuid, '${targetId}'::uuid,
               'RUNNING', 'RUNNING', 'HEALTHY', 'AUTO_ELIGIBLE', '${administratorId}'::uuid, '${administratorId}'::uuid);
       INSERT INTO ape_dts_platform.task_revisions
         (id, task_id, revision_number, state, configuration, created_by_user_id, updated_by_user_id)
       VALUES ('${revisionId}'::uuid, '${taskId}'::uuid, 1, 'APPLIED', '{}'::jsonb, '${administratorId}'::uuid, '${administratorId}'::uuid);
       UPDATE ape_dts_platform.tasks SET current_applied_revision_id = '${revisionId}'::uuid WHERE id = '${taskId}'::uuid;
       INSERT INTO ape_dts_platform.task_operations
         (id, task_id, action, idempotency_key, target_revision_id, state)
       VALUES ('${operationId}'::uuid, '${taskId}'::uuid, 'EMERGENCY_KILL',
               'unit8-${operationId}', '${revisionId}'::uuid, 'EXECUTING');
       INSERT INTO ape_dts_platform.task_runs
         (id, task_id, task_revision_id, operation_id, status, current_stage)
       VALUES ('${runId}'::uuid, '${taskId}'::uuid, '${revisionId}'::uuid,
               '${operationId}'::uuid, 'RUNNING', 'RUNNING');
       UPDATE ape_dts_platform.task_operations SET run_id = '${runId}'::uuid
        WHERE id = '${operationId}'::uuid`,
    );

    const insertEmergency = `INSERT INTO ape_dts_platform.emergency_runtime_events
      (event_id, platform_instance_id, task_id, run_id, revision_id, operation_id,
       action, actor_type, reason,
       unit_count, process_binding_hash, occurred_at)
      VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6::uuid,
              'EMERGENCY_FORCE_KILL_REQUESTED', 'DEPLOYMENT_ADMIN', $7,
              1, $8, statement_timestamp())`;
    await expect(
      pool.query(insertEmergency, [
        randomUUID(),
        platformInstanceId,
        taskId,
        runId,
        revisionId,
        operationId,
        null,
        'a'.repeat(64),
      ]),
    ).rejects.toMatchObject({ code: '23514', constraint: 'emergency_runtime_event_reason' });
    await expect(
      pool.query(insertEmergency, [
        randomUUID(),
        platformInstanceId,
        taskId,
        runId,
        revisionId,
        operationId,
        'operator confirmed force kill',
        'not-a-binding-hash',
      ]),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('reads all seven persisted recovery stages and applies a capacity selection transactionally', async () => {
    const selectedCandidate = await createRuntimeFixture();
    const blockedCandidate = await createRuntimeFixture();
    const unrelated = await createRuntimeFixture();
    const platformInstanceId = randomUUID();
    const sessionId = randomUUID();
    const actorUserId = randomUUID();
    const stepIds = Array.from({ length: 7 }, () => randomUUID());
    await pool.query(
      `INSERT INTO ape_dts_platform.platform_instances
         (id, platform_version, started_at, last_heartbeat_at)
       VALUES ('${platformInstanceId}'::uuid, '0.1.0', statement_timestamp(), statement_timestamp());
       INSERT INTO ape_dts_platform.recovery_sessions (id, platform_instance_id, state)
       VALUES ('${sessionId}'::uuid, '${platformInstanceId}'::uuid, 'PAUSED');
       UPDATE ape_dts_platform.platform_instances SET current_recovery_session_id = '${sessionId}'::uuid WHERE id = '${platformInstanceId}'::uuid;
       INSERT INTO ape_dts_platform.recovery_steps
         (id, recovery_session_id, step_type, position, status, progress)
       SELECT step_id, '${sessionId}'::uuid, step_type, position,
              CASE WHEN step_type = 'PLAN_CAPACITY' THEN 'PAUSED' ELSE 'SUCCEEDED' END,
              jsonb_build_object('position', position)
       FROM unnest(
         ARRAY['${stepIds.join("','")}']::uuid[],
         ARRAY['LOAD_METADATA','UNLOCK_KEYS','ENSURE_ARTIFACTS','VERIFY_CONTINUITY',
               'PLAN_CAPACITY','RESTORE_BATCHES','READY_OBSERVABILITY']
       ) WITH ORDINALITY AS steps(step_id, step_type, position);
       INSERT INTO ape_dts_platform.users
         (id, normalized_username, display_name, role, status, password_kdf_algorithm,
          password_kdf_parameters, password_salt, password_hash)
       VALUES ('${actorUserId}'::uuid, 'unit8-recovery-${actorUserId}', 'Recovery actor', 'ADMIN', 'ACTIVE', 'ARGON2ID', '{}',
               decode(repeat('03', 16), 'hex'), decode(repeat('04', 32), 'hex'))
       ON CONFLICT (id) DO NOTHING;
       INSERT INTO ape_dts_platform.platform_settings (singleton_id, max_runtime_units)
       VALUES (1, 2)
       ON CONFLICT (singleton_id) DO UPDATE SET max_runtime_units = EXCLUDED.max_runtime_units`,
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.task_run_units (id, task_id, unit_key, status, run_id)
       VALUES ($1::uuid, $2::uuid, 'partition:0', 'BLOCKED', $3::uuid),
              ($4::uuid, $5::uuid, 'partition:0', 'BLOCKED', $6::uuid),
              ($7::uuid, $8::uuid, 'partition:0', 'RUNNING', $9::uuid)`,
      [
        randomUUID(),
        selectedCandidate.taskId,
        selectedCandidate.runId,
        randomUUID(),
        blockedCandidate.taskId,
        blockedCandidate.runId,
        randomUUID(),
        unrelated.taskId,
        unrelated.runId,
      ],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.capacity_reservations
         (id, task_id, task_revision_id, unit_key, state, run_id)
       VALUES ($1::uuid, $2::uuid, $3::uuid, 'partition:0', 'ACTIVE', $4::uuid),
              ($5::uuid, $6::uuid, $7::uuid, 'partition:0', 'RESERVED', $8::uuid),
              ($9::uuid, $10::uuid, $11::uuid, 'partition:0', 'ACTIVE', $12::uuid)`,
      [
        randomUUID(),
        selectedCandidate.taskId,
        selectedCandidate.revisionId,
        selectedCandidate.runId,
        randomUUID(),
        blockedCandidate.taskId,
        blockedCandidate.revisionId,
        blockedCandidate.runId,
        randomUUID(),
        unrelated.taskId,
        unrelated.revisionId,
        unrelated.runId,
      ],
    );
    await pool.query(
      `UPDATE ape_dts_platform.tasks
          SET recovery_disposition = 'MANUAL_REQUIRED'
        WHERE id = $1::uuid`,
      [unrelated.taskId],
    );

    const database = new PlatformDatabaseService();
    const bundle = await loadMigrationBundle();
    await database.inspectAndUse(
      {
        database: databaseName,
        host: '127.0.0.1',
        password,
        port,
        tls: { mode: 'DISABLE' },
        username: role,
      },
      bundle.manifest,
    );
    const store = new PostgresRecoveryStore(database);
    try {
      const snapshot = await store.getCurrentSession(platformInstanceId);
      expect(snapshot).toMatchObject({ id: sessionId, state: 'PAUSED' });
      expect(snapshot?.steps.map((step) => step.stepType)).toEqual([
        'LOAD_METADATA',
        'UNLOCK_KEYS',
        'ENSURE_ARTIFACTS',
        'VERIFY_CONTINUITY',
        'PLAN_CAPACITY',
        'RESTORE_BATCHES',
        'READY_OBSERVABILITY',
      ]);
      expect(snapshot?.steps[4]).toMatchObject({
        progress: { position: 5 },
        status: 'PAUSED',
      });
      await expect(store.capacity(snapshot!.candidates)).resolves.toMatchObject({
        kind: 'READY',
        limit: 2,
        retained: 2,
        used: 3,
      });

      await expect(
        store.applyCapacitySelection(
          sessionId,
          [selectedCandidate.taskId],
          snapshot!.rowVersion,
          actorUserId,
        ),
      ).resolves.toMatchObject({
        kind: 'APPLIED',
        candidates: [expect.objectContaining({ taskId: selectedCandidate.taskId, units: 1 })],
      });
      await expect(store.getSession(sessionId)).resolves.toMatchObject({
        state: 'RUNNING',
        steps: expect.arrayContaining([
          expect.objectContaining({ status: 'SUCCEEDED', stepType: 'PLAN_CAPACITY' }),
        ]),
      });
      const facts = await pool.query<{
        recovery_disposition: string;
        reservation_state: string;
        task_id: string;
      }>(
        `SELECT task.id AS task_id, task.recovery_disposition,
                reservation.state AS reservation_state
           FROM ape_dts_platform.tasks AS task
           JOIN ape_dts_platform.capacity_reservations AS reservation
             ON reservation.task_id = task.id
          WHERE task.id = ANY($1::uuid[])
          ORDER BY task.id`,
        [[selectedCandidate.taskId, blockedCandidate.taskId, unrelated.taskId]],
      );
      const byTask = new Map(facts.rows.map((row) => [row.task_id, row]));
      expect(byTask.get(selectedCandidate.taskId)).toMatchObject({
        recovery_disposition: 'AUTO_ELIGIBLE',
        reservation_state: 'ACTIVE',
      });
      expect(byTask.get(blockedCandidate.taskId)).toMatchObject({
        recovery_disposition: 'CAPACITY_BLOCKED',
        reservation_state: 'RELEASED',
      });
      expect(byTask.get(unrelated.taskId)).toMatchObject({
        recovery_disposition: 'MANUAL_REQUIRED',
        reservation_state: 'ACTIVE',
      });
    } finally {
      await database.onApplicationShutdown();
    }
  });

  it('keeps pending facts on transaction failure and retries the same event_id idempotently', async () => {
    const fixture = await createRuntimeFixture();
    const database = new PlatformDatabaseService();
    const bundle = await loadMigrationBundle();
    await database.inspectAndUse(
      {
        database: databaseName,
        host: '127.0.0.1',
        password,
        port,
        tls: { mode: 'DISABLE' },
        username: role,
      },
      bundle.manifest,
    );
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    const availability = new MetadataAvailabilityService(database, readiness);
    const pending = new EmergencyPendingEventQueue(8, { write: () => undefined });
    const runtime = new IntegrationRuntimeReconciler();
    const reconciliation = new EmergencyReconciliationService(
      availability,
      database,
      new AuditLedgerService(database),
      new BootstrapStateService(fixture.platformInstanceId),
      pending,
      runtime as never,
    );
    try {
      const incomplete = {
        ...confirmedEvent(fixture),
        operationId: undefined,
      } as EmergencyStructuredLogEvent;
      expect(pending.enqueue(incomplete)).toBe(true);
      await expect(reconciliation.reconcile()).rejects.toThrow(
        'EMERGENCY_RECONCILIATION_FACT_INCOMPLETE',
      );
      expect(pending.snapshot()).toHaveLength(1);

      pending.acknowledgeThrough(pending.snapshot()[0]!.sequence);
      const eventId = randomUUID();
      const event = confirmedEvent(fixture, eventId);
      pending.enqueue(event);
      await reconciliation.reconcile();
      expect(pending.snapshot()).toEqual([]);
      pending.enqueue(event);
      await reconciliation.reconcile();
      expect(pending.snapshot()).toEqual([]);
      await expect(
        pool.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM ape_dts_platform.emergency_runtime_events WHERE event_id = $1',
          [eventId],
        ),
      ).resolves.toMatchObject({ rows: [{ count: '1' }] });
    } finally {
      await database.onApplicationShutdown();
    }
  });

  it('flushes the outage fact after a real PostgreSQL probe and reconciliation', async () => {
    const fixture = await createRuntimeFixture();
    const database = new PlatformDatabaseService();
    const bundle = await loadMigrationBundle();
    await database.inspectAndUse(
      {
        database: databaseName,
        host: '127.0.0.1',
        password,
        port,
        tls: { mode: 'DISABLE' },
        username: role,
      },
      bundle.manifest,
    );
    const readiness = new PlatformReadinessService();
    readiness.markReady();
    const availability = new MetadataAvailabilityService(database, readiness);
    const pending = new EmergencyPendingEventQueue(8, { write: () => undefined });
    const runtime = new IntegrationRuntimeReconciler();
    const reconciliation = new EmergencyReconciliationService(
      availability,
      database,
      new AuditLedgerService(database),
      new BootstrapStateService(fixture.platformInstanceId),
      pending,
      runtime as never,
    );
    reconciliation.onModuleInit();
    try {
      expect(availability.reportFailure({ code: '08006' }, 'unit8_integration')).toBe(true);
      const outage = availability.pendingOutageFact();
      expect(outage).not.toBeNull();
      await availability.ensureManagementAvailable();
      expect(readiness.getPhase()).toBe('READY');
      expect(availability.pendingOutageFact()).toBeNull();
      await expect(
        pool.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM ape_dts_platform.metadata_outage_events WHERE id = $1',
          [outage!.eventId],
        ),
      ).resolves.toMatchObject({ rows: [{ count: '1' }] });
      expect(runtime.calls).toBe(1);
    } finally {
      reconciliation.onModuleDestroy();
      await database.onApplicationShutdown();
    }
  });

  it('only acknowledges STOPPED for the exact run, operation and desired-state binding', async () => {
    const database = new PlatformDatabaseService();
    const bundle = await loadMigrationBundle();
    await database.inspectAndUse(
      {
        database: databaseName,
        host: '127.0.0.1',
        password,
        port,
        tls: { mode: 'DISABLE' },
        username: role,
      },
      bundle.manifest,
    );
    try {
      for (const mutation of ['NONE', 'NEW_RUN', 'NEW_OPERATION', 'DESIRED_STOPPED'] as const) {
        const fixture = await createRuntimeFixture();
        if (mutation === 'NEW_RUN') {
          await pool.query(
            `INSERT INTO ape_dts_platform.task_runs
               (id, task_id, task_revision_id, platform_instance_id, process_binding_hash, status, current_stage)
             VALUES ($1, $2, $3, $4, $5, 'RUNNING', 'RUNNING')`,
            [
              randomUUID(),
              fixture.taskId,
              fixture.revisionId,
              fixture.platformInstanceId,
              'b'.repeat(64),
            ],
          );
        } else if (mutation === 'NEW_OPERATION') {
          await pool.query(
            `UPDATE ape_dts_platform.task_operations SET state = 'SUCCEEDED' WHERE id = $1`,
            [fixture.operationId],
          );
          await pool.query(
            `INSERT INTO ape_dts_platform.task_operations
               (id, task_id, action, idempotency_key, target_revision_id, state)
             VALUES ($1, $2, 'STOP', $3, $4, 'EXECUTING')`,
            [randomUUID(), fixture.taskId, randomUUID(), fixture.revisionId],
          );
        } else if (mutation === 'DESIRED_STOPPED') {
          await pool.query(
            `UPDATE ape_dts_platform.tasks SET desired_state = 'STOPPED', observed_state = 'STOPPING'
              WHERE id = $1`,
            [fixture.taskId],
          );
        }
        const pending = new EmergencyPendingEventQueue(2, { write: () => undefined });
        pending.enqueue(confirmedEvent(fixture));
        const reconciliation = new EmergencyReconciliationService(
          new MetadataAvailabilityService(database, new PlatformReadinessService()),
          database,
          new AuditLedgerService(database),
          new BootstrapStateService(fixture.platformInstanceId),
          pending,
          new IntegrationRuntimeReconciler() as never,
        );
        await reconciliation.reconcile();
        const persisted = await pool.query<{ reconciliation_code: string }>(
          `SELECT reconciliation_code FROM ape_dts_platform.emergency_runtime_events
            WHERE task_id = $1 ORDER BY created_at DESC LIMIT 1`,
          [fixture.taskId],
        );
        expect(persisted.rows[0]?.reconciliation_code).toBe(
          mutation === 'NONE' ? 'CONFIRMED_APPLIED' : 'STALE_OR_BINDING_MISMATCH',
        );
        const task = await pool.query<{ desired_state: string }>(
          'SELECT desired_state FROM ape_dts_platform.tasks WHERE id = $1',
          [fixture.taskId],
        );
        expect(task.rows[0]?.desired_state).toBe(
          mutation === 'NONE' || mutation === 'DESIRED_STOPPED' ? 'STOPPED' : 'RUNNING',
        );
      }
    } finally {
      await database.onApplicationShutdown();
    }
  });

  it('reads and downloads real LOCAL log facts without exposing policy-deleted content', async () => {
    const schema = await pool.query<{ tasks: string | null }>(
      "SELECT to_regclass('ape_dts_platform.tasks')::text AS tasks",
    );
    if (schema.rows[0]!.tasks === null) {
      await new MigrationRunner(new PgMigrationStore(pool)).migrate(
        await loadMigrationBundle(),
        randomUUID(),
        { expectedState: 'ABSENT' },
      );
    }
    const fixture = await createRuntimeFixture();
    const unitId = randomUUID();
    const profileId = randomUUID();
    const administrator = await pool.query<{ id: string }>(
      `SELECT created_by_user_id AS id FROM ape_dts_platform.tasks WHERE id=$1`,
      [fixture.taskId],
    );
    const actorId = administrator.rows[0]!.id;
    const logRoot = await mkdtemp(path.join(tmpdir(), 'scs-unit8-log-read-'));
    const objects = new LocalLogSegmentObjectStore(logRoot);
    const good = Buffer.from(
      `${JSON.stringify({ continuation: 0, continues: false, occurredAt: '2026-09-08T00:00:02.000Z', payload: 'INFO visible-two', stream: 'STDOUT', unitId })}\n` +
        `${JSON.stringify({ continuation: 0, continues: false, occurredAt: '2026-09-08T00:00:04.000Z', payload: 'ERROR visible-four', stream: 'STDOUT', unitId })}\n`,
    );
    const damaged = Buffer.from('damaged-after-metadata');
    const deleted = Buffer.from('must-never-be-downloaded');
    const keys = [
      'runs/integration/good.log',
      'runs/integration/damaged.log',
      'runs/integration/deleted.log',
    ];
    await objects.put({ content: good, objectKey: keys[0]! });
    await objects.put({ content: damaged, objectKey: keys[1]! });
    await objects.put({ content: deleted, objectKey: keys[2]! });
    const objectIds = [randomUUID(), randomUUID(), randomUUID()];
    const segmentIds = [randomUUID(), randomUUID(), randomUUID()];
    await queryBatch(
      `INSERT INTO ape_dts_platform.task_run_units (id,task_id,unit_key,status,run_id) VALUES ($1,$2,'partition:0','RUNNING',$3);
       INSERT INTO ape_dts_platform.storage_profiles (id,profile_key,display_name,version,driver,config,state,created_by_user_id,activated_at)
       VALUES ($4,$5,'Unit 8 log profile',1,'LOCAL','{"relative_prefix":""}','READ_ONLY_REFERENCED',$6,statement_timestamp());
       INSERT INTO ape_dts_platform.stored_objects (id,storage_profile_id,kind,object_key,size_bytes,sha256,content_type,state,created_by_actor_type,created_by_user_id,available_at)
       VALUES ($7,$4,'LOG_SEGMENT',$8,$9,$10,'application/x-ndjson','AVAILABLE','USER',$6,statement_timestamp()),
              ($11,$4,'LOG_SEGMENT',$12,$13,$14,'application/x-ndjson','AVAILABLE','USER',$6,statement_timestamp()),
              ($15,$4,'LOG_SEGMENT',$16,$17,$18,'application/x-ndjson','AVAILABLE','USER',$6,statement_timestamp());
       UPDATE ape_dts_platform.task_runs SET log_storage_profile_id=$4 WHERE id=$3;
       INSERT INTO ape_dts_platform.log_segments (id,run_id,run_unit_id,stream,sequence,started_at,ended_at,stored_object_id,byte_size,sha256,state,deleted_at,deleted_reason)
       VALUES ($19,$3,$1,'STDOUT',1,'2026-09-08T00:00:01Z','2026-09-08T00:00:04Z',$7,$9,$10,'AVAILABLE',NULL,NULL),
              ($20,$3,$1,'STDERR',1,'2026-09-08T00:00:02Z','2026-09-08T00:00:03Z',$11,$13,$14,'AVAILABLE',NULL,NULL),
              ($21,$3,$1,'STDOUT',2,'2026-09-08T00:00:04Z','2026-09-08T00:00:05Z',$15,$17,$18,'DELETED','2026-09-08T00:00:05Z','CAPACITY_ROLLOVER');
       INSERT INTO ape_dts_platform.log_segment_deletion_requests (log_segment_id,run_id,stored_object_id,reason,deleted_at,completed_at)
       VALUES ($21,$3,$15,'CAPACITY_ROLLOVER','2026-09-08T00:00:05Z','2026-09-08T00:00:05Z');
       INSERT INTO ape_dts_platform.log_gaps (id,run_id,run_unit_id,reason,lost_bytes,occurred_at)
       VALUES ($22,$3,$1,'STREAM_ERROR',NULL,'2026-09-08T00:00:01Z')`,
      [
        unitId,
        fixture.taskId,
        fixture.runId,
        profileId,
        `unit8-log-${profileId}`,
        actorId,
        objectIds[0],
        keys[0],
        good.length,
        Buffer.from(segmentSha256(good), 'hex'),
        objectIds[1],
        keys[1],
        damaged.length,
        Buffer.alloc(32, 9),
        objectIds[2],
        keys[2],
        deleted.length,
        Buffer.from(segmentSha256(deleted), 'hex'),
        segmentIds[0],
        segmentIds[1],
        segmentIds[2],
        randomUUID(),
      ],
    );
    const database = new PlatformDatabaseService();
    const bundle = await loadMigrationBundle();
    await database.inspectAndUse(
      {
        database: databaseName,
        host: '127.0.0.1',
        password,
        port,
        tls: { mode: 'DISABLE' },
        username: role,
      },
      bundle.manifest,
    );
    const service = new RuntimeLogReadService(database, logRoot);
    const reads = new RuntimeReadService(database);
    const actor = {
      displayName: 'Unit 8 user',
      id: actorId,
      role: 'ADMIN' as const,
      status: 'ACTIVE' as const,
      username: 'unit8',
    };
    let app: NestFastifyApplication | undefined;
    try {
      const oldRunId = randomUUID();
      const purgedRunId = randomUUID();
      const unrelated = await createRuntimeFixture();
      await pool.query(
        `INSERT INTO ape_dts_platform.task_runs
          (id,task_id,task_revision_id,status,current_stage,started_at,ended_at,record_purged_at)
         VALUES ($1,$3,$4,'STOPPED','CDC',now()-interval '10 days',now()-interval '9 days',NULL),
                ($2,$3,$4,'STOPPED','CDC',now()-interval '21 days',now()-interval '20 days',now())`,
        [oldRunId, purgedRunId, fixture.taskId, fixture.revisionId],
      );
      const history = await reads.listRuns(actor, fixture.taskId);
      expect(history.runs.map((run) => run.id)).toEqual([fixture.runId, oldRunId]);
      expect(history.runs.some((run) => run.id === unrelated.runId || run.id === purgedRunId)).toBe(
        false,
      );
      expect((await reads.get(actor, fixture.taskId)).run?.id).toBe(fixture.runId);
      await expect(
        reads.listRuns({ ...actor, role: 'DEVELOPER' }, fixture.taskId),
      ).rejects.toBeInstanceOf(RuntimeReadNotFoundError);
      const listed = await service.list(actor, fixture.taskId, fixture.runId, { limit: 50 });
      expect(
        listed.facts.map((fact) => (fact.kind === 'LINE' ? fact.payload : fact.reason)),
      ).toEqual([
        'STREAM_ERROR',
        'INFO visible-two',
        'INTEGRITY_FAILED',
        'ERROR visible-four',
        'CAPACITY_ROLLOVER',
      ]);
      const filtered = await service.list(actor, fixture.taskId, fixture.runId, {
        limit: 50,
        stream: 'STDERR',
        unitKey: 'partition:0',
      });
      expect(filtered.facts).toEqual([
        expect.objectContaining({ kind: 'GAP', reason: 'STREAM_ERROR', stream: null }),
        expect.objectContaining({ kind: 'GAP', reason: 'INTEGRITY_FAILED', stream: 'STDERR' }),
      ]);
      const tail = await service.list(actor, fixture.taskId, fixture.runId, {
        limit: 3,
        order: 'DESC',
      });
      expect(tail.limited).toBe(true);
      expect(tail.facts.map((fact) => (fact.kind === 'LINE' ? fact.payload : fact.reason))).toEqual(
        ['INTEGRITY_FAILED', 'ERROR visible-four', 'CAPACITY_ROLLOVER'],
      );
      const errors = await service.list(actor, fixture.taskId, fixture.runId, {
        limit: 50,
        order: 'DESC',
        level: 'ERROR',
      });
      expect(
        errors.facts.map((fact) => (fact.kind === 'LINE' ? fact.payload : fact.reason)),
      ).toEqual(['STREAM_ERROR', 'INTEGRITY_FAILED', 'ERROR visible-four', 'CAPACITY_ROLLOVER']);
      const module = await Test.createTestingModule({
        controllers: [RuntimeController],
        providers: [
          { provide: RuntimeSupervisorService, useValue: {} },
          { provide: RuntimeReadService, useValue: reads },
          { provide: RuntimeLogReadService, useValue: service },
          { provide: LogPipelineCoordinatorService, useValue: { retryStorage: async () => ({}) } },
        ],
      })
        .overrideGuard(SessionAuthGuard)
        .useValue({
          canActivate(context: { switchToHttp(): { getRequest(): Record<string, unknown> } }) {
            context.switchToHttp().getRequest().authContext = {
              sessionGeneration: '1',
              sessionId: randomUUID(),
              sourceFingerprint: Buffer.alloc(32),
              user: actor,
            };
            return true;
          },
        })
        .overrideGuard(CsrfGuard)
        .useValue({ canActivate: () => true })
        .compile();
      app = module.createNestApplication<NestFastifyApplication>(
        new FastifyAdapter({ logger: false }),
      );
      app.setGlobalPrefix('api');
      await app.init();
      const historyResponse = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/runs`,
      });
      expect(historyResponse.statusCode).toBe(200);
      expect(historyResponse.json().runs.map((run: { id: string }) => run.id)).toEqual([
        fixture.runId,
        oldRunId,
      ]);
      const missingHistory = await app.inject({
        method: 'GET',
        url: `/api/tasks/${randomUUID()}/runtime/runs`,
      });
      expect(missingHistory.statusCode).toBe(404);
      const tailResponse = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/${fixture.runId}/logs?order=DESC&level=ERROR&limit=2`,
      });
      expect(tailResponse.statusCode).toBe(200);
      expect(
        tailResponse
          .json()
          .facts.map((fact: { payload?: string; reason?: string }) => fact.payload ?? fact.reason),
      ).toEqual(['ERROR visible-four', 'CAPACITY_ROLLOVER']);
      const response = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/${fixture.runId}/logs/download`,
      });
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toContain('text/plain');
      expect(response.headers['x-content-type-options']).toBe('nosniff');
      expect(response.headers['content-disposition']).toBe(
        `attachment; filename="runtime-${fixture.runId}.log"`,
      );
      expect(response.body).toContain('visible-two');
      expect(response.body).toContain('INTEGRITY_FAILED');
      expect(response.body).toContain('CAPACITY_ROLLOVER');
      expect(response.body).not.toContain('must-never-be-downloaded');
      const errorDownload = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/${fixture.runId}/logs/download?level=ERROR`,
      });
      expect(errorDownload.statusCode).toBe(200);
      expect(errorDownload.body).toContain('ERROR visible-four');
      expect(errorDownload.body).not.toContain('INFO visible-two');
      const continuationUnit = randomUUID();
      await pool.query(
        `INSERT INTO ape_dts_platform.task_run_units (id,task_id,unit_key,status,run_id) VALUES ($1,$2,'continuation','RUNNING',$3)`,
        [continuationUnit, fixture.taskId, fixture.runId],
      );
      for (const [index, payload] of ['ERROR split head', 'split error tail'].entries()) {
        const timestamp = `2026-09-08T00:00:0${6 + index}.000Z`;
        const content = Buffer.from(
          `${JSON.stringify({ continuation: index, continues: index === 0, occurredAt: timestamp, payload, stream: 'STDOUT', unitId: continuationUnit })}\n`,
        );
        const key = `runs/integration/continuation-${index}.log`;
        await objects.put({ objectKey: key, content });
        const objectId = randomUUID();
        await pool.query(
          `INSERT INTO ape_dts_platform.stored_objects (id,storage_profile_id,kind,object_key,size_bytes,sha256,content_type,state,created_by_actor_type,created_by_user_id,available_at) VALUES ($1,$2,'LOG_SEGMENT',$3,$4,$5,'application/x-ndjson','AVAILABLE','USER',$6,statement_timestamp())`,
          [
            objectId,
            profileId,
            key,
            content.length,
            Buffer.from(segmentSha256(content), 'hex'),
            actorId,
          ],
        );
        await pool.query(
          `INSERT INTO ape_dts_platform.log_segments (id,run_id,run_unit_id,stream,sequence,started_at,ended_at,stored_object_id,byte_size,sha256,state) VALUES ($1,$2,$3,'STDOUT',$4,$5,$5,$6,$7,$8,'AVAILABLE')`,
          [
            randomUUID(),
            fixture.runId,
            continuationUnit,
            4 + index,
            timestamp,
            objectId,
            content.length,
            Buffer.from(segmentSha256(content), 'hex'),
          ],
        );
      }
      const continuationResponse = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/${fixture.runId}/logs?order=DESC&level=ERROR&unitKey=continuation`,
      });
      expect(continuationResponse.statusCode).toBe(200);
      expect(
        continuationResponse.json().facts.map((fact: { payload?: string }) => fact.payload),
      ).toEqual(['ERROR split head', 'split error tail']);
      const continuationDownload = await app.inject({
        method: 'GET',
        url: `/api/tasks/${fixture.taskId}/runtime/${fixture.runId}/logs/download?level=ERROR&unitKey=continuation`,
      });
      expect(continuationDownload.statusCode).toBe(200);
      expect(continuationDownload.body).toContain('split error tail');
    } finally {
      await app?.close();
      await database.onApplicationShutdown();
      await rm(logRoot, { force: true, recursive: true });
    }
  });

  it('enforces a run log limit from PostgreSQL segments created before the current pipeline', async () => {
    const fixture = await createRuntimeFixture();
    const unitId = randomUUID();
    const profileId = randomUUID();
    const administrator = await pool.query<{ id: string }>(
      `SELECT created_by_user_id AS id FROM ape_dts_platform.tasks WHERE id=$1::uuid`,
      [fixture.taskId],
    );
    const logRoot = await mkdtemp(path.join(tmpdir(), 'scs-unit8-log-capacity-'));
    await pool.query(
      `INSERT INTO ape_dts_platform.task_run_units (id,task_id,unit_key,status,run_id)
       VALUES ($1::uuid,$2::uuid,'partition:capacity','RUNNING',$3::uuid)`,
      [unitId, fixture.taskId, fixture.runId],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.storage_profiles
         (id,profile_key,display_name,version,driver,config,state,created_by_user_id,activated_at)
       VALUES ($1::uuid,$2,'Unit 8 capacity profile',1,'LOCAL','{"relative_prefix":""}',
               'ACTIVE_WRITE',$3::uuid,statement_timestamp())`,
      [profileId, `unit8-capacity-${profileId}`, administrator.rows[0]!.id],
    );
    const database = new PlatformDatabaseService();
    (database as unknown as { pool: Pool }).pool = pool;
    const store = new PostgresLogSegmentStore(database, logRoot);
    const firstProcess = new LogPipeline({
      maxBufferedBytes: 64,
      maxRunBytes: 6,
      maxSegmentBytes: 3,
      retryDelayMilliseconds: 60_000,
      store,
      structuredRecords: false,
    });
    try {
      firstProcess.append({
        chunk: `abcdefghi${'x'.repeat(1024)}`,
        runId: fixture.runId,
        stream: 'STDOUT',
        unitId,
      });
      await expect(firstProcess.settleRun(fixture.runId)).resolves.toMatchObject({
        recoveryState: 'NONE',
      });
      const first = await pool.query<{ available: string; bytes: string; deleted: string }>(
        `SELECT count(*) FILTER (WHERE state='AVAILABLE')::text AS available,
                count(*) FILTER (WHERE state='DELETED')::text AS deleted,
                COALESCE(sum(byte_size) FILTER (WHERE state='AVAILABLE'),0)::text AS bytes
           FROM ape_dts_platform.log_segments
          WHERE run_id=$1::uuid`,
        [fixture.runId],
      );
      expect(first.rows[0]).toEqual({ available: '2', bytes: '6', deleted: '1' });

      // Simulate a process restart: the new coordinator has no in-memory runSegments map.
      const enforcement = await store.enforceAllRunCapacities(3);
      expect(enforcement.failures).toBe(0);
      expect(enforcement.affectedRuns).toBeGreaterThanOrEqual(1);
      expect(enforcement.deletedSegments).toBeGreaterThanOrEqual(1);
      const restarted = await pool.query<{ available: string; bytes: string; deleted: string }>(
        `SELECT count(*) FILTER (WHERE state='AVAILABLE')::text AS available,
                count(*) FILTER (WHERE state='DELETED')::text AS deleted,
                COALESCE(sum(byte_size) FILTER (WHERE state='AVAILABLE'),0)::text AS bytes
           FROM ape_dts_platform.log_segments
          WHERE run_id=$1::uuid`,
        [fixture.runId],
      );
      expect(restarted.rows[0]).toEqual({ available: '1', bytes: '3', deleted: '2' });
      await expect(
        pool.query(`SELECT truncated,has_gap FROM ape_dts_platform.task_runs WHERE id=$1`, [
          fixture.runId,
        ]),
      ).resolves.toMatchObject({
        rows: [expect.objectContaining({ has_gap: true, truncated: true })],
      });
    } finally {
      firstProcess.finalizeRun(fixture.runId);
      await rm(logRoot, { force: true, recursive: true });
    }
  });
});
