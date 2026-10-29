import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { LogRetentionService } from '../../../apps/api/src/log-lifecycle/log-retention.service.js';
import type { PostgresLogSegmentStore } from '../../../apps/api/src/storage/postgres-log-segment-store.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';

const suffix = `${process.pid}_${Date.now()}`;
const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const role = `scs_log_lifecycle_${suffix}`;
const databaseName = `scs_log_lifecycle_${suffix}`;
const password = randomBytes(24).toString('base64url');
let pool: Pool;

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

beforeAll(async () => {
  expect(docker(['inspect', '--format', '{{.State.Running}}', container])).toBe('true');
  const binding = docker(['port', container, '5432/tcp']);
  const port = Number(binding.slice(binding.lastIndexOf(':') + 1));
  administer(
    `CREATE ROLE "${role}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${databaseName}" OWNER "${role}";`,
  );
  pool = new Pool({ database: databaseName, host: '127.0.0.1', password, port, user: role });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
});

afterAll(async () => {
  await pool?.end();
  try {
    administer(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${databaseName}' AND pid<>pg_backend_pid(); DROP DATABASE IF EXISTS "${databaseName}"; DROP ROLE IF EXISTS "${role}";`,
    );
  } catch (error) {
    process.stderr.write(
      `failed to clean up log lifecycle PostgreSQL resources: ${String(error)}\n`,
    );
  }
});

describe.sequential('log lifecycle PostgreSQL integration', () => {
  it('tombstones only an eligible old run and preserves its operation, task and audit history', async () => {
    const ids = {
      activeRun: randomUUID(),
      administrator: randomUUID(),
      blockedRun: randomUUID(),
      capacity: randomUUID(),
      eligibleRun: randomUUID(),
      operation: randomUUID(),
      revision: randomUUID(),
      source: randomUUID(),
      target: randomUUID(),
      task: randomUUID(),
    };
    await pool.query(
      `INSERT INTO ape_dts_platform.platform_settings (singleton_id,log_retention_days,run_retention_days) VALUES (1,500,365);
       INSERT INTO ape_dts_platform.users
         (id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash)
       VALUES ('${ids.administrator}','log-lifecycle-admin','Administrator','ADMIN','ACTIVE','ARGON2ID','{}',decode(repeat('01',16),'hex'),decode(repeat('02',32),'hex'));
       INSERT INTO ape_dts_platform.data_sources
         (id,normalized_name,source_type,configuration,state,created_by_user_id,updated_by_user_id)
       VALUES ('${ids.source}','log-lifecycle-source','KAFKA','{"type":"KAFKA"}','ACTIVE','${ids.administrator}','${ids.administrator}'),
              ('${ids.target}','log-lifecycle-target','KAFKA','{"type":"KAFKA"}','ACTIVE','${ids.administrator}','${ids.administrator}');
       INSERT INTO ape_dts_platform.tasks
         (id,normalized_name,display_name,task_type,source_data_source_id,target_data_source_id,desired_state,observed_state,observability_health,recovery_disposition,created_by_user_id,updated_by_user_id)
       VALUES ('${ids.task}','log-lifecycle-task','Lifecycle task','KAFKA_KAFKA','${ids.source}','${ids.target}','STOPPED','STOPPED','HEALTHY','AUTO_ELIGIBLE','${ids.administrator}','${ids.administrator}');
       INSERT INTO ape_dts_platform.task_revisions
         (id,task_id,revision_number,state,configuration,created_by_user_id,updated_by_user_id)
       VALUES ('${ids.revision}','${ids.task}',1,'APPLIED','{}','${ids.administrator}','${ids.administrator}');
       INSERT INTO ape_dts_platform.task_operations (id,task_id,action,idempotency_key,target_revision_id,state,completed_at)
       VALUES ('${ids.operation}','${ids.task}','STOP','lifecycle-stop','${ids.revision}','SUCCEEDED',statement_timestamp()-interval '400 days');
       INSERT INTO ape_dts_platform.task_runs
         (id,task_id,task_revision_id,operation_id,status,current_stage,started_at,ended_at)
       VALUES ('${ids.eligibleRun}','${ids.task}','${ids.revision}','${ids.operation}','STOPPED','STOPPED',statement_timestamp()-interval '401 days',statement_timestamp()-interval '400 days'),
              ('${ids.blockedRun}','${ids.task}','${ids.revision}',NULL,'STOPPED','STOPPED',statement_timestamp()-interval '401 days',statement_timestamp()-interval '400 days'),
              ('${ids.activeRun}','${ids.task}','${ids.revision}',NULL,'RUNNING','RUNNING',statement_timestamp(),NULL);
       UPDATE ape_dts_platform.task_operations SET run_id='${ids.eligibleRun}' WHERE id='${ids.operation}';
       INSERT INTO ape_dts_platform.capacity_reservations
         (id,task_id,task_revision_id,run_id,unit_key,state)
       VALUES ('${ids.capacity}','${ids.task}','${ids.revision}','${ids.blockedRun}','blocked-unit','ACTIVE')`,
    );

    const segmentId = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.log_segments
         (id,run_id,stream,sequence,started_at,ended_at,byte_size,sha256,state)
       VALUES ($1,$2,'STDOUT',1,statement_timestamp()-interval '400 days',
               statement_timestamp()-interval '400 days',128,decode(repeat('03',32),'hex'),'AVAILABLE')`,
      [segmentId, ids.eligibleRun],
    );

    const database = new PlatformDatabaseService();
    (database as unknown as { pool: Pool }).pool = pool;
    const store = {
      deleteSegment: async () => {
        await pool.query(
          `UPDATE ape_dts_platform.log_segments
              SET state='DELETED',deleted_at=statement_timestamp(),deleted_reason='RETENTION'
            WHERE id=$1`,
          [segmentId],
        );
      },
    } as unknown as PostgresLogSegmentStore;
    const retention = new LogRetentionService(database, store);
    await expect(retention.runOnce()).resolves.toEqual({ logSegmentsDeleted: 1, runsPurged: 1 });

    const result = await pool.query<{
      active_purged: Date | null;
      blocked_purged: Date | null;
      eligible_purged: Date | null;
      operation_count: string;
      task_count: string;
    }>(
      `SELECT
         (SELECT record_purged_at FROM ape_dts_platform.task_runs WHERE id=$1) eligible_purged,
         (SELECT record_purged_at FROM ape_dts_platform.task_runs WHERE id=$2) active_purged,
         (SELECT record_purged_at FROM ape_dts_platform.task_runs WHERE id=$3) blocked_purged,
         (SELECT count(*)::text FROM ape_dts_platform.task_operations WHERE id=$4) operation_count,
         (SELECT count(*)::text FROM ape_dts_platform.tasks WHERE id=$5) task_count`,
      [ids.eligibleRun, ids.activeRun, ids.blockedRun, ids.operation, ids.task],
    );
    expect(result.rows[0]).toMatchObject({
      active_purged: null,
      blocked_purged: null,
      operation_count: '1',
      task_count: '1',
    });
    expect(result.rows[0]?.eligible_purged).toBeInstanceOf(Date);
  });
});
