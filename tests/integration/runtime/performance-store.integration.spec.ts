import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it } from 'vitest';

import { RuntimePerformanceStore } from '../../../apps/api/src/runtime/performance-store.js';
import {
  RuntimePerformanceNotFoundError,
  RuntimePerformanceReadService,
} from '../../../apps/api/src/runtime/performance-read.service.js';
import { createRuntimePerformanceCollector } from '../../../apps/api/src/runtime/performance-collection.js';
import type { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import type { RuntimeSupervisorService } from '../../../apps/api/src/runtime/runtime-supervisor.service.js';
import type { UserSummary } from '../../../packages/contracts/src/index.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const database = `scs_perf_${process.pid}_${Date.now()}`;
const role = `scs_perf_role_${process.pid}_${Date.now()}`;
const password = randomBytes(24).toString('base64url');
let pool: Pool;
const admin: UserSummary = {
  id: randomUUID(),
  username: 'admin',
  displayName: 'Admin',
  role: 'ADMIN',
  status: 'ACTIVE',
};
const databaseService = () => ({ requirePool: () => pool }) as PlatformDatabaseService;
function docker(args: string[], input?: string): string {
  return execFileSync('docker', args, { encoding: 'utf8', input, timeout: 120_000 }).trim();
}
function adminSql(sql: string): void {
  docker(
    ['exec', '-i', container, 'psql', '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1'],
    sql,
  );
}

beforeAll(async () => {
  const port = Number(docker(['port', container, '5432/tcp']).split(':').at(-1));
  adminSql(
    `CREATE ROLE "${role}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${database}" OWNER "${role}";`,
  );
  pool = new Pool({ host: '127.0.0.1', port, database, user: role, password });
  await pool.query(`CREATE SCHEMA ape_dts_platform;
    CREATE TABLE ape_dts_platform.tasks(id uuid PRIMARY KEY, deleted_at timestamptz,
      source_data_source_id uuid, target_data_source_id uuid);
    CREATE TABLE ape_dts_platform.data_source_grants(user_id uuid, data_source_id uuid, revoked_at timestamptz);
    CREATE TABLE ape_dts_platform.task_runs(id uuid PRIMARY KEY, task_id uuid, status text,
      started_at timestamptz DEFAULT now(), ended_at timestamptz, record_purged_at timestamptz);
    CREATE TABLE ape_dts_platform.task_run_units(id uuid PRIMARY KEY, task_id uuid, run_id uuid,
      status text, process_token text, process_pid integer, metrics_port integer,
      metrics_port_released_at timestamptz, started_at timestamptz, ended_at timestamptz);`);
  const sql = await readFile('packages/database/migrations/000020_runtime_performance.sql', 'utf8');
  await pool.query(sql);
});

it('requires both live data-source grants for batch and detail visibility', async () => {
  const taskId = randomUUID();
  const sourceId = randomUUID();
  const targetId = randomUUID();
  const viewer: UserSummary = { ...admin, id: randomUUID(), role: 'OPERATOR' };
  await pool.query(
    'INSERT INTO ape_dts_platform.tasks(id,source_data_source_id,target_data_source_id) VALUES ($1,$2,$3)',
    [taskId, sourceId, targetId],
  );
  const reads = new RuntimePerformanceReadService(databaseService());
  await pool.query(
    'INSERT INTO ape_dts_platform.data_source_grants(user_id,data_source_id) VALUES ($1,$2)',
    [viewer.id, sourceId],
  );
  expect((await reads.query(viewer, [taskId])).items).toEqual([]);
  await expect(reads.detail(viewer, taskId, '1h')).rejects.toBeInstanceOf(
    RuntimePerformanceNotFoundError,
  );
  await pool.query(
    'INSERT INTO ape_dts_platform.data_source_grants(user_id,data_source_id) VALUES ($1,$2)',
    [viewer.id, targetId],
  );
  expect((await reads.query(viewer, [taskId])).items).toMatchObject([{ taskId }]);
  expect((await reads.detail(viewer, taskId, '1h')).summary.taskId).toBe(taskId);
  await pool.query(
    'UPDATE ape_dts_platform.data_source_grants SET revoked_at=now() WHERE user_id=$1 AND data_source_id=$2',
    [viewer.id, sourceId],
  );
  expect((await reads.query(viewer, [taskId])).items).toEqual([]);
  await expect(reads.detail(viewer, taskId, '1h')).rejects.toBeInstanceOf(
    RuntimePerformanceNotFoundError,
  );
});

it('keeps stopped-run samples and lets an authorized user select an older run without mixing newer samples', async () => {
  const taskId = randomUUID();
  const otherTask = randomUUID();
  const oldRun = randomUUID();
  const newRun = randomUUID();
  const oldUnit = randomUUID();
  const newUnit = randomUUID();
  const at = new Date(Math.floor((Date.now() - 120_000) / 15000) * 15000);
  await new RuntimePerformanceStore(databaseService()).maintain();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1),($2)', [taskId, otherTask]);
  await pool.query(
    `INSERT INTO ape_dts_platform.task_runs(id,task_id,status,started_at,ended_at)
    VALUES ($1,$3,'STOPPED',$4::timestamptz-interval '10 minutes',$4),
           ($2,$3,'STOPPED',$4::timestamptz-interval '5 minutes',$4)`,
    [oldRun, newRun, taskId, at],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status)
    VALUES ($1,$3,$4,'STOPPED'),($2,$3,$5,'STOPPED')`,
    [oldUnit, newUnit, taskId, oldRun, newRun],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,records_per_second)
    VALUES ($1,$2,$4,$6,12),($1,$3,$5,$6,34)`,
    [taskId, oldRun, newRun, oldUnit, newUnit, at],
  );
  const reads = new RuntimePerformanceReadService(databaseService());
  const previous = await reads.detail(admin, taskId, '1h', oldRun);
  expect(previous.summary).toMatchObject({
    status: 'NOT_RUNNING',
    runId: oldRun,
    collectedAt: null,
  });
  expect(previous.points).toEqual([expect.objectContaining({ recordsPerSecond: 12, gap: false })]);
  expect(previous.runs?.map((run) => run.id)).toEqual([newRun, oldRun]);
  const latest = await reads.detail(admin, taskId, '1h');
  expect(latest.points).toEqual([expect.objectContaining({ recordsPerSecond: 34 })]);
  await expect(reads.detail(admin, otherTask, '1h', oldRun)).rejects.toBeInstanceOf(
    RuntimePerformanceNotFoundError,
  );
});

it('sums fresh units only when every running unit has a sample, without partial totals', async () => {
  const taskId = randomUUID();
  const runId = randomUUID();
  const unit1 = randomUUID();
  const unit2 = randomUUID();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [taskId]);
  await pool.query(
    "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
    [runId, taskId],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status) VALUES ($1,$3,$4,'RUNNING'),($2,$3,$4,'RUNNING')",
    [unit1, unit2, taskId, runId],
  );
  const at = new Date(Date.now() - 5_000);
  const store = new RuntimePerformanceStore(databaseService());
  // Direct metric rows exercise the read aggregation without inventing process ownership.
  await store.maintain(new Date());
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,records_per_second,bytes_per_second,sink_records,sink_bytes,queue_records,queue_bytes)
    VALUES ($1,$2,$3,$4,10,100,1000,10000,10,100)`,
    [taskId, runId, unit1, at],
  );
  const reads = new RuntimePerformanceReadService(databaseService());
  expect((await reads.query(admin, [taskId])).items[0]).toMatchObject({
    status: 'PARTIAL',
    values: { recordsPerSecond: null },
  });
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,records_per_second,bytes_per_second,sink_records,sink_bytes,queue_records,queue_bytes)
    VALUES ($1,$2,$3,$4,20,200,2000,20000,20,200)`,
    [taskId, runId, unit2, at],
  );
  expect((await reads.query(admin, [taskId])).items[0]).toMatchObject({
    status: 'FRESH',
    values: { recordsPerSecond: 30, sinkRecords: 3000, queueRecords: 30 },
  });
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,gap_reason) VALUES ($1,$2,$3,$4,'METRICS_TIMEOUT')`,
    [taskId, runId, unit2, new Date()],
  );
  expect((await reads.query(admin, [taskId])).items[0]).toMatchObject({
    status: 'PARTIAL',
    values: { recordsPerSecond: null, sinkRecords: null },
  });
});

it('uses only the latest run for summary and detail after a run switch', async () => {
  const taskId = randomUUID();
  const oldRun = randomUUID();
  const newRun = randomUUID();
  const oldUnit = randomUUID();
  const newUnit = randomUUID();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [taskId]);
  await pool.query(
    `INSERT INTO ape_dts_platform.task_runs(id,task_id,status,started_at) VALUES
    ($1,$3,'STOPPED',now()-interval '10 minutes'),($2,$3,'RUNNING',now())`,
    [oldRun, newRun, taskId],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status) VALUES
    ($1,$3,$4,'STOPPED'),($2,$3,$5,'RUNNING')`,
    [oldUnit, newUnit, taskId, oldRun, newRun],
  );
  await new RuntimePerformanceStore(databaseService()).maintain(new Date());
  const at = new Date(Date.now() - 5_000);
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,records_per_second,sink_records)
    VALUES ($1,$2,$3,$4,999,9999),($1,$5,$6,$4,7,70)`,
    [taskId, oldRun, oldUnit, at, newRun, newUnit],
  );
  const detail = await new RuntimePerformanceReadService(databaseService()).detail(
    admin,
    taskId,
    '1h',
  );
  expect(detail.summary.runId).toBe(newRun);
  expect(detail.summary.values.recordsPerSecond).toBe(7);
  expect(detail.points.some((point) => point.recordsPerSecond === 999)).toBe(false);
  expect(detail.points.some((point) => point.recordsPerSecond === 7)).toBe(true);
});

it('marks missing 1h buckets and keeps a 7d near-tail bucket after coarse aggregation', async () => {
  const taskId = randomUUID();
  const runId = randomUUID();
  const unitId = randomUUID();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [taskId]);
  await pool.query(
    "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
    [runId, taskId],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status) VALUES ($1,$2,$3,'RUNNING')",
    [unitId, taskId, runId],
  );
  await new RuntimePerformanceStore(databaseService()).maintain(new Date());
  const now = Date.now();
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_samples_high
    (task_id,run_id,unit_id,collected_at,records_per_second,sink_records)
    VALUES ($1,$2,$3,$4,1,10),($1,$2,$3,$5,2,20)`,
    [taskId, runId, unitId, new Date(now - 70_000), new Date(now - 5_000)],
  );
  const reads = new RuntimePerformanceReadService(databaseService());
  const hourly = await reads.detail(admin, taskId, '1h');
  expect(hourly.points.some((point) => point.gap && point.recordsPerSecond === null)).toBe(true);
  expect(hourly.points.some((point) => point.sinkRecords === 20)).toBe(true);
  const minute = (value: number) => new Date(Math.floor(value / 60_000) * 60_000);
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_rollups_1m
    (task_id,run_id,unit_id,bucket_start,records_per_second,sink_records,gap,sample_count)
    VALUES ($1,$2,$3,$4,1,10,false,4),($1,$2,$3,$5,2,20,false,4)`,
    [taskId, runId, unitId, minute(now - 7 * 86400_000 + 600_000), minute(now - 60_000)],
  );
  const incompleteBucket = Math.floor((now - 30 * 60_000) / 600_000) * 600_000;
  for (let index = 0; index < 9; index++) {
    await pool.query(
      `INSERT INTO ape_dts_platform.metric_rollups_1m
      (task_id,run_id,unit_id,bucket_start,records_per_second,sink_records,gap,sample_count)
      VALUES ($1,$2,$3,$4,3,30,false,4)`,
      [taskId, runId, unitId, new Date(incompleteBucket + index * 60_000)],
    );
  }
  const week = await reads.detail(admin, taskId, '7d');
  expect(week.points.length).toBeLessThanOrEqual(2000);
  const latestBucket = new Date(Math.floor((now - 60_000) / 600_000) * 600_000).toISOString();
  expect(week.points.at(-1)?.collectedAt).toBe(latestBucket);
  expect(week.points.at(-1)?.gap).toBe(true); // incomplete latest coarse bucket is explicit, never treated as full throughput
  expect(
    week.points.find((point) => point.collectedAt === new Date(incompleteBucket).toISOString()),
  ).toMatchObject({
    gap: true,
    recordsPerSecond: null,
  });
});

it('does not demand an ended snapshot unit in a later CDC trend bucket', async () => {
  const taskId = randomUUID();
  const runId = randomUUID();
  const snapshot = randomUUID();
  const cdc = randomUUID();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [taskId]);
  await pool.query(
    "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
    [runId, taskId],
  );
  const now = Date.now();
  await pool.query(
    `INSERT INTO ape_dts_platform.task_run_units
    (id,task_id,run_id,status,started_at,ended_at) VALUES
    ($1,$3,$4,'STOPPED',$5,$6),($2,$3,$4,'RUNNING',$6,NULL)`,
    [snapshot, cdc, taskId, runId, new Date(now - 40 * 60_000), new Date(now - 20 * 60_000)],
  );
  await new RuntimePerformanceStore(databaseService()).maintain(new Date());
  const cdcAt = new Date(Math.floor((now - 5 * 60_000) / 60_000) * 60_000);
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_rollups_1m
    (task_id,run_id,unit_id,bucket_start,records_per_second,sink_records,gap,sample_count)
    VALUES ($1,$2,$3,$4,3,30,false,4)`,
    [taskId, runId, cdc, cdcAt],
  );
  const detail = await new RuntimePerformanceReadService(databaseService()).detail(
    admin,
    taskId,
    '24h',
  );
  expect(detail.points.find((point) => point.sinkRecords === 30)).toMatchObject({ gap: false });
});

it('returns only visible task data and preserves fresh per-field values without inventing rate', async () => {
  const binding = {
    taskId: randomUUID(),
    runId: randomUUID(),
    unitId: randomUUID(),
    processToken: randomUUID(),
    pid: 12346,
    port: 19002,
  };
  const hidden = randomUUID();
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1),($2)', [
    binding.taskId,
    hidden,
  ]);
  await pool.query(
    "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
    [binding.runId, binding.taskId],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status,process_token,process_pid,metrics_port) VALUES ($1,$2,$3,'RUNNING',$4,$5,$6)",
    [
      binding.unitId,
      binding.taskId,
      binding.runId,
      binding.processToken,
      binding.pid,
      binding.port,
    ],
  );
  const databaseService = { requirePool: () => pool } as PlatformDatabaseService;
  const store = new RuntimePerformanceStore(databaseService);
  const reads = new RuntimePerformanceReadService(databaseService);
  const collectedAt = new Date(Date.now() - 5_000);
  await store.append(binding, {
    collectedAt,
    gapReason: null,
    values: {
      recordsPerSecond: null,
      bytesPerSecond: null,
      sinkRecords: 42,
      sinkBytes: 420,
      queueRecords: 2,
      queueBytes: 20,
    },
  });
  const admin: UserSummary = {
    id: randomUUID(),
    username: 'admin',
    displayName: 'Admin',
    role: 'ADMIN',
    status: 'ACTIVE',
  };
  const result = await reads.query(admin, [binding.taskId, hidden]);
  expect(result.items).toHaveLength(2);
  expect(result.items[0]).toMatchObject({
    status: 'PARTIAL',
    values: {
      recordsPerSecond: null,
      sinkRecords: 42,
      queueRecords: 2,
    },
  });
  const detail = await reads.detail(admin, binding.taskId, '1h');
  expect(detail.points).toHaveLength(1);
  expect(detail.points[0]).toMatchObject({ sinkRecords: 42, recordsPerSecond: null, gap: false });
  const viewer: UserSummary = { ...admin, id: randomUUID(), role: 'OPERATOR' };
  expect((await reads.query(viewer, [binding.taskId])).items).toHaveLength(0);
});

it('collects a real loopback HTTP response into PostgreSQL and reads it back', async () => {
  let counter = 100;
  const server = createServer((_request, response) => {
    response.end(
      `sinker_sinked_records ${counter}\nsinker_sinked_bytes ${counter * 10}\n` +
        'pipeline_queue_size 2\npipeline_queue_bytes 20\n',
    );
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const binding = {
      taskId: randomUUID(),
      runId: randomUUID(),
      unitId: randomUUID(),
      processToken: randomUUID(),
      pid: 12347,
      port: (server.address() as AddressInfo).port,
    };
    await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [binding.taskId]);
    await pool.query(
      "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
      [binding.runId, binding.taskId],
    );
    await pool.query(
      "INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status,process_token,process_pid,metrics_port) VALUES ($1,$2,$3,'RUNNING',$4,$5,$6)",
      [
        binding.unitId,
        binding.taskId,
        binding.runId,
        binding.processToken,
        binding.pid,
        binding.port,
      ],
    );
    const databaseService = {
      requirePool: () => pool,
      hasPool: () => true,
    } as PlatformDatabaseService;
    const store = new RuntimePerformanceStore(databaseService);
    const runtime: Pick<RuntimeSupervisorService, 'listActiveRuntimeTasks'> = {
      listActiveRuntimeTasks: () => [
        {
          taskId: binding.taskId,
          runId: binding.runId,
          operationId: randomUUID(),
          platformInstanceId: randomUUID(),
          revisionId: randomUUID(),
          units: [{ unitId: binding.unitId, processToken: binding.processToken, pid: binding.pid }],
        },
      ],
    };
    const collector = createRuntimePerformanceCollector(
      databaseService,
      runtime,
      store,
      async () => undefined,
    );
    await collector.runOnce();
    counter = 110;
    await new Promise((resolve) => setTimeout(resolve, 20));
    await collector.runOnce();
    const admin: UserSummary = {
      id: randomUUID(),
      username: 'admin',
      displayName: 'Admin',
      role: 'ADMIN',
      status: 'ACTIVE',
    };
    const detail = await new RuntimePerformanceReadService(databaseService).detail(
      admin,
      binding.taskId,
      '1h',
    );
    expect(detail.summary.values).toMatchObject({ sinkRecords: 110, queueRecords: 2 });
    expect(detail.summary.values.recordsPerSecond).toBeGreaterThan(0);
    expect(detail.points.some((point) => point.sinkRecords === 110)).toBe(true);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

afterAll(async () => {
  await pool?.end();
  adminSql(`DROP DATABASE IF EXISTS "${database}";`);
  adminSql(`DROP ROLE IF EXISTS "${role}";`);
});

it('persists only a live exact binding and aggregates without changing runtime tables', async () => {
  const binding = {
    taskId: randomUUID(),
    runId: randomUUID(),
    unitId: randomUUID(),
    processToken: randomUUID(),
    pid: 12345,
    port: 19001,
  };
  await pool.query('INSERT INTO ape_dts_platform.tasks(id) VALUES ($1)', [binding.taskId]);
  await pool.query(
    "INSERT INTO ape_dts_platform.task_runs(id,task_id,status) VALUES ($1,$2,'RUNNING')",
    [binding.runId, binding.taskId],
  );
  await pool.query(
    "INSERT INTO ape_dts_platform.task_run_units(id,task_id,run_id,status,process_token,process_pid,metrics_port) VALUES ($1,$2,$3,'RUNNING',$4,$5,$6)",
    [
      binding.unitId,
      binding.taskId,
      binding.runId,
      binding.processToken,
      binding.pid,
      binding.port,
    ],
  );
  const store = new RuntimePerformanceStore({ requirePool: () => pool } as PlatformDatabaseService);
  const at = new Date('2026-09-30T12:00:15Z');
  // This fixture uses a historical clock, independent of earlier live-clock collection tests.
  await pool.query("DELETE FROM ape_dts_platform.metric_maintenance_state WHERE id='global'");
  const values = {
    recordsPerSecond: 10,
    bytesPerSecond: 100,
    sinkRecords: 1000,
    sinkBytes: 10000,
    queueRecords: 5,
    queueBytes: 50,
  };
  expect(await store.append(binding, { collectedAt: at, values, gapReason: null })).toBe(true);
  expect(
    await store.append(
      { ...binding, processToken: 'wrong' },
      { collectedAt: at, values, gapReason: null },
    ),
  ).toBe(false);
  await pool.query(
    `INSERT INTO ape_dts_platform.observability_gaps
    (task_id,run_id,unit_id,started_at,ended_at,reason) VALUES
    ($1,$2,$3,'2025-09-29T12:00:00Z','2025-09-29T13:00:00Z','OLD_GAP'),
    ($1,$2,$3,'2025-09-29T12:00:00Z','2025-09-30T14:00:00Z','SPANNING_GAP')`,
    [binding.taskId, binding.runId, binding.unitId],
  );
  await store.maintain(new Date('2026-09-30T13:00:00Z'));
  const minute = await pool.query(
    'SELECT records_per_second, sample_count FROM ape_dts_platform.metric_rollups_1m WHERE run_id=$1',
    [binding.runId],
  );
  expect(minute.rows).toMatchObject([{ records_per_second: 10, sample_count: 1 }]);
  const hour = await pool.query(
    'SELECT records_per_second, sample_count FROM ape_dts_platform.metric_rollups_1h WHERE run_id=$1',
    [binding.runId],
  );
  expect(hour.rows).toMatchObject([{ records_per_second: 10, sample_count: 1 }]);
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_rollups_1h
    (task_id,run_id,unit_id,bucket_start,records_per_second,gap,sample_count)
    VALUES ($1,$2,$3,'2026-09-23T13:00:00Z',999,false,4)`,
    [binding.taskId, binding.runId, binding.unitId],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.metric_rollups_1m
    (task_id,run_id,unit_id,bucket_start,records_per_second,gap,sample_count)
    VALUES ($1,$2,$3,'2026-09-23T13:21:00Z',1,false,1)`,
    [binding.taskId, binding.runId, binding.unitId],
  );
  await pool.query(
    "UPDATE ape_dts_platform.metric_maintenance_state SET status='FAILED' WHERE id='global'",
  );
  await store.maintain(new Date('2026-09-30T13:22:00Z'));
  const expiredHourly = await pool.query(
    `SELECT records_per_second FROM ape_dts_platform.metric_rollups_1h
    WHERE run_id=$1 AND bucket_start='2026-09-23T13:00:00Z'`,
    [binding.runId],
  );
  expect(expiredHourly.rows).toEqual([]);
  const expiredMinute = await pool.query(
    `SELECT records_per_second FROM ape_dts_platform.metric_rollups_1m
    WHERE run_id=$1 AND bucket_start='2026-09-23T13:21:00Z'`,
    [binding.runId],
  );
  expect(expiredMinute.rows).toEqual([]);
  const retained = await pool.query(
    'SELECT reason FROM ape_dts_platform.observability_gaps WHERE run_id=$1 ORDER BY reason',
    [binding.runId],
  );
  expect(retained.rows).toEqual([]);
  await pool.query(
    "UPDATE ape_dts_platform.task_run_units SET status='STOPPED', metrics_port_released_at=now() WHERE id=$1",
    [binding.unitId],
  );
  await store.recordGap(binding, {
    startedAt: at,
    endedAt: new Date(at.getTime() + 30_000),
    reason: 'DB_UNAVAILABLE',
  });
  expect(
    (
      await pool.query(
        'SELECT count(*)::integer AS n FROM ape_dts_platform.observability_gaps WHERE run_id=$1',
        [binding.runId],
      )
    ).rows[0].n,
  ).toBe(1);
  expect(
    await store.append(binding, {
      collectedAt: new Date(at.getTime() + 45_000),
      values,
      gapReason: null,
    }),
  ).toBe(false);
});
