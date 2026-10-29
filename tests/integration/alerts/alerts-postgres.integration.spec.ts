import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import { hashPassword } from '../../../packages/security/src/index.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import { AuthModule } from '../../../apps/api/src/auth/auth.module.js';
import { AlertsController } from '../../../apps/api/src/alerts/alerts.controller.js';
import { AlertsService } from '../../../apps/api/src/alerts/alerts.service.js';

const suffix = `${process.pid}_${Date.now()}`;
const dbName = `scs_alerts_meta_${suffix}`;
const roleName = `scs_alerts_${suffix}`;
const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const password = randomBytes(24).toString('base64url');
const adminId = randomUUID(),
  developerId = randomUUID(),
  taskId = randomUUID();
const policy = { memoryKiB: 1024, parallelism: 2, passes: 2, tagLength: 32, version: 1 };
const defaults = {
  channel: 'WECOM' as const,
  enabled: true,
  notifyRecovery: true,
  durationSeconds: 0,
  repeatSeconds: 30,
  slotInactive: true,
  slotMissing: true,
  retainedWalBytes: null,
  unconfirmedWalBytes: null,
  kafkaOffsetLag: null,
  delayMilliseconds: null,
};
let pool: Pool,
  database: PlatformDatabaseService,
  service: AlertsService,
  app: NestFastifyApplication;
const externalRequest = vi.fn<typeof fetch>();
function docker(args: string[], input?: string) {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120000,
  }).trim();
}
function admin(sql: string) {
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
  const portText = docker(['port', container, '5432/tcp']);
  const port = Number(portText.slice(portText.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(port) || port < 1) throw new Error('Invalid PostgreSQL fixture port');
  admin(
    `CREATE ROLE "${roleName}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${dbName}" OWNER "${roleName}";`,
  );
  pool = new Pool({ database: dbName, host: '127.0.0.1', port, user: roleName, password });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  database = new PlatformDatabaseService();
  await database.inspectAndUse(
    {
      database: dbName,
      host: '127.0.0.1',
      port,
      username: roleName,
      password,
      tls: { mode: 'DISABLE' },
    },
    bundle.manifest,
  );
  const hashed = await hashPassword(password, policy);
  for (const [id, name, role] of [
    [adminId, 'alert-admin', 'ADMIN'],
    [developerId, 'alert-developer', 'DEVELOPER'],
  ]) {
    await pool.query(
      `INSERT INTO ape_dts_platform.users (id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash) VALUES ($1,$2,$2,$3,'ACTIVE','ARGON2ID',$4::jsonb,$5,$6)`,
      [
        id,
        name,
        role,
        JSON.stringify(hashed.parameters),
        Buffer.from(hashed.salt, 'base64'),
        Buffer.from(hashed.hash, 'base64'),
      ],
    );
  }
  const source = randomUUID(),
    target = randomUUID();
  for (const id of [source, target])
    await pool.query(
      `INSERT INTO ape_dts_platform.data_sources (id,normalized_name,source_type,configuration,state,created_by_user_id,updated_by_user_id) VALUES ($1,$2,'KAFKA','{"type":"KAFKA","bootstrapServers":["127.0.0.1:9092"],"plaintextRiskAcknowledged":true}','ACTIVE',$3,$3)`,
      [id, id, adminId],
    );
  await pool.query(
    `INSERT INTO ape_dts_platform.tasks (id,normalized_name,display_name,task_type,source_data_source_id,target_data_source_id,desired_state,observed_state,observability_health,recovery_disposition,created_by_user_id,updated_by_user_id) VALUES ($1,'alert-task','Alert task','KAFKA_KAFKA',$2,$3,'STOPPED','STOPPED','HEALTHY','NOT_APPLICABLE',$4,$4)`,
    [taskId, source, target, adminId],
  );
  const keys = new UnlockedKeyService();
  keys.set(Buffer.alloc(32, 11));
  // Mock only the external transport, never the database, audit ledger or guards.
  vi.spyOn(globalThis, 'fetch').mockImplementation(externalRequest);
  service = new AlertsService(database, keys, new AuditLedgerService(database));
  const module = await Test.createTestingModule({
    imports: [AuthModule],
    controllers: [AlertsController],
    providers: [{ provide: AlertsService, useValue: service }],
  })
    .overrideProvider(PlatformDatabaseService)
    .useValue(database)
    .overrideProvider(ARGON2ID_POLICY)
    .useValue(policy)
    .compile();
  app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter({ logger: false }));
  app.setGlobalPrefix('api');
  await app.init();
  await app.getHttpAdapter().getInstance().ready();
});
afterAll(async () => {
  vi.restoreAllMocks();
  await app?.close();
  await database?.onApplicationShutdown();
  await pool?.end();
  if (!/^scs_alerts_meta_\d+_\d+$/u.test(dbName) || !/^scs_alerts_\d+_\d+$/u.test(roleName))
    throw new Error('Unsafe fixture cleanup');
  admin(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${dbName}' AND pid<>pg_backend_pid(); DROP DATABASE IF EXISTS "${dbName}"; DROP ROLE IF EXISTS "${roleName}";`,
  );
});
describe('alert persistence with real PostgreSQL and HTTP guards', () => {
  it('encrypts configuration, strips credentials and serializes concurrent settings updates', async () => {
    const original = await service.get();
    const first = await service.update(adminId, {
      ...defaults,
      expectedRowVersion: original.rowVersion,
      webhookUrl: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=integration-key',
    });
    expect(JSON.stringify(first)).not.toContain('integration-key');
    expect(first.configured).toBe(true);
    const stored = await pool.query(
      'SELECT encrypted_webhook::text AS value FROM ape_dts_platform.alert_settings',
    );
    expect(stored.rows[0].value).not.toContain('integration-key');
    const results = await Promise.allSettled([
      service.update(adminId, { ...defaults, expectedRowVersion: first.rowVersion }),
      service.update(adminId, { ...defaults, expectedRowVersion: first.rowVersion }),
    ]);
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
  });
  it('persists failed notification outcomes, repeats after the interval and sends one recovery', async () => {
    externalRequest.mockRejectedValueOnce(new Error('private destination must not escape'));
    await service.observe(
      taskId,
      'Alert task',
      'PG_RETAINED_WAL',
      true,
      true,
      '当前值：300 B\n阈值：100 B',
    );
    let history = (await service.deliveries()).items;
    expect(history[0]!.status).toBe('FAILED');
    expect(history[0]!.failureCode).toBe('CHANNEL_UNREACHABLE');
    expect(
      (
        await pool.query(
          'SELECT alarm_sent FROM ape_dts_platform.alert_rule_states WHERE task_id=$1',
          [taskId],
        )
      ).rows[0].alarm_sent,
    ).toBe(false);
    await service.observe(taskId, 'Alert task', 'PG_RETAINED_WAL', true, true);
    expect((await service.deliveries()).items).toHaveLength(history.length);
    await pool.query(
      "UPDATE ape_dts_platform.alert_rule_states SET last_attempt_at=last_attempt_at-interval '31 seconds' WHERE task_id=$1",
      [taskId],
    );
    externalRequest.mockResolvedValueOnce(new Response('{"errcode":0}'));
    await service.observe(
      taskId,
      'Alert task',
      'PG_RETAINED_WAL',
      true,
      true,
      '当前值：300 B\n阈值：100 B',
    );
    const body = JSON.parse(externalRequest.mock.calls.at(-1)![1]!.body as string);
    expect(body.text.content).toContain('Alert task');
    expect(body.text.content).toContain('PG 保留 WAL 超限');
    expect(body.text.content).toContain('阈值：100 B');
    externalRequest.mockResolvedValueOnce(new Response('{"errcode":0}'));
    await service.observe(taskId, 'Alert task', 'PG_RETAINED_WAL', false, true);
    history = (await service.deliveries()).items;
    expect(history[0]!.event).toBe('RECOVERY');
    expect(history[0]!.status).toBe('SENT');
    await service.observe(taskId, 'Alert task', 'PG_RETAINED_WAL', false, true);
    expect((await service.deliveries()).items).toHaveLength(history.length);
  });
  it('requires administrator access and CSRF for mutations while allowing authenticated reads', async () => {
    expect((await app.inject({ method: 'GET', url: '/api/settings/alerts' })).statusCode).toBe(401);
    async function login(username: string) {
      const result = await app.inject({
        method: 'POST',
        url: '/api/auth/login',
        payload: { username, password },
      });
      expect(result.statusCode).toBe(200);
      return {
        cookie: String(result.headers['set-cookie']).split(';')[0]!,
        csrf: result.json().csrfToken as string,
      };
    }
    const developer = await login('alert-developer');
    const contextUrl = `/api/settings/alerts/deliveries/${randomUUID()}/context`;
    expect((await app.inject({ method: 'GET', url: contextUrl })).statusCode).toBe(401);
    expect(
      (await app.inject({ method: 'GET', url: contextUrl, headers: { cookie: developer.cookie } }))
        .statusCode,
    ).toBe(403);
    expect(
      (
        await app.inject({
          method: 'GET',
          url: '/api/settings/alerts',
          headers: { cookie: developer.cookie },
        })
      ).statusCode,
    ).toBe(403);
    const administrator = await login('alert-admin');
    expect(
      (
        await app.inject({
          method: 'GET',
          url: contextUrl,
          headers: { cookie: administrator.cookie },
        })
      ).statusCode,
    ).toBe(404);
    expect(
      (
        await app.inject({
          method: 'GET',
          url: '/api/settings/alerts/deliveries/not-a-uuid/context',
          headers: { cookie: administrator.cookie },
        })
      ).statusCode,
    ).toBe(400);
    const current = await service.get();
    expect(
      (
        await app.inject({
          method: 'PATCH',
          url: '/api/settings/alerts',
          headers: { cookie: administrator.cookie },
          payload: { ...defaults, expectedRowVersion: current.rowVersion },
        })
      ).statusCode,
    ).toBe(403);
    const saved = await app.inject({
      method: 'PATCH',
      url: '/api/settings/alerts',
      headers: { cookie: administrator.cookie, 'x-csrf-token': administrator.csrf },
      payload: { ...defaults, expectedRowVersion: current.rowVersion },
    });
    expect(saved.statusCode).toBe(200);
    expect(saved.json().configured).toBe(true);
    externalRequest.mockResolvedValueOnce(new Response('{"errcode":0}'));
    const test = await app.inject({
      method: 'POST',
      url: '/api/settings/alerts/test',
      headers: { cookie: administrator.cookie, 'x-csrf-token': administrator.csrf },
    });
    expect(test.statusCode).toBe(201);
    expect(test.json().event).toBe('TEST');
    expect(test.json().status).toBe('SENT');
    const contextResponse = await app.inject({
      method: 'GET',
      url: `/api/settings/alerts/deliveries/${test.json().id}/context`,
      headers: { cookie: administrator.cookie },
    });
    expect(contextResponse.statusCode).toBe(200);
    expect(contextResponse.json()).toMatchObject({
      association: 'NONE',
      run: null,
      logWindow: null,
    });
  });

  it('returns task names and keeps recovery context bound to the failed run after a new run starts', async () => {
    const fixture = await diagnosticFixture();
    await fixture.run('FAILED', '2026-10-27T00:00:00Z', '2026-10-27T00:10:00Z');
    const oldRun = fixture.lastRunId;
    await pool.query(
      `INSERT INTO ape_dts_platform.task_run_units (id,task_id,run_id,unit_key,status,exit_code,failure_code) VALUES ($1,$2,$3,'source','FAILED',1,'RUNTIME_UNEXPECTED_EXIT')`,
      [randomUUID(), fixture.taskId, oldRun],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.preflight_runs (id,task_revision_id,state,started_at,completed_at,result,failure_code,failure_summary)
      VALUES ($1,$2,'FAILED','2026-10-27T00:11:00Z','2026-10-27T00:11:01Z',$3::jsonb,'PREFLIGHT_FAILED','Failed')`,
      [
        randomUUID(),
        fixture.revisionId,
        JSON.stringify({
          checks: [
            {
              key: 'SOURCE_METADATA',
              state: 'FAILED',
              code: 'TIMEOUT',
              summary: '来源元数据读取超时。',
            },
          ],
        }),
      ],
    );
    await fixture.audit({ oldRunId: oldRun, reason: 'TIMEOUT' });
    const alarmId = await fixture.delivery('RECOVERY_FAILED', 'ALARM');
    await fixture.run('RUNNING', new Date().toISOString());
    const recoveryId = await fixture.delivery('RECOVERY_FAILED', 'RECOVERY');
    const alarm = (await service.deliveries()).items.find((item) => item.id === alarmId);
    expect(alarm).toMatchObject({ taskName: '诊断任务', failureCode: null });
    const context = await service.deliveryContext(recoveryId);
    expect(context).toMatchObject({
      association: 'AUDIT',
      delivery: { taskName: '诊断任务', faultCode: 'TIMEOUT', failureCode: null },
      run: { id: oldRun, status: 'FAILED' },
      units: [{ exitCode: 1 }],
    });
    expect(context.preflightChecks).toEqual([
      { key: 'SOURCE_METADATA', code: 'TIMEOUT', summary: '来源元数据读取超时。' },
    ]);
    expect(context.logWindow).toEqual({
      from: '2026-10-27T00:08:00.000Z',
      to: '2026-10-27T00:12:00.000Z',
    });
    // A new incident must not reuse the preceding incident's precise audit association.
    const nextAlarm = await fixture.delivery('RECOVERY_FAILED', 'ALARM');
    const next = await service.deliveryContext(nextAlarm);
    expect(next.association).toBe('TIME');
    expect(next.run?.id).toBe(fixture.lastRunId);
    expect(next.delivery.faultCode).toBeNull();
  });

  it('uses the original alarm time for older recovery records without a precise audit binding', async () => {
    const fixture = await diagnosticFixture();
    await fixture.run('FAILED', '2026-10-25T00:00:00Z', '2026-10-25T01:00:00Z');
    const oldRun = fixture.lastRunId;
    await fixture.delivery('PG_SLOT_INACTIVE', 'ALARM', '2026-10-25T00:59:00Z');
    await fixture.run('RUNNING', '2026-10-25T02:00:00Z');
    const recoveryId = await fixture.delivery(
      'PG_SLOT_INACTIVE',
      'RECOVERY',
      '2026-10-25T02:01:00Z',
    );
    const context = await service.deliveryContext(recoveryId);
    expect(context.association).toBe('TIME');
    expect(context.run?.id).toBe(oldRun);
    expect(context.logWindow?.from).toBe('2026-10-25T00:57:00.000Z');
    const orphanRecovery = await fixture.delivery('START_FAILED', 'RECOVERY');
    expect(await service.deliveryContext(orphanRecovery)).toMatchObject({
      association: 'NONE',
      run: null,
      logWindow: null,
    });
  });

  it('rejects cross-task audit run bindings and keeps notification transport errors separate', async () => {
    const fixture = await diagnosticFixture();
    const other = await diagnosticFixture();
    await other.run('FAILED', '2026-10-25T00:00:00Z', '2026-10-25T01:00:00Z');
    await fixture.audit({ oldRunId: other.lastRunId, reason: 'TIMEOUT' });
    const id = await fixture.delivery('RECOVERY_FAILED', 'ALARM');
    await pool.query(
      "UPDATE ape_dts_platform.alert_deliveries SET status='FAILED', failure_code='CHANNEL_UNREACHABLE' WHERE id=$1",
      [id],
    );
    const context = await service.deliveryContext(id);
    expect(context).toMatchObject({
      association: 'NONE',
      run: null,
      units: [],
      delivery: { failureCode: 'CHANNEL_UNREACHABLE', faultCode: 'TIMEOUT' },
    });
  });
});

async function diagnosticFixture() {
  const id = randomUUID(),
    revisionId = randomUUID();
  await pool.query(
    `INSERT INTO ape_dts_platform.tasks (id,normalized_name,display_name,task_type,source_data_source_id,target_data_source_id,desired_state,observed_state,observability_health,recovery_disposition,created_by_user_id,updated_by_user_id)
    SELECT $1::uuid,($1::uuid)::text,'诊断任务',task_type,source_data_source_id,target_data_source_id,'STOPPED','STOPPED','HEALTHY','NOT_APPLICABLE',$2,$2 FROM ape_dts_platform.tasks WHERE id=$3`,
    [id, adminId, taskId],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.task_revisions (id,task_id,revision_number,state,configuration,created_by_user_id,updated_by_user_id) VALUES ($1,$2,1,'INCOMPLETE_DRAFT','{}',$3,$3)`,
    [revisionId, id, adminId],
  );
  let lastRunId = '';
  return {
    taskId: id,
    revisionId,
    get lastRunId() {
      return lastRunId;
    },
    async run(status: 'FAILED' | 'RUNNING', startedAt: string, endedAt: string | null = null) {
      lastRunId = randomUUID();
      await pool.query(
        `INSERT INTO ape_dts_platform.task_runs (id,task_id,task_revision_id,status,current_stage,started_at,ended_at,failure_code,failure_summary) VALUES ($1,$2,$3,$4,'CDC',$5,$6,$7,$8)`,
        [
          lastRunId,
          id,
          revisionId,
          status,
          startedAt,
          endedAt,
          status === 'FAILED' ? 'RUNTIME_UNEXPECTED_EXIT' : null,
          status === 'FAILED' ? 'Unexpected exit' : null,
        ],
      );
    },
    async delivery(rule: string, event: 'ALARM' | 'RECOVERY', at?: string) {
      const deliveryId = randomUUID();
      await pool.query(
        `INSERT INTO ape_dts_platform.alert_deliveries (id,task_id,rule,event,status,created_at) VALUES ($1,$2,$3,$4,'SENT',COALESCE($5::timestamptz,statement_timestamp()))`,
        [deliveryId, id, rule, event, at ?? null],
      );
      return deliveryId;
    },
    async audit(details: Record<string, unknown>) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await new AuditLedgerService(database).append(client, {
          action: 'TASK_RUNTIME_AUTO_RECOVERY_MANUAL_REQUIRED',
          actorType: 'SYSTEM',
          actorUserId: null,
          subjectType: 'TASK',
          subjectId: id,
          details,
        });
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    },
  };
}
