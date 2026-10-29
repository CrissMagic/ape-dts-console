import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { AuthModule } from '../../../apps/api/src/auth/auth.module.js';
import { UsersModule } from '../../../apps/api/src/users/users.module.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import { resetAdministratorPassword } from '../../../apps/api/src/auth/reset-administrator-password.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import {
  hashPassword,
  hashOpaqueToken,
  type Argon2idParameters,
} from '../../../packages/security/src/index.js';
import { authenticationFingerprint } from '../../../apps/api/src/auth/auth-policy.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const name = `scs_password_${process.pid}_${Date.now()}`;
const databasePassword = randomBytes(24).toString('base64url');
const initialPassword = randomBytes(24).toString('base64url');
const policy: Argon2idParameters = {
  memoryKiB: 1024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};
let app: NestFastifyApplication;
let pool: Pool;
let created = false;
const legacyOwner = randomUUID();
const legacySessionToken = randomBytes(24).toString('base64url');
const expiredSessionToken = randomBytes(24).toString('base64url');
let legacyBefore: { idle_expires_at: Date; absolute_expires_at: Date }[] = [];
function docker(args: string[], input?: string) {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}
function sql(query: string) {
  docker(
    ['exec', '-i', container, 'psql', '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1'],
    query,
  );
}
function cookie(response: Response) {
  const header = response.headers.get('set-cookie')!;
  expect(header).not.toMatch(/;\s*Secure(?:;|$)/i);
  expect(header).toContain('HttpOnly');
  expect(header).toContain('SameSite=Strict');
  expect(header).toContain('Path=/api');
  return header.split(';')[0]!;
}
async function request(path: string, body?: unknown, session?: string, csrf?: string) {
  return fetch(`${await app.getUrl()}/api${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(session ? { cookie: session } : {}),
      ...(csrf ? { 'x-csrf-token': csrf } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
beforeAll(async () => {
  const portText = docker(['port', container, '5432/tcp']);
  const port = Number(portText.slice(portText.lastIndexOf(':') + 1));
  sql(
    `CREATE ROLE "${name}" LOGIN PASSWORD '${databasePassword}'; CREATE DATABASE "${name}" OWNER "${name}";`,
  );
  created = true;
  pool = new Pool({
    host: '127.0.0.1',
    port,
    database: name,
    user: name,
    password: databasePassword,
  });
  const bundle = await loadMigrationBundle();
  const previous = {
    manifest: {
      ...bundle.manifest,
      migrations: bundle.manifest.migrations.filter((item) => item.version < 28),
    },
    migrations: bundle.migrations.filter((item) => item.version < 28),
  };
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(previous, randomUUID(), {
    expectedState: 'ABSENT',
  });
  const legacyPassword = await hashPassword(initialPassword, policy);
  await pool.query(
    `INSERT INTO ape_dts_platform.users
    (id,normalized_username,display_name,role,status,password_kdf_algorithm,password_kdf_parameters,password_salt,password_hash)
    VALUES ($1,'legacy','legacy','DEVELOPER','ACTIVE','ARGON2ID',$2,$3,$4)`,
    [
      legacyOwner,
      JSON.stringify(legacyPassword.parameters),
      Buffer.from(legacyPassword.salt, 'base64'),
      Buffer.from(legacyPassword.hash, 'base64'),
    ],
  );
  for (const [token, expired] of [
    [legacySessionToken, false],
    [expiredSessionToken, true],
  ] as const) {
    await pool.query(
      `INSERT INTO ape_dts_platform.auth_sessions
      (id,user_id,token_hash,csrf_token_hash,source_fingerprint,session_generation,created_at,last_seen_at,idle_expires_at,absolute_expires_at,reauthenticated_at)
      VALUES ($1,$2,$3,$4,$5,1,now()-interval '1 hour',now()-interval '1 hour',
        now()+CASE WHEN $6 THEN interval '-1 minute' ELSE interval '30 minutes' END,now()+interval '12 hours',now()-interval '1 hour')`,
      [
        randomUUID(),
        legacyOwner,
        hashOpaqueToken(token),
        randomBytes(32),
        authenticationFingerprint('127.0.0.1'),
        expired,
      ],
    );
  }
  legacyBefore = (
    await pool.query(
      'SELECT idle_expires_at,absolute_expires_at FROM ape_dts_platform.auth_sessions WHERE user_id=$1 ORDER BY idle_expires_at',
      [legacyOwner],
    )
  ).rows;
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'OLDER',
  });
  const module = await Test.createTestingModule({ imports: [AuthModule, UsersModule] })
    .overrideProvider(ARGON2ID_POLICY)
    .useValue(policy)
    .compile();
  app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter({ logger: false }));
  app.setGlobalPrefix('api');
  await app.get(PlatformDatabaseService).inspectAndUse(
    {
      database: name,
      host: '127.0.0.1',
      port,
      username: name,
      password: databasePassword,
      tls: { mode: 'DISABLE' },
    },
    bundle.manifest,
  );
  const hashed = await hashPassword(initialPassword, policy);
  for (const [username, role, mustChange] of [
    ['admin', 'ADMIN', true],
    ['developer', 'DEVELOPER', false],
    ['backup-admin', 'ADMIN', false],
  ] as const) {
    await pool.query(
      `INSERT INTO ape_dts_platform.users
      (id, normalized_username, display_name, role, status, password_kdf_algorithm,
       password_kdf_parameters, password_salt, password_hash, must_change_password)
      VALUES ($1, $2, $2, $3, 'ACTIVE', 'ARGON2ID', $4, $5, $6, $7)`,
      [
        randomUUID(),
        username,
        role,
        JSON.stringify(hashed.parameters),
        Buffer.from(hashed.salt, 'base64'),
        Buffer.from(hashed.hash, 'base64'),
        mustChange,
      ],
    );
  }
  await app.listen(0, '127.0.0.1');
});
afterAll(async () => {
  await app?.close();
  await pool?.end();
  if (created) sql(`DROP DATABASE "${name}"; DROP ROLE "${name}";`);
});

it('enforces first password change in HTTP guards, invalidates old sessions, and retains developer RBAC', async () => {
  const login = await request('/auth/login', { username: 'admin', password: initialPassword });
  expect(login.status).toBe(200);
  const initialSession = await login.json();
  expect(initialSession).toMatchObject({ status: 'AUTHENTICATED', mustChangePassword: true });
  const oldCookie = cookie(login);
  const denied = await request('/users', undefined, oldCookie);
  expect(denied.status).toBe(403);
  expect(await denied.json()).toMatchObject({ code: 'PASSWORD_CHANGE_REQUIRED' });
  const nextPassword = randomBytes(24).toString('base64url');
  expect(
    (
      await request(
        '/auth/password/change',
        { currentPassword: initialPassword, newPassword: nextPassword },
        oldCookie,
      )
    ).status,
  ).toBe(403);
  const wrong = await request(
    '/auth/password/change',
    { currentPassword: 'wrong-password-value', newPassword: nextPassword },
    oldCookie,
    initialSession.csrfToken,
  );
  expect(wrong.status).toBe(400);
  expect(await wrong.json()).toMatchObject({ code: 'CURRENT_PASSWORD_INVALID' });
  const unchanged = await request(
    '/auth/password/change',
    { currentPassword: initialPassword, newPassword: initialPassword },
    oldCookie,
    initialSession.csrfToken,
  );
  expect(unchanged.ok).toBe(false);
  const changed = await request(
    '/auth/password/change',
    { currentPassword: initialPassword, newPassword: nextPassword },
    oldCookie,
    initialSession.csrfToken,
  );
  expect(changed.status).toBe(200);
  expect(await changed.json()).toMatchObject({ mustChangePassword: false });
  expect((await request('/users', undefined, oldCookie)).status).toBe(401);
  expect((await request('/users', undefined, cookie(changed))).status).toBe(200);
  const nextLogin = await request('/auth/login', { username: 'admin', password: nextPassword });
  expect(nextLogin.status).toBe(200);
  expect(
    (await request('/auth/login', { username: 'admin', password: initialPassword })).status,
  ).toBe(401);
  const developer = await request('/auth/login', {
    username: 'developer',
    password: initialPassword,
  });
  expect(developer.status).toBe(200);
  expect(await developer.json()).toMatchObject({ mustChangePassword: false });
  expect((await request('/users', undefined, cookie(developer))).status).toBe(403);

  // The fixed recovery identity cannot be demoted, even when another active admin exists.
  const backupLogin = await request('/auth/login', {
    username: 'backup-admin',
    password: initialPassword,
  });
  expect(backupLogin.status).toBe(200);
  const backupSession = await backupLogin.json();
  const beforeFixed = await pool.query(
    "SELECT id, role, session_generation::text FROM ape_dts_platform.users WHERE normalized_username = 'admin'",
  );
  const refused = await fetch(`${await app.getUrl()}/api/users/${beforeFixed.rows[0].id}/role`, {
    method: 'PATCH',
    headers: {
      'content-type': 'application/json',
      cookie: cookie(backupLogin),
      'x-csrf-token': backupSession.csrfToken,
    },
    body: JSON.stringify({ role: 'DEVELOPER' }),
  });
  expect(refused.status).toBe(409);
  expect(await refused.json()).toMatchObject({ code: 'FIXED_ADMIN_PROTECTED' });
  const afterFixed = await pool.query(
    "SELECT id, role, session_generation::text FROM ape_dts_platform.users WHERE normalized_username = 'admin'",
  );
  expect(afterFixed.rows).toEqual(beforeFixed.rows);
  expect((await request('/auth/session', undefined, cookie(nextLogin))).status).toBe(200);
  expect((await request('/auth/session', undefined, cookie(backupLogin))).status).toBe(200);

  const temporary = await resetAdministratorPassword(pool, app.get(AuditLedgerService), policy);
  expect(temporary.length).toBeGreaterThanOrEqual(24);
  expect((await request('/users', undefined, cookie(nextLogin))).status).toBe(401);
  const resetLogin = await request('/auth/login', { username: 'admin', password: temporary });
  expect(resetLogin.status).toBe(200);
  expect(await resetLogin.json()).toMatchObject({ mustChangePassword: true });
  // Resetting admin must not alter another user's password or live session.
  expect((await request('/auth/session', undefined, cookie(developer))).status).toBe(200);
  expect(
    (await request('/auth/login', { username: 'developer', password: initialPassword })).status,
  ).toBe(200);
  const audit = await pool.query(
    `SELECT actor_type, canonical_details FROM ape_dts_platform.audit_events WHERE action = 'ADMINISTRATOR_PASSWORD_RESET'`,
  );
  expect(audit.rows).toHaveLength(1);
  expect(audit.rows[0].actor_type).toBe('DEPLOYMENT_ADMIN');
  expect(JSON.stringify(audit.rows)).not.toContain(temporary);
});

it('retains the HTTP login session and clears it on CSRF-protected logout', async () => {
  const login = await request('/auth/login', { username: 'developer', password: initialPassword });
  expect(login.status).toBe(200);
  const sessionCookie = cookie(login);
  const session = await request('/auth/session', undefined, sessionCookie);
  expect(session.status).toBe(200);
  const current = await session.json();
  expect((await request('/auth/logout', {}, sessionCookie)).status).toBe(403);
  const logout = await request('/auth/logout', {}, sessionCookie, current.csrfToken);
  expect(logout.status).toBe(204);
  expect(cookie(logout)).toBe('scs_session=');
  expect(logout.headers.get('set-cookie')).toContain('Max-Age=0');
  expect((await request('/auth/session', undefined, sessionCookie)).status).toBe(401);
});

it('preserves legacy session deadlines during upgrade and does not revive expired sessions', async () => {
  const rows = await pool.query(
    'SELECT idle_expires_at,absolute_expires_at,idle_timeout_minutes FROM ape_dts_platform.auth_sessions WHERE user_id=$1 ORDER BY idle_expires_at',
    [legacyOwner],
  );
  expect(
    rows.rows.map(({ idle_expires_at, absolute_expires_at }) => ({
      idle_expires_at,
      absolute_expires_at,
    })),
  ).toEqual(legacyBefore);
  expect(rows.rows.every((row) => row.idle_timeout_minutes === 30)).toBe(true);
  expect(
    (await request('/auth/session', undefined, `scs_session=${expiredSessionToken}`)).status,
  ).toBe(401);
  const refreshed = await request('/auth/session', undefined, `scs_session=${legacySessionToken}`);
  expect(refreshed.status).toBe(200);
  const value = await refreshed.json();
  expect(Date.parse(value.idleExpiresAt) - Date.now()).toBeGreaterThan(29 * 60_000);
  expect(Date.parse(value.idleExpiresAt) - Date.now()).toBeLessThanOrEqual(30 * 60_000);
});

it('applies settings only to new sessions and enforces administrator, CSRF and version checks', async () => {
  const login = await request('/auth/login', {
    username: 'backup-admin',
    password: initialPassword,
  });
  expect(login.status).toBe(200);
  const adminSession = await login.json();
  const adminCookie = cookie(login);
  const before = await request('/settings/sessions', undefined, adminCookie);
  expect(before.status).toBe(200);
  const settings = await before.json();
  expect(settings).toMatchObject({ idleTimeoutMinutes: 480, absoluteTimeoutHours: 168 });
  expect(Date.parse(adminSession.absoluteExpiresAt) - Date.parse(adminSession.idleExpiresAt)).toBe(
    160 * 3_600_000,
  );
  const baseUrl = await app.getUrl();
  const update = (body: unknown, session = adminCookie, csrf?: string) =>
    fetch(`${baseUrl}/api/settings/sessions`, {
      method: 'PATCH',
      headers: {
        'content-type': 'application/json',
        cookie: session,
        ...(csrf ? { 'x-csrf-token': csrf } : {}),
      },
      body: JSON.stringify(body),
    });
  const next = {
    idleTimeoutMinutes: 60,
    absoluteTimeoutHours: 24,
    expectedRowVersion: settings.rowVersion,
  };
  for (const expectedRowVersion of ['invalid', '', '0', '-1', '9223372036854775808']) {
    expect(
      (await update({ ...next, expectedRowVersion }, adminCookie, adminSession.csrfToken)).status,
    ).toBe(400);
  }
  expect((await update(next)).status).toBe(403);
  const developerLogin = await request('/auth/login', {
    username: 'developer',
    password: initialPassword,
  });
  const developer = await developerLogin.json();
  expect((await request('/settings/sessions', undefined, cookie(developerLogin))).status).toBe(403);
  expect((await update(next, cookie(developerLogin), developer.csrfToken)).status).toBe(403);
  expect(
    (await update({ ...next, idleTimeoutMinutes: 1441 }, adminCookie, adminSession.csrfToken))
      .status,
  ).toBe(400);
  expect((await update(next, adminCookie, adminSession.csrfToken)).status).toBe(200);
  expect((await update(next, adminCookie, adminSession.csrfToken)).status).toBe(409);
  const old = await request('/auth/session', undefined, adminCookie);
  const oldValue = await old.json();
  expect(oldValue.absoluteExpiresAt).toBe(adminSession.absoluteExpiresAt);
  expect(Date.parse(oldValue.idleExpiresAt) - Date.now()).toBeGreaterThan(479 * 60_000);
  const newer = await request('/auth/login', { username: 'developer', password: initialPassword });
  const newValue = await newer.json();
  expect(Date.parse(newValue.idleExpiresAt) - Date.now()).toBeGreaterThan(59 * 60_000);
  expect(Date.parse(newValue.absoluteExpiresAt) - Date.parse(newValue.idleExpiresAt)).toBe(
    23 * 3_600_000,
  );
  expect(Number(newer.headers.get('set-cookie')?.match(/Max-Age=(\d+)/u)?.[1])).toBeGreaterThan(
    23 * 3_600,
  );
  // Activity can renew idle time, but never past this session's absolute deadline.
  const tokenHash = hashOpaqueToken(decodeURIComponent(cookie(newer).slice('scs_session='.length)));
  await pool.query(
    "UPDATE ape_dts_platform.auth_sessions SET idle_expires_at=now()+interval '30 seconds',absolute_expires_at=now()+interval '30 seconds' WHERE token_hash=$1",
    [tokenHash],
  );
  const nearingExpiry = await request('/auth/session', undefined, cookie(newer));
  expect(nearingExpiry.status).toBe(200);
  const capped = await nearingExpiry.json();
  expect(capped.idleExpiresAt).toBe(capped.absoluteExpiresAt);
  // Expiry is server-enforced even if the browser continues to send the cookie.
  await pool.query(
    "UPDATE ape_dts_platform.auth_sessions SET absolute_expires_at=now()-interval '1 second',idle_expires_at=now()-interval '1 second',created_at=now()-interval '1 hour',last_seen_at=now()-interval '1 hour' WHERE token_hash=$1",
    [tokenHash],
  );
  expect((await request('/auth/session', undefined, cookie(newer))).status).toBe(401);
  const audit = await pool.query(
    "SELECT actor_user_id,canonical_details FROM ape_dts_platform.audit_events WHERE action='SESSION_SETTINGS_UPDATED'",
  );
  expect(audit.rows).toHaveLength(1);
  expect(audit.rows[0].canonical_details).toMatchObject({
    appliesTo: 'NEW_SESSIONS',
    previous: { idleTimeoutMinutes: 480 },
    updated: { idleTimeoutMinutes: 60 },
  });
});

it('allows only one concurrent administrator policy update and audits only the winner', async () => {
  const login = await request('/auth/login', {
    username: 'backup-admin',
    password: initialPassword,
  });
  const session = await login.json();
  const current = await request('/settings/sessions', undefined, cookie(login));
  const previous = await current.json();
  const baseUrl = await app.getUrl();
  const responses = await Promise.all(
    [90, 120].map((idleTimeoutMinutes) =>
      fetch(`${baseUrl}/api/settings/sessions`, {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
          cookie: cookie(login),
          'x-csrf-token': session.csrfToken,
        },
        body: JSON.stringify({
          idleTimeoutMinutes,
          absoluteTimeoutHours: 24,
          expectedRowVersion: previous.rowVersion,
        }),
      }),
    ),
  );
  expect(responses.map((response) => response.status).sort()).toEqual([200, 409]);
  const winner = await responses.find((response) => response.status === 200)!.json();
  expect(winner.rowVersion).toBe(String(Number(previous.rowVersion) + 1));
  const persisted = await request('/settings/sessions', undefined, cookie(login));
  expect(await persisted.json()).toEqual(winner);
  const audit = await pool.query(
    "SELECT canonical_details FROM ape_dts_platform.audit_events WHERE action='SESSION_SETTINGS_UPDATED' AND canonical_details->'previous'->>'rowVersion'=$1",
    [previous.rowVersion],
  );
  expect(audit.rows).toHaveLength(1);
  expect(audit.rows[0].canonical_details).toMatchObject({ previous, updated: winner });
});
