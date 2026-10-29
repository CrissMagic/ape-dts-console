import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Logger } from '../../../apps/api/node_modules/@nestjs/common/index.js';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';

import { AppModule } from '../../../apps/api/src/app.module.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import type { Argon2idParameters } from '../../../packages/security/src/index.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const name = `scs_bootstrap_http_${process.pid}_${Date.now()}`;
const password = randomBytes(24).toString('base64url');
const policy: Argon2idParameters = {
  memoryKiB: 1024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};
let app: NestFastifyApplication | undefined;
let pool: Pool | undefined;
let directory: string | undefined;
let temporaryPassword: string;
let created = false;
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
async function request(path: string, body?: unknown, cookie?: string) {
  return fetch(`${await app!.getUrl()}/api${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(cookie ? { cookie } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
beforeAll(async () => {
  const portText = docker(['port', container, '5432/tcp']);
  const port = Number(portText.slice(portText.lastIndexOf(':') + 1));
  sql(
    `CREATE ROLE "${name}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${name}" OWNER "${name}";`,
  );
  created = true;
  pool = new Pool({ host: '127.0.0.1', port, database: name, user: name, password });
  directory = await mkdtemp(join(tmpdir(), 'scs-bootstrap-http-'));
  vi.stubEnv('SCS_PLATFORM_CONFIG_PATH', join(directory, 'private', 'config.json'));
  vi.stubEnv(
    'SCS_PLATFORM_DATABASE_CONFIG',
    JSON.stringify({
      host: '127.0.0.1',
      port,
      database: name,
      username: name,
      password,
      tls: { mode: 'DISABLE' },
    }),
  );
  const log = vi.spyOn(Logger, 'warn').mockImplementation((message: unknown) => {
    if (typeof message !== 'string' || !message.includes('bootstrap.administrator_created')) return;
    temporaryPassword = JSON.parse(message).temporaryPassword;
  });
  try {
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(ARGON2ID_POLICY)
      .useValue(policy)
      .compile();
    app = module.createNestApplication<NestFastifyApplication>(
      new FastifyAdapter({ logger: false }),
    );
    app.setGlobalPrefix('api');
    await app.listen(0, '127.0.0.1');
  } finally {
    log.mockRestore();
  }
});
afterAll(async () => {
  await app?.close();
  await pool?.end();
  vi.unstubAllEnvs();
  if (created) sql(`DROP DATABASE "${name}"; DROP ROLE "${name}";`);
  if (directory) await rm(directory, { recursive: true, force: true });
});

it('starts READY, authenticates the generated admin password, and requires its change', async () => {
  expect(await (await request('/bootstrap/status')).json()).toMatchObject({ phase: 'READY' });
  expect((await request('/health/ready')).status).toBe(200);
  expect(temporaryPassword.length).toBeGreaterThanOrEqual(24);
  const login = await request('/auth/login', { username: 'admin', password: temporaryPassword });
  expect(login.status).toBe(200);
  expect(await login.json()).toMatchObject({ status: 'AUTHENTICATED', mustChangePassword: true });
  const denied = await request('/users', undefined, login.headers.get('set-cookie')!.split(';')[0]);
  expect(denied.status).toBe(403);
  expect(await denied.json()).toMatchObject({ code: 'PASSWORD_CHANGE_REQUIRED' });
});

it('removes all former bootstrap mutation endpoints without changing persistent state', async () => {
  const before = await pool!.query(
    'SELECT id, normalized_username, password_hash, must_change_password FROM ape_dts_platform.users ORDER BY id',
  );
  const wrappersBefore = await pool!.query(
    'SELECT id, ciphertext FROM ape_dts_platform.key_wrappers ORDER BY id',
  );
  for (const path of [
    'verify-code',
    'database/inspect',
    'initialize',
    'totp/verify',
    'migrate',
    'unlock',
  ]) {
    expect((await request(`/bootstrap/${path}`, {})).status).toBe(404);
  }
  for (const path of ['database/inspection', 'enrollment']) {
    expect((await request(`/bootstrap/${path}`)).status).toBe(404);
  }
  expect(
    (
      await pool!.query(
        'SELECT id, normalized_username, password_hash, must_change_password FROM ape_dts_platform.users ORDER BY id',
      )
    ).rows,
  ).toEqual(before.rows);
  expect(
    (await pool!.query('SELECT id, ciphertext FROM ape_dts_platform.key_wrappers ORDER BY id'))
      .rows,
  ).toEqual(wrappersBefore.rows);
});
