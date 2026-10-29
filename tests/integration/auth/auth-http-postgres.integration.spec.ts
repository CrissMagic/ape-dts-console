import { execFileSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';

import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AppModule } from '../../../apps/api/src/app.module.js';
import {
  RUNTIME_ENVIRONMENT_CHECK,
  RuntimeEnvironmentInspector,
} from '../../../apps/api/src/preflight/runtime-environment-check.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { PlatformReadinessService } from '../../../apps/api/src/health/platform-readiness.service.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import { UnlockedKeyService } from '../../../apps/api/src/security/unlocked-key.service.js';
import type { UserSummary } from '../../../packages/contracts/src/index.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import {
  encryptSecret,
  generateRecoveryCodes,
  generateTotpSecret,
  hashPassword,
  hashRecoveryCode,
  type Argon2idParameters,
} from '../../../packages/security/src/index.js';

const uniqueSuffix = `${process.pid}_${Date.now()}`;
const postgresContainer = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const databaseRole = `scs_auth_http_${uniqueSuffix}`;
const metadataDatabase = `scs_auth_http_meta_${uniqueSuffix}`;
const databasePassword = randomBytes(24).toString('base64url');
const administratorPassword = 'administrator password for unit five';
const testKdfPolicy: Argon2idParameters = {
  memoryKiB: 1_024,
  parallelism: 2,
  passes: 2,
  tagLength: 32,
  version: 1,
};

let app: NestFastifyApplication | undefined;
let pool: Pool | undefined;
let postgresPort = 0;
let administratorId = '';
let administratorTotpSecret = Buffer.alloc(0);
let administratorRecoveryCodes: readonly string[] = [];

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
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
    throw new Error('test pool is not initialized');
  }
  return pool;
}

function requireApp(): NestFastifyApplication {
  if (app === undefined) {
    throw new Error('test application is not initialized');
  }
  return app;
}

async function insertAdministrator(dek: Buffer): Promise<void> {
  administratorId = randomUUID();
  const secretId = randomUUID();
  const password = await hashPassword(administratorPassword, testKdfPolicy);
  administratorTotpSecret = generateTotpSecret();
  const encrypted = encryptSecret(dek, administratorTotpSecret, {
    aadVersion: 1,
    ownerId: administratorId,
    ownerType: 'USER',
    purpose: 'TOTP_SEED',
    secretId,
  });
  await requirePool().query(
    `INSERT INTO ape_dts_platform.secrets (
       id, owner_type, owner_id, purpose, algorithm, dek_version, aad_version,
       nonce, ciphertext, tag
     ) VALUES (
       $1::uuid, 'USER', $2::uuid, 'TOTP_SEED', $3, 1, $4, $5, $6, $7
     )`,
    [
      secretId,
      administratorId,
      encrypted.algorithm,
      encrypted.aadVersion,
      Buffer.from(encrypted.nonce, 'base64'),
      Buffer.from(encrypted.ciphertext, 'base64'),
      Buffer.from(encrypted.tag, 'base64'),
    ],
  );
  await requirePool().query(
    `INSERT INTO ape_dts_platform.users (
       id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
       totp_secret_id
     ) VALUES (
       $1::uuid, 'unit5-admin', 'Unit 5 administrator', 'ADMIN', 'ACTIVE',
       'ARGON2ID', $2::jsonb, $3, $4, $5::uuid
     )`,
    [
      administratorId,
      JSON.stringify(password.parameters),
      Buffer.from(password.salt, 'base64'),
      Buffer.from(password.hash, 'base64'),
      secretId,
    ],
  );
  administratorRecoveryCodes = generateRecoveryCodes(4);
  const batchId = randomUUID();
  await requirePool().query(
    `INSERT INTO ape_dts_platform.user_recovery_code_batches (id, user_id, state)
     VALUES ($1::uuid, $2::uuid, 'ACTIVE')`,
    [batchId, administratorId],
  );
  for (const [index, code] of administratorRecoveryCodes.entries()) {
    const hashed = hashRecoveryCode(code);
    await requirePool().query(
      `INSERT INTO ape_dts_platform.user_recovery_codes (
         id, batch_id, ordinal, code_salt, code_hash
       ) VALUES ($1::uuid, $2::uuid, $3, $4, $5)`,
      [randomUUID(), batchId, index + 1, hashed.salt, hashed.hash],
    );
  }
}

interface HttpRequestOptions {
  readonly body?: unknown;
  readonly cookie?: string;
  readonly csrf?: string;
}

async function httpRequest(
  method: 'DELETE' | 'GET' | 'PATCH' | 'POST',
  path: string,
  options: HttpRequestOptions = {},
): Promise<Response> {
  const headers = new Headers();
  if (options.body !== undefined) {
    headers.set('content-type', 'application/json');
  }
  if (options.cookie !== undefined) {
    headers.set('cookie', options.cookie);
  }
  if (options.csrf !== undefined) {
    headers.set('x-csrf-token', options.csrf);
  }
  return await fetch(`${await requireApp().getUrl()}${path}`, {
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    headers,
    method,
  });
}

function sessionCookie(response: Response): string {
  const header = response.headers.get('set-cookie');
  if (header === null) {
    throw new Error('session cookie was not returned');
  }
  return header.split(';', 1)[0]!;
}

beforeAll(async () => {
  const running = docker(['inspect', '--format', '{{.State.Running}}', postgresContainer]);
  if (running !== 'true') {
    throw new Error(`PostgreSQL container ${postgresContainer} is not running`);
  }
  const portOutput = docker(['port', postgresContainer, '5432/tcp']);
  postgresPort = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(postgresPort) || postgresPort <= 0) {
    throw new Error(`unable to resolve PostgreSQL port: ${portOutput}`);
  }
  psqlAsContainerAdministrator(
    'postgres',
    `CREATE ROLE ${quoteIdentifier(databaseRole)} LOGIN PASSWORD '${databasePassword}';
     CREATE DATABASE ${quoteIdentifier(metadataDatabase)} OWNER ${quoteIdentifier(databaseRole)};`,
  );
  pool = new Pool({
    database: metadataDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port: postgresPort,
    user: databaseRole,
  });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });

  const module = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(RUNTIME_ENVIRONMENT_CHECK)
    .useValue(new RuntimeEnvironmentInspector({}))
    .overrideProvider(ARGON2ID_POLICY)
    .useValue(testKdfPolicy)
    .compile();
  app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter({ logger: false }));
  app.setGlobalPrefix('api');
  app.enableShutdownHooks();
  const connection = {
    database: metadataDatabase,
    host: '127.0.0.1',
    password: databasePassword,
    port: postgresPort,
    tls: { mode: 'DISABLE' as const },
    username: databaseRole,
  };
  await app.get(PlatformDatabaseService).inspectAndUse(connection, bundle.manifest);
  const dek = randomBytes(32);
  app.get(UnlockedKeyService).set(dek);
  await insertAdministrator(dek);
  dek.fill(0);
  await app.listen(0, '127.0.0.1');
  app.get(PlatformReadinessService).markReady();
}, 120_000);

afterAll(async () => {
  administratorTotpSecret.fill(0);
  const failures: unknown[] = [];
  async function attempt(cleanup: () => Promise<void> | void): Promise<void> {
    try {
      await cleanup();
    } catch (error) {
      failures.push(error);
    }
  }

  await attempt(() => app?.close());
  await attempt(() => pool?.end());
  await attempt(() => {
    psqlAsContainerAdministrator(
      'postgres',
      `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = '${metadataDatabase}'
           AND pid <> pg_backend_pid();
       DROP DATABASE IF EXISTS ${quoteIdentifier(metadataDatabase)};
       DROP ROLE IF EXISTS ${quoteIdentifier(databaseRole)};`,
    );
  });
  if (failures.length > 0) {
    throw new AggregateError(failures, 'failed to clean up auth HTTP test resources');
  }
});

describe('authentication and user management HTTP flow', () => {
  it('enforces factors, CSRF, RBAC, session invalidation and audit integrity', async () => {
    const unknownAccount = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: 'incorrect password value',
        username: 'missing-user',
      },
    });
    const wrongPassword = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: 'incorrect password value',
        username: 'unit5-admin',
      },
    });
    expect(unknownAccount.status).toBe(401);
    expect(wrongPassword.status).toBe(401);
    expect(await unknownAccount.json()).toEqual(await wrongPassword.json());

    const passwordLogin = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: administratorPassword,
        username: 'UNIT5-ADMIN',
      },
    });
    expect(passwordLogin.status).toBe(200);
    const passwordLoginBody = (await passwordLogin.json()) as {
      status: string;
      csrfToken: string;
      user: UserSummary;
    };
    expect(passwordLoginBody.status).toBe('AUTHENTICATED');
    const cookieHeader = passwordLogin.headers.get('set-cookie');
    expect(cookieHeader).not.toMatch(/;\s*Secure(?:;|$)/i);
    expect(cookieHeader).toContain('HttpOnly');
    expect(cookieHeader).toContain('SameSite=Strict');
    expect(cookieHeader).toContain('Path=/api');
    const administratorCookie = sessionCookie(passwordLogin);
    const unknownPerformanceTask = randomUUID();
    expect(
      (await httpRequest('GET', `/api/tasks/${unknownPerformanceTask}/performance`)).status,
    ).toBe(401);
    expect(
      (
        await httpRequest('POST', '/api/tasks/performance/query', {
          cookie: administratorCookie,
          body: { taskIds: [unknownPerformanceTask] },
        })
      ).status,
    ).toBe(403);
    const performanceQuery = await httpRequest('POST', '/api/tasks/performance/query', {
      cookie: administratorCookie,
      csrf: passwordLoginBody.csrfToken,
      body: { taskIds: [unknownPerformanceTask] },
    });
    expect(performanceQuery.status).toBe(200);
    expect(await performanceQuery.json()).toEqual({ items: [] });
    expect(
      (
        await httpRequest('GET', `/api/tasks/${unknownPerformanceTask}/performance`, {
          cookie: administratorCookie,
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await httpRequest(
          'GET',
          `/api/tasks/${unknownPerformanceTask}/performance?window=unknown`,
          {
            cookie: administratorCookie,
          },
        )
      ).status,
    ).toBe(400);
    const anonymousEnvironment = await httpRequest(
      'GET',
      '/api/settings/runtime-capacity/environment',
    );
    expect(anonymousEnvironment.status).toBe(401);
    const environment = await httpRequest('GET', '/api/settings/runtime-capacity/environment', {
      cookie: administratorCookie,
    });
    expect(environment.status).toBe(200);
    expect(await environment.json()).toMatchObject({
      state: 'FAILED',
      code: 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED',
      details: {
        issues: [expect.objectContaining({ code: 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED' })],
      },
    });
    const recoverySession = passwordLoginBody;
    expect(recoverySession.user).toMatchObject({
      id: administratorId,
      role: 'ADMIN',
    });

    const retiredFactor = await httpRequest('POST', '/api/auth/login/complete', {
      body: { challengeToken: 'x'.repeat(40), code: '123456', method: 'TOTP' },
    });
    expect(retiredFactor.status).toBe(410);

    const refreshed = await httpRequest('GET', '/api/auth/session', {
      cookie: administratorCookie,
    });
    expect(refreshed.status).toBe(200);
    const refreshedBody = (await refreshed.json()) as { csrfToken: string };
    expect(refreshedBody.csrfToken).not.toBe(recoverySession.csrfToken);

    const userList = await httpRequest('GET', '/api/users', {
      cookie: administratorCookie,
    });
    expect(userList.status).toBe(200);

    const staleCsrf = await httpRequest('POST', '/api/users', {
      body: {
        displayName: 'Rejected developer',
        role: 'DEVELOPER',
        username: 'stale-csrf-user',
      },
      cookie: administratorCookie,
      csrf: recoverySession.csrfToken,
    });
    expect(staleCsrf.status).toBe(403);
    expect(await staleCsrf.json()).toMatchObject({ code: 'CSRF_INVALID' });

    await requirePool().query(
      `UPDATE ape_dts_platform.auth_sessions
       SET created_at = statement_timestamp() - interval '10 minutes',
           reauthenticated_at = statement_timestamp() - interval '10 minutes'
       WHERE user_id = $1::uuid AND revoked_at IS NULL`,
      [administratorId],
    );

    const finalAdministratorDisable = await httpRequest(
      'PATCH',
      `/api/users/${administratorId}/status`,
      {
        body: { status: 'DISABLED' },
        cookie: administratorCookie,
        csrf: refreshedBody.csrfToken,
      },
    );
    expect(finalAdministratorDisable.status).toBe(409);
    expect(await finalAdministratorDisable.json()).toMatchObject({
      code: 'FINAL_ACTIVE_ADMINISTRATOR',
    });

    const invitation = await httpRequest('POST', '/api/users', {
      body: {
        displayName: 'Unit 5 developer',
        role: 'DEVELOPER',
        username: 'unit5-developer',
      },
      cookie: administratorCookie,
      csrf: refreshedBody.csrfToken,
    });
    expect(invitation.status).toBe(201);
    const invitationBody = (await invitation.json()) as {
      activationCode: string;
      user: { id: string; status: string };
    };
    expect(invitationBody.user.status).toBe('INVITED');
    const invitedCredentials = await requirePool().query(
      `SELECT password_hash, password_salt
       FROM ape_dts_platform.users
       WHERE id = $1::uuid`,
      [invitationBody.user.id],
    );
    expect(invitedCredentials.rows).toEqual([{ password_hash: null, password_salt: null }]);

    const developerPassword = 'developer password for unit five';
    const activation = await httpRequest('POST', '/api/users/activate', {
      body: {
        activationCode: invitationBody.activationCode,
        password: developerPassword,
        username: 'unit5-developer',
      },
    });
    expect(activation.status).toBe(200);
    expect(await activation.json()).toMatchObject({ status: 'ACTIVE' });

    const developerLogin = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: developerPassword,
        username: 'unit5-developer',
      },
    });
    expect(developerLogin.status).toBe(200);
    const developerCookie = sessionCookie(developerLogin);
    const developerSession = (await developerLogin.json()) as {
      csrfToken: string;
      user: UserSummary;
    };
    expect(developerSession.user.role).toBe('DEVELOPER');
    expect(
      (
        await httpRequest('GET', '/api/settings/runtime-capacity/environment', {
          cookie: developerCookie,
        })
      ).status,
    ).toBe(403);
    const forbiddenUsers = await httpRequest('GET', '/api/users', {
      cookie: developerCookie,
    });
    expect(forbiddenUsers.status).toBe(403);

    const forbiddenUserCreation = await httpRequest('POST', '/api/users', {
      body: { displayName: 'Forbidden', role: 'DEVELOPER', username: 'forbidden-user' },
      cookie: developerCookie,
      csrf: developerSession.csrfToken,
    });
    expect(forbiddenUserCreation.status).toBe(403);
    expect(await forbiddenUserCreation.json()).toMatchObject({ code: 'ADMIN_ROLE_REQUIRED' });

    const disableDeveloper = await httpRequest(
      'PATCH',
      `/api/users/${developerSession.user.id}/status`,
      {
        body: { status: 'DISABLED' },
        cookie: administratorCookie,
        csrf: refreshedBody.csrfToken,
      },
    );
    expect(disableDeveloper.status).toBe(200);
    const invalidatedDeveloperSession = await httpRequest('GET', '/api/auth/session', {
      cookie: developerCookie,
    });
    expect(invalidatedDeveloperSession.status).toBe(401);

    const continuedAdministratorSession = await httpRequest('GET', '/api/auth/session', {
      cookie: administratorCookie,
    });
    expect(continuedAdministratorSession.status).toBe(200);
    const continuedAdministratorSessionBody = (await continuedAdministratorSession.json()) as {
      csrfToken: string;
    };

    const enableDeveloper = await httpRequest(
      'PATCH',
      '/api/users/' + developerSession.user.id + '/status',
      {
        body: { status: 'ACTIVE' },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(enableDeveloper.status).toBe(200);

    const enabledDeveloperLogin = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: developerPassword,
        username: 'unit5-developer',
      },
    });
    expect(enabledDeveloperLogin.status).toBe(200);
    const enabledDeveloperCookie = sessionCookie(enabledDeveloperLogin);
    const enabledDeveloperSession = (await enabledDeveloperLogin.json()) as {
      csrfToken: string;
      user: UserSummary;
    };

    const kafkaDataSourceRequest = {
      configuration: {
        bootstrapServers: ['kafka.unit-test.internal:9092'],
        plaintextRiskAcknowledged: true,
        type: 'KAFKA',
      },
      credentials: { mode: 'NONE' },
      name: 'Unit 6 HTTP Kafka source',
    };
    expect((await httpRequest('GET', '/api/data-sources')).status).toBe(401);
    expect(
      (
        await httpRequest('POST', '/api/data-sources', {
          body: kafkaDataSourceRequest,
          cookie: administratorCookie,
        })
      ).status,
    ).toBe(403);
    expect(
      (
        await httpRequest('POST', '/api/data-sources', {
          body: kafkaDataSourceRequest,
          cookie: enabledDeveloperCookie,
          csrf: enabledDeveloperSession.csrfToken,
        })
      ).status,
    ).toBe(403);

    const createdDataSource = await httpRequest('POST', '/api/data-sources', {
      body: kafkaDataSourceRequest,
      cookie: administratorCookie,
      csrf: continuedAdministratorSessionBody.csrfToken,
    });
    expect(createdDataSource.status).toBe(201);
    const createdDataSourceBody = (await createdDataSource.json()) as {
      id: string;
      rowVersion: string;
    };

    const updatedDataSource = await httpRequest(
      'PATCH',
      `/api/data-sources/${createdDataSourceBody.id}`,
      {
        body: {
          ...kafkaDataSourceRequest,
          expectedRowVersion: createdDataSourceBody.rowVersion,
          name: 'Unit 6 HTTP Kafka source updated',
        },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(updatedDataSource.status).toBe(200);
    const updatedDataSourceBody = (await updatedDataSource.json()) as {
      id: string;
      rowVersion: string;
    };
    expect(updatedDataSourceBody.id).toBe(createdDataSourceBody.id);

    const copyPath = `/api/data-sources/${updatedDataSourceBody.id}/copy`;
    const copyBody = {
      name: 'unit 6 http kafka copy',
      expectedRowVersion: updatedDataSourceBody.rowVersion,
    };
    expect((await httpRequest('POST', copyPath, { body: copyBody })).status).toBe(401);
    expect(
      (await httpRequest('POST', copyPath, { body: copyBody, cookie: administratorCookie })).status,
    ).toBe(403);
    expect(
      (
        await httpRequest('POST', copyPath, {
          body: copyBody,
          cookie: enabledDeveloperCookie,
          csrf: enabledDeveloperSession.csrfToken,
        })
      ).status,
    ).toBe(403);
    const copiedDataSource = await httpRequest('POST', copyPath, {
      body: copyBody,
      cookie: administratorCookie,
      csrf: continuedAdministratorSessionBody.csrfToken,
    });
    expect(copiedDataSource.status).toBe(201);
    expect(await copiedDataSource.json()).toMatchObject({
      name: copyBody.name,
      configuration: kafkaDataSourceRequest.configuration,
      state: 'ACTIVE',
    });

    const grantDataSource = await httpRequest(
      'POST',
      `/api/data-sources/${updatedDataSourceBody.id}/grants`,
      {
        body: { userId: enabledDeveloperSession.user.id },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(grantDataSource.status).toBe(201);
    const developerDataSources = await httpRequest('GET', '/api/data-sources', {
      cookie: enabledDeveloperCookie,
    });
    expect(developerDataSources.status).toBe(200);
    expect(await developerDataSources.json()).toMatchObject({
      dataSources: [expect.objectContaining({ id: updatedDataSourceBody.id })],
    });

    const revokeDataSource = await httpRequest(
      'DELETE',
      `/api/data-sources/${updatedDataSourceBody.id}/grants/${enabledDeveloperSession.user.id}`,
      {
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(revokeDataSource.status).toBe(204);
    const revokedMetadata = await httpRequest(
      'POST',
      `/api/data-sources/${updatedDataSourceBody.id}/metadata`,
      {
        body: { resource: 'TOPICS' },
        cookie: enabledDeveloperCookie,
        csrf: enabledDeveloperSession.csrfToken,
      },
    );
    expect(revokedMetadata.status).toBe(403);
    expect(await revokedMetadata.json()).toMatchObject({ code: 'DATA_SOURCE_ACCESS_DENIED' });

    const deletedDataSource = await httpRequest(
      'DELETE',
      `/api/data-sources/${updatedDataSourceBody.id}`,
      {
        body: { expectedRowVersion: updatedDataSourceBody.rowVersion },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(deletedDataSource.status).toBe(200);
    expect(await deletedDataSource.json()).toMatchObject({
      id: updatedDataSourceBody.id,
      state: 'DELETED',
    });

    const revokeDeveloperSessions = await httpRequest(
      'POST',
      '/api/users/' + developerSession.user.id + '/sessions/revoke',
      {
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(revokeDeveloperSessions.status).toBe(204);
    expect(
      (
        await httpRequest('GET', '/api/auth/session', {
          cookie: enabledDeveloperCookie,
        })
      ).status,
    ).toBe(401);

    const credentialReset = await httpRequest(
      'POST',
      '/api/users/' + developerSession.user.id + '/credentials/reset',
      {
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(credentialReset.status).toBe(201);
    const credentialResetBody = (await credentialReset.json()) as {
      activationCode: string;
    };
    expect(
      (
        await httpRequest('POST', '/api/auth/login', {
          body: {
            password: developerPassword,
            username: 'unit5-developer',
          },
        })
      ).status,
    ).toBe(401);

    const resetDeveloperPassword = 'reset developer password unit five';
    const resetActivation = await httpRequest('POST', '/api/users/activate', {
      body: {
        activationCode: credentialResetBody.activationCode,
        password: resetDeveloperPassword,
        username: 'unit5-developer',
      },
    });
    expect(resetActivation.status).toBe(200);
    const resetDeveloperLogin = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: resetDeveloperPassword,
        username: 'unit5-developer',
      },
    });
    expect(resetDeveloperLogin.status).toBe(200);
    const resetDeveloperCookie = sessionCookie(resetDeveloperLogin);

    const elevation = await httpRequest(
      'PATCH',
      '/api/users/' + developerSession.user.id + '/role',
      {
        body: { role: 'ADMIN' },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(elevation.status).toBe(200);
    const elevationBody = (await elevation.json()) as {
      activationCode: string;
      user: { status: string };
    };
    expect(elevationBody.user.status).toBe('ACTIVE');
    expect(
      (
        await httpRequest('GET', '/api/auth/session', {
          cookie: resetDeveloperCookie,
        })
      ).status,
    ).toBe(401);

    const promotedAdministratorPassword = resetDeveloperPassword;
    const promotedAdministratorLogin = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: promotedAdministratorPassword,
        username: 'unit5-developer',
      },
    });
    const promotedAdministratorSession = promotedAdministratorLogin;
    expect(promotedAdministratorSession.status).toBe(200);
    const promotedAdministratorCookie = sessionCookie(promotedAdministratorSession);
    expect(
      (
        await httpRequest('GET', '/api/users', {
          cookie: promotedAdministratorCookie,
        })
      ).status,
    ).toBe(200);

    const demotion = await httpRequest(
      'PATCH',
      '/api/users/' + developerSession.user.id + '/role',
      {
        body: { role: 'DEVELOPER' },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(demotion.status).toBe(200);
    expect(
      (
        await httpRequest('GET', '/api/auth/session', {
          cookie: promotedAdministratorCookie,
        })
      ).status,
    ).toBe(401);

    const finalAdministratorDemotion = await httpRequest(
      'PATCH',
      '/api/users/' + administratorId + '/role',
      {
        body: { role: 'DEVELOPER' },
        cookie: administratorCookie,
        csrf: continuedAdministratorSessionBody.csrfToken,
      },
    );
    expect(finalAdministratorDemotion.status).toBe(409);
    expect(await finalAdministratorDemotion.json()).toMatchObject({
      code: 'FINAL_ACTIVE_ADMINISTRATOR',
    });

    await requirePool().query(
      `UPDATE ape_dts_platform.auth_sessions
       SET created_at = statement_timestamp() - interval '1 hour',
           idle_expires_at = statement_timestamp() - interval '1 second'
       WHERE user_id = $1::uuid AND revoked_at IS NULL`,
      [administratorId],
    );
    const expiredOperation = await httpRequest('POST', '/api/users', {
      body: { displayName: 'Expired', role: 'DEVELOPER', username: 'expired-user' },
      cookie: administratorCookie,
      csrf: continuedAdministratorSessionBody.csrfToken,
    });
    expect(expiredOperation.status).toBe(401);
    expect(await expiredOperation.json()).toMatchObject({ code: 'SESSION_INVALID' });
    const renewedLogin = await httpRequest('POST', '/api/auth/login', {
      body: { username: 'unit5-admin', password: administratorPassword },
    });
    expect(renewedLogin.status).toBe(200);
    expect(
      (
        await httpRequest('GET', '/api/users', {
          cookie: sessionCookie(renewedLogin),
        })
      ).status,
    ).toBe(200);

    for (let index = 0; index < 5; index += 1) {
      const failed = await httpRequest('POST', '/api/auth/login', {
        body: {
          password: 'incorrect password value',
          username: 'rate-limited-account',
        },
      });
      expect(failed.status).toBe(401);
    }
    const rateLimited = await httpRequest('POST', '/api/auth/login', {
      body: {
        password: 'incorrect password value',
        username: 'rate-limited-account',
      },
    });
    expect(rateLimited.status).toBe(429);
    expect(await rateLimited.json()).toMatchObject({
      code: 'AUTHENTICATION_RATE_LIMITED',
    });

    await expect(requireApp().get(AuditLedgerService).verify()).resolves.toMatchObject({
      valid: true,
    });
  });
});
