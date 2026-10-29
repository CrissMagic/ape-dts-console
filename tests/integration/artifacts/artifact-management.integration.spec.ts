import { execFileSync } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { mkdtemp, mkdir, rm, writeFile, copyFile, readdir, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ArtifactCache, LocalObjectStorage } from '../../../packages/storage/src/index.js';
import {
  loadMigrationBundle,
  MigrationRunner,
  PgMigrationStore,
} from '../../../packages/database/src/index.js';
import type { UserSummary } from '../../../packages/contracts/src/index.js';
import { PlatformDatabaseService } from '../../../apps/api/src/database/platform-database.service.js';
import { AuditLedgerService } from '../../../apps/api/src/audit/audit-ledger.service.js';
import { ArtifactsService } from '../../../apps/api/src/artifacts/artifacts.service.js';
import { ArtifactFiles } from '../../../apps/api/src/artifacts/artifact-files.js';
import { ArtifactsController } from '../../../apps/api/src/artifacts/artifacts.controller.js';
import { TasksService } from '../../../apps/api/src/tasks/tasks.service.js';
import { RuntimeCapacityService } from '../../../apps/api/src/settings/runtime-capacity.service.js';
import { platformVersion } from '../../../apps/api/src/platform-version.js';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { AuthModule } from '../../../apps/api/src/auth/auth.module.js';
import { ArtifactUploadParser } from '../../../apps/api/src/artifacts/artifact-upload-parser.js';
import { ARGON2ID_POLICY } from '../../../apps/api/src/security/security-config.js';
import { hashPassword } from '../../../packages/security/src/index.js';
import type { AuthenticatedFastifyRequest } from '../../../apps/api/src/auth/auth-http.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const databaseName = `scs_artifacts_meta_${suffix}`;
const roleName = `scs_artifacts_${suffix}`;
const password = randomBytes(24).toString('base64url');
const actor: UserSummary = {
  id: randomUUID(),
  displayName: 'Artifact integration administrator',
  username: `artifact-admin-${suffix}`,
  role: 'ADMIN',
  status: 'ACTIVE',
};
let pool: Pool;
let database: PlatformDatabaseService;
let service: ArtifactsService;
let tasks: TasksService;
let controller: ArtifactsController;
let root: string | undefined;
let sha256: string;
let size: number;
let app: NestFastifyApplication;
const loginPassword = randomBytes(24).toString('base64url');
const policy = { memoryKiB: 1024, parallelism: 2, passes: 2, tagLength: 32, version: 1 };

function docker(args: string[], input?: string): string {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
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
function request(): AuthenticatedFastifyRequest {
  return { authContext: { user: actor } } as AuthenticatedFastifyRequest;
}
function registration(version = randomUUID()) {
  return {
    versionIdentifier: version,
    declaredSource: 'Integration fixture: /bin/true, not ape-dts',
    objectKey: 'fixtures/elf-binary',
    sha256,
    sizeBytes: size,
  };
}

beforeAll(async () => {
  const portText = docker(['port', container, '5432/tcp']);
  const port = Number(portText.slice(portText.lastIndexOf(':') + 1));
  if (!Number.isSafeInteger(port) || port <= 0) throw new Error('Invalid PostgreSQL test port');
  admin(
    `CREATE ROLE "${roleName}" LOGIN PASSWORD '${password}'; CREATE DATABASE "${databaseName}" OWNER "${roleName}";`,
  );
  pool = new Pool({ database: databaseName, host: '127.0.0.1', user: roleName, password, port });
  const bundle = await loadMigrationBundle();
  await new MigrationRunner(new PgMigrationStore(pool)).migrate(bundle, randomUUID(), {
    expectedState: 'ABSENT',
  });
  database = new PlatformDatabaseService();
  await database.inspectAndUse(
    {
      database: databaseName,
      host: '127.0.0.1',
      username: roleName,
      password,
      port,
      tls: { mode: 'DISABLE' },
    },
    bundle.manifest,
  );
  const hashed = await hashPassword(loginPassword, policy);
  await pool.query(
    `INSERT INTO ape_dts_platform.users (id, normalized_username, display_name, role, status,
    password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash)
    VALUES ($1, $2, $3, 'ADMIN', 'ACTIVE', 'ARGON2ID', $4::jsonb, $5, $6)`,
    [
      actor.id,
      actor.username,
      actor.displayName,
      JSON.stringify(hashed.parameters),
      Buffer.from(hashed.salt, 'base64'),
      Buffer.from(hashed.hash, 'base64'),
    ],
  );
  await pool.query(
    `INSERT INTO ape_dts_platform.storage_profiles (id, profile_key, display_name, version, driver,
    config, state, created_by_user_id, activated_at)
    VALUES ($1, 'artifact-integration', 'Artifact integration', 1, 'LOCAL', '{"relative_prefix":"prefix"}',
      'ACTIVE_WRITE', $2, statement_timestamp())`,
    [randomUUID(), actor.id],
  );
  root = await mkdtemp(path.join(tmpdir(), 'scs-artifact-management-'));
  await mkdir(path.join(root, 'storage', 'prefix', 'fixtures'), { recursive: true });
  const binary = execFileSync('docker', ['exec', container, 'cat', '/bin/true'], {
    timeout: 120_000,
  });
  sha256 = createHash('sha256').update(binary).digest('hex');
  size = binary.length;
  await writeFile(path.join(root, 'storage', 'prefix', 'fixtures', 'elf-binary'), binary);
  const audit = new AuditLedgerService(database);
  service = new ArtifactsService(
    database,
    audit,
    new ArtifactCache(path.join(root, 'cache')),
    new LocalObjectStorage(path.join(root, 'storage')),
    new ArtifactFiles(path.join(root, 'storage')),
  );
  controller = new ArtifactsController(service);
  tasks = new TasksService(database, audit);
  const module = await Test.createTestingModule({
    imports: [AuthModule],
    controllers: [ArtifactsController],
    providers: [ArtifactUploadParser, { provide: ArtifactsService, useValue: service }],
  })
    .overrideProvider(PlatformDatabaseService)
    .useValue(database)
    .overrideProvider(ARGON2ID_POLICY)
    .useValue(policy)
    .compile();
  app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter({ logger: false }));
  app.setGlobalPrefix('api');
  await app.listen(0, '127.0.0.1');
});

afterAll(async () => {
  await app?.close();
  await database?.onApplicationShutdown();
  await pool?.end();
  // Only these generated names are eligible for cleanup; never the preview database.
  if (
    !/^scs_artifacts_meta_\d+_\d+$/u.test(databaseName) ||
    !/^scs_artifacts_\d+_\d+$/u.test(roleName)
  ) {
    throw new Error('Unsafe integration cleanup identifier');
  }
  admin(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${databaseName}' AND pid <> pg_backend_pid();
    DROP DATABASE IF EXISTS "${databaseName}"; DROP ROLE IF EXISTS "${roleName}";`);
  if (
    root &&
    path.dirname(root) === path.resolve(tmpdir()) &&
    path.basename(root).startsWith('scs-artifact-management-')
  ) {
    await rm(root, { recursive: true, force: true });
  }
});

describe('real PostgreSQL artifact management and task draft binding', () => {
  it('logically deletes registrations idempotently, retaining bytes, evidence and audit', async () => {
    const artifact = await service.register(actor, registration());
    const before = await readFile(path.join(root!, 'storage', 'prefix', artifact.objectKey));
    expect(await controller.delete(request(), artifact.id)).toEqual({ deleted: true });
    expect(await controller.delete(request(), artifact.id)).toEqual({ deleted: true });
    expect((await service.list(actor)).some((item) => item.id === artifact.id)).toBe(false);
    await expect(service.verify(actor, artifact.id, true)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_NOT_FOUND' },
    });
    expect(await readFile(path.join(root!, 'storage', 'prefix', artifact.objectKey))).toEqual(
      before,
    );
    expect(
      (
        await pool.query(
          'SELECT count(*)::int AS count FROM ape_dts_platform.artifact_probe_runs WHERE artifact_id = $1',
          [artifact.id],
        )
      ).rows[0].count,
    ).toBeGreaterThan(0);
    expect(
      (
        await pool.query(
          "SELECT count(*)::int AS count FROM ape_dts_platform.audit_events WHERE subject_id = $1 AND action = 'ARTIFACT_DELETED'",
          [artifact.id],
        )
      ).rows[0].count,
    ).toBe(1);
  });
  it('protects current task bindings and serializes new bindings with deletion', async () => {
    const artifact = await service.register(actor, registration());
    const source = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.data_sources
      (id, normalized_name, source_type, configuration, state, created_by_user_id, updated_by_user_id)
      VALUES ($1, $2, 'KAFKA', $3::jsonb, 'ACTIVE', $4, $4)`,
      [
        source,
        `delete-source-${source}`,
        JSON.stringify({
          type: 'KAFKA',
          bootstrapServers: ['127.0.0.1:9092'],
          plaintextRiskAcknowledged: true,
        }),
        actor.id,
      ],
    );
    const task = await tasks.create(actor, {
      artifactId: artifact.id,
      configuration: {},
      name: `delete-binding-${randomUUID()}`,
      sourceDataSourceId: source,
      targetDataSourceId: source,
      taskType: 'KAFKA_KAFKA',
    });
    await expect(service.delete(actor, artifact.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_IN_USE', tasks: [{ id: task.id, displayName: task.name }] },
    });
    if (!task.draft) throw new Error('Missing draft');
    await tasks.saveDraft(actor, task.id, {
      artifactId: null,
      configuration: {},
      revisionId: task.draft.id,
      expectedDraftVersion: task.draft.draftVersion,
      expectedTaskRowVersion: task.rowVersion,
      taskType: 'KAFKA_KAFKA',
    });
    const runId = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.task_runs
      (id, task_id, task_revision_id, artifact_id, status, current_stage)
      VALUES ($1, $2, $3, $4, 'STARTING', 'PREPARING')`,
      [runId, task.id, task.draft.id, artifact.id],
    );
    await expect(service.delete(actor, artifact.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_IN_USE' },
    });
    await pool.query(
      "UPDATE ape_dts_platform.task_runs SET status = 'STOPPED', ended_at = statement_timestamp() WHERE id = $1",
      [runId],
    );
    const deletion = await pool.connect();
    try {
      await deletion.query('BEGIN');
      await deletion.query('SELECT id FROM ape_dts_platform.artifacts WHERE id = $1 FOR UPDATE', [
        artifact.id,
      ]);
      const attempt = pool.query(
        'UPDATE ape_dts_platform.task_revisions SET artifact_id = $1 WHERE id = $2',
        [artifact.id, task.draft.id],
      );
      // Hold the artifact lock while a separate transaction attempts a new binding.
      const rejected = expect(attempt).rejects.toMatchObject({ code: '23514' });
      await deletion.query(
        'UPDATE ape_dts_platform.artifacts SET deleted_at = statement_timestamp() WHERE id = $1',
        [artifact.id],
      );
      await deletion.query('COMMIT');
      await rejected;
    } finally {
      await deletion.query('ROLLBACK');
      deletion.release();
    }
    expect((await tasks.get(actor, task.id)).draft?.artifactId).toBeNull();
    expect(
      (
        await pool.query('SELECT artifact_id FROM ape_dts_platform.task_runs WHERE id = $1', [
          runId,
        ])
      ).rows[0].artifact_id,
    ).toBe(artifact.id);
  });
  it('enforces session, administrator and CSRF checks on deletion over HTTP', async () => {
    const artifact = await service.register(actor, registration());
    const base = await app.getUrl();
    const endpoint = `${base}/api/artifacts/${artifact.id}`;
    expect((await fetch(endpoint, { method: 'DELETE' })).status).toBe(401);
    const login = await fetch(`${base}/api/auth/login`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username: actor.username, password: loginPassword }),
    });
    const cookie = login.headers.get('set-cookie')!.split(';')[0]!;
    const session = await login.json();
    expect((await fetch(endpoint, { method: 'DELETE', headers: { cookie } })).status).toBe(403);
    const developerName = `delete-dev-${randomUUID()}`;
    await pool.query(
      `INSERT INTO ape_dts_platform.users
      (id, normalized_username, display_name, role, status, password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash)
      SELECT $1, $2, 'Delete developer', 'DEVELOPER', 'ACTIVE', password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
      FROM ape_dts_platform.users WHERE id = $3`,
      [randomUUID(), developerName, actor.id],
    );
    const developerLogin = await fetch(`${base}/api/auth/login`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username: developerName, password: loginPassword }),
    });
    const developerSession = await developerLogin.json();
    expect(
      (
        await fetch(endpoint, {
          method: 'DELETE',
          headers: {
            cookie: developerLogin.headers.get('set-cookie')!.split(';')[0]!,
            'x-csrf-token': developerSession.csrfToken,
          },
        })
      ).status,
    ).toBe(403);
    const deleted = await fetch(endpoint, {
      method: 'DELETE',
      headers: { cookie, 'x-csrf-token': session.csrfToken },
    });
    expect(deleted.status).toBe(200);
    expect(await deleted.json()).toEqual({ deleted: true });
  });
  it('records the explicit unsigned-upload policy for runtime without claiming a signature pass', async () => {
    const artifact = await service.register(actor, registration());
    await service.verify(actor, artifact.id);
    const checks = await pool.query(
      `SELECT status,policy_version,signer_id FROM ape_dts_platform.artifact_signature_checks
       WHERE artifact_id=$1::uuid`,
      [artifact.id],
    );
    expect(checks.rows).toHaveLength(2);
    for (const check of checks.rows)
      expect(check).toEqual({
        status: 'NOT_REQUIRED',
        policy_version: 'local-upload-signature-disabled-v1',
        signer_id: null,
      });
    expect((await service.list(actor)).find((item) => item.id === artifact.id)?.state).toBe(
      'INACTIVE',
    );
  });

  it('streams uploads through actual HTTP guards, validates and registers without overwriting', async () => {
    const url = `${await app.getUrl()}/api`;
    const bytes = await readFile(path.join(root!, 'storage/prefix/fixtures/elf-binary'));
    const name = `http-${randomUUID()}`;
    const login = await fetch(`${url}/auth/login`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username: actor.username, password: loginPassword }),
    });
    expect(login.status).toBe(200);
    let session = await login.json();
    let cookie = login.headers.get('set-cookie')!.split(';')[0]!;
    const relogin = async () => {
      const response = await fetch(`${url}/auth/login`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username: actor.username, password: loginPassword }),
      });
      expect(response.status).toBe(200);
      session = await response.json();
      cookie = response.headers.get('set-cookie')!.split(';')[0]!;
    };
    const upload = (headers: Record<string, string> = {}) =>
      fetch(`${url}/artifacts/files/upload`, {
        method: 'POST',
        headers: {
          'content-type': 'application/octet-stream',
          'x-artifact-filename': encodeURIComponent(name),
          ...headers,
        },
        body: bytes,
      });
    expect((await upload()).status).toBe(401);
    expect((await upload({ cookie })).status).toBe(403);
    await pool.query(
      'UPDATE ape_dts_platform.users SET must_change_password = true WHERE id = $1',
      [actor.id],
    );
    const forced = await upload({ cookie, 'x-csrf-token': session.csrfToken });
    expect(forced.status).toBe(403);
    expect(await forced.json()).toMatchObject({ code: 'PASSWORD_CHANGE_REQUIRED' });
    await pool.query(
      `INSERT INTO ape_dts_platform.users (id, normalized_username, display_name, role, status,
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash)
       SELECT $1, 'http-backup-admin', 'HTTP backup admin', 'ADMIN', 'ACTIVE',
       password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
       FROM ape_dts_platform.users WHERE id = $2`,
      [randomUUID(), actor.id],
    );
    await pool.query(
      "UPDATE ape_dts_platform.users SET must_change_password = false, role = 'DEVELOPER' WHERE id = $1",
      [actor.id],
    );
    await relogin();
    expect((await upload({ cookie, 'x-csrf-token': session.csrfToken })).status).toBe(403);
    await pool.query("UPDATE ape_dts_platform.users SET role = 'ADMIN' WHERE id = $1", [actor.id]);
    await relogin();
    expect((await service.listFiles()).files).toEqual([]);
    const before = (await service.list(actor)).length;
    const response = await upload({ cookie, 'x-csrf-token': session.csrfToken });
    expect(response.status).toBe(201);
    const uploaded = await response.json();
    expect(uploaded).toMatchObject({
      file: { name, sizeBytes: size, kind: 'BINARY' },
      prepared: { sha256, sizeBytes: size, suggestedVersion: name },
    });
    const largeBytes = Buffer.concat([bytes, Buffer.alloc(2 * 1024 * 1024)]);
    const largeResponse = await fetch(`${url}/artifacts/files/upload`, {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'x-artifact-filename': `${name}-large`,
        cookie,
        'x-csrf-token': session.csrfToken,
      },
      body: largeBytes,
    });
    expect(largeResponse.status).toBe(201);
    expect(await largeResponse.json()).toMatchObject({
      file: { sizeBytes: largeBytes.length },
      prepared: { sizeBytes: largeBytes.length },
    });
    expect((await service.list(actor)).length).toBe(before);
    const duplicate = await upload({ cookie, 'x-csrf-token': session.csrfToken });
    expect(duplicate.status).toBe(409);
    expect(await duplicate.json()).toMatchObject({ code: 'ARTIFACT_FILE_ALREADY_EXISTS' });
    const registered = await fetch(`${url}/artifacts/files/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', cookie, 'x-csrf-token': session.csrfToken },
      body: JSON.stringify({
        file: uploaded.file,
        expectedSha256: uploaded.prepared.sha256,
        versionIdentifier: name,
        declaredSource: 'HTTP upload fixture, not ape-dts',
      }),
    });
    expect(registered.status).toBe(201);
    expect(await registered.json()).toMatchObject({
      state: 'INACTIVE',
      sha256,
      sizeBytes: String(size),
    });
    expect(await readFile(path.join(root!, 'storage/prefix/artifact-inbox', name))).toEqual(bytes);
  });
  it('selects an inbox binary, previews without DB writes and registers an independent verified copy', async () => {
    const listed = await controller.files();
    const inbox = listed!.directory;
    const name = `ape-dts-${randomUUID()}`;
    await copyFile(
      path.join(root!, 'storage', 'prefix', 'fixtures', 'elf-binary'),
      path.join(inbox, name),
    );
    const file = (await controller.files())!.files.find((entry) => entry.name === name)!;
    expect(file.kind).toBe('BINARY');
    const before = (await service.list(actor)).length;
    const preview = await controller.prepareFile(file);
    expect(preview).toMatchObject({ sha256, sizeBytes: size });
    expect((await service.list(actor)).length).toBe(before);
    await expect(
      controller.registerFile(request(), {
        file,
        expectedSha256: '0'.repeat(64),
        versionIdentifier: name,
        declaredSource: name,
      }),
    ).rejects.toMatchObject({ response: { code: 'ARTIFACT_FILE_CHANGED' } });
    expect((await service.list(actor)).length).toBe(before);
    const input = {
      file,
      expectedSha256: preview!.sha256,
      versionIdentifier: name,
      declaredSource: `Container file: ${name}`,
    };
    const registered = await controller.registerFile(request(), input);
    expect(registered).toMatchObject({
      state: 'INACTIVE',
      sha256,
      sizeBytes: String(size),
      certifications: [],
    });
    expect(registered!.objectKey).toMatch(/^artifacts\/imported\//u);
    const imports = path.join(root!, 'storage', 'prefix', 'artifacts', 'imported');
    const importedBefore = await readdir(imports);
    await expect(controller.registerFile(request(), input)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_VERSION_ALREADY_EXISTS' },
    });
    expect(await readdir(imports)).toEqual(importedBefore);
    // Replacing the inbox source cannot silently alter an already registered binary.
    await writeFile(path.join(inbox, name), 'replaced source');
    await expect(
      controller.registerFile(request(), { ...input, versionIdentifier: `${name}-changed` }),
    ).rejects.toMatchObject({ response: { code: 'ARTIFACT_FILE_CHANGED' } });
    expect(await controller.verify(request(), registered!.id)).toMatchObject({
      verified: true,
      sha256,
    });
    expect(
      createHash('sha256')
        .update(await readFile(path.join(root!, 'storage', 'prefix', registered!.objectKey)))
        .digest('hex'),
    ).toBe(sha256);
  });

  it.each(['gz', 'xz'])(
    'previews and registers a real tar.%s package without certifying or executing it',
    async (format) => {
      const inbox = (await controller.files())!.directory;
      const packaging = await mkdtemp(path.join(root!, 'package-'));
      await copyFile(
        path.join(root!, 'storage', 'prefix', 'fixtures', 'elf-binary'),
        path.join(packaging, 'ape-dts'),
      );
      const name = `package-${randomUUID()}.tar.${format}`;
      const archive = execFileSync('tar', ['-cf', '-', '-C', packaging, 'ape-dts']);
      await writeFile(
        path.join(inbox, name),
        format === 'gz'
          ? gzipSync(archive)
          : execFileSync('xz', ['--compress', '--stdout'], { input: archive }),
      );
      const file = (await controller.files())!.files.find((entry) => entry.name === name)!;
      const preview = await controller.prepareFile(file);
      expect(preview).toMatchObject({ sha256, sizeBytes: size });
      const registered = await controller.registerFile(request(), {
        file,
        expectedSha256: preview!.sha256,
        versionIdentifier: name,
        declaredSource: 'Disposable test archive',
      });
      expect(registered).toMatchObject({ state: 'INACTIVE', certifications: [], sha256 });
    },
  );
  it('updates only runtime capacity with version checks and atomic audit, and rejects limits below real reservations', async () => {
    const capacity = new RuntimeCapacityService(database, new AuditLedgerService(database));
    await pool.query(`INSERT INTO ape_dts_platform.platform_settings
      (singleton_id, log_retention_days, run_retention_days) VALUES (1, 41, 401)`);
    expect(await capacity.get()).toEqual({ maxRuntimeUnits: null, rowVersion: '1', used: 0 });
    expect(
      await capacity.update(actor.id, { expectedRowVersion: '1', maxRuntimeUnits: 4 }),
    ).toEqual({ maxRuntimeUnits: 4, rowVersion: '2', used: 0 });
    const persisted = await pool.query(`SELECT max_runtime_units, row_version::text, updated_by,
      log_retention_days, run_retention_days, xmin::text AS transaction_id
      FROM ape_dts_platform.platform_settings WHERE singleton_id = 1`);
    expect(persisted.rows[0]).toMatchObject({
      max_runtime_units: 4,
      row_version: '2',
      updated_by: actor.id,
      log_retention_days: 41,
      run_retention_days: 401,
    });
    const audit =
      await pool.query(`SELECT actor_type, actor_user_id, canonical_details, xmin::text AS transaction_id
      FROM ape_dts_platform.audit_events WHERE action = 'RUNTIME_CAPACITY_UPDATED' ORDER BY sequence DESC LIMIT 1`);
    expect(audit.rows[0]).toMatchObject({
      actor_type: 'USER',
      actor_user_id: actor.id,
      canonical_details: { previousLimit: null, limit: 4, used: 0 },
      transaction_id: persisted.rows[0].transaction_id,
    });
    await expect(
      capacity.update(actor.id, { expectedRowVersion: '1', maxRuntimeUnits: 8 }),
    ).rejects.toMatchObject({ response: { code: 'CAPACITY_VERSION_CONFLICT' } });
    expect(await capacity.get()).toEqual({ maxRuntimeUnits: 4, rowVersion: '2', used: 0 });

    const source = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.data_sources
      (id, normalized_name, source_type, configuration, state, created_by_user_id, updated_by_user_id)
      VALUES ($1, $2, 'KAFKA', $3::jsonb, 'ACTIVE', $4, $4)`,
      [
        source,
        `capacity-source-${suffix}`,
        JSON.stringify({
          bootstrapServers: ['127.0.0.1:9092'],
          plaintextRiskAcknowledged: true,
          type: 'KAFKA',
        }),
        actor.id,
      ],
    );
    const task = await tasks.create(actor, {
      configuration: {},
      name: `capacity-task-${suffix}`,
      sourceDataSourceId: source,
      targetDataSourceId: source,
      taskType: 'KAFKA_KAFKA',
    });
    const runId = randomUUID();
    await pool.query(
      `INSERT INTO ape_dts_platform.task_runs
      (id, task_id, task_revision_id, status, current_stage) VALUES ($1, $2, $3, 'RUNNING', 'FORWARD')`,
      [runId, task.id, task.draft!.id],
    );
    for (const unit of ['partition-0', 'partition-1']) {
      await pool.query(
        `INSERT INTO ape_dts_platform.capacity_reservations
        (id, task_id, task_revision_id, run_id, unit_key, state) VALUES ($1, $2, $3, $4, $5, 'ACTIVE')`,
        [randomUUID(), task.id, task.draft!.id, runId, unit],
      );
    }
    expect(await capacity.get()).toEqual({ maxRuntimeUnits: 4, rowVersion: '2', used: 2 });
    await expect(
      capacity.update(actor.id, { expectedRowVersion: '2', maxRuntimeUnits: 1 }),
    ).rejects.toMatchObject({ response: { code: 'CAPACITY_BELOW_USAGE', used: 2 } });
    expect(
      await capacity.update(actor.id, { expectedRowVersion: '2', maxRuntimeUnits: 2 }),
    ).toEqual({ maxRuntimeUnits: 2, rowVersion: '3', used: 2 });

    // A real PostgreSQL failure in the audit insert must roll back the preceding settings update.
    await pool.query(`CREATE FUNCTION ape_dts_platform.reject_capacity_audit_fixture() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN IF NEW.action = 'RUNTIME_CAPACITY_UPDATED' THEN
      RAISE EXCEPTION 'integration audit failure'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER reject_capacity_audit_fixture BEFORE INSERT ON ape_dts_platform.audit_events
      FOR EACH ROW EXECUTE FUNCTION ape_dts_platform.reject_capacity_audit_fixture()`);
    try {
      await expect(
        capacity.update(actor.id, { expectedRowVersion: '3', maxRuntimeUnits: 6 }),
      ).rejects.toThrow('integration audit failure');
    } finally {
      await pool.query(
        'DROP TRIGGER reject_capacity_audit_fixture ON ape_dts_platform.audit_events; DROP FUNCTION ape_dts_platform.reject_capacity_audit_fixture()',
      );
    }
    expect(await capacity.get()).toEqual({ maxRuntimeUnits: 2, rowVersion: '3', used: 2 });
    expect(
      (
        await pool.query(`SELECT count(*)::integer AS count FROM ape_dts_platform.audit_events
      WHERE action = 'RUNTIME_CAPACITY_UPDATED'`)
      ).rows[0].count,
    ).toBe(2);
  });
  it('requires a test-only passed behavior record before activation and rejects quarantine afterward', async () => {
    const registered = await service.register(actor, registration());
    const probeId = randomUUID();
    // This synthetic certification exists only in this disposable test database.
    // It exercises the database activation guard and must never be imported as ape-dts evidence.
    await pool.query(
      `INSERT INTO ape_dts_platform.artifact_probe_runs
      (id, artifact_id, probe_kind, probe_version, status, started_by_user_id, ended_at, evidence_summary)
      VALUES ($1, $2, 'BEHAVIOR', 'integration-fixture-only', 'PASSED', $3, statement_timestamp(),
        '{"fixtureOnly":true}'::jsonb)`,
      [probeId, registered.id, actor.id],
    );
    await pool.query(
      `INSERT INTO ape_dts_platform.artifact_certifications
      (id, artifact_id, platform_version, task_scenario, adapter_id, adapter_version, state, evidence_probe_run_id)
      VALUES ($1, $2, 'integration-fixture-only', 'KAFKA_RAW_FORWARD', 'integration-fixture-only', '1', 'PASSED', $3)`,
      [randomUUID(), registered.id, probeId],
    );
    await expect(controller.activate(request(), registered.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_CERTIFICATION_MISSING' },
      status: 409,
    });
    // Old-platform certification cannot enable a binary for the current platform.
    expect((await service.list(actor)).find((item) => item.id === registered.id)?.state).toBe(
      'INACTIVE',
    );
    await pool.query(
      'UPDATE ape_dts_platform.artifact_certifications SET platform_version = $2 WHERE artifact_id = $1',
      [registered.id, platformVersion()],
    );
    expect(await controller.activate(request(), registered.id)).toMatchObject({
      state: 'ACTIVE',
      verified: true,
    });
    expect(
      (await service.list({ ...actor, role: 'DEVELOPER' })).some(
        (item) => item.id === registered.id,
      ),
    ).toBe(true);
    await pool.query(
      `UPDATE ape_dts_platform.artifacts SET state = 'QUARANTINED', state_reason = 'integration fixture quarantine' WHERE id = $1`,
      [registered.id],
    );
    await expect(controller.verify(request(), registered.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_QUARANTINED' },
      status: 409,
    });
    await expect(controller.activate(request(), registered.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_QUARANTINED' },
      status: 409,
    });
  });
  it('registers and verifies actual ELF bytes without inventing certification or enabling execution', async () => {
    const registered = await controller.register(request(), registration());
    expect(registered).toMatchObject({
      state: 'INACTIVE',
      verified: true,
      sha256,
      sizeBytes: String(size),
      certifications: [],
    });
    if (!registered) throw new Error('Registration returned no artifact');
    const verified = await controller.verify(request(), registered.id);
    expect(verified).toMatchObject({ state: 'INACTIVE', verified: true });
    await expect(controller.activate(request(), registered.id)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_CERTIFICATION_MISSING' },
      status: 409,
    });
    const result = await pool.query(
      `SELECT a.state, o.created_by_actor_type, count(p.id)::integer AS probes
      FROM ape_dts_platform.artifacts a JOIN ape_dts_platform.stored_objects o ON o.id = a.stored_object_id
      JOIN ape_dts_platform.artifact_probe_runs p ON p.artifact_id = a.id WHERE a.id = $1
      GROUP BY a.state, o.created_by_actor_type`,
      [registered.id],
    );
    expect(result.rows[0]).toEqual({ state: 'INACTIVE', created_by_actor_type: 'USER', probes: 2 });
    expect(
      (await service.list({ ...actor, role: 'DEVELOPER' })).some(
        (item) => item.id === registered.id,
      ),
    ).toBe(false);
    expect(
      (
        await pool.query(
          'SELECT count(*)::integer AS count FROM ape_dts_platform.artifact_certifications WHERE artifact_id = $1',
          [registered.id],
        )
      ).rows[0]?.count,
    ).toBe(0);
  });

  it('maps duplicate versions, missing files and bad hashes without persisting invalid registrations', async () => {
    const input = registration();
    await controller.register(request(), input);
    await expect(controller.register(request(), input)).rejects.toMatchObject({
      response: { code: 'ARTIFACT_VERSION_ALREADY_EXISTS' },
      status: 409,
    });
    const invalid = registration();
    await expect(
      controller.register(request(), { ...invalid, sha256: '0'.repeat(64) }),
    ).rejects.toMatchObject({
      response: { code: 'ARTIFACT_SHA256_MISMATCH' },
      status: 409,
    });
    await expect(
      controller.register(request(), { ...invalid, objectKey: 'fixtures/missing' }),
    ).rejects.toMatchObject({
      response: { code: 'ARTIFACT_FILE_NOT_FOUND' },
      status: 409,
    });
    expect(
      (
        await pool.query(
          'SELECT count(*)::integer AS count FROM ape_dts_platform.artifacts WHERE version_identifier = $1',
          [invalid.versionIdentifier],
        )
      ).rows[0]?.count,
    ).toBe(0);
  });

  it('persists a selected artifact, preserves omitted selection, clears explicit null and rejects stale rebinding', async () => {
    const artifact = await service.register(actor, registration());
    const source = randomUUID();
    const target = randomUUID();
    for (const id of [source, target]) {
      await pool.query(
        `INSERT INTO ape_dts_platform.data_sources
        (id, normalized_name, source_type, configuration, state, created_by_user_id, updated_by_user_id)
        VALUES ($1, $2, 'KAFKA', $3::jsonb, 'ACTIVE', $4, $4)`,
        [
          id,
          `artifact-kafka-${id}`,
          JSON.stringify({
            bootstrapServers: ['127.0.0.1:9092'],
            plaintextRiskAcknowledged: true,
            type: 'KAFKA',
          }),
          actor.id,
        ],
      );
    }
    const created = await tasks.create(actor, {
      artifactId: artifact.id,
      configuration: {},
      name: `binding-${suffix}`,
      sourceDataSourceId: source,
      targetDataSourceId: target,
      taskType: 'KAFKA_KAFKA',
    });
    expect(created.draft?.artifactId).toBe(artifact.id);
    if (!created.draft) throw new Error('Missing created draft');
    const preserved = await tasks.saveDraft(actor, created.id, {
      configuration: {},
      revisionId: created.draft.id,
      expectedDraftVersion: created.draft.draftVersion,
      expectedTaskRowVersion: created.rowVersion,
      taskType: 'KAFKA_KAFKA',
    });
    expect(preserved.draft?.artifactId).toBe(artifact.id);
    if (!preserved.draft) throw new Error('Missing preserved draft');
    const cleared = await tasks.saveDraft(actor, created.id, {
      artifactId: null,
      configuration: {},
      revisionId: preserved.draft.id,
      expectedDraftVersion: preserved.draft.draftVersion,
      expectedTaskRowVersion: preserved.rowVersion,
      taskType: 'KAFKA_KAFKA',
    });
    expect(cleared.draft?.artifactId).toBeNull();
    await expect(
      tasks.saveDraft(actor, created.id, {
        artifactId: artifact.id,
        configuration: {},
        revisionId: preserved.draft.id,
        expectedDraftVersion: preserved.draft.draftVersion,
        expectedTaskRowVersion: preserved.rowVersion,
        taskType: 'KAFKA_KAFKA',
      }),
    ).rejects.toMatchObject({ code: 'TASK_DRAFT_VERSION_CONFLICT' });
    expect((await tasks.get(actor, created.id)).draft?.artifactId).toBeNull();
    expect(
      (
        await pool.query('SELECT artifact_id FROM ape_dts_platform.task_revisions WHERE id = $1', [
          preserved.draft.id,
        ])
      ).rows[0]?.artifact_id,
    ).toBeNull();
  });
});
