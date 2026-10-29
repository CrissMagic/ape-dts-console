import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

import {
  ApeDtsV1Adapter,
  CertifiedAdapterRegistry,
  type PlatformTaskConfig,
} from '@scs/config-model';
import { encryptSecret } from '@scs/security';
import { ArtifactCache, LocalObjectStorage } from '@scs/storage';
import { afterEach, describe, expect, it } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import {
  AuthenticatedRuntimeLaunchSpecError,
  AuthenticatedRuntimeLaunchSpecFactory,
  type AuthenticatedRuntimeLaunchSpecOptions,
} from './authenticated-runtime-launch-spec.js';
import type { RuntimeLaunchSpecFactoryInput } from './runtime-launch-spec.js';

const taskId = '11111111-1111-4111-8111-111111111111';
const revisionId = '22222222-2222-4222-8222-222222222222';
const runId = '33333333-3333-4333-8333-333333333333';
const unitId = '44444444-4444-4444-8444-444444444444';
const sourceId = '55555555-5555-4555-8555-555555555555';
const targetId = '66666666-6666-4666-8666-666666666666';
const artifactId = '77777777-7777-4777-8777-777777777777';
const secretId = '88888888-8888-4888-8888-888888888888';
const dek = Buffer.alloc(32, 7);
const roots: string[] = [];

const kafkaConfig = {
  kind: 'KAFKA_RAW_FORWARD' as const,
  schemaVersion: 1 as const,
  source: { dataSourceId: sourceId, groupId: 'runtime-group', topic: 'runtime.source' },
  target: { dataSourceId: targetId, topic: 'runtime.target' },
  tuning: {
    ackIntervalSeconds: 5,
    batchSize: 100,
    bufferSize: 16_000,
    checkpointIntervalSeconds: 15,
    requiredAcks: 'all' as const,
  },
};

const pgConfig = {
  kind: 'PG_TO_KAFKA' as const,
  schemaVersion: 1 as const,
  source: {
    dataSourceId: sourceId,
    includeSnapshot: false,
    publicationName: 'orders_pub',
    resourceMode: 'SELF_MANAGED' as const,
    slotName: 'orders_slot',
    table: { name: 'orders', schema: 'public' },
  },
  target: { dataSourceId: targetId, topic: 'runtime.target' },
  tuning: {
    batchSize: 100,
    bufferSize: 16_000,
    checkpointIntervalSeconds: 15,
    requiredAcks: 'all' as const,
  },
};

const mysqlConfig = {
  kind: 'MYSQL_TO_KAFKA' as const,
  schemaVersion: 2 as const,
  source: { dataSourceId: sourceId, includeSnapshot: true },
  target: {
    dataSourceId: targetId,
    messageFormat: 'json' as const,
    jsonTemplate: 'cloudcanal' as const,
  },
  mappings: [
    { source: { schema: 'orders', name: 'events' }, targets: [{ topic: 'runtime.target' }] },
  ],
  tuning: pgConfig.tuning,
};

afterEach(async () => {
  await Promise.all(
    roots.splice(0).map(async (root) => {
      const { rm } = await import('node:fs/promises');
      await rm(root, { force: true, recursive: true });
    }),
  );
});

function input(config: PlatformTaskConfig = kafkaConfig): RuntimeLaunchSpecFactoryInput {
  return {
    config,
    metricsPort: 35_001,
    revisionId,
    runId,
    taskId,
    unitId,
    unitKey: config.kind === 'KAFKA_RAW_FORWARD' ? 'partition:3' : 'primary',
  };
}

async function fixture(
  config: PlatformTaskConfig = kafkaConfig,
  overrides: Record<string, unknown> = {},
  sourceBytes = Buffer.from('authenticated-ape-dts-binary'),
  mysqlHost = 'mysql.internal',
  pgTls = { mode: 'REQUIRE' } as {
    mode: 'REQUIRE' | 'DISABLE' | 'VERIFY_FULL';
    caCertificate?: string;
    riskAcknowledged?: boolean;
  },
) {
  const root = await import('node:fs/promises').then(({ mkdtemp }) =>
    mkdtemp(path.join(os.tmpdir(), 'runtime-launch-')),
  );
  roots.push(root);
  const storageRoot = path.join(root, 'storage');
  await mkdir(path.join(storageRoot, 'artifacts'), { recursive: true });
  await writeFile(path.join(storageRoot, 'artifacts', 'ape-dts'), sourceBytes);
  const sha256 = createHash('sha256').update(sourceBytes).digest('hex');
  const encrypted = encryptSecret(dek, Buffer.from('s3cret'), {
    aadVersion: 1,
    ownerId: sourceId,
    ownerType: 'DATA_SOURCE',
    purpose: 'CONNECTION_PASSWORD',
    secretId,
  });
  const bindingRow = {
    actual_sha256: sha256,
    adapter_id: 'ape-dts-ini',
    adapter_version: '1',
    artifact_state: 'ACTIVE',
    certification_state: 'PASSED',
    configuration: config,
    declared_sha256: sha256,
    desired_state: 'RUNNING',
    driver: 'LOCAL',
    object_key: 'ape-dts',
    probe_status: 'PASSED',
    relative_prefix: 'artifacts',
    revision_artifact_id: artifactId,
    revision_state: 'APPLIED',
    current_applied_revision_id: revisionId,
    current_draft_revision_id: null,
    signature_status: 'NOT_REQUIRED',
    size_bytes: String(sourceBytes.length),
    source_data_source_id: sourceId,
    stored_object_sha256: sha256,
    stored_object_state: 'AVAILABLE',
    target_data_source_id: targetId,
    task_deleted_at: null,
    task_type:
      config.kind === 'PG_TO_KAFKA'
        ? 'PG_KAFKA'
        : config.kind === 'MYSQL_TO_KAFKA'
          ? 'MYSQL_KAFKA'
          : 'KAFKA_KAFKA',
    ...overrides,
  };
  const kafkaRow = {
    aad_version: null,
    algorithm: null,
    ciphertext: null,
    configuration: {
      bootstrapServers: ['kafka.internal:9092'],
      plaintextRiskAcknowledged: true,
      type: 'KAFKA',
    },
    connection_secret_id: null,
    nonce: null,
    secret_id: null,
    secret_owner_id: null,
    secret_owner_type: null,
    secret_purpose: null,
    source_type: 'KAFKA',
    tag: null,
  };
  const pgRow = {
    aad_version: 1,
    algorithm: encrypted.algorithm,
    ciphertext: Buffer.from(encrypted.ciphertext, 'base64'),
    configuration: {
      database: 'orders',
      host: 'postgres.internal',
      port: 5432,
      tls: pgTls,
      type: 'POSTGRES',
      username: 'replicator',
    },
    connection_secret_id: secretId,
    nonce: Buffer.from(encrypted.nonce, 'base64'),
    secret_id: secretId,
    secret_owner_id: sourceId,
    secret_owner_type: 'DATA_SOURCE',
    secret_purpose: 'CONNECTION_PASSWORD',
    source_type: 'POSTGRES',
    tag: Buffer.from(encrypted.tag, 'base64'),
  };
  const pool = {
    query: async (sql: string, values: readonly unknown[]) => {
      if (sql.includes('SELECT revision.configuration')) {
        return { rows: [{ configuration: config }] };
      }
      if (sql.includes('FROM ape_dts_platform.task_revisions')) return { rows: [bindingRow] };
      if (sql.includes('FROM ape_dts_platform.data_sources')) {
        return {
          rows: [
            values[0] !== sourceId || config.kind === 'KAFKA_RAW_FORWARD'
              ? kafkaRow
              : config.kind === 'MYSQL_TO_KAFKA'
                ? {
                    ...pgRow,
                    source_type: 'MYSQL',
                    configuration: {
                      ...pgRow.configuration,
                      type: 'MYSQL',
                      host: mysqlHost,
                      port: 3306,
                    },
                  }
                : pgRow,
          ],
        };
      }
      throw new Error('UNEXPECTED_QUERY');
    },
  };
  const registry = new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]);
  registry.register({
    adapter: new ApeDtsV1Adapter(),
    artifactSha256: sha256,
    platformVersion: '0.1.0',
    scenario: config.kind === 'KAFKA_RAW_FORWARD' ? 'KAFKA_RAW_FORWARD' : 'PG_CDC_TO_KAFKA',
  });
  const unlockedKey = new UnlockedKeyService();
  unlockedKey.set(dek);
  const options: AuthenticatedRuntimeLaunchSpecOptions = {
    adapters: registry,
    artifactCache: new ArtifactCache(path.join(root, 'cache')),
    database: { requirePool: () => pool } as unknown as PlatformDatabaseService,
    localObjectStorage: new LocalObjectStorage(storageRoot),
    platformVersion: '0.1.0',
    runtimeRoot: '/run/ape-dts',
    unlockedKey,
  };
  return { bindingRow, options, root, sha256 };
}

function expectCode(error: unknown, code: string): boolean {
  expect(error).toBeInstanceOf(AuthenticatedRuntimeLaunchSpecError);
  expect(error).toMatchObject({ code, message: code });
  expect((error as Error).message).not.toContain('s3cret');
  return true;
}

describe('AuthenticatedRuntimeLaunchSpecFactory', () => {
  it.each(['::1', '2001:db8::1'])('renders a valid MySQL runtime URI for IPv6 %s', async (host) => {
    const { options } = await fixture(mysqlConfig, {}, undefined, host);
    const spec = await new AuthenticatedRuntimeLaunchSpecFactory(options).create({
      ...input(mysqlConfig),
      mysql: {
        phase: 'CDC',
        binlogFilename: 'mysql-bin.000003',
        binlogPosition: 120,
        serverId: 2000,
      },
    });
    const connection = spec.iniContent
      .split('\n')
      .find((line) => line.startsWith('url=mysql:'))!
      .slice('url='.length);
    expect(new URL(connection).hostname).toBe(`[${host}]`);
    expect(new URL(connection).port).toBe('3306');
  });
  it('launches MySQL CDC from persisted bindings even when the original task included snapshot', async () => {
    const { options } = await fixture(mysqlConfig);
    const factory = new AuthenticatedRuntimeLaunchSpecFactory(options);
    const spec = await factory.create({
      ...input(mysqlConfig),
      mysql: {
        phase: 'CDC',
        binlogFilename: 'mysql-bin.000003',
        binlogPosition: 1234,
        serverId: 2000,
      },
    });
    expect(spec.iniContent).toContain('db_type=mysql\nextract_type=cdc');
    expect(spec.iniContent).toContain('binlog_filename=mysql-bin.000003');
    expect(spec.iniContent).toContain('binlog_position=1234');
    expect(spec.iniContent).toContain('server_id=2000');
    expect(spec.iniContent).toContain('mysql://replicator:s3cret@mysql.internal:3306/orders');
    expect(spec.iniContent).toContain('ssl_mode=require');
    expect(spec.log4rsContent).toContain('mysql-checkpoint.log');
    expect(spec.sensitiveValues).toContain('s3cret');
    await expect(factory.create(input(mysqlConfig))).rejects.toMatchObject({
      code: 'RUNTIME_MYSQL_POSITION_REQUIRED',
    });
  });

  it.each([
    ['KAFKA_KAFKA', kafkaConfig],
    ['PG_KAFKA', pgConfig],
  ] as const)(
    'creates an authenticated %s launch from the immutable revision',
    async (_type, config) => {
      const { options, sha256 } = await fixture(config);
      const spec = await new AuthenticatedRuntimeLaunchSpecFactory(options).create(input(config));
      expect(spec.executablePath).toContain(sha256);
      expect(spec.args).toEqual(['--config', spec.iniPath]);
      expect(spec.runtimeDirectory).toBe(path.resolve('/run/ape-dts', taskId, runId, unitId));
      expect(spec.iniPath).toBe(path.join(spec.runtimeDirectory, 'config.ini'));
      expect(spec.env).toEqual({});
      expect(spec.log4rsContent).toContain('kind: console');
      expect(spec.log4rsContent).not.toMatch(/file|rolling/iu);
      expect(spec.iniContent).toContain('http_port=35001');
      if (config.kind === 'KAFKA_RAW_FORWARD') {
        expect(spec.iniContent).toContain('partition=3');
        expect(spec.sensitiveValues).toEqual([]);
      } else {
        expect(spec.iniContent).toContain('postgres.internal');
        expect(spec.sensitiveValues).toEqual(['s3cret', expect.stringContaining('postgresql://')]);
        expect(Object.values(spec.env).join('')).not.toContain('s3cret');
      }
    },
  );

  it.each([false, true])(
    'passes the PostgreSQL private CA without downgrading verification (snapshot=%s)',
    async (includeSnapshot) => {
      const config = { ...pgConfig, source: { ...pgConfig.source, includeSnapshot } };
      const { options } = await fixture(config, {}, undefined, undefined, {
        mode: 'VERIFY_FULL',
        caCertificate: 'private-pg-ca',
      });
      const spec = await new AuthenticatedRuntimeLaunchSpecFactory(options).create(input(config));
      const url = new URL(
        spec.iniContent
          .split('\n')
          .find((line) => line.startsWith('url=postgresql:'))!
          .slice(4),
      );
      expect(url.searchParams.get('sslmode')).toBe('verify-full');
      expect(url.searchParams.get('sslrootcert')).toBe(
        `/run/ape-dts/${taskId}/${runId}/${unitId}/postgres-ca.pem`,
      );
      expect(spec.postgresCaCertificate).toBe('private-pg-ca');
      expect(spec.iniContent).not.toContain('private-pg-ca');
      expect(spec.caCertificate).toBeUndefined();
    },
  );

  it.each(['DISABLE', 'REQUIRE'] as const)(
    'preserves PostgreSQL %s without writing a custom CA',
    async (mode) => {
      const { options } = await fixture(pgConfig, {}, undefined, undefined, {
        mode,
        ...(mode === 'DISABLE' ? { riskAcknowledged: true } : {}),
      });
      const spec = await new AuthenticatedRuntimeLaunchSpecFactory(options).create(input(pgConfig));
      expect(spec.postgresCaCertificate).toBeUndefined();
      expect(spec.iniContent).not.toContain('sslrootcert=');
      expect(spec.iniContent).toContain(`sslmode=${mode.toLowerCase()}`);
    },
  );

  it('rejects a PostgreSQL VERIFY_FULL configuration without its CA rather than silently downgrading it', async () => {
    const { options } = await fixture(pgConfig, {}, undefined, undefined, { mode: 'VERIFY_FULL' });
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create(input(pgConfig)),
    ).rejects.toMatchObject({ code: 'RUNTIME_SECRET_UNAVAILABLE' });
  });

  it('downloads through an atomic, idempotent, non-writable hash cache', async () => {
    const { options, sha256 } = await fixture();
    const factory = new AuthenticatedRuntimeLaunchSpecFactory(options);
    await Promise.all([
      factory.create(input()),
      factory.create({ ...input(), unitId: randomUUID() }),
    ]);
    const cached = options.artifactCache.objectPath(sha256);
    expect(await readFile(cached, 'utf8')).toBe('authenticated-ape-dts-binary');
    if (process.platform !== 'win32') expect((await stat(cached)).mode & 0o777).toBe(0o500);
    expect(
      (await import('node:fs/promises')).readdir(path.join(options.artifactCache.root, '.staging')),
    ).resolves.toEqual([]);
  });

  it('materializes and rebinds the exact artifact during recovery verification', async () => {
    const { options, sha256 } = await fixture();
    const factory = new AuthenticatedRuntimeLaunchSpecFactory(options);

    await factory.verifyArtifact({ revisionId, taskId });

    expect(await readFile(options.artifactCache.objectPath(sha256), 'utf8')).toBe(
      'authenticated-ape-dts-binary',
    );
  });

  it('renders and verifies an uploaded artifact without activation or certification', async () => {
    const { options } = await fixture(kafkaConfig, {
      artifact_state: 'INACTIVE',
      certification_state: null,
      probe_status: null,
      adapter_id: null,
      adapter_version: null,
    });
    const factory = new AuthenticatedRuntimeLaunchSpecFactory(options);
    expect((await factory.create(input())).iniContent).toContain('partition=3');
    await expect(factory.verifyArtifact({ revisionId, taskId })).resolves.toBeUndefined();
  });

  it('rejects quarantined artifacts', async () => {
    const { options } = await fixture(kafkaConfig, { artifact_state: 'QUARANTINED' });
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create(input()),
    ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_ARTIFACT_QUARANTINED'));
  });

  it('rejects source bytes whose actual hash differs from the authenticated hash', async () => {
    const expectedSha256 = createHash('sha256').update('expected!').digest('hex');
    const { options } = await fixture(
      kafkaConfig,
      {
        actual_sha256: expectedSha256,
        declared_sha256: expectedSha256,
        size_bytes: '9',
        stored_object_sha256: expectedSha256,
      },
      Buffer.from('tampered!'),
    );
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create(input()),
    ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_ARTIFACT_HASH_MISMATCH'));
  });

  it('rejects a stale task/revision binding', async () => {
    const { options } = await fixture(kafkaConfig, { desired_state: 'STOPPED' });
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create(input()),
    ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_REVISION_BINDING_CHANGED'));
  });

  it.each(['PREFLIGHT_PASSED', 'RISK_CONFIRMED'])(
    'renders a current stopped %s draft before the start transaction applies it',
    async (state) => {
      const { options } = await fixture(kafkaConfig, {
        desired_state: 'STOPPED',
        revision_state: state,
        current_applied_revision_id: null,
        current_draft_revision_id: revisionId,
      });
      const launch = await new AuthenticatedRuntimeLaunchSpecFactory(options).create(input());
      expect(launch.iniContent).toContain('runtime.target');
    },
  );

  it.each(['READY_DRAFT', 'PREFLIGHT_PASSED'])(
    'rejects a noncurrent or unchecked %s draft',
    async (state) => {
      const { options } = await fixture(kafkaConfig, {
        desired_state: 'STOPPED',
        revision_state: state,
        current_applied_revision_id: null,
        current_draft_revision_id: state === 'READY_DRAFT' ? revisionId : randomUUID(),
      });
      await expect(
        new AuthenticatedRuntimeLaunchSpecFactory(options).create(input()),
      ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_REVISION_BINDING_CHANGED'));
    },
  );

  it('requires the single PG unit to use the stable primary key', async () => {
    const { options } = await fixture(pgConfig);
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create({
        ...input(pgConfig),
        unitKey: 'partition:0',
      }),
    ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_UNIT_BINDING_INVALID'));
  });

  it('redacts decryption failures behind a stable code', async () => {
    const { options } = await fixture(pgConfig);
    options.unlockedKey.set(Buffer.alloc(32, 9));
    await expect(
      new AuthenticatedRuntimeLaunchSpecFactory(options).create(input(pgConfig)),
    ).rejects.toSatisfy((error) => expectCode(error, 'RUNTIME_SECRET_DECRYPT_FAILED'));
  });

  it('redacts renderer failures behind a stable code', async () => {
    const { options } = await fixture();
    const adapters = new CertifiedAdapterRegistry([
      {
        id: 'ape-dts-ini',
        version: '1',
        render: () => {
          throw new Error('s3cret');
        },
      },
    ]);
    const factory = new AuthenticatedRuntimeLaunchSpecFactory({ ...options, adapters });
    await expect(factory.create(input())).rejects.toSatisfy((error) =>
      expectCode(error, 'RUNTIME_CONFIGURATION_RENDER_FAILED'),
    );
  });
});
