import { createHash } from 'node:crypto';
import { open, rm } from 'node:fs/promises';
import path from 'node:path';
import { isIP } from 'node:net';

import {
  CertifiedAdapterRegistry,
  parsePlatformTaskConfig,
  runtimeText,
  type PlatformTaskConfig,
  type TaskScenario,
} from '@scs/config-model';
import { dataSourceConfigurationSchema, type DataSourceConfiguration } from '@scs/contracts';
import { decryptSecret, type EncryptedSecret } from '@scs/security';
import { ArtifactCache, ArtifactIntegrityError, type ObjectStorageDriver } from '@scs/storage';
import type { QueryResultRow } from 'pg';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { UnlockedKeyService } from '../security/unlocked-key.service.js';
import {
  ControlledRuntimeLaunchSpecFactory,
  RuntimeLaunchSpecFactory,
  type RuntimeArtifactVerificationInput,
  type RuntimeLaunchSpecFactoryInput,
} from './runtime-launch-spec.js';
import type { RuntimeSpawnSpec } from './process-supervisor.js';
import { kafkaSourceBinding } from './kafka-source-units.js';

const SHA256_PATTERN = /^[a-f0-9]{64}$/u;

export type AuthenticatedRuntimeLaunchSpecErrorCode =
  | 'RUNTIME_ARTIFACT_INACTIVE'
  | 'RUNTIME_ARTIFACT_QUARANTINED'
  | 'RUNTIME_ARTIFACT_MISSING'
  | 'RUNTIME_ARTIFACT_HASH_MISMATCH'
  | 'RUNTIME_ARTIFACT_CERTIFICATION_INVALID'
  | 'RUNTIME_ARTIFACT_SIGNATURE_INVALID'
  | 'RUNTIME_ARTIFACT_STORAGE_UNSUPPORTED'
  | 'RUNTIME_REVISION_BINDING_CHANGED'
  | 'RUNTIME_SECRET_UNAVAILABLE'
  | 'RUNTIME_SECRET_DECRYPT_FAILED'
  | 'RUNTIME_CONFIGURATION_RENDER_FAILED'
  | 'RUNTIME_UNIT_BINDING_INVALID'
  | 'RUNTIME_MYSQL_POSITION_REQUIRED';

export interface AuthenticatedRuntimeLaunchSpecOptions {
  readonly artifactCache: ArtifactCache;
  readonly database: PlatformDatabaseService;
  readonly environment?: Readonly<Record<string, string>>;
  readonly localObjectStorage: ObjectStorageDriver;
  readonly platformVersion: string;
  readonly adapters: CertifiedAdapterRegistry;
  readonly runtimeRoot?: string;
  readonly unlockedKey: UnlockedKeyService;
}

interface LaunchBindingRow extends QueryResultRow {
  actual_sha256: string | null;
  artifact_state: 'ACTIVE' | 'INACTIVE' | 'QUARANTINED';
  configuration: unknown;
  current_applied_revision_id: string | null;
  current_draft_revision_id: string | null;
  declared_sha256: string;
  desired_state: string;
  driver: string;
  object_key: string;
  relative_prefix: string | null;
  revision_artifact_id: string | null;
  revision_state: string;
  signature_status: string | null;
  size_bytes: string;
  source_data_source_id: string;
  stored_object_sha256: string;
  stored_object_state: string;
  target_data_source_id: string;
  task_deleted_at: Date | null;
  task_type: string;
}

interface DataSourceRuntimeRow extends QueryResultRow {
  aad_version: number | null;
  algorithm: 'AES_256_GCM' | null;
  ciphertext: Buffer | null;
  configuration: unknown;
  connection_secret_id: string | null;
  nonce: Buffer | null;
  secret_id: string | null;
  secret_owner_id: string | null;
  secret_owner_type: string | null;
  secret_purpose: string | null;
  source_type: string;
  tag: Buffer | null;
}

export class AuthenticatedRuntimeLaunchSpecError extends Error {
  constructor(
    readonly code: AuthenticatedRuntimeLaunchSpecErrorCode,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'AuthenticatedRuntimeLaunchSpecError';
  }
}

function fail(code: AuthenticatedRuntimeLaunchSpecErrorCode, cause?: unknown): never {
  throw new AuthenticatedRuntimeLaunchSpecError(code, cause === undefined ? undefined : { cause });
}

function scenarioFor(
  config: PlatformTaskConfig,
  input: RuntimeLaunchSpecFactoryInput,
): TaskScenario {
  if (config.kind === 'KAFKA_RAW_FORWARD') return 'KAFKA_RAW_FORWARD';
  if (config.kind === 'MYSQL_TO_KAFKA') {
    if (input.mysql === undefined) fail('RUNTIME_MYSQL_POSITION_REQUIRED');
    return input.mysql.phase === 'SNAPSHOT' ? 'MYSQL_SNAPSHOT_TO_KAFKA' : 'MYSQL_CDC_TO_KAFKA';
  }
  return config.source.includeSnapshot ? 'PG_SNAPSHOT_TO_KAFKA' : 'PG_CDC_TO_KAFKA';
}

function objectKey(row: LaunchBindingRow): string {
  if (row.driver !== 'LOCAL') fail('RUNTIME_ARTIFACT_STORAGE_UNSUPPORTED');
  const prefix = row.relative_prefix ?? '';
  return prefix.length === 0 ? row.object_key : `${prefix}/${row.object_key}`;
}

function mysqlUrlHost(host: string): string {
  return isIP(host) === 6 ? `[${host}]` : host;
}

function expectedSize(value: string): number {
  const size = Number(value);
  if (!Number.isSafeInteger(size) || size <= 0) fail('RUNTIME_ARTIFACT_MISSING');
  return size;
}

function encryptedSecret(row: DataSourceRuntimeRow): EncryptedSecret {
  if (
    row.algorithm !== 'AES_256_GCM' ||
    row.aad_version !== 1 ||
    row.ciphertext === null ||
    row.nonce === null ||
    row.tag === null
  ) {
    fail('RUNTIME_SECRET_UNAVAILABLE');
  }
  return {
    aadVersion: 1,
    algorithm: row.algorithm,
    ciphertext: row.ciphertext.toString('base64'),
    nonce: row.nonce.toString('base64'),
    tag: row.tag.toString('base64'),
  };
}

function pgConnection(
  configuration: Extract<DataSourceConfiguration, { type: 'POSTGRES' }>,
  password: string,
  caPath?: string,
): string {
  const sslMode =
    configuration.tls.mode === 'DISABLE'
      ? 'disable'
      : configuration.tls.mode === 'REQUIRE'
        ? 'require'
        : 'verify-full';
  return `postgresql://${encodeURIComponent(configuration.username)}:${encodeURIComponent(password)}@${configuration.host}:${configuration.port}/${encodeURIComponent(configuration.database)}?sslmode=${sslMode}${caPath === undefined ? '' : `&sslrootcert=${encodeURIComponent(caPath)}`}`;
}

function previewConnection(configuration: DataSourceConfiguration): string {
  if (configuration.type === 'KAFKA') return configuration.bootstrapServers.join(',');
  const host =
    configuration.type === 'MYSQL' ? mysqlUrlHost(configuration.host) : configuration.host;
  return `${configuration.type === 'MYSQL' ? 'mysql' : 'postgresql'}://${encodeURIComponent(configuration.username)}:[REDACTED]@${host}:${configuration.port}/${encodeURIComponent(configuration.database)}`;
}

export class AuthenticatedRuntimeLaunchSpecFactory extends RuntimeLaunchSpecFactory {
  constructor(private readonly options: AuthenticatedRuntimeLaunchSpecOptions) {
    super();
    if (options.platformVersion.trim().length === 0)
      throw new Error('RUNTIME_PLATFORM_VERSION_INVALID');
  }

  async create(input: RuntimeLaunchSpecFactoryInput): Promise<RuntimeSpawnSpec> {
    const row = await this.binding(input);
    const config = this.configuration(row, input);
    if (config.kind !== 'KAFKA_RAW_FORWARD' && input.unitKey !== 'primary') {
      fail('RUNTIME_UNIT_BINDING_INVALID');
    }
    const scenario = scenarioFor(config, input);
    const sha256 = this.assertArtifact(row);
    const size = expectedSize(row.size_bytes);
    await this.ensureCached(row, sha256, size);
    const binding = await this.bindCached(sha256, size);
    try {
      const [source, target] = await Promise.all([
        this.dataSource(row.source_data_source_id),
        this.dataSource(row.target_data_source_id),
      ]);
      const parsedSource = dataSourceConfigurationSchema.safeParse(source.configuration);
      if (!parsedSource.success || parsedSource.data.type !== source.source_type)
        fail('RUNTIME_SECRET_UNAVAILABLE');
      const sourceConfig = parsedSource.data;
      const pgCaPath =
        sourceConfig.type === 'POSTGRES' && sourceConfig.tls.mode === 'VERIFY_FULL'
          ? path.posix.join(
              this.options.runtimeRoot ?? '/run/ape-dts',
              input.taskId,
              input.runId,
              input.unitId,
              'postgres-ca.pem',
            )
          : undefined;
      const sourceConnection = this.connection(source, pgCaPath);
      const targetConnection = this.connection(target);
      try {
        const controlled = new ControlledRuntimeLaunchSpecFactory({
          environment: this.options.environment ?? {},
          executablePath: binding.path,
          runtimeRoot: this.options.runtimeRoot ?? '/run/ape-dts',
          render: (controlledInput) => {
            try {
              const runtimeDirectory = path.posix.join(
                this.options.runtimeRoot ?? '/run/ape-dts',
                controlledInput.taskId,
                controlledInput.runId,
                controlledInput.unitId,
              );
              const adapter = this.options.adapters.resolveRuntime({
                artifactSha256: sha256,
                platformVersion: this.options.platformVersion,
                scenario,
              });
              const sourceBinding =
                config.kind === 'KAFKA_RAW_FORWARD'
                  ? kafkaSourceBinding(config, controlledInput.unitKey)
                  : null;
              if (config.kind === 'KAFKA_RAW_FORWARD' && sourceBinding === null)
                fail('RUNTIME_UNIT_BINDING_INVALID');
              const rendered = adapter.render({
                config,
                runtime: {
                  log4rsPath: path.posix.join(runtimeDirectory, 'log4rs.yaml'),
                  logDirectory: runtimeDirectory,
                  logLevel: 'info',
                  metricsPort: controlledInput.metricsPort,
                  sourceConnection: sourceConnection.text,
                  ...sourceBinding,
                  targetConnection: targetConnection.text,
                  ...(config.kind === 'MYSQL_TO_KAFKA' && sourceConfig.type === 'MYSQL'
                    ? {
                        mysqlCdc: controlledInput.mysql!,
                        mysqlTls: {
                          mode:
                            sourceConfig.tls.mode === 'DISABLE'
                              ? ('disable' as const)
                              : sourceConfig.tls.mode === 'REQUIRE'
                                ? ('require' as const)
                                : ('verify_full' as const),
                          ...(sourceConfig.tls.mode === 'VERIFY_FULL'
                            ? { caPath: path.posix.join(runtimeDirectory, 'mysql-ca.pem') }
                            : {}),
                        },
                      }
                    : {}),
                },
                scenario,
              });
              return { iniContent: rendered.runtimeIni, log4rsContent: rendered.log4rsYaml };
            } catch (error) {
              if (error instanceof AuthenticatedRuntimeLaunchSpecError) throw error;
              fail('RUNTIME_CONFIGURATION_RENDER_FAILED', error);
            }
          },
        });
        return {
          ...controlled.create({ ...input, config }),
          ...(sourceConfig.type === 'MYSQL' && sourceConfig.tls.mode === 'VERIFY_FULL'
            ? { caCertificate: sourceConfig.tls.caCertificate }
            : {}),
          ...(sourceConfig.type === 'POSTGRES' && sourceConfig.tls.mode === 'VERIFY_FULL'
            ? { postgresCaCertificate: sourceConfig.tls.caCertificate }
            : {}),
          sensitiveValues: [
            ...sourceConnection.sensitiveValues,
            ...targetConnection.sensitiveValues,
          ],
        };
      } finally {
        sourceConnection.text.reveal();
        targetConnection.text.reveal();
      }
    } finally {
      await binding.close();
    }
  }

  override async verifyArtifact(input: RuntimeArtifactVerificationInput): Promise<void> {
    const configurationResult = await this.options.database.requirePool().query<{
      configuration: unknown;
    }>(
      `SELECT revision.configuration
         FROM ape_dts_platform.tasks AS task
         JOIN ape_dts_platform.task_revisions AS revision
           ON revision.id = task.current_applied_revision_id
          AND revision.task_id = task.id
        WHERE task.id = $1::uuid
          AND task.deleted_at IS NULL
          AND task.desired_state = 'RUNNING'
          AND revision.id = $2::uuid
          AND revision.state = 'APPLIED'`,
      [input.taskId, input.revisionId],
    );
    const configuration = configurationResult.rows[0]?.configuration;
    if (configuration === undefined) fail('RUNTIME_REVISION_BINDING_CHANGED');
    let config: PlatformTaskConfig;
    try {
      config = parsePlatformTaskConfig(configuration);
    } catch (error) {
      fail('RUNTIME_REVISION_BINDING_CHANGED', error);
    }
    const row = await this.binding({ ...input, config });
    this.configuration(row, { ...input, config });
    const sha256 = this.assertArtifact(row);
    const size = expectedSize(row.size_bytes);
    await this.ensureCached(row, sha256, size);
    const cached = await this.bindCached(sha256, size);
    await cached.close();
  }

  private async binding(
    input: Pick<RuntimeLaunchSpecFactoryInput, 'config' | 'revisionId' | 'taskId'>,
  ): Promise<LaunchBindingRow> {
    const result = await this.options.database.requirePool().query<LaunchBindingRow>(
      `SELECT revision.state AS revision_state, revision.configuration,
              revision.artifact_id AS revision_artifact_id,
              task.task_type, task.source_data_source_id, task.target_data_source_id,
              task.current_applied_revision_id, task.current_draft_revision_id,
              task.desired_state, task.deleted_at AS task_deleted_at,
              artifact.state AS artifact_state,
              encode(artifact.declared_sha256, 'hex') AS declared_sha256,
              encode(artifact.actual_sha256, 'hex') AS actual_sha256,
              object.object_key, object.size_bytes::text,
              object.state AS stored_object_state,
              encode(object.sha256, 'hex') AS stored_object_sha256,
              profile.driver, profile.config ->> 'relative_prefix' AS relative_prefix,
              signature.status AS signature_status
         FROM ape_dts_platform.task_revisions AS revision
         JOIN ape_dts_platform.tasks AS task
           ON task.id = revision.task_id
          AND (task.current_applied_revision_id = revision.id
            OR task.current_draft_revision_id = revision.id)
         LEFT JOIN ape_dts_platform.artifacts AS artifact ON artifact.id = revision.artifact_id AND artifact.deleted_at IS NULL
         LEFT JOIN ape_dts_platform.stored_objects AS object ON object.id = artifact.stored_object_id
         LEFT JOIN ape_dts_platform.storage_profiles AS profile ON profile.id = object.storage_profile_id
         LEFT JOIN LATERAL (
           SELECT status FROM ape_dts_platform.artifact_signature_checks
            WHERE artifact_id = artifact.id ORDER BY checked_at DESC LIMIT 1
         ) AS signature ON true
        WHERE revision.id = $1::uuid AND revision.task_id = $2::uuid`,
      [input.revisionId, input.taskId],
    );
    const row = result.rows[0];
    if (
      row === undefined ||
      row.task_deleted_at !== null ||
      !(
        (row.desired_state === 'RUNNING' &&
          row.revision_state === 'APPLIED' &&
          row.current_applied_revision_id === input.revisionId) ||
        // The supervisor locks and validates the current draft before rendering;
        // it is applied only after every runtime unit has started successfully.
        (row.desired_state === 'STOPPED' &&
          ['PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(row.revision_state) &&
          row.current_draft_revision_id === input.revisionId)
      )
    ) {
      fail('RUNTIME_REVISION_BINDING_CHANGED');
    }
    return row;
  }

  private configuration(
    row: LaunchBindingRow,
    input: Pick<RuntimeLaunchSpecFactoryInput, 'config'>,
  ): PlatformTaskConfig {
    try {
      const config = parsePlatformTaskConfig(row.configuration);
      const requestedConfig = parsePlatformTaskConfig(input.config);
      if (
        JSON.stringify(config) !== JSON.stringify(requestedConfig) ||
        config.source.dataSourceId !== row.source_data_source_id ||
        config.target.dataSourceId !== row.target_data_source_id ||
        row.task_type !==
          (config.kind === 'PG_TO_KAFKA'
            ? 'PG_KAFKA'
            : config.kind === 'MYSQL_TO_KAFKA'
              ? 'MYSQL_KAFKA'
              : 'KAFKA_KAFKA')
      )
        fail('RUNTIME_REVISION_BINDING_CHANGED');
      return config;
    } catch (error) {
      if (error instanceof AuthenticatedRuntimeLaunchSpecError) throw error;
      fail('RUNTIME_REVISION_BINDING_CHANGED', error);
    }
  }

  private assertArtifact(row: LaunchBindingRow): string {
    if (row.revision_artifact_id === null) fail('RUNTIME_ARTIFACT_MISSING');
    if (row.artifact_state === 'QUARANTINED') fail('RUNTIME_ARTIFACT_QUARANTINED');
    if (row.stored_object_state !== 'AVAILABLE') fail('RUNTIME_ARTIFACT_MISSING');
    if (
      !SHA256_PATTERN.test(row.declared_sha256) ||
      row.actual_sha256 !== row.declared_sha256 ||
      row.stored_object_sha256 !== row.declared_sha256
    )
      fail('RUNTIME_ARTIFACT_HASH_MISMATCH');
    if (row.signature_status !== 'PASSED' && row.signature_status !== 'NOT_REQUIRED')
      fail('RUNTIME_ARTIFACT_SIGNATURE_INVALID');
    return row.declared_sha256;
  }

  private async ensureCached(row: LaunchBindingRow, sha256: string, size: number): Promise<void> {
    try {
      const cached = await this.options.artifactCache.bind(sha256, size);
      await cached.close();
      return;
    } catch (error) {
      if (error instanceof ArtifactIntegrityError) fail('RUNTIME_ARTIFACT_HASH_MISMATCH', error);
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    }
    const staging = await this.options.artifactCache.createStagingPath();
    let exists = false;
    try {
      const output = await open(staging, 'wx', 0o600);
      exists = true;
      const digest = createHash('sha256');
      let actualSize = 0;
      try {
        const source = await this.options.localObjectStorage.openObject(objectKey(row));
        for await (const chunkValue of source) {
          const chunk = Buffer.isBuffer(chunkValue)
            ? chunkValue
            : Buffer.from(chunkValue as Uint8Array);
          actualSize += chunk.length;
          if (actualSize > size) fail('RUNTIME_ARTIFACT_HASH_MISMATCH');
          digest.update(chunk);
          await output.write(chunk);
        }
        await output.sync();
      } finally {
        await output.close();
      }
      if (actualSize !== size || digest.digest('hex') !== sha256)
        fail('RUNTIME_ARTIFACT_HASH_MISMATCH');
      await this.options.artifactCache.publish(staging, sha256);
      exists = false;
    } catch (error) {
      if (error instanceof AuthenticatedRuntimeLaunchSpecError) throw error;
      fail('RUNTIME_ARTIFACT_MISSING', error);
    } finally {
      if (exists) await rm(staging, { force: true });
    }
  }

  private async bindCached(sha256: string, size: number) {
    try {
      return await this.options.artifactCache.bind(sha256, size);
    } catch (error) {
      fail('RUNTIME_ARTIFACT_HASH_MISMATCH', error);
    }
  }

  private async dataSource(id: string): Promise<DataSourceRuntimeRow> {
    const result = await this.options.database.requirePool().query<DataSourceRuntimeRow>(
      `SELECT source.source_type, source.configuration, source.connection_secret_id,
              secret.id AS secret_id, secret.owner_type AS secret_owner_type,
              secret.owner_id AS secret_owner_id, secret.purpose AS secret_purpose,
              secret.algorithm, secret.aad_version, secret.nonce, secret.ciphertext, secret.tag
         FROM ape_dts_platform.data_sources AS source
         LEFT JOIN ape_dts_platform.secrets AS secret
           ON secret.id = source.connection_secret_id AND secret.destroyed_at IS NULL
        WHERE source.id = $1::uuid AND source.state = 'ACTIVE' AND source.deleted_at IS NULL`,
      [id],
    );
    if (result.rows[0] === undefined) fail('RUNTIME_SECRET_UNAVAILABLE');
    return result.rows[0];
  }

  private connection(row: DataSourceRuntimeRow, pgCaPath?: string) {
    const parsed = dataSourceConfigurationSchema.safeParse(row.configuration);
    if (!parsed.success || parsed.data.type !== row.source_type) fail('RUNTIME_SECRET_UNAVAILABLE');
    if (parsed.data.type === 'KAFKA') {
      if (row.connection_secret_id !== null) fail('RUNTIME_SECRET_UNAVAILABLE');
      const value = parsed.data.bootstrapServers.join(',');
      return { sensitiveValues: [] as string[], text: runtimeText(value, value) };
    }
    if (
      !['POSTGRES', 'MYSQL'].includes(parsed.data.type) ||
      row.secret_id === null ||
      row.secret_owner_id === null ||
      row.secret_owner_type === null ||
      row.secret_purpose === null
    )
      fail('RUNTIME_SECRET_UNAVAILABLE');
    let plaintext: Buffer;
    try {
      plaintext = decryptSecret(this.options.unlockedKey.require(), encryptedSecret(row), {
        aadVersion: 1,
        ownerId: row.secret_owner_id,
        ownerType: row.secret_owner_type,
        purpose: row.secret_purpose,
        secretId: row.secret_id,
      });
    } catch (error) {
      fail('RUNTIME_SECRET_DECRYPT_FAILED', error);
    }
    try {
      const password = plaintext.toString('utf8');
      const connection =
        parsed.data.type === 'POSTGRES'
          ? pgConnection(parsed.data, password, pgCaPath)
          : `mysql://${encodeURIComponent(parsed.data.username)}:${encodeURIComponent(password)}@${mysqlUrlHost(parsed.data.host)}:${parsed.data.port}/${encodeURIComponent(parsed.data.database)}`;
      return {
        sensitiveValues: [password, connection],
        text: runtimeText(connection, previewConnection(parsed.data)),
      };
    } finally {
      plaintext.fill(0);
    }
  }
}
