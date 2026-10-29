import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import {
  createKafkaTopicRequestSchema,
  type CreateKafkaTopicRequest,
  type CreateKafkaTopicResponse,
  dataSourceConfigurationSchema,
  type ConnectionTestPurpose,
  type ConnectionTestResponse,
  type CreateDataSourceGrantRequest,
  type CreateDataSourceRequest,
  type CopyDataSourceRequest,
  type DataSourceConfiguration,
  type DataSourceCatalogQuery,
  type DataSourceCatalogResponse,
  type DataSourceGrant,
  type DataSourceIdentity,
  type DataSourceSummary,
  type DataSourceType,
  type MetadataQuery,
  type MetadataResponse,
  type MysqlCdcInspection,
  type UpdateDataSourceRequest,
  type UserSummary,
} from '@scs/contracts';
import { decryptSecret, encryptSecret, type EncryptedSecret } from '@scs/security';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import {
  DataSourceAdapterError,
  DataSourceAdapterRegistry,
  type PostgresContinuityFact,
  type MysqlBinlogFile,
  type PostgresResourceInspection,
  type PostgresResourcePreparationRequest,
  toAdapterError,
} from './adapters/types.js';

export type DataSourcesServiceErrorCode =
  | 'DATA_SOURCE_ACCESS_DENIED'
  | 'DATA_SOURCE_CHANGED_DURING_OPERATION'
  | 'DATA_SOURCE_CONFIGURATION_UNAVAILABLE'
  | 'DATA_SOURCE_GRANT_ALREADY_EXISTS'
  | 'DATA_SOURCE_GRANT_NOT_FOUND'
  | 'DATA_SOURCE_GRANT_TARGET_INVALID'
  | 'DATA_SOURCE_NAME_ALREADY_EXISTS'
  | 'DATA_SOURCE_NOT_FOUND'
  | 'DATA_SOURCE_REFERENCED_BY_RESOURCE'
  | 'DATA_SOURCE_REFERENCED_BY_TASK'
  | 'DATA_SOURCE_SECRET_UNAVAILABLE'
  | 'DATA_SOURCE_TYPE_IMMUTABLE'
  | 'DATA_SOURCE_VERSION_CONFLICT'
  | 'SECURITY_POLICY_UNAVAILABLE';

export class DataSourcesServiceError extends Error {
  constructor(readonly code: DataSourcesServiceErrorCode) {
    super(code);
    this.name = 'DataSourcesServiceError';
  }
}

export class KafkaTopicAuditError extends Error {
  readonly code = 'KAFKA_TOPIC_AUDIT_FAILED';
  constructor(
    readonly result: CreateKafkaTopicResponse,
    options: ErrorOptions,
    readonly completedResults: readonly CreateKafkaTopicResponse[] = [result],
  ) {
    super(
      'Topic 操作已完成，但审计记录或事务提交失败；请核实 Topic 状态，不要重复修改参数。',
      options,
    );
  }
}

interface DataSourceRow extends QueryResultRow {
  configuration: unknown;
  connection_secret_id: string | null;
  credentials_required?: boolean;
  created_at: Date;
  created_by_user_id: string;
  deleted_at: Date | null;
  id: string;
  normalized_name: string;
  row_version: string;
  source_type: DataSourceType;
  state: 'ACTIVE' | 'DELETED';
  updated_at: Date;
  updated_by_user_id: string;
}

interface SecretRow extends QueryResultRow {
  aad_version: number;
  algorithm: 'AES_256_GCM';
  ciphertext: Buffer;
  id: string;
  nonce: Buffer;
  owner_id: string;
  owner_type: string;
  purpose: string;
  tag: Buffer;
}

interface GrantUserRow extends QueryResultRow {
  display_name: string;
  id: string;
  normalized_username: string;
  role: UserSummary['role'];
  status: UserSummary['status'];
}

interface DataSourceGrantRow extends QueryResultRow {
  data_source_id: string;
  granted_at: Date;
  granted_by_display_name: string;
  granted_by_id: string;
  granted_by_normalized_username: string;
  granted_by_role: UserSummary['role'];
  granted_by_status: UserSummary['status'];
  user_display_name: string;
  user_id: string;
  user_normalized_username: string;
  user_role: UserSummary['role'];
  user_status: UserSummary['status'];
}

type DataSourceLockMode = 'FOR KEY SHARE' | 'FOR UPDATE';

interface InsertedConnectionTestRow extends QueryResultRow {
  completed_at: Date;
  id: string;
}

function postgresErrorCode(error: unknown): string | null {
  if (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string'
  ) {
    return error.code;
  }
  return null;
}

function normalizedDataSourceName(name: string): string {
  return name.normalize('NFKC').trim().toLocaleLowerCase('en-US');
}

function encryptedSecret(row: SecretRow): EncryptedSecret {
  return {
    aadVersion: row.aad_version as 1,
    algorithm: row.algorithm,
    ciphertext: row.ciphertext.toString('base64'),
    nonce: row.nonce.toString('base64'),
    tag: row.tag.toString('base64'),
  };
}

function userSummary(row: GrantUserRow): UserSummary {
  return {
    displayName: row.display_name,
    id: row.id,
    role: row.role,
    status: row.status,
    username: row.normalized_username,
  };
}

function grantSummary(row: DataSourceGrantRow): DataSourceGrant {
  return {
    dataSourceId: row.data_source_id,
    grantedAt: row.granted_at.toISOString(),
    grantedBy: {
      displayName: row.granted_by_display_name,
      id: row.granted_by_id,
      role: row.granted_by_role,
      status: row.granted_by_status,
      username: row.granted_by_normalized_username,
    },
    user: {
      displayName: row.user_display_name,
      id: row.user_id,
      role: row.user_role,
      status: row.user_status,
      username: row.user_normalized_username,
    },
  };
}

function configurationFor(row: DataSourceRow): DataSourceConfiguration {
  const parsed = dataSourceConfigurationSchema.safeParse(row.configuration);
  if (!parsed.success || parsed.data.type !== row.source_type) {
    throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
  }
  return parsed.data;
}

function dataSourceSummary(row: DataSourceRow): DataSourceSummary {
  return {
    ...(row.credentials_required ? { credentialsRequired: true } : {}),
    configuration: configurationFor(row),
    createdAt: row.created_at.toISOString(),
    deletedAt: row.deleted_at?.toISOString() ?? null,
    id: row.id,
    name: row.normalized_name,
    rowVersion: row.row_version,
    state: row.state,
    updatedAt: row.updated_at.toISOString(),
  };
}

function createPassword(request: CreateDataSourceRequest): string | null {
  if (request.configuration.type === 'KAFKA') {
    if (request.credentials.mode !== 'NONE') {
      throw new Error('DATA_SOURCE_CREATE_CREDENTIALS_TYPE_MISMATCH');
    }
    return null;
  }
  if (request.credentials.mode !== 'SET') {
    throw new Error('DATA_SOURCE_CREATE_CREDENTIALS_TYPE_MISMATCH');
  }
  return request.credentials.password;
}

function replacementPassword(request: UpdateDataSourceRequest): string | null {
  if (request.configuration.type === 'KAFKA') {
    if (request.credentials.mode !== 'NONE') {
      throw new Error('DATA_SOURCE_UPDATE_CREDENTIALS_TYPE_MISMATCH');
    }
    return null;
  }
  if (request.credentials.mode === 'REPLACE') {
    return request.credentials.password;
  }
  if (request.credentials.mode !== 'RETAIN') {
    throw new Error('DATA_SOURCE_UPDATE_CREDENTIALS_TYPE_MISMATCH');
  }
  return null;
}

const DATA_SOURCE_COLUMNS = `
  id,
  normalized_name,
  source_type,
  configuration,
  connection_secret_id,
  credentials_required,
  state,
  row_version::text,
  created_by_user_id,
  created_at,
  updated_by_user_id,
  updated_at,
  deleted_at`;

const JOINED_DATA_SOURCE_COLUMNS = `
  sources.id,
  sources.normalized_name,
  sources.source_type,
  sources.configuration,
  sources.connection_secret_id,
  sources.credentials_required,
  sources.state,
  sources.row_version::text,
  sources.created_by_user_id,
  sources.created_at,
  sources.updated_by_user_id,
  sources.updated_at,
  sources.deleted_at`;

@Injectable()
export class DataSourcesService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    private readonly unlockedKey: UnlockedKeyService,
    private readonly adapters: DataSourceAdapterRegistry,
  ) {}

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private requireUnlockedKey(): Buffer {
    try {
      return this.unlockedKey.require();
    } catch {
      throw new DataSourcesServiceError('SECURITY_POLICY_UNAVAILABLE');
    }
  }

  private encryptConnectionPassword(
    dataSourceId: string,
    secretId: string,
    password: string,
  ): EncryptedSecret {
    const plaintext = Buffer.from(password, 'utf8');
    try {
      return encryptSecret(this.requireUnlockedKey(), plaintext, {
        aadVersion: 1,
        ownerId: dataSourceId,
        ownerType: 'DATA_SOURCE',
        purpose: 'CONNECTION_PASSWORD',
        secretId,
      });
    } finally {
      plaintext.fill(0);
    }
  }

  private async insertConnectionSecret(
    client: PoolClient,
    dataSourceId: string,
    secretId: string,
    password: string,
  ): Promise<void> {
    const encrypted = this.encryptConnectionPassword(dataSourceId, secretId, password);
    await client.query(
      `INSERT INTO ape_dts_platform.secrets (
         id, owner_type, owner_id, purpose, algorithm, dek_version, aad_version,
         nonce, ciphertext, tag
       ) VALUES (
         $1::uuid, 'DATA_SOURCE', $2::uuid, 'CONNECTION_PASSWORD', $3, 1, $4, $5, $6, $7
       )`,
      [
        secretId,
        dataSourceId,
        encrypted.algorithm,
        encrypted.aadVersion,
        Buffer.from(encrypted.nonce, 'base64'),
        Buffer.from(encrypted.ciphertext, 'base64'),
        Buffer.from(encrypted.tag, 'base64'),
      ],
    );
  }

  private async requireActiveSource(
    dataSourceId: string,
    client?: PoolClient,
    lockMode?: DataSourceLockMode,
  ): Promise<DataSourceRow> {
    const executor = client ?? this.database.requirePool();
    const lockClause = lockMode === undefined ? '' : ` ${lockMode}`;
    const result = await executor.query<DataSourceRow>(
      `SELECT ${DATA_SOURCE_COLUMNS}
       FROM ape_dts_platform.data_sources
       WHERE id = $1::uuid
         AND state = 'ACTIVE'
         AND deleted_at IS NULL${lockClause}`,
      [dataSourceId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new DataSourcesServiceError('DATA_SOURCE_NOT_FOUND');
    }
    return row;
  }

  private async requireAccessibleSource(
    actor: UserSummary,
    dataSourceId: string,
    client?: PoolClient,
  ): Promise<DataSourceRow> {
    if (actor.role === 'ADMIN') {
      return await this.requireActiveSource(dataSourceId, client);
    }
    const result = await (client ?? this.database.requirePool()).query<DataSourceRow>(
      `SELECT ${JOINED_DATA_SOURCE_COLUMNS}
       FROM ape_dts_platform.data_sources AS sources
       JOIN ape_dts_platform.data_source_grants AS grants
         ON grants.data_source_id = sources.id
        AND grants.user_id = $2::uuid
        AND grants.revoked_at IS NULL
       WHERE sources.id = $1::uuid
         AND sources.state = 'ACTIVE'
         AND sources.deleted_at IS NULL`,
      [dataSourceId, actor.id],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new DataSourcesServiceError('DATA_SOURCE_ACCESS_DENIED');
    }
    return row;
  }

  private async assertUnchangedAccessibleSource(
    actor: UserSummary,
    snapshot: DataSourceRow,
  ): Promise<void> {
    const current = await this.requireAccessibleSource(actor, snapshot.id);
    if (current.row_version !== snapshot.row_version) {
      throw new DataSourcesServiceError('DATA_SOURCE_CHANGED_DURING_OPERATION');
    }
  }

  private async requireActiveConnectionSecret(
    source: DataSourceRow,
    client?: PoolClient,
  ): Promise<void> {
    if (source.source_type === 'KAFKA') {
      if (source.connection_secret_id !== null) {
        throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
      }
      return;
    }
    if (source.connection_secret_id === null) {
      throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
    }
    const executor = client ?? this.database.requirePool();
    const result = await executor.query<{ id: string }>(
      `SELECT id
       FROM ape_dts_platform.secrets
       WHERE id = $1::uuid
         AND owner_type = 'DATA_SOURCE'
         AND owner_id = $2::uuid
         AND purpose = 'CONNECTION_PASSWORD'
         AND destroyed_at IS NULL`,
      [source.connection_secret_id, source.id],
    );
    if (result.rows[0] === undefined) {
      throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
    }
  }

  private async decryptedConnectionPassword(
    source: DataSourceRow,
    client?: PoolClient,
  ): Promise<Buffer | null> {
    if (source.source_type === 'KAFKA') {
      if (source.connection_secret_id !== null) {
        throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
      }
      return null;
    }
    if (source.connection_secret_id === null) {
      throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
    }
    const result = await (client ?? this.database.requirePool()).query<SecretRow>(
      `SELECT id, owner_type, owner_id, purpose, algorithm, aad_version, nonce, ciphertext, tag
       FROM ape_dts_platform.secrets
       WHERE id = $1::uuid
         AND owner_type = 'DATA_SOURCE'
         AND owner_id = $2::uuid
         AND purpose = 'CONNECTION_PASSWORD'
         AND destroyed_at IS NULL`,
      [source.connection_secret_id, source.id],
    );
    const secret = result.rows[0];
    if (secret === undefined) {
      throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
    }
    try {
      return decryptSecret(this.requireUnlockedKey(), encryptedSecret(secret), {
        aadVersion: 1,
        ownerId: secret.owner_id,
        ownerType: secret.owner_type,
        purpose: secret.purpose,
        secretId: secret.id,
      });
    } catch {
      throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
    }
  }

  private async withConnectionPassword<T>(
    source: DataSourceRow,
    operation: (password: string | null) => Promise<T>,
  ): Promise<T> {
    const passwordBytes = await this.decryptedConnectionPassword(source);
    try {
      return await operation(passwordBytes?.toString('utf8') ?? null);
    } finally {
      passwordBytes?.fill(0);
    }
  }

  async list(actor: UserSummary): Promise<readonly DataSourceSummary[]> {
    if (actor.role === 'ADMIN') {
      const result = await this.database.requirePool().query<DataSourceRow>(
        `SELECT ${DATA_SOURCE_COLUMNS}
         FROM ape_dts_platform.data_sources
         WHERE state = 'ACTIVE'
           AND deleted_at IS NULL
         ORDER BY normalized_name, id`,
      );
      return result.rows.map(dataSourceSummary);
    }
    const result = await this.database.requirePool().query<DataSourceRow>(
      `SELECT ${JOINED_DATA_SOURCE_COLUMNS}
       FROM ape_dts_platform.data_sources AS sources
       JOIN ape_dts_platform.data_source_grants AS grants
         ON grants.data_source_id = sources.id
        AND grants.user_id = $1::uuid
        AND grants.revoked_at IS NULL
       WHERE sources.state = 'ACTIVE'
         AND sources.deleted_at IS NULL
       ORDER BY sources.normalized_name, sources.id`,
      [actor.id],
    );
    return result.rows.map(dataSourceSummary);
  }

  async catalog(
    actor: UserSummary,
    query: DataSourceCatalogQuery,
  ): Promise<DataSourceCatalogResponse> {
    // One statement keeps totals, page bounds and rows on the same authorization snapshot.
    const result = await this.database
      .requirePool()
      .query<(DataSourceRow | { id: null }) & { total: string; current_page: number }>(
        `WITH filtered AS (
         SELECT ${JOINED_DATA_SOURCE_COLUMNS}
         FROM ape_dts_platform.data_sources AS sources
         WHERE sources.state = 'ACTIVE' AND sources.deleted_at IS NULL
           AND ($2::boolean OR EXISTS (
             SELECT 1 FROM ape_dts_platform.data_source_grants AS grants
             WHERE grants.data_source_id = sources.id AND grants.user_id = $1::uuid
               AND grants.revoked_at IS NULL
           ))
           AND ($4::text IS NULL OR sources.source_type = $4::text)
           AND ($3::text = '' OR strpos(lower(concat_ws(' ',
             sources.normalized_name, sources.configuration->>'host',
             sources.configuration->>'database', sources.configuration->>'port',
             (sources.configuration->'bootstrapServers')::text
           )), lower($3::text)) > 0)
       ), totals AS (SELECT count(*) AS total FROM filtered), bounds AS (
         SELECT total, least($5::int, greatest(1, ceil(total::numeric / $6::int)::int)) AS current_page
         FROM totals
       )
       SELECT bounds.total::text, bounds.current_page, entries.*
       FROM bounds LEFT JOIN LATERAL (
         SELECT * FROM filtered ORDER BY normalized_name, id
         LIMIT $6::int OFFSET (bounds.current_page - 1) * $6::int
       ) AS entries ON true
       ORDER BY entries.normalized_name, entries.id`,
        [actor.id, actor.role === 'ADMIN', query.q, query.type ?? null, query.page, query.pageSize],
      );
    const bounds = result.rows[0]!;
    return {
      dataSources: result.rows.flatMap((row) => (row.id === null ? [] : [dataSourceSummary(row)])),
      total: Number(bounds.total),
      page: bounds.current_page,
      pageSize: query.pageSize,
    };
  }

  async get(
    actor: UserSummary,
    dataSourceId: string,
    client?: PoolClient,
  ): Promise<DataSourceSummary> {
    const source = await this.requireAccessibleSource(actor, dataSourceId, client);
    return dataSourceSummary(source);
  }

  async getForSystem(dataSourceId: string): Promise<DataSourceSummary> {
    return dataSourceSummary(await this.requireActiveSource(dataSourceId));
  }

  async readProgressForSystem(
    dataSourceId: string,
    request: { slotName?: string; topic?: string; groupId?: string },
    expectedSourceVersion: string,
  ) {
    const source = await this.requireActiveSource(dataSourceId);
    if (source.row_version !== expectedSourceVersion)
      throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
    const configuration = configurationFor(source);
    const adapter = this.adapters.require(configuration.type);
    if (!adapter.readProgress) throw new Error('SOURCE_PROGRESS_UNSUPPORTED');
    return this.withConnectionPassword(source, (password) =>
      adapter.readProgress!({ configuration, password }, request),
    );
  }

  async preparePostgresResources(
    actor: UserSummary,
    dataSourceId: string,
    request: PostgresResourcePreparationRequest,
    client: PoolClient,
    beforeWrite: (inspection: PostgresResourceInspection) => Promise<void>,
  ) {
    if (actor.role !== 'ADMIN' || actor.status !== 'ACTIVE')
      throw new DataSourcesServiceError('DATA_SOURCE_ACCESS_DENIED');
    const source = await this.requireActiveSource(dataSourceId, client, 'FOR UPDATE');
    const configuration = configurationFor(source);
    if (configuration.type !== 'POSTGRES')
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const adapter = this.adapters.require('POSTGRES');
    if (adapter.preparePostgresResources === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const password = await this.decryptedConnectionPassword(source, client);
    try {
      return await adapter.preparePostgresResources(
        { configuration, password: password?.toString('utf8') ?? null },
        request,
        beforeWrite,
      );
    } finally {
      password?.fill(0);
    }
  }

  async readPostgresResources(
    actor: UserSummary,
    dataSourceId: string,
    request: Omit<PostgresResourcePreparationRequest, 'resourceMode'>,
  ) {
    const source = await this.requireAccessibleSource(actor, dataSourceId);
    return this.readPostgresResourcesFromSource(source, request);
  }

  async readPostgresResourcesForSystem(
    dataSourceId: string,
    request: Omit<PostgresResourcePreparationRequest, 'resourceMode'>,
  ) {
    const source = await this.requireActiveSource(dataSourceId);
    return this.readPostgresResourcesFromSource(source, request);
  }

  private async readPostgresResourcesFromSource(
    source: DataSourceRow,
    request: Omit<PostgresResourcePreparationRequest, 'resourceMode'>,
  ) {
    const configuration = configurationFor(source);
    if (configuration.type !== 'POSTGRES')
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const adapter = this.adapters.require('POSTGRES');
    if (adapter.readPostgresResources === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    return this.withConnectionPassword(source, (password) =>
      adapter.readPostgresResources!({ configuration, password }, request),
    );
  }

  async createKafkaTopic(
    actor: UserSummary,
    dataSourceId: string,
    request: CreateKafkaTopicRequest,
    transactionClient?: PoolClient,
  ): Promise<CreateKafkaTopicResponse> {
    if (actor.role !== 'ADMIN' || actor.status !== 'ACTIVE') {
      throw new DataSourcesServiceError('DATA_SOURCE_ACCESS_DENIED');
    }
    const parsed = createKafkaTopicRequestSchema.parse(request);
    let completed: CreateKafkaTopicResponse | undefined;
    try {
      const createWithin = async (client: PoolClient) => {
        // Lock only this ACTIVE, non-deleted source; updates/deletion cannot switch
        // broker configuration while the external mutation is in progress.
        const source = await this.requireActiveSource(dataSourceId, client, 'FOR UPDATE');
        if (source.row_version !== parsed.expectedRowVersion) {
          throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
        }
        const configuration = configurationFor(source);
        if (configuration.type !== 'KAFKA') {
          throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
        }
        await this.requireActiveConnectionSecret(source, client);
        const adapter = this.adapters.require('KAFKA');
        if (adapter.createKafkaTopic === undefined) {
          throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
        }
        completed = await adapter.createKafkaTopic({ configuration, password: null }, parsed);
        await this.audit.append(client, {
          action: completed.created ? 'KAFKA_TOPIC_CREATED' : 'KAFKA_TOPIC_ALREADY_EXISTS',
          actorType: 'USER',
          actorUserId: actor.id,
          details: {
            topic: parsed.topic,
            numPartitions: parsed.numPartitions,
            replicationFactor: parsed.replicationFactor,
            created: completed.created,
          },
          subjectId: dataSourceId,
          subjectType: 'DATA_SOURCE',
        });
        return completed;
      };
      // Callers composing task preparation keep one connection and own COMMIT.
      // They must retain this result when reporting an outer commit failure.
      return transactionClient === undefined
        ? await this.transaction(createWithin)
        : await createWithin(transactionClient);
    } catch (error) {
      if (completed !== undefined) throw new KafkaTopicAuditError(completed, { cause: error });
      throw error;
    }
  }

  private async insertSource(
    client: PoolClient,
    actorUserId: string,
    name: string,
    configuration: DataSourceConfiguration,
    password: string | null,
    copiedFromId?: string,
    credentialsRequired = false,
  ): Promise<DataSourceSummary> {
    const sourceId = randomUUID();
    const secretId = password === null ? null : randomUUID();
    if (secretId !== null && password !== null) {
      await this.insertConnectionSecret(client, sourceId, secretId, password);
    }
    const inserted = await client.query<DataSourceRow>(
      `INSERT INTO ape_dts_platform.data_sources (
             id, normalized_name, source_type, configuration, connection_secret_id,
             state, created_by_user_id, updated_by_user_id, credentials_required
           ) VALUES (
             $1::uuid, $2, $3, $4::jsonb, $5::uuid, 'ACTIVE', $6::uuid, $6::uuid, $7
           )
           RETURNING ${DATA_SOURCE_COLUMNS}`,
      [
        sourceId,
        normalizedDataSourceName(name),
        configuration.type,
        JSON.stringify(configuration),
        secretId,
        actorUserId,
        credentialsRequired,
      ],
    );
    const row = inserted.rows[0];
    if (row === undefined) {
      throw new Error('DATA_SOURCE_INSERT_FAILED');
    }
    await this.audit.append(client, {
      action: copiedFromId === undefined ? 'DATA_SOURCE_CREATED' : 'DATA_SOURCE_COPIED',
      actorType: 'USER',
      actorUserId,
      details: {
        sourceType: row.source_type,
        ...(copiedFromId === undefined ? {} : { copiedFromId }),
      },
      subjectId: row.id,
      subjectType: 'DATA_SOURCE',
    });
    return dataSourceSummary(row);
  }

  async create(actorUserId: string, request: CreateDataSourceRequest): Promise<DataSourceSummary> {
    try {
      return await this.transaction((client) =>
        this.insertSource(
          client,
          actorUserId,
          request.name,
          request.configuration,
          createPassword(request),
        ),
      );
    } catch (error) {
      if (postgresErrorCode(error) === '23505')
        throw new DataSourcesServiceError('DATA_SOURCE_NAME_ALREADY_EXISTS');
      throw error;
    }
  }

  // Configuration migration deliberately creates no secret and no grants.
  async importConfigurationWithin(
    client: PoolClient,
    actorUserId: string,
    input: {
      name: string;
      configuration: DataSourceConfiguration;
    },
  ): Promise<DataSourceSummary> {
    return this.insertSource(
      client,
      actorUserId,
      input.name,
      input.configuration,
      null,
      undefined,
      input.configuration.type !== 'KAFKA',
    );
  }

  async copy(
    actorUserId: string,
    dataSourceId: string,
    request: CopyDataSourceRequest,
  ): Promise<DataSourceSummary> {
    try {
      return await this.transaction(async (client) => {
        const source = await this.requireActiveSource(dataSourceId, client, 'FOR UPDATE');
        if (source.row_version !== request.expectedRowVersion)
          throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
        const password = source.credentials_required
          ? null
          : await this.decryptedConnectionPassword(source, client);
        try {
          return await this.insertSource(
            client,
            actorUserId,
            request.name,
            configurationFor(source),
            password?.toString('utf8') ?? null,
            source.id,
            source.credentials_required === true,
          );
        } finally {
          password?.fill(0);
        }
      });
    } catch (error) {
      if (postgresErrorCode(error) === '23505') {
        throw new DataSourcesServiceError('DATA_SOURCE_NAME_ALREADY_EXISTS');
      }
      throw error;
    }
  }

  async update(
    actorUserId: string,
    dataSourceId: string,
    request: UpdateDataSourceRequest,
  ): Promise<DataSourceSummary> {
    try {
      return await this.transaction(async (client) => {
        const existing = await this.requireActiveSource(dataSourceId, client);
        if (existing.row_version !== request.expectedRowVersion) {
          throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
        }
        if (existing.source_type !== request.configuration.type) {
          throw new DataSourcesServiceError('DATA_SOURCE_TYPE_IMMUTABLE');
        }
        const password = replacementPassword(request);
        let connectionSecretId = existing.connection_secret_id;
        if (password !== null) {
          if (existing.connection_secret_id === null && !existing.credentials_required) {
            throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
          }
          if (existing.connection_secret_id !== null) {
            const retired = await client.query(
              `UPDATE ape_dts_platform.secrets
             SET destroyed_at = statement_timestamp()
             WHERE id = $1::uuid
               AND owner_type = 'DATA_SOURCE'
               AND owner_id = $2::uuid
               AND purpose = 'CONNECTION_PASSWORD'
               AND destroyed_at IS NULL`,
              [existing.connection_secret_id, existing.id],
            );
            if (retired.rowCount !== 1) {
              throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
            }
          }
          connectionSecretId = randomUUID();
          await this.insertConnectionSecret(client, existing.id, connectionSecretId, password);
        } else if (existing.source_type !== 'KAFKA') {
          await this.requireActiveConnectionSecret(existing, client);
        }
        const updated = await client.query<DataSourceRow>(
          `UPDATE ape_dts_platform.data_sources
           SET normalized_name = $2,
               configuration = $3::jsonb,
               connection_secret_id = $4::uuid,
               credentials_required = false,
               row_version = row_version + 1,
               updated_by_user_id = $5::uuid,
               updated_at = statement_timestamp()
           WHERE id = $1::uuid
             AND state = 'ACTIVE'
             AND deleted_at IS NULL
             AND row_version = $6::bigint
           RETURNING ${DATA_SOURCE_COLUMNS}`,
          [
            existing.id,
            normalizedDataSourceName(request.name),
            JSON.stringify(request.configuration),
            connectionSecretId,
            actorUserId,
            request.expectedRowVersion,
          ],
        );
        const row = updated.rows[0];
        if (row === undefined) {
          throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
        }
        await this.audit.append(client, {
          action: 'DATA_SOURCE_UPDATED',
          actorType: 'USER',
          actorUserId,
          details: {
            connectionChange: request.credentials.mode,
            sourceType: row.source_type,
          },
          subjectId: row.id,
          subjectType: 'DATA_SOURCE',
        });
        return dataSourceSummary(row);
      });
    } catch (error) {
      if (postgresErrorCode(error) === '23505') {
        throw new DataSourcesServiceError('DATA_SOURCE_NAME_ALREADY_EXISTS');
      }
      throw error;
    }
  }

  async delete(
    actorUserId: string,
    dataSourceId: string,
    expectedRowVersion: string,
  ): Promise<DataSourceSummary> {
    return await this.transaction(async (client) => {
      const existing = await this.requireActiveSource(dataSourceId, client, 'FOR UPDATE');
      if (existing.row_version !== expectedRowVersion) {
        throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
      }
      const taskReference = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
           SELECT 1
             FROM ape_dts_platform.tasks
            WHERE deleted_at IS NULL
              AND (source_data_source_id = $1::uuid OR target_data_source_id = $1::uuid)
         ) AS exists`,
        [existing.id],
      );
      if (taskReference.rows[0]?.exists === true) {
        throw new DataSourcesServiceError('DATA_SOURCE_REFERENCED_BY_TASK');
      }
      const resourceReference = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
           SELECT 1
             FROM ape_dts_platform.external_resources
            WHERE data_source_id = $1::uuid
              AND lifecycle_state IN (
                'BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
                'CLEANUP_REQUESTED', 'CLEANING', 'CLEANUP_FAILED'
              )
         ) AS exists`,
        [existing.id],
      );
      if (resourceReference.rows[0]?.exists === true) {
        throw new DataSourcesServiceError('DATA_SOURCE_REFERENCED_BY_RESOURCE');
      }
      const deleted = await client.query<DataSourceRow>(
        `UPDATE ape_dts_platform.data_sources
         SET state = 'DELETED',
             deleted_at = statement_timestamp(),
             row_version = row_version + 1,
             updated_by_user_id = $2::uuid,
             updated_at = statement_timestamp()
         WHERE id = $1::uuid
           AND state = 'ACTIVE'
           AND deleted_at IS NULL
           AND row_version = $3::bigint
           AND NOT EXISTS (
             SELECT 1
               FROM ape_dts_platform.tasks
              WHERE tasks.deleted_at IS NULL
                AND (
                  tasks.source_data_source_id = ape_dts_platform.data_sources.id
                  OR tasks.target_data_source_id = ape_dts_platform.data_sources.id
                )
           )
           AND NOT EXISTS (
             SELECT 1
               FROM ape_dts_platform.external_resources
              WHERE external_resources.data_source_id = ape_dts_platform.data_sources.id
                AND external_resources.lifecycle_state IN (
                  'BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
                  'CLEANUP_REQUESTED', 'CLEANING', 'CLEANUP_FAILED'
                )
           )
         RETURNING ${DATA_SOURCE_COLUMNS}`,
        [existing.id, actorUserId, expectedRowVersion],
      );
      const row = deleted.rows[0];
      if (row === undefined) {
        throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
      }
      if (existing.connection_secret_id !== null) {
        const destroyed = await client.query(
          `UPDATE ape_dts_platform.secrets
           SET destroyed_at = statement_timestamp()
           WHERE id = $1::uuid
             AND owner_type = 'DATA_SOURCE'
             AND owner_id = $2::uuid
             AND purpose = 'CONNECTION_PASSWORD'
             AND destroyed_at IS NULL`,
          [existing.connection_secret_id, existing.id],
        );
        if (destroyed.rowCount !== 1) {
          throw new DataSourcesServiceError('DATA_SOURCE_SECRET_UNAVAILABLE');
        }
      }
      await client.query(
        `UPDATE ape_dts_platform.data_source_grants
         SET revoked_at = statement_timestamp(),
             revoked_by_user_id = $2::uuid
         WHERE data_source_id = $1::uuid
           AND revoked_at IS NULL`,
        [existing.id, actorUserId],
      );
      await this.audit.append(client, {
        action: 'DATA_SOURCE_DELETED',
        actorType: 'USER',
        actorUserId,
        details: { sourceType: row.source_type },
        subjectId: row.id,
        subjectType: 'DATA_SOURCE',
      });
      return dataSourceSummary(row);
    });
  }

  async listGrants(dataSourceId: string): Promise<readonly DataSourceGrant[]> {
    await this.requireActiveSource(dataSourceId);
    const result = await this.database.requirePool().query<DataSourceGrantRow>(
      `SELECT
         grants.data_source_id,
         grants.granted_at,
         users.id AS user_id,
         users.normalized_username AS user_normalized_username,
         users.display_name AS user_display_name,
         users.role AS user_role,
         users.status AS user_status,
         granted_by.id AS granted_by_id,
         granted_by.normalized_username AS granted_by_normalized_username,
         granted_by.display_name AS granted_by_display_name,
         granted_by.role AS granted_by_role,
         granted_by.status AS granted_by_status
       FROM ape_dts_platform.data_source_grants AS grants
       JOIN ape_dts_platform.users AS users ON users.id = grants.user_id
       JOIN ape_dts_platform.users AS granted_by ON granted_by.id = grants.granted_by_user_id
       WHERE grants.data_source_id = $1::uuid
         AND grants.revoked_at IS NULL
       ORDER BY users.normalized_username, users.id`,
      [dataSourceId],
    );
    return result.rows.map(grantSummary);
  }

  async grant(
    actor: UserSummary,
    dataSourceId: string,
    request: CreateDataSourceGrantRequest,
  ): Promise<DataSourceGrant> {
    try {
      return await this.transaction(async (client) => {
        await this.requireActiveSource(dataSourceId, client, 'FOR KEY SHARE');
        const userResult = await client.query<GrantUserRow>(
          `SELECT id, normalized_username, display_name, role, status
           FROM ape_dts_platform.users
           WHERE id = $1::uuid
           FOR KEY SHARE`,
          [request.userId],
        );
        const user = userResult.rows[0];
        if (user === undefined || user.role !== 'DEVELOPER' || user.status !== 'ACTIVE') {
          throw new DataSourcesServiceError('DATA_SOURCE_GRANT_TARGET_INVALID');
        }
        const inserted = await client.query<{ granted_at: Date }>(
          `INSERT INTO ape_dts_platform.data_source_grants (
             id, data_source_id, user_id, granted_by_user_id
           ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid)
           RETURNING granted_at`,
          [randomUUID(), dataSourceId, user.id, actor.id],
        );
        const grantedAt = inserted.rows[0]?.granted_at;
        if (grantedAt === undefined) {
          throw new Error('DATA_SOURCE_GRANT_INSERT_FAILED');
        }
        await this.audit.append(client, {
          action: 'DATA_SOURCE_GRANT_CREATED',
          actorType: 'USER',
          actorUserId: actor.id,
          details: { grantedUserId: user.id },
          subjectId: dataSourceId,
          subjectType: 'DATA_SOURCE',
        });
        return {
          dataSourceId,
          grantedAt: grantedAt.toISOString(),
          grantedBy: actor,
          user: userSummary(user),
        };
      });
    } catch (error) {
      if (postgresErrorCode(error) === '23505') {
        throw new DataSourcesServiceError('DATA_SOURCE_GRANT_ALREADY_EXISTS');
      }
      throw error;
    }
  }

  async revoke(actorUserId: string, dataSourceId: string, userId: string): Promise<void> {
    await this.transaction(async (client) => {
      await this.requireActiveSource(dataSourceId, client);
      const revoked = await client.query(
        `UPDATE ape_dts_platform.data_source_grants
         SET revoked_at = statement_timestamp(),
             revoked_by_user_id = $3::uuid
         WHERE data_source_id = $1::uuid
           AND user_id = $2::uuid
           AND revoked_at IS NULL`,
        [dataSourceId, userId, actorUserId],
      );
      if (revoked.rowCount !== 1) {
        throw new DataSourcesServiceError('DATA_SOURCE_GRANT_NOT_FOUND');
      }
      await this.audit.append(client, {
        action: 'DATA_SOURCE_GRANT_REVOKED',
        actorType: 'USER',
        actorUserId,
        details: { revokedUserId: userId },
        subjectId: dataSourceId,
        subjectType: 'DATA_SOURCE',
      });
    });
  }

  private async recordConnectionTest(
    actorUserId: string | null,
    dataSourceId: string,
    purpose: ConnectionTestPurpose,
    startedAt: Date,
    completedAt: Date,
    identity: DataSourceIdentity | null,
    failure: DataSourceAdapterError | null,
  ): Promise<ConnectionTestResponse> {
    const id = randomUUID();
    const durationMs = Math.max(0, completedAt.getTime() - startedAt.getTime());
    return await this.transaction(async (client) => {
      const inserted = await client.query<InsertedConnectionTestRow>(
        `INSERT INTO ape_dts_platform.connection_test_runs (
           id, data_source_id, initiated_by_user_id, purpose, state,
           started_at, completed_at, duration_ms, error_code, error_summary
         ) VALUES (
           $1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10
         )
         RETURNING id, completed_at`,
        [
          id,
          dataSourceId,
          actorUserId,
          purpose,
          failure === null ? 'SUCCEEDED' : 'FAILED',
          startedAt,
          completedAt,
          durationMs,
          failure?.code ?? null,
          failure?.summary ?? null,
        ],
      );
      if (inserted.rows[0] === undefined) {
        throw new Error('CONNECTION_TEST_RUN_INSERT_FAILED');
      }
      if (identity !== null) {
        await client.query(
          `INSERT INTO ape_dts_platform.data_source_identity_observations (
             id, data_source_id, connection_test_run_id, identity_kind,
             identity_value, server_version, observed_at
           ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7)`,
          [
            randomUUID(),
            dataSourceId,
            id,
            identity.kind,
            identity.value,
            identity.serverVersion,
            completedAt,
          ],
        );
      }
      await this.audit.append(client, {
        action:
          failure === null
            ? 'DATA_SOURCE_CONNECTION_TEST_SUCCEEDED'
            : 'DATA_SOURCE_CONNECTION_TEST_FAILED',
        actorType: actorUserId === null ? 'SYSTEM' : 'USER',
        actorUserId,
        details: { outcome: failure === null ? 'SUCCEEDED' : 'FAILED', purpose },
        subjectId: dataSourceId,
        subjectType: 'DATA_SOURCE',
      });
      return {
        completedAt: completedAt.toISOString(),
        dataSourceId,
        durationMs,
        errorCode: failure?.code ?? null,
        errorSummary: failure?.summary ?? null,
        id,
        identity,
        purpose,
        startedAt: startedAt.toISOString(),
        state: failure === null ? 'SUCCEEDED' : 'FAILED',
      };
    });
  }

  private connectionFailure(error: unknown): DataSourceAdapterError {
    if (error instanceof DataSourcesServiceError) {
      if (error.code === 'DATA_SOURCE_SECRET_UNAVAILABLE') {
        return new DataSourceAdapterError('CONNECTION_FAILED', '当前数据源凭据不可用。');
      }
      if (error.code === 'DATA_SOURCE_CONFIGURATION_UNAVAILABLE') {
        return new DataSourceAdapterError('CONNECTION_FAILED', '当前数据源配置不可用。');
      }
    }
    return toAdapterError(error);
  }

  async test(
    actor: UserSummary,
    dataSourceId: string,
    purpose: ConnectionTestPurpose = 'MANUAL',
  ): Promise<ConnectionTestResponse> {
    const source = await this.requireAccessibleSource(actor, dataSourceId);
    const startedAt = new Date();
    let identity: DataSourceIdentity | null = null;
    let failure: DataSourceAdapterError | null = null;
    try {
      identity = await this.withConnectionPassword(
        source,
        async (password) =>
          await this.adapters.require(source.source_type).test({
            configuration: configurationFor(source),
            password,
          }),
      );
    } catch (error) {
      if (
        error instanceof DataSourcesServiceError &&
        error.code === 'SECURITY_POLICY_UNAVAILABLE'
      ) {
        throw error;
      }
      failure = this.connectionFailure(error);
    }
    const response = await this.recordConnectionTest(
      actor.id,
      source.id,
      purpose,
      startedAt,
      new Date(),
      identity,
      failure,
    );
    await this.assertUnchangedAccessibleSource(actor, source);
    return response;
  }

  async testForSystem(
    dataSourceId: string,
    purpose: ConnectionTestPurpose = 'PREFLIGHT',
  ): Promise<ConnectionTestResponse> {
    const source = await this.requireActiveSource(dataSourceId);
    const startedAt = new Date();
    let identity: DataSourceIdentity | null = null;
    let failure: DataSourceAdapterError | null = null;
    try {
      identity = await this.withConnectionPassword(
        source,
        async (password) =>
          await this.adapters.require(source.source_type).test({
            configuration: configurationFor(source),
            password,
          }),
      );
    } catch (error) {
      if (
        error instanceof DataSourcesServiceError &&
        error.code === 'SECURITY_POLICY_UNAVAILABLE'
      ) {
        throw error;
      }
      failure = this.connectionFailure(error);
    }
    const response = await this.recordConnectionTest(
      null,
      source.id,
      purpose,
      startedAt,
      new Date(),
      identity,
      failure,
    );
    const current = await this.requireActiveSource(source.id);
    if (current.row_version !== source.row_version) {
      throw new DataSourcesServiceError('DATA_SOURCE_CHANGED_DURING_OPERATION');
    }
    return response;
  }

  private metadataFailure(
    source: DataSourceRow,
    query: MetadataQuery,
    attemptedAt: Date,
    error: unknown,
  ): MetadataResponse {
    const adapterError = this.connectionFailure(error);
    const base = {
      attemptedAt: attemptedAt.toISOString(),
      dataSourceId: source.id,
      errorCode: adapterError.code,
      errorSummary: adapterError.summary,
      resource: query.resource,
    };
    if (adapterError.code === 'PERMISSION_DENIED') {
      return { ...base, state: 'PERMISSION_DENIED' };
    }
    if (adapterError.code === 'TIMEOUT') {
      return { ...base, state: 'TIMEOUT' };
    }
    return { ...base, state: 'ERROR' };
  }

  async metadata(
    actor: UserSummary,
    dataSourceId: string,
    query: MetadataQuery,
  ): Promise<MetadataResponse> {
    const source = await this.requireAccessibleSource(actor, dataSourceId);
    const attemptedAt = new Date();
    try {
      const items = await this.withConnectionPassword(
        source,
        async (password) =>
          await this.adapters
            .require(source.source_type)
            .metadata({ configuration: configurationFor(source), password }, query),
      );
      try {
        await this.assertUnchangedAccessibleSource(actor, source);
      } catch (error) {
        if (
          error instanceof DataSourcesServiceError &&
          error.code === 'DATA_SOURCE_CHANGED_DURING_OPERATION'
        ) {
          return {
            attemptedAt: attemptedAt.toISOString(),
            dataSourceId: source.id,
            errorCode: 'DATA_SOURCE_CHANGED',
            errorSummary: '数据源在读取期间已变更，请刷新后重试。',
            resource: query.resource,
            state: 'ERROR',
          };
        }
        throw error;
      }
      const refreshedAt = new Date().toISOString();
      if (items.length === 0) {
        return {
          attemptedAt: attemptedAt.toISOString(),
          dataSourceId: source.id,
          refreshedAt,
          resource: query.resource,
          state: 'EMPTY',
        };
      }
      return {
        attemptedAt: attemptedAt.toISOString(),
        dataSourceId: source.id,
        items: [...items],
        refreshedAt,
        resource: query.resource,
        state: 'SUCCESS',
      };
    } catch (error) {
      if (
        error instanceof DataSourcesServiceError &&
        error.code === 'SECURITY_POLICY_UNAVAILABLE'
      ) {
        throw error;
      }
      return this.metadataFailure(source, query, attemptedAt, error);
    }
  }

  async metadataForSystem(dataSourceId: string, query: MetadataQuery): Promise<MetadataResponse> {
    const source = await this.requireActiveSource(dataSourceId);
    const attemptedAt = new Date();
    try {
      const items = await this.withConnectionPassword(
        source,
        async (password) =>
          await this.adapters
            .require(source.source_type)
            .metadata({ configuration: configurationFor(source), password }, query),
      );
      const current = await this.requireActiveSource(source.id);
      if (current.row_version !== source.row_version) {
        return {
          attemptedAt: attemptedAt.toISOString(),
          dataSourceId: source.id,
          errorCode: 'DATA_SOURCE_CHANGED',
          errorSummary: '数据源在读取期间已变更，请刷新后重试。',
          resource: query.resource,
          state: 'ERROR',
        };
      }
      const refreshedAt = new Date().toISOString();
      return items.length === 0
        ? {
            attemptedAt: attemptedAt.toISOString(),
            dataSourceId: source.id,
            refreshedAt,
            resource: query.resource,
            state: 'EMPTY',
          }
        : {
            attemptedAt: attemptedAt.toISOString(),
            dataSourceId: source.id,
            items: [...items],
            refreshedAt,
            resource: query.resource,
            state: 'SUCCESS',
          };
    } catch (error) {
      if (
        error instanceof DataSourcesServiceError &&
        error.code === 'SECURITY_POLICY_UNAVAILABLE'
      ) {
        throw error;
      }
      return this.metadataFailure(source, query, attemptedAt, error);
    }
  }

  async inspectMysqlCdc(
    actor: UserSummary,
    dataSourceId: string,
    expectedRowVersion: string,
  ): Promise<MysqlCdcInspection> {
    const source = await this.requireAccessibleSource(actor, dataSourceId);
    if (source.row_version !== expectedRowVersion)
      throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
    if (source.source_type !== 'MYSQL')
      throw new DataSourceAdapterError('METADATA_UNSUPPORTED', '仅 MySQL 数据源支持 Binlog 检查。');
    const adapter = this.adapters.require(source.source_type);
    if (adapter.inspectMysqlCdc === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const inspection = await this.withConnectionPassword(
      source,
      async (password) =>
        await adapter.inspectMysqlCdc!({ configuration: configurationFor(source), password }),
    );
    await this.assertUnchangedAccessibleSource(actor, source);
    return inspection;
  }

  async inspectMysqlCdcForSystem(
    dataSourceId: string,
    expectedRowVersion?: string,
  ): Promise<MysqlCdcInspection> {
    const source = await this.requireActiveSource(dataSourceId);
    if (expectedRowVersion !== undefined && source.row_version !== expectedRowVersion)
      throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
    const adapter = this.adapters.require(source.source_type);
    if (source.source_type !== 'MYSQL' || adapter.inspectMysqlCdc === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const result = await this.withConnectionPassword(
      source,
      async (password) =>
        await adapter.inspectMysqlCdc!({ configuration: configurationFor(source), password }),
    );
    const current = await this.requireActiveSource(source.id);
    if (current.row_version !== source.row_version)
      throw new DataSourcesServiceError('DATA_SOURCE_CHANGED_DURING_OPERATION');
    return result;
  }

  async readMysqlBinlogsForSystem(
    dataSourceId: string,
    expectedRowVersion?: string,
  ): Promise<readonly MysqlBinlogFile[]> {
    const source = await this.requireActiveSource(dataSourceId);
    if (expectedRowVersion !== undefined && source.row_version !== expectedRowVersion)
      throw new DataSourcesServiceError('DATA_SOURCE_VERSION_CONFLICT');
    const adapter = this.adapters.require(source.source_type);
    if (source.source_type !== 'MYSQL' || adapter.readMysqlBinlogs === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const result = await this.withConnectionPassword(
      source,
      async (password) =>
        await adapter.readMysqlBinlogs!({ configuration: configurationFor(source), password }),
    );
    const current = await this.requireActiveSource(source.id);
    if (current.row_version !== source.row_version)
      throw new DataSourcesServiceError('DATA_SOURCE_CHANGED_DURING_OPERATION');
    return result;
  }

  async readPostgresContinuityForSystem(
    dataSourceId: string,
    slotName: string,
  ): Promise<PostgresContinuityFact> {
    const source = await this.requireActiveSource(dataSourceId);
    if (source.source_type !== 'POSTGRES')
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const adapter = this.adapters.require(source.source_type);
    if (adapter.readPostgresContinuity === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    try {
      const fact = await this.withConnectionPassword(
        source,
        async (password) =>
          await adapter.readPostgresContinuity!(
            { configuration: configurationFor(source), password },
            slotName,
          ),
      );
      const current = await this.requireActiveSource(source.id);
      if (current.row_version !== source.row_version)
        throw new DataSourcesServiceError('DATA_SOURCE_CHANGED_DURING_OPERATION');
      return fact;
    } catch (error) {
      if (error instanceof DataSourcesServiceError) throw error;
      throw this.connectionFailure(error);
    }
  }

  async readPostgresContinuity(
    actor: UserSummary,
    dataSourceId: string,
    slotName: string,
  ): Promise<PostgresContinuityFact> {
    const source = await this.requireAccessibleSource(actor, dataSourceId);
    if (source.source_type !== 'POSTGRES')
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    const adapter = this.adapters.require(source.source_type);
    if (adapter.readPostgresContinuity === undefined)
      throw new DataSourcesServiceError('DATA_SOURCE_CONFIGURATION_UNAVAILABLE');
    try {
      const fact = await this.withConnectionPassword(
        source,
        async (password) =>
          await adapter.readPostgresContinuity!(
            { configuration: configurationFor(source), password },
            slotName,
          ),
      );
      await this.assertUnchangedAccessibleSource(actor, source);
      return fact;
    } catch (error) {
      if (error instanceof DataSourcesServiceError) throw error;
      throw this.connectionFailure(error);
    }
  }
}
