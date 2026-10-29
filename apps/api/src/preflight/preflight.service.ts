import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';
import {
  CertifiedAdapterRegistry,
  type PlatformTaskConfig,
  platformTaskConfigSchema,
  type TaskScenario,
  type PgToKafkaTaskConfig,
  type MysqlToKafkaTaskConfig,
  pgTaskMappings,
  kafkaTaskMappings,
  taskTargetTopics,
} from '@scs/config-model';
import type {
  ConnectionTestResponse,
  DataSourceSummary,
  MetadataItem,
  MetadataResponse,
  RunTaskPreflightRequest,
  TaskPreflightCheck,
  TaskPreflightResult,
  TaskPreflightRunSummary,
  TaskPreflightCheckKey,
  TaskType,
  UserSummary,
  CreateKafkaTopicResponse,
} from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';
import { taskPreflightResultSchema } from '@scs/contracts';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import {
  CapacityReservationError,
  CapacityReservationService,
} from '../pg-resources/capacity-reservation.service.js';
import { DataSourceAdapterError } from '../data-sources/adapters/types.js';
import {
  DataSourcesService,
  DataSourcesServiceError,
  KafkaTopicAuditError,
} from '../data-sources/data-sources.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { confirmedPgTableExpansion } from '../tasks/pg-table-expansion.js';
import { ContinuityService, type ContinuityObservation } from './continuity.service.js';
import {
  RUNTIME_ENVIRONMENT_CHECK,
  type RuntimeEnvironmentCheck,
} from './runtime-environment-check.js';

export const PREFLIGHT_PLATFORM_VERSION = Symbol('PREFLIGHT_PLATFORM_VERSION');

export type PreflightServiceErrorCode =
  | 'PREFLIGHT_ACCESS_DENIED'
  | 'PREFLIGHT_ALREADY_RUNNING'
  | 'PREFLIGHT_REVISION_NOT_CURRENT'
  | 'PREFLIGHT_TASK_NOT_FOUND'
  | 'PREFLIGHT_TASK_VERSION_CONFLICT';

export class PreflightServiceError extends Error {
  constructor(readonly code: PreflightServiceErrorCode) {
    super(code);
    this.name = 'PreflightServiceError';
  }
}

interface TaskFact extends QueryResultRow {
  current_applied_revision_id: string | null;
  current_draft_revision_id: string | null;
  desired_state: 'STOPPED' | 'RUNNING' | 'DELETED';
  deleted_at: Date | null;
  id: string;
  row_version: string;
  source_data_source_id: string;
  target_data_source_id: string;
  task_type: TaskType;
}

interface RevisionFact extends QueryResultRow {
  artifact_id: string | null;
  configuration: unknown;
  draft_version: string;
  id: string;
  state: string;
  task_id: string;
}

interface PreflightRunRow extends QueryResultRow {
  completed_at: Date | null;
  failure_code: string | null;
  failure_summary: string | null;
  id: string;
  result: TaskPreflightResult;
  revision_id: string;
  started_at: Date;
  state: TaskPreflightRunSummary['state'];
  task_id: string;
}

interface ArtifactRow extends QueryResultRow {
  actual_sha256: string | null;
  id: string;
  state: 'ACTIVE' | 'INACTIVE' | 'QUARANTINED';
}

interface DataSourceVersionRow extends QueryResultRow {
  id: string;
  row_version: string;
}

interface ResourceRow extends QueryResultRow {
  data_source_id: string;
  database_name: string | null;
  lifecycle_state: string;
  normalized_name: string;
  ownership_mode: string;
  resource_type: string;
  stable_identity: string;
}

interface TopicObservation {
  readonly clusterId: string;
  readonly dataSourceId: string;
  readonly partitionIds: readonly number[];
  readonly partitionCount: number | null;
  readonly topic: string;
  readonly topicId: string | null;
  readonly resourceType: 'KAFKA_SOURCE_TOPIC' | 'KAFKA_TARGET_TOPIC';
}

interface LoadedFacts {
  readonly revision: RevisionFact;
  readonly task: TaskFact;
  readonly source: DataSourceSummary;
  readonly target: DataSourceSummary;
}

interface Evaluation {
  readonly continuityObservation: ContinuityObservation | null;
  readonly passed: boolean;
  readonly primaryKey: string | null;
  readonly result: TaskPreflightResult;
  readonly topicObservations: readonly TopicObservation[];
}

export interface RunSystemRecoveryPreflightRequest {
  /** Manual recovery reserves new capacity only after the old reservation is fully released. */
  readonly reserveFreshCapacity?: boolean;
  readonly expectedOldRunId: string;
  readonly expectedPlatformInstanceId?: string;
  readonly expectedTaskRowVersion: string;
  readonly revisionId: string;
  readonly taskId: string;
}

export const SYSTEM_RECOVERY_PREFLIGHT = Symbol('SYSTEM_RECOVERY_PREFLIGHT');
export interface SystemRecoveryPreflightPort {
  prepareFirstStartTopic?(
    actor: UserSummary,
    taskId: string,
    request: RunTaskPreflightRequest,
  ): Promise<boolean>;
  runForSystemRecovery(
    request: RunSystemRecoveryPreflightRequest,
  ): Promise<TaskPreflightRunSummary>;
  revalidateContinuityBeforeRuntime(task: {
    readonly desiredState: 'DELETED' | 'RUNNING' | 'STOPPED';
    readonly id: string;
    readonly revisionId: string;
    readonly rowVersion: string;
    readonly reinitializeMysqlSnapshot?: boolean;
  }): Promise<void>;
}

type PreflightActor =
  | { readonly oldRunId: string; readonly type: 'SYSTEM'; readonly reserveFreshCapacity?: boolean }
  | {
      readonly type: 'USER';
      readonly user: UserSummary;
      readonly reinitializeMysqlSnapshot?: boolean;
    };

const TASK_COLUMNS = `
  id,
  task_type,
  source_data_source_id,
  target_data_source_id,
  current_draft_revision_id,
  current_applied_revision_id,
  desired_state,
  row_version::text,
  deleted_at`;

const REVISION_COLUMNS = `
  id,
  task_id,
  state,
  configuration,
  artifact_id,
  draft_version::text`;

const ACTIVE_RESOURCE_STATES = [
  'BOUND',
  'TRACKED_AFTER_STOP',
  'TRACKED_AFTER_DELETE',
  'CLEANUP_REQUESTED',
  'CLEANING',
  'CLEANUP_FAILED',
] as const;

function normalizedResourceName(value: string): string {
  return value.normalize('NFKC').trim().toLocaleLowerCase('en-US');
}

function scenarioFor(config: PlatformTaskConfig): TaskScenario {
  if (config.kind === 'KAFKA_RAW_FORWARD') {
    return 'KAFKA_RAW_FORWARD';
  }
  if (config.kind === 'MYSQL_TO_KAFKA')
    return config.source.includeSnapshot ? 'MYSQL_SNAPSHOT_TO_KAFKA' : 'MYSQL_CDC_TO_KAFKA';
  return config.source.includeSnapshot ? 'PG_SNAPSHOT_TO_KAFKA' : 'PG_CDC_TO_KAFKA';
}

function safeExternalError(error: unknown): { readonly code: string; readonly summary: string } {
  if (error instanceof DataSourceAdapterError) {
    return { code: error.code, summary: error.summary };
  }
  if (error instanceof DataSourcesServiceError) {
    switch (error.code) {
      case 'DATA_SOURCE_ACCESS_DENIED':
        return { code: 'DATA_SOURCE_ACCESS_DENIED', summary: '当前账号没有使用该数据源的权限。' };
      case 'DATA_SOURCE_CONFIGURATION_UNAVAILABLE':
        return { code: 'DATA_SOURCE_CONFIGURATION_UNAVAILABLE', summary: '数据源配置当前不可用。' };
      case 'DATA_SOURCE_SECRET_UNAVAILABLE':
        return { code: 'DATA_SOURCE_SECRET_UNAVAILABLE', summary: '数据源连接凭据当前不可用。' };
      case 'SECURITY_POLICY_UNAVAILABLE':
        return {
          code: 'SECURITY_POLICY_UNAVAILABLE',
          summary: '平台密钥当前未解锁，无法读取数据源凭据。',
        };
      case 'DATA_SOURCE_NOT_FOUND':
        return { code: 'DATA_SOURCE_NOT_FOUND', summary: '数据源不存在或已经不可用。' };
      default:
        return { code: error.code, summary: '数据源检查失败。' };
    }
  }
  if (error instanceof CapacityReservationError) {
    return { code: error.code, summary: '容量检查失败。' };
  }
  return { code: 'PREFLIGHT_EXTERNAL_CHECK_FAILED', summary: '外部依赖检查失败。' };
}

function check(
  key: TaskPreflightCheckKey,
  state: TaskPreflightCheck['state'],
  code: string,
  summary: string,
  details: Readonly<Record<string, unknown>> = {},
): TaskPreflightCheck {
  return { code, details, key, state, summary };
}

function metadataItems(response: MetadataResponse): readonly MetadataItem[] | null {
  return response.state === 'SUCCESS' || response.state === 'PARTIAL' ? response.items : null;
}

function metadataFailure(response: MetadataResponse): TaskPreflightCheck {
  if (response.state === 'SUCCESS' || response.state === 'EMPTY') {
    return check(
      'SOURCE_METADATA',
      'FAILED',
      'DATA_SOURCE_METADATA_EMPTY',
      '未读取到所需元数据。',
      {
        state: response.state,
      },
    );
  }
  if (response.state === 'PERMISSION_DENIED') {
    return check('SOURCE_METADATA', 'FAILED', response.errorCode, response.errorSummary, {
      state: response.state,
    });
  }
  return check('SOURCE_METADATA', 'FAILED', response.errorCode, response.errorSummary, {
    state: response.state,
  });
}

function connectionCheck(
  key: Extract<TaskPreflightCheckKey, 'SOURCE_CONNECTION' | 'TARGET_CONNECTION'>,
  response: ConnectionTestResponse,
): TaskPreflightCheck {
  if (response.state === 'FAILED') {
    return check(
      key,
      'FAILED',
      response.errorCode ?? 'DATA_SOURCE_CONNECTION_FAILED',
      response.errorSummary ?? '数据源连接检查失败。',
      { identity: response.identity, state: response.state },
    );
  }
  if (response.identity?.value === null || response.identity === null) {
    return check(
      key,
      'FAILED',
      'DATA_SOURCE_IDENTITY_UNAVAILABLE',
      '数据源连接成功，但无法读取稳定身份。',
      { identity: response.identity, state: response.state },
    );
  }
  return check(key, 'PASSED', 'DATA_SOURCE_CONNECTION_OK', '数据源连接和稳定身份检查通过。', {
    identity: response.identity,
    state: response.state,
  });
}

function topicItem(response: MetadataResponse, topic: string): MetadataItem | null {
  const items = metadataItems(response);
  if (items === null) {
    return null;
  }
  return items.find((item) => item.name === topic) ?? null;
}

function partitionIds(response: MetadataResponse): readonly number[] | null {
  const items = metadataItems(response);
  if (items === null) {
    return null;
  }
  const ids = items.map((item) => Number(item.name));
  if (ids.some((id) => !Number.isSafeInteger(id) || id < 0) || new Set(ids).size !== ids.length) {
    return null;
  }
  return ids.sort((left, right) => left - right);
}

function partitionCount(item: MetadataItem): number | null {
  const value = item.attributes.partitionCount;
  if (value === undefined || !/^\d+$/u.test(value)) {
    return null;
  }
  const count = Number(value);
  return Number.isSafeInteger(count) && count > 0 ? count : null;
}

function resultWith(
  result: TaskPreflightResult,
  update: Partial<TaskPreflightResult>,
): TaskPreflightResult {
  return { ...result, ...update };
}

@Injectable()
export class PreflightService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    private readonly dataSources: DataSourcesService,
    private readonly capacity: CapacityReservationService,
    private readonly adapters: CertifiedAdapterRegistry,
    private readonly continuity: ContinuityService,
    @Inject(PREFLIGHT_PLATFORM_VERSION) private readonly platformVersion: string,
    @Inject(RUNTIME_ENVIRONMENT_CHECK) private readonly runtimeEnvironment: RuntimeEnvironmentCheck,
  ) {}

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const value = await work(client);
      await client.query('COMMIT');
      return value;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async additionalCapacity(
    actor: PreflightActor,
    requested: number,
  ): Promise<number | null> {
    if (actor.type === 'USER') return requested;
    const held = await this.database.requirePool().query<{ count: string }>(
      `SELECT count(*)::text AS count
         FROM ape_dts_platform.capacity_reservations
        WHERE run_id = $1::uuid
          AND state IN ('RESERVED', 'ACTIVE')`,
      [actor.oldRunId],
    );
    const retained = Number(held.rows[0]?.count ?? '0');
    if (actor.reserveFreshCapacity) return retained === 0 ? requested : null;
    return retained === requested ? 0 : null;
  }

  private async loadFacts(
    actor: UserSummary,
    taskId: string,
    request: RunTaskPreflightRequest,
    client?: PoolClient,
  ): Promise<LoadedFacts> {
    const executor = client ?? this.database.requirePool();
    const taskResult = await executor.query<TaskFact>(
      `SELECT ${TASK_COLUMNS}
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid`,
      [taskId],
    );
    const task = taskResult.rows[0];
    if (task === undefined || task.deleted_at !== null) {
      throw new PreflightServiceError('PREFLIGHT_TASK_NOT_FOUND');
    }
    if (task.row_version !== request.expectedTaskRowVersion) {
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    }
    if (task.current_draft_revision_id !== request.revisionId) {
      throw new PreflightServiceError('PREFLIGHT_REVISION_NOT_CURRENT');
    }
    const revisionResult = await executor.query<RevisionFact>(
      `SELECT ${REVISION_COLUMNS}
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid
          AND task_id = $2::uuid`,
      [request.revisionId, taskId],
    );
    const revision = revisionResult.rows[0];
    if (revision === undefined) {
      throw new PreflightServiceError('PREFLIGHT_REVISION_NOT_CURRENT');
    }
    let source: DataSourceSummary;
    let target: DataSourceSummary;
    try {
      source = await this.dataSources.get(actor, task.source_data_source_id, client);
      target = await this.dataSources.get(actor, task.target_data_source_id, client);
    } catch (error) {
      if (error instanceof DataSourcesServiceError) {
        if (error.code === 'DATA_SOURCE_ACCESS_DENIED') {
          throw new PreflightServiceError('PREFLIGHT_ACCESS_DENIED');
        }
        if (error.code === 'DATA_SOURCE_NOT_FOUND') {
          throw new PreflightServiceError('PREFLIGHT_TASK_NOT_FOUND');
        }
      }
      throw error;
    }
    const facts = { revision, source, target, task };
    if (request.reinitializeMysqlSnapshot === true)
      await this.assertMysqlReinitializationAllowed(facts, executor);
    return facts;
  }

  private async assertMysqlReinitializationAllowed(
    facts: LoadedFacts,
    executor: Pick<PoolClient, 'query'>,
  ): Promise<void> {
    const parsed = platformTaskConfigSchema.safeParse(facts.revision.configuration);
    if (
      facts.task.task_type !== 'MYSQL_KAFKA' ||
      facts.task.desired_state !== 'STOPPED' ||
      !parsed.success ||
      parsed.data.kind !== 'MYSQL_TO_KAFKA' ||
      !parsed.data.source.includeSnapshot
    )
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    const active = await executor.query<{ exists: boolean }>(
      `SELECT EXISTS (
        SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id = $1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
        UNION ALL
        SELECT 1 FROM ape_dts_platform.task_run_units WHERE task_id = $1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
      ) AS exists`,
      [facts.task.id],
    );
    if (active.rows[0]?.exists !== false)
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
  }

  private async loadSystemFacts(request: RunSystemRecoveryPreflightRequest): Promise<LoadedFacts> {
    const taskResult = await this.database.requirePool().query<TaskFact>(
      `SELECT ${TASK_COLUMNS}
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid
          AND deleted_at IS NULL
          AND desired_state = 'RUNNING'
          AND current_applied_revision_id = $2::uuid
          AND row_version = $3::bigint`,
      [request.taskId, request.revisionId, request.expectedTaskRowVersion],
    );
    const task = taskResult.rows[0];
    if (task === undefined) {
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    }
    const runResult = await this.database.requirePool().query<{ id: string }>(
      `SELECT runs.id
         FROM ape_dts_platform.task_runs AS runs
        WHERE runs.id = $1::uuid
          AND runs.task_id = $2::uuid
          AND runs.task_revision_id = $3::uuid
          AND ($4::uuid IS NULL OR runs.platform_instance_id = $4::uuid)
          AND (runs.status IN ('STARTING', 'RUNNING', 'STOPPING', 'FAILED')
               OR (runs.status = 'BLOCKED' AND runs.current_stage = 'RECONCILIATION'))
          AND NOT EXISTS (
            SELECT 1
              FROM ape_dts_platform.task_runs AS newer
             WHERE newer.task_id = runs.task_id
               AND (newer.started_at, newer.id) > (runs.started_at, runs.id)
          )`,
      [
        request.expectedOldRunId,
        request.taskId,
        request.revisionId,
        request.expectedPlatformInstanceId ?? null,
      ],
    );
    if (runResult.rows[0] === undefined) {
      throw new PreflightServiceError('PREFLIGHT_REVISION_NOT_CURRENT');
    }
    const revisionResult = await this.database.requirePool().query<RevisionFact>(
      `SELECT ${REVISION_COLUMNS}
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid
          AND task_id = $2::uuid
          AND state = 'APPLIED'`,
      [request.revisionId, request.taskId],
    );
    const revision = revisionResult.rows[0];
    if (revision === undefined) {
      throw new PreflightServiceError('PREFLIGHT_REVISION_NOT_CURRENT');
    }
    const source = await this.dataSources.getForSystem(task.source_data_source_id);
    const target = await this.dataSources.getForSystem(task.target_data_source_id);
    return { revision, source, target, task };
  }

  private async startRun(
    actor: PreflightActor,
    taskId: string,
    revisionId: string,
    runId: string,
    result: TaskPreflightResult,
  ): Promise<void> {
    await this.transaction(async (client) => {
      await client.query(
        `SELECT id
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid
          FOR UPDATE`,
        [revisionId],
      );
      const running = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.preflight_runs
          WHERE task_revision_id = $1::uuid
            AND state = 'RUNNING'
          LIMIT 1`,
        [revisionId],
      );
      if (running.rows[0] !== undefined) {
        throw new PreflightServiceError('PREFLIGHT_ALREADY_RUNNING');
      }
      await client.query(
        `INSERT INTO ape_dts_platform.preflight_runs (
           id, task_revision_id, state, result
         ) VALUES ($1::uuid, $2::uuid, 'RUNNING', $3::jsonb)`,
        [runId, revisionId, JSON.stringify(result)],
      );
      await this.audit.append(client, {
        action: 'TASK_PREFLIGHT_STARTED',
        actorType: actor.type,
        actorUserId: actor.type === 'USER' ? actor.user.id : null,
        details: { revisionId, taskId },
        subjectId: taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async startSystemRun(
    request: RunSystemRecoveryPreflightRequest,
    runId: string,
    result: TaskPreflightResult,
  ): Promise<void> {
    await this.transaction(async (client) => {
      const bound = await client.query<{ id: string }>(
        `SELECT tasks.id
           FROM ape_dts_platform.tasks AS tasks
           JOIN ape_dts_platform.task_runs AS runs
             ON runs.id = $4::uuid
            AND runs.task_id = tasks.id
            AND runs.task_revision_id = $2::uuid
            AND ($5::uuid IS NULL OR runs.platform_instance_id = $5::uuid)
            AND (runs.status IN ('STARTING', 'RUNNING', 'STOPPING', 'FAILED')
                 OR (runs.status = 'BLOCKED' AND runs.current_stage = 'RECONCILIATION'))
          WHERE tasks.id = $1::uuid
            AND tasks.deleted_at IS NULL
            AND tasks.desired_state = 'RUNNING'
            AND tasks.current_applied_revision_id = $2::uuid
            AND tasks.row_version = $3::bigint
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_runs AS newer
               WHERE newer.task_id = tasks.id
                 AND (newer.started_at, newer.id) > (runs.started_at, runs.id)
            )
          FOR UPDATE OF tasks, runs`,
        [
          request.taskId,
          request.revisionId,
          request.expectedTaskRowVersion,
          request.expectedOldRunId,
          request.expectedPlatformInstanceId ?? null,
        ],
      );
      if (bound.rows[0] === undefined) {
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      }
      const running = await client.query<{ id: string }>(
        `SELECT id FROM ape_dts_platform.preflight_runs
          WHERE task_revision_id = $1::uuid AND state = 'RUNNING' LIMIT 1`,
        [request.revisionId],
      );
      if (running.rows[0] !== undefined) {
        throw new PreflightServiceError('PREFLIGHT_ALREADY_RUNNING');
      }
      await client.query(
        `INSERT INTO ape_dts_platform.preflight_runs (id, task_revision_id, state, result)
         VALUES ($1::uuid, $2::uuid, 'RUNNING', $3::jsonb)`,
        [runId, request.revisionId, JSON.stringify(result)],
      );
      await this.audit.append(client, {
        action: 'TASK_PREFLIGHT_STARTED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: {
          oldRunId: request.expectedOldRunId,
          revisionId: request.revisionId,
          taskId: request.taskId,
        },
        subjectId: request.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async connection(
    actor: PreflightActor,
    dataSourceId: string,
    key: Extract<TaskPreflightCheckKey, 'SOURCE_CONNECTION' | 'TARGET_CONNECTION'>,
  ): Promise<{
    readonly check: TaskPreflightCheck;
    readonly response: ConnectionTestResponse | null;
  }> {
    try {
      const response =
        actor.type === 'USER'
          ? await this.dataSources.test(actor.user, dataSourceId, 'PREFLIGHT')
          : await this.dataSources.testForSystem(dataSourceId, 'PREFLIGHT');
      return { check: connectionCheck(key, response), response };
    } catch (error) {
      const classified = safeExternalError(error);
      return {
        check: check(key, 'FAILED', classified.code, classified.summary),
        response: null,
      };
    }
  }

  private async metadata(
    actor: PreflightActor,
    dataSourceId: string,
    query: Parameters<DataSourcesService['metadata']>[2],
    key: Extract<TaskPreflightCheckKey, 'SOURCE_METADATA' | 'TARGET_METADATA'>,
  ): Promise<{ readonly check: TaskPreflightCheck; readonly response: MetadataResponse | null }> {
    try {
      const response =
        actor.type === 'USER'
          ? await this.dataSources.metadata(actor.user, dataSourceId, query)
          : await this.dataSources.metadataForSystem(dataSourceId, query);
      if (response.state === 'SUCCESS') {
        const legacyTopics =
          query.resource === 'TOPICS' && response.items.some((item) => item.identity === null);
        return {
          check: check(
            key,
            'PASSED',
            legacyTopics ? 'KAFKA_TOPIC_ID_UNAVAILABLE' : 'DATA_SOURCE_METADATA_OK',
            legacyTopics
              ? '已读取 Topic 元数据；Broker 未提供稳定 Topic ID，无法检测同名 Topic 删除后重建。'
              : '所需元数据读取完成。',
            {
              itemCount: response.items.length,
              resource: response.resource,
            },
          ),
          response,
        };
      }
      const failed = metadataFailure(response);
      const optional =
        key === 'TARGET_METADATA' &&
        query.resource === 'TOPICS' &&
        response.state !== 'PERMISSION_DENIED' &&
        failed.code !== 'PERMISSION_DENIED' &&
        failed.code !== 'CONNECTION_FAILED';
      return {
        check: {
          ...failed,
          key,
          ...(optional
            ? {
                state: 'SKIPPED' as const,
                summary: `${failed.summary} 将使用手动指定的 Topic；目标元数据暂不可核验。`,
              }
            : {}),
        },
        response,
      };
    } catch (error) {
      const classified = safeExternalError(error);
      return {
        check: check(key, 'FAILED', classified.code, classified.summary),
        response: null,
      };
    }
  }

  private async evaluateArtifact(
    revision: RevisionFact,
    config: PlatformTaskConfig | null,
  ): Promise<TaskPreflightCheck> {
    if (revision.artifact_id === null) {
      return check('ARTIFACT', 'FAILED', 'ARTIFACT_NOT_BOUND', '当前修订尚未绑定 ape-dts 制品。');
    }
    if (config === null) {
      return check('ARTIFACT', 'SKIPPED', 'CONFIGURATION_INVALID', '配置无效，暂不检查制品。');
    }
    const artifactResult = await this.database.requirePool().query<ArtifactRow>(
      `SELECT id, state, encode(actual_sha256, 'hex') AS actual_sha256
         FROM ape_dts_platform.artifacts
        WHERE id = $1::uuid AND deleted_at IS NULL`,
      [revision.artifact_id],
    );
    const artifact = artifactResult.rows[0];
    if (artifact === undefined) {
      return check('ARTIFACT', 'FAILED', 'ARTIFACT_NOT_FOUND', '绑定的 ape-dts 制品不存在。');
    }
    if (artifact.state === 'QUARANTINED') {
      return check(
        'ARTIFACT',
        'FAILED',
        'ARTIFACT_NOT_ACTIVE',
        '绑定的 ape-dts 制品当前不可启动。',
        {
          state: artifact.state,
        },
      );
    }
    if (artifact.actual_sha256 === null) {
      return check(
        'ARTIFACT',
        'FAILED',
        'ARTIFACT_HASH_UNAVAILABLE',
        '制品尚未形成可验证的实际哈希。',
      );
    }
    const scenario = scenarioFor(config);
    try {
      const adapter = this.adapters.resolveRuntime({
        artifactSha256: artifact.actual_sha256,
        platformVersion: this.platformVersion,
        scenario,
      });
      return check(
        'ARTIFACT',
        'PASSED',
        'ARTIFACT_AVAILABLE',
        '制品已绑定，使用平台内置配置适配器；实际兼容性将在运行时验证。',
        {
          adapterId: adapter.id,
          adapterVersion: adapter.version,
          platformVersion: this.platformVersion,
          scenario,
        },
      );
    } catch {
      return check(
        'ARTIFACT',
        'FAILED',
        'ARTIFACT_ADAPTER_BINDING_MISSING',
        '当前制品没有可解析的配置适配器绑定。',
        { platformVersion: this.platformVersion, scenario },
      );
    }
  }

  private async evaluatePgResources(
    actor: PreflightActor,
    taskId: string,
    source: DataSourceSummary,
    config: PgToKafkaTaskConfig,
  ): Promise<TaskPreflightCheck> {
    if (source.configuration.type !== 'POSTGRES') {
      return check(
        'PG_RESOURCES',
        'FAILED',
        'DATA_SOURCE_TYPE_INVALID',
        'PG 任务 Source 不是 PostgreSQL 数据源。',
      );
    }
    const expected = [
      { name: config.source.slotName, type: 'PG_SLOT' as const },
      { name: config.source.publicationName, type: 'PG_PUBLICATION' as const },
    ];
    const rows = await this.database.requirePool().query<ResourceRow>(
      `SELECT data_source_id, resource_type, normalized_name, database_name, ownership_mode,
              stable_identity, lifecycle_state
         FROM ape_dts_platform.external_resources
        WHERE task_id = $1::uuid
          AND data_source_id = $2::uuid
          AND resource_type = ANY($3::text[])
          AND lifecycle_state = ANY($4::text[])
        FOR SHARE`,
      [taskId, source.id, expected.map((item) => item.type), ACTIVE_RESOURCE_STATES],
    );
    const missing: string[] = [];
    const conflicts: string[] = [];
    for (const item of expected) {
      const matches = rows.rows.filter(
        (row) =>
          row.resource_type === item.type &&
          row.normalized_name === normalizedResourceName(item.name),
      );
      if (matches.length === 0) {
        missing.push(item.type);
        continue;
      }
      if (
        matches.length !== 1 ||
        matches[0]?.database_name !== source.configuration.database ||
        matches[0]?.ownership_mode !== config.source.resourceMode
      ) {
        conflicts.push(item.type);
      }
    }
    if (conflicts.length > 0) {
      return check(
        'PG_RESOURCES',
        'FAILED',
        'PG_RESOURCE_CONFLICT',
        'Slot 或 Publication 的绑定身份/所有权不匹配。',
        {
          conflicts,
        },
      );
    }
    if (missing.length > 0) {
      const code =
        config.source.resourceMode === 'PLATFORM_MANAGED'
          ? 'PG_RESOURCE_PROVISIONING_REQUIRED'
          : config.source.resourceMode === 'APE_DTS_MANAGED'
            ? 'PG_RESOURCE_RUNTIME_VERIFICATION_REQUIRED'
            : 'PG_RESOURCE_NOT_BOUND';
      return check(
        'PG_RESOURCES',
        'FAILED',
        code,
        'Slot 或 Publication 尚未形成可核验的外部资源绑定。',
        {
          missing,
          resourceMode: config.source.resourceMode,
        },
      );
    }
    try {
      const request = {
        slotName: config.source.slotName,
        publicationName: config.source.publicationName,
        ...(config.schemaVersion === 1
          ? { schema: config.source.table.schema, table: config.source.table.name }
          : { tables: pgTaskMappings(config).map((mapping) => mapping.source) }),
      };
      const observed =
        actor.type === 'USER'
          ? await this.dataSources.readPostgresResources(actor.user, source.id, request)
          : await this.dataSources.readPostgresResourcesForSystem(source.id, request);
      for (const item of expected) {
        const bound = rows.rows.find(
          (row) =>
            row.resource_type === item.type &&
            row.normalized_name === normalizedResourceName(item.name),
        );
        const actual = item.type === 'PG_SLOT' ? observed.slot : observed.publication;
        if (
          observed.databaseName !== source.configuration.database ||
          bound?.stable_identity !== actual.stableIdentity
        ) {
          return check(
            'PG_RESOURCES',
            'FAILED',
            'PG_RESOURCE_CONFLICT',
            '当前复制资源与已绑定身份不一致，请核实资源是否被替换。',
            { resourceType: item.type },
          );
        }
      }
    } catch (error) {
      const classified = safeExternalError(error);
      return check('PG_RESOURCES', 'FAILED', classified.code, classified.summary, {
        resourceMode: config.source.resourceMode,
      });
    }
    return check(
      'PG_RESOURCES',
      'PASSED',
      'PG_RESOURCES_BOUND',
      'Slot 和 Publication 实际状态、绑定身份、数据库及所有权检查通过。',
      {
        resources: expected.map((item) => {
          const row = rows.rows.find(
            (candidate) =>
              candidate.resource_type === item.type &&
              candidate.normalized_name === normalizedResourceName(item.name),
          );
          return {
            databaseName: row?.database_name ?? null,
            ownershipMode: row?.ownership_mode ?? null,
            resourceType: item.type,
            stableIdentity: row?.stable_identity ?? null,
            normalizedName: normalizedResourceName(item.name),
          };
        }),
        resourceMode: config.source.resourceMode,
      },
    );
  }

  private async evaluateTopicBaseline(
    taskId: string,
    observations: readonly TopicObservation[],
    requireExisting = false,
  ): Promise<TaskPreflightCheck> {
    if (observations.length === 0) {
      return check(
        'TOPIC_IDENTITIES',
        'FAILED',
        'KAFKA_TOPIC_OBSERVATION_MISSING',
        '未形成完整的 Kafka Topic 身份观测。',
      );
    }
    return check(
      'TOPIC_IDENTITIES',
      'PASSED',
      'KAFKA_TOPIC_IDENTITIES_OK',
      requireExisting
        ? '已形成系统恢复所需的完整 Kafka 身份观测。'
        : '已形成完整 Kafka 身份观测，连续性将在事务内判定。',
      {
        topics: observations.map((item) => ({
          clusterId: item.clusterId,
          partitionCount: item.partitionCount,
          partitionIds: item.partitionIds,
          resourceType: item.resourceType,
          topic: item.topic,
          topicId: item.topicId,
        })),
      },
    );
  }

  private async persistTopicBaselines(
    client: PoolClient,
    actor: UserSummary,
    taskId: string,
    observations: readonly TopicObservation[],
  ): Promise<void> {
    for (const observation of observations) {
      // No stable identity exists on older Kafka brokers. Never persist a fabricated one.
      if (observation.topicId === null) continue;
      const stableIdentity = `${observation.clusterId}:${observation.topicId}`;
      const current = await client.query<ResourceRow>(
        `SELECT data_source_id, resource_type, normalized_name, database_name, ownership_mode,
                stable_identity, lifecycle_state
           FROM ape_dts_platform.external_resources
          WHERE task_id = $1::uuid
            AND resource_type = $2
            AND data_source_id = $3::uuid
            AND normalized_name = $4
            AND lifecycle_state = ANY($5::text[])
          FOR UPDATE`,
        [
          taskId,
          observation.resourceType,
          observation.dataSourceId,
          observation.topic,
          ACTIVE_RESOURCE_STATES,
        ],
      );
      if (current.rows.some((row) => row.stable_identity !== stableIdentity)) {
        throw new Error('PREFLIGHT_TOPIC_IDENTITY_CHANGED_DURING_FINALIZE');
      }
      if (current.rows.length === 0) {
        await client.query(
          `INSERT INTO ape_dts_platform.external_resources (
             id, task_id, data_source_id, resource_type, normalized_name,
             database_name, ownership_mode, stable_identity, lifecycle_state,
             platform_created, created_by_user_id
           ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, NULL,
                     'SELF_MANAGED', $6, 'BOUND', false, $7::uuid)`,
          [
            randomUUID(),
            taskId,
            observation.dataSourceId,
            observation.resourceType,
            observation.topic,
            stableIdentity,
            actor.id,
          ],
        );
      }
    }
  }

  private async evaluateRelationalTables(
    actor: PreflightActor,
    source: DataSourceSummary,
    config: PgToKafkaTaskConfig | MysqlToKafkaTaskConfig,
  ) {
    const tables = [];
    for (const { source: table } of config.kind === 'MYSQL_TO_KAFKA'
      ? config.mappings
      : pgTaskMappings(config)) {
      if (
        config.kind === 'MYSQL_TO_KAFKA' &&
        (source.configuration.type !== 'MYSQL' || source.configuration.database !== table.schema)
      ) {
        const invalid = check(
          'SOURCE_METADATA',
          'FAILED',
          'MYSQL_SOURCE_DATABASE_MISMATCH',
          '来源表必须属于所选 MySQL 数据源的数据库。',
          { table },
        );
        tables.push({
          table,
          metadata: invalid,
          primaryKeyCheck: { ...invalid, key: 'PRIMARY_KEY' as const },
        });
        continue;
      }
      const metadata = await this.metadata(
        actor,
        source.id,
        {
          database:
            source.configuration.type === 'POSTGRES' || source.configuration.type === 'MYSQL'
              ? source.configuration.database
              : undefined,
          resource: 'COLUMNS',
          schema: table.schema,
          table: table.name,
        },
        'SOURCE_METADATA',
      );
      const columns = metadata.response === null ? null : metadataItems(metadata.response);
      const keys = columns?.filter((column) => column.attributes.isPrimaryKey === 'true') ?? [];
      const primaryKeyCheck =
        columns === null || columns.length === 0
          ? check(
              'PRIMARY_KEY',
              'FAILED',
              'SOURCE_TABLE_OR_COLUMNS_UNAVAILABLE',
              '未读取到所选源表字段，无法确认非空主键。',
            )
          : keys.length === 0
            ? check(
                'PRIMARY_KEY',
                'FAILED',
                'SOURCE_PRIMARY_KEY_MISSING',
                config.kind === 'MYSQL_TO_KAFKA'
                  ? 'MySQL 源表必须具有主键。'
                  : '源表必须具有主键（支持联合主键）。',
                { primaryKeyCount: keys.length },
              )
            : keys.some((key) => key.attributes.isNullable === 'true')
              ? check(
                  'PRIMARY_KEY',
                  'FAILED',
                  'SOURCE_PRIMARY_KEY_NULLABLE',
                  '源表主键列不能允许 NULL。',
                  { primaryKey: keys[0]?.name, primaryKeys: keys.map((key) => key.name) },
                )
              : check('PRIMARY_KEY', 'PASSED', 'SOURCE_PRIMARY_KEY_OK', '源表非空主键检查通过。', {
                  primaryKey:
                    config.kind === 'MYSQL_TO_KAFKA' || keys.length === 1 ? keys[0]!.name : null,
                  primaryKeys: keys.map((key) => key.name),
                });
      tables.push({ table, metadata: metadata.check, primaryKeyCheck });
    }
    const failedMetadata = tables.find((item) => item.metadata.state === 'FAILED');
    const failedPrimaryKey = tables.find((item) => item.primaryKeyCheck.state === 'FAILED');
    const first = tables[0];
    return {
      metadata:
        failedMetadata === undefined
          ? check(
              'SOURCE_METADATA',
              'PASSED',
              'DATA_SOURCE_METADATA_OK',
              '全部所选源表元数据读取完成。',
              { tables: tables.map((item) => item.table) },
            )
          : {
              ...failedMetadata.metadata,
              details: { ...failedMetadata.metadata.details, table: failedMetadata.table },
            },
      primaryKeyCheck:
        failedPrimaryKey === undefined
          ? check(
              'PRIMARY_KEY',
              'PASSED',
              'SOURCE_PRIMARY_KEY_OK',
              '全部源表非空主键检查通过。',
              tables.length === 1
                ? first?.primaryKeyCheck.details
                : {
                    tables: tables.map((item) => ({
                      ...item.table,
                      ...item.primaryKeyCheck.details,
                    })),
                  },
            )
          : {
              ...failedPrimaryKey.primaryKeyCheck,
              details: {
                ...failedPrimaryKey.primaryKeyCheck.details,
                table: failedPrimaryKey.table,
              },
            },
      primaryKey:
        tables.length === 1 && typeof first?.primaryKeyCheck.details.primaryKey === 'string'
          ? first.primaryKeyCheck.details.primaryKey
          : null,
    };
  }

  private async evaluate(actor: PreflightActor, facts: LoadedFacts): Promise<Evaluation> {
    let result: TaskPreflightResult = {
      capacity: { available: null, limit: null, requested: null, used: null },
      checks: [],
      dataSourceVersions: {
        source: facts.source.rowVersion,
        target: facts.target.rowVersion,
      },
      identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
      taskRevision: {
        draftVersion: facts.revision.draft_version,
        taskRowVersion: facts.task.row_version,
      },
    };
    const add = (item: TaskPreflightCheck): void => {
      result = resultWith(result, { checks: [...result.checks, item] });
    };

    add(await this.runtimeEnvironment.inspect());

    const parsedResult = platformTaskConfigSchema.safeParse(facts.revision.configuration);
    const config = parsedResult.success ? parsedResult.data : null;
    if (config === null) {
      add(
        check(
          'CONFIGURATION',
          'FAILED',
          'CONFIGURATION_INVALID',
          '当前修订配置不完整或不符合任务类型 Schema。',
        ),
      );
      add(
        check(
          'DATA_SOURCE_ACCESS',
          'PASSED',
          'DATA_SOURCE_ACCESS_OK',
          '任务引用的数据源访问权限已通过。',
        ),
      );
      add(
        check(
          'SOURCE_CONNECTION',
          'SKIPPED',
          'CONFIGURATION_INVALID',
          '配置无效，暂不连接 Source。',
        ),
      );
      add(
        check(
          'TARGET_CONNECTION',
          'SKIPPED',
          'CONFIGURATION_INVALID',
          '配置无效，暂不连接 Target。',
        ),
      );
      add(
        check(
          'SOURCE_METADATA',
          'SKIPPED',
          'CONFIGURATION_INVALID',
          '配置无效，暂不读取 Source 元数据。',
        ),
      );
      add(
        check(
          'TARGET_METADATA',
          'SKIPPED',
          'CONFIGURATION_INVALID',
          '配置无效，暂不读取 Target 元数据。',
        ),
      );
      add(check('PRIMARY_KEY', 'SKIPPED', 'CONFIGURATION_INVALID', '配置无效，暂不检查主键。'));
      add(
        check(
          'TOPIC_IDENTITIES',
          'SKIPPED',
          'CONFIGURATION_INVALID',
          '配置无效，暂不检查 Topic 身份。',
        ),
      );
      add(
        check('PG_RESOURCES', 'SKIPPED', 'CONFIGURATION_INVALID', '配置无效，暂不检查 PG 资源。'),
      );
      add(await this.evaluateArtifact(facts.revision, null));
      add(check('CAPACITY', 'SKIPPED', 'CONFIGURATION_INVALID', '配置无效，暂不检查容量。'));
      return {
        continuityObservation: null,
        passed: false,
        primaryKey: null,
        result,
        topicObservations: [],
      };
    }

    // Recovery has already bound the current applied revision and latest run;
    // user preflight remains limited to editable draft states.
    const revisionCanBePreflighted =
      actor.type === 'SYSTEM'
        ? facts.revision.state === 'APPLIED'
        : ['READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(facts.revision.state);
    if (!revisionCanBePreflighted) {
      add(
        check('CONFIGURATION', 'FAILED', 'REVISION_NOT_READY', '当前修订状态不允许执行预检。', {
          state: facts.revision.state,
        }),
      );
    } else if (
      config.kind !==
        (facts.task.task_type === 'PG_KAFKA'
          ? 'PG_TO_KAFKA'
          : facts.task.task_type === 'MYSQL_KAFKA'
            ? 'MYSQL_TO_KAFKA'
            : 'KAFKA_RAW_FORWARD') ||
      config.source.dataSourceId !== facts.task.source_data_source_id ||
      config.target.dataSourceId !== facts.task.target_data_source_id
    ) {
      add(
        check(
          'CONFIGURATION',
          'FAILED',
          'CONFIGURATION_TASK_BINDING_MISMATCH',
          '修订配置与任务类型或数据源绑定不一致。',
        ),
      );
    } else {
      add(
        check(
          'CONFIGURATION',
          'PASSED',
          'CONFIGURATION_VALID',
          '修订配置 Schema 和任务绑定检查通过。',
          actor.type === 'USER' && actor.reinitializeMysqlSnapshot === true
            ? { reinitializeMysqlSnapshot: true }
            : {},
        ),
      );
    }
    add(
      facts.source.configuration.type ===
        (config.kind === 'PG_TO_KAFKA'
          ? 'POSTGRES'
          : config.kind === 'MYSQL_TO_KAFKA'
            ? 'MYSQL'
            : 'KAFKA') && facts.target.configuration.type === 'KAFKA'
        ? check(
            'DATA_SOURCE_ACCESS',
            'PASSED',
            'DATA_SOURCE_ACCESS_OK',
            '任务引用的数据源类型和访问权限已通过。',
          )
        : check(
            'DATA_SOURCE_ACCESS',
            'FAILED',
            'DATA_SOURCE_TYPE_INVALID',
            '任务端点的数据源类型不符合任务类型。',
            {
              sourceType: facts.source.configuration.type,
              targetType: facts.target.configuration.type,
            },
          ),
    );

    const sourceConnection = await this.connection(actor, facts.source.id, 'SOURCE_CONNECTION');
    const targetConnection = await this.connection(actor, facts.target.id, 'TARGET_CONNECTION');
    add(sourceConnection.check);
    add(targetConnection.check);
    result = resultWith(result, {
      identities: {
        source: sourceConnection.response?.identity?.value ?? null,
        sourceTopic: null,
        target: targetConnection.response?.identity?.value ?? null,
        targetTopic: null,
      },
    });

    const topicObservations: TopicObservation[] = [];
    let continuityObservation: ContinuityObservation | null = null;
    let primaryKey: string | null = null;
    if (config.kind === 'PG_TO_KAFKA' || config.kind === 'MYSQL_TO_KAFKA') {
      const pg = config;
      const tables = await this.evaluateRelationalTables(actor, facts.source, pg);
      const targetMetadata = await this.metadata(
        actor,
        facts.target.id,
        { resource: 'TOPICS' },
        'TARGET_METADATA',
      );
      add(tables.metadata);
      add(targetMetadata.check);
      add(tables.primaryKeyCheck);
      primaryKey = tables.primaryKey;
      const targets = taskTargetTopics(pg);
      const missingTargets = targets.filter(
        (item) =>
          targetMetadata.response !== null &&
          ['SUCCESS', 'EMPTY'].includes(targetMetadata.response.state) &&
          topicItem(targetMetadata.response, item.topic) === null,
      );
      const targetCluster = targetConnection.response?.identity;
      let mysqlIdentity: string | null = null;
      let pgResources: TaskPreflightCheck;
      if (pg.kind === 'MYSQL_TO_KAFKA') {
        pgResources = check(
          'PG_RESOURCES',
          'SKIPPED',
          'NOT_APPLICABLE',
          'MySQL 任务不适用 PostgreSQL 复制资源检查。',
        );
        try {
          const inspection =
            actor.type === 'USER'
              ? await this.dataSources.inspectMysqlCdc(
                  actor.user,
                  facts.source.id,
                  facts.source.rowVersion,
                )
              : await this.dataSources.inspectMysqlCdcForSystem(
                  facts.source.id,
                  facts.source.rowVersion,
                );
          const failed = inspection.checks.filter((item) => item.state === 'FAILED');
          const identity =
            inspection.identity.kind === 'MYSQL_DATABASE' ? inspection.identity.value : null;
          const matched =
            identity !== null && identity === sourceConnection.response?.identity?.value;
          add(
            check(
              'MYSQL_CDC',
              failed.length === 0 && matched ? 'PASSED' : 'FAILED',
              failed.length === 0 && matched ? 'MYSQL_CDC_READY' : 'MYSQL_CDC_NOT_READY',
              failed.length === 0 && matched
                ? 'Binlog 设置、明确位点与源实例身份检查通过；复制流权限将在启动时验证。'
                : 'MySQL Binlog 设置、位点或实例身份检查未通过。',
              {
                checks: inspection.checks,
                binlog: inspection.binlog,
                position: inspection.position,
              },
            ),
          );
          if (failed.length === 0 && matched) mysqlIdentity = identity;
        } catch (error) {
          const classified = safeExternalError(error);
          add(check('MYSQL_CDC', 'FAILED', classified.code, classified.summary));
        }
      } else {
        pgResources = await this.evaluatePgResources(actor, facts.task.id, facts.source, pg);
      }
      if (
        missingTargets.length > 0 ||
        targetCluster?.kind !== 'KAFKA_CLUSTER' ||
        targetCluster.value === null
      ) {
        add(
          check(
            'TOPIC_IDENTITIES',
            'FAILED',
            missingTargets.length > 0 &&
              missingTargets.every((item) => item.topicCreation !== undefined) &&
              actor.type === 'USER' &&
              targetCluster?.kind === 'KAFKA_CLUSTER' &&
              targetCluster.value !== null &&
              ['SUCCESS', 'EMPTY'].includes(targetMetadata.response?.state ?? '')
              ? 'TARGET_TOPIC_CREATION_PENDING'
              : 'TARGET_TOPIC_IDENTITY_UNAVAILABLE',
            missingTargets.length > 0 &&
              missingTargets.every((item) => item.topicCreation !== undefined)
              ? '目标 Topic 尚未就绪。首次启动可由管理员准备 Topic，随后须重新预检，通过后可启动。'
              : '未找到目标 Topic 或 Kafka 集群不可用；请确认 Topic 已创建且连接正常。',
            { missingTopics: missingTargets.map((item) => item.topic) },
          ),
        );
      } else {
        result = resultWith(result, {
          identities: {
            ...result.identities,
            targetTopic:
              config.schemaVersion === 1 && targetMetadata.response !== null
                ? (topicItem(targetMetadata.response, config.target.topic)?.identity ?? null)
                : null,
          },
        });
        for (const item of targets) {
          const observed =
            targetMetadata.response === null
              ? null
              : topicItem(targetMetadata.response, item.topic);
          topicObservations.push({
            clusterId: targetCluster.value,
            dataSourceId: facts.target.id,
            partitionCount: observed === null ? null : partitionCount(observed),
            partitionIds: [],
            resourceType: 'KAFKA_TARGET_TOPIC',
            topic: item.topic,
            topicId: observed?.identity ?? null,
          });
        }
        if (pg.kind === 'MYSQL_TO_KAFKA' && mysqlIdentity !== null) {
          continuityObservation = {
            kind: 'MYSQL',
            sourceIdentity: mysqlIdentity,
            target: topicObservations[0]!,
            topics: topicObservations,
          };
        } else if (pg.kind === 'PG_TO_KAFKA' && pgResources.state === 'PASSED') {
          try {
            const source =
              actor.type === 'USER'
                ? await this.dataSources.readPostgresContinuity(
                    actor.user,
                    facts.source.id,
                    pg.source.slotName,
                  )
                : await this.dataSources.readPostgresContinuityForSystem(
                    facts.source.id,
                    pg.source.slotName,
                  );
            continuityObservation = {
              kind: 'POSTGRES',
              source,
              target: {
                clusterId: targetCluster.value,
                partitionCount: topicObservations[0]?.partitionCount ?? null,
                partitionIds: [],
                topicId: topicObservations[0]?.topicId ?? null,
              },
              topics: topicObservations,
            };
          } catch (error) {
            continuityObservation = null;
            const classified = safeExternalError(error);
            pgResources = check('PG_RESOURCES', 'FAILED', classified.code, classified.summary, {
              resourceMode: pg.source.resourceMode,
            });
          }
        }
        add(
          await this.evaluateTopicBaseline(
            facts.task.id,
            topicObservations,
            actor.type === 'SYSTEM',
          ),
        );
      }
      add(pgResources);
      add(await this.evaluateArtifact(facts.revision, config));
      const requested = 1;
      try {
        const capacity = await this.capacity.snapshot(requested);
        const additional = await this.additionalCapacity(actor, requested);
        result = resultWith(result, {
          capacity: {
            available: capacity.available,
            limit: capacity.limit,
            requested: capacity.requested,
            used: capacity.used,
          },
        });
        add(
          additional === null
            ? check(
                'CAPACITY',
                'FAILED',
                'CAPACITY_RESERVATION_MISMATCH',
                '自动恢复所需的旧运行容量预留不完整。',
              )
            : capacity.limit === null
              ? check(
                  'CAPACITY',
                  'FAILED',
                  'CAPACITY_NOT_CONFIGURED',
                  '平台尚未配置最大运行单元数。',
                )
              : capacity.used + additional > capacity.limit
                ? check(
                    'CAPACITY',
                    'FAILED',
                    'CAPACITY_INSUFFICIENT',
                    '当前容量不足以启动该任务。',
                    {
                      limit: capacity.limit,
                      requested,
                      used: capacity.used,
                    },
                  )
                : check('CAPACITY', 'PASSED', 'CAPACITY_AVAILABLE', '当前容量足以容纳该任务。', {
                    limit: capacity.limit,
                    requested,
                    used: capacity.used,
                  }),
        );
      } catch (error) {
        const classified = safeExternalError(error);
        add(check('CAPACITY', 'FAILED', classified.code, classified.summary));
      }
    } else {
      const sourceTopics = await this.metadata(
        actor,
        facts.source.id,
        { resource: 'TOPICS' },
        'SOURCE_METADATA',
      );
      const targetTopics = await this.metadata(
        actor,
        facts.target.id,
        { resource: 'TOPICS' },
        'TARGET_METADATA',
      );
      add(sourceTopics.check);
      add(targetTopics.check);
      const sourceCluster = sourceConnection.response?.identity;
      const targetCluster = targetConnection.response?.identity;
      let sourceCount: number | null = 0;
      for (const mapping of kafkaTaskMappings(config)) {
        const topic =
          sourceTopics.response === null ? null : topicItem(sourceTopics.response, mapping.source);
        const count = topic === null ? null : partitionCount(topic);
        const partitions =
          topic === null
            ? null
            : await this.metadata(
                actor,
                facts.source.id,
                { resource: 'PARTITIONS', topic: mapping.source },
                'SOURCE_METADATA',
              );
        const ids = partitions?.response == null ? null : partitionIds(partitions.response);
        if (partitions?.check.state === 'FAILED') add(partitions.check);
        if (
          topic === null ||
          count === null ||
          ids === null ||
          ids.length !== count ||
          sourceCluster?.kind !== 'KAFKA_CLUSTER' ||
          sourceCluster.value === null
        ) {
          sourceCount = null;
          continue;
        }
        if (sourceCount !== null) sourceCount += count;
        topicObservations.push({
          clusterId: sourceCluster.value,
          dataSourceId: facts.source.id,
          partitionCount: count,
          partitionIds: ids,
          resourceType: 'KAFKA_SOURCE_TOPIC',
          topic: mapping.source,
          topicId: topic.identity,
        });
      }
      const targets = taskTargetTopics(config);
      const missingTargets = targets.filter(
        (item) =>
          targetTopics.response !== null &&
          ['SUCCESS', 'EMPTY'].includes(targetTopics.response.state) &&
          topicItem(targetTopics.response, item.topic) === null,
      );
      const sourceNames = new Set(kafkaTaskMappings(config).map((item) => item.source));
      const loop =
        sourceCluster?.kind === 'KAFKA_CLUSTER' &&
        sourceCluster.value !== null &&
        targetCluster?.kind === 'KAFKA_CLUSTER' &&
        sourceCluster.value === targetCluster.value &&
        targets.some((item) => sourceNames.has(item.topic));
      if (loop) {
        add(
          check(
            'TOPIC_IDENTITIES',
            'FAILED',
            'KAFKA_TOPIC_LOOP',
            '同一 Kafka 集群中，目标 Topic 不能是任务的任一来源 Topic。',
          ),
        );
      } else if (
        sourceCount === null ||
        missingTargets.length > 0 ||
        sourceCluster?.kind !== 'KAFKA_CLUSTER' ||
        sourceCluster.value === null ||
        targetCluster?.kind !== 'KAFKA_CLUSTER' ||
        targetCluster.value === null
      ) {
        const canCreate =
          sourceCount !== null &&
          missingTargets.length > 0 &&
          missingTargets.every((item) => item.topicCreation !== undefined) &&
          actor.type === 'USER' &&
          sourceCluster?.kind === 'KAFKA_CLUSTER' &&
          sourceCluster.value !== null &&
          targetCluster?.kind === 'KAFKA_CLUSTER' &&
          targetCluster.value !== null;
        add(
          check(
            'TOPIC_IDENTITIES',
            'FAILED',
            canCreate ? 'TARGET_TOPIC_CREATION_PENDING' : 'KAFKA_TOPIC_METADATA_INVALID',
            canCreate
              ? '目标 Topic 尚未就绪。首次启动可由管理员准备 Topic，随后须重新预检，通过后可启动；源 Topic 和集群必须可用。'
              : '请确认全部 Topic 已创建、集群连接正常，且每个源 Topic 可读取完整 Partition 集合以计算运行容量。',
            { missingTopics: missingTargets.map((item) => item.topic) },
          ),
        );
      } else {
        for (const item of targets) {
          const topic =
            targetTopics.response === null ? null : topicItem(targetTopics.response, item.topic);
          topicObservations.push({
            clusterId: targetCluster.value,
            dataSourceId: facts.target.id,
            partitionCount: topic === null ? null : partitionCount(topic),
            partitionIds: [],
            resourceType: 'KAFKA_TARGET_TOPIC',
            topic: item.topic,
            topicId: topic?.identity ?? null,
          });
        }
        const source = topicObservations.find((item) => item.resourceType === 'KAFKA_SOURCE_TOPIC');
        const target = topicObservations.find((item) => item.resourceType === 'KAFKA_TARGET_TOPIC');
        if (source === undefined || target === undefined)
          throw new Error('TOPIC_OBSERVATIONS_EMPTY');
        result = resultWith(result, {
          identities: {
            ...result.identities,
            sourceTopic: config.schemaVersion === 1 ? source.topicId : null,
            targetTopic: config.schemaVersion === 1 ? target.topicId : null,
          },
        });
        continuityObservation = { kind: 'KAFKA', source, target, topics: topicObservations };
        add(
          await this.evaluateTopicBaseline(
            facts.task.id,
            topicObservations,
            actor.type === 'SYSTEM',
          ),
        );
      }
      add(
        check(
          'PRIMARY_KEY',
          'SKIPPED',
          'NOT_APPLICABLE',
          'Kafka Raw 任务不适用 PostgreSQL 主键检查。',
        ),
      );
      add(
        check(
          'PG_RESOURCES',
          'SKIPPED',
          'NOT_APPLICABLE',
          'Kafka Raw 任务不适用 PostgreSQL 资源检查。',
        ),
      );
      add(await this.evaluateArtifact(facts.revision, config));
      if (sourceCount === null) {
        add(
          check(
            'CAPACITY',
            'FAILED',
            'KAFKA_PARTITION_COUNT_UNAVAILABLE',
            '无法确认 Source Topic Partition 数，不能计算容量。',
          ),
        );
      } else {
        try {
          const capacity = await this.capacity.snapshot(sourceCount);
          const additional = await this.additionalCapacity(actor, sourceCount);
          result = resultWith(result, {
            capacity: {
              available: capacity.available,
              limit: capacity.limit,
              requested: capacity.requested,
              used: capacity.used,
            },
          });
          add(
            additional === null
              ? check(
                  'CAPACITY',
                  'FAILED',
                  'CAPACITY_RESERVATION_MISMATCH',
                  '自动恢复所需的旧运行容量预留不完整。',
                )
              : capacity.limit === null
                ? check(
                    'CAPACITY',
                    'FAILED',
                    'CAPACITY_NOT_CONFIGURED',
                    '平台尚未配置最大运行单元数。',
                  )
                : capacity.used + additional > capacity.limit
                  ? check(
                      'CAPACITY',
                      'FAILED',
                      'CAPACITY_INSUFFICIENT',
                      '当前容量不足以启动全部 Source Partition 运行单元。',
                      {
                        limit: capacity.limit,
                        requested: sourceCount,
                        used: capacity.used,
                      },
                    )
                  : check(
                      'CAPACITY',
                      'PASSED',
                      'CAPACITY_AVAILABLE',
                      '当前容量足以容纳全部 Source Partition 运行单元。',
                      {
                        limit: capacity.limit,
                        requested: sourceCount,
                        used: capacity.used,
                      },
                    ),
          );
        } catch (error) {
          const classified = safeExternalError(error);
          add(check('CAPACITY', 'FAILED', classified.code, classified.summary));
        }
      }
    }
    const blockingChecks = result.checks.filter((item) => item.state !== 'SKIPPED');
    return {
      continuityObservation,
      passed: blockingChecks.length > 0 && blockingChecks.every((item) => item.state === 'PASSED'),
      primaryKey,
      result,
      topicObservations,
    };
  }

  private async collectRuntimeContinuity(
    config: PlatformTaskConfig,
  ): Promise<ContinuityObservation> {
    const sourceId = config.source.dataSourceId;
    const targetId = config.target.dataSourceId;
    const [sourceConnection, targetConnection, targetMetadata] = await Promise.all([
      this.dataSources.testForSystem(sourceId, 'PREFLIGHT'),
      this.dataSources.testForSystem(targetId, 'PREFLIGHT'),
      this.dataSources.metadataForSystem(targetId, { resource: 'TOPICS' }),
    ]);
    if (
      targetMetadata.state === 'PERMISSION_DENIED' ||
      ('errorCode' in targetMetadata &&
        ['PERMISSION_DENIED', 'CONNECTION_FAILED'].includes(targetMetadata.errorCode)) ||
      targetConnection.identity?.kind !== 'KAFKA_CLUSTER' ||
      targetConnection.identity.value === null
    )
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    const topics: TopicObservation[] = [];
    for (const item of taskTargetTopics(config)) {
      const topic = topicItem(targetMetadata, item.topic);
      if (topic === null && ['SUCCESS', 'EMPTY'].includes(targetMetadata.state))
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      topics.push({
        clusterId: targetConnection.identity.value,
        dataSourceId: targetId,
        partitionCount: topic === null ? null : partitionCount(topic),
        partitionIds: [],
        resourceType: 'KAFKA_TARGET_TOPIC',
        topic: item.topic,
        topicId: topic?.identity ?? null,
      });
    }
    const target = topics[0];
    if (target === undefined) throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    let observation: ContinuityObservation;
    if (config.kind === 'PG_TO_KAFKA') {
      observation = {
        kind: 'POSTGRES',
        source: await this.dataSources.readPostgresContinuityForSystem(
          sourceId,
          config.source.slotName,
        ),
        target,
        topics,
      };
    } else if (config.kind === 'MYSQL_TO_KAFKA') {
      const source = await this.dataSources.inspectMysqlCdcForSystem(sourceId);
      if (
        source.identity.kind !== 'MYSQL_DATABASE' ||
        source.identity.value === null ||
        sourceConnection.identity?.kind !== 'MYSQL_DATABASE' ||
        sourceConnection.identity.value !== source.identity.value ||
        source.checks.some((item) => item.state === 'FAILED')
      )
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      observation = { kind: 'MYSQL', sourceIdentity: source.identity.value, target, topics };
    } else {
      const sourceMetadata = await this.dataSources.metadataForSystem(sourceId, {
        resource: 'TOPICS',
      });
      const sources: TopicObservation[] = [];
      for (const mapping of kafkaTaskMappings(config)) {
        const source = topicItem(sourceMetadata, mapping.source);
        const count = source === null ? null : partitionCount(source);
        const partitions = partitionIds(
          await this.dataSources.metadataForSystem(sourceId, {
            resource: 'PARTITIONS',
            topic: mapping.source,
          }),
        );
        if (
          source === null ||
          count === null ||
          partitions === null ||
          partitions.length !== count ||
          sourceConnection.identity?.kind !== 'KAFKA_CLUSTER' ||
          sourceConnection.identity.value === null
        )
          throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
        if (
          sourceConnection.identity.value === target.clusterId &&
          taskTargetTopics(config).some((item) => item.topic === mapping.source)
        )
          throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
        sources.push({
          clusterId: sourceConnection.identity.value,
          dataSourceId: sourceId,
          partitionCount: count,
          partitionIds: partitions,
          resourceType: 'KAFKA_SOURCE_TOPIC',
          topic: mapping.source,
          topicId: source.identity,
        });
      }
      const source = sources[0];
      if (source === undefined) throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      observation = { kind: 'KAFKA', source, target, topics: [...sources, ...topics] };
    }
    return observation;
  }

  async revalidateContinuityBeforeRuntime(task: {
    readonly desiredState: 'DELETED' | 'RUNNING' | 'STOPPED';
    readonly id: string;
    readonly revisionId: string;
    readonly rowVersion: string;
    readonly reinitializeMysqlSnapshot?: boolean;
  }): Promise<void> {
    const loaded = await this.database.requirePool().query<{ configuration: unknown }>(
      `SELECT revision.configuration
         FROM ape_dts_platform.tasks AS current_task
         JOIN ape_dts_platform.task_revisions AS revision
           ON revision.id = $2::uuid AND revision.task_id = current_task.id
        WHERE current_task.id = $1::uuid AND current_task.deleted_at IS NULL
          AND current_task.row_version = $3::bigint AND current_task.desired_state = $4
          AND (current_task.current_draft_revision_id = revision.id
               OR current_task.current_applied_revision_id = revision.id)`,
      [task.id, task.revisionId, task.rowVersion, task.desiredState],
    );
    const configuration = platformTaskConfigSchema.parse(loaded.rows[0]?.configuration);
    // External probes use metadata connections themselves. Never hold a transaction
    // connection while awaiting them, or concurrent recoveries can exhaust the pool.
    const observation = await this.collectRuntimeContinuity(configuration);
    const passed = await this.transaction(
      async (client) =>
        (
          await this.continuity.decideWithin(
            client,
            { type: 'SYSTEM' },
            task.id,
            task.revisionId,
            task.rowVersion,
            task.desiredState,
            observation,
          )
        ).state === 'PASSED',
    );
    if (!passed) throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
  }

  private async loadRun(runId: string): Promise<TaskPreflightRunSummary> {
    const result = await this.database.requirePool().query<PreflightRunRow>(
      `SELECT id, task_revision_id AS revision_id,
              (SELECT task_id FROM ape_dts_platform.task_revisions WHERE id = preflight_runs.task_revision_id) AS task_id,
              state, started_at, completed_at, result, failure_code, failure_summary
         FROM ape_dts_platform.preflight_runs
        WHERE id = $1::uuid`,
      [runId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error('PREFLIGHT_RUN_NOT_FOUND');
    }
    return {
      completedAt: row.completed_at?.toISOString() ?? null,
      failureCode: row.failure_code,
      failureSummary: row.failure_summary,
      id: row.id,
      result: row.result,
      revisionId: row.revision_id,
      startedAt: row.started_at.toISOString(),
      state: row.state,
      taskId: row.task_id,
    };
  }

  private async finalize(
    actor: UserSummary,
    facts: LoadedFacts,
    request: RunTaskPreflightRequest,
    runId: string,
    evaluation: Evaluation,
  ): Promise<TaskPreflightRunSummary> {
    await this.transaction(async (client) => {
      const currentTaskResult = await client.query<TaskFact>(
        `SELECT ${TASK_COLUMNS}
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
          FOR UPDATE`,
        [facts.task.id],
      );
      const currentTask = currentTaskResult.rows[0];
      const currentRevisionResult = await client.query<RevisionFact>(
        `SELECT ${REVISION_COLUMNS}
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid
            AND task_id = $2::uuid
          FOR UPDATE`,
        [facts.revision.id, facts.task.id],
      );
      const currentRevision = currentRevisionResult.rows[0];
      const currentDataSourcesResult = await client.query<DataSourceVersionRow>(
        `SELECT id, row_version::text
           FROM ape_dts_platform.data_sources
          WHERE id = ANY($1::uuid[])
            AND state = 'ACTIVE'
            AND deleted_at IS NULL
          FOR KEY SHARE`,
        [[facts.source.id, facts.target.id]],
      );
      const currentDataSourceVersions = new Map(
        currentDataSourcesResult.rows.map((row) => [row.id, row.row_version]),
      );
      const stale =
        currentTask === undefined ||
        currentRevision === undefined ||
        currentTask.deleted_at !== null ||
        currentTask.row_version !== request.expectedTaskRowVersion ||
        currentTask.current_draft_revision_id !== facts.revision.id ||
        currentRevision.draft_version !== facts.revision.draft_version ||
        currentDataSourceVersions.get(facts.source.id) !== facts.source.rowVersion ||
        currentDataSourceVersions.get(facts.target.id) !== facts.target.rowVersion;
      if (stale) {
        await client.query(
          `UPDATE ape_dts_platform.preflight_runs
              SET state = 'STALE', completed_at = statement_timestamp(),
                  result = $2::jsonb,
                  failure_code = 'PREFLIGHT_STALE',
                  failure_summary = '任务或修订在预检期间发生变化，结果不能用于启动。'
            WHERE id = $1::uuid
              AND state = 'RUNNING'`,
          [runId, JSON.stringify(evaluation.result)],
        );
        await this.audit.append(client, {
          action: 'TASK_PREFLIGHT_STALE',
          actorType: 'USER',
          actorUserId: actor.id,
          details: { revisionId: facts.revision.id, taskId: facts.task.id },
          subjectId: facts.task.id,
          subjectType: 'TASK',
        });
        return;
      }

      let finalizedEvaluation = evaluation;
      if (
        evaluation.continuityObservation !== null &&
        evaluation.result.checks
          .filter((item) => item.key === 'TOPIC_IDENTITIES')
          .every((item) => item.state === 'PASSED')
      ) {
        if (request.reinitializeMysqlSnapshot === true && evaluation.passed)
          await this.assertMysqlReinitializationAllowed(facts, client);
        const continuityCheck =
          request.reinitializeMysqlSnapshot === true
            ? evaluation.passed && evaluation.continuityObservation.kind === 'MYSQL'
              ? await this.continuity.reinitializeMysqlWithin(
                  client,
                  actor.id,
                  facts.task.id,
                  facts.revision.id,
                  evaluation.continuityObservation,
                )
              : evaluation.result.checks.find((item) => item.key === 'TOPIC_IDENTITIES')!
            : await this.continuity.decideWithin(
                client,
                { type: 'USER', userId: actor.id },
                facts.task.id,
                facts.revision.id,
                currentTask.row_version,
                currentTask.desired_state,
                evaluation.continuityObservation,
              );
        const checks = evaluation.result.checks.map((item) =>
          item.key === 'TOPIC_IDENTITIES'
            ? { ...continuityCheck, details: { ...item.details, ...continuityCheck.details } }
            : item,
        );
        const blocking = checks.filter((item) => item.state !== 'SKIPPED');
        finalizedEvaluation = {
          ...evaluation,
          passed: blocking.length > 0 && blocking.every((item) => item.state === 'PASSED'),
          result: resultWith(evaluation.result, { checks }),
        };
      }

      if (finalizedEvaluation.passed) {
        if (finalizedEvaluation.topicObservations.length > 0) {
          await this.persistTopicBaselines(
            client,
            actor,
            facts.task.id,
            finalizedEvaluation.topicObservations,
          );
        }
        if (facts.task.task_type === 'PG_KAFKA') {
          // This legacy scalar is not a runtime key definition. Clear it for composite/multi-table
          // keys rather than leaving a previous single-column value; full keys live in the result.
          await client.query(
            `UPDATE ape_dts_platform.pg_kafka_revision_configs
                SET primary_key_column = $2
              WHERE task_revision_id = $1::uuid`,
            [facts.revision.id, finalizedEvaluation.primaryKey],
          );
        }
        const updatedRevision = await client.query(
          `UPDATE ape_dts_platform.task_revisions
              SET state = 'PREFLIGHT_PASSED',
                  row_version = row_version + 1,
                  updated_by_user_id = $3::uuid,
                  updated_at = statement_timestamp()
            WHERE id = $1::uuid
              AND task_id = $2::uuid
              AND state IN ('READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED')
              AND draft_version = $4::bigint`,
          [facts.revision.id, facts.task.id, actor.id, facts.revision.draft_version],
        );
        if (updatedRevision.rowCount !== 1) {
          throw new Error('PREFLIGHT_REVISION_UPDATE_CONFLICT');
        }
        await client.query(
          `DELETE FROM ape_dts_platform.risk_acceptances
            WHERE task_revision_id = $1::uuid`,
          [facts.revision.id],
        );
      } else if (
        currentRevision.state === 'PREFLIGHT_PASSED' ||
        currentRevision.state === 'RISK_CONFIRMED'
      ) {
        await client.query(
          `UPDATE ape_dts_platform.task_revisions
              SET state = 'READY_DRAFT',
                  row_version = row_version + 1,
                  updated_by_user_id = $3::uuid,
                  updated_at = statement_timestamp()
            WHERE id = $1::uuid
              AND task_id = $2::uuid
              AND state IN ('PREFLIGHT_PASSED', 'RISK_CONFIRMED')
              AND draft_version = $4::bigint`,
          [facts.revision.id, facts.task.id, actor.id, facts.revision.draft_version],
        );
        await client.query(
          `DELETE FROM ape_dts_platform.risk_acceptances
            WHERE task_revision_id = $1::uuid`,
          [facts.revision.id],
        );
      }

      const failed = finalizedEvaluation.result.checks.find((item) => item.state === 'FAILED');
      await client.query(
        `UPDATE ape_dts_platform.preflight_runs
            SET state = $2,
                completed_at = statement_timestamp(),
                result = $3::jsonb,
                failure_code = $4,
                failure_summary = $5
          WHERE id = $1::uuid
            AND state = 'RUNNING'`,
        [
          runId,
          finalizedEvaluation.passed ? 'PASSED' : 'FAILED',
          JSON.stringify(finalizedEvaluation.result),
          finalizedEvaluation.passed ? null : (failed?.code ?? 'PREFLIGHT_FAILED'),
          finalizedEvaluation.passed ? null : (failed?.summary ?? '预检未通过。'),
        ],
      );
      await this.audit.append(client, {
        action: finalizedEvaluation.passed ? 'TASK_PREFLIGHT_PASSED' : 'TASK_PREFLIGHT_FAILED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: {
          checkCount: finalizedEvaluation.result.checks.length,
          revisionId: facts.revision.id,
          taskId: facts.task.id,
        },
        subjectId: facts.task.id,
        subjectType: 'TASK',
      });
    });
    return await this.loadRun(runId);
  }

  private async finalizeSystem(
    facts: LoadedFacts,
    request: RunSystemRecoveryPreflightRequest,
    runId: string,
    evaluation: Evaluation,
  ): Promise<TaskPreflightRunSummary> {
    await this.transaction(async (client) => {
      const bound = await client.query<{ id: string }>(
        `SELECT tasks.id
           FROM ape_dts_platform.tasks AS tasks
           JOIN ape_dts_platform.task_runs AS runs
             ON runs.id = $4::uuid
            AND runs.task_id = tasks.id
            AND runs.task_revision_id = $2::uuid
            AND ($5::uuid IS NULL OR runs.platform_instance_id = $5::uuid)
            AND (runs.status IN ('STARTING', 'RUNNING', 'STOPPING', 'FAILED')
                 OR (runs.status = 'BLOCKED' AND runs.current_stage = 'RECONCILIATION'))
          WHERE tasks.id = $1::uuid
            AND tasks.deleted_at IS NULL
            AND tasks.desired_state = 'RUNNING'
            AND tasks.current_applied_revision_id = $2::uuid
            AND tasks.row_version = $3::bigint
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_runs AS newer
               WHERE newer.task_id = tasks.id
                 AND (newer.started_at, newer.id) > (runs.started_at, runs.id)
            )
          FOR UPDATE OF tasks, runs`,
        [
          request.taskId,
          request.revisionId,
          request.expectedTaskRowVersion,
          request.expectedOldRunId,
          request.expectedPlatformInstanceId ?? null,
        ],
      );
      const versions = await client.query<DataSourceVersionRow>(
        `SELECT id, row_version::text FROM ape_dts_platform.data_sources
          WHERE id = ANY($1::uuid[]) AND state = 'ACTIVE' AND deleted_at IS NULL
          FOR KEY SHARE`,
        [[facts.source.id, facts.target.id]],
      );
      const currentVersions = new Map(versions.rows.map((row) => [row.id, row.row_version]));
      const stale =
        bound.rows[0] === undefined ||
        currentVersions.get(facts.source.id) !== facts.source.rowVersion ||
        currentVersions.get(facts.target.id) !== facts.target.rowVersion;
      if (stale) {
        await client.query(
          `UPDATE ape_dts_platform.preflight_runs
              SET state = 'STALE', completed_at = statement_timestamp(), result = $2::jsonb,
                  failure_code = 'PREFLIGHT_STALE',
                  failure_summary = '任务、旧运行或数据源在系统预检期间发生变化，结果不能用于自动恢复。'
            WHERE id = $1::uuid AND task_revision_id = $3::uuid AND state = 'RUNNING'`,
          [runId, JSON.stringify(evaluation.result), request.revisionId],
        );
        await this.audit.append(client, {
          action: 'TASK_PREFLIGHT_STALE',
          actorType: 'SYSTEM',
          actorUserId: null,
          details: {
            oldRunId: request.expectedOldRunId,
            revisionId: request.revisionId,
            taskId: request.taskId,
          },
          subjectId: request.taskId,
          subjectType: 'TASK',
        });
        return;
      }
      let finalizedEvaluation = evaluation;
      if (
        evaluation.continuityObservation !== null &&
        evaluation.result.checks
          .filter((item) => item.key === 'TOPIC_IDENTITIES')
          .every((item) => item.state === 'PASSED')
      ) {
        const continuityCheck = await this.continuity.decideWithin(
          client,
          { type: 'SYSTEM' },
          request.taskId,
          request.revisionId,
          request.expectedTaskRowVersion,
          facts.task.desired_state,
          evaluation.continuityObservation,
        );
        const checks = evaluation.result.checks.map((item) =>
          item.key === 'TOPIC_IDENTITIES'
            ? { ...continuityCheck, details: { ...item.details, ...continuityCheck.details } }
            : item,
        );
        const blocking = checks.filter((item) => item.state !== 'SKIPPED');
        finalizedEvaluation = {
          ...evaluation,
          passed: blocking.length > 0 && blocking.every((item) => item.state === 'PASSED'),
          result: resultWith(evaluation.result, { checks }),
        };
      }
      const failed = finalizedEvaluation.result.checks.find((item) => item.state === 'FAILED');
      await client.query(
        `UPDATE ape_dts_platform.preflight_runs
            SET state = $2, completed_at = statement_timestamp(), result = $3::jsonb,
                failure_code = $4, failure_summary = $5
          WHERE id = $1::uuid AND task_revision_id = $6::uuid AND state = 'RUNNING'`,
        [
          runId,
          finalizedEvaluation.passed ? 'PASSED' : 'FAILED',
          JSON.stringify(finalizedEvaluation.result),
          finalizedEvaluation.passed ? null : (failed?.code ?? 'PREFLIGHT_FAILED'),
          finalizedEvaluation.passed ? null : (failed?.summary ?? '预检未通过。'),
          request.revisionId,
        ],
      );
      await this.audit.append(client, {
        action: finalizedEvaluation.passed ? 'TASK_PREFLIGHT_PASSED' : 'TASK_PREFLIGHT_FAILED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: {
          checkCount: finalizedEvaluation.result.checks.length,
          oldRunId: request.expectedOldRunId,
          revisionId: request.revisionId,
          taskId: request.taskId,
        },
        subjectId: request.taskId,
        subjectType: 'TASK',
      });
    });
    return await this.loadRun(runId);
  }

  /** Called only by an explicit START request, never by reads, saves or preflight. */
  async prepareFirstStartTopic(
    actor: UserSummary,
    taskId: string,
    request: RunTaskPreflightRequest,
  ): Promise<boolean> {
    const completed: CreateKafkaTopicResponse[] = [];
    const prepared = await this.transaction(async (client) => {
      // Lock the exact task while checking its revision and performing the external
      // mutation. A concurrent draft edit/start cannot change the selected target.
      const locked = await client.query<TaskFact & { observed_state: string }>(
        `SELECT ${TASK_COLUMNS}, observed_state FROM ape_dts_platform.tasks
          WHERE id = $1::uuid FOR UPDATE`,
        [taskId],
      );
      const task = locked.rows[0];
      if (task === undefined || task.deleted_at !== null)
        throw new PreflightServiceError('PREFLIGHT_TASK_NOT_FOUND');
      if (task.row_version !== request.expectedTaskRowVersion)
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      if (task.current_draft_revision_id !== request.revisionId)
        throw new PreflightServiceError('PREFLIGHT_REVISION_NOT_CURRENT');
      const facts = await this.loadFacts(actor, taskId, request, client);
      const config = platformTaskConfigSchema.parse(facts.revision.configuration);
      const expansion = await confirmedPgTableExpansion(client, taskId, request.revisionId);
      const targets = taskTargetTopics(config).filter((item) => item.topicCreation !== undefined);
      if (targets.length === 0) return false;
      const latest = await client.query<PreflightRunRow>(
        `SELECT state, result, completed_at FROM ape_dts_platform.preflight_runs
          WHERE task_revision_id = $1::uuid ORDER BY started_at DESC, id DESC LIMIT 1`,
        [request.revisionId],
      );
      const pending = latest.rows[0];
      if (pending?.state !== 'FAILED' || pending.completed_at === null) return false;
      const result = taskPreflightResultSchema.parse(pending.result);
      const failures = result.checks.filter((item) => item.state === 'FAILED');
      if (failures.length !== 1 || failures[0]?.code !== 'TARGET_TOPIC_CREATION_PENDING')
        return false;
      if (actor.role !== 'ADMIN' || actor.status !== 'ACTIVE')
        throw new PreflightServiceError('PREFLIGHT_ACCESS_DENIED');
      if (
        task.desired_state !== 'STOPPED' ||
        task.observed_state !== 'STOPPED' ||
        (task.current_applied_revision_id !== null && !expansion) ||
        result.taskRevision.taskRowVersion !== task.row_version ||
        result.taskRevision.draftVersion !== facts.revision.draft_version ||
        result.dataSourceVersions.source !== facts.source.rowVersion ||
        result.dataSourceVersions.target !== facts.target.rowVersion
      )
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      const history = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
           SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id = $1::uuid
           UNION ALL
           SELECT 1 FROM ape_dts_platform.task_continuity_baselines WHERE task_id = $1::uuid
         ) AS exists`,
        [taskId],
      );
      if (history.rows[0]?.exists !== false && !expansion)
        throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
      for (const target of targets) {
        completed.push(
          await this.dataSources.createKafkaTopic(
            actor,
            facts.target.id,
            {
              topic: target.topic,
              ...target.topicCreation!,
              expectedRowVersion: facts.target.rowVersion,
            },
            client,
          ),
        );
      }
      return true;
    }).catch((error: unknown) => {
      // Kafka cannot participate in the database transaction. A failed outer
      // COMMIT must not erase the known successful external result.
      if (error instanceof KafkaTopicAuditError) {
        if (completed.length === 0) throw error;
        throw new KafkaTopicAuditError(error.result, { cause: error }, [
          ...completed,
          ...error.completedResults,
        ]);
      }
      const last = completed.at(-1);
      if (last !== undefined) throw new KafkaTopicAuditError(last, { cause: error }, completed);
      throw error;
    });
    if (prepared) await this.run(actor, taskId, request);
    return prepared;
  }

  async run(
    actor: UserSummary,
    taskId: string,
    request: RunTaskPreflightRequest,
  ): Promise<TaskPreflightRunSummary> {
    const facts = await this.loadFacts(actor, taskId, request);
    const runId = randomUUID();
    const initialResult: TaskPreflightResult = {
      capacity: { available: null, limit: null, requested: null, used: null },
      checks: [],
      dataSourceVersions: {
        source: facts.source.rowVersion,
        target: facts.target.rowVersion,
      },
      identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
      taskRevision: {
        draftVersion: facts.revision.draft_version,
        taskRowVersion: facts.task.row_version,
      },
    };
    const preflightActor: PreflightActor = {
      type: 'USER',
      user: actor,
      ...(request.reinitializeMysqlSnapshot === true ? { reinitializeMysqlSnapshot: true } : {}),
    };
    await this.startRun(preflightActor, taskId, request.revisionId, runId, initialResult);
    try {
      const evaluation = await this.evaluate(preflightActor, facts);
      return await this.finalize(actor, facts, request, runId, evaluation);
    } catch {
      const result: TaskPreflightResult = {
        ...initialResult,
        checks: [
          check(
            'CONFIGURATION',
            'FAILED',
            'PREFLIGHT_INTERNAL_ERROR',
            '预检执行失败，未能形成可信结果。',
          ),
        ],
      };
      const evaluation: Evaluation = {
        continuityObservation: null,
        passed: false,
        primaryKey: null,
        result,
        topicObservations: [],
      };
      return await this.finalize(actor, facts, request, runId, evaluation);
    }
  }

  async runForSystemRecovery(
    request: RunSystemRecoveryPreflightRequest,
  ): Promise<TaskPreflightRunSummary> {
    const facts = await this.loadSystemFacts(request);
    const runId = randomUUID();
    const initialResult: TaskPreflightResult = {
      capacity: { available: null, limit: null, requested: null, used: null },
      checks: [],
      dataSourceVersions: { source: facts.source.rowVersion, target: facts.target.rowVersion },
      identities: { source: null, sourceTopic: null, target: null, targetTopic: null },
      taskRevision: {
        draftVersion: facts.revision.draft_version,
        taskRowVersion: facts.task.row_version,
      },
    };
    await this.startSystemRun(request, runId, initialResult);
    try {
      return await this.finalizeSystem(
        facts,
        request,
        runId,
        await this.evaluate(
          {
            oldRunId: request.expectedOldRunId,
            type: 'SYSTEM',
            ...(request.reserveFreshCapacity ? { reserveFreshCapacity: true } : {}),
          },
          facts,
        ),
      );
    } catch {
      const result: TaskPreflightResult = {
        ...initialResult,
        checks: [
          check(
            'CONFIGURATION',
            'FAILED',
            'PREFLIGHT_INTERNAL_ERROR',
            '预检执行失败，未能形成可信结果。',
          ),
        ],
      };
      return await this.finalizeSystem(facts, request, runId, {
        continuityObservation: null,
        passed: false,
        primaryKey: null,
        result,
        topicObservations: [],
      });
    }
  }
}
