import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import {
  platformTaskConfigSchema,
  pgTableAddition,
  type KafkaRawForwardTaskConfig,
  type PgToKafkaTaskConfig,
  type PlatformTaskConfig,
} from '@scs/config-model';
import type {
  AssignTaskGroupRequest,
  CreateTaskGroupRequest,
  CreateTaskRequest,
  DeleteTaskGroupRequest,
  RenameTaskGroupRequest,
  ReorderTaskGroupsRequest,
  SaveTaskDraftRequest,
  TaskRevisionState,
  TaskGroupSummary,
  TaskSummary,
  TaskType,
  UserSummary,
} from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';

export type TasksServiceErrorCode =
  | 'TASK_ACCESS_DENIED'
  | 'TASK_ARTIFACT_NOT_AVAILABLE'
  | 'TASK_ACTIVE_RUN_UNITS'
  | 'TASK_CONFIGURATION_DATA_SOURCE_MISMATCH'
  | 'TASK_CONFIGURATION_INVALID'
  | 'TASK_DATA_SOURCE_NOT_FOUND'
  | 'TASK_DATA_SOURCE_TYPE_INVALID'
  | 'TASK_DRAFT_VERSION_CONFLICT'
  | 'TASK_GROUP_NOT_FOUND'
  | 'TASK_GROUP_NAME_ALREADY_EXISTS'
  | 'TASK_GROUP_ASSIGNMENT_CONFLICT'
  | 'TASK_GROUP_CHANGED'
  | 'TASK_GROUP_NOT_EMPTY'
  | 'TASK_NAME_ALREADY_EXISTS'
  | 'TASK_NOT_FOUND'
  | 'TASK_NOT_STOPPED'
  | 'TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED'
  | 'TASK_PG_TABLE_ADDITION_NOT_ALLOWED'
  | 'TASK_REVISION_IMMUTABLE'
  | 'TASK_TYPE_CONFIGURATION_MISMATCH'
  | 'TASK_TYPE_IMMUTABLE'
  | 'TASK_VERSION_CONFLICT';

export class TasksServiceError extends Error {
  constructor(readonly code: TasksServiceErrorCode) {
    super(code);
    this.name = 'TasksServiceError';
  }
}

interface TaskRow extends QueryResultRow {
  created_at: Date;
  created_by_user_id: string;
  current_applied_revision_id: string | null;
  current_draft_revision_id: string | null;
  deleted_at: Date | null;
  desired_state: TaskSummary['desiredState'];
  display_name: string;
  group_id: string | null;
  id: string;
  manual_gate_reason: string | null;
  normalized_name: string;
  observability_health: TaskSummary['observabilityHealth'];
  observed_state: TaskSummary['observedState'];
  recovery_disposition: TaskSummary['recoveryDisposition'];
  row_version: string;
  source_data_source_id: string;
  target_data_source_id: string;
  task_type: TaskType;
  updated_at: Date;
  updated_by_user_id: string;
}

interface RevisionRow extends QueryResultRow {
  artifact_id: string | null;
  configuration: unknown;
  configuration_schema_version: number | null;
  created_at: Date;
  draft_version: string;
  id: string;
  revision_number: string;
  state: TaskRevisionState;
  task_id: string;
  updated_at: Date;
}

const TASK_COLUMNS = `
  (SELECT group_id FROM ape_dts_platform.task_group_memberships WHERE task_id = tasks.id) AS group_id,
  id,
  normalized_name,
  display_name,
  task_type,
  source_data_source_id,
  target_data_source_id,
  desired_state,
  observed_state,
  observability_health,
  recovery_disposition,
  current_draft_revision_id,
  current_applied_revision_id,
  manual_gate_reason,
  row_version::text,
  created_by_user_id,
  created_at,
  updated_by_user_id,
  updated_at,
  deleted_at`;

const REVISION_COLUMNS = `
  id,
  task_id,
  revision_number::text,
  state,
  configuration_schema_version,
  configuration,
  artifact_id,
  draft_version::text,
  created_at,
  updated_at`;

const ACTIVE_DRAFT_STATES: readonly TaskRevisionState[] = [
  'INCOMPLETE_DRAFT',
  'READY_DRAFT',
  'PREFLIGHT_PASSED',
  'RISK_CONFIRMED',
];

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

function normalizedTaskName(name: string): string {
  return name.normalize('NFKC').trim().toLocaleLowerCase('en-US');
}

function objectConfiguration(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TasksServiceError('TASK_CONFIGURATION_INVALID');
  }
  return value as Record<string, unknown>;
}

function configKind(taskType: TaskType): PlatformTaskConfig['kind'] {
  return taskType === 'PG_KAFKA'
    ? 'PG_TO_KAFKA'
    : taskType === 'MYSQL_KAFKA'
      ? 'MYSQL_TO_KAFKA'
      : 'KAFKA_RAW_FORWARD';
}

function schemaVersion(value: Record<string, unknown>): number | null {
  const candidate = value.schemaVersion;
  return typeof candidate === 'number' && Number.isSafeInteger(candidate) && candidate > 0
    ? candidate
    : null;
}

interface DraftEvaluation {
  readonly configuration: Record<string, unknown>;
  readonly parsed: PlatformTaskConfig | null;
  readonly state: Extract<TaskRevisionState, 'INCOMPLETE_DRAFT' | 'READY_DRAFT'>;
}

function declaredDataSourceIds(configuration: Record<string, unknown>): {
  readonly source: string | undefined;
  readonly target: string | undefined;
} {
  const source = configuration.source;
  const target = configuration.target;
  const sourceId =
    typeof source === 'object' && source !== null && !Array.isArray(source)
      ? (source as Record<string, unknown>).dataSourceId
      : undefined;
  const targetId =
    typeof target === 'object' && target !== null && !Array.isArray(target)
      ? (target as Record<string, unknown>).dataSourceId
      : undefined;
  return {
    source: typeof sourceId === 'string' ? sourceId : undefined,
    target: typeof targetId === 'string' ? targetId : undefined,
  };
}

function evaluateDraft(
  taskId: string,
  taskType: TaskType,
  rawConfiguration: Record<string, unknown>,
): DraftEvaluation {
  const rawKind = rawConfiguration.kind;
  const rawSource = rawConfiguration.source;
  if (typeof rawKind === 'string' && rawKind !== configKind(taskType)) {
    throw new TasksServiceError('TASK_TYPE_CONFIGURATION_MISMATCH');
  }
  if (
    rawKind === 'PG_TO_KAFKA' &&
    typeof rawSource === 'object' &&
    rawSource !== null &&
    !Array.isArray(rawSource) &&
    (rawSource as Record<string, unknown>).resourceMode === 'APE_DTS_MANAGED' &&
    (rawSource as Record<string, unknown>).includeSnapshot === true
  ) {
    throw new TasksServiceError('TASK_CONFIGURATION_INVALID');
  }
  const configuration =
    taskType === 'KAFKA_KAFKA' &&
    rawConfiguration.kind === 'KAFKA_RAW_FORWARD' &&
    typeof rawConfiguration.source === 'object' &&
    rawConfiguration.source !== null &&
    !Array.isArray(rawConfiguration.source)
      ? {
          ...rawConfiguration,
          source: {
            ...(rawConfiguration.source as Record<string, unknown>),
            groupId: `ape-dts-task-${taskId}`,
          },
        }
      : rawConfiguration;
  const parsed = platformTaskConfigSchema.safeParse(configuration);
  if (!parsed.success) {
    return {
      configuration,
      parsed: null,
      state: 'INCOMPLETE_DRAFT',
    };
  }
  if (parsed.data.kind !== configKind(taskType)) {
    throw new TasksServiceError('TASK_TYPE_CONFIGURATION_MISMATCH');
  }
  return {
    configuration: parsed.data as unknown as Record<string, unknown>,
    parsed: parsed.data,
    state: 'READY_DRAFT',
  };
}

function revisionSummary(row: RevisionRow): NonNullable<TaskSummary['draft']> {
  return {
    artifactId: row.artifact_id,
    configuration: objectConfiguration(row.configuration),
    configurationSchemaVersion: row.configuration_schema_version,
    createdAt: row.created_at.toISOString(),
    draftVersion: row.draft_version,
    id: row.id,
    revisionNumber: row.revision_number,
    state: row.state,
    updatedAt: row.updated_at.toISOString(),
  };
}

function taskSummary(
  row: TaskRow,
  draft: RevisionRow | undefined,
  applied: RevisionRow | undefined,
): TaskSummary {
  return {
    createdAt: row.created_at.toISOString(),
    currentAppliedRevisionId: row.current_applied_revision_id,
    deletedAt: row.deleted_at?.toISOString() ?? null,
    desiredState: row.desired_state,
    draft: draft === undefined ? null : revisionSummary(draft),
    applied: applied === undefined ? null : revisionSummary(applied),
    groupId: row.group_id,
    id: row.id,
    name: row.display_name,
    observabilityHealth: row.observability_health,
    observedState: row.observed_state,
    recoveryDisposition: row.recovery_disposition,
    rowVersion: row.row_version,
    sourceDataSourceId: row.source_data_source_id,
    targetDataSourceId: row.target_data_source_id,
    taskType: row.task_type,
    updatedAt: row.updated_at.toISOString(),
  };
}

@Injectable()
export class TasksService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
  ) {}

  async listGroups(): Promise<readonly TaskGroupSummary[]> {
    const result = await this.database
      .requirePool()
      .query<TaskGroupSummary & QueryResultRow>(
        `SELECT id, display_name AS name FROM ape_dts_platform.task_groups ORDER BY sort_order, normalized_name, id`,
      );
    return result.rows;
  }

  async createGroup(
    actor: UserSummary,
    request: CreateTaskGroupRequest,
    transactionClient?: PoolClient,
  ): Promise<TaskGroupSummary> {
    const name = request.name.normalize('NFKC').trim();
    try {
      return await this.transaction(async (client) => {
        await this.lockGroupMutationsWithin(client);
        const result = await client.query<TaskGroupSummary & QueryResultRow>(
          `INSERT INTO ape_dts_platform.task_groups(id,display_name,normalized_name,created_by_user_id,sort_order)
           SELECT $1::uuid,$2,$3,$4::uuid,COALESCE(MAX(sort_order),0)+1 FROM ape_dts_platform.task_groups
           RETURNING id,display_name AS name`,
          [randomUUID(), name, normalizedTaskName(name), actor.id],
        );
        const group = result.rows[0]!;
        await this.audit.append(client, {
          action: 'TASK_GROUP_CREATED',
          actorType: 'USER',
          actorUserId: actor.id,
          details: { name },
          subjectId: group.id,
          subjectType: 'TASK_GROUP',
        });
        return group;
      }, transactionClient);
    } catch (error) {
      if (postgresErrorCode(error) === '23505')
        throw new TasksServiceError('TASK_GROUP_NAME_ALREADY_EXISTS');
      throw error;
    }
  }

  // Serialize folder mutations, including the full-set check used when reordering.
  async lockGroupMutationsWithin(client: PoolClient) {
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [1396921159, 1]);
  }

  async renameGroup(actor: UserSummary, groupId: string, request: RenameTaskGroupRequest) {
    try {
      return await this.transaction(async (client) => {
        await this.lockGroupMutationsWithin(client);
        const group = await this.lockGroup(client, groupId, request.expectedName);
        const name = request.name.normalize('NFKC').trim();
        await client.query(
          'UPDATE ape_dts_platform.task_groups SET display_name=$2,normalized_name=$3 WHERE id=$1::uuid',
          [groupId, name, normalizedTaskName(name)],
        );
        await this.audit.append(client, {
          action: 'TASK_GROUP_RENAMED',
          actorType: 'USER',
          actorUserId: actor.id,
          subjectType: 'TASK_GROUP',
          subjectId: groupId,
          details: { previousName: group.name, name },
        });
        return { id: groupId, name };
      });
    } catch (error) {
      if (postgresErrorCode(error) === '23505')
        throw new TasksServiceError('TASK_GROUP_NAME_ALREADY_EXISTS');
      throw error;
    }
  }

  private async lockGroup(client: PoolClient, groupId: string, expectedName: string) {
    const group = (
      await client.query<TaskGroupSummary & QueryResultRow>(
        'SELECT id,display_name AS name FROM ape_dts_platform.task_groups WHERE id=$1::uuid FOR UPDATE',
        [groupId],
      )
    ).rows[0];
    if (!group) throw new TasksServiceError('TASK_GROUP_NOT_FOUND');
    if (group.name !== expectedName) throw new TasksServiceError('TASK_GROUP_CHANGED');
    return group;
  }

  async deleteGroup(actor: UserSummary, groupId: string, request: DeleteTaskGroupRequest) {
    return this.transaction(async (client) => {
      await this.lockGroupMutationsWithin(client);
      const group = await this.lockGroup(client, groupId, request.expectedName);
      // The group lock blocks new memberships. Never delete tasks or alter runtime/configuration.
      const occupied = await client.query(
        `SELECT 1 FROM ape_dts_platform.task_group_memberships m
         JOIN ape_dts_platform.tasks t ON t.id=m.task_id
         WHERE m.group_id=$1::uuid AND t.deleted_at IS NULL LIMIT 1`,
        [groupId],
      );
      if (occupied.rows.length && !request.detachTasks)
        throw new TasksServiceError('TASK_GROUP_NOT_EMPTY');
      await client.query(
        'DELETE FROM ape_dts_platform.task_group_memberships WHERE group_id=$1::uuid',
        [groupId],
      );
      await client.query('DELETE FROM ape_dts_platform.task_groups WHERE id=$1::uuid', [groupId]);
      await this.audit.append(client, {
        action: 'TASK_GROUP_DELETED',
        actorType: 'USER',
        actorUserId: actor.id,
        subjectType: 'TASK_GROUP',
        subjectId: groupId,
        details: { name: group.name, detachTasks: request.detachTasks },
      });
      return { deleted: true as const };
    });
  }

  async reorderGroups(actor: UserSummary, request: ReorderTaskGroupsRequest) {
    return this.transaction(async (client) => {
      await this.lockGroupMutationsWithin(client);
      const groups = (
        await client.query<TaskGroupSummary & QueryResultRow>(
          'SELECT id,display_name AS name FROM ape_dts_platform.task_groups ORDER BY sort_order,normalized_name,id',
        )
      ).rows;
      if (
        JSON.stringify(groups.map((group) => group.id)) !==
          JSON.stringify(request.expectedGroupIds) ||
        request.groupIds.length !== groups.length ||
        new Set(request.groupIds).size !== groups.length ||
        request.groupIds.some((id) => !groups.some((group) => group.id === id))
      )
        throw new TasksServiceError('TASK_GROUP_CHANGED');
      await client.query(
        `UPDATE ape_dts_platform.task_groups g SET sort_order=ordered.position
         FROM unnest($1::uuid[]) WITH ORDINALITY AS ordered(id,position) WHERE g.id=ordered.id`,
        [request.groupIds],
      );
      await this.audit.append(client, {
        action: 'TASK_GROUPS_REORDERED',
        actorType: 'USER',
        actorUserId: actor.id,
        subjectType: 'TASK_GROUP',
        subjectId: null,
        details: { groupIds: request.groupIds },
      });
      return { groups: request.groupIds.map((id) => groups.find((group) => group.id === id)!) };
    });
  }

  private async requireGroup(
    client: PoolClient,
    groupId: string | null | undefined,
  ): Promise<void> {
    if (groupId == null) return;
    const result = await client.query(
      `SELECT id FROM ape_dts_platform.task_groups WHERE id=$1::uuid FOR SHARE`,
      [groupId],
    );
    if (result.rows.length === 0) throw new TasksServiceError('TASK_GROUP_NOT_FOUND');
  }

  async assignGroup(
    actor: UserSummary,
    taskId: string,
    request: AssignTaskGroupRequest,
  ): Promise<TaskSummary> {
    return await this.transaction(async (client) => {
      const row = await this.requireTask(client, taskId, true);
      if (row.deleted_at !== null) throw new TasksServiceError('TASK_NOT_FOUND');
      await this.requireTaskDataSourceAccess(
        client,
        actor,
        row.source_data_source_id,
        row.target_data_source_id,
        row.task_type,
      );
      // The task row serializes assignment changes without invalidating its runtime/preflight version.
      const membership = await client.query<{ group_id: string }>(
        `SELECT group_id FROM ape_dts_platform.task_group_memberships WHERE task_id=$1::uuid`,
        [taskId],
      );
      const previousGroupId = membership.rows[0]?.group_id ?? null;
      if (previousGroupId !== request.expectedGroupId)
        throw new TasksServiceError('TASK_GROUP_ASSIGNMENT_CONFLICT');
      await this.requireGroup(client, request.groupId);
      if (previousGroupId !== request.groupId) {
        if (request.groupId === null) {
          await client.query(
            `DELETE FROM ape_dts_platform.task_group_memberships WHERE task_id=$1::uuid`,
            [taskId],
          );
        } else {
          await client.query(
            `INSERT INTO ape_dts_platform.task_group_memberships(task_id,group_id) VALUES($1::uuid,$2::uuid)
            ON CONFLICT(task_id) DO UPDATE SET group_id=EXCLUDED.group_id`,
            [taskId, request.groupId],
          );
        }
        await this.audit.append(client, {
          action: 'TASK_GROUP_ASSIGNED',
          actorType: 'USER',
          actorUserId: actor.id,
          details: { previousGroupId, groupId: request.groupId },
          subjectId: taskId,
          subjectType: 'TASK',
        });
      }
      return await this.loadSummary(client, { ...row, group_id: request.groupId });
    });
  }

  private async requireArtifact(
    client: PoolClient,
    _actor: UserSummary,
    artifactId: string | null | undefined,
  ): Promise<void> {
    if (artifactId == null) return;
    const result = await client.query(
      `SELECT id FROM ape_dts_platform.artifacts
        WHERE id = $1::uuid AND deleted_at IS NULL AND state <> 'QUARANTINED' FOR SHARE`,
      [artifactId],
    );
    if (result.rows.length === 0) throw new TasksServiceError('TASK_ARTIFACT_NOT_AVAILABLE');
  }

  private async transaction<T>(
    work: (client: PoolClient) => Promise<T>,
    transactionClient?: PoolClient,
  ): Promise<T> {
    if (transactionClient) return work(transactionClient);
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

  private async requireTask(
    client: PoolClient,
    taskId: string,
    forUpdate = false,
  ): Promise<TaskRow> {
    const result = await client.query<TaskRow>(
      `SELECT ${TASK_COLUMNS}
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid${forUpdate ? ' FOR UPDATE' : ''}`,
      [taskId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new TasksServiceError('TASK_NOT_FOUND');
    }
    return row;
  }

  private async requireDraft(
    client: PoolClient,
    taskId: string,
    revisionId: string,
  ): Promise<RevisionRow> {
    const result = await client.query<RevisionRow>(
      `SELECT ${REVISION_COLUMNS}
         FROM ape_dts_platform.task_revisions
        WHERE id = $1::uuid
          AND task_id = $2::uuid
        FOR UPDATE`,
      [revisionId, taskId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new TasksServiceError('TASK_NOT_FOUND');
    }
    return row;
  }

  private async requireTaskDataSourceAccess(
    client: PoolClient,
    actor: UserSummary,
    sourceDataSourceId: string,
    targetDataSourceId: string,
    taskType?: TaskType,
  ): Promise<void> {
    const ids = [...new Set([sourceDataSourceId, targetDataSourceId])];
    const active = await client.query<{ id: string; source_type: string }>(
      `SELECT id, source_type
         FROM ape_dts_platform.data_sources
        WHERE id = ANY($1::uuid[])
          AND state = 'ACTIVE'
          AND deleted_at IS NULL`,
      [ids],
    );
    if (active.rows.length !== ids.length) {
      throw new TasksServiceError('TASK_DATA_SOURCE_NOT_FOUND');
    }
    if (taskType !== undefined) {
      const byId = new Map(active.rows.map((row) => [row.id, row.source_type]));
      const expectedSourceType =
        taskType === 'PG_KAFKA' ? 'POSTGRES' : taskType === 'MYSQL_KAFKA' ? 'MYSQL' : 'KAFKA';
      if (
        byId.get(sourceDataSourceId) !== expectedSourceType ||
        byId.get(targetDataSourceId) !== 'KAFKA'
      ) {
        throw new TasksServiceError('TASK_DATA_SOURCE_TYPE_INVALID');
      }
    }
    if (actor.role === 'ADMIN') {
      return;
    }
    const grants = await client.query<{ count: string }>(
      `SELECT count(DISTINCT data_source_id)::text AS count
         FROM ape_dts_platform.data_source_grants
        WHERE user_id = $1::uuid
          AND data_source_id = ANY($2::uuid[])
          AND revoked_at IS NULL`,
      [actor.id, ids],
    );
    if (Number(grants.rows[0]?.count ?? '0') !== ids.length) {
      throw new TasksServiceError('TASK_ACCESS_DENIED');
    }
  }

  private async sourceDatabaseName(client: PoolClient, dataSourceId: string): Promise<string> {
    const result = await client.query<{ database: string | null }>(
      `SELECT configuration ->> 'database' AS database
         FROM ape_dts_platform.data_sources
        WHERE id = $1::uuid
          AND source_type IN ('POSTGRES', 'MYSQL')
          AND state = 'ACTIVE'
          AND deleted_at IS NULL`,
      [dataSourceId],
    );
    const database = result.rows[0]?.database;
    if (database === undefined || database === null || database.trim().length === 0) {
      throw new TasksServiceError('TASK_DATA_SOURCE_NOT_FOUND');
    }
    return database;
  }

  private async loadSummary(client: PoolClient, row: TaskRow): Promise<TaskSummary> {
    const revisionIds = [row.current_draft_revision_id, row.current_applied_revision_id].filter(
      (id) => id !== null,
    );
    const revisions =
      revisionIds.length === 0
        ? []
        : (
            await client.query<RevisionRow>(
              `SELECT ${REVISION_COLUMNS}
           FROM ape_dts_platform.task_revisions
          WHERE id = ANY($1::uuid[])
            AND task_id = $2::uuid`,
              [revisionIds, row.id],
            )
          ).rows;
    return taskSummary(
      row,
      revisions.find((revision) => revision.id === row.current_draft_revision_id),
      revisions.find((revision) => revision.id === row.current_applied_revision_id),
    );
  }

  private async upsertTypedConfiguration(
    client: PoolClient,
    revisionId: string,
    parsed: PlatformTaskConfig | null,
    sourceDataSourceId: string,
  ): Promise<void> {
    await client.query(
      `DELETE FROM ape_dts_platform.pg_kafka_revision_configs
        WHERE task_revision_id = $1::uuid`,
      [revisionId],
    );
    await client.query(
      `DELETE FROM ape_dts_platform.kafka_kafka_revision_configs
        WHERE task_revision_id = $1::uuid`,
      [revisionId],
    );
    await client.query(
      'DELETE FROM ape_dts_platform.mysql_kafka_revision_configs WHERE task_revision_id = $1::uuid',
      [revisionId],
    );
    if (parsed?.kind === 'MYSQL_TO_KAFKA') {
      const database = await this.sourceDatabaseName(client, sourceDataSourceId);
      if (parsed.mappings.some((mapping) => mapping.source.schema !== database))
        throw new TasksServiceError('TASK_CONFIGURATION_DATA_SOURCE_MISMATCH');
      await client.query(
        `INSERT INTO ape_dts_platform.mysql_kafka_revision_configs
          (task_revision_id, source_database, include_snapshot, mappings)
          VALUES ($1::uuid, $2, $3, $4::jsonb)`,
        [revisionId, database, parsed.source.includeSnapshot, JSON.stringify(parsed.mappings)],
      );
    }
    if (parsed?.kind === 'PG_TO_KAFKA') {
      const pg = parsed as PgToKafkaTaskConfig;
      await client.query(
        `INSERT INTO ape_dts_platform.pg_kafka_revision_configs (
           task_revision_id, source_database, source_schema, source_table,
           target_topic, resource_mode, slot_name, publication_name, include_snapshot, mappings
         ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)`,
        [
          revisionId,
          await this.sourceDatabaseName(client, sourceDataSourceId),
          pg.schemaVersion === 1 ? pg.source.table.schema : null,
          pg.schemaVersion === 1 ? pg.source.table.name : null,
          pg.schemaVersion === 1 ? pg.target.topic : null,
          pg.source.resourceMode,
          pg.source.slotName,
          pg.source.publicationName,
          pg.source.includeSnapshot,
          pg.schemaVersion === 2 ? JSON.stringify(pg.mappings) : null,
        ],
      );
    }
    if (parsed?.kind === 'KAFKA_RAW_FORWARD') {
      const kafka = parsed as KafkaRawForwardTaskConfig;
      await client.query(
        `INSERT INTO ape_dts_platform.kafka_kafka_revision_configs (
           task_revision_id, source_topic, target_topic, group_id, required_acks, mappings
         ) VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb)`,
        [
          revisionId,
          kafka.schemaVersion === 1 ? kafka.source.topic : null,
          kafka.schemaVersion === 1 ? kafka.target.topic : null,
          kafka.source.groupId,
          kafka.tuning.requiredAcks,
          kafka.schemaVersion === 2 ? JSON.stringify(kafka.mappings) : null,
        ],
      );
    }
  }

  private assertConfigurationPolicy(
    actor: UserSummary,
    rawConfiguration: Record<string, unknown>,
    parsed: PlatformTaskConfig | null,
  ): void {
    const source = rawConfiguration.source;
    const resourceMode =
      typeof source === 'object' && source !== null && !Array.isArray(source)
        ? (source as Record<string, unknown>).resourceMode
        : undefined;
    if (
      actor.role !== 'ADMIN' &&
      ((parsed?.kind === 'PG_TO_KAFKA' && parsed.source.resourceMode === 'APE_DTS_MANAGED') ||
        (rawConfiguration.kind === 'PG_TO_KAFKA' && resourceMode === 'APE_DTS_MANAGED'))
    ) {
      throw new TasksServiceError('TASK_ACCESS_DENIED');
    }
  }

  async create(
    actor: UserSummary,
    request: CreateTaskRequest,
    transactionClient?: PoolClient,
  ): Promise<TaskSummary> {
    const taskId = randomUUID();
    const revisionId = randomUUID();
    const configuration = objectConfiguration(request.configuration);
    const declaredIds = declaredDataSourceIds(configuration);
    if (
      (declaredIds.source !== undefined && declaredIds.source !== request.sourceDataSourceId) ||
      (declaredIds.target !== undefined && declaredIds.target !== request.targetDataSourceId)
    ) {
      throw new TasksServiceError('TASK_CONFIGURATION_DATA_SOURCE_MISMATCH');
    }
    const evaluation = evaluateDraft(taskId, request.taskType, configuration);
    this.assertConfigurationPolicy(actor, configuration, evaluation.parsed);
    if (evaluation.parsed !== null) {
      const parsed = evaluation.parsed;
      const sourceId = parsed.source.dataSourceId;
      const targetId = parsed.target.dataSourceId;
      if (sourceId !== request.sourceDataSourceId || targetId !== request.targetDataSourceId) {
        throw new TasksServiceError('TASK_CONFIGURATION_DATA_SOURCE_MISMATCH');
      }
    }
    try {
      return await this.transaction(async (client) => {
        await this.requireTaskDataSourceAccess(
          client,
          actor,
          request.sourceDataSourceId,
          request.targetDataSourceId,
          request.taskType,
        );
        await this.requireArtifact(client, actor, request.artifactId);
        await this.requireGroup(client, request.groupId);
        const insertedTask = await client.query<TaskRow>(
          `INSERT INTO ape_dts_platform.tasks (
             id, normalized_name, display_name, task_type,
             source_data_source_id, target_data_source_id,
             desired_state, observed_state, observability_health, recovery_disposition,
             current_draft_revision_id, created_by_user_id, updated_by_user_id
           ) VALUES (
             $1::uuid, $2, $3, $4, $5::uuid, $6::uuid,
             'STOPPED', 'STOPPED', 'UNAVAILABLE', 'NOT_APPLICABLE',
             NULL, $7::uuid, $7::uuid
           )
           RETURNING ${TASK_COLUMNS}`,
          [
            taskId,
            normalizedTaskName(request.name),
            request.name.trim(),
            request.taskType,
            request.sourceDataSourceId,
            request.targetDataSourceId,
            actor.id,
          ],
        );
        const task = insertedTask.rows[0];
        if (task === undefined) {
          throw new Error('TASK_INSERT_FAILED');
        }
        if (request.groupId != null) {
          await client.query(
            `INSERT INTO ape_dts_platform.task_group_memberships(task_id,group_id) VALUES($1::uuid,$2::uuid)`,
            [taskId, request.groupId],
          );
        }
        await client.query(
          `INSERT INTO ape_dts_platform.task_revisions (
             id, task_id, revision_number, state,
             configuration_schema_version, configuration, artifact_id,
             draft_version, row_version, created_by_user_id, updated_by_user_id
           ) VALUES (
             $1::uuid, $2::uuid, 1, $3,
             $4::integer, $5::jsonb, $7::uuid,
             1, 1, $6::uuid, $6::uuid
           )`,
          [
            revisionId,
            taskId,
            evaluation.state,
            evaluation.parsed?.schemaVersion ?? schemaVersion(evaluation.configuration),
            JSON.stringify(evaluation.configuration),
            actor.id,
            request.artifactId ?? null,
          ],
        );
        await this.upsertTypedConfiguration(
          client,
          revisionId,
          evaluation.parsed,
          request.sourceDataSourceId,
        );
        const linked = await client.query<TaskRow>(
          `UPDATE ape_dts_platform.tasks
              SET current_draft_revision_id = $2::uuid
            WHERE id = $1::uuid
              AND deleted_at IS NULL
              AND row_version = 1
            RETURNING ${TASK_COLUMNS}`,
          [taskId, revisionId],
        );
        const linkedTask = linked.rows[0];
        if (linkedTask === undefined) {
          throw new TasksServiceError('TASK_VERSION_CONFLICT');
        }
        await this.audit.append(client, {
          action: 'TASK_CREATED',
          actorType: 'USER',
          actorUserId: actor.id,
          details: {
            state: evaluation.state,
            taskType: request.taskType,
            groupId: request.groupId ?? null,
          },
          subjectId: taskId,
          subjectType: 'TASK',
        });
        return await this.loadSummary(client, linkedTask);
      }, transactionClient);
    } catch (error) {
      if (postgresErrorCode(error) === '23505') {
        throw new TasksServiceError('TASK_NAME_ALREADY_EXISTS');
      }
      throw error;
    }
  }

  async get(actor: UserSummary, taskId: string): Promise<TaskSummary> {
    const client = await this.database.requirePool().connect();
    try {
      const row = await this.requireTask(client, taskId);
      await this.requireTaskDataSourceAccess(
        client,
        actor,
        row.source_data_source_id,
        row.target_data_source_id,
        row.task_type,
      );
      return await this.loadSummary(client, row);
    } finally {
      client.release();
    }
  }

  async list(actor: UserSummary): Promise<readonly TaskSummary[]> {
    const client = await this.database.requirePool().connect();
    try {
      const result = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS}
           FROM ape_dts_platform.tasks
          WHERE deleted_at IS NULL
          ORDER BY display_name, id`,
      );
      if (result.rows.length === 0) return [];
      const ids = [
        ...new Set(
          result.rows.flatMap((row) => [row.source_data_source_id, row.target_data_source_id]),
        ),
      ];
      const sources = await client.query<{ id: string; source_type: string }>(
        `SELECT id, source_type FROM ape_dts_platform.data_sources
          WHERE id=ANY($1::uuid[]) AND state='ACTIVE' AND deleted_at IS NULL`,
        [ids],
      );
      const types = new Map(sources.rows.map((source) => [source.id, source.source_type]));
      const grants =
        actor.role === 'ADMIN'
          ? null
          : new Set(
              (
                await client.query<{ data_source_id: string }>(
                  `SELECT DISTINCT data_source_id FROM ape_dts_platform.data_source_grants
          WHERE user_id=$1::uuid AND data_source_id=ANY($2::uuid[]) AND revoked_at IS NULL`,
                  [actor.id, ids],
                )
              ).rows.map((row) => row.data_source_id),
            );
      const visible = result.rows.filter((row) => {
        if (!types.has(row.source_data_source_id) || !types.has(row.target_data_source_id))
          throw new TasksServiceError('TASK_DATA_SOURCE_NOT_FOUND');
        const expectedSourceType =
          row.task_type === 'PG_KAFKA'
            ? 'POSTGRES'
            : row.task_type === 'MYSQL_KAFKA'
              ? 'MYSQL'
              : 'KAFKA';
        if (
          types.get(row.source_data_source_id) !== expectedSourceType ||
          types.get(row.target_data_source_id) !== 'KAFKA'
        )
          throw new TasksServiceError('TASK_DATA_SOURCE_TYPE_INVALID');
        return (
          grants === null ||
          (grants.has(row.source_data_source_id) && grants.has(row.target_data_source_id))
        );
      });
      const revisionIds = [
        ...new Set(
          visible
            .flatMap((row) => [row.current_draft_revision_id, row.current_applied_revision_id])
            .filter((id) => id !== null),
        ),
      ];
      const revisions =
        revisionIds.length === 0
          ? []
          : (
              await client.query<RevisionRow>(
                `SELECT ${REVISION_COLUMNS} FROM ape_dts_platform.task_revisions
          WHERE id=ANY($1::uuid[]) AND task_id=ANY($2::uuid[])`,
                [revisionIds, visible.map((row) => row.id)],
              )
            ).rows;
      const byId = new Map(revisions.map((revision) => [revision.id, revision]));
      const boundRevision = (row: TaskRow, id: string | null) => {
        const revision = id === null ? undefined : byId.get(id);
        return revision?.task_id === row.id ? revision : undefined;
      };
      return visible.map((row) =>
        taskSummary(
          row,
          boundRevision(row, row.current_draft_revision_id),
          boundRevision(row, row.current_applied_revision_id),
        ),
      );
    } finally {
      client.release();
    }
  }

  async saveDraft(
    actor: UserSummary,
    taskId: string,
    request: SaveTaskDraftRequest,
  ): Promise<TaskSummary> {
    const configuration = objectConfiguration(request.configuration);
    return await this.transaction(async (client) => {
      const task = await this.requireTask(client, taskId, true);
      if (task.deleted_at !== null) {
        throw new TasksServiceError('TASK_NOT_FOUND');
      }
      await this.requireTaskDataSourceAccess(
        client,
        actor,
        task.source_data_source_id,
        task.target_data_source_id,
        task.task_type,
      );
      if (task.task_type !== request.taskType) {
        throw new TasksServiceError('TASK_TYPE_IMMUTABLE');
      }
      const revision = await this.requireDraft(client, taskId, request.revisionId);
      const forkApplied =
        task.current_draft_revision_id === null &&
        task.current_applied_revision_id === revision.id &&
        revision.state === 'APPLIED';
      if (!forkApplied && !ACTIVE_DRAFT_STATES.includes(revision.state)) {
        throw new TasksServiceError('TASK_REVISION_IMMUTABLE');
      }
      if (
        (!forkApplied && task.current_draft_revision_id !== revision.id) ||
        task.row_version !== request.expectedTaskRowVersion ||
        revision.draft_version !== request.expectedDraftVersion
      ) {
        throw new TasksServiceError('TASK_DRAFT_VERSION_CONFLICT');
      }
      const applied =
        task.current_applied_revision_id === null
          ? null
          : await this.requireDraft(client, taskId, task.current_applied_revision_id);
      const addition = pgTableAddition(applied?.configuration, configuration);
      if (addition.addedTables.length > 0) {
        if (
          !addition.eligible ||
          (configuration.source as Record<string, unknown>).includeSnapshot !== false
        )
          throw new TasksServiceError('TASK_PG_TABLE_ADDITION_NOT_ALLOWED');
        if (request.acknowledgePgTableAddition !== true)
          throw new TasksServiceError('TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED');
      }
      if (forkApplied || addition.addedTables.length > 0) {
        if (
          task.desired_state !== 'STOPPED' ||
          !['STOPPED', 'FAILED', 'BLOCKED'].includes(task.observed_state)
        )
          throw new TasksServiceError('TASK_NOT_STOPPED');
        const active = await client.query<{ exists: boolean }>(
          `SELECT EXISTS (
          SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
          UNION ALL SELECT 1 FROM ape_dts_platform.task_run_units WHERE task_id=$1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
        ) AS exists`,
          [taskId],
        );
        if (active.rows[0]?.exists !== false) throw new TasksServiceError('TASK_ACTIVE_RUN_UNITS');
      }
      const evaluation = evaluateDraft(taskId, request.taskType, configuration);
      this.assertConfigurationPolicy(actor, configuration, evaluation.parsed);
      const declaredIds = declaredDataSourceIds(configuration);
      if (
        (declaredIds.source !== undefined && declaredIds.source !== task.source_data_source_id) ||
        (declaredIds.target !== undefined && declaredIds.target !== task.target_data_source_id)
      ) {
        throw new TasksServiceError('TASK_CONFIGURATION_DATA_SOURCE_MISMATCH');
      }
      if (evaluation.parsed !== null) {
        if (
          evaluation.parsed.source.dataSourceId !== task.source_data_source_id ||
          evaluation.parsed.target.dataSourceId !== task.target_data_source_id
        ) {
          throw new TasksServiceError('TASK_CONFIGURATION_DATA_SOURCE_MISMATCH');
        }
      }
      const artifactId =
        request.artifactId === undefined ? revision.artifact_id : request.artifactId;
      await this.requireArtifact(client, actor, artifactId);
      const updatedRevision = forkApplied
        ? await client.query<RevisionRow>(
            `INSERT INTO ape_dts_platform.task_revisions
            (id,task_id,revision_number,state,configuration_schema_version,configuration,artifact_id,
             draft_version,row_version,created_by_user_id,updated_by_user_id)
           VALUES ($1::uuid,$2::uuid,(SELECT COALESCE(max(revision_number),0)+1 FROM ape_dts_platform.task_revisions WHERE task_id=$2::uuid),
             $3,$4::integer,$5::jsonb,$6::uuid,1,1,$7::uuid,$7::uuid)
           RETURNING ${REVISION_COLUMNS}`,
            [
              randomUUID(),
              taskId,
              evaluation.state,
              evaluation.parsed?.schemaVersion ?? schemaVersion(evaluation.configuration),
              JSON.stringify(evaluation.configuration),
              artifactId,
              actor.id,
            ],
          )
        : await client.query<RevisionRow>(
            `UPDATE ape_dts_platform.task_revisions
            SET state = $4,
                configuration_schema_version = $5::integer,
                configuration = $6::jsonb,
                artifact_id = $8::uuid,
                draft_version = draft_version + 1,
                row_version = row_version + 1,
                updated_by_user_id = $7::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND task_id = $2::uuid
            AND state IN ('INCOMPLETE_DRAFT', 'READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED')
            AND draft_version = $3::bigint
          RETURNING ${REVISION_COLUMNS}`,
            [
              revision.id,
              taskId,
              request.expectedDraftVersion,
              evaluation.state,
              evaluation.parsed?.schemaVersion ?? schemaVersion(evaluation.configuration),
              JSON.stringify(evaluation.configuration),
              actor.id,
              artifactId,
            ],
          );
      const updated = updatedRevision.rows[0];
      if (updated === undefined) {
        throw new TasksServiceError('TASK_DRAFT_VERSION_CONFLICT');
      }
      await this.upsertTypedConfiguration(
        client,
        updated.id,
        evaluation.parsed,
        task.source_data_source_id,
      );
      const updatedTask = await client.query<TaskRow>(
        `UPDATE ape_dts_platform.tasks
            SET row_version = row_version + 1,
                current_draft_revision_id = $4::uuid,
                updated_by_user_id = $3::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND row_version = $2::bigint
          RETURNING ${TASK_COLUMNS}`,
        [taskId, request.expectedTaskRowVersion, actor.id, updated.id],
      );
      const taskRow = updatedTask.rows[0];
      if (taskRow === undefined) {
        throw new TasksServiceError('TASK_DRAFT_VERSION_CONFLICT');
      }
      await this.audit.append(client, {
        action: 'TASK_DRAFT_SAVED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: {
          draftVersion: updated.draft_version,
          taskType: task.task_type,
          ...(forkApplied ? { sourceAppliedRevisionId: revision.id } : {}),
          ...(addition.addedTables.length > 0
            ? {
                pgTableAddition: {
                  appliedRevisionId: applied!.id,
                  revisionId: updated.id,
                  draftVersion: updated.draft_version,
                  addedTables: addition.addedTables,
                },
              }
            : {}),
        },
        subjectId: taskId,
        subjectType: 'TASK',
      });
      return await this.loadSummary(client, taskRow);
    });
  }

  async delete(
    actor: UserSummary,
    taskId: string,
    expectedRowVersion: string,
  ): Promise<TaskSummary> {
    return await this.transaction(async (client) => {
      const task = await this.requireTask(client, taskId, true);
      if (task.deleted_at !== null) {
        throw new TasksServiceError('TASK_NOT_FOUND');
      }
      await this.requireTaskDataSourceAccess(
        client,
        actor,
        task.source_data_source_id,
        task.target_data_source_id,
        task.task_type,
      );
      if (task.row_version !== expectedRowVersion) {
        throw new TasksServiceError('TASK_VERSION_CONFLICT');
      }
      if (
        task.desired_state !== 'STOPPED' ||
        !['STOPPED', 'FAILED', 'BLOCKED'].includes(task.observed_state)
      ) {
        throw new TasksServiceError('TASK_NOT_STOPPED');
      }
      const activeUnits = await client.query<{ count: string }>(
        `SELECT count(*)::text AS count
           FROM ape_dts_platform.task_run_units
          WHERE task_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
        [taskId],
      );
      if (Number(activeUnits.rows[0]?.count ?? '0') !== 0) {
        throw new TasksServiceError('TASK_ACTIVE_RUN_UNITS');
      }
      const deleted = await client.query<TaskRow>(
        `UPDATE ape_dts_platform.tasks
            SET desired_state = 'DELETED',
                observed_state = 'DELETED',
                deleted_at = statement_timestamp(),
                row_version = row_version + 1,
                updated_by_user_id = $2::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND row_version = $3::bigint
            AND desired_state = 'STOPPED'
            AND observed_state IN ('STOPPED', 'FAILED', 'BLOCKED')
            AND NOT EXISTS (
              SELECT 1
                FROM ape_dts_platform.task_run_units
               WHERE task_run_units.task_id = ape_dts_platform.tasks.id
                 AND task_run_units.status IN ('STARTING', 'RUNNING', 'STOPPING')
            )
          RETURNING ${TASK_COLUMNS}`,
        [taskId, actor.id, expectedRowVersion],
      );
      const row = deleted.rows[0];
      if (row === undefined) {
        throw new TasksServiceError('TASK_VERSION_CONFLICT');
      }
      await client.query(
        `INSERT INTO ape_dts_platform.task_operations (
           id, task_id, action, idempotency_key, target_revision_id,
           state, initiated_by_user_id, reason
         ) VALUES ($1::uuid, $2::uuid, 'DELETE', $3, $4::uuid, 'SUCCEEDED', $5::uuid, $6)`,
        [
          randomUUID(),
          taskId,
          `delete:${expectedRowVersion}`,
          row.current_draft_revision_id,
          actor.id,
          'task soft-deleted',
        ],
      );
      await this.audit.append(client, {
        action: 'TASK_DELETED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: { expectedRowVersion },
        subjectId: taskId,
        subjectType: 'TASK',
      });
      return await this.loadSummary(client, row);
    });
  }
}
