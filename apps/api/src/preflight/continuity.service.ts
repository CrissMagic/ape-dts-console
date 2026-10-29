import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import {
  platformTaskConfigSchema,
  kafkaTaskMappings,
  taskTargetTopics,
  pgTableAddition,
} from '@scs/config-model';
import type {
  AcceptTaskContinuityRequest,
  TaskContinuityAcceptance,
  TaskPreflightCheck,
  UserSummary,
} from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { PostgresContinuityFact } from '../data-sources/adapters/types.js';
import { confirmedPgTableExpansion } from '../tasks/pg-table-expansion.js';

export interface KafkaContinuityTopic {
  readonly clusterId: string;
  readonly partitionCount: number | null;
  readonly partitionIds: readonly number[];
  readonly topicId: string | null;
}

export interface ContinuityTopicSnapshot extends KafkaContinuityTopic {
  readonly topic: string;
  readonly resourceType: 'KAFKA_SOURCE_TOPIC' | 'KAFKA_TARGET_TOPIC';
}

export type ContinuityObservation =
  | {
      readonly kind: 'MYSQL';
      readonly sourceIdentity: string;
      readonly target: KafkaContinuityTopic;
      readonly topics?: readonly ContinuityTopicSnapshot[];
    }
  | {
      readonly kind: 'KAFKA';
      readonly source: KafkaContinuityTopic;
      readonly target: KafkaContinuityTopic;
      readonly topics?: readonly ContinuityTopicSnapshot[];
    }
  | {
      readonly kind: 'POSTGRES';
      readonly pgTableAdditionBaselineVersion?: string;
      readonly source: PostgresContinuityFact;
      readonly target: KafkaContinuityTopic;
      readonly topics?: readonly ContinuityTopicSnapshot[];
    };

type ContinuityActor =
  { readonly type: 'SYSTEM' } | { readonly type: 'USER'; readonly userId: string };

interface BaselineRow extends QueryResultRow {
  baseline_kind: 'KAFKA' | 'POSTGRES' | 'MYSQL';
  mysql_source_identity: string | null;
  id: string;
  pg_confirmed_flush_lsn: string | null;
  pg_database_name: string | null;
  pg_database_oid: string | null;
  pg_slot_name: string | null;
  pg_slot_plugin: string | null;
  pg_system_identifier: string | null;
  row_version: string;
  source_cluster_id: string | null;
  source_partition_ids: number[] | null;
  source_topic_id: string | null;
  target_cluster_id: string;
  target_partition_count: number | null;
  target_topic_id: string | null;
  task_revision_id: string;
  topic_snapshots?: readonly ContinuityTopicSnapshot[] | null;
}

interface ObservationRow extends QueryResultRow {
  baseline_id: string;
  decision: string;
  id: string;
  observation: ContinuityObservation;
  task_id: string;
  task_revision_id: string;
}

export type ContinuityServiceErrorCode =
  | 'CONTINUITY_ACCESS_DENIED'
  | 'CONTINUITY_BASELINE_VERSION_CONFLICT'
  | 'CONTINUITY_OBSERVATION_NOT_FOUND'
  | 'CONTINUITY_OBSERVATION_NOT_MANUAL'
  | 'CONTINUITY_TASK_VERSION_CONFLICT';

export class ContinuityServiceError extends Error {
  constructor(readonly code: ContinuityServiceErrorCode) {
    super(code);
    this.name = 'ContinuityServiceError';
  }
}

function continuityCheck(
  state: 'FAILED' | 'PASSED',
  code: string,
  summary: string,
  details?: Record<string, unknown>,
): TaskPreflightCheck {
  return { code, details: details ?? {}, key: 'TOPIC_IDENTITIES', state, summary };
}

function sorted(values: readonly number[]): number[] {
  return [...values].sort((left, right) => left - right);
}

function equalPartitions(left: readonly number[], right: readonly number[]): boolean {
  const a = sorted(left);
  const b = sorted(right);
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

function strictSuperset(observed: readonly number[], baseline: readonly number[]): boolean {
  return observed.length > baseline.length && baseline.every((value) => observed.includes(value));
}

function partitionOffsets(ids: readonly number[]): Record<string, unknown> {
  return Object.fromEntries(
    sorted(ids).map((id) => [String(id), { logEnd: 'unknown', logStart: 'unknown' }]),
  );
}

function preserveTopicEvidence(
  current: readonly ContinuityTopicSnapshot[],
  previous: readonly ContinuityTopicSnapshot[] | null,
) {
  return current.map((topic) => {
    const old = previous?.find(
      (item) => item.resourceType === topic.resourceType && item.topic === topic.topic,
    );
    return {
      ...topic,
      topicId: topic.topicId ?? old?.topicId ?? null,
      partitionCount: topic.partitionCount ?? old?.partitionCount ?? null,
    };
  });
}

@Injectable()
export class ContinuityService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
  ) {}

  private async loadBaseline(client: PoolClient, taskId: string): Promise<BaselineRow | null> {
    const result = await client.query<BaselineRow>(
      `SELECT id, task_revision_id, baseline_kind, source_cluster_id, source_topic_id,
              source_partition_ids, target_cluster_id, target_topic_id, target_partition_count,
              pg_system_identifier, pg_database_name, pg_database_oid::text, pg_slot_name,
              pg_slot_plugin, pg_confirmed_flush_lsn::text, row_version::text, topic_snapshots, mysql_source_identity
         FROM ape_dts_platform.task_continuity_baselines
        WHERE task_id = $1::uuid
        FOR UPDATE`,
      [taskId],
    );
    return result.rows[0] ?? null;
  }

  private async insertObservation(
    client: PoolClient,
    actor: ContinuityActor,
    taskId: string,
    revisionId: string,
    baselineId: string | null,
    observation: ContinuityObservation,
    decision: 'MATCHED' | 'MANUAL_REQUIRED' | 'PERMANENTLY_BLOCKED',
    decisionCode: string,
  ): Promise<string> {
    const id = randomUUID();
    await client.query(
      `INSERT INTO ape_dts_platform.task_continuity_observations
         (id, task_id, task_revision_id, baseline_id, observation, decision,
          decision_code, actor_type, actor_user_id)
       VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::jsonb, $6, $7, $8, $9::uuid)`,
      [
        id,
        taskId,
        revisionId,
        baselineId,
        JSON.stringify(observation),
        decision,
        decisionCode,
        actor.type,
        actor.type === 'USER' ? actor.userId : null,
      ],
    );
    return id;
  }

  private async legacyTopics(
    client: PoolClient,
    baseline: BaselineRow,
  ): Promise<readonly ContinuityTopicSnapshot[] | null> {
    // Drafts are edited in place; the last successful preflight preserves their old names.
    const prior = await client.query<{ topics: unknown }>(
      `SELECT entry.value -> 'details' -> 'topics' AS topics
         FROM ape_dts_platform.preflight_runs AS run
         CROSS JOIN LATERAL jsonb_array_elements(run.result -> 'checks') AS entry(value)
        WHERE run.task_revision_id = $1::uuid AND run.state = 'PASSED'
          AND entry.value ->> 'key' = 'TOPIC_IDENTITIES'
        ORDER BY run.completed_at DESC, run.id DESC LIMIT 1`,
      [baseline.task_revision_id],
    );
    const values = prior.rows[0]?.topics;
    if (Array.isArray(values)) {
      const topic = (type: ContinuityTopicSnapshot['resourceType']) => {
        const matching = values.filter(
          (value: unknown): value is Record<string, unknown> =>
            typeof value === 'object' &&
            value !== null &&
            (value as Record<string, unknown>).resourceType === type,
        );
        const value = matching[0];
        return matching.length === 1 &&
          value !== undefined &&
          typeof value.topic === 'string' &&
          /^[A-Za-z0-9._-]{1,249}$/u.test(value.topic)
          ? value
          : null;
      };
      const target = topic('KAFKA_TARGET_TOPIC');
      const source = topic('KAFKA_SOURCE_TOPIC');
      if (
        target !== null &&
        target.clusterId === baseline.target_cluster_id &&
        target.topicId === baseline.target_topic_id &&
        (baseline.baseline_kind === 'POSTGRES' ||
          (source !== null &&
            source.clusterId === baseline.source_cluster_id &&
            source.topicId === baseline.source_topic_id &&
            Array.isArray(source.partitionIds) &&
            baseline.source_partition_ids !== null &&
            source.partitionIds.every((value: unknown) => Number.isSafeInteger(value)) &&
            equalPartitions(source.partitionIds as number[], baseline.source_partition_ids)))
      ) {
        const topics: ContinuityTopicSnapshot[] = [
          {
            clusterId: baseline.target_cluster_id,
            topic: String(target.topic),
            topicId: baseline.target_topic_id,
            partitionCount: baseline.target_partition_count,
            partitionIds: [],
            resourceType: 'KAFKA_TARGET_TOPIC',
          },
        ];
        if (baseline.baseline_kind === 'KAFKA' && source !== null)
          topics.push({
            clusterId: baseline.source_cluster_id!,
            topic: String(source.topic),
            topicId: baseline.source_topic_id,
            partitionCount: baseline.source_partition_ids!.length,
            partitionIds: baseline.source_partition_ids!,
            resourceType: 'KAFKA_SOURCE_TOPIC',
          });
        return topics;
      }
    }
    const revision = await client.query<{ configuration: unknown }>(
      'SELECT configuration FROM ape_dts_platform.task_revisions WHERE id = $1::uuid',
      [baseline.task_revision_id],
    );
    const parsed = platformTaskConfigSchema.safeParse(revision.rows[0]?.configuration);
    if (!parsed.success || parsed.data.schemaVersion !== 1) return null;
    const config = parsed.data;
    const target = taskTargetTopics(config)[0];
    if (target === undefined) return null;
    const topics: ContinuityTopicSnapshot[] = [
      {
        clusterId: baseline.target_cluster_id,
        partitionCount: baseline.target_partition_count,
        partitionIds: [],
        resourceType: 'KAFKA_TARGET_TOPIC',
        topic: target.topic,
        topicId: baseline.target_topic_id,
      },
    ];
    if (config.kind === 'KAFKA_RAW_FORWARD') {
      const source = kafkaTaskMappings(config)[0];
      if (
        source === undefined ||
        baseline.source_cluster_id === null ||
        baseline.source_partition_ids === null
      )
        return null;
      topics.push({
        clusterId: baseline.source_cluster_id,
        partitionCount: baseline.source_partition_ids.length,
        partitionIds: baseline.source_partition_ids,
        resourceType: 'KAFKA_SOURCE_TOPIC',
        topic: source.source,
        topicId: baseline.source_topic_id,
      });
    }
    return topics;
  }

  private async gateTask(
    client: PoolClient,
    taskId: string,
    revisionId: string,
    expectedTaskRowVersion: string,
    expectedDesiredState: 'DELETED' | 'RUNNING' | 'STOPPED',
    expectedBaselineRowVersion: string | null,
    disposition: 'MANUAL_REQUIRED' | 'PERMANENTLY_BLOCKED',
    reason: string,
  ): Promise<void> {
    const updated = await client.query(
      `UPDATE ape_dts_platform.tasks AS task
          SET observed_state = 'BLOCKED', recovery_disposition = $5,
              manual_gate_reason = $6, row_version = task.row_version + 1,
              updated_at = statement_timestamp()
        WHERE task.id = $1::uuid
          AND task.deleted_at IS NULL
          AND (task.current_draft_revision_id = $2::uuid OR task.current_applied_revision_id = $2::uuid)
          AND task.row_version = $3::bigint
          AND task.desired_state = $7
          AND ($4::bigint IS NULL OR EXISTS (
            SELECT 1 FROM ape_dts_platform.task_continuity_baselines AS baseline
             WHERE baseline.task_id = task.id AND baseline.row_version = $4::bigint
          ))`,
      [
        taskId,
        revisionId,
        expectedTaskRowVersion,
        expectedBaselineRowVersion,
        disposition,
        reason,
        expectedDesiredState,
      ],
    );
    if (updated.rowCount !== 1)
      throw new ContinuityServiceError('CONTINUITY_TASK_VERSION_CONFLICT');
  }

  async decideWithin(
    client: PoolClient,
    actor: ContinuityActor,
    taskId: string,
    revisionId: string,
    expectedTaskRowVersion: string,
    expectedDesiredState: 'DELETED' | 'RUNNING' | 'STOPPED',
    observation: ContinuityObservation,
  ): Promise<TaskPreflightCheck> {
    const baseline = await this.loadBaseline(client, taskId);
    if (baseline === null) {
      const runs = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id = $1::uuid) AS exists`,
        [taskId],
      );
      if (actor.type === 'SYSTEM' || runs.rows[0]?.exists === true) {
        await this.insertObservation(
          client,
          actor,
          taskId,
          revisionId,
          null,
          observation,
          'PERMANENTLY_BLOCKED',
          'BASELINE_UNAVAILABLE',
        );
        await this.gateTask(
          client,
          taskId,
          revisionId,
          expectedTaskRowVersion,
          expectedDesiredState,
          null,
          'PERMANENTLY_BLOCKED',
          'BASELINE_UNAVAILABLE',
        );
        return continuityCheck(
          'FAILED',
          'BASELINE_UNAVAILABLE',
          '历史任务缺少运行连续性基线，必须删除旧任务并新建。',
        );
      }
      if (observation.kind === 'KAFKA') {
        await client.query(
          `INSERT INTO ape_dts_platform.task_continuity_baselines
             (id, task_id, task_revision_id, baseline_kind, source_cluster_id, source_topic_id,
              source_partition_ids, source_partition_offsets, target_cluster_id, target_topic_id,
              target_partition_count, topic_snapshots)
           VALUES ($1::uuid, $2::uuid, $3::uuid, 'KAFKA', $4, $5, $6::integer[], $7::jsonb, $8, $9, $10, $11::jsonb)`,
          [
            randomUUID(),
            taskId,
            revisionId,
            observation.source.clusterId,
            observation.source.topicId,
            sorted(observation.source.partitionIds),
            JSON.stringify(partitionOffsets(observation.source.partitionIds)),
            observation.target.clusterId,
            observation.target.topicId,
            observation.target.partitionCount,
            observation.topics === undefined ? null : JSON.stringify(observation.topics),
          ],
        );
      } else if (observation.kind === 'MYSQL') {
        await client.query(
          `INSERT INTO ape_dts_platform.task_continuity_baselines
             (id, task_id, task_revision_id, baseline_kind, target_cluster_id, target_topic_id,
              target_partition_count, mysql_source_identity, topic_snapshots)
           VALUES ($1::uuid, $2::uuid, $3::uuid, 'MYSQL', $4, $5, $6, $7, $8::jsonb)`,
          [
            randomUUID(),
            taskId,
            revisionId,
            observation.target.clusterId,
            observation.target.topicId,
            observation.target.partitionCount,
            observation.sourceIdentity,
            observation.topics === undefined ? null : JSON.stringify(observation.topics),
          ],
        );
      } else {
        await client.query(
          `INSERT INTO ape_dts_platform.task_continuity_baselines
             (id, task_id, task_revision_id, baseline_kind, target_cluster_id, target_topic_id,
              target_partition_count, pg_system_identifier, pg_database_name, pg_database_oid,
              pg_slot_name, pg_slot_plugin, pg_confirmed_flush_lsn, topic_snapshots)
           VALUES ($1::uuid, $2::uuid, $3::uuid, 'POSTGRES', $4, $5, $6, $7, $8, $9::oid, $10, $11, $12::pg_lsn, $13::jsonb)`,
          [
            randomUUID(),
            taskId,
            revisionId,
            observation.target.clusterId,
            observation.target.topicId,
            observation.target.partitionCount,
            observation.source.systemIdentifier,
            observation.source.databaseName,
            observation.source.databaseOid,
            observation.source.slotName,
            observation.source.slotPlugin,
            observation.source.confirmedFlushLsn,
            observation.topics === undefined ? null : JSON.stringify(observation.topics),
          ],
        );
      }
      const created = await this.loadBaseline(client, taskId);
      await this.insertObservation(
        client,
        actor,
        taskId,
        revisionId,
        created?.id ?? null,
        observation,
        'MATCHED',
        'BASELINE_CREATED',
      );
      return continuityCheck(
        'PASSED',
        'CONTINUITY_BASELINE_CREATED',
        '首次用户预检已建立运行连续性基线。',
      );
    }

    let decision: 'MATCHED' | 'MANUAL_REQUIRED' | 'PERMANENTLY_BLOCKED' = 'MATCHED';
    let code = 'CONTINUITY_MATCHED';
    const manualRisks: string[] = [];
    const previousTopics =
      observation.topics === undefined
        ? null
        : (baseline.topic_snapshots ?? (await this.legacyTopics(client, baseline)));
    const expansion =
      observation.kind === 'POSTGRES' &&
      (await confirmedPgTableExpansion(client, taskId, revisionId));
    let approvedExpansionTopics: readonly ContinuityTopicSnapshot[] | null = null;
    if (expansion && actor.type === 'SYSTEM') {
      const approved = await client.query<{ topics: ContinuityTopicSnapshot[] }>(
        `SELECT entry.value->'details'->'topics' AS topics
         FROM ape_dts_platform.preflight_runs run
         JOIN ape_dts_platform.task_revisions revision ON revision.id=run.task_revision_id
         CROSS JOIN LATERAL jsonb_array_elements(run.result->'checks') entry(value)
         WHERE run.task_revision_id=$1::uuid AND run.state='PASSED'
           AND run.result->'taskRevision'->>'draftVersion'=revision.draft_version::text
           AND entry.value->>'key'='TOPIC_IDENTITIES'
         ORDER BY run.completed_at DESC,run.id DESC LIMIT 1`,
        [revisionId],
      );
      approvedExpansionTopics = approved.rows[0]?.topics ?? null;
      if (!Array.isArray(approvedExpansionTopics))
        return continuityCheck(
          'FAILED',
          'PG_TABLE_ADDITION_PREFLIGHT_REQUIRED',
          '新增表草稿缺少当前版本的成功预检，请重新预检。',
        );
    }
    if (observation.kind === 'POSTGRES' && !expansion) {
      const revisions = await client.query<{ applied: unknown; candidate: unknown }>(
        `SELECT applied.configuration AS applied,candidate.configuration AS candidate
         FROM ape_dts_platform.tasks task
         JOIN ape_dts_platform.task_revisions applied ON applied.id=task.current_applied_revision_id AND applied.task_id=task.id
         JOIN ape_dts_platform.task_revisions candidate ON candidate.id=$2::uuid AND candidate.task_id=task.id
         WHERE task.id=$1::uuid AND task.current_draft_revision_id=candidate.id`,
        [taskId, revisionId],
      );
      const revision = revisions.rows[0];
      if (revision && pgTableAddition(revision.applied, revision.candidate).addedTables.length > 0)
        return continuityCheck(
          'FAILED',
          'PG_TABLE_ADDITION_CONFIRMATION_REQUIRED',
          '新增表配置尚未确认或不满足保留原槽续传条件，请返回编辑页核对并确认保存。',
        );
    }
    if (observation.topics !== undefined && previousTopics === null) {
      decision = 'PERMANENTLY_BLOCKED';
      code = 'BASELINE_UNAVAILABLE';
    } else if (baseline.baseline_kind !== observation.kind) {
      decision = 'PERMANENTLY_BLOCKED';
      code = 'CONTINUITY_KIND_CHANGED';
    } else if (baseline.target_cluster_id !== observation.target.clusterId) {
      decision = 'PERMANENTLY_BLOCKED';
      code = 'KAFKA_TARGET_CLUSTER_CHANGED';
    } else if (observation.kind === 'KAFKA' && observation.topics === undefined) {
      if (baseline.source_cluster_id !== observation.source.clusterId) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'KAFKA_SOURCE_CLUSTER_CHANGED';
      } else if (
        baseline.source_topic_id !== null &&
        observation.source.topicId !== null &&
        baseline.source_topic_id !== observation.source.topicId
      ) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'KAFKA_SOURCE_TOPIC_ID_CHANGED';
      } else if (
        !equalPartitions(baseline.source_partition_ids ?? [], observation.source.partitionIds)
      ) {
        if (strictSuperset(observation.source.partitionIds, baseline.source_partition_ids ?? []))
          manualRisks.push('KAFKA_SOURCE_PARTITIONS_ADDED');
        else {
          decision = 'PERMANENTLY_BLOCKED';
          code = 'KAFKA_SOURCE_PARTITION_SET_CHANGED';
        }
      }
    } else if (observation.kind === 'MYSQL') {
      if (baseline.mysql_source_identity !== observation.sourceIdentity) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'MYSQL_SOURCE_IDENTITY_CHANGED';
      }
    } else if (observation.kind === 'POSTGRES') {
      const source = observation.source;
      if (baseline.pg_system_identifier !== source.systemIdentifier) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'PG_SYSTEM_IDENTIFIER_CHANGED';
      } else if (
        baseline.pg_database_name !== source.databaseName ||
        baseline.pg_database_oid !== source.databaseOid
      ) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'PG_DATABASE_IDENTITY_CHANGED';
      } else if (baseline.pg_slot_name !== source.slotName) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'PG_SLOT_NAME_CHANGED';
      } else if (baseline.pg_slot_plugin !== source.slotPlugin) {
        decision = 'PERMANENTLY_BLOCKED';
        code = 'PG_SLOT_PLUGIN_CHANGED';
      } else {
        const lsn = await client.query<{ monotonic: boolean }>(
          `SELECT $1::pg_lsn >= $2::pg_lsn AS monotonic`,
          [source.confirmedFlushLsn, baseline.pg_confirmed_flush_lsn],
        );
        if (lsn.rows[0]?.monotonic !== true) {
          decision = 'PERMANENTLY_BLOCKED';
          code = 'PG_CONFIRMED_FLUSH_LSN_REGRESSED';
        }
      }
    }
    if (
      decision !== 'PERMANENTLY_BLOCKED' &&
      observation.topics !== undefined &&
      previousTopics !== null
    ) {
      const key = (topic: ContinuityTopicSnapshot) => `${topic.resourceType}:${topic.topic}`;
      const previous = new Map(previousTopics.map((topic) => [key(topic), topic]));
      const current = new Map(observation.topics.map((topic) => [key(topic), topic]));
      if (
        (previous.size !== current.size && !expansion) ||
        [...previous.keys()].some((name) => !current.has(name))
      )
        manualRisks.push('KAFKA_TOPIC_MAPPING_CHANGED');
      for (const [name, observed] of current) {
        const old =
          previous.get(name) ?? approvedExpansionTopics?.find((topic) => key(topic) === name);
        if (old === undefined && approvedExpansionTopics !== null) {
          decision = 'PERMANENTLY_BLOCKED';
          code = 'KAFKA_TOPIC_MAPPING_CHANGED';
          break;
        }
        if (old === undefined) continue;
        if (old.clusterId !== observed.clusterId) {
          decision = 'PERMANENTLY_BLOCKED';
          code =
            observed.resourceType === 'KAFKA_SOURCE_TOPIC'
              ? 'KAFKA_SOURCE_CLUSTER_CHANGED'
              : 'KAFKA_TARGET_CLUSTER_CHANGED';
          break;
        }
        const idChanged =
          old.topicId !== null && observed.topicId !== null && old.topicId !== observed.topicId;
        if (observed.resourceType === 'KAFKA_SOURCE_TOPIC') {
          if (idChanged) {
            decision = 'PERMANENTLY_BLOCKED';
            code = 'KAFKA_SOURCE_TOPIC_ID_CHANGED';
            break;
          }
          if (!equalPartitions(old.partitionIds, observed.partitionIds)) {
            if (strictSuperset(observed.partitionIds, old.partitionIds))
              manualRisks.push('KAFKA_SOURCE_PARTITIONS_ADDED');
            else {
              decision = 'PERMANENTLY_BLOCKED';
              code = 'KAFKA_SOURCE_PARTITION_SET_CHANGED';
              break;
            }
          }
        } else {
          if (idChanged) manualRisks.push('KAFKA_TARGET_TOPIC_RECREATED');
          if (
            old.partitionCount !== null &&
            observed.partitionCount !== null &&
            old.partitionCount !== observed.partitionCount
          )
            manualRisks.push('KAFKA_TARGET_PARTITION_COUNT_CHANGED');
        }
      }
    }
    if (decision !== 'PERMANENTLY_BLOCKED') {
      if (
        observation.topics === undefined &&
        baseline.target_topic_id !== null &&
        observation.target.topicId !== null &&
        baseline.target_topic_id !== observation.target.topicId
      )
        manualRisks.push('KAFKA_TARGET_TOPIC_RECREATED');
      if (
        observation.topics === undefined &&
        baseline.target_partition_count !== null &&
        observation.target.partitionCount !== null &&
        baseline.target_partition_count !== observation.target.partitionCount
      )
        manualRisks.push('KAFKA_TARGET_PARTITION_COUNT_CHANGED');
      if (manualRisks.length > 0) {
        decision = 'MANUAL_REQUIRED';
        code = [...new Set(manualRisks)].join(',');
      }
    }
    if (actor.type === 'SYSTEM' && decision === 'MANUAL_REQUIRED') code = `SYSTEM_${code}`;
    if (expansion && decision === 'MATCHED' && observation.kind === 'POSTGRES') {
      code = 'PG_TABLE_ADDITION_READY';
      observation = { ...observation, pgTableAdditionBaselineVersion: baseline.row_version };
    }
    const observationId = await this.insertObservation(
      client,
      actor,
      taskId,
      revisionId,
      baseline.id,
      observation,
      decision,
      code,
    );
    if (decision === 'PERMANENTLY_BLOCKED' || decision === 'MANUAL_REQUIRED') {
      await this.gateTask(
        client,
        taskId,
        revisionId,
        expectedTaskRowVersion,
        expectedDesiredState,
        baseline.row_version,
        decision,
        code,
      );
      return continuityCheck(
        'FAILED',
        code,
        decision === 'MANUAL_REQUIRED'
          ? '检测到需人工确认的运行连续性变化。'
          : '检测到永久不连续，必须删除旧任务并新建。',
        { baselineRowVersion: baseline.row_version, observationId, risks: manualRisks },
      );
    }
    if (expansion)
      return continuityCheck(
        'PASSED',
        'PG_TABLE_ADDITION_READY',
        '新增表增量续传检查通过；原连续性基线将在新运行成功后更新。',
      );
    if (
      observation.topics !== undefined ||
      (observation.kind === 'POSTGRES' &&
        baseline.pg_confirmed_flush_lsn !== observation.source.confirmedFlushLsn)
    ) {
      const topics =
        observation.topics === undefined
          ? undefined
          : preserveTopicEvidence(observation.topics, previousTopics);
      await client.query(
        `UPDATE ape_dts_platform.task_continuity_baselines
            SET pg_confirmed_flush_lsn = $2::pg_lsn, task_revision_id = $3::uuid,
                topic_snapshots = COALESCE($5::jsonb, topic_snapshots),
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE id = $1::uuid AND row_version = $4::bigint`,
        [
          baseline.id,
          observation.kind === 'POSTGRES' ? observation.source.confirmedFlushLsn : null,
          revisionId,
          baseline.row_version,
          topics === undefined ? null : JSON.stringify(topics),
        ],
      );
    }
    return continuityCheck('PASSED', 'CONTINUITY_MATCHED', '运行连续性基线核验通过。');
  }

  async reinitializeMysqlWithin(
    client: PoolClient,
    actorUserId: string,
    taskId: string,
    revisionId: string,
    observation: Extract<ContinuityObservation, { kind: 'MYSQL' }>,
  ): Promise<TaskPreflightCheck> {
    const previous = await this.loadBaseline(client, taskId);
    if (previous !== null && previous.baseline_kind !== 'MYSQL')
      throw new ContinuityServiceError('CONTINUITY_TASK_VERSION_CONFLICT');
    const baselineId = previous?.id ?? randomUUID();
    if (previous === null) {
      await client.query(
        `INSERT INTO ape_dts_platform.task_continuity_baselines
        (id,task_id,task_revision_id,baseline_kind,mysql_source_identity,target_cluster_id,target_topic_id,target_partition_count,topic_snapshots)
        VALUES ($1::uuid,$2::uuid,$3::uuid,'MYSQL',$4,$5,$6,$7,$8::jsonb)`,
        [
          baselineId,
          taskId,
          revisionId,
          observation.sourceIdentity,
          observation.target.clusterId,
          observation.target.topicId,
          observation.target.partitionCount,
          observation.topics === undefined ? null : JSON.stringify(observation.topics),
        ],
      );
    } else {
      const updated = await client.query(
        `UPDATE ape_dts_platform.task_continuity_baselines
        SET task_revision_id=$2::uuid, mysql_source_identity=$3,target_cluster_id=$4,target_topic_id=$5,
          target_partition_count=$6,topic_snapshots=$7::jsonb,row_version=row_version+1,updated_at=statement_timestamp()
        WHERE id=$1::uuid AND row_version=$8::bigint AND baseline_kind='MYSQL'`,
        [
          baselineId,
          revisionId,
          observation.sourceIdentity,
          observation.target.clusterId,
          observation.target.topicId,
          observation.target.partitionCount,
          observation.topics === undefined ? null : JSON.stringify(observation.topics),
          previous.row_version,
        ],
      );
      if (updated.rowCount !== 1)
        throw new ContinuityServiceError('CONTINUITY_BASELINE_VERSION_CONFLICT');
    }
    await this.insertObservation(
      client,
      { type: 'USER', userId: actorUserId },
      taskId,
      revisionId,
      baselineId,
      observation,
      'MATCHED',
      'MYSQL_SNAPSHOT_REINITIALIZATION_PREFLIGHT',
    );
    await this.audit.append(client, {
      action: 'TASK_MYSQL_SNAPSHOT_REINITIALIZATION_PREFLIGHT',
      actorType: 'USER',
      actorUserId,
      subjectType: 'TASK',
      subjectId: taskId,
      details: { revisionId, previousBaseline: previous, observation },
    });
    return continuityCheck(
      'PASSED',
      'MYSQL_SNAPSHOT_REINITIALIZATION_READY',
      '重新全量预检通过；启动时将建立新位点，可能重复投递历史数据。',
    );
  }

  async accept(
    actor: UserSummary,
    taskId: string,
    request: AcceptTaskContinuityRequest,
  ): Promise<TaskContinuityAcceptance> {
    if (actor.role !== 'ADMIN') throw new ContinuityServiceError('CONTINUITY_ACCESS_DENIED');
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const observationResult = await client.query<ObservationRow>(
        `SELECT id, task_id, task_revision_id, baseline_id, observation, decision
           FROM ape_dts_platform.task_continuity_observations
          WHERE id = $1::uuid AND task_id = $2::uuid FOR SHARE`,
        [request.observationId, taskId],
      );
      const observation = observationResult.rows[0];
      if (observation === undefined)
        throw new ContinuityServiceError('CONTINUITY_OBSERVATION_NOT_FOUND');
      if (observation.decision !== 'MANUAL_REQUIRED')
        throw new ContinuityServiceError('CONTINUITY_OBSERVATION_NOT_MANUAL');
      const baseline = await this.loadBaseline(client, taskId);
      if (
        baseline === null ||
        baseline.id !== observation.baseline_id ||
        baseline.row_version !== request.expectedBaselineRowVersion
      )
        throw new ContinuityServiceError('CONTINUITY_BASELINE_VERSION_CONFLICT');
      const task = await client.query<{
        desired_state: 'DELETED' | 'RUNNING' | 'STOPPED';
        row_version: string;
      }>(
        `SELECT desired_state, row_version::text FROM ape_dts_platform.tasks
          WHERE id = $1::uuid AND deleted_at IS NULL AND current_draft_revision_id = $2::uuid
            AND recovery_disposition = 'MANUAL_REQUIRED' FOR UPDATE`,
        [taskId, observation.task_revision_id],
      );
      if (task.rows[0]?.row_version !== request.expectedTaskRowVersion)
        throw new ContinuityServiceError('CONTINUITY_TASK_VERSION_CONFLICT');
      const value = observation.observation;
      const previousTopics =
        baseline.topic_snapshots ??
        (value.topics === undefined ? null : await this.legacyTopics(client, baseline));
      const topics =
        value.topics === undefined
          ? previousTopics
          : preserveTopicEvidence(value.topics, previousTopics);
      const updated =
        value.kind === 'KAFKA'
          ? await client.query(
              `UPDATE ape_dts_platform.task_continuity_baselines
                SET task_revision_id=$2::uuid, source_partition_ids=$3::integer[],
                    source_partition_offsets=$4::jsonb, target_topic_id=COALESCE($5, target_topic_id),
                    target_partition_count=COALESCE($6, target_partition_count), topic_snapshots=$8::jsonb, row_version=row_version+1,
                    updated_at=statement_timestamp()
              WHERE id=$1::uuid AND row_version=$7::bigint`,
              [
                baseline.id,
                observation.task_revision_id,
                sorted(value.source.partitionIds),
                JSON.stringify(partitionOffsets(value.source.partitionIds)),
                value.target.topicId,
                value.target.partitionCount,
                request.expectedBaselineRowVersion,
                topics === null ? null : JSON.stringify(topics),
              ],
            )
          : await client.query(
              `UPDATE ape_dts_platform.task_continuity_baselines
                SET task_revision_id=$2::uuid, target_topic_id=COALESCE($3, target_topic_id), target_partition_count=COALESCE($4, target_partition_count),
                    pg_confirmed_flush_lsn=$5::pg_lsn, topic_snapshots=$7::jsonb, row_version=row_version+1,
                    updated_at=statement_timestamp()
              WHERE id=$1::uuid AND row_version=$6::bigint`,
              [
                baseline.id,
                observation.task_revision_id,
                value.target.topicId,
                value.target.partitionCount,
                value.kind === 'POSTGRES' ? value.source.confirmedFlushLsn : null,
                request.expectedBaselineRowVersion,
                topics === null ? null : JSON.stringify(topics),
              ],
            );
      if (updated.rowCount !== 1)
        throw new ContinuityServiceError('CONTINUITY_BASELINE_VERSION_CONFLICT');
      const acceptanceId = randomUUID();
      await client.query(
        `INSERT INTO ape_dts_platform.task_continuity_acceptances
           (id, observation_id, expected_baseline_row_version, accepted_by_user_id, reason, accepted_observation)
         VALUES ($1::uuid,$2::uuid,$3::bigint,$4::uuid,$5,$6::jsonb)`,
        [
          acceptanceId,
          observation.id,
          request.expectedBaselineRowVersion,
          actor.id,
          request.reason,
          JSON.stringify(value),
        ],
      );
      const taskUpdate = await client.query(
        `UPDATE ape_dts_platform.tasks
            SET recovery_disposition='NOT_APPLICABLE', observed_state='STOPPED', manual_gate_reason=NULL,
                row_version=row_version+1, updated_by_user_id=$2::uuid, updated_at=statement_timestamp()
          WHERE id=$1::uuid AND deleted_at IS NULL AND row_version=$3::bigint
            AND desired_state=$4 AND recovery_disposition='MANUAL_REQUIRED'`,
        [taskId, actor.id, request.expectedTaskRowVersion, task.rows[0]?.desired_state],
      );
      if (taskUpdate.rowCount !== 1)
        throw new ContinuityServiceError('CONTINUITY_TASK_VERSION_CONFLICT');
      await this.audit.append(client, {
        action: 'TASK_CONTINUITY_ACCEPTED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: { observationId: observation.id, reason: request.reason },
        subjectId: taskId,
        subjectType: 'TASK',
      });
      await client.query('COMMIT');
      return {
        acceptedAt: new Date().toISOString(),
        baselineRowVersion: String(BigInt(baseline.row_version) + 1n),
        id: acceptanceId,
        observationId: observation.id,
        taskId,
        taskRowVersion: String(BigInt(request.expectedTaskRowVersion) + 1n),
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
