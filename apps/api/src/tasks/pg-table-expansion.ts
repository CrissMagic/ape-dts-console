import { pgTableAddition, pgToKafkaTaskConfigSchema } from '@scs/config-model';
import type { PoolClient } from 'pg';
import type {
  ContinuityObservation,
  ContinuityTopicSnapshot,
} from '../preflight/continuity.service.js';

/** The saved revision/version, not an arbitrary prior acknowledgment, authorizes extension. */
export async function confirmedPgTableExpansion(
  client: PoolClient,
  taskId: string,
  revisionId: string,
): Promise<boolean> {
  const result = await client.query<{
    configuration: unknown;
    applied_configuration: unknown;
    confirmed: boolean;
  }>(
    `SELECT draft.configuration, applied.configuration AS applied_configuration,
       EXISTS (SELECT 1 FROM ape_dts_platform.audit_events event
         WHERE event.subject_id=task.id AND event.subject_type='TASK'
           AND event.action='TASK_DRAFT_SAVED'
           AND event.canonical_details->'pgTableAddition'->>'revisionId'=draft.id::text
           AND event.canonical_details->'pgTableAddition'->>'draftVersion'=draft.draft_version::text
           AND event.canonical_details->'pgTableAddition'->>'appliedRevisionId'=applied.id::text
       ) AS confirmed
     FROM ape_dts_platform.tasks task
     JOIN ape_dts_platform.task_revisions draft ON draft.id=task.current_draft_revision_id AND draft.task_id=task.id
     JOIN ape_dts_platform.task_revisions applied ON applied.id=task.current_applied_revision_id AND applied.task_id=task.id
     WHERE task.id=$1::uuid AND draft.id=$2::uuid AND task.deleted_at IS NULL`,
    [taskId, revisionId],
  );
  const row = result.rows[0];
  if (!row?.confirmed || !pgTableAddition(row.applied_configuration, row.configuration).eligible)
    return false;
  const parsed = pgToKafkaTaskConfigSchema.safeParse(row.configuration);
  return parsed.success && !parsed.data.source.includeSnapshot;
}

/** Called inside the successful start transaction, before changing the applied revision. */
export async function applyPgTableExpansionContinuity(
  client: PoolClient,
  taskId: string,
  revisionId: string,
): Promise<void> {
  if (!(await confirmedPgTableExpansion(client, taskId, revisionId))) return;
  const result = await client.query<{
    id: string;
    row_version: string;
    topic_snapshots: ContinuityTopicSnapshot[] | null;
    observation: ContinuityObservation;
    decision: string;
    decision_code: string;
  }>(
    `SELECT baseline.id,baseline.row_version::text,baseline.topic_snapshots,
      evidence.observation,evidence.decision,evidence.decision_code
     FROM ape_dts_platform.task_continuity_baselines baseline
     JOIN LATERAL (SELECT observation,decision,decision_code FROM ape_dts_platform.task_continuity_observations
       WHERE task_id=baseline.task_id AND task_revision_id=$2::uuid
       ORDER BY observed_at DESC,id DESC LIMIT 1) evidence ON true
     WHERE baseline.task_id=$1::uuid FOR UPDATE OF baseline`,
    [taskId, revisionId],
  );
  const row = result.rows[0];
  const fact = row?.observation;
  if (
    !row ||
    fact?.kind !== 'POSTGRES' ||
    row.decision !== 'MATCHED' ||
    row.decision_code !== 'PG_TABLE_ADDITION_READY' ||
    fact.pgTableAdditionBaselineVersion !== row.row_version
  )
    throw new Error('PG_TABLE_ADDITION_CONTINUITY_STALE');
  const topics = fact.topics?.map((topic) => {
    const old = row.topic_snapshots?.find(
      (item) => item.resourceType === topic.resourceType && item.topic === topic.topic,
    );
    return {
      ...topic,
      topicId: topic.topicId ?? old?.topicId ?? null,
      partitionCount: topic.partitionCount ?? old?.partitionCount ?? null,
    };
  });
  const updated = await client.query(
    `UPDATE ape_dts_platform.task_continuity_baselines SET task_revision_id=$2::uuid,
      topic_snapshots=COALESCE($3::jsonb,topic_snapshots),pg_confirmed_flush_lsn=$4::pg_lsn,
      target_topic_id=$5,target_partition_count=$6,row_version=row_version+1,updated_at=statement_timestamp()
     WHERE id=$1::uuid AND row_version=$7::bigint`,
    [
      row.id,
      revisionId,
      topics === undefined ? null : JSON.stringify(topics),
      fact.source.confirmedFlushLsn,
      fact.target.topicId,
      fact.target.partitionCount,
      row.row_version,
    ],
  );
  if (updated.rowCount !== 1) throw new Error('PG_TABLE_ADDITION_CONTINUITY_STALE');
}
