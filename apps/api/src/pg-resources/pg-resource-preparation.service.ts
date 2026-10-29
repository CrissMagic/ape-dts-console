import { Injectable } from '@nestjs/common';
import { pgTableAddition, pgTaskMappings, platformTaskConfigSchema } from '@scs/config-model';
import type {
  PreparePgResourcesRequest,
  PreparePgResourcesResponse,
  UserSummary,
} from '@scs/contracts';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { DataSourcesService } from '../data-sources/data-sources.service.js';
import {
  DataSourceAdapterError,
  PostgresResourcePreparationError,
  type PreparedPostgresResources,
} from '../data-sources/adapters/types.js';
import { PgResourceManager } from './pg-resource-manager.js';
import { confirmedPgTableExpansion } from '../tasks/pg-table-expansion.js';

export class PgResourcePreparationError extends Error {
  constructor(
    readonly code:
      | 'PG_RESOURCE_ACCESS_DENIED'
      | 'PG_RESOURCE_TASK_NOT_FOUND'
      | 'PG_RESOURCE_VERSION_CONFLICT'
      | 'PG_RESOURCE_PREPARATION_NOT_ALLOWED'
      | 'PG_RESOURCE_ENGINE_MANAGED',
  ) {
    super(code);
  }
}

@Injectable()
export class PgResourcePreparationService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly dataSources: DataSourcesService,
    private readonly resources: PgResourceManager,
  ) {}

  async prepare(
    actor: UserSummary,
    taskId: string,
    request: PreparePgResourcesRequest,
  ): Promise<PreparePgResourcesResponse> {
    if (actor.role !== 'ADMIN' || actor.status !== 'ACTIVE')
      throw new PgResourcePreparationError('PG_RESOURCE_ACCESS_DENIED');
    const client = await this.database.requirePool().connect();
    let completed: PreparedPostgresResources | undefined;
    let sourceVersion: string | undefined;
    try {
      await client.query('BEGIN');
      const validateCurrentDraft = async () => {
        const user = await client.query<{ role: string; status: string }>(
          'SELECT role,status FROM ape_dts_platform.users WHERE id=$1::uuid FOR SHARE',
          [actor.id],
        );
        if (user.rows[0]?.role !== 'ADMIN' || user.rows[0]?.status !== 'ACTIVE')
          throw new PgResourcePreparationError('PG_RESOURCE_ACCESS_DENIED');
        const tasks = await client.query<{
          current_draft_revision_id: string | null;
          current_applied_revision_id: string | null;
          row_version: string;
          desired_state: string;
          observed_state: string;
          source_data_source_id: string;
        }>(
          `SELECT current_draft_revision_id,current_applied_revision_id,row_version::text,
          desired_state,observed_state,source_data_source_id
          FROM ape_dts_platform.tasks WHERE id=$1::uuid AND deleted_at IS NULL FOR UPDATE`,
          [taskId],
        );
        const task = tasks.rows[0];
        if (!task) throw new PgResourcePreparationError('PG_RESOURCE_TASK_NOT_FOUND');
        if (
          task.row_version !== request.expectedTaskRowVersion ||
          task.current_draft_revision_id !== request.revisionId
        )
          throw new PgResourcePreparationError('PG_RESOURCE_VERSION_CONFLICT');
        if (task.desired_state !== 'STOPPED' || task.observed_state !== 'STOPPED')
          throw new PgResourcePreparationError('PG_RESOURCE_PREPARATION_NOT_ALLOWED');
        const revisions = await client.query<{
          configuration: unknown;
          draft_version: string;
          state: string;
        }>(
          `SELECT configuration,draft_version::text,state FROM ape_dts_platform.task_revisions
          WHERE id=$1::uuid AND task_id=$2::uuid FOR SHARE`,
          [request.revisionId, taskId],
        );
        const revision = revisions.rows[0];
        if (!revision || revision.draft_version !== request.expectedDraftVersion)
          throw new PgResourcePreparationError('PG_RESOURCE_VERSION_CONFLICT');
        if (!['READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(revision.state))
          throw new PgResourcePreparationError('PG_RESOURCE_PREPARATION_NOT_ALLOWED');
        const config = platformTaskConfigSchema.parse(revision.configuration);
        if (
          config.kind !== 'PG_TO_KAFKA' ||
          config.source.dataSourceId !== task.source_data_source_id
        )
          throw new PgResourcePreparationError('PG_RESOURCE_PREPARATION_NOT_ALLOWED');
        if (config.source.resourceMode === 'APE_DTS_MANAGED')
          throw new PgResourcePreparationError('PG_RESOURCE_ENGINE_MANAGED');
        const extending = task.current_applied_revision_id !== null;
        const active = await client.query<{ exists: boolean }>(
          `SELECT EXISTS (
             SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
             UNION ALL SELECT 1 FROM ape_dts_platform.task_run_units WHERE task_id=$1::uuid AND status IN ('STARTING','RUNNING','STOPPING')
           ) AS exists`,
          [taskId],
        );
        if (
          active.rows[0]?.exists !== false ||
          (extending && !(await confirmedPgTableExpansion(client, taskId, request.revisionId)))
        )
          throw new PgResourcePreparationError('PG_RESOURCE_PREPARATION_NOT_ALLOWED');
        const history = await client.query<{ exists: boolean }>(
          `SELECT EXISTS (
        SELECT 1 FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid UNION ALL
        SELECT 1 FROM ape_dts_platform.task_continuity_baselines WHERE task_id=$1::uuid
      ) AS exists`,
          [taskId],
        );
        if (!extending && history.rows[0]?.exists !== false)
          throw new PgResourcePreparationError('PG_RESOURCE_PREPARATION_NOT_ALLOWED');
        const source = await client.query<{ row_version: string }>(
          `SELECT row_version::text FROM ape_dts_platform.data_sources
           WHERE id=$1::uuid AND state='ACTIVE' AND deleted_at IS NULL FOR UPDATE`,
          [task.source_data_source_id],
        );
        if (
          !source.rows[0] ||
          (sourceVersion !== undefined && source.rows[0].row_version !== sourceVersion)
        )
          throw new PgResourcePreparationError('PG_RESOURCE_VERSION_CONFLICT');
        sourceVersion = source.rows[0].row_version;
        return { task, config, mode: config.source.resourceMode, extending };
      };
      const { task, config, mode, extending } = await validateCurrentDraft();
      const applied = extending
        ? await client.query<{ configuration: unknown }>(
            `SELECT configuration FROM ape_dts_platform.task_revisions
             WHERE id=$1::uuid AND task_id=$2::uuid FOR SHARE`,
            [task.current_applied_revision_id, taskId],
          )
        : null;
      const addedTables = pgTableAddition(applied?.rows[0]?.configuration, config).addedTables;
      // Do not retain row locks while waiting for the source's advisory lock.
      // The callback revalidates this snapshot before performing any CREATE.
      await client.query('COMMIT');
      completed = await this.dataSources.preparePostgresResources(
        actor,
        task.source_data_source_id,
        {
          resourceMode: mode,
          slotName: config.source.slotName,
          publicationName: config.source.publicationName,
          tables: pgTaskMappings(config).map((mapping) => mapping.source),
          ...(extending ? { extendPublication: { addedTables } } : {}),
        },
        client,
        async (inspection) => {
          await client.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [
            `scs-pg:${inspection.identityPrefix}`,
          ]);
          await client.query('BEGIN');
          await validateCurrentDraft();
          // Existing active bindings for this task or physical database are checked
          // before any external CREATE, including aliases via another data source.
          const existing = await client.query<{
            task_id: string;
            resource_type: string;
            normalized_name: string;
            stable_identity: string;
            ownership_mode: string;
            data_source_id: string;
            lifecycle_state: string;
          }>(
            `SELECT task_id,resource_type,normalized_name,stable_identity,ownership_mode,data_source_id,lifecycle_state
          FROM ape_dts_platform.external_resources
          WHERE resource_type IN ('PG_SLOT','PG_PUBLICATION')
            AND lifecycle_state = ANY($4::text[])
            AND (task_id=$1::uuid OR data_source_id=$2::uuid OR stable_identity LIKE $3)
          FOR UPDATE`,
            [
              taskId,
              task.source_data_source_id,
              `${inspection.identityPrefix}%`,
              PgResourceManager.activeLifecycleStates,
            ],
          );
          if (
            extending &&
            ['PG_SLOT', 'PG_PUBLICATION'].some(
              (resourceType) =>
                existing.rows.filter(
                  (row) => row.task_id === taskId && row.resource_type === resourceType,
                ).length !== 1,
            )
          )
            throw new DataSourceAdapterError(
              'PG_RESOURCE_CONFLICT',
              '历史任务必须保留已绑定的复制槽和发布。',
            );
          for (const row of existing.rows) {
            const name =
              row.resource_type === 'PG_SLOT'
                ? config.source.slotName
                : config.source.publicationName;
            const identity =
              row.resource_type === 'PG_SLOT'
                ? inspection.slotIdentity
                : inspection.publicationIdentity;
            const normalized = name.normalize('NFKC').trim().toLocaleLowerCase('en-US');
            if (row.task_id !== taskId && row.normalized_name !== normalized) continue;
            if (
              row.task_id !== taskId ||
              row.normalized_name !== normalized ||
              row.stable_identity !== identity ||
              row.ownership_mode !== mode ||
              row.data_source_id !== task.source_data_source_id ||
              row.lifecycle_state !== 'BOUND'
            )
              throw new DataSourceAdapterError(
                'PG_RESOURCE_CONFLICT',
                '复制资源已有冲突绑定；不会接管、覆盖或重建。',
              );
          }
          // A logical slot waits for outstanding XIDs across the whole cluster.
          // Release row-lock transactions before external CREATE so a platform
          // database in this same cluster cannot block its own slot creation.
          // The session advisory lock still serializes physical resource claims.
          await client.query('COMMIT');
        },
      );
      await client.query('BEGIN');
      // Edits or starts during external I/O must not bind a stale draft. If this
      // fails, keep the external objects and report the concrete creation result.
      await validateCurrentDraft();
      for (const [resourceType, resource] of [
        ['PG_SLOT', completed.slot],
        ['PG_PUBLICATION', completed.publication],
      ] as const) {
        await this.resources.bind(
          actor,
          {
            taskId,
            dataSourceId: task.source_data_source_id,
            databaseName: completed.databaseName,
            name: resource.name,
            ownershipMode: mode,
            resourceType,
            stableIdentity: resource.stableIdentity,
            platformCreated: resource.created,
          },
          client,
        );
      }
      await client.query('COMMIT');
      return {
        taskId,
        revisionId: request.revisionId,
        ...completed,
        resourceMode: mode,
        requiresPreflight: true,
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (completed)
        throw new PostgresResourcePreparationError(
          {
            slotName: completed.slot.name,
            publicationName: completed.publication.name,
            created: { slot: completed.slot.created, publication: completed.publication.created },
            ...(completed.publication.extendedTables?.length
              ? { publicationExtendedTables: completed.publication.extendedTables }
              : {}),
            requiresInspection: true,
          },
          { cause: error },
        );
      throw error;
    } finally {
      client.release(true);
    }
  }
}
