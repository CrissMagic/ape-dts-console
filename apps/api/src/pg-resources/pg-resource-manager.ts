import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { UserSummary } from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';

export type PgResourceType = 'PG_SLOT' | 'PG_PUBLICATION';
export type PgResourceOwnership = 'SELF_MANAGED' | 'PLATFORM_MANAGED' | 'APE_DTS_MANAGED';

export type PgResourceManagerErrorCode =
  'PG_RESOURCE_CONFLICT' | 'PG_RESOURCE_INVALID' | 'PG_RESOURCE_NOT_FOUND';

export class PgResourceManagerError extends Error {
  constructor(readonly code: PgResourceManagerErrorCode) {
    super(code);
    this.name = 'PgResourceManagerError';
  }
}

export interface BindPgResourceRequest {
  readonly platformCreated?: boolean;
  readonly dataSourceId: string;
  readonly databaseName: string;
  readonly name: string;
  readonly ownershipMode: PgResourceOwnership;
  readonly resourceType: PgResourceType;
  readonly stableIdentity: string;
  readonly taskId: string;
}

export interface PgResourceSummary {
  readonly dataSourceId: string;
  readonly databaseName: string;
  readonly id: string;
  readonly lifecycleState: string;
  readonly name: string;
  readonly ownershipMode: PgResourceOwnership;
  readonly resourceType: PgResourceType;
  readonly stableIdentity: string;
  readonly taskId: string;
}

interface ResourceRow extends QueryResultRow {
  data_source_id: string;
  database_name: string | null;
  id: string;
  lifecycle_state: string;
  normalized_name: string;
  ownership_mode: PgResourceOwnership;
  resource_type: PgResourceType;
  stable_identity: string;
  task_id: string;
}

const ACTIVE_RESOURCE_STATES = [
  'BOUND',
  'TRACKED_AFTER_STOP',
  'TRACKED_AFTER_DELETE',
  'CLEANUP_REQUESTED',
  'CLEANING',
  'CLEANUP_FAILED',
] as const;

function normalizedResourceName(name: string): string {
  return name.normalize('NFKC').trim().toLocaleLowerCase('en-US');
}

function resourceSummary(row: ResourceRow): PgResourceSummary {
  if (row.database_name === null) {
    throw new PgResourceManagerError('PG_RESOURCE_INVALID');
  }
  return {
    dataSourceId: row.data_source_id,
    databaseName: row.database_name,
    id: row.id,
    lifecycleState: row.lifecycle_state,
    name: row.normalized_name,
    ownershipMode: row.ownership_mode,
    resourceType: row.resource_type,
    stableIdentity: row.stable_identity,
    taskId: row.task_id,
  };
}

@Injectable()
export class PgResourceManager {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
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

  async bind(
    actor: UserSummary,
    request: BindPgResourceRequest,
    transactionClient?: PoolClient,
  ): Promise<PgResourceSummary> {
    const databaseName = request.databaseName.trim();
    const normalizedName = normalizedResourceName(request.name);
    const stableIdentity = request.stableIdentity.trim();
    if (
      databaseName.length === 0 ||
      normalizedName.length === 0 ||
      stableIdentity.length === 0 ||
      !['PG_SLOT', 'PG_PUBLICATION'].includes(request.resourceType) ||
      (request.platformCreated === true && request.ownershipMode !== 'PLATFORM_MANAGED')
    ) {
      throw new PgResourceManagerError('PG_RESOURCE_INVALID');
    }
    try {
      const bindWithin = async (client: PoolClient) => {
        const source = await client.query<{
          configured_database: string | null;
          source_type: string;
        }>(
          `SELECT source_type, configuration ->> 'database' AS configured_database
             FROM ape_dts_platform.data_sources
            WHERE id = $1::uuid
              AND state = 'ACTIVE'
              AND deleted_at IS NULL
            FOR KEY SHARE`,
          [request.dataSourceId],
        );
        const sourceRow = source.rows[0];
        if (
          sourceRow === undefined ||
          sourceRow.source_type !== 'POSTGRES' ||
          sourceRow.configured_database !== databaseName
        ) {
          throw new PgResourceManagerError('PG_RESOURCE_INVALID');
        }
        const task = await client.query<{ source_data_source_id: string }>(
          `SELECT source_data_source_id
             FROM ape_dts_platform.tasks
            WHERE id = $1::uuid
              AND deleted_at IS NULL
            FOR KEY SHARE`,
          [request.taskId],
        );
        if (task.rows[0]?.source_data_source_id !== request.dataSourceId) {
          throw new PgResourceManagerError('PG_RESOURCE_INVALID');
        }
        const existing = await client.query<ResourceRow>(
          `SELECT id, task_id, data_source_id, resource_type, normalized_name,
                  database_name, ownership_mode, stable_identity, lifecycle_state
             FROM ape_dts_platform.external_resources
            WHERE data_source_id = $1::uuid
              AND resource_type = $2
              AND database_name = $3
              AND normalized_name = $4
              AND lifecycle_state IN ('BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
                                      'CLEANUP_REQUESTED', 'CLEANING', 'CLEANUP_FAILED')
            FOR UPDATE`,
          [request.dataSourceId, request.resourceType, databaseName, normalizedName],
        );
        const current = existing.rows[0];
        if (current !== undefined) {
          if (
            current.task_id !== request.taskId ||
            current.stable_identity !== stableIdentity ||
            current.ownership_mode !== request.ownershipMode
          ) {
            throw new PgResourceManagerError('PG_RESOURCE_CONFLICT');
          }
          return resourceSummary(current);
        }

        const taskResource = await client.query<ResourceRow>(
          `SELECT id, task_id, data_source_id, resource_type, normalized_name,
                  database_name, ownership_mode, stable_identity, lifecycle_state
             FROM ape_dts_platform.external_resources
            WHERE task_id = $1::uuid
              AND resource_type = $2
              AND lifecycle_state IN ('BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
                                      'CLEANUP_REQUESTED', 'CLEANING', 'CLEANUP_FAILED')
            FOR UPDATE`,
          [request.taskId, request.resourceType],
        );
        if (taskResource.rows[0] !== undefined) {
          throw new PgResourceManagerError('PG_RESOURCE_CONFLICT');
        }

        const inserted = await client.query<ResourceRow>(
          `INSERT INTO ape_dts_platform.external_resources (
             id, task_id, data_source_id, resource_type, normalized_name,
             database_name, ownership_mode, stable_identity, lifecycle_state,
             platform_created, created_by_user_id
           ) VALUES (
             $1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
             $7, $8, 'BOUND', $9, $10::uuid
           )
           RETURNING id, task_id, data_source_id, resource_type, normalized_name,
                     database_name, ownership_mode, stable_identity, lifecycle_state`,
          [
            randomUUID(),
            request.taskId,
            request.dataSourceId,
            request.resourceType,
            normalizedName,
            databaseName,
            request.ownershipMode,
            stableIdentity,
            request.platformCreated === true,
            actor.id,
          ],
        );
        const row = inserted.rows[0];
        if (row === undefined) {
          throw new PgResourceManagerError('PG_RESOURCE_NOT_FOUND');
        }
        await this.audit.append(client, {
          action: 'PG_RESOURCE_BOUND',
          actorType: 'USER',
          actorUserId: actor.id,
          details: {
            ownershipMode: request.ownershipMode,
            resourceType: request.resourceType,
            platformCreated: request.platformCreated === true,
          },
          subjectId: request.taskId,
          subjectType: 'TASK',
        });
        return resourceSummary(row);
      };
      return transactionClient === undefined
        ? await this.transaction(bindWithin)
        : await bindWithin(transactionClient);
    } catch (error) {
      if (error instanceof PgResourceManagerError) {
        throw error;
      }
      if (
        typeof error === 'object' &&
        error !== null &&
        'code' in error &&
        error.code === '23505'
      ) {
        throw new PgResourceManagerError('PG_RESOURCE_CONFLICT');
      }
      throw error;
    }
  }

  static readonly activeLifecycleStates = ACTIVE_RESOURCE_STATES;
}
