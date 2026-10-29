import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type {
  AcceptTaskRevisionRiskRequest,
  TaskRevisionRiskAcceptance,
  UserSummary,
} from '@scs/contracts';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';

export type RiskAcceptanceServiceErrorCode =
  | 'RISK_ACCEPTANCE_ACCESS_DENIED'
  | 'RISK_ACCEPTANCE_ALREADY_EXISTS'
  | 'RISK_ACCEPTANCE_PREFLIGHT_REQUIRED'
  | 'RISK_ACCEPTANCE_REVISION_NOT_CURRENT'
  | 'RISK_ACCEPTANCE_REVISION_VERSION_CONFLICT'
  | 'RISK_ACCEPTANCE_TASK_NOT_FOUND'
  | 'RISK_ACCEPTANCE_TASK_VERSION_CONFLICT';

export class RiskAcceptanceServiceError extends Error {
  constructor(readonly code: RiskAcceptanceServiceErrorCode) {
    super(code);
    this.name = 'RiskAcceptanceServiceError';
  }
}

interface TaskRow extends QueryResultRow {
  current_draft_revision_id: string | null;
  deleted_at: Date | null;
  id: string;
  row_version: string;
}

interface RevisionRow extends QueryResultRow {
  draft_version: string;
  id: string;
  state: string;
  task_id: string;
}

interface PreflightRow extends QueryResultRow {
  completed_at: Date | null;
  id: string;
  state: string;
}

interface AcceptanceRow extends QueryResultRow {
  accepted_at: Date;
  accepted_by_user_id: string;
  id: string;
  note: string | null;
  task_revision_id: string;
}

@Injectable()
export class RiskAcceptanceService {
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

  async accept(
    actor: UserSummary,
    taskId: string,
    request: AcceptTaskRevisionRiskRequest,
  ): Promise<TaskRevisionRiskAcceptance> {
    if (actor.role !== 'ADMIN') {
      throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_ACCESS_DENIED');
    }

    return await this.transaction(async (client) => {
      const taskResult = await client.query<TaskRow>(
        `SELECT id, current_draft_revision_id, row_version::text, deleted_at
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
          FOR UPDATE`,
        [taskId],
      );
      const task = taskResult.rows[0];
      if (task === undefined || task.deleted_at !== null) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_TASK_NOT_FOUND');
      }
      if (task.row_version !== request.expectedTaskRowVersion) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_TASK_VERSION_CONFLICT');
      }

      const revisionResult = await client.query<RevisionRow>(
        `SELECT id, task_id, state, draft_version::text
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid
            AND task_id = $2::uuid
          FOR UPDATE`,
        [request.revisionId, taskId],
      );
      const revision = revisionResult.rows[0];
      if (revision === undefined || task.current_draft_revision_id !== revision.id) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_REVISION_NOT_CURRENT');
      }
      if (revision.draft_version !== request.expectedDraftVersion) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_REVISION_VERSION_CONFLICT');
      }

      const existing = await client.query<{ id: string }>(
        `SELECT acceptance.id
           FROM ape_dts_platform.risk_acceptances AS acceptance
           JOIN ape_dts_platform.task_revisions AS revision
             ON revision.id = acceptance.task_revision_id
          WHERE revision.task_id = $1::uuid
            AND revision.id = $2::uuid
            AND acceptance.task_revision_id = $2::uuid`,
        [taskId, request.revisionId],
      );
      if (existing.rows[0] !== undefined) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_ALREADY_EXISTS');
      }
      if (revision.state !== 'PREFLIGHT_PASSED') {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_PREFLIGHT_REQUIRED');
      }

      const preflightResult = await client.query<PreflightRow>(
        `SELECT preflight.id, preflight.state, preflight.completed_at
           FROM ape_dts_platform.preflight_runs AS preflight
           JOIN ape_dts_platform.task_revisions AS revision
             ON revision.id = preflight.task_revision_id
          WHERE revision.task_id = $1::uuid
            AND revision.id = $2::uuid
            AND preflight.task_revision_id = $2::uuid
          ORDER BY preflight.started_at DESC, preflight.id DESC
          LIMIT 1
          FOR SHARE OF preflight`,
        [taskId, request.revisionId],
      );
      const preflight = preflightResult.rows[0];
      if (
        preflight === undefined ||
        preflight.state !== 'PASSED' ||
        preflight.completed_at === null
      ) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_PREFLIGHT_REQUIRED');
      }

      const updatedRevision = await client.query(
        `UPDATE ape_dts_platform.task_revisions
            SET state = 'RISK_CONFIRMED',
                row_version = row_version + 1,
                updated_by_user_id = $4::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND task_id = $2::uuid
            AND state = 'PREFLIGHT_PASSED'
            AND draft_version = $3::bigint`,
        [request.revisionId, taskId, request.expectedDraftVersion, actor.id],
      );
      if (updatedRevision.rowCount !== 1) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_REVISION_VERSION_CONFLICT');
      }

      const acceptanceId = randomUUID();
      const inserted = await client.query<AcceptanceRow>(
        `INSERT INTO ape_dts_platform.risk_acceptances (
           id, task_revision_id, accepted_by_user_id, accepted_at, note
         )
         SELECT $1::uuid, revision.id, $4::uuid, statement_timestamp(), $5
           FROM ape_dts_platform.task_revisions AS revision
          WHERE revision.id = $2::uuid
            AND revision.task_id = $3::uuid
            AND revision.state = 'RISK_CONFIRMED'
            AND revision.draft_version = $6::bigint
         RETURNING id, task_revision_id, accepted_by_user_id, accepted_at, note`,
        [
          acceptanceId,
          request.revisionId,
          taskId,
          actor.id,
          request.note ?? null,
          request.expectedDraftVersion,
        ],
      );
      const acceptance = inserted.rows[0];
      if (acceptance === undefined) {
        throw new RiskAcceptanceServiceError('RISK_ACCEPTANCE_REVISION_VERSION_CONFLICT');
      }

      await this.audit.append(client, {
        action: 'TASK_REVISION_RISK_ACCEPTED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: { acceptanceId: acceptance.id, revisionId: acceptance.task_revision_id },
        subjectId: taskId,
        subjectType: 'TASK',
      });

      return {
        acceptedAt: acceptance.accepted_at.toISOString(),
        acceptedByUserId: acceptance.accepted_by_user_id,
        id: acceptance.id,
        note: acceptance.note,
        revisionId: acceptance.task_revision_id,
        taskId,
      };
    });
  }
}
