import type { UserSummary } from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { RiskAcceptanceService, RiskAcceptanceServiceError } from './risk-acceptance.service.js';

const taskId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3';
const revisionId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4';
const administrator: UserSummary = {
  displayName: 'Risk administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'risk-admin',
};
const developer: UserSummary = {
  ...administrator,
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f6',
  role: 'DEVELOPER',
  username: 'risk-developer',
};

interface RecordedQuery {
  readonly parameters: readonly unknown[];
  readonly sql: string;
}

interface FakeDatabaseOptions {
  readonly existingAcceptance?: boolean;
  readonly latestPreflightState?: 'FAILED' | 'PASSED' | 'RUNNING';
  readonly revisionFound?: boolean;
  readonly revisionState?: string;
}

function context(options: FakeDatabaseOptions = {}) {
  const queries: RecordedQuery[] = [];
  const release = vi.fn();
  const query = vi.fn(async (sql: string, parameters: readonly unknown[] = []) => {
    queries.push({ parameters, sql });
    const normalized = sql.replace(/\s+/gu, ' ').trim().toLowerCase();
    if (normalized === 'begin' || normalized === 'commit' || normalized === 'rollback') {
      return { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.tasks')) {
      return {
        rows: [
          {
            current_draft_revision_id: revisionId,
            deleted_at: null,
            id: taskId,
            row_version: '3',
          },
        ],
        rowCount: 1,
      };
    }
    if (normalized.startsWith('insert into ape_dts_platform.risk_acceptances')) {
      return {
        rows: [
          {
            accepted_at: new Date('2026-09-07T00:00:01.000Z'),
            accepted_by_user_id: parameters[3],
            id: parameters[0],
            note: parameters[4],
            task_revision_id: revisionId,
          },
        ],
        rowCount: 1,
      };
    }
    if (
      normalized.startsWith('select') &&
      normalized.includes('from ape_dts_platform.task_revisions') &&
      !normalized.includes('join ape_dts_platform.task_revisions')
    ) {
      return options.revisionFound === false
        ? { rows: [], rowCount: 0 }
        : {
            rows: [
              {
                draft_version: '7',
                id: revisionId,
                state: options.revisionState ?? 'PREFLIGHT_PASSED',
                task_id: taskId,
              },
            ],
            rowCount: 1,
          };
    }
    if (normalized.includes('from ape_dts_platform.risk_acceptances')) {
      return options.existingAcceptance === true
        ? { rows: [{ id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f7' }], rowCount: 1 }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.preflight_runs')) {
      const state = options.latestPreflightState ?? 'PASSED';
      return {
        rows: [
          {
            completed_at: state === 'RUNNING' ? null : new Date('2026-09-07T00:00:00.000Z'),
            id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f8',
            state,
          },
        ],
        rowCount: 1,
      };
    }
    if (normalized.startsWith('update ape_dts_platform.task_revisions')) {
      return { rows: [], rowCount: 1 };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  });
  const connect = vi.fn(async () => ({ query, release }));
  const database = { requirePool: () => ({ connect }) } as unknown as PlatformDatabaseService;
  const audit = { append: vi.fn(async () => '1') } as unknown as AuditLedgerService;
  const service = new RiskAcceptanceService(database, audit);
  return { audit, connect, queries, service };
}

function request(note?: string) {
  return {
    expectedDraftVersion: '7',
    expectedTaskRowVersion: '3',
    ...(note === undefined ? {} : { note }),
    revisionId,
  };
}

function normalized(sql: string): string {
  return sql.replace(/\s+/gu, ' ').trim().toLowerCase();
}

describe('RiskAcceptanceService', () => {
  it('binds a user acceptance to the exact current task revision and audits it', async () => {
    const test = context();

    const result = await test.service.accept(administrator, taskId, request('reviewed impact'));

    expect(result).toMatchObject({
      acceptedByUserId: administrator.id,
      note: 'reviewed impact',
      revisionId,
      taskId,
    });
    const update = test.queries.find((entry) =>
      normalized(entry.sql).startsWith('update ape_dts_platform.task_revisions'),
    );
    expect(normalized(update?.sql ?? '')).toContain(
      "where id = $1::uuid and task_id = $2::uuid and state = 'preflight_passed' and draft_version = $3::bigint",
    );
    expect(update?.parameters).toEqual([revisionId, taskId, '7', administrator.id]);
    const insert = test.queries.find((entry) =>
      normalized(entry.sql).startsWith('insert into ape_dts_platform.risk_acceptances'),
    );
    expect(normalized(insert?.sql ?? '')).toContain(
      "where revision.id = $2::uuid and revision.task_id = $3::uuid and revision.state = 'risk_confirmed' and revision.draft_version = $6::bigint",
    );
    expect(insert?.parameters.slice(1)).toEqual([
      revisionId,
      taskId,
      administrator.id,
      'reviewed impact',
      '7',
    ]);
    expect(test.audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: 'TASK_REVISION_RISK_ACCEPTED',
        actorType: 'USER',
        actorUserId: administrator.id,
        details: expect.objectContaining({ revisionId }),
        subjectId: taskId,
        subjectType: 'TASK',
      }),
    );
    const preflight = test.queries.find((entry) =>
      normalized(entry.sql).includes('from ape_dts_platform.preflight_runs'),
    );
    expect(normalized(preflight?.sql ?? '')).toContain(
      'order by preflight.started_at desc, preflight.id desc',
    );
    expect(normalized(insert?.sql ?? '')).toContain('statement_timestamp()');
    expect(insert?.parameters.some((value) => value instanceof Date)).toBe(false);
  });

  it('stores an omitted note as null without generating a default explanation', async () => {
    const test = context();

    const result = await test.service.accept(administrator, taskId, request());

    const insert = test.queries.find((entry) =>
      normalized(entry.sql).startsWith('insert into ape_dts_platform.risk_acceptances'),
    );
    expect(insert?.parameters[4]).toBeNull();
    expect(result.note).toBeNull();
  });

  it('rejects a duplicate revision acceptance without writing or auditing again', async () => {
    const test = context({ existingAcceptance: true, revisionState: 'RISK_CONFIRMED' });

    await expect(test.service.accept(administrator, taskId, request())).rejects.toMatchObject({
      code: 'RISK_ACCEPTANCE_ALREADY_EXISTS',
    });

    expect(
      test.queries.some((entry) =>
        normalized(entry.sql).startsWith('update ape_dts_platform.task_revisions'),
      ),
    ).toBe(false);
    expect(test.audit.append).not.toHaveBeenCalled();
    expect(test.queries.map((entry) => normalized(entry.sql))).toContain('rollback');
  });

  it('does not accept a revision that does not belong to the requested task', async () => {
    const test = context({ revisionFound: false });

    await expect(test.service.accept(administrator, taskId, request())).rejects.toMatchObject({
      code: 'RISK_ACCEPTANCE_REVISION_NOT_CURRENT',
    });

    expect(test.audit.append).not.toHaveBeenCalled();
    expect(
      test.queries.some((entry) =>
        normalized(entry.sql).startsWith('insert into ape_dts_platform.risk_acceptances'),
      ),
    ).toBe(false);
  });

  it('requires the latest preflight for the exact revision to have passed', async () => {
    const test = context({ latestPreflightState: 'RUNNING' });

    await expect(test.service.accept(administrator, taskId, request())).rejects.toMatchObject({
      code: 'RISK_ACCEPTANCE_PREFLIGHT_REQUIRED',
    });

    expect(test.audit.append).not.toHaveBeenCalled();
  });

  it('rejects a developer before opening a database transaction', async () => {
    const test = context();

    await expect(test.service.accept(developer, taskId, request())).rejects.toBeInstanceOf(
      RiskAcceptanceServiceError,
    );

    expect(test.connect).not.toHaveBeenCalled();
    expect(test.audit.append).not.toHaveBeenCalled();
  });
});
