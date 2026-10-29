import { describe, expect, it, vi } from 'vitest';
import type { UserSummary } from '@scs/contracts';
import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { TasksService } from './tasks.service.js';

const actor: UserSummary = {
  id: 'user',
  username: 'admin',
  displayName: 'Admin',
  status: 'ACTIVE',
  role: 'ADMIN',
};
function fixture(count = 100) {
  const now = new Date();
  const tasks = Array.from({ length: count }, (_, index) => ({
    id: `task-${index}`,
    display_name: `Task ${index}`,
    task_type: 'PG_KAFKA',
    source_data_source_id: 'source',
    target_data_source_id: 'target',
    current_draft_revision_id: `draft-${index}`,
    current_applied_revision_id: `applied-${index}`,
    desired_state: 'STOPPED',
    observed_state: 'STOPPED',
    observability_health: 'UNAVAILABLE',
    recovery_disposition: 'NOT_APPLICABLE',
    group_id: null,
    row_version: '1',
    created_at: now,
    updated_at: now,
    deleted_at: null,
  }));
  const revisions = tasks.flatMap((task) =>
    ['draft', 'applied'].map((kind) => ({
      id: `${kind}-${task.id.slice(5)}`,
      task_id: task.id,
      artifact_id: `artifact-${kind}`,
      configuration: { marker: `${task.id}-${kind}` },
      configuration_schema_version: 1,
      state: kind === 'draft' ? 'READY_DRAFT' : 'APPLIED',
      revision_number: '1',
      draft_version: '1',
      created_at: now,
      updated_at: now,
    })),
  );
  const sources = [
    { id: 'source', source_type: 'POSTGRES' },
    { id: 'target', source_type: 'KAFKA' },
  ];
  const grants = [{ data_source_id: 'source' }, { data_source_id: 'target' }];
  const query = vi.fn(async (sql: string) => {
    if (sql.includes('FROM ape_dts_platform.tasks')) return { rows: tasks };
    if (sql.includes('FROM ape_dts_platform.data_sources')) return { rows: sources };
    if (sql.includes('FROM ape_dts_platform.data_source_grants')) return { rows: grants };
    if (sql.includes('FROM ape_dts_platform.task_revisions')) return { rows: revisions };
    throw new Error('Unexpected query');
  });
  const release = vi.fn();
  const service = new TasksService(
    {
      requirePool: () => ({ connect: async () => ({ query, release }) }),
    } as unknown as PlatformDatabaseService,
    {} as AuditLedgerService,
  );
  return { service, query, release, tasks, revisions, sources, grants };
}

describe('task list batch loading', () => {
  it('loads 100 task summaries with both bound revisions in three queries and preserves order', async () => {
    const f = fixture();
    const result = await f.service.list(actor);
    expect(result).toHaveLength(100);
    expect(result.map((task) => task.id)).toEqual(f.tasks.map((task) => task.id));
    for (const [index, task] of result.entries()) {
      expect(task.draft).toMatchObject({
        id: `draft-${index}`,
        configuration: { marker: `task-${index}-draft` },
      });
      expect(task.applied).toMatchObject({
        id: `applied-${index}`,
        artifactId: 'artifact-applied',
      });
    }
    expect(f.query).toHaveBeenCalledTimes(3);
    expect(f.release).toHaveBeenCalledOnce();
  });

  it('requires both source and target grants for a developer without per-task queries', async () => {
    const f = fixture();
    const developer = { ...actor, role: 'DEVELOPER' as const };
    expect(await f.service.list(developer)).toHaveLength(100);
    expect(f.query).toHaveBeenCalledTimes(4);
    f.grants.pop();
    f.query.mockClear();
    expect(await f.service.list(developer)).toEqual([]);
    expect(f.query).toHaveBeenCalledTimes(3);
  });

  it('does not attach a foreign task revision even if an inconsistent pointer references it', async () => {
    const f = fixture(2);
    f.tasks[0]!.current_draft_revision_id = 'draft-1';
    const result = await f.service.list(actor);
    expect(result[0]!.draft).toBeNull();
    expect(result[1]!.draft?.id).toBe('draft-1');
  });

  it.each(['missing', 'wrong type'])('preserves errors for a %s data source', async (reason) => {
    const f = fixture(1);
    if (reason === 'missing') f.sources.pop();
    else f.sources[0]!.source_type = 'MYSQL';
    await expect(f.service.list(actor)).rejects.toMatchObject({
      code: reason === 'missing' ? 'TASK_DATA_SOURCE_NOT_FOUND' : 'TASK_DATA_SOURCE_TYPE_INVALID',
    });
    expect(f.release).toHaveBeenCalledOnce();
  });

  it('does not fetch sources or revisions for an empty list', async () => {
    const f = fixture(0);
    expect(await f.service.list(actor)).toEqual([]);
    expect(f.query).toHaveBeenCalledOnce();
  });
});
