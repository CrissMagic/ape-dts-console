import { describe, expect, it, vi } from 'vitest';
import type { UserSummary } from '@scs/contracts';

import { RuntimeReadNotFoundError, RuntimeReadService } from './runtime-read.service.js';

const actor: UserSummary = {
  displayName: 'Developer',
  id: '33333333-3333-4333-8333-333333333333',
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: 'developer',
};
const taskId = '11111111-1111-4111-8111-111111111111';
const runId = '22222222-2222-4222-8222-222222222222';
const date = new Date('2026-09-08T00:00:00.000Z');

function service(responses: unknown[]) {
  const query = vi.fn(async (sql: string, parameters?: readonly unknown[]) => {
    void sql;
    void parameters;
    return responses.shift();
  });
  return { query, service: new RuntimeReadService({ requirePool: () => ({ query }) } as never) };
}

describe('RuntimeReadService', () => {
  it('lists retained runs without restricting history to the metrics seven-day window', async () => {
    const old = new Date('2026-08-01T00:00:00Z');
    const context = service([
      { rowCount: 1, rows: [{ id: taskId }] },
      { rows: [{ id: runId, status: 'STOPPED', started_at: old, ended_at: date }] },
    ]);
    await expect(context.service.listRuns(actor, taskId)).resolves.toEqual({
      runs: [
        { id: runId, status: 'STOPPED', startedAt: old.toISOString(), endedAt: date.toISOString() },
      ],
    });
  });

  it('does not disclose run history for a deleted or inaccessible task', async () => {
    const context = service([{ rowCount: 0, rows: [] }]);
    await expect(context.service.listRuns(actor, taskId)).rejects.toBeInstanceOf(
      RuntimeReadNotFoundError,
    );
    expect(context.query).toHaveBeenCalledTimes(1);
  });

  it('hides deleted or unauthorized tasks using the same two-data-source grant rule', async () => {
    const context = service([{ rowCount: 0, rows: [] }]);
    await expect(context.service.get(actor, taskId)).rejects.toBeInstanceOf(
      RuntimeReadNotFoundError,
    );
    expect(String(context.query.mock.calls[0]?.[0])).toContain('grant_row.revoked_at IS NULL');
  });

  it('returns an explicit empty runtime without inventing a run', async () => {
    const context = service([{ rowCount: 1, rows: [{ id: taskId }] }, { rows: [] }, { rows: [] }]);
    await expect(context.service.get(actor, taskId)).resolves.toEqual({
      operation: null,
      retryable: false,
      run: null,
      stages: [],
      taskId,
      units: [],
    });
  });

  it('preserves failed-stage facts, null total, and database ordering', async () => {
    const context = service([
      { rowCount: 1, rows: [{ id: taskId }] },
      {
        rows: [
          {
            id: runId,
            operation_id: '44444444-4444-4444-8444-444444444444',
            current_stage: 'SNAPSHOT',
            status: 'FAILED',
            started_at: date,
            ended_at: date,
            failure_code: 'SNAPSHOT_FAILED',
            failure_summary: 'source failed',
            log_recovery_state: 'DURABLE_PENDING',
            log_storage_state: 'DEGRADED',
          },
        ],
      },
      {
        rows: [
          {
            action: 'START',
            completed_at: date,
            created_at: date,
            failure_code: 'RETRY',
            failure_summary: 'retryable',
            id: '44444444-4444-4444-8444-444444444444',
            run_id: runId,
            state: 'FAILED_RETRYABLE',
            updated_at: date,
          },
        ],
      },
      {
        rows: [
          {
            progress_completed: '7',
            ended_at: date,
            failure_code: 'READ_FAILED',
            failure_summary: 'read failed',
            heartbeat_at: date,
            stage_order: 2,
            started_at: date,
            status: 'FAILED',
            throughput: null,
            progress_total: null,
            stage_type: 'SNAPSHOT',
          },
        ],
      },
      {
        rows: [
          {
            ended_at: date,
            exit_code: 1,
            exit_signal: null,
            failure_code: 'EXIT_1',
            failure_summary: 'failed',
            started_at: date,
            status: 'FAILED',
            unit_key: 'partition:1',
          },
        ],
      },
    ]);
    const result = await context.service.get(actor, taskId);
    expect(result.retryable).toBe(true);
    expect(result.run).toMatchObject({
      logRecoveryState: 'DURABLE_PENDING',
      logStorageState: 'DEGRADED',
    });
    expect(result.stages[0]).toMatchObject({
      order: 2,
      retryable: true,
      total: null,
      failureCode: 'READ_FAILED',
    });
    expect(String(context.query.mock.calls[3]?.[0])).toContain('ORDER BY stage_order');
    expect(String(context.query.mock.calls[4]?.[0])).toContain('ORDER BY unit_key');
  });
});
