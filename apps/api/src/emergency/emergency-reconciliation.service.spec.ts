import { describe, expect, it, vi } from 'vitest';

import { EmergencyPendingEventQueue } from './emergency-pending-events.js';
import { EmergencyReconciliationService } from './emergency-reconciliation.service.js';

const IDS = {
  event: '00000000-0000-4000-8000-000000000001',
  instance: '00000000-0000-4000-8000-000000000002',
  operation: '00000000-0000-4000-8000-000000000003',
  revision: '00000000-0000-4000-8000-000000000004',
  run: '00000000-0000-4000-8000-000000000005',
  task: '00000000-0000-4000-8000-000000000006',
};

function fixture(options: { eventExists?: boolean; taskMatches?: boolean } = {}) {
  const sql: string[] = [];
  const query = vi.fn(async (statement: string) => {
    sql.push(statement);
    if (statement.includes('SELECT 1 FROM ape_dts_platform.emergency_runtime_events')) {
      return { rowCount: options.eventExists ? 1 : 0, rows: options.eventExists ? [{}] : [] };
    }
    if (statement.includes('UPDATE ape_dts_platform.tasks AS task')) {
      return { rowCount: options.taskMatches === false ? 0 : 1, rows: [] };
    }
    if (statement.includes('FROM ape_dts_platform.platform_instances AS instance')) {
      return { rowCount: 0, rows: [] };
    }
    return { rowCount: 1, rows: [] };
  });
  const client = { query, release: vi.fn() };
  const database = { requirePool: () => ({ connect: async () => client }) };
  const audit = { append: vi.fn(async () => '41') };
  const availability = {
    acknowledgeOutageFact: vi.fn(),
    pendingOutageFact: vi.fn(() => null),
    registerReconciler: vi.fn(() => vi.fn()),
  };
  const runtime = { reconcilePersistedRuns: vi.fn(async () => 0) };
  const pending = new EmergencyPendingEventQueue(2, { write: vi.fn() });
  const service = new EmergencyReconciliationService(
    availability as never,
    database as never,
    audit as never,
    { getStatus: () => ({ platformInstanceId: IDS.instance }) } as never,
    pending,
    runtime as never,
  );
  return { audit, availability, client, pending, query, runtime, service, sql };
}

function enqueue(
  queue: EmergencyPendingEventQueue,
  action: 'EMERGENCY_STOP_CONFIRMED' | 'EMERGENCY_STOP_REQUESTED',
) {
  queue.enqueue({
    action,
    details: { runId: IDS.run, unitCount: 1 },
    eventId: IDS.event,
    occurredAt: '2026-09-08T00:00:00.000Z',
    operationId: IDS.operation,
    platformInstanceId: IDS.instance,
    processBindingHash: 'a'.repeat(64),
    reason: null,
    revisionId: IDS.revision,
    taskId: IDS.task,
  });
}

describe('EmergencyReconciliationService', () => {
  it('atomically persists CONFIRMED, converges the precisely bound run and only then acknowledges', async () => {
    const context = fixture();
    enqueue(context.pending, 'EMERGENCY_STOP_CONFIRMED');

    await context.service.reconcile();

    const taskUpdate = context.sql.find((statement) =>
      statement.includes('UPDATE ape_dts_platform.tasks AS task'),
    )!;
    expect(taskUpdate).toContain("task.desired_state = 'RUNNING'");
    expect(taskUpdate).toContain('target_run.task_revision_id = $3::uuid');
    expect(taskUpdate).toContain('target_run.operation_id = $4::uuid');
    expect(taskUpdate).toContain('target_run.platform_instance_id = $5::uuid');
    expect(taskUpdate).toContain('target_run.process_binding_hash = $6');
    expect(taskUpdate).toContain('newer_run.id <> $2::uuid');
    expect(taskUpdate).toContain('newer_operation.id <> $4::uuid');
    expect(taskUpdate).toContain(
      "newer_operation.state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')",
    );
    expect(
      context.sql.some((statement) => statement.includes('UPDATE ape_dts_platform.task_run_units')),
    ).toBe(true);
    expect(
      context.sql.some((statement) =>
        statement.includes('UPDATE ape_dts_platform.capacity_reservations'),
      ),
    ).toBe(true);
    expect(context.audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actorType: 'SYSTEM' }),
    );
    expect(context.runtime.reconcilePersistedRuns).toHaveBeenCalledOnce();
    expect(context.pending.snapshot()).toEqual([]);
    const instanceQuery = context.sql.find((statement) =>
      statement.includes('FROM ape_dts_platform.platform_instances AS instance'),
    );
    expect(instanceQuery).toContain('instance.ended_at IS NULL');
    expect(instanceQuery).toContain('instance.clean_shutdown IS DISTINCT FROM TRUE');
    expect(instanceQuery).not.toContain('stopped_at');
  });

  it('persists REQUESTED as deployment-admin evidence without changing task or run state', async () => {
    const context = fixture();
    enqueue(context.pending, 'EMERGENCY_STOP_REQUESTED');

    await context.service.reconcile();

    expect(
      context.sql.some((statement) => statement.includes('UPDATE ape_dts_platform.tasks AS task')),
    ).toBe(false);
    expect(context.audit.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actorType: 'DEPLOYMENT_ADMIN' }),
    );
  });

  it('records a stale binding without overwriting a newer run or releasing capacity', async () => {
    const context = fixture({ taskMatches: false });
    enqueue(context.pending, 'EMERGENCY_STOP_CONFIRMED');

    await context.service.reconcile();

    expect(
      context.sql.some((statement) => statement.includes('UPDATE ape_dts_platform.task_run_units')),
    ).toBe(false);
    expect(
      context.sql.some((statement) =>
        statement.includes('UPDATE ape_dts_platform.capacity_reservations'),
      ),
    ).toBe(false);
    expect(context.query).toHaveBeenCalledWith(
      expect.stringContaining('emergency_runtime_events'),
      expect.arrayContaining(['STALE_OR_BINDING_MISMATCH', '41']),
    );
  });

  it('is idempotent by event_id and does not append a second audit event', async () => {
    const context = fixture({ eventExists: true });
    enqueue(context.pending, 'EMERGENCY_STOP_CONFIRMED');

    await context.service.reconcile();

    expect(context.audit.append).not.toHaveBeenCalled();
    expect(context.pending.snapshot()).toEqual([]);
  });

  it('does not acknowledge events or overflow gaps when a later reconciliation step fails', async () => {
    const context = fixture();
    const overflowQueue = new EmergencyPendingEventQueue(1, { write: vi.fn() });
    const overflowService = new EmergencyReconciliationService(
      context.availability as never,
      { requirePool: () => ({ connect: async () => context.client }) } as never,
      context.audit as never,
      { getStatus: () => ({ platformInstanceId: IDS.instance }) } as never,
      overflowQueue,
      context.runtime as never,
    );
    enqueue(overflowQueue, 'EMERGENCY_STOP_REQUESTED');
    expect(
      overflowQueue.enqueue({
        action: 'EMERGENCY_STOP_REQUESTED',
        eventId: 'overflow',
        occurredAt: '2026-09-08T00:00:01.000Z',
        reason: null,
        taskId: IDS.task,
      }),
    ).toBe(false);
    context.runtime.reconcilePersistedRuns.mockRejectedValueOnce(new Error('database failed'));

    await expect(overflowService.reconcile()).rejects.toThrow('database failed');

    expect(overflowQueue.snapshot()).toHaveLength(1);
    expect(overflowQueue.gapFact()).toEqual({ eventId: expect.any(String), omittedEvents: 1 });
  });
});
