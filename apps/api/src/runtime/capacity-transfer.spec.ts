import { describe, expect, it, vi } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import {
  CapacityReservationError,
  CapacityReservationService,
} from '../pg-resources/capacity-reservation.service.js';

const binding = {
  expectedTaskRowVersion: '8',
  newRunId: '22222222-2222-4222-8222-222222222222',
  oldProcessToken: '33333333-3333-4333-8333-333333333333',
  oldRunId: '11111111-1111-4111-8111-111111111111',
  platformInstanceId: '44444444-4444-4444-8444-444444444444',
  taskId: '55555555-5555-4555-8555-555555555555',
  taskRevisionId: '66666666-6666-4666-8666-666666666666',
};

describe('CapacityReservationService transferWithin', () => {
  it('binds transfer to old/new run, task revision, instance, process token and user intent', async () => {
    const query = vi.fn(async (...parameters: readonly unknown[]) => {
      void parameters;
      return { rowCount: 2, rows: [] };
    });
    const service = new CapacityReservationService({} as PlatformDatabaseService);

    await expect(service.transferWithin({ query } as never, binding, 2)).resolves.toBe(2);

    const normalized = String(query.mock.calls[0]?.[0]).replace(/\s+/gu, ' ');
    expect(normalized).toContain("task.desired_state = 'RUNNING'");
    expect(normalized).toContain('task.row_version = $5::bigint');
    expect(normalized).toContain('task.current_applied_revision_id = reservation.task_revision_id');
    expect(normalized).toContain('old_run.platform_instance_id = $6::uuid');
    expect(normalized).toContain('old_unit.process_token = $7::text');
    expect(normalized).toContain("old_run.status = 'FAILED'");
    expect(normalized).toContain("new_run.status = 'STARTING'");
    expect(normalized).toContain('NOT EXISTS');
    expect(query.mock.calls[0]?.[1]).toEqual([
      binding.oldRunId,
      binding.newRunId,
      binding.taskId,
      binding.taskRevisionId,
      binding.expectedTaskRowVersion,
      binding.platformInstanceId,
      binding.oldProcessToken,
    ]);
  });

  it('fails atomically when every expected unit was not transferred', async () => {
    const query = vi.fn(async (...parameters: readonly unknown[]) => {
      void parameters;
      return { rowCount: 1, rows: [] };
    });
    const service = new CapacityReservationService({} as PlatformDatabaseService);

    await expect(service.transferWithin({ query } as never, binding, 2)).rejects.toEqual(
      new CapacityReservationError('CAPACITY_ALREADY_RESERVED', { requested: 2, used: 1 }),
    );
  });
});
