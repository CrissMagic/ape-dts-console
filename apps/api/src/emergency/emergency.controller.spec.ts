import {
  BadRequestException,
  ConflictException,
  NotFoundException,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';

import { METADATA_AVAILABILITY_BYPASS } from '../metadata-availability/metadata-availability.decorator.js';
import { EmergencyController } from './emergency.controller.js';
import { EmergencyError } from './emergency.service.js';

function fixture() {
  const service = {
    forceKill: vi.fn(async () => undefined),
    list: vi.fn(() => [{ runId: 'run-1', taskId: 'task-1', unitCount: 1 }]),
    stop: vi.fn(async () => undefined),
  };
  return { controller: new EmergencyController(service as never), service };
}

describe('EmergencyController', () => {
  it('bypasses the global metadata guard and delegates list auth only through the header', () => {
    const { controller, service } = fixture();

    expect(Reflect.getMetadata(METADATA_AVAILABILITY_BYPASS, EmergencyController)).toBe(true);
    expect(controller.list('emergency-code')).toEqual([
      { runId: 'run-1', taskId: 'task-1', unitCount: 1 },
    ]);
    expect(service.list).toHaveBeenCalledWith('emergency-code');
  });

  it('accepts no business body for graceful stop', async () => {
    const { controller, service } = fixture();

    await expect(controller.stop('task-1', 'emergency-code')).resolves.toEqual({
      status: 'stop_requested',
    });
    expect(service.stop).toHaveBeenCalledWith({
      emergencyCode: 'emergency-code',
      taskId: 'task-1',
    });
  });

  it('requires an exact confirmed/reason force-kill body', async () => {
    const { controller, service } = fixture();

    await expect(
      controller.forceKill('task-1', 'emergency-code', {
        confirmed: true,
        extra: 'not-allowed',
        reason: 'incident containment',
      }),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      controller.forceKill('task-1', 'emergency-code', {
        confirmed: false,
        reason: 'incident containment',
      }),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(service.forceKill).not.toHaveBeenCalled();
  });

  it('passes the trimmed force-kill reason to the service', async () => {
    const { controller, service } = fixture();

    await expect(
      controller.forceKill('task-1', 'emergency-code', {
        confirmed: true,
        reason: '  incident containment  ',
      }),
    ).resolves.toEqual({ status: 'force_kill_requested' });
    expect(service.forceKill).toHaveBeenCalledWith({
      confirmed: true,
      emergencyCode: 'emergency-code',
      reason: 'incident containment',
      taskId: 'task-1',
    });
  });

  it.each([
    ['EMERGENCY_ACCESS_DENIED', UnauthorizedException],
    ['EMERGENCY_TASK_NOT_RUNNING', NotFoundException],
    ['EMERGENCY_METADATA_AVAILABLE', ConflictException],
    ['EMERGENCY_TERMINATION_UNCONFIRMED', ServiceUnavailableException],
  ] as const)('maps %s to an explicit HTTP status', async (code, exceptionType) => {
    const { controller, service } = fixture();
    service.stop.mockRejectedValueOnce(new EmergencyError(code));

    await expect(controller.stop('task-1', 'code')).rejects.toBeInstanceOf(exceptionType);
  });
});
