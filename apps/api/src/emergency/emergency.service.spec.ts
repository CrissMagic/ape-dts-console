import { describe, expect, it, vi } from 'vitest';

import { EmergencyPendingEventQueue } from './emergency-pending-events.js';
import type {
  EmergencyMetadataStatePort,
  EmergencyRuntimePort,
  EmergencyStructuredLogPort,
} from './emergency-ports.js';
import { EmergencyService } from './emergency.service.js';
import { EmergencyStopCodeService } from './emergency-stop-code.service.js';

function fixture(metadataUnavailable = true) {
  const access = new EmergencyStopCodeService();
  access.activate();
  const emergencyCode = access.consumePlaintextOnce()!;
  const runtime = {
    forceKill: vi.fn(async () => undefined),
    listActiveTasks: vi.fn(() => [
      {
        operationId: 'operation-1',
        platformInstanceId: 'instance-1',
        processBindingHash: 'a'.repeat(64),
        runId: 'run-1',
        revisionId: 'revision-1',
        taskId: 'task-1',
        unitCount: 2,
      },
    ]),
    sendSigint: vi.fn(async () => undefined),
  } satisfies EmergencyRuntimePort;
  const metadata = {
    isMetadataUnavailable: vi.fn(() => metadataUnavailable),
  } satisfies EmergencyMetadataStatePort;
  const log = { write: vi.fn() } satisfies EmergencyStructuredLogPort;
  const pending = new EmergencyPendingEventQueue(4, log);
  const service = new EmergencyService(access, runtime, metadata, log, pending);
  return { emergencyCode, log, pending, runtime, service };
}

describe('EmergencyService', () => {
  it('lists only the runtime port in-memory truth while metadata is unavailable', () => {
    const { emergencyCode, runtime, service } = fixture();

    expect(service.list(emergencyCode)).toEqual([
      { runId: 'run-1', taskId: 'task-1', unitCount: 2 },
    ]);
    expect(runtime.listActiveTasks).toHaveBeenCalledOnce();
  });

  it('rejects the emergency surface while metadata is available', () => {
    const { emergencyCode, service } = fixture(false);

    expect(() => service.list(emergencyCode)).toThrow('EMERGENCY_METADATA_AVAILABLE');
  });

  it('writes and queues the emergency warning before sending SIGINT', async () => {
    const { emergencyCode, log, pending, runtime, service } = fixture();
    const order: string[] = [];
    log.write.mockImplementation(() => order.push('log'));
    runtime.sendSigint.mockImplementation(async () => {
      order.push('sigint');
    });

    await service.stop({ emergencyCode, taskId: 'task-1' });

    expect(order).toEqual(['log', 'sigint', 'log']);
    expect(pending.snapshot()).toEqual([
      expect.objectContaining({
        action: 'EMERGENCY_STOP_REQUESTED',
        details: { runId: 'run-1', unitCount: 2 },
        processBindingHash: 'a'.repeat(64),
        taskId: 'task-1',
      }),
      expect.objectContaining({
        action: 'EMERGENCY_STOP_CONFIRMED',
        details: { runId: 'run-1', unitCount: 2 },
        processBindingHash: 'a'.repeat(64),
        taskId: 'task-1',
      }),
    ]);
  });

  it('requires explicit confirmation and a reason before force kill', async () => {
    const { emergencyCode, runtime, service } = fixture();

    await expect(
      service.forceKill({
        confirmed: false,
        emergencyCode,
        reason: 'incident',
        taskId: 'task-1',
      } as never),
    ).rejects.toThrow('EMERGENCY_FORCE_CONFIRMATION_REQUIRED');
    await expect(
      service.forceKill({ confirmed: true, emergencyCode, reason: '   ', taskId: 'task-1' }),
    ).rejects.toThrow('EMERGENCY_FORCE_REASON_REQUIRED');
    expect(runtime.forceKill).not.toHaveBeenCalled();
  });

  it('rejects stale task ids rather than consulting database history', async () => {
    const { emergencyCode, runtime, service } = fixture();
    runtime.listActiveTasks.mockReturnValue([]);

    await expect(service.stop({ emergencyCode, taskId: 'old-task' })).rejects.toThrow(
      'EMERGENCY_TASK_NOT_RUNNING',
    );
  });
});
