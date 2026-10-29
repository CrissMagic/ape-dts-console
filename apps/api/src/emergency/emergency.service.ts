import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';

import { EmergencyStopCodeService } from './emergency-stop-code.service.js';
import { EmergencyPendingEventQueue } from './emergency-pending-events.js';
import {
  EMERGENCY_METADATA_STATE_PORT,
  EMERGENCY_RUNTIME_PORT,
  EMERGENCY_STRUCTURED_LOG_PORT,
  type EmergencyMetadataStatePort,
  type EmergencyRuntimeBinding,
  type EmergencyRuntimePort,
  type EmergencyRuntimeTask,
  type EmergencyStructuredLogEvent,
  type EmergencyStructuredLogPort,
} from './emergency-ports.js';

export interface EmergencyStopTask {
  readonly emergencyCode: string;
  readonly taskId: string;
}

export interface EmergencyForceKillTask extends EmergencyStopTask {
  readonly confirmed: true;
  readonly reason: string;
}

export type EmergencyErrorCode =
  | 'EMERGENCY_ACCESS_DENIED'
  | 'EMERGENCY_FORCE_CONFIRMATION_REQUIRED'
  | 'EMERGENCY_FORCE_REASON_REQUIRED'
  | 'EMERGENCY_METADATA_AVAILABLE'
  | 'EMERGENCY_TASK_NOT_RUNNING'
  | 'EMERGENCY_TERMINATION_UNCONFIRMED';

export class EmergencyError extends Error {
  constructor(readonly code: EmergencyErrorCode) {
    super(code);
    this.name = 'EmergencyError';
  }
}

@Injectable()
export class EmergencyService {
  constructor(
    private readonly access: EmergencyStopCodeService,
    @Inject(EMERGENCY_RUNTIME_PORT) private readonly runtime: EmergencyRuntimePort,
    @Inject(EMERGENCY_METADATA_STATE_PORT) private readonly metadata: EmergencyMetadataStatePort,
    @Inject(EMERGENCY_STRUCTURED_LOG_PORT) private readonly log: EmergencyStructuredLogPort,
    private readonly pending: EmergencyPendingEventQueue,
  ) {}

  list(emergencyCode: string): readonly EmergencyRuntimeTask[] {
    this.assertAvailableAndAuthorized(emergencyCode);
    return this.runtime.listActiveTasks().map(({ runId, taskId, unitCount }) => ({
      runId,
      taskId,
      unitCount,
    }));
  }

  async stop(request: EmergencyStopTask): Promise<void> {
    this.assertAvailableAndAuthorized(request.emergencyCode);
    const task = this.requireRunning(request.taskId);
    this.record(this.event('EMERGENCY_STOP_REQUESTED', task, null));
    await this.runtime.sendSigint(request.taskId);
    this.record(this.event('EMERGENCY_STOP_CONFIRMED', task, null));
  }

  async forceKill(request: EmergencyForceKillTask): Promise<void> {
    this.assertAvailableAndAuthorized(request.emergencyCode);
    if (request.confirmed !== true) {
      throw new EmergencyError('EMERGENCY_FORCE_CONFIRMATION_REQUIRED');
    }
    const reason = request.reason.trim();
    if (reason.length === 0) {
      throw new EmergencyError('EMERGENCY_FORCE_REASON_REQUIRED');
    }
    const task = this.requireRunning(request.taskId);
    this.record(this.event('EMERGENCY_FORCE_KILL_REQUESTED', task, reason));
    await this.runtime.forceKill(request.taskId);
    this.record(this.event('EMERGENCY_FORCE_KILL_CONFIRMED', task, reason));
  }

  private assertAvailableAndAuthorized(code: string): void {
    if (!this.metadata.isMetadataUnavailable()) {
      throw new EmergencyError('EMERGENCY_METADATA_AVAILABLE');
    }
    if (!this.access.verify(code)) {
      throw new EmergencyError('EMERGENCY_ACCESS_DENIED');
    }
  }

  private requireRunning(taskId: string): EmergencyRuntimeBinding {
    const task = this.runtime.listActiveTasks().find((candidate) => candidate.taskId === taskId);
    if (task === undefined) {
      throw new EmergencyError('EMERGENCY_TASK_NOT_RUNNING');
    }
    return task;
  }

  private event(
    action: EmergencyStructuredLogEvent['action'],
    task: EmergencyRuntimeBinding,
    reason: string | null,
  ): EmergencyStructuredLogEvent {
    return {
      action,
      details: { runId: task.runId, unitCount: task.unitCount },
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      operationId: task.operationId,
      platformInstanceId: task.platformInstanceId,
      processBindingHash: task.processBindingHash,
      reason,
      revisionId: task.revisionId,
      taskId: task.taskId,
    };
  }

  private record(event: EmergencyStructuredLogEvent): void {
    this.log.write(event);
    this.pending.enqueue(event);
  }
}
