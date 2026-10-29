import { createHash } from 'node:crypto';

import { Injectable } from '@nestjs/common';

import {
  RuntimeSupervisorError,
  RuntimeSupervisorService,
} from '../runtime/runtime-supervisor.service.js';
import type { EmergencyRuntimeBinding, EmergencyRuntimePort } from './emergency-ports.js';
import { EmergencyError } from './emergency.service.js';

@Injectable()
export class EmergencyRuntimeAdapter implements EmergencyRuntimePort {
  constructor(private readonly runtime: RuntimeSupervisorService) {}

  listActiveTasks(): readonly EmergencyRuntimeBinding[] {
    return this.runtime.listActiveRuntimeTasks().map((task) => ({
      operationId: task.operationId,
      platformInstanceId: task.platformInstanceId,
      processBindingHash: createHash('sha256')
        .update(
          task.units
            .map((unit) => `${unit.unitId}:${unit.processToken}`)
            .sort((left, right) => left.localeCompare(right))
            .join('\n'),
          'utf8',
        )
        .digest('hex'),
      runId: task.runId,
      revisionId: task.revisionId,
      taskId: task.taskId,
      unitCount: task.units.length,
    }));
  }

  async sendSigint(taskId: string): Promise<void> {
    await this.terminate(taskId, 'GRACEFUL');
  }

  async forceKill(taskId: string): Promise<void> {
    await this.terminate(taskId, 'FORCE');
  }

  private async terminate(taskId: string, mode: 'FORCE' | 'GRACEFUL'): Promise<void> {
    try {
      const result = await this.runtime.terminateActiveTask(taskId, mode);
      if (!result.allConfirmed) {
        throw new EmergencyError('EMERGENCY_TERMINATION_UNCONFIRMED');
      }
    } catch (error) {
      if (
        error instanceof RuntimeSupervisorError &&
        error.code === 'RUNTIME_PROCESS_HANDLE_MISSING'
      ) {
        throw new EmergencyError('EMERGENCY_TASK_NOT_RUNNING');
      }
      throw error;
    }
  }
}
