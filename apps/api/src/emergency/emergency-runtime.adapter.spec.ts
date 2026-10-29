import { describe, expect, it, vi } from 'vitest';
import { createHash } from 'node:crypto';

import { RuntimeSupervisorError } from '../runtime/runtime-supervisor.service.js';
import { EmergencyRuntimeAdapter } from './emergency-runtime.adapter.js';

function fixture() {
  const runtime = {
    listActiveRuntimeTasks: vi.fn(() => [
      {
        operationId: 'operation-1',
        platformInstanceId: 'instance-1',
        runId: 'run-1',
        revisionId: 'revision-1',
        taskId: 'task-1',
        units: [
          { pid: 10, processToken: 'token-1', unitId: 'unit-1' },
          { pid: 11, processToken: 'token-2', unitId: 'unit-2' },
        ],
      },
    ]),
    terminateActiveTask: vi.fn(async () => ({
      allConfirmed: true,
      mode: 'GRACEFUL' as const,
      runId: 'run-1',
      taskId: 'task-1',
      units: [],
    })),
  };
  return { adapter: new EmergencyRuntimeAdapter(runtime as never), runtime };
}

describe('EmergencyRuntimeAdapter', () => {
  it('projects a deterministic binding digest without returning process tokens', () => {
    const { adapter } = fixture();

    const expectedHash = createHash('sha256')
      .update('unit-1:token-1\nunit-2:token-2', 'utf8')
      .digest('hex');
    expect(adapter.listActiveTasks()).toEqual([
      {
        operationId: 'operation-1',
        platformInstanceId: 'instance-1',
        processBindingHash: expectedHash,
        runId: 'run-1',
        revisionId: 'revision-1',
        taskId: 'task-1',
        unitCount: 2,
      },
    ]);
    expect(JSON.stringify(adapter.listActiveTasks())).not.toContain('token-1');
  });

  it('uses graceful and force termination modes explicitly', async () => {
    const { adapter, runtime } = fixture();

    await adapter.sendSigint('task-1');
    await adapter.forceKill('task-1');

    expect(runtime.terminateActiveTask).toHaveBeenNthCalledWith(1, 'task-1', 'GRACEFUL');
    expect(runtime.terminateActiveTask).toHaveBeenNthCalledWith(2, 'task-1', 'FORCE');
  });

  it('rejects an unconfirmed supervisor result', async () => {
    const { adapter, runtime } = fixture();
    runtime.terminateActiveTask.mockResolvedValueOnce({
      allConfirmed: false,
      mode: 'GRACEFUL',
      runId: 'run-1',
      taskId: 'task-1',
      units: [],
    });

    await expect(adapter.sendSigint('task-1')).rejects.toThrow('EMERGENCY_TERMINATION_UNCONFIRMED');
  });

  it('maps a disappeared runtime handle to task-not-running', async () => {
    const { adapter, runtime } = fixture();
    runtime.terminateActiveTask.mockRejectedValueOnce(
      new RuntimeSupervisorError('RUNTIME_PROCESS_HANDLE_MISSING'),
    );

    await expect(adapter.forceKill('task-1')).rejects.toThrow('EMERGENCY_TASK_NOT_RUNNING');
  });
});
