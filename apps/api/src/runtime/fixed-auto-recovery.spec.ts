import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  AUTO_RECOVERY_HEALTHY_RESET_MS,
  AUTO_RECOVERY_RETRY_DELAY_MS,
  FixedAutoRecoveryCoordinator,
  AutoRecoveryManualRequiredError,
  type AutoRecoveryBinding,
  type AutoRecoveryPort,
} from './fixed-auto-recovery.js';

const binding: AutoRecoveryBinding = {
  expectedTaskRowVersion: '8',
  platformInstanceId: 'platform-instance-id',
  processToken: 'process-token',
  revisionId: 'revision-id',
  runId: 'run-id',
  taskId: 'task-id',
};

function port(): AutoRecoveryPort {
  return {
    preflight: vi.fn(async () => undefined),
    requireManualRecovery: vi.fn(async () => undefined),
    restartWithRetainedCapacity: vi.fn(async () => binding),
    warn: vi.fn(async () => undefined),
  };
}

describe('FixedAutoRecoveryCoordinator', () => {
  afterEach(() => vi.useRealTimers());
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-08T00:00:00.000Z'));
  });

  it('waits 30 seconds, runs real preflight, and restarts once as SYSTEM', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);

    await expect(coordinator.observeUnexpectedExit({ ...binding, eligible: true })).resolves.toBe(
      'RETRY_WAIT',
    );
    expect(recoveryPort.preflight).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    expect(recoveryPort.warn).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ actorType: 'SYSTEM', ...binding }),
      'FIRST_EXIT',
    );
    expect(recoveryPort.warn).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ actorType: 'SYSTEM', ...binding }),
      'AUTO_RECOVERY_ATTEMPT',
    );
    expect(recoveryPort.preflight).toHaveBeenCalledBefore(
      recoveryPort.restartWithRetainedCapacity as ReturnType<typeof vi.fn>,
    );
    expect(coordinator.state(binding.taskId)).toBe('RECOVERED');
  });

  it('moves a second eligible exit in the rolling window directly to manual recovery', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    await expect(coordinator.observeUnexpectedExit({ ...binding, eligible: true })).resolves.toBe(
      'MANUAL_REQUIRED',
    );
    expect(recoveryPort.restartWithRetainedCapacity).toHaveBeenCalledTimes(1);
    expect(recoveryPort.requireManualRecovery).toHaveBeenCalledWith(
      expect.objectContaining(binding),
      'RUNTIME_AUTO_RECOVERY_LIMIT_REACHED',
    );
  });

  it('resets the allowance only after 30 minutes of continuous health', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);
    expect(coordinator.state(binding.taskId)).toBe('RECOVERED');

    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_HEALTHY_RESET_MS - 1);
    expect(coordinator.state(binding.taskId)).toBe('RECOVERED');
    await vi.advanceTimersByTimeAsync(1);
    expect(coordinator.state(binding.taskId)).toBe('HEALTHY');

    await expect(coordinator.observeUnexpectedExit({ ...binding, eligible: true })).resolves.toBe(
      'RETRY_WAIT',
    );
  });

  it('does not trigger or consume allowance for an explicitly ineligible event', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);

    await expect(coordinator.observeUnexpectedExit({ ...binding, eligible: false })).resolves.toBe(
      'HEALTHY',
    );
    expect(recoveryPort.warn).not.toHaveBeenCalled();
    expect(recoveryPort.preflight).not.toHaveBeenCalled();
  });

  it('cancels the pending retry when a user stop is accepted', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });

    coordinator.cancel(binding.taskId);
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    expect(recoveryPort.preflight).not.toHaveBeenCalled();
    expect(recoveryPort.restartWithRetainedCapacity).not.toHaveBeenCalled();
    expect(coordinator.state(binding.taskId)).toBe('HEALTHY');
  });

  it('fails explicitly into the manual gate when preflight or restart fails', async () => {
    const recoveryPort = port();
    vi.mocked(recoveryPort.preflight).mockRejectedValueOnce(new Error('IDENTITY_CHANGED'));
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);

    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    expect(recoveryPort.restartWithRetainedCapacity).not.toHaveBeenCalled();
    expect(recoveryPort.requireManualRecovery).toHaveBeenCalledWith(
      expect.objectContaining(binding),
      'IDENTITY_CHANGED',
    );
    expect(coordinator.state(binding.taskId)).toBe('MANUAL_REQUIRED');
  });

  it('does not report the manual gate completed when its persistence/release fails', async () => {
    const recoveryPort = port();
    vi.mocked(recoveryPort.preflight).mockRejectedValueOnce(new Error('PREFLIGHT_FAILED'));
    vi.mocked(recoveryPort.requireManualRecovery).mockRejectedValueOnce(
      new Error('CAPACITY_RELEASE_FAILED'),
    );
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);

    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    expect(coordinator.state(binding.taskId)).toBe('MANUAL_PERSIST_FAILED');

    await expect(coordinator.observeUnexpectedExit({ ...binding, eligible: true })).resolves.toBe(
      'MANUAL_REQUIRED',
    );
    expect(recoveryPort.requireManualRecovery).toHaveBeenCalledTimes(2);
    expect(recoveryPort.requireManualRecovery).toHaveBeenLastCalledWith(
      expect.objectContaining(binding),
      'PREFLIGHT_FAILED',
    );
  });

  it('ignores stale bindings and clears all timers during shutdown', async () => {
    const recoveryPort = port();
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });

    await expect(
      coordinator.observeUnexpectedExit({
        ...binding,
        eligible: true,
        processToken: 'new-token',
        runId: 'new-run',
      }),
    ).resolves.toBe('RETRY_WAIT');
    coordinator.onApplicationShutdown();
    await vi.advanceTimersByTimeAsync(AUTO_RECOVERY_RETRY_DELAY_MS);

    expect(recoveryPort.preflight).not.toHaveBeenCalled();
    expect(coordinator.state(binding.taskId)).toBe('HEALTHY');
  });

  it('performs the configured number of attempts with exponential backoff', async () => {
    const recoveryPort = port();
    recoveryPort.policy = vi.fn(async () => ({
      enabled: true,
      maxAttempts: 3,
      retryDelaySeconds: 5,
    }));
    vi.mocked(recoveryPort.preflight).mockRejectedValue(new Error('SOURCE_CONNECTION_FAILED'));
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(4999);
    expect(recoveryPort.preflight).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(10001);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(19999);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(3);
    expect(recoveryPort.requireManualRecovery).toHaveBeenCalledOnce();
    expect(coordinator.state(binding.taskId)).toBe('MANUAL_REQUIRED');
    await vi.advanceTimersByTimeAsync(60000);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(3);
  });

  it('recovers on a later attempt and spends the same allowance on repeated exits', async () => {
    const recoveryPort = port();
    recoveryPort.policy = async () => ({ enabled: true, maxAttempts: 3, retryDelaySeconds: 5 });
    vi.mocked(recoveryPort.preflight).mockRejectedValueOnce(new Error('SOURCE_CONNECTION_FAILED'));
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(15000);
    expect(coordinator.state(binding.taskId)).toBe('RECOVERED');
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(20000);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(3);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    expect(coordinator.state(binding.taskId)).toBe('MANUAL_REQUIRED');
  });

  it('does not run when disabled or after the allowance is dynamically lowered', async () => {
    const recoveryPort = port();
    recoveryPort.policy = vi.fn(async () => ({
      enabled: false,
      maxAttempts: 3,
      retryDelaySeconds: 5,
    }));
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    expect(recoveryPort.requireManualRecovery).toHaveBeenCalledWith(
      expect.anything(),
      'RUNTIME_AUTO_RECOVERY_DISABLED',
    );
    expect(recoveryPort.preflight).not.toHaveBeenCalled();
    coordinator.cancel(binding.taskId);
    vi.mocked(recoveryPort.policy).mockResolvedValue({
      enabled: true,
      maxAttempts: 3,
      retryDelaySeconds: 5,
    });
    vi.mocked(recoveryPort.preflight).mockRejectedValue(new Error('SOURCE_CONNECTION_FAILED'));
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(5000);
    vi.mocked(recoveryPort.policy).mockResolvedValue({
      enabled: true,
      maxAttempts: 1,
      retryDelaySeconds: 5,
    });
    await vi.advanceTimersByTimeAsync(10000);
    expect(recoveryPort.preflight).toHaveBeenCalledTimes(1);
    expect(coordinator.state(binding.taskId)).toBe('MANUAL_REQUIRED');
  });

  it.each(['cancel', 'shutdown'] as const)(
    'does not restart or persist a late preflight failure after %s',
    async (action) => {
      const recoveryPort = port();
      let reject!: (error: Error) => void;
      vi.mocked(recoveryPort.preflight).mockImplementation(
        () =>
          new Promise((_, fail) => {
            reject = fail;
          }),
      );
      const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
      await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
      await vi.advanceTimersByTimeAsync(30000);
      if (action === 'cancel') coordinator.cancel(binding.taskId);
      else coordinator.onApplicationShutdown();
      reject(new Error('LATE_FAILURE'));
      await vi.advanceTimersByTimeAsync(1);
      expect(recoveryPort.requireManualRecovery).not.toHaveBeenCalled();
      expect(recoveryPort.restartWithRetainedCapacity).not.toHaveBeenCalled();
    },
  );

  it('does not retry against an obsolete run when startup has terminalized a new run', async () => {
    const recoveryPort = port();
    recoveryPort.policy = async () => ({ enabled: true, maxAttempts: 3, retryDelaySeconds: 5 });
    vi.mocked(recoveryPort.restartWithRetainedCapacity).mockRejectedValue(
      new AutoRecoveryManualRequiredError('RUNTIME_PROCESS_HANDLE_MISSING'),
    );
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(30000);
    expect(recoveryPort.restartWithRetainedCapacity).toHaveBeenCalledOnce();
    expect(recoveryPort.requireManualRecovery).not.toHaveBeenCalled();
    expect(coordinator.state(binding.taskId)).toBe('MANUAL_REQUIRED');
  });

  it('retains an exit reported by the new process before its start call has returned', async () => {
    const recoveryPort = port();
    recoveryPort.policy = async () => ({ enabled: true, maxAttempts: 2, retryDelaySeconds: 5 });
    const coordinator = new FixedAutoRecoveryCoordinator(recoveryPort);
    const next = {
      ...binding,
      runId: 'next-run',
      processToken: 'next-token',
      expectedTaskRowVersion: '9',
    };
    vi.mocked(recoveryPort.restartWithRetainedCapacity)
      .mockImplementationOnce(async () => {
        await coordinator.observeUnexpectedExit({
          ...next,
          expectedTaskRowVersion: '10',
          eligible: true,
        });
        return next;
      })
      .mockResolvedValue(next);
    await coordinator.observeUnexpectedExit({ ...binding, eligible: true });
    await vi.advanceTimersByTimeAsync(5000);
    expect(coordinator.state(binding.taskId)).toBe('RETRY_WAIT');
    await vi.advanceTimersByTimeAsync(10000);
    expect(recoveryPort.restartWithRetainedCapacity).toHaveBeenCalledTimes(2);
    expect(recoveryPort.preflight).toHaveBeenLastCalledWith(
      expect.objectContaining({ ...next, expectedTaskRowVersion: '10' }),
    );
  });
});
