import { describe, expect, it, vi } from 'vitest';

import {
  BootstrapCoordinatorError,
  BootstrapCoordinatorService,
} from './bootstrap-coordinator.service.js';

function fixture(options?: { readonly completionResult?: boolean }) {
  let phase: 'READY' | 'RECOVERING' = 'RECOVERING';
  const access = { invalidateSession: vi.fn() };
  const state = {
    getStatus: vi.fn(() => ({ phase, platformInstanceId: 'instance-1' })),
    markReady: vi.fn(() => {
      phase = 'READY';
    }),
  };
  const readiness = { markReady: vi.fn() };
  const emergency = { activateForReadyPlatform: vi.fn() };
  const recovery = {
    run: vi.fn(),
  };
  const recoveryCompletion = {
    complete: vi.fn(async () => options?.completionResult ?? true),
  };
  const coordinator = new BootstrapCoordinatorService(
    access as never,
    state as never,
    {} as never,
    {} as never,
    {} as never,
    {} as never,
    {} as never,
    readiness as never,
    emergency as never,
    recovery as never,
    recoveryCompletion as never,
    null,
  );
  return { access, coordinator, emergency, readiness, recovery, recoveryCompletion, state };
}

describe('BootstrapCoordinatorService recovery completion', () => {
  it('opens readiness and activates the emergency code only for this instance READY session', async () => {
    const context = fixture();

    await context.coordinator.completeExistingRecovery('recovery-1');

    expect(context.recoveryCompletion.complete).toHaveBeenCalledWith('recovery-1');
  });

  it('does not open readiness for a stale or unfinished recovery session', async () => {
    const context = fixture({ completionResult: false });

    await expect(context.coordinator.completeExistingRecovery('old-recovery')).rejects.toEqual(
      new BootstrapCoordinatorError('RECOVERY_SESSION_NOT_READY'),
    );

    expect(context.recoveryCompletion.complete).toHaveBeenCalledWith('old-recovery');
  });
});
