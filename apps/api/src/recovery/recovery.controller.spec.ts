import { BadRequestException, ForbiddenException, NotFoundException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';

import { RecoveryController } from './recovery.controller.js';
import { RecoveryCoordinatorError } from './recovery-coordinator.service.js';

const sessionId = '0199c617-249d-7459-bbcb-f97ad47a3110';

function request(role: 'ADMIN' | 'DEVELOPER') {
  return {
    authContext: {
      user: { id: '0199c617-249d-7459-bbcb-f97ad47a3120', role },
    },
  } as never;
}

function fixture(state: 'PAUSED' | 'FAILED' | 'READY' = 'PAUSED') {
  const session = { id: sessionId, state };
  const coordinator = {
    confirmOldRuntimeStopped: vi.fn(async () => session),
    get: vi.fn(async () => session),
    getCurrent: vi.fn(async () => session),
    run: vi.fn(async () => session),
  };
  const bootstrapState = {
    getStatus: vi.fn(() => ({ phase: 'RECOVERING', platformInstanceId: 'instance-1' })),
  };
  const bootstrap = { complete: vi.fn(async () => true) };
  const taskRecovery = { retry: vi.fn(async () => session) };
  return {
    bootstrap,
    taskRecovery,
    controller: new RecoveryController(
      coordinator as never,
      bootstrapState as never,
      bootstrap as never,
      taskRecovery as never,
    ),
    coordinator,
  };
}

describe('RecoveryController', () => {
  it('requires administrator confirmation before retrying a selected task', async () => {
    const context = fixture();
    const parameters = { taskId: sessionId };
    await expect(
      context.controller.retryTask(parameters, { expectedTaskRowVersion: '4' }, request('ADMIN')),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      context.controller.retryTask(
        parameters,
        { expectedTaskRowVersion: '4', oldRuntimeStoppedConfirmed: true },
        request('DEVELOPER'),
      ),
    ).rejects.toBeInstanceOf(ForbiddenException);
    expect(context.taskRecovery.retry).not.toHaveBeenCalled();
    await context.controller.retryTask(
      parameters,
      { expectedTaskRowVersion: '4', oldRuntimeStoppedConfirmed: true },
      request('ADMIN'),
    );
    expect(context.taskRecovery.retry).toHaveBeenCalledWith(
      expect.objectContaining({ role: 'ADMIN' }),
      sessionId,
      '4',
    );
  });

  it('discovers only the current platform instance recovery session for an administrator', async () => {
    const context = fixture();

    await expect(context.controller.current(request('ADMIN'))).resolves.toEqual({
      id: sessionId,
      state: 'PAUSED',
    });
    expect(context.coordinator.getCurrent).toHaveBeenCalledWith('instance-1');
  });

  it('rejects developers from the restricted recovery surface', async () => {
    const context = fixture();

    await expect(context.controller.current(request('DEVELOPER'))).rejects.toBeInstanceOf(
      ForbiddenException,
    );
  });

  it('maps a missing current session to HTTP 404', async () => {
    const context = fixture();
    vi.mocked(context.coordinator.getCurrent).mockRejectedValueOnce(
      new RecoveryCoordinatorError('RECOVERY_SESSION_NOT_FOUND'),
    );
    await expect(context.controller.current(request('ADMIN'))).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });

  it('maps a missing session by id to HTTP 404', async () => {
    const context = fixture();
    vi.mocked(context.coordinator.get).mockRejectedValueOnce(
      new RecoveryCoordinatorError('RECOVERY_SESSION_NOT_FOUND'),
    );
    await expect(context.controller.get({ sessionId }, request('ADMIN'))).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });

  it.each(['READY', 'FAILED', 'PAUSED'] as const)(
    'opens management after %s task recovery',
    async (state) => {
      const context = fixture(state);
      await context.controller.run({ sessionId }, request('ADMIN'));
      expect(context.bootstrap.complete).toHaveBeenCalledWith(sessionId);
    },
  );

  it('passes an explicit reason and session version to the old-runtime confirmation command', async () => {
    const context = fixture();
    await context.controller.confirmOldRuntimeStopped(
      { sessionId },
      { expectedSessionRowVersion: '4', reason: 'deployment verified the old container is gone' },
      request('ADMIN'),
    );
    expect(context.coordinator.confirmOldRuntimeStopped).toHaveBeenCalledWith(
      sessionId,
      '4',
      'deployment verified the old container is gone',
      '0199c617-249d-7459-bbcb-f97ad47a3120',
    );
  });
});
