import 'reflect-metadata';

import { BadRequestException, ServiceUnavailableException } from '@nestjs/common';
import { GUARDS_METADATA } from '@nestjs/common/constants';
import type { UserSummary } from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  LogLifecycleSettingsError,
  type LogLifecycleSettingsService,
} from './log-lifecycle-settings.service.js';
import { LogLifecycleController } from './log-lifecycle.controller.js';

const actor: UserSummary = {
  displayName: 'Administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'admin',
};

const request = { authContext: { user: actor } } as unknown as AuthenticatedFastifyRequest;

function setup() {
  const get = vi.fn();
  const preview = vi.fn();
  const save = vi.fn();
  return {
    controller: new LogLifecycleController({
      get,
      preview,
      save,
    } as unknown as LogLifecycleSettingsService),
    get,
    preview,
    save,
  };
}

describe('LogLifecycleController', () => {
  it('protects reads as administrator-only and mutations with the high-risk guard chain', () => {
    expect(Reflect.getMetadata(GUARDS_METADATA, LogLifecycleController.prototype.get)).toEqual([
      SessionAuthGuard,
      AdminGuard,
    ]);
    for (const method of [
      LogLifecycleController.prototype.preview,
      LogLifecycleController.prototype.update,
    ]) {
      expect(Reflect.getMetadata(GUARDS_METADATA, method)).toEqual([
        SessionAuthGuard,
        CsrfGuard,
        AdminGuard,
      ]);
    }
  });

  it('validates the local DTO before invoking the service', async () => {
    const test = setup();
    await expect(
      test.controller.preview({ key: 'logRetentionDays', rowVersion: '0', value: 30 }, request),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(test.preview).not.toHaveBeenCalled();
  });

  it('passes the authenticated actor and exact mutation fields without adding defaults', async () => {
    const test = setup();
    test.save.mockResolvedValue({ rowVersion: '8' });
    const body = {
      confirmationToken: 'confirmation',
      key: 'runRetentionDays' as const,
      rowVersion: '7',
      value: 365,
    };
    await test.controller.update(body, request);
    expect(test.save).toHaveBeenCalledWith(actor, body);
  });

  it('maps unavailable runtime limit coordination to 503', async () => {
    const test = setup();
    test.preview.mockRejectedValue(
      new LogLifecycleSettingsError('LOG_RUNTIME_LIMIT_COORDINATOR_UNAVAILABLE'),
    );
    await expect(
      test.controller.preview(
        { key: 'runLogLimitBytes', rowVersion: '1', value: 1_073_741_824 },
        request,
      ),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
  });
});
