import 'reflect-metadata';

import { BadRequestException, ConflictException } from '@nestjs/common';
import { GUARDS_METADATA } from '@nestjs/common/constants';
import type { TaskRevisionRiskAcceptance, UserSummary } from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { RiskAcceptanceController } from './risk-acceptance.controller.js';
import { RiskAcceptanceService, RiskAcceptanceServiceError } from './risk-acceptance.service.js';

const taskId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3';
const revisionId = '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f4';
const administrator: UserSummary = {
  displayName: 'Risk administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'risk-admin',
};
const acceptance: TaskRevisionRiskAcceptance = {
  acceptedAt: '2026-09-07T00:00:01.000Z',
  acceptedByUserId: administrator.id,
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f6',
  note: 'reviewed impact',
  revisionId,
  taskId,
};

function request(): AuthenticatedFastifyRequest {
  return {
    authContext: { user: administrator },
  } as unknown as AuthenticatedFastifyRequest;
}

function controller() {
  const accept = vi.fn(async () => acceptance);
  const service = { accept } as unknown as RiskAcceptanceService;
  return { accept, controller: new RiskAcceptanceController(service) };
}

const validBody = {
  expectedDraftVersion: '7',
  expectedTaskRowVersion: '3',
  revisionId,
};

describe('RiskAcceptanceController', () => {
  it('uses the complete high-risk administrator guard chain', () => {
    const guards = Reflect.getMetadata(
      GUARDS_METADATA,
      RiskAcceptanceController.prototype.accept,
    ) as readonly unknown[];

    expect(guards).toEqual([SessionAuthGuard, CsrfGuard, AdminGuard]);
  });

  it('validates and trims the request before recording the authenticated user action', async () => {
    const test = controller();

    await expect(
      test.controller.accept({ taskId }, { ...validBody, note: '  reviewed impact  ' }, request()),
    ).resolves.toEqual(acceptance);

    expect(test.accept).toHaveBeenCalledWith(administrator, taskId, {
      ...validBody,
      note: 'reviewed impact',
    });
  });

  it('rejects a whitespace-only note before calling the service', async () => {
    const test = controller();

    await expect(
      test.controller.accept({ taskId }, { ...validBody, note: '   ' }, request()),
    ).rejects.toBeInstanceOf(BadRequestException);

    expect(test.accept).not.toHaveBeenCalled();
  });

  it('maps a duplicate acceptance to the specified 409 response', async () => {
    const test = controller();
    test.accept.mockRejectedValue(new RiskAcceptanceServiceError('RISK_ACCEPTANCE_ALREADY_EXISTS'));

    try {
      await test.controller.accept({ taskId }, validBody, request());
      throw new Error('expected duplicate acceptance to fail');
    } catch (error) {
      expect(error).toBeInstanceOf(ConflictException);
      expect((error as ConflictException).getResponse()).toEqual({
        code: 'RISK_ACCEPTANCE_ALREADY_EXISTS',
      });
    }
  });
});
