import { UnauthorizedException } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { hashOpaqueToken } from '@scs/security';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { AuthService } from '../auth/auth.service.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { ConfigTransferService } from './config-transfer.service.js';
import { ConfigTransferController } from './config-transfer.controller.js';

let app: NestFastifyApplication;
const transfer = { export: vi.fn(), preview: vi.fn(), import: vi.fn() };
beforeAll(async () => {
  const module = await Test.createTestingModule({
    controllers: [ConfigTransferController],
    providers: [
      SessionAuthGuard,
      CsrfGuard,
      AdminGuard,
      { provide: ConfigTransferService, useValue: transfer },
      {
        provide: AuthService,
        useValue: {
          authenticateSession: async (token: string) => {
            if (!token) throw new UnauthorizedException({ code: 'SESSION_INVALID' });
            return {
              csrfTokenHash: hashOpaqueToken('test-csrf'),
              user: {
                id: '11111111-1111-4111-8111-111111111111',
                role: token === 'admin' ? 'ADMIN' : 'DEVELOPER',
                status: 'ACTIVE',
              },
            };
          },
          assertCsrf: AuthService.prototype.assertCsrf,
        },
      },
    ],
  }).compile();
  app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter({ logger: false }));
  app.setGlobalPrefix('api');
  await app.init();
});
afterAll(async () => {
  await app?.close();
});
it.each(['export', 'preview', 'import'] as const)(
  'protects configuration %s behind session, CSRF and administrator checks',
  async (action) => {
    const url = `/api/settings/config-transfer/${action}`;
    expect((await app.inject({ method: 'POST', url, payload: {} })).statusCode).toBe(401);
    expect(
      (
        await app.inject({
          method: 'POST',
          url,
          headers: { cookie: 'scs_session=developer', 'x-csrf-token': 'test-csrf' },
          payload: {},
        })
      ).statusCode,
    ).toBe(403);
    expect(
      (
        await app.inject({
          method: 'POST',
          url,
          headers: { cookie: 'scs_session=admin' },
          payload: {},
        })
      ).statusCode,
    ).toBe(403);
    expect(transfer[action]).not.toHaveBeenCalled();
  },
);
it('rejects credential-bearing or unknown-version files before the import service is reached', async () => {
  const headers = { cookie: 'scs_session=admin', 'x-csrf-token': 'test-csrf' };
  for (const payload of [
    { format: 'scs-platform-config', version: 999 },
    {
      format: 'scs-platform-config',
      version: 1,
      exportedAt: '2026-10-23T00:00:00Z',
      dataSources: [],
      groups: [],
      tasks: [],
      system: null,
      password: 'must-not-pass',
    },
  ]) {
    expect(
      (
        await app.inject({
          method: 'POST',
          url: '/api/settings/config-transfer/preview',
          headers,
          payload,
        })
      ).statusCode,
    ).toBe(400);
  }
  expect(transfer.preview).not.toHaveBeenCalled();
});
