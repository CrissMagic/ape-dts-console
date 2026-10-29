import { UnauthorizedException } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { hashOpaqueToken } from '@scs/security';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { AuthService } from '../auth/auth.service.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { RuntimeRecoverySettingsController } from './runtime-recovery.controller.js';
import { RuntimeRecoverySettingsService } from './runtime-recovery.service.js';

let app: NestFastifyApplication;
const settings = { enabled: true, maxAttempts: 3, retryDelaySeconds: 30, rowVersion: '1' };
const service = {
  get: vi.fn(async () => settings),
  update: vi.fn(async () => ({ ...settings, rowVersion: '2' })),
};
beforeAll(async () => {
  const module = await Test.createTestingModule({
    controllers: [RuntimeRecoverySettingsController],
    providers: [
      SessionAuthGuard,
      CsrfGuard,
      AdminGuard,
      { provide: RuntimeRecoverySettingsService, useValue: service },
      {
        provide: AuthService,
        useValue: {
          authenticateSession: async (token: string) => {
            if (!token) throw new UnauthorizedException({ code: 'SESSION_INVALID' });
            return {
              csrfTokenHash: hashOpaqueToken('csrf'),
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
const url = '/api/settings/runtime-recovery';
const payload = { enabled: true, maxAttempts: 5, retryDelaySeconds: 60, expectedRowVersion: '1' };
it('allows only administrators to read or update recovery policy and requires CSRF for writes', async () => {
  expect((await app.inject({ method: 'GET', url })).statusCode).toBe(401);
  expect(
    (await app.inject({ method: 'GET', url, headers: { cookie: 'scs_session=developer' } }))
      .statusCode,
  ).toBe(403);
  expect(
    (await app.inject({ method: 'GET', url, headers: { cookie: 'scs_session=admin' } })).statusCode,
  ).toBe(200);
  expect(
    (await app.inject({ method: 'PATCH', url, headers: { cookie: 'scs_session=admin' }, payload }))
      .statusCode,
  ).toBe(403);
  expect(service.update).not.toHaveBeenCalled();
  expect(
    (
      await app.inject({
        method: 'PATCH',
        url,
        headers: { cookie: 'scs_session=admin', 'x-csrf-token': 'csrf' },
        payload,
      })
    ).statusCode,
  ).toBe(200);
  expect(service.update).toHaveBeenCalledExactlyOnceWith(
    '11111111-1111-4111-8111-111111111111',
    payload,
  );
});
it.each([
  { maxAttempts: 0 },
  { maxAttempts: 11 },
  { maxAttempts: 1.5 },
  { retryDelaySeconds: 4 },
  { retryDelaySeconds: 3601 },
])('rejects unsafe recovery settings %j', async (patch) => {
  service.update.mockClear();
  expect(
    (
      await app.inject({
        method: 'PATCH',
        url,
        headers: { cookie: 'scs_session=admin', 'x-csrf-token': 'csrf' },
        payload: { ...payload, ...patch },
      })
    ).statusCode,
  ).toBe(400);
  expect(service.update).not.toHaveBeenCalled();
});
