import { UnauthorizedException } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { hashOpaqueToken } from '@scs/security';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { AuthService } from '../auth/auth.service.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { TaskGroupsController } from './tasks.controller.js';
import { TasksService, TasksServiceError } from './tasks.service.js';

const id = '11111111-1111-4111-8111-111111111111';
const service = { renameGroup: vi.fn(), deleteGroup: vi.fn(), reorderGroups: vi.fn() };
const headers = { cookie: 'scs_session=developer', 'x-csrf-token': 'csrf' };
let app: NestFastifyApplication;
beforeAll(async () => {
  const module = await Test.createTestingModule({
    controllers: [TaskGroupsController],
    providers: [
      SessionAuthGuard,
      CsrfGuard,
      { provide: TasksService, useValue: service },
      {
        provide: AuthService,
        useValue: {
          authenticateSession: async (token: string) => {
            if (!token) throw new UnauthorizedException({ code: 'SESSION_INVALID' });
            return {
              csrfTokenHash: hashOpaqueToken('csrf'),
              user: { id, role: 'DEVELOPER', status: 'ACTIVE' },
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

it.each([
  {
    method: 'PATCH' as const,
    url: `/api/task-groups/${id}`,
    payload: { name: '新组', expectedName: '原组' },
  },
  {
    method: 'DELETE' as const,
    url: `/api/task-groups/${id}`,
    payload: { expectedName: '原组', detachTasks: true },
  },
  {
    method: 'PATCH' as const,
    url: '/api/task-groups/order',
    payload: { groupIds: [], expectedGroupIds: [] },
  },
])('requires login and CSRF for $method $url', async (request) => {
  expect((await app.inject(request)).statusCode).toBe(401);
  expect((await app.inject({ ...request, headers: { cookie: headers.cookie } })).statusCode).toBe(
    403,
  );
});

it('routes sorting separately from folder renaming and rejects repeated folder IDs', async () => {
  service.reorderGroups.mockResolvedValue({ groups: [] });
  expect(
    (
      await app.inject({
        method: 'PATCH',
        url: '/api/task-groups/order',
        headers,
        payload: { groupIds: [], expectedGroupIds: [] },
      })
    ).statusCode,
  ).toBe(200);
  expect(service.reorderGroups).toHaveBeenCalledOnce();
  expect(service.renameGroup).not.toHaveBeenCalled();
  expect(
    (
      await app.inject({
        method: 'PATCH',
        url: '/api/task-groups/order',
        headers,
        payload: { groupIds: [id, id], expectedGroupIds: [id] },
      })
    ).statusCode,
  ).toBe(400);
  expect(service.reorderGroups).toHaveBeenCalledOnce();
});

it('surfaces the occupied-folder confirmation requirement without retrying deletion', async () => {
  service.deleteGroup.mockRejectedValue(new TasksServiceError('TASK_GROUP_NOT_EMPTY'));
  const response = await app.inject({
    method: 'DELETE',
    url: `/api/task-groups/${id}`,
    headers,
    payload: { expectedName: '原组', detachTasks: false },
  });
  expect(response.statusCode).toBe(409);
  expect(response.json()).toMatchObject({ code: 'TASK_GROUP_NOT_EMPTY' });
  expect(service.deleteGroup).toHaveBeenCalledOnce();
});
