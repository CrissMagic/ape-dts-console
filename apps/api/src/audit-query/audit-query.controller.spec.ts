import { GUARDS_METADATA } from '@nestjs/common/constants';
import { BadRequestException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';

import { AdminGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { AuditQueryController } from './audit-query.controller.js';
import { AuditQueryService } from './audit-query.service.js';

function setup() {
  const audit = { list: vi.fn() };
  return {
    audit,
    controller: new AuditQueryController(audit as unknown as AuditQueryService),
  };
}

describe('AuditQueryController', () => {
  it('requires both an authenticated session and administrator role', () => {
    expect(Reflect.getMetadata(GUARDS_METADATA, AuditQueryController.prototype.list)).toEqual([
      SessionAuthGuard,
      AdminGuard,
    ]);
  });

  it('passes only parsed bounded filters to the query service', async () => {
    const test = setup();
    test.audit.list.mockResolvedValue({ events: [], nextCursor: null });

    await expect(
      test.controller.list({ actorType: 'SYSTEM', limit: '25', subjectType: 'TASK' }),
    ).resolves.toEqual({ events: [], nextCursor: null });
    expect(test.audit.list).toHaveBeenCalledWith({
      actorType: 'SYSTEM',
      limit: 25,
      subjectType: 'TASK',
    });
  });

  it('rejects malformed filters before querying storage', async () => {
    const test = setup();

    await expect(test.controller.list({ actorType: 'SERVICE' })).rejects.toBeInstanceOf(
      BadRequestException,
    );
    expect(test.audit.list).not.toHaveBeenCalled();
  });

  it('maps an invalid calendar date in an opaque cursor before querying storage', async () => {
    const query = vi.fn();
    const service = new AuditQueryService({
      requirePool: () => ({ query }),
    } as unknown as PlatformDatabaseService);
    const controller = new AuditQueryController(service);
    const cursor = Buffer.from(
      JSON.stringify({ occurredAt: '2026-02-30T00:00:00.000Z', sequence: '1' }),
      'utf8',
    ).toString('base64url');

    await expect(controller.list({ cursor })).rejects.toMatchObject({
      response: { code: 'AUDIT_CURSOR_INVALID' },
    });
    expect(query).not.toHaveBeenCalled();
  });

  it('rejects a year zero cursor before querying storage', async () => {
    const query = vi.fn();
    const service = new AuditQueryService({
      requirePool: () => ({ query }),
    } as unknown as PlatformDatabaseService);
    const controller = new AuditQueryController(service);
    const cursor = Buffer.from(
      JSON.stringify({ occurredAt: '0000-01-01T00:00:00.000Z', sequence: '1' }),
      'utf8',
    ).toString('base64url');

    await expect(controller.list({ cursor })).rejects.toMatchObject({
      response: { code: 'AUDIT_CURSOR_INVALID' },
    });
    expect(query).not.toHaveBeenCalled();
  });
});
