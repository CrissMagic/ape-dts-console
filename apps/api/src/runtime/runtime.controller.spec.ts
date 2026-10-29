import {
  BadRequestException,
  ConflictException,
  ServiceUnavailableException,
} from '@nestjs/common';
import type { UserSummary } from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { RuntimeLogReadError } from './runtime-log-read.service.js';
import { RuntimeController } from './runtime.controller.js';
import {
  RuntimeSupervisorError,
  type RuntimeSupervisorService,
} from './runtime-supervisor.service.js';

const taskId = '11111111-1111-4111-8111-111111111111';
const revisionId = '22222222-2222-4222-8222-222222222222';
const runId = '55555555-5555-4555-8555-555555555555';
const administrator: UserSummary = {
  displayName: 'Runtime administrator',
  id: '33333333-3333-4333-8333-333333333333',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'runtime-admin',
};

function request(): AuthenticatedFastifyRequest {
  return {
    authContext: {
      sessionGeneration: '1',
      sessionId: '44444444-4444-4444-8444-444444444444',
      sourceFingerprint: Buffer.alloc(32),
      user: administrator,
    },
  } as unknown as AuthenticatedFastifyRequest;
}

function controller() {
  const runtime = { start: vi.fn(), stop: vi.fn(), discardRecovery: vi.fn() };
  const reads = { get: vi.fn() };
  const logs = { list: vi.fn(), download: vi.fn() };
  const logPipeline = { retryStorage: vi.fn() };
  return {
    controller: new RuntimeController(
      runtime as unknown as RuntimeSupervisorService,
      reads as never,
      logs as never,
      logPipeline as never,
    ),
    logPipeline,
    logs,
    reads,
    runtime,
  };
}

describe('RuntimeController', () => {
  it('reports concurrent artifact removal as a task conflict without hiding unrelated database failures', async () => {
    const context = controller();
    const error = Object.assign(new Error('artifact unavailable'), { code: '23514' });
    context.runtime.start.mockRejectedValue(error);
    const input = {
      revisionId,
      expectedTaskRowVersion: '1',
      idempotencyKey: 'artifact-delete-race',
    };
    await expect(context.controller.start({ taskId }, input, request())).rejects.toMatchObject({
      response: { code: 'RUNTIME_ARTIFACT_NOT_ACTIVE' },
    });
    const unrelated = Object.assign(new Error('other constraint'), { code: '23514' });
    context.runtime.start.mockRejectedValue(unrelated);
    await expect(context.controller.start({ taskId }, input, request())).rejects.toBe(unrelated);
  });
  it('requires an explicit old-process stop confirmation before discarding recovery', async () => {
    const context = controller();
    await expect(
      context.controller.discardRecovery({ taskId }, { expectedTaskRowVersion: '1' }, request()),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(context.runtime.discardRecovery).not.toHaveBeenCalled();
    await context.controller.discardRecovery(
      { taskId },
      { expectedTaskRowVersion: '1', oldRuntimeStoppedConfirmed: true },
      request(),
    );
    expect(context.runtime.discardRecovery).toHaveBeenCalledWith(administrator, taskId, '1');
  });
  it('reads runtime facts using the authenticated actor and path task id', async () => {
    const context = controller();
    context.reads.get.mockResolvedValue({ taskId });
    await expect(context.controller.get({ taskId }, request())).resolves.toEqual({ taskId });
    expect(context.reads.get).toHaveBeenCalledWith(administrator, taskId);
  });

  it('reads bounded logs with parsed stream and unit filters', async () => {
    const context = controller();
    context.logs.list.mockResolvedValue({ facts: [], limited: false, runId, taskId });
    await expect(
      context.controller.logsList(
        { runId, taskId },
        {
          from: '2026-09-08T00:00:00.000Z',
          limit: '25',
          stream: 'STDERR',
          to: '2026-09-09T00:00:00.000Z',
          unitKey: 'partition:1',
        },
        request(),
      ),
    ).resolves.toMatchObject({ facts: [], limited: false });
    expect(context.logs.list).toHaveBeenCalledWith(administrator, taskId, runId, {
      limit: 25,
      from: '2026-09-08T00:00:00.000Z',
      stream: 'STDERR',
      to: '2026-09-09T00:00:00.000Z',
      unitKey: 'partition:1',
    });
  });

  it('rejects an invalid log limit before invoking storage', async () => {
    const context = controller();
    await expect(
      context.controller.logsList({ runId, taskId }, { limit: '1001' }, request()),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(context.logs.list).not.toHaveBeenCalled();
  });

  it('rejects a reversed log time range before invoking storage', async () => {
    const context = controller();
    await expect(
      context.controller.logsList(
        { runId, taskId },
        { from: '2026-09-09T00:00:00.000Z', to: '2026-09-08T00:00:00.000Z' },
        request(),
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(context.logs.list).not.toHaveBeenCalled();
  });

  it('retries log storage for the exact authorized task run', async () => {
    const context = controller();
    context.logPipeline.retryStorage.mockResolvedValue({
      durableRecovery: null,
      recoveryState: 'NONE',
      runId,
      storageState: 'DEGRADED',
      taskId,
    });

    await expect(context.controller.logsRetry({ runId, taskId }, request())).resolves.toMatchObject(
      {
        recoveryState: 'NONE',
        runId,
        taskId,
      },
    );
    expect(context.logPipeline.retryStorage).toHaveBeenCalledWith(administrator, taskId, runId);
  });

  it('sends log downloads as a hardened attachment instead of a JSON wrapper', async () => {
    const context = controller();
    const content = { pipe: vi.fn() };
    context.logs.download.mockResolvedValue({ content, filename: `runtime-${runId}.log` });
    const reply = { header: vi.fn(), send: vi.fn((body) => body) };
    await expect(
      context.controller.logsDownload({ runId, taskId }, {}, request(), reply as never),
    ).resolves.toBe(content);
    expect(reply.header).toHaveBeenCalledWith('Content-Type', 'text/plain; charset=utf-8');
    expect(reply.header).toHaveBeenCalledWith('X-Content-Type-Options', 'nosniff');
    expect(reply.header).toHaveBeenCalledWith(
      'Content-Disposition',
      `attachment; filename="runtime-${runId}.log"`,
    );
  });

  it('rejects the list-only limit parameter on an unbounded log download', async () => {
    const context = controller();
    const reply = { header: vi.fn(), send: vi.fn() };

    await expect(
      context.controller.logsDownload(
        { runId, taskId },
        { limit: '25' },
        request(),
        reply as never,
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(context.logs.download).not.toHaveBeenCalled();
  });

  it('maps download preflight failures before writing attachment headers', async () => {
    const context = controller();
    context.logs.download.mockRejectedValue(
      new RuntimeLogReadError('LOG_SEGMENT_OBJECT_TOO_LARGE'),
    );
    const reply = { header: vi.fn(), send: vi.fn() };

    await expect(
      context.controller.logsDownload({ runId, taskId }, {}, request(), reply as never),
    ).rejects.toMatchObject({ response: { code: 'LOG_SEGMENT_OBJECT_TOO_LARGE' } });
    expect(reply.header).not.toHaveBeenCalled();
    expect(reply.send).not.toHaveBeenCalled();
  });

  it('passes only the parsed start body plus the path task id to the supervisor', async () => {
    const context = controller();
    context.runtime.start.mockResolvedValue({ id: 'operation' });

    await expect(
      context.controller.start(
        { taskId },
        {
          expectedTaskRowVersion: '9',
          idempotencyKey: 'start-1',
          revisionId,
        },
        request(),
      ),
    ).resolves.toEqual({ id: 'operation' });
    expect(context.runtime.start).toHaveBeenCalledWith(administrator, {
      expectedTaskRowVersion: '9',
      idempotencyKey: 'start-1',
      revisionId,
      taskId,
    });
  });

  it('rejects malformed input before calling the supervisor', async () => {
    const context = controller();

    await expect(
      context.controller.start(
        { taskId },
        {
          expectedTaskRowVersion: '9',
          idempotencyKey: 'start-1',
          revisionId,
          units: [{ metricsPort: 19_001, unitKey: 'partition:0' }],
        },
        request(),
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(context.runtime.start).not.toHaveBeenCalled();
  });

  it('maps launch configuration failures to service unavailable', async () => {
    const context = controller();
    context.runtime.start.mockRejectedValue(
      new RuntimeSupervisorError('RUNTIME_LAUNCH_SPEC_UNCONFIGURED'),
    );

    await expect(
      context.controller.start(
        { taskId },
        {
          expectedTaskRowVersion: '9',
          idempotencyKey: 'start-1',
          revisionId,
        },
        request(),
      ),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('maps task state conflicts from stop without changing the error code', async () => {
    const context = controller();
    context.runtime.stop.mockRejectedValue(new RuntimeSupervisorError('RUNTIME_STOP_NOT_ALLOWED'));

    try {
      await context.controller.stop(
        { taskId },
        { expectedTaskRowVersion: '9', idempotencyKey: 'stop-1' },
        request(),
      );
      throw new Error('expected stop to reject');
    } catch (error) {
      expect(error).toBeInstanceOf(ConflictException);
      expect((error as ConflictException).getResponse()).toEqual({
        code: 'RUNTIME_STOP_NOT_ALLOWED',
      });
    }
  });
});
