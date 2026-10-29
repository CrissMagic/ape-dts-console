import type { TaskSummary } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { authApi, auditApi, emergencyApi, tasksApi, uploadArtifactFile } from './platform-api';

describe('uploadArtifactFile transport', () => {
  class UploadRequest {
    static latest: UploadRequest;
    constructor() {
      UploadRequest.latest = this;
    }
    upload = {
      onprogress: null as
        ((event: { lengthComputable: boolean; loaded: number; total: number }) => void) | null,
    };
    open = vi.fn();
    setRequestHeader = vi.fn();
    send = vi.fn();
    abort = vi.fn(() => {
      this.onabort?.();
      this.onloadend?.();
    });
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    onabort: (() => void) | null = null;
    onloadend: (() => void) | null = null;
    status = 409;
    responseText = '{"code":"ARTIFACT_FILE_ALREADY_EXISTS"}';
  }
  it('uses the current session CSRF token and resolves a completed upload for registration', async () => {
    const csrf = 'upload-session-token-'.repeat(3);
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            status: 'AUTHENTICATED',
            csrfToken: csrf,
            mustChangePassword: false,
            absoluteExpiresAt: '2026-09-28T12:00:00.000Z',
            idleExpiresAt: '2026-09-28T11:00:00.000Z',
            user: {
              id: '11111111-1111-4111-8111-111111111111',
              username: 'admin',
              displayName: '管理员',
              role: 'ADMIN',
              status: 'ACTIVE',
            },
          }),
        ),
      ),
    );
    await authApi.session();
    vi.stubGlobal('XMLHttpRequest', UploadRequest);
    const file = new File(['elf'], 'ape-dts');
    const signal = new AbortController().signal;
    const promise = uploadArtifactFile(file, { signal, onProgress: vi.fn() });
    const request = UploadRequest.latest;
    expect(request.open).toHaveBeenCalledWith('POST', '/api/artifacts/files/upload');
    expect(request.setRequestHeader).toHaveBeenCalledWith('x-csrf-token', csrf);
    expect(request.setRequestHeader).toHaveBeenCalledWith(
      'content-type',
      'application/octet-stream',
    );
    const result = {
      file: {
        name: 'ape-dts',
        sizeBytes: 3,
        modifiedAt: '2026-09-28T10:00:00.000Z',
        kind: 'BINARY',
      },
      prepared: {
        fileName: 'ape-dts',
        sizeBytes: 3,
        sha256: 'a'.repeat(64),
        suggestedVersion: 'ape-dts',
      },
    };
    request.status = 201;
    request.responseText = JSON.stringify(result);
    request.onload?.();
    request.onloadend?.();
    await expect(promise).resolves.toEqual(result);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })));
    await authApi.logout();
  });
  it('streams the File directly, reports progress and surfaces a server rejection', async () => {
    vi.stubGlobal('XMLHttpRequest', UploadRequest);
    const file = new File(['content'], '制品.tar.gz');
    const controller = new AbortController();
    const onProgress = vi.fn();
    const promise = uploadArtifactFile(file, { signal: controller.signal, onProgress });
    const request = UploadRequest.latest;
    expect(request.send).toHaveBeenCalledWith(file);
    expect(request.setRequestHeader).toHaveBeenCalledWith(
      'x-artifact-filename',
      encodeURIComponent(file.name),
    );
    request.upload.onprogress?.({ lengthComputable: true, loaded: 5, total: 10 });
    expect(onProgress).toHaveBeenCalledWith(50);
    request.onload?.();
    request.onloadend?.();
    await expect(promise).rejects.toMatchObject({
      code: 'ARTIFACT_FILE_ALREADY_EXISTS',
      status: 409,
    });
    controller.abort();
    expect(request.abort).not.toHaveBeenCalled();
  });
  it('aborts the transport when cancelled and rejects without sending an already cancelled upload', async () => {
    vi.stubGlobal('XMLHttpRequest', UploadRequest);
    const controller = new AbortController();
    const promise = uploadArtifactFile(new File(['elf'], 'ape-dts'), {
      signal: controller.signal,
      onProgress: vi.fn(),
    });
    controller.abort();
    await expect(promise).rejects.toMatchObject({ code: 'UPLOAD_CANCELLED' });
    expect(UploadRequest.latest.abort).toHaveBeenCalledOnce();
    await expect(
      uploadArtifactFile(new File(['elf'], 'ape-dts'), {
        signal: controller.signal,
        onProgress: vi.fn(),
      }),
    ).rejects.toMatchObject({ code: 'UPLOAD_CANCELLED' });
    expect(UploadRequest.latest.send).not.toHaveBeenCalled();
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('emergencyApi', () => {
  it('sends the emergency code only in the dedicated header', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([]), {
        headers: { 'content-type': 'application/json' },
        status: 200,
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await emergencyApi.list('process-secret');

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('/api/emergency/runtime-tasks');
    expect(url).not.toContain('process-secret');
    expect(init.headers).toMatchObject({ 'x-emergency-stop-code': 'process-secret' });
  });
});

describe('auditApi', () => {
  it('encodes exact audit filters and the opaque cursor', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ events: [], nextCursor: null }), {
        headers: { 'content-type': 'application/json' },
        status: 200,
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await auditApi.list({
      action: 'TASK_RUNTIME_START_ACCEPTED',
      actorType: 'USER',
      cursor: 'opaque/cursor',
      limit: 25,
      subjectId: '11111111-1111-4111-8111-111111111111',
      subjectType: 'TASK',
    });

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(
      '/api/audit/events?action=TASK_RUNTIME_START_ACCEPTED&actorType=USER&cursor=opaque%2Fcursor&limit=25&subjectId=11111111-1111-4111-8111-111111111111&subjectType=TASK',
    );
  });
});

describe('tasksApi', () => {
  it('does not send a runtime request when the browser has no secure random generator', () => {
    vi.stubGlobal('crypto', {});
    const transport = vi.fn();
    vi.stubGlobal('fetch', transport);
    expect(() => tasksApi.stop({ id: 'task', rowVersion: '1' } as TaskSummary)).toThrow(
      '当前浏览器不支持安全随机数生成，请更新浏览器后重试。',
    );
    expect(transport).not.toHaveBeenCalled();
  });

  it.each(['start', 'stop'] as const)(
    'can %s tasks over HTTP without crypto.randomUUID while preserving unique request identities',
    async (action) => {
      vi.stubGlobal('crypto', {
        getRandomValues: globalThis.crypto.getRandomValues.bind(globalThis.crypto),
      });
      const operation = {
        action: action === 'start' ? 'START' : 'STOP',
        completedAt: null,
        createdAt: '2026-09-18T08:03:00.000Z',
        failureCode: null,
        failureSummary: null,
        id: '88888888-8888-4888-8888-888888888888',
        runId: '99999999-9999-4999-8999-999999999999',
        state: 'EXECUTING',
        taskId: '11111111-1111-4111-8111-111111111111',
        updatedAt: '2026-09-18T08:03:00.000Z',
      };
      const transport = vi.fn(async () => new Response(JSON.stringify(operation)));
      vi.stubGlobal('fetch', transport);
      const task = {
        draft: { id: '22222222-2222-4222-8222-222222222222' },
        id: operation.taskId,
        rowVersion: '7',
      } as TaskSummary;
      await expect(tasksApi[action](task)).resolves.toEqual(operation);
      await expect(tasksApi[action](task)).resolves.toEqual(operation);
      const keys = transport.mock.calls.map((call) => {
        const [, init] = call as unknown as [string, RequestInit];
        return (JSON.parse(String(init.body)) as { idempotencyKey: string }).idempotencyKey;
      });
      expect(new Set(keys).size).toBe(2);
      for (const key of keys) {
        expect(key).toMatch(/^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/u);
      }
    },
  );

  it.each(['KAFKA_PARTITION_SET_CHANGED', null, 42])(
    'preserves a textual failure reason for task notifications: %j',
    async (reason) => {
      vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue(
          new Response(
            JSON.stringify({
              code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH',
              details: { reason },
            }),
            { status: 409 },
          ),
        ),
      );
      await expect(tasksApi.list()).rejects.toMatchObject({
        code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH',
        reason: typeof reason === 'string' ? reason : null,
      });
    },
  );

  it('sends only the current row version when deleting a task', async () => {
    const deletedTask = {
      createdAt: '2026-09-18T08:00:00.000Z',
      currentAppliedRevisionId: null,
      deletedAt: '2026-09-18T08:04:00.000Z',
      desiredState: 'DELETED',
      draft: null,
      id: '11111111-1111-4111-8111-111111111111',
      name: '订单同步',
      observabilityHealth: 'UNAVAILABLE',
      observedState: 'DELETED',
      recoveryDisposition: 'NOT_APPLICABLE',
      rowVersion: '8',
      sourceDataSourceId: '22222222-2222-4222-8222-222222222222',
      targetDataSourceId: '33333333-3333-4333-8333-333333333333',
      taskType: 'KAFKA_KAFKA',
      updatedAt: '2026-09-18T08:04:00.000Z',
    };
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(deletedTask), {
        headers: { 'content-type': 'application/json' },
        status: 200,
      }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const task = {
      id: deletedTask.id,
      rowVersion: '7',
    } as TaskSummary;

    await expect(tasksApi.remove(task)).resolves.toEqual(deletedTask);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(`/api/tasks/${task.id}`);
    expect(init.method).toBe('DELETE');
    expect(JSON.parse(String(init.body))).toEqual({ expectedRowVersion: '7' });
  });

  it('keeps runtime unit identities and metrics ports out of the public start request', async () => {
    const operation = {
      action: 'START',
      completedAt: null,
      createdAt: '2026-09-18T08:03:00.000Z',
      failureCode: null,
      failureSummary: null,
      id: '88888888-8888-4888-8888-888888888888',
      runId: '99999999-9999-4999-8999-999999999999',
      state: 'EXECUTING',
      taskId: '11111111-1111-4111-8111-111111111111',
      updatedAt: '2026-09-18T08:03:00.000Z',
    };
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(operation), {
        headers: { 'content-type': 'application/json' },
        status: 200,
      }),
    );
    vi.stubGlobal('fetch', fetchMock);
    vi.spyOn(globalThis.crypto, 'randomUUID').mockReturnValue(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    );
    const task = {
      draft: { id: '22222222-2222-4222-8222-222222222222' },
      id: operation.taskId,
      rowVersion: '7',
    } as TaskSummary;

    await tasksApi.start(task);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(`/api/tasks/${task.id}/runtime/start`);
    expect(JSON.parse(String(init.body))).toEqual({
      expectedTaskRowVersion: '7',
      idempotencyKey: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      revisionId: task.draft!.id,
    });
  });

  it('serializes the same explicit log time range for list reads', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          facts: [],
          limited: false,
          runId: '22222222-2222-4222-8222-222222222222',
          taskId: '11111111-1111-4111-8111-111111111111',
        }),
        {
          headers: { 'content-type': 'application/json' },
          status: 200,
        },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    await tasksApi.logs(
      '11111111-1111-4111-8111-111111111111',
      '22222222-2222-4222-8222-222222222222',
      { from: '2026-09-08T00:00:00.000Z', to: '2026-09-09T00:00:00.000Z' },
    );

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('from=2026-09-08T00%3A00%3A00.000Z');
    expect(url).toContain('to=2026-09-09T00%3A00%3A00.000Z');
  });

  it('serializes the same explicit log time range for downloads', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response('download', {
        headers: { 'content-disposition': 'attachment; filename="runtime-run.log"' },
        status: 200,
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await tasksApi.downloadLogs(
      '11111111-1111-4111-8111-111111111111',
      '22222222-2222-4222-8222-222222222222',
      { from: '2026-09-08T00:00:00.000Z', to: '2026-09-09T00:00:00.000Z' },
    );

    const [url] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('from=2026-09-08T00%3A00%3A00.000Z');
    expect(url).toContain('to=2026-09-09T00%3A00%3A00.000Z');
  });
});
