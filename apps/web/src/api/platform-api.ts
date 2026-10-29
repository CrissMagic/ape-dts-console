import {
  configBundleSchema,
  configImportPreviewSchema,
  configImportResultSchema,
  type ConfigBundle,
  type ConfigExportOptions,
  sessionSettingsSchema,
  runtimeRecoverySettingsSchema,
  type UpdateRuntimeRecoverySettings,
  type UpdateSessionSettingsRequest,
  alertSettingsSchema,
  alertDeliverySchema,
  alertDeliveryListSchema,
  alertDeliveryContextSchema,
  type UpdateAlertSettingsRequest,
  activateUserResponseSchema,
  artifactListResponseSchema,
  deleteArtifactResponseSchema,
  artifactReferencesSchema,
  artifactSummarySchema,
  artifactFileListSchema,
  preparedArtifactFileSchema,
  uploadArtifactFileResponseSchema,
  type ArtifactFileCandidate,
  type RegisterArtifactFileRequest,
  type RegisterArtifactRequest,
  auditEventListResponseSchema,
  authenticatedSessionResponseSchema,
  activationGrantResponseSchema,
  connectionTestResponseSchema,
  type CopyDataSourceRequest,
  createKafkaTopicResponseSchema,
  dataSourceGrantListResponseSchema,
  dataSourceGrantSchema,
  dataSourceListResponseSchema,
  dataSourceSummarySchema,
  loginResponseSchema,
  metadataResponseSchema,
  mysqlCdcInspectionSchema,
  platformRuntimePhaseSchema,
  preparePgResourcesResponseSchema,
  runtimeOperationSummarySchema,
  runtimeReadSchema,
  runtimeRunListResponseSchema,
  runtimePerformanceQueryResponseSchema,
  runtimePerformanceDetailSchema,
  runtimeLogReadSchema,
  runtimeLogStorageRetryResponseSchema,
  taskPreflightRunSummarySchema,
  taskPreflightCheckSchema,
  taskRevisionRiskAcceptanceSchema,
  taskListResponseSchema,
  taskSummarySchema,
  taskGroupListResponseSchema,
  taskGroupSummarySchema,
  deleteTaskGroupResponseSchema,
  taskProgressSchema,
  userListResponseSchema,
  userMutationResponseSchema,
  type ActivateUserRequest,
  type AuditEventListQuery,
  type ChangeUserRoleRequest,
  type CreateUserRequest,
  type CreateDataSourceRequest,
  type CreateTaskRequest,
  type CreateKafkaTopicRequest,
  type DataSourceSummary,
  type DeleteTaskRequest,
  type LoginRequest,
  type MetadataQuery,
  type PlatformRuntimePhase,
  type RuntimePerformanceWindow,
  type TaskSummary,
  type SaveTaskDraftRequest,
  type UpdateDataSourceRequest,
  type UserSummary,
} from '@scs/contracts';
import { z, type ZodType } from 'zod';

export class PlatformApiError extends Error {
  artifactReferences: z.infer<typeof artifactReferencesSchema> = [];
  reason: string | null = null;
  constructor(
    readonly code: string,
    readonly status: number,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'PlatformApiError';
  }
}

let csrfToken: string | null = null;

export const configTransferApi = {
  export: (options: ConfigExportOptions) =>
    request('/settings/config-transfer/export', configBundleSchema, post(options), true),
  preview: (bundle: ConfigBundle) =>
    request('/settings/config-transfer/preview', configImportPreviewSchema, post(bundle), true),
  import: (bundle: ConfigBundle, expectedPreviewHash: string) =>
    request(
      '/settings/config-transfer/import',
      configImportResultSchema,
      post({ bundle, expectedPreviewHash }),
      true,
    ),
};

export const taskProgressApi = {
  list: () => request('/tasks/progress', z.array(taskProgressSchema)),
};

export const taskGroupsApi = {
  list() {
    return request('/task-groups', taskGroupListResponseSchema);
  },
  create(name: string) {
    return request('/task-groups', taskGroupSummarySchema, post({ name }), true);
  },
  rename(group: { id: string; name: string }, name: string) {
    return request(
      `/task-groups/${encodeURIComponent(group.id)}`,
      taskGroupSummarySchema,
      { method: 'PATCH', body: JSON.stringify({ name, expectedName: group.name }) },
      true,
    );
  },
  delete(group: { id: string; name: string }, detachTasks: boolean) {
    return request(
      `/task-groups/${encodeURIComponent(group.id)}`,
      deleteTaskGroupResponseSchema,
      { method: 'DELETE', body: JSON.stringify({ expectedName: group.name, detachTasks }) },
      true,
    );
  },
  reorder(groupIds: string[], expectedGroupIds: string[]) {
    return request(
      '/task-groups/order',
      taskGroupListResponseSchema,
      { method: 'PATCH', body: JSON.stringify({ groupIds, expectedGroupIds }) },
      true,
    );
  },
  assign(task: TaskSummary, groupId: string | null) {
    return request(
      `/tasks/${encodeURIComponent(task.id)}/group`,
      taskSummarySchema,
      { method: 'PATCH', body: JSON.stringify({ groupId, expectedGroupId: task.groupId ?? null }) },
      true,
    );
  },
};

async function request<T>(
  path: string,
  schema: ZodType<T>,
  init?: RequestInit,
  requireCsrf = false,
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      cache: 'no-store',
      credentials: 'same-origin',
      ...init,
      headers: {
        ...(init?.body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(requireCsrf && csrfToken !== null ? { 'x-csrf-token': csrfToken } : {}),
        ...init?.headers,
      },
    });
  } catch (cause) {
    throw new PlatformApiError('NETWORK_ERROR', 0, { cause });
  }
  const payload = (await response.json().catch(() => null)) as unknown;
  if (!response.ok) {
    const code =
      typeof payload === 'object' &&
      payload !== null &&
      'code' in payload &&
      typeof payload.code === 'string'
        ? payload.code
        : `HTTP_${response.status}`;
    const error = new PlatformApiError(code, response.status);
    if (typeof payload === 'object' && payload !== null && 'details' in payload) {
      const details = z.object({ reason: z.string() }).safeParse(payload.details);
      if (details.success) error.reason = details.data.reason;
    }
    if (
      code === 'ARTIFACT_IN_USE' &&
      typeof payload === 'object' &&
      payload !== null &&
      'tasks' in payload
    ) {
      const references = artifactReferencesSchema.safeParse(payload.tasks);
      if (references.success) error.artifactReferences = references.data;
    }
    throw error;
  }
  const parsed = schema.safeParse(payload);
  if (!parsed.success) {
    throw new PlatformApiError('INVALID_SERVER_RESPONSE', response.status, {
      cause: parsed.error,
    });
  }
  return parsed.data;
}

async function requestWithoutBody(
  path: string,
  init: RequestInit,
  requireCsrf: boolean,
): Promise<void> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      cache: 'no-store',
      credentials: 'same-origin',
      ...init,
      headers: {
        ...(requireCsrf && csrfToken !== null ? { 'x-csrf-token': csrfToken } : {}),
        ...init.headers,
      },
    });
  } catch (cause) {
    throw new PlatformApiError('NETWORK_ERROR', 0, { cause });
  }
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as unknown;
    const code =
      typeof payload === 'object' &&
      payload !== null &&
      'code' in payload &&
      typeof payload.code === 'string'
        ? payload.code
        : `HTTP_${response.status}`;
    throw new PlatformApiError(code, response.status);
  }
}

async function downloadRequest(path: string): Promise<{ blob: Blob; filename: string }> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, { cache: 'no-store', credentials: 'same-origin' });
  } catch (cause) {
    throw new PlatformApiError('NETWORK_ERROR', 0, { cause });
  }
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { code?: unknown } | null;
    throw new PlatformApiError(
      typeof payload?.code === 'string' ? payload.code : `HTTP_${response.status}`,
      response.status,
    );
  }
  const disposition = response.headers.get('content-disposition') ?? '';
  const filename = /filename="([A-Za-z0-9._-]+)"/u.exec(disposition)?.[1];
  if (filename === undefined || !filename.endsWith('.log'))
    throw new PlatformApiError('INVALID_SERVER_RESPONSE', response.status);
  return { blob: await response.blob(), filename };
}

function post(body: unknown): RequestInit {
  return { body: JSON.stringify(body), method: 'POST' };
}

function runtimeRequestId(): string {
  const random = globalThis.crypto;
  if (typeof random?.randomUUID === 'function') return random.randomUUID();
  // getRandomValues is also available on HTTP origins, unlike randomUUID.
  if (typeof random?.getRandomValues !== 'function') {
    throw new Error('当前浏览器不支持安全随机数生成，请更新浏览器后重试。');
  }
  const bytes = random.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function queryString(values: Readonly<Record<string, string | number | undefined>>): string {
  const parameters = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) parameters.set(key, String(value));
  }
  const encoded = parameters.toString();
  return encoded.length === 0 ? '' : `?${encoded}`;
}

export const auditApi = {
  list(query: AuditEventListQuery) {
    return request(
      `/audit/events${queryString({
        action: query.action,
        actorType: query.actorType,
        cursor: query.cursor,
        limit: query.limit,
        subjectId: query.subjectId,
        subjectType: query.subjectType,
      })}`,
      auditEventListResponseSchema,
    );
  },
};

export function listArtifacts() {
  return request('/artifacts', artifactListResponseSchema);
}
export function deleteArtifact(id: string) {
  return request(
    `/artifacts/${encodeURIComponent(id)}`,
    deleteArtifactResponseSchema,
    { method: 'DELETE' },
    true,
  );
}
export function listArtifactFiles() {
  return request('/artifacts/files', artifactFileListSchema);
}
export function prepareArtifactFile(file: ArtifactFileCandidate) {
  return request('/artifacts/files/prepare', preparedArtifactFileSchema, post(file), true);
}
export function uploadArtifactFile(
  file: File,
  options: { signal: AbortSignal; onProgress: (percent: number) => void },
): Promise<z.infer<typeof uploadArtifactFileResponseSchema>> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const abort = () => {
      xhr.abort();
      reject(new PlatformApiError('UPLOAD_CANCELLED', 0));
    };
    if (options.signal.aborted) {
      abort();
      return;
    }
    xhr.open('POST', '/api/artifacts/files/upload');
    xhr.setRequestHeader('content-type', 'application/octet-stream');
    xhr.setRequestHeader('x-artifact-filename', encodeURIComponent(file.name));
    if (csrfToken !== null) xhr.setRequestHeader('x-csrf-token', csrfToken);
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable)
        options.onProgress(Math.min(100, Math.round((event.loaded / event.total) * 100)));
    };
    xhr.onload = () => {
      let payload: unknown;
      try {
        payload = JSON.parse(xhr.responseText);
      } catch {
        payload = null;
      }
      if (xhr.status < 200 || xhr.status >= 300) {
        const code =
          typeof payload === 'object' &&
          payload !== null &&
          'code' in payload &&
          typeof payload.code === 'string'
            ? payload.code
            : `HTTP_${xhr.status}`;
        reject(new PlatformApiError(code, xhr.status));
        return;
      }
      const parsed = uploadArtifactFileResponseSchema.safeParse(payload);
      if (!parsed.success) reject(new PlatformApiError('INVALID_SERVER_RESPONSE', xhr.status));
      else resolve(parsed.data);
    };
    xhr.onerror = () => reject(new PlatformApiError('NETWORK_ERROR', 0));
    xhr.onabort = () => reject(new PlatformApiError('UPLOAD_CANCELLED', 0));
    xhr.onloadend = () => options.signal.removeEventListener('abort', abort);
    options.signal.addEventListener('abort', abort, { once: true });
    xhr.send(file);
  });
}
export function registerArtifactFile(body: RegisterArtifactFileRequest) {
  return request('/artifacts/files/register', artifactSummarySchema, post(body), true);
}
export function registerArtifact(body: RegisterArtifactRequest) {
  return request('/artifacts', artifactSummarySchema, post(body), true);
}
export function verifyArtifact(id: string) {
  return request(
    `/artifacts/${encodeURIComponent(id)}/verify`,
    artifactSummarySchema,
    post({}),
    true,
  );
}
export function activateArtifact(id: string) {
  return request(
    `/artifacts/${encodeURIComponent(id)}/activate`,
    artifactSummarySchema,
    post({}),
    true,
  );
}
const runtimeCapacitySchema = z.object({
  maxRuntimeUnits: z.number().int().positive().nullable(),
  rowVersion: z.string(),
  used: z.number().int().nonnegative(),
});
export const runtimeCapacityApi = {
  environment() {
    return request('/settings/runtime-capacity/environment', taskPreflightCheckSchema);
  },
  get() {
    return request('/settings/runtime-capacity', runtimeCapacitySchema);
  },
  update(body: { maxRuntimeUnits: number; expectedRowVersion: string }) {
    return request(
      '/settings/runtime-capacity',
      runtimeCapacitySchema,
      { method: 'PATCH', body: JSON.stringify(body) },
      true,
    );
  },
};

export const runtimeRecoverySettingsApi = {
  get: () => request('/settings/runtime-recovery', runtimeRecoverySettingsSchema),
  update: (body: UpdateRuntimeRecoverySettings) =>
    request(
      '/settings/runtime-recovery',
      runtimeRecoverySettingsSchema,
      { method: 'PATCH', body: JSON.stringify(body) },
      true,
    ),
};

export const sessionSettingsApi = {
  get() {
    return request('/settings/sessions', sessionSettingsSchema);
  },
  update(body: UpdateSessionSettingsRequest) {
    return request(
      '/settings/sessions',
      sessionSettingsSchema,
      { method: 'PATCH', body: JSON.stringify(body) },
      true,
    );
  },
};

export const performanceApi = {
  query(taskIds: readonly string[]) {
    return request(
      '/tasks/performance/query',
      runtimePerformanceQueryResponseSchema,
      { ...post({ taskIds }), signal: AbortSignal.timeout(10000) },
      true,
    );
  },
  detail(taskId: string, window: RuntimePerformanceWindow = '1h', runId?: string) {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/performance?window=${window}${runId === undefined ? '' : `&runId=${encodeURIComponent(runId)}`}`,
      runtimePerformanceDetailSchema,
      { signal: AbortSignal.timeout(10000) },
    );
  },
};

export const authApi = {
  async login(body: LoginRequest) {
    const result = await request('/auth/login', loginResponseSchema, post(body));
    if (result.status === 'AUTHENTICATED') {
      csrfToken = result.csrfToken;
    }
    return result;
  },

  async session() {
    const result = await request('/auth/session', authenticatedSessionResponseSchema);
    csrfToken = result.csrfToken;
    return result;
  },

  async changePassword(body: { currentPassword: string; newPassword: string }) {
    const result = await request(
      '/auth/password/change',
      authenticatedSessionResponseSchema,
      post(body),
      true,
    );
    csrfToken = result.csrfToken;
    return result;
  },

  async logout() {
    await requestWithoutBody('/auth/logout', { method: 'POST' }, true);
    csrfToken = null;
  },
};

export const usersApi = {
  list() {
    return request('/users', userListResponseSchema);
  },

  create(body: CreateUserRequest) {
    return request('/users', activationGrantResponseSchema, post(body), true);
  },

  activate(body: ActivateUserRequest) {
    return request('/users/activate', activateUserResponseSchema, post(body));
  },

  changeStatus(user: UserSummary, status: 'ACTIVE' | 'DISABLED') {
    return request(
      `/users/${user.id}/status`,
      userMutationResponseSchema,
      { body: JSON.stringify({ status }), method: 'PATCH' },
      true,
    );
  },

  changeRole(user: UserSummary, role: ChangeUserRoleRequest['role']) {
    return request(
      `/users/${user.id}/role`,
      z.union([activationGrantResponseSchema, userMutationResponseSchema]),
      { body: JSON.stringify({ role }), method: 'PATCH' },
      true,
    );
  },

  resetCredentials(user: UserSummary) {
    return request(
      `/users/${user.id}/credentials/reset`,
      activationGrantResponseSchema,
      post({}),
      true,
    );
  },

  revokeSessions(user: UserSummary) {
    return requestWithoutBody(`/users/${user.id}/sessions/revoke`, { method: 'POST' }, true);
  },
};

export const dataSourcesApi = {
  createTopic(
    dataSource: DataSourceSummary,
    body: Omit<CreateKafkaTopicRequest, 'expectedRowVersion'>,
  ) {
    return request(
      `/data-sources/${dataSource.id}/topics`,
      createKafkaTopicResponseSchema,
      post({ ...body, expectedRowVersion: dataSource.rowVersion }),
      true,
    );
  },
  list() {
    return request('/data-sources', dataSourceListResponseSchema);
  },

  create(body: CreateDataSourceRequest) {
    return request('/data-sources', dataSourceSummarySchema, post(body), true);
  },

  copy(dataSource: DataSourceSummary, body: Omit<CopyDataSourceRequest, 'expectedRowVersion'>) {
    return request(
      `/data-sources/${dataSource.id}/copy`,
      dataSourceSummarySchema,
      post({ ...body, expectedRowVersion: dataSource.rowVersion }),
      true,
    );
  },

  update(dataSource: DataSourceSummary, body: UpdateDataSourceRequest) {
    return request(
      `/data-sources/${dataSource.id}`,
      dataSourceSummarySchema,
      { body: JSON.stringify(body), method: 'PATCH' },
      true,
    );
  },

  remove(dataSource: DataSourceSummary) {
    return request(
      `/data-sources/${dataSource.id}`,
      dataSourceSummarySchema,
      { body: JSON.stringify({ expectedRowVersion: dataSource.rowVersion }), method: 'DELETE' },
      true,
    );
  },

  listGrants(dataSource: DataSourceSummary) {
    return request(`/data-sources/${dataSource.id}/grants`, dataSourceGrantListResponseSchema);
  },

  grant(dataSource: DataSourceSummary, userId: string) {
    return request(
      `/data-sources/${dataSource.id}/grants`,
      dataSourceGrantSchema,
      post({ userId }),
      true,
    );
  },

  revoke(dataSource: DataSourceSummary, userId: string) {
    return requestWithoutBody(
      `/data-sources/${dataSource.id}/grants/${userId}`,
      { method: 'DELETE' },
      true,
    );
  },

  test(dataSource: DataSourceSummary) {
    return request(
      `/data-sources/${dataSource.id}/test`,
      connectionTestResponseSchema,
      post({}),
      true,
    );
  },

  inspectMysqlCdc(dataSource: DataSourceSummary) {
    return request(
      `/data-sources/${dataSource.id}/mysql-cdc-check`,
      mysqlCdcInspectionSchema,
      post({ expectedRowVersion: dataSource.rowVersion }),
      true,
    );
  },

  metadata(dataSource: DataSourceSummary, query: MetadataQuery) {
    return request(
      `/data-sources/${dataSource.id}/metadata`,
      metadataResponseSchema,
      post(query),
      true,
    );
  },
};

export const tasksApi = {
  preparePgResources(task: TaskSummary) {
    if (task.draft === null) throw new Error('TASK_DRAFT_NOT_FOUND');
    return request(
      `/tasks/${encodeURIComponent(task.id)}/pg-resources/prepare`,
      preparePgResourcesResponseSchema,
      post({
        revisionId: task.draft.id,
        expectedTaskRowVersion: task.rowVersion,
        expectedDraftVersion: task.draft.draftVersion,
        acknowledgeReplicationRisk: true,
      }),
      true,
    );
  },
  list() {
    return request('/tasks', taskListResponseSchema);
  },

  create(body: CreateTaskRequest) {
    return request('/tasks', taskSummarySchema, post(body), true);
  },

  remove(task: TaskSummary) {
    const body: DeleteTaskRequest = { expectedRowVersion: task.rowVersion };
    return request(
      `/tasks/${encodeURIComponent(task.id)}`,
      taskSummarySchema,
      { body: JSON.stringify(body), method: 'DELETE' },
      true,
    );
  },

  saveDraft(
    task: TaskSummary,
    configuration: Record<string, unknown>,
    artifactId?: string | null,
    acknowledgePgTableAddition?: true,
  ) {
    const revision = task.draft ?? task.applied;
    if (revision == null) throw new Error('TASK_DRAFT_NOT_FOUND');
    const body: SaveTaskDraftRequest = {
      configuration,
      ...(artifactId === undefined ? {} : { artifactId }),
      ...(acknowledgePgTableAddition === true ? { acknowledgePgTableAddition: true } : {}),
      expectedDraftVersion: revision.draftVersion,
      expectedTaskRowVersion: task.rowVersion,
      revisionId: revision.id,
      taskType: task.taskType,
    };
    return request(
      `/tasks/${encodeURIComponent(task.id)}/draft`,
      taskSummarySchema,
      { body: JSON.stringify(body), method: 'PATCH' },
      true,
    );
  },

  preflight(task: TaskSummary, options?: { reinitializeMysqlSnapshot?: boolean }) {
    if (task.draft === null) throw new Error('TASK_DRAFT_NOT_FOUND');
    return request(
      `/tasks/${encodeURIComponent(task.id)}/preflight`,
      taskPreflightRunSummarySchema,
      post({
        expectedTaskRowVersion: task.rowVersion,
        revisionId: task.draft.id,
        ...(options?.reinitializeMysqlSnapshot === true ? { reinitializeMysqlSnapshot: true } : {}),
      }),
      true,
    );
  },

  acceptRisk(task: TaskSummary, note?: string) {
    if (task.draft === null) throw new Error('TASK_DRAFT_NOT_FOUND');
    return request(
      `/tasks/${encodeURIComponent(task.id)}/risk-acceptance`,
      taskRevisionRiskAcceptanceSchema,
      post({
        expectedDraftVersion: task.draft.draftVersion,
        expectedTaskRowVersion: task.rowVersion,
        ...(note === undefined ? {} : { note }),
        revisionId: task.draft.id,
      }),
      true,
    );
  },

  start(task: TaskSummary, options?: { reinitializeMysqlSnapshot?: boolean }) {
    if (task.draft === null) throw new Error('TASK_DRAFT_NOT_FOUND');
    return request(
      `/tasks/${encodeURIComponent(task.id)}/runtime/start`,
      runtimeOperationSummarySchema,
      post({
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: runtimeRequestId(),
        revisionId: task.draft.id,
        ...(options?.reinitializeMysqlSnapshot === true ? { reinitializeMysqlSnapshot: true } : {}),
      }),
      true,
    );
  },

  stop(task: TaskSummary) {
    return request(
      `/tasks/${task.id}/runtime/stop`,
      runtimeOperationSummarySchema,
      post({
        expectedTaskRowVersion: task.rowVersion,
        idempotencyKey: runtimeRequestId(),
      }),
      true,
    );
  },

  discardRecovery(task: TaskSummary) {
    return request(
      `/tasks/${encodeURIComponent(task.id)}/runtime/discard-recovery`,
      z.object({ discarded: z.literal(true) }).strict(),
      post({ expectedTaskRowVersion: task.rowVersion, oldRuntimeStoppedConfirmed: true }),
      true,
    );
  },

  runtime(taskId: string) {
    return request(`/tasks/${encodeURIComponent(taskId)}/runtime`, runtimeReadSchema);
  },

  runtimeRuns(taskId: string) {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/runtime/runs`,
      runtimeRunListResponseSchema,
    );
  },

  logs(
    taskId: string,
    runId: string,
    filter: {
      from?: string;
      level?: 'ERROR' | 'WARN' | 'INFO';
      order?: 'ASC' | 'DESC';
      stream?: 'STDOUT' | 'STDERR';
      to?: string;
      unitKey?: string;
    } = {},
  ) {
    const query = new URLSearchParams({ limit: '500' });
    if (filter.level !== undefined) query.set('level', filter.level);
    if (filter.order !== undefined) query.set('order', filter.order);
    if (filter.from !== undefined) query.set('from', filter.from);
    if (filter.stream !== undefined) query.set('stream', filter.stream);
    if (filter.to !== undefined) query.set('to', filter.to);
    if (filter.unitKey !== undefined) query.set('unitKey', filter.unitKey);
    return request(
      `/tasks/${encodeURIComponent(taskId)}/runtime/${encodeURIComponent(runId)}/logs?${query}`,
      runtimeLogReadSchema,
    );
  },

  downloadLogs(
    taskId: string,
    runId: string,
    filter: {
      from?: string;
      level?: 'ERROR' | 'WARN' | 'INFO';
      stream?: 'STDOUT' | 'STDERR';
      to?: string;
      unitKey?: string;
    } = {},
  ) {
    const query = new URLSearchParams();
    if (filter.level !== undefined) query.set('level', filter.level);
    if (filter.from !== undefined) query.set('from', filter.from);
    if (filter.stream !== undefined) query.set('stream', filter.stream);
    if (filter.to !== undefined) query.set('to', filter.to);
    if (filter.unitKey !== undefined) query.set('unitKey', filter.unitKey);
    return downloadRequest(
      `/tasks/${encodeURIComponent(taskId)}/runtime/${encodeURIComponent(runId)}/logs/download?${query}`,
    );
  },

  retryLogStorage(taskId: string, runId: string) {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/runtime/${encodeURIComponent(runId)}/logs/retry`,
      runtimeLogStorageRetryResponseSchema,
      post({}),
      true,
    );
  },
};

const recoveryStepTypeSchema = z.enum([
  'LOAD_METADATA',
  'UNLOCK_KEYS',
  'ENSURE_ARTIFACTS',
  'VERIFY_CONTINUITY',
  'PLAN_CAPACITY',
  'RESTORE_BATCHES',
  'READY_OBSERVABILITY',
]);
const recoverySessionSchema = z
  .object({
    candidates: z.array(
      z.object({
        revisionId: z.uuid(),
        taskId: z.uuid(),
        taskName: z.string().min(1),
        taskType: z.enum(['PG_KAFKA', 'MYSQL_KAFKA', 'KAFKA_KAFKA']),
        unitKeys: z.array(z.string().min(1)),
        units: z.number().int().nonnegative(),
      }),
    ),
    id: z.uuid(),
    rowVersion: z.string().regex(/^[1-9]\d*$/u),
    state: z.enum(['RUNNING', 'READY', 'FAILED', 'PAUSED']),
    steps: z.array(
      z.object({
        attemptCount: z.number().int().nonnegative(),
        errorSummary: z.string().nullable(),
        failureCode: z.string().nullable(),
        position: z.number().int().positive(),
        progress: z.record(z.string(), z.unknown()),
        status: z.enum(['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PAUSED']),
        stepType: recoveryStepTypeSchema,
      }),
    ),
  })
  .strict();

export type WebRecoverySession = z.infer<typeof recoverySessionSchema>;
export type WebRecoveryStepType = z.infer<typeof recoveryStepTypeSchema>;

export const recoveryApi = {
  retryTask(task: TaskSummary) {
    return request(
      `/recovery-sessions/tasks/${encodeURIComponent(task.id)}/retry`,
      z.union([
        recoverySessionSchema,
        z.object({ operation: runtimeOperationSummarySchema }).strict(),
      ]),
      post({ expectedTaskRowVersion: task.rowVersion, oldRuntimeStoppedConfirmed: true }),
      true,
    );
  },
  current() {
    return request('/recovery-sessions/current', recoverySessionSchema);
  },

  get(sessionId: string) {
    return request(`/recovery-sessions/${sessionId}`, recoverySessionSchema);
  },

  retry(sessionId: string, stepType: WebRecoveryStepType) {
    return request(
      `/recovery-sessions/${sessionId}/retry`,
      recoverySessionSchema,
      post({ stepType }),
      true,
    );
  },

  continueAfterFailedTask(session: WebRecoverySession) {
    return request(
      `/recovery-sessions/${session.id}/continue`,
      recoverySessionSchema,
      post({ expectedSessionRowVersion: session.rowVersion }),
      true,
    );
  },

  confirmOldRuntimeStopped(session: WebRecoverySession, reason: string) {
    return request(
      `/recovery-sessions/${session.id}/confirm-old-runtime-stopped`,
      recoverySessionSchema,
      post({ expectedSessionRowVersion: session.rowVersion, reason }),
      true,
    );
  },

  selectCapacityCandidates(session: WebRecoverySession, taskIds: readonly string[]) {
    return request(
      `/recovery-sessions/${session.id}/capacity-selection`,
      recoverySessionSchema,
      post({ expectedSessionRowVersion: session.rowVersion, taskIds }),
      true,
    );
  },
};

const emergencyRuntimeTaskListSchema = z.array(
  z.object({
    runId: z.string().min(1),
    taskId: z.string().min(1),
    unitCount: z.number().int().nonnegative(),
  }),
);
export type EmergencyRuntimeTaskView = z.infer<typeof emergencyRuntimeTaskListSchema>[number];

export const emergencyApi = {
  list(emergencyCode: string) {
    return request('/emergency/runtime-tasks', emergencyRuntimeTaskListSchema, {
      headers: { 'x-emergency-stop-code': emergencyCode },
    });
  },

  stop(emergencyCode: string, taskId: string) {
    return requestWithoutBody(
      `/emergency/runtime-tasks/${encodeURIComponent(taskId)}/stop`,
      { headers: { 'x-emergency-stop-code': emergencyCode }, method: 'POST' },
      false,
    );
  },

  forceKill(emergencyCode: string, taskId: string, reason: string) {
    return requestWithoutBody(
      `/emergency/runtime-tasks/${encodeURIComponent(taskId)}/force-kill`,
      {
        ...post({ confirmed: true, reason }),
        headers: { 'x-emergency-stop-code': emergencyCode },
      },
      false,
    );
  },
};

export const healthApi = {
  async phase(): Promise<PlatformRuntimePhase> {
    const response = await fetch('/api/health/ready', { cache: 'no-store' });
    const payload = (await response.json().catch(() => null)) as unknown;
    if (response.ok) return 'READY';
    const parsed = z.object({ phase: platformRuntimePhaseSchema }).safeParse(payload);
    if (!parsed.success) throw new PlatformApiError('INVALID_SERVER_RESPONSE', response.status);
    return parsed.data.phase;
  },
};

export const emptyResponseSchema = z.object({}).passthrough();

export const alertsApi = {
  get() {
    return request('/settings/alerts', alertSettingsSchema);
  },
  update(body: UpdateAlertSettingsRequest) {
    return request(
      '/settings/alerts',
      alertSettingsSchema,
      { ...post(body), method: 'PATCH' },
      true,
    );
  },
  test() {
    return request('/settings/alerts/test', alertDeliverySchema, post({}), true);
  },
  deliveries() {
    return request('/settings/alerts/deliveries', alertDeliveryListSchema);
  },
  context(deliveryId: string) {
    return request(
      `/settings/alerts/deliveries/${encodeURIComponent(deliveryId)}/context`,
      alertDeliveryContextSchema,
    );
  },
};
