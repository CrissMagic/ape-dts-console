import { describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { KafkaTopicAuditError } from '../data-sources/data-sources.service.js';
import type { CapacityReservationService } from '../pg-resources/capacity-reservation.service.js';
import {
  PreflightServiceError,
  type SystemRecoveryPreflightPort,
} from '../preflight/preflight.service.js';
import {
  runtimeStageTypes,
  RuntimeSupervisorService,
  type RuntimeOperationSummary,
} from './runtime-supervisor.service.js';
import type { RuntimeSupervisorError } from './runtime-supervisor.service.js';
import type { RuntimeProcessSupervisor } from './process-supervisor.js';
import type { RuntimeLaunchSpecFactory } from './runtime-launch-spec.js';
import {
  RuntimeMetricsPortAllocatorError,
  type RuntimeMetricsPortAllocator,
} from './runtime-metrics-port-allocator.js';
import type { TaskPreflightResult, UserSummary } from '@scs/contracts';
import type { PlatformTaskConfig } from '@scs/config-model';

const taskId = '11111111-1111-4111-8111-111111111111';
const revisionId = '22222222-2222-4222-8222-222222222222';
const sourceId = '33333333-3333-4333-8333-333333333333';
const targetId = '44444444-4444-4444-8444-444444444444';

const administrator: UserSummary = {
  displayName: 'Runtime test administrator',
  id: '55555555-5555-4555-8555-555555555555',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'runtime-test-admin',
};

const developer: UserSummary = {
  displayName: 'Runtime test developer',
  id: '77777777-7777-4777-8777-777777777777',
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: 'runtime-test-developer',
};

const validTask = {
  current_applied_revision_id: null,
  current_draft_revision_id: revisionId,
  deleted_at: null,
  desired_state: 'STOPPED' as const,
  id: taskId,
  observed_state: 'STOPPED' as const,
  row_version: '1',
  source_data_source_id: sourceId,
  target_data_source_id: targetId,
};

const validRevision = {
  artifact_id: '66666666-6666-4666-8666-666666666666',
  configuration: {
    kind: 'KAFKA_RAW_FORWARD',
    schemaVersion: 1,
    source: { dataSourceId: sourceId, groupId: 'runtime-test-group', topic: 'source.events' },
    target: { dataSourceId: targetId, topic: 'target.events' },
    tuning: {
      ackIntervalSeconds: 5,
      batchSize: 100,
      bufferSize: 16_000,
      checkpointIntervalSeconds: 15,
      requiredAcks: 'all',
    },
  },
  draft_version: '1',
  id: revisionId,
  state: 'RISK_CONFIRMED',
  task_id: taskId,
};

const snapshotConfiguration: PlatformTaskConfig = {
  kind: 'PG_TO_KAFKA',
  schemaVersion: 1,
  source: {
    dataSourceId: sourceId,
    includeSnapshot: true,
    publicationName: 'runtime_publication',
    resourceMode: 'PLATFORM_MANAGED',
    slotName: 'runtime_slot',
    table: { name: 'events', schema: 'public' },
  },
  target: { dataSourceId: targetId, topic: 'runtime.events' },
  tuning: {
    batchSize: 100,
    bufferSize: 16_000,
    checkpointIntervalSeconds: 15,
    requiredAcks: 'all',
  },
};

interface FakePool {
  readonly connect: ReturnType<typeof vi.fn>;
  readonly query: ReturnType<typeof vi.fn>;
  readonly writes: string[];
  readonly committedWrites: string[];
  readonly rollback: ReturnType<typeof vi.fn>;
  readonly commit: ReturnType<typeof vi.fn>;
}

interface FakePoolOptions {
  readonly existingOperation?: boolean;
  readonly existingOperationAction?: 'START' | 'STOP';
  readonly grantCount?: string;
  readonly missingTask?: boolean;
  readonly preflight?: boolean;
  readonly preflightResult?: unknown;
  readonly riskAcceptance?: boolean;
  readonly revision?: Omit<Partial<typeof validRevision>, 'configuration'> & {
    readonly configuration?: PlatformTaskConfig;
  };
  readonly task?: Partial<typeof validTask>;
}

function fakePool(options: FakePoolOptions = {}): FakePool {
  const writes: string[] = [];
  const committedWrites: string[] = [];
  let inTransaction = false;
  const rollback = vi.fn(async () => {
    writes.length = 0;
    inTransaction = false;
    return { rows: [], rowCount: 0 };
  });
  const commit = vi.fn(async () => {
    committedWrites.push(...writes);
    writes.length = 0;
    inTransaction = false;
    return { rows: [], rowCount: 0 };
  });
  const task = { ...validTask, ...options.task };
  const revision = { ...validRevision, ...options.revision };
  const dispatch = async (sql: string): Promise<{ rows: unknown[]; rowCount: number }> => {
    const normalized = sql.replace(/\s+/gu, ' ').trim().toLowerCase();
    if (normalized === 'begin') {
      inTransaction = true;
      return { rows: [], rowCount: 0 };
    }
    if (normalized === 'commit') {
      return await commit();
    }
    if (normalized === 'rollback') {
      return await rollback();
    }
    if (/^(insert|update|delete)\b/u.test(normalized)) {
      if (inTransaction) {
        writes.push(normalized);
      }
      return { rows: [], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.tasks')) {
      return options.missingTask === true
        ? { rows: [], rowCount: 0 }
        : { rows: [task], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.data_source_grants')) {
      return { rows: [{ count: options.grantCount ?? '0' }], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.task_operations')) {
      return options.existingOperation === true
        ? {
            rows: [
              {
                action: options.existingOperationAction ?? 'START',
                completed_at: new Date('2026-09-03T00:00:01.000Z'),
                created_at: new Date('2026-09-03T00:00:00.000Z'),
                failure_code: null,
                failure_summary: null,
                id: '88888888-8888-4888-8888-888888888888',
                run_id: '99999999-9999-4999-8999-999999999999',
                state: 'SUCCEEDED',
                task_id: taskId,
                updated_at: new Date('2026-09-03T00:00:01.000Z'),
              },
            ],
            rowCount: 1,
          }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.task_revisions')) {
      return { rows: [revision], rowCount: 1 };
    }
    if (normalized.includes('from ape_dts_platform.risk_acceptances')) {
      return options.riskAcceptance === true
        ? { rows: [{ accepted_at: new Date('2026-09-03T00:00:00.000Z'), id: 'risk' }], rowCount: 1 }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.preflight_runs')) {
      return options.preflight === true
        ? {
            rows: [
              {
                completed_at: new Date('2026-09-03T00:00:00.000Z'),
                id: 'preflight',
                result: options.preflightResult ?? {},
                state: 'PASSED',
              },
            ],
            rowCount: 1,
          }
        : { rows: [], rowCount: 0 };
    }
    if (normalized.includes('from ape_dts_platform.task_run_units')) {
      return { rows: [], rowCount: 0 };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  };
  const query = vi.fn(dispatch);
  const connect = vi.fn(async () => ({
    query,
    release: vi.fn(),
  }));
  return { commit, committedWrites, connect, query, rollback, writes };
}

interface ServiceOptions {
  readonly prepareFirstStartTopic?: SystemRecoveryPreflightPort['prepareFirstStartTopic'];
  readonly capacity?: CapacityReservationService;
  readonly launchFactory?: RuntimeLaunchSpecFactory;
  readonly metricsPorts?: RuntimeMetricsPortAllocator;
}

function service(
  pool: FakePool,
  revalidateContinuityBeforeRuntime = vi.fn(async () => undefined),
  options: ServiceOptions = {},
): {
  readonly capacity: CapacityReservationService;
  readonly launchFactory: RuntimeLaunchSpecFactory;
  readonly processes: { readonly start: ReturnType<typeof vi.fn> };
  readonly runtime: RuntimeSupervisorService;
} {
  const processes = { start: vi.fn() };
  const database = { requirePool: () => pool } as unknown as PlatformDatabaseService;
  const audit = { append: vi.fn() } as unknown as AuditLedgerService;
  const capacity = options.capacity ?? ({} as unknown as CapacityReservationService);
  const processSupervisor = processes as unknown as RuntimeProcessSupervisor;
  const launchFactory = options.launchFactory ?? ({} as unknown as RuntimeLaunchSpecFactory);
  return {
    capacity,
    launchFactory,
    processes,
    runtime: new RuntimeSupervisorService(
      database,
      audit,
      capacity,
      processSupervisor,
      launchFactory,
      undefined,
      {
        revalidateContinuityBeforeRuntime,
        ...(options.prepareFirstStartTopic === undefined
          ? {}
          : { prepareFirstStartTopic: options.prepareFirstStartTopic }),
      } as unknown as SystemRecoveryPreflightPort,
      options.metricsPorts,
    ),
  };
}

function startRequest(overrides: Record<string, unknown> = {}) {
  return {
    expectedTaskRowVersion: '1',
    idempotencyKey: 'runtime-test-start',
    revisionId,
    taskId,
    ...overrides,
  };
}

function plannedStartRequest(overrides: Record<string, unknown> = {}) {
  return {
    expectedTaskRowVersion: '1',
    idempotencyKey: 'runtime-test-planned-start',
    revisionId,
    taskId,
    ...overrides,
  };
}

function preflightResult(
  partitionIds: readonly number[] = [0, 1],
  requested = partitionIds.length,
): TaskPreflightResult {
  return {
    capacity: { available: 4, limit: 4, requested, used: 0 },
    checks: [
      {
        code: 'KAFKA_TOPIC_IDENTITIES_OK',
        details: {
          topics: [
            {
              partitionIds,
              resourceType: 'KAFKA_SOURCE_TOPIC',
              topic: 'source.events',
            },
          ],
        },
        key: 'TOPIC_IDENTITIES',
        state: 'PASSED',
        summary: 'Kafka topic identities matched.',
      },
    ],
    dataSourceVersions: { source: '1', target: '1' },
    identities: {
      source: 'source-cluster',
      sourceTopic: 'source-topic-id',
      target: 'target-cluster',
      targetTopic: 'target-topic-id',
    },
    taskRevision: { draftVersion: '1', taskRowVersion: '1' },
  };
}

const preparedOperation: RuntimeOperationSummary = {
  action: 'START',
  completedAt: null,
  createdAt: '2026-09-03T00:00:00.000Z',
  failureCode: null,
  failureSummary: null,
  id: '88888888-8888-4888-8888-888888888888',
  runId: '99999999-9999-4999-8999-999999999999',
  state: 'EXECUTING',
  taskId,
  updatedAt: '2026-09-03T00:00:00.000Z',
};

function planningContext(options: {
  readonly configuration?: PlatformTaskConfig;
  readonly metricsPorts?: RuntimeMetricsPortAllocator;
  readonly preflight: TaskPreflightResult;
}) {
  const pool = fakePool({
    preflight: true,
    preflightResult: options.preflight,
    riskAcceptance: true,
    ...(options.configuration === undefined
      ? {}
      : { revision: { configuration: options.configuration } }),
  });
  const create = vi.fn(async (input: Parameters<RuntimeLaunchSpecFactory['create']>[0]) => ({
    args: [],
    cwd: `C:/runtime/${input.unitKey}`,
    env: {},
    executablePath: 'C:/runtime/ape-dts.exe',
    iniContent: '',
    iniPath: `C:/runtime/${input.unitKey}/ape-dts.ini`,
    log4rsContent: '',
    log4rsPath: `C:/runtime/${input.unitKey}/log4rs.yaml`,
    runtimeDirectory: `C:/runtime/${input.unitKey}`,
    unitId: input.unitId,
  }));
  const reserveWithin = vi.fn(async () => undefined);
  const context = service(
    pool,
    vi.fn(async () => undefined),
    {
      capacity: { reserveWithin } as unknown as CapacityReservationService,
      launchFactory: { create } as unknown as RuntimeLaunchSpecFactory,
      ...(options.metricsPorts === undefined ? {} : { metricsPorts: options.metricsPorts }),
    },
  );
  let prepared: unknown;
  const internals = context.runtime as unknown as {
    assertContinuityReady(): Promise<void>;
    assertStartPreconditions(): Promise<void>;
    launchPreparedStart(value: unknown): Promise<RuntimeOperationSummary>;
    requireOperationWithin(): Promise<RuntimeOperationSummary>;
    requirePlatformInstanceId(): Promise<string>;
  };
  internals.assertStartPreconditions = vi.fn(async () => undefined);
  internals.assertContinuityReady = vi.fn(async () => undefined);
  internals.requirePlatformInstanceId = vi.fn(async () => 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');
  internals.requireOperationWithin = vi.fn(async () => preparedOperation);
  internals.launchPreparedStart = vi.fn(async (value) => {
    prepared = value;
    return preparedOperation;
  });
  return { ...context, create, pool, prepared: () => prepared, reserveWithin };
}

async function expectRejectedWithoutPersistence(
  promise: Promise<unknown>,
  pool: FakePool,
  error: Partial<RuntimeSupervisorError>,
): Promise<void> {
  await expect(promise).rejects.toMatchObject(error);
  if (
    error.code === 'RUNTIME_TASK_NOT_FOUND' ||
    error.code === 'RUNTIME_ACCESS_DENIED' ||
    error.code === 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED'
  ) {
    expect(pool.committedWrites).toEqual([]);
  } else {
    expect(pool.committedWrites).toHaveLength(1);
    expect(pool.committedWrites[0]).toContain('insert into ape_dts_platform.task_operations');
    expect(pool.committedWrites[0]).toContain("'rejected'");
  }
  expect(pool.writes).toEqual([]);
}

describe('RuntimeSupervisorService startup barrier', () => {
  it('keeps MySQL CDC pending until its snapshot has completed, and resumes CDC without rerunning snapshot', () => {
    const config: PlatformTaskConfig = {
      kind: 'MYSQL_TO_KAFKA',
      schemaVersion: 2,
      source: { dataSourceId: sourceId, includeSnapshot: true },
      target: { dataSourceId: targetId },
      mappings: [
        { source: { schema: 'data_demo', name: 'events' }, targets: [{ topic: 'events' }] },
      ],
      tuning: snapshotConfiguration.tuning,
    };
    expect(runtimeStageTypes(config)).toContain('SNAPSHOT');
    expect(runtimeStageTypes(config, 'CDC')).not.toContain('SNAPSHOT');
    expect(runtimeStageTypes(config, 'CDC')).toContain('CDC_STARTED');
  });
  it('returns the existing start operation without trying to prepare a Topic again', async () => {
    const prepareFirstStartTopic = vi.fn(async () => true);
    const context = service(fakePool({ existingOperation: true }), undefined, {
      prepareFirstStartTopic,
    });
    const operation = await context.runtime.start(administrator, startRequest());
    expect(operation.action).toBe('START');
    expect(prepareFirstStartTopic).not.toHaveBeenCalled();
    expect(context.processes.start).not.toHaveBeenCalled();
  });
  it('requires refreshed preflight and risk confirmation after Topic preparation, without launching', async () => {
    const pool = fakePool();
    const revalidate = vi.fn(async () => undefined);
    const prepareFirstStartTopic = vi.fn(async () => true);
    const context = service(pool, revalidate, { prepareFirstStartTopic });
    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      {
        code: 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED',
      },
    );
    expect(prepareFirstStartTopic).toHaveBeenCalledOnce();
    expect(revalidate).not.toHaveBeenCalled();
    expect(context.processes.start).not.toHaveBeenCalled();
  });
  it('never launches when external Topic preparation or its audit fails', async () => {
    const pool = fakePool();
    const context = service(pool, undefined, {
      prepareFirstStartTopic: vi.fn(async () => {
        throw new Error('audit unavailable');
      }),
    });
    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      {
        code: 'RUNTIME_TOPIC_PREPARATION_FAILED',
        details: { requiresMetadataRefresh: true },
      },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });
  it('reports an externally created Topic when its audit fails, without launching', async () => {
    const pool = fakePool();
    const context = service(pool, undefined, {
      prepareFirstStartTopic: vi.fn(async () => {
        throw new KafkaTopicAuditError(
          { topic: 'target.events', created: true },
          { cause: new Error('private database failure') },
        );
      }),
    });
    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      {
        code: 'RUNTIME_TOPIC_PREPARATION_FAILED',
        details: {
          requiresMetadataRefresh: true,
          reason: 'KAFKA_TOPIC_AUDIT_FAILED',
          topic: 'target.events',
          created: true,
        },
      },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });
  it('blocks before runtime persistence when the independently committed continuity gate rejects', async () => {
    const pool = fakePool({ preflight: true, riskAcceptance: true });
    const revalidate = vi.fn(async () => {
      throw new PreflightServiceError('PREFLIGHT_TASK_VERSION_CONFLICT');
    });
    const context = service(pool, revalidate);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_CONTINUITY_BLOCKED' },
    );
    expect(revalidate).toHaveBeenCalledOnce();
    expect(context.processes.start).not.toHaveBeenCalled();
  });
  it.each(['USER start', 'SYSTEM recovery'])(
    '%s keeps CDC pending behind the snapshot stage',
    () => {
      expect(runtimeStageTypes(snapshotConfiguration)).toEqual([
        'PREFLIGHT',
        'ENSURE_ARTIFACTS',
        'PREPARE_RESOURCES',
        'GENERATE_INI',
        'STARTING_BARRIER',
        'SNAPSHOT',
        'CDC_STARTED',
        'MONITORING_READY',
      ]);
    },
  );

  it('completes monitoring by stage type while leaving snapshot CDC pending', async () => {
    const pool = fakePool({
      revision: { state: 'APPLIED' },
      task: { desired_state: 'RUNNING', row_version: '2' } as never,
    });
    const context = service(pool);
    const internals = context.runtime as unknown as {
      capacity: { activateWithin: ReturnType<typeof vi.fn> };
      finishStartSuccess(prepared: unknown, started: readonly unknown[]): Promise<void>;
    };
    internals.capacity = { activateWithin: vi.fn(async () => 1) };
    await internals.finishStartSuccess(
      {
        actorUserId: null,
        operationId: 'operation',
        revisionId,
        runId: 'run',
        runtimeStage: 'SNAPSHOT',
        systemRecovery: true,
        taskId,
        taskRowVersionAfterPrepare: '2',
      },
      [{ process: { pid: 123 }, unit: { id: 'unit' } }],
    );
    const monitoringUpdate = pool.committedWrites.find((sql) =>
      sql.includes("stage_type = 'monitoring_ready'"),
    );
    expect(monitoringUpdate).toBeDefined();
    expect(monitoringUpdate).not.toContain('stage_order = 7');
    const scenarioUpdate = pool.committedWrites.find((sql) => sql.includes('stage_order = 6'));
    expect(scenarioUpdate).toContain("stage_type = 'snapshot'");
    expect(scenarioUpdate).toContain("then 'running'");
  });

  it('blocks shutdown until every previously admitted START releases its lifecycle permit', async () => {
    const context = service(fakePool());
    const internals = context.runtime as unknown as {
      acquireStartPermit(): () => void;
    };
    const release = internals.acquireStartPermit();
    let shutdownCompleted = false;

    const shutdown = context.runtime.onApplicationShutdown().then(() => {
      shutdownCompleted = true;
    });
    await Promise.resolve();

    expect(shutdownCompleted).toBe(false);
    expect(() => internals.acquireStartPermit()).toThrowError(
      expect.objectContaining({ code: 'RUNTIME_OPERATION_IN_PROGRESS' }),
    );

    release();
    await shutdown;
    expect(shutdownCompleted).toBe(true);
  });

  it('registers unconfirmed partial-start handles before attempting terminal persistence', async () => {
    const context = service(fakePool());
    const stop = vi.fn(async () => ({ confirmed: false, terminationTimedOut: true }));
    const internals = context.runtime as unknown as {
      activeProcesses: Map<string, Map<string, unknown>>;
      finishStartFailure: ReturnType<typeof vi.fn>;
      finishStartedProcessFailure(
        prepared: unknown,
        started: readonly unknown[],
        error: unknown,
      ): Promise<boolean>;
      settleRunLogState: ReturnType<typeof vi.fn>;
    };
    internals.settleRunLogState = vi.fn(async () => ({
      persistInTerminalTransaction: false,
      state: null,
    }));
    internals.finishStartFailure = vi.fn(async () => {
      expect(internals.activeProcesses.get('run')?.has('unit')).toBe(true);
      throw new Error('database unavailable');
    });

    await expect(
      internals.finishStartedProcessFailure(
        { runId: 'run', taskId: 'task', units: [{ id: 'unit', processToken: 'token' }] },
        [{ process: { pid: 123, stop }, unit: { id: 'unit' } }],
        new Error('spawn failed'),
      ),
    ).rejects.toThrow('database unavailable');
    expect(internals.activeProcesses.get('run')?.has('unit')).toBe(true);
  });

  it('stops every owned child before attempting persistence when the database is unavailable', async () => {
    const pool = fakePool();
    const context = service(pool);
    const stop = vi.fn(async () => ({
      confirmed: true,
      terminationTimedOut: false,
      logGapBytes: 0,
      logStreamError: false,
      logStorageState: 'HEALTHY',
      logRecoveryState: 'NONE',
    }));
    const internals = context.runtime as unknown as {
      activeProcesses: Map<string, Map<string, unknown>>;
      activeRuntimeFacts: Map<string, unknown>;
      settleRunLogState: ReturnType<typeof vi.fn>;
    };
    for (const runId of ['run-a', 'run-b']) {
      internals.activeProcesses.set(runId, new Map([['unit', { stop }]]));
      internals.activeRuntimeFacts.set(runId, { taskId: runId });
    }
    pool.query.mockImplementation(async () => {
      throw new Error('database unavailable');
    });
    pool.connect.mockImplementation(async () => {
      expect(stop).toHaveBeenCalledTimes(2);
      throw new Error('database unavailable');
    });
    internals.settleRunLogState = vi.fn(async () => {
      throw new Error('log store unavailable');
    });
    await expect(context.runtime.onApplicationShutdown('SIGTERM')).resolves.toBeUndefined();
    expect(stop).toHaveBeenCalledTimes(2);
    expect(pool.query).not.toHaveBeenCalled();
    expect(internals.activeProcesses.size).toBe(2);
    await context.runtime.onApplicationShutdown('SIGTERM');
    expect(stop).toHaveBeenCalledTimes(2);
  });

  it('does not claim a clean terminal state or forget handles when final checkpoint or log settlement fails', async () => {
    const pool = fakePool();
    const context = service(pool);
    const stop = vi.fn(async () => ({ confirmed: true, terminationTimedOut: false }));
    const internals = context.runtime as unknown as {
      activeProcesses: Map<string, Map<string, unknown>>;
      activeRuntimeFacts: Map<string, unknown>;
      settleRunLogState: ReturnType<typeof vi.fn>;
    };
    internals.activeProcesses.set('run', new Map([['unit', { stop }]]));
    internals.activeRuntimeFacts.set('run', { taskId });
    internals.settleRunLogState = vi.fn(async () => {
      throw new Error('checkpoint unavailable');
    });
    await expect(context.runtime.onApplicationShutdown()).resolves.toBeUndefined();
    expect(stop).toHaveBeenCalledOnce();
    expect(internals.activeProcesses.has('run')).toBe(true);
    expect(pool.committedWrites.filter((sql) => sql.startsWith('update'))).toEqual([]);
  });

  it('rejects a missing task without creating operation, run, or reservation rows', async () => {
    const pool = fakePool({ missingTask: true });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_TASK_NOT_FOUND' },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });

  it('rejects a stale task row version before any runtime fact is written', async () => {
    const pool = fakePool({ task: { row_version: '2' } });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_TASK_VERSION_CONFLICT' },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });

  it('requires a preflight-passed revision and does not treat READY_DRAFT as runnable', async () => {
    const pool = fakePool({ revision: { state: 'READY_DRAFT' } });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_PREFLIGHT_REQUIRED' },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });

  it('checks preflight without requiring a risk acceptance for a passed revision', async () => {
    const pool = fakePool({ revision: { state: 'PREFLIGHT_PASSED' } });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_PREFLIGHT_REQUIRED' },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });

  it('requires a passed preflight result before attempting launch', async () => {
    const pool = fakePool({ riskAcceptance: true });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_PREFLIGHT_REQUIRED' },
    );
    expect(context.processes.start).not.toHaveBeenCalled();
  });

  it('uses a stable latest-preflight order before validating its result', async () => {
    const pool = fakePool({ preflight: true, riskAcceptance: true });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, startRequest()),
      pool,
      { code: 'RUNTIME_PREFLIGHT_STALE' },
    );
    const preflightQuery = pool.query.mock.calls.find(([sql]) =>
      String(sql).includes('ape_dts_platform.preflight_runs'),
    );
    expect(String(preflightQuery?.[0]).replace(/\s+/gu, ' ')).toContain(
      'ORDER BY started_at DESC, id DESC',
    );
  });

  it('plans the single PG runtime unit and its metrics port on the server', async () => {
    const allocate = vi.fn(async () => [36_001] as const);
    const context = planningContext({
      configuration: snapshotConfiguration,
      metricsPorts: { allocate } as unknown as RuntimeMetricsPortAllocator,
      preflight: preflightResult([], 1),
    });

    await expect(context.runtime.start(administrator, plannedStartRequest())).resolves.toEqual(
      preparedOperation,
    );

    expect(allocate).toHaveBeenCalledWith(expect.anything(), 1);
    expect(context.create).toHaveBeenCalledWith(
      expect.objectContaining({ metricsPort: 36_001, unitKey: 'primary' }),
    );
    const unitInsert = context.pool.query.mock.calls.find(([sql]) =>
      String(sql).includes('INSERT INTO ape_dts_platform.task_run_units'),
    );
    expect(unitInsert?.[1]).toEqual([
      expect.any(String),
      taskId,
      expect.any(String),
      'primary',
      'C:/runtime/primary',
      'C:/runtime/primary/ape-dts.ini',
      'C:/runtime/primary/log4rs.yaml',
      36_001,
      expect.any(String),
    ]);
    expect(context.reserveWithin).toHaveBeenCalledWith(
      expect.anything(),
      taskId,
      revisionId,
      ['primary'],
      expect.any(String),
    );
  });

  it('sorts sparse Kafka partition identities and uses the same generated units everywhere', async () => {
    const allocate = vi.fn(async () => [36_010, 36_011, 36_012] as const);
    const context = planningContext({
      metricsPorts: { allocate } as unknown as RuntimeMetricsPortAllocator,
      preflight: preflightResult([7, 2, 12], 3),
    });

    await expect(context.runtime.start(administrator, plannedStartRequest())).resolves.toEqual(
      preparedOperation,
    );

    const expectedUnits = [
      { metricsPort: 36_010, unitKey: 'partition:2' },
      { metricsPort: 36_011, unitKey: 'partition:7' },
      { metricsPort: 36_012, unitKey: 'partition:12' },
    ];
    expect(allocate).toHaveBeenCalledWith(expect.anything(), 3);
    expect(context.create.mock.calls.map(([input]) => input)).toEqual(
      expectedUnits.map((unit) => expect.objectContaining(unit)),
    );
    const persistedUnits = context.pool.query.mock.calls
      .filter(([sql]) => String(sql).includes('INSERT INTO ape_dts_platform.task_run_units'))
      .map(([, parameters]) => ({
        metricsPort: (parameters as unknown[])[7],
        unitKey: (parameters as unknown[])[3],
      }));
    expect(persistedUnits).toEqual(expectedUnits);
    expect(context.reserveWithin).toHaveBeenCalledWith(
      expect.anything(),
      taskId,
      revisionId,
      expectedUnits.map((unit) => unit.unitKey),
      expect.any(String),
    );
    expect(context.prepared()).toMatchObject({
      units: expectedUnits.map((unit) => expect.objectContaining({ unitKey: unit.unitKey })),
    });
  });

  it.each([
    ['missing source topic baseline', { ...preflightResult([2]), checks: [] }],
    ['empty partition baseline', preflightResult([])],
    ['duplicate partition baseline', preflightResult([2, 2])],
    ['negative partition baseline', preflightResult([-1])],
    ['non-integer partition baseline', preflightResult([1.5])],
  ])('rejects %s before allocating ports or persisting runtime facts', async (_name, preflight) => {
    const allocate = vi.fn(async () => [36_020]);
    const context = planningContext({
      metricsPorts: { allocate } as unknown as RuntimeMetricsPortAllocator,
      preflight,
    });

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, plannedStartRequest()),
      context.pool,
      { code: 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH' },
    );
    expect(allocate).not.toHaveBeenCalled();
    expect(context.create).not.toHaveBeenCalled();
    expect(context.reserveWithin).not.toHaveBeenCalled();
  });

  it('fails without persistence when the metrics port range is not configured', async () => {
    const context = planningContext({ preflight: preflightResult([0, 1]) });

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, plannedStartRequest()),
      context.pool,
      { code: 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED' },
    );
    expect(context.create).not.toHaveBeenCalled();
    expect(context.reserveWithin).not.toHaveBeenCalled();
  });

  it('fails without persistence when the metrics port range cannot satisfy every unit', async () => {
    const allocate = vi.fn(async () => {
      throw new RuntimeMetricsPortAllocatorError('RUNTIME_METRICS_PORTS_UNAVAILABLE');
    });
    const context = planningContext({
      metricsPorts: { allocate } as unknown as RuntimeMetricsPortAllocator,
      preflight: preflightResult([0, 1]),
    });

    await expectRejectedWithoutPersistence(
      context.runtime.start(administrator, plannedStartRequest()),
      context.pool,
      { code: 'RUNTIME_METRICS_PORTS_UNAVAILABLE' },
    );
    expect(allocate).toHaveBeenCalledWith(expect.anything(), 2);
    expect(context.create).not.toHaveBeenCalled();
    expect(context.reserveWithin).not.toHaveBeenCalled();
  });

  it('requires only one grant when source and target use the same data source', async () => {
    const pool = fakePool({
      grantCount: '1',
      task: { row_version: '2', target_data_source_id: sourceId },
    });
    const context = service(pool);

    await expectRejectedWithoutPersistence(context.runtime.start(developer, startRequest()), pool, {
      code: 'RUNTIME_TASK_VERSION_CONFLICT',
    });
    const grantQuery = pool.query.mock.calls.find(([sql]) =>
      String(sql).includes('ape_dts_platform.data_source_grants'),
    );
    expect(grantQuery?.[1]).toEqual([developer.id, [sourceId]]);
  });

  it('rechecks current grants before returning an idempotent operation', async () => {
    const pool = fakePool({ existingOperation: true, grantCount: '0' });
    const context = service(pool);

    await expectRejectedWithoutPersistence(context.runtime.start(developer, startRequest()), pool, {
      code: 'RUNTIME_ACCESS_DENIED',
    });
    expect(
      pool.query.mock.calls.some(([sql]) =>
        String(sql).includes('ape_dts_platform.task_operations'),
      ),
    ).toBe(false);
  });

  it('rechecks current grants before returning an idempotent stop operation', async () => {
    const pool = fakePool({
      existingOperation: true,
      existingOperationAction: 'STOP',
      grantCount: '0',
    });
    const context = service(pool);

    await expectRejectedWithoutPersistence(
      context.runtime.stop(developer, {
        expectedTaskRowVersion: '1',
        idempotencyKey: 'runtime-test-stop',
        taskId,
      }),
      pool,
      { code: 'RUNTIME_ACCESS_DENIED' },
    );
    expect(
      pool.query.mock.calls.some(([sql]) =>
        String(sql).includes('ape_dts_platform.task_operations'),
      ),
    ).toBe(false);
  });

  it('returns the idempotent operation after current grants are verified', async () => {
    const pool = fakePool({ existingOperation: true, grantCount: '2' });
    const context = service(pool);

    await expect(context.runtime.start(developer, startRequest())).resolves.toMatchObject({
      action: 'START',
      id: '88888888-8888-4888-8888-888888888888',
      state: 'SUCCEEDED',
    });
    expect(pool.commit).toHaveBeenCalledOnce();
    expect(context.processes.start).not.toHaveBeenCalled();
  });
});

describe('RuntimeSupervisorService emergency memory port', () => {
  function seedActive(
    runtime: RuntimeSupervisorService,
    input: {
      taskId: string;
      runId: string;
      unitId: string;
      processToken: string;
      process: {
        pid: number;
        stop: ReturnType<typeof vi.fn>;
        forceKill: ReturnType<typeof vi.fn>;
      };
    },
  ): void {
    const internals = runtime as unknown as {
      activeProcesses: Map<string, Map<string, unknown>>;
      activeRuntimeFacts: Map<
        string,
        {
          operationId: string;
          platformInstanceId: string;
          revisionId: string;
          taskId: string;
          runId: string;
          units: Map<string, { pid: number; processToken: string }>;
        }
      >;
    };
    const handles = internals.activeProcesses.get(input.runId) ?? new Map<string, unknown>();
    handles.set(input.unitId, input.process);
    internals.activeProcesses.set(input.runId, handles);
    const units = internals.activeRuntimeFacts.get(input.runId)?.units ?? new Map();
    units.set(input.unitId, { pid: input.process.pid, processToken: input.processToken });
    internals.activeRuntimeFacts.set(input.runId, {
      operationId: '00000000-0000-4000-8000-000000000010',
      platformInstanceId: '00000000-0000-4000-8000-000000000011',
      revisionId: '00000000-0000-4000-8000-000000000012',
      runId: input.runId,
      taskId: input.taskId,
      units,
    });
  }

  const confirmedExit = (signal: NodeJS.Signals, forcedKill: boolean) => ({
    confirmed: true,
    code: 0,
    forcedKill,
    logGapBytes: 0,
    logRecoveryState: 'NONE' as const,
    logStorageState: 'HEALTHY' as const,
    logStreamError: false,
    signal,
    signals: [signal],
    terminationTimedOut: false,
  });

  it('lists only in-memory active facts and terminates only the requested task', async () => {
    const pool = fakePool();
    const context = service(pool);
    const target = {
      pid: 101,
      stop: vi.fn(async () => confirmedExit('SIGINT', false)),
      forceKill: vi.fn(),
    };
    const other = {
      pid: 202,
      stop: vi.fn(async () => confirmedExit('SIGINT', false)),
      forceKill: vi.fn(),
    };
    seedActive(context.runtime, {
      taskId: 'task-target',
      runId: 'run-target',
      unitId: 'unit-target',
      processToken: 'token-target',
      process: target,
    });
    seedActive(context.runtime, {
      taskId: 'task-other',
      runId: 'run-other',
      unitId: 'unit-other',
      processToken: 'token-other',
      process: other,
    });

    expect(context.runtime.listActiveRuntimeTasks()).toEqual([
      expect.objectContaining({
        runId: 'run-other',
        taskId: 'task-other',
        units: [{ pid: 202, processToken: 'token-other', unitId: 'unit-other' }],
      }),
      expect.objectContaining({
        runId: 'run-target',
        taskId: 'task-target',
        units: [{ pid: 101, processToken: 'token-target', unitId: 'unit-target' }],
      }),
    ]);
    const result = await context.runtime.terminateActiveTask('task-target', 'GRACEFUL');

    expect(result.allConfirmed).toBe(true);
    expect(result.units[0]).toMatchObject({
      confirmed: true,
      forced: false,
      signal: 'SIGINT',
      timeout: false,
    });
    expect(target.stop).toHaveBeenCalledOnce();
    expect(other.stop).not.toHaveBeenCalled();
    expect(pool.query).not.toHaveBeenCalled();
    expect(context.runtime.listActiveRuntimeTasks()).toEqual([
      expect.objectContaining({
        runId: 'run-other',
        taskId: 'task-other',
        units: [{ pid: 202, processToken: 'token-other', unitId: 'unit-other' }],
      }),
    ]);
  });

  it('uses direct force kill, retains all handles when any unit is unconfirmed, and merges concurrent calls', async () => {
    const context = service(fakePool());
    let release!: (value: ReturnType<typeof confirmedExit>) => void;
    const gate = new Promise<ReturnType<typeof confirmedExit>>((resolve) => {
      release = resolve;
    });
    const confirmed = { pid: 301, stop: vi.fn(), forceKill: vi.fn(async () => await gate) };
    const timedOut = {
      pid: 302,
      stop: vi.fn(),
      forceKill: vi.fn(async () => ({
        ...confirmedExit('SIGKILL', true),
        confirmed: false,
        terminationTimedOut: true,
      })),
    };
    seedActive(context.runtime, {
      taskId: 'task-force',
      runId: 'run-force',
      unitId: 'unit-a',
      processToken: 'token-a',
      process: confirmed,
    });
    seedActive(context.runtime, {
      taskId: 'task-force',
      runId: 'run-force',
      unitId: 'unit-b',
      processToken: 'token-b',
      process: timedOut,
    });

    const first = context.runtime.terminateActiveTask('task-force', 'FORCE');
    const second = context.runtime.terminateActiveTask('task-force', 'FORCE');
    release(confirmedExit('SIGKILL', true));
    const [firstResult, secondResult] = await Promise.all([first, second]);

    expect(firstResult).toEqual(secondResult);
    expect(confirmed.forceKill).toHaveBeenCalledOnce();
    expect(timedOut.forceKill).toHaveBeenCalledOnce();
    expect(confirmed.stop).not.toHaveBeenCalled();
    expect(firstResult.allConfirmed).toBe(false);
    expect(context.runtime.listActiveRuntimeTasks()).toHaveLength(1);
  });
});
