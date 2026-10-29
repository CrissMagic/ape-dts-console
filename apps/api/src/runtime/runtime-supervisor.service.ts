import { createHash, randomUUID } from 'node:crypto';

import {
  Inject,
  Injectable,
  Logger,
  Optional,
  type BeforeApplicationShutdown,
  type OnApplicationShutdown,
} from '@nestjs/common';
import {
  platformTaskConfigSchema,
  kafkaTaskMappings,
  taskTargetTopics,
  type PlatformTaskConfig,
  type MysqlToKafkaTaskConfig,
} from '@scs/config-model';
import {
  taskPreflightResultSchema,
  type RuntimeOperationSummary as RuntimeOperationContract,
  type RuntimeUnitLaunchRequest as RuntimeUnitLaunchContract,
  type StartTaskRuntimeRequest as StartTaskRuntimeContract,
  type StopTaskRuntimeRequest as StopTaskRuntimeContract,
  type TaskPreflightCheck,
  type TaskPreflightResult,
  type UserSummary,
} from '@scs/contracts';
import type { LogRecoveryState, LogStorageState } from '@scs/storage';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { RuntimeRecoverySettingsService } from '../settings/runtime-recovery.service.js';
import {
  confirmedPgTableExpansion,
  applyPgTableExpansionContinuity,
} from '../tasks/pg-table-expansion.js';
import {
  KafkaTopicAuditError,
  DataSourcesServiceError,
  DataSourcesService,
} from '../data-sources/data-sources.service.js';
import { DataSourceAdapterError } from '../data-sources/adapters/types.js';
import { recoveryFailureReason } from './recovery-failure.js';
import { AlertsService } from '../alerts/alerts.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import {
  CapacityReservationError,
  CapacityReservationService,
} from '../pg-resources/capacity-reservation.service.js';
import {
  SYSTEM_RECOVERY_PREFLIGHT,
  PreflightServiceError,
  type SystemRecoveryPreflightPort,
} from '../preflight/preflight.service.js';
import {
  AutoRecoveryManualRequiredError,
  FixedAutoRecoveryCoordinator,
  type AutoRecoveryContext,
  type AutoRecoveryPort,
} from './fixed-auto-recovery.js';
import {
  AuthenticatedRuntimeLaunchSpecError,
  type AuthenticatedRuntimeLaunchSpecErrorCode,
} from './authenticated-runtime-launch-spec.js';
import {
  RuntimeConfigurationCleanupError,
  RuntimeProcessSupervisor,
} from './process-supervisor.js';
import type {
  ManagedRuntimeProcess,
  RuntimeProcessExit,
  RuntimeSpawnSpec,
} from './process-supervisor.js';
import { RuntimeLaunchSpecError, RuntimeLaunchSpecFactory } from './runtime-launch-spec.js';
import {
  RuntimeMetricsPortAllocator,
  RuntimeMetricsPortAllocatorError,
} from './runtime-metrics-port-allocator.js';
import { kafkaSourceBinding, kafkaUnitKey } from './kafka-source-units.js';
import {
  MysqlRuntimeStateStore,
  MysqlRuntimeStateError,
  readMysqlSnapshotCompleted,
  type MysqlRuntimeBinding,
  type MysqlRuntimeStateErrorCode,
  type MysqlRuntimePhase,
} from './mysql-runtime-state.js';

export type RuntimeSupervisorErrorCode =
  | AuthenticatedRuntimeLaunchSpecErrorCode
  | MysqlRuntimeStateErrorCode
  | 'RUNTIME_ACCESS_DENIED'
  | 'RUNTIME_ALREADY_RUNNING'
  | 'RUNTIME_ARTIFACT_BINDING_INVALID'
  | 'RUNTIME_ARTIFACT_NOT_ACTIVE'
  | 'RUNTIME_CAPACITY_ERROR'
  | 'RUNTIME_CONFIGURATION_CLEANUP_FAILED'
  | 'RUNTIME_CONTINUITY_BLOCKED'
  | 'RUNTIME_DATA_SOURCE_VERSION_CONFLICT'
  | 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH'
  | 'RUNTIME_IDEMPOTENCY_KEY_CONFLICT'
  | 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED'
  | 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED'
  | 'RUNTIME_METRICS_PORT_CONFLICT'
  | 'RUNTIME_METRICS_PORTS_UNAVAILABLE'
  | 'RUNTIME_OPERATION_IN_PROGRESS'
  | 'RUNTIME_PREFLIGHT_REQUIRED'
  | 'RUNTIME_RECOVERY_PREFLIGHT_FAILED'
  | 'RUNTIME_PREFLIGHT_STALE'
  | 'RUNTIME_PROCESS_HANDLE_MISSING'
  | 'RUNTIME_REVISION_INVALID'
  | 'RUNTIME_REVISION_NOT_CURRENT'
  | 'RUNTIME_RISK_CONFIRMATION_REQUIRED'
  | 'RUNTIME_STOP_NOT_ALLOWED'
  | 'RUNTIME_TASK_NOT_FOUND'
  | 'RUNTIME_TASK_VERSION_CONFLICT'
  | 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED'
  | 'RUNTIME_TOPIC_PREPARATION_FAILED'
  | 'RUNTIME_UNIT_CONFIGURATION_INVALID';

export class RuntimeSupervisorError extends Error {
  constructor(
    readonly code: RuntimeSupervisorErrorCode,
    readonly details?: Readonly<Record<string, unknown>>,
  ) {
    super(code);
    this.name = 'RuntimeSupervisorError';
  }
}

type RuntimeUnitLaunchRequest = Readonly<RuntimeUnitLaunchContract>;

export type StartTaskRuntimeRequest = StartTaskRuntimeContract & {
  readonly taskId: string;
};

type PlannedStartTaskRuntimeRequest = StartTaskRuntimeRequest & {
  readonly units: readonly RuntimeUnitLaunchRequest[];
};

export type StopTaskRuntimeRequest = StopTaskRuntimeContract & {
  readonly taskId: string;
};

export type RuntimeOperationSummary = RuntimeOperationContract;

export type MemoryRuntimeTerminationMode = 'FORCE' | 'GRACEFUL';

export interface ActiveRuntimeUnitSnapshot {
  readonly pid: number | undefined;
  readonly processToken: string;
  readonly unitId: string;
}

export interface ActiveRuntimeTaskSnapshot {
  readonly operationId: string;
  readonly platformInstanceId: string;
  readonly revisionId: string;
  readonly runId: string;
  readonly taskId: string;
  readonly units: readonly ActiveRuntimeUnitSnapshot[];
}

export interface MemoryRuntimeTerminationUnitResult extends ActiveRuntimeUnitSnapshot {
  readonly confirmed: boolean;
  readonly forced: boolean;
  readonly signal: NodeJS.Signals | null;
  readonly timeout: boolean;
}

export interface MemoryRuntimeTerminationResult {
  readonly allConfirmed: boolean;
  readonly mode: MemoryRuntimeTerminationMode;
  readonly runId: string;
  readonly taskId: string;
  readonly units: readonly MemoryRuntimeTerminationUnitResult[];
}

interface TaskRow extends QueryResultRow {
  current_applied_revision_id: string | null;
  current_draft_revision_id: string | null;
  deleted_at: Date | null;
  desired_state: 'STOPPED' | 'RUNNING' | 'DELETED';
  id: string;
  observed_state:
    'STOPPED' | 'STARTING' | 'RUNNING' | 'STOPPING' | 'FAILED' | 'BLOCKED' | 'DELETED';
  row_version: string;
  source_data_source_id: string;
  target_data_source_id: string;
}

interface RevisionRow extends QueryResultRow {
  artifact_id: string | null;
  configuration: unknown;
  draft_version: string;
  id: string;
  state: string;
  task_id: string;
}

interface PreflightRow extends QueryResultRow {
  completed_at: Date | null;
  id: string;
  result: unknown;
  state: 'RUNNING' | 'PASSED' | 'FAILED' | 'STALE';
}

interface DataSourceRow extends QueryResultRow {
  configuration: unknown;
  deleted_at: Date | null;
  id: string;
  row_version: string;
  state: 'ACTIVE' | 'DELETED';
}

interface IdentityObservationRow extends QueryResultRow {
  data_source_id: string;
  identity_kind: string;
  identity_value: string | null;
}

interface ArtifactBindingRow extends QueryResultRow {
  declared_sha256: string | null;
  actual_sha256: string | null;
  artifact_state: 'ACTIVE' | 'INACTIVE' | 'QUARANTINED';
  stored_object_sha256: string | null;
  stored_object_state: string | null;
}

interface ExternalResourceRow extends QueryResultRow {
  data_source_id: string;
  database_name: string | null;
  lifecycle_state: string;
  normalized_name: string;
  ownership_mode: string;
  resource_type: string;
  stable_identity: string;
}

const recoveryErrorCode = recoveryFailureReason;

interface OperationRow extends QueryResultRow {
  action: RuntimeOperationSummary['action'];
  completed_at: Date | null;
  created_at: Date;
  failure_code: string | null;
  failure_summary: string | null;
  id: string;
  run_id: string | null;
  state: RuntimeOperationSummary['state'];
  task_id: string;
  updated_at: Date;
}

interface PreparedUnit {
  readonly id: string;
  readonly launch: Omit<RuntimeSpawnSpec, 'runId' | 'unitId'>;
  readonly processToken: string;
  readonly unitKey: string;
  readonly metricsPort: number;
}

interface PreparedStart {
  readonly actorUserId: string | null;
  readonly kind: 'PREPARED';
  readonly operation: RuntimeOperationSummary;
  readonly operationId: string;
  readonly platformInstanceId: string;
  readonly revisionId: string;
  readonly runtimeStage: 'CDC_STARTED' | 'SNAPSHOT';
  readonly runId: string;
  readonly taskId: string;
  readonly taskRowVersionAfterPrepare: string;
  readonly systemRecovery: boolean;
  readonly units: readonly PreparedUnit[];
  readonly mysql?: {
    readonly config: MysqlToKafkaTaskConfig;
    readonly position: MysqlRuntimeBinding;
  };
}

interface SystemRecoveryStartBinding {
  readonly freshCapacity?: boolean;
  readonly actorUserId?: string;
  readonly expectedTaskRowVersion: string;
  readonly kind: 'AUTO' | 'CONTAINER';
  readonly oldProcessToken?: string;
  readonly oldRunId: string;
  readonly platformInstanceId: string;
  readonly revisionId: string;
  readonly taskId: string;
  readonly unitKeys?: readonly string[];
}

interface PreparedStop {
  readonly kind: 'PREPARED';
  readonly operationId: string;
  readonly runId: string;
  readonly taskId: string;
  readonly unitIds: readonly string[];
}

interface ExistingOperation {
  readonly kind: 'EXISTING';
  readonly operation: RuntimeOperationSummary;
}

interface StartFailureDisposition {
  readonly auditDetails?: Readonly<Record<string, unknown>>;
  readonly blockManual: boolean;
  readonly failureCode: string;
  readonly failureSummary: string;
  readonly releaseCapacity: boolean;
}

interface UnexpectedExitBlockReason {
  readonly failureCode: string;
  readonly failureSummary: string;
  readonly processesConfirmed: boolean;
  readonly releaseCapacity: boolean;
}

const TASK_COLUMNS = `
  id,
  source_data_source_id,
  target_data_source_id,
  desired_state,
  observed_state,
  current_draft_revision_id,
  current_applied_revision_id,
  row_version::text,
  deleted_at`;

const OPERATION_COLUMNS = `
  id,
  task_id,
  action,
  state,
  run_id,
  failure_code,
  failure_summary,
  created_at,
  updated_at,
  completed_at`;

function operationSummary(row: OperationRow): RuntimeOperationSummary {
  return {
    action: row.action,
    completedAt: row.completed_at?.toISOString() ?? null,
    createdAt: row.created_at.toISOString(),
    failureCode: row.failure_code,
    failureSummary: row.failure_summary,
    id: row.id,
    runId: row.run_id,
    state: row.state,
    taskId: row.task_id,
    updatedAt: row.updated_at.toISOString(),
  };
}

interface RuntimeLogState {
  readonly recoveryState: LogRecoveryState;
  readonly storageState: LogStorageState;
}

interface RuntimeLogStateResolution {
  readonly persistInTerminalTransaction: boolean;
  readonly state: RuntimeLogState | null;
}

function aggregateRuntimeLogState(results: readonly RuntimeProcessExit[]): RuntimeLogState {
  const hasMemoryPending = results.some((result) =>
    ['MEMORY_PENDING', 'MIXED_PENDING'].includes(result.logRecoveryState),
  );
  const hasDurablePending = results.some((result) =>
    ['DURABLE_PENDING', 'MIXED_PENDING'].includes(result.logRecoveryState),
  );
  const recoveryState: LogRecoveryState =
    hasMemoryPending && hasDurablePending
      ? 'MIXED_PENDING'
      : hasMemoryPending
        ? 'MEMORY_PENDING'
        : hasDurablePending
          ? 'DURABLE_PENDING'
          : 'NONE';
  const storageState: LogStorageState = results.some(
    (result) => result.logStorageState === 'DEGRADED',
  )
    ? 'DEGRADED'
    : results.some((result) => result.logStorageState === 'UNKNOWN')
      ? 'UNKNOWN'
      : 'HEALTHY';
  return { recoveryState, storageState };
}

function hasConfigurationCleanupFailure(results: readonly RuntimeProcessExit[]): boolean {
  return results.some((result) => result.configurationCleanupFailed === true);
}

const CONFIGURATION_CLEANUP_FAILURE_SUMMARY =
  '运行进程已确认退出，但明文运行配置清理失败，需人工清除残留。';

function assertIdempotencyKey(value: string): void {
  if (value.trim().length === 0 || value.length > 255 || value.includes('\u0000')) {
    throw new RuntimeSupervisorError('RUNTIME_IDEMPOTENCY_KEY_CONFLICT');
  }
}

function assertUnitRequests(units: readonly RuntimeUnitLaunchRequest[]): void {
  if (
    units.length === 0 ||
    new Set(units.map((unit) => unit.unitKey)).size !== units.length ||
    new Set(units.map((unit) => unit.metricsPort)).size !== units.length
  ) {
    throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
  }
  if (
    units.some(
      (unit) =>
        unit.unitKey.trim().length === 0 ||
        !Number.isSafeInteger(unit.metricsPort) ||
        unit.metricsPort < 1 ||
        unit.metricsPort > 65_535,
    )
  ) {
    throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
  }
}

function incrementRowVersion(value: string): string {
  try {
    return (BigInt(value) + 1n).toString();
  } catch {
    throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
  }
}

function parseRevisionConfiguration(value: unknown): PlatformTaskConfig {
  const parsed = platformTaskConfigSchema.safeParse(value);
  if (!parsed.success) {
    throw new RuntimeSupervisorError('RUNTIME_REVISION_INVALID');
  }
  return parsed.data;
}

function isPostgresConstraintViolation(error: unknown, constraint: string): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    error.code === '23505' &&
    'constraint' in error &&
    error.constraint === constraint
  );
}

const PREFLIGHT_REQUIRED_CHECKS = [
  'CONFIGURATION',
  'DATA_SOURCE_ACCESS',
  'SOURCE_CONNECTION',
  'TARGET_CONNECTION',
  'SOURCE_METADATA',
  'TARGET_METADATA',
  'TOPIC_IDENTITIES',
  'ARTIFACT',
  'CAPACITY',
] as const;

const PREFLIGHT_RESOURCE_STATES = [
  'BOUND',
  'TRACKED_AFTER_STOP',
  'TRACKED_AFTER_DELETE',
  'CLEANUP_REQUESTED',
  'CLEANING',
  'CLEANUP_FAILED',
] as const;

function scenarioForConfiguration(config: PlatformTaskConfig): string {
  if (config.kind === 'KAFKA_RAW_FORWARD') {
    return 'KAFKA_RAW_FORWARD';
  }
  return config.source.includeSnapshot ? 'SNAPSHOT' : 'CDC_STARTED';
}

export function runtimeStageTypes(
  config: PlatformTaskConfig,
  mysqlPhase?: MysqlRuntimePhase,
): readonly string[] {
  const common = [
    'PREFLIGHT',
    'ENSURE_ARTIFACTS',
    'PREPARE_RESOURCES',
    'GENERATE_INI',
    'STARTING_BARRIER',
  ] as const;
  return (mysqlPhase ?? scenarioForConfiguration(config)) === 'SNAPSHOT'
    ? [...common, 'SNAPSHOT', 'CDC_STARTED', 'MONITORING_READY']
    : [...common, 'CDC_STARTED', 'MONITORING_READY'];
}

function objectString(value: unknown, key: string): string | null {
  if (typeof value !== 'object' || value === null) {
    return null;
  }
  const candidate = (value as Record<string, unknown>)[key];
  return typeof candidate === 'string' && candidate.trim().length > 0 ? candidate : null;
}

function objectStringOrNull(value: unknown, key: string): string | null {
  if (typeof value !== 'object' || value === null) {
    return null;
  }
  const candidate = (value as Record<string, unknown>)[key];
  return candidate === null || candidate === undefined
    ? null
    : typeof candidate === 'string'
      ? candidate
      : null;
}

function preflightCheck(
  result: TaskPreflightResult,
  key: TaskPreflightCheck['key'],
): TaskPreflightCheck[] {
  return result.checks.filter((item) => item.key === key);
}

function assertPassedPreflightChecks(
  result: TaskPreflightResult,
  config: PlatformTaskConfig,
): void {
  const required = [...PREFLIGHT_REQUIRED_CHECKS];
  for (const key of required) {
    const checks = preflightCheck(result, key);
    if (checks.length === 0 || checks.some((item) => item.state !== 'PASSED')) {
      throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
        checkKey: key,
      });
    }
  }

  const primaryKeyChecks = preflightCheck(result, 'PRIMARY_KEY');
  const resourceChecks = preflightCheck(result, 'PG_RESOURCES');
  const expectedState = config.kind === 'KAFKA_RAW_FORWARD' ? 'SKIPPED' : 'PASSED';
  if (
    primaryKeyChecks.length === 0 ||
    primaryKeyChecks.some((item) => item.state !== expectedState) ||
    resourceChecks.length === 0 ||
    resourceChecks.some(
      (item) => item.state !== (config.kind === 'PG_TO_KAFKA' ? 'PASSED' : 'SKIPPED'),
    )
  ) {
    throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
      checkKey: config.kind === 'KAFKA_RAW_FORWARD' ? 'PRIMARY_KEY' : 'PG_RESOURCES',
    });
  }
  if (
    config.kind === 'MYSQL_TO_KAFKA' &&
    (preflightCheck(result, 'MYSQL_CDC').length !== 1 ||
      preflightCheck(result, 'MYSQL_CDC')[0]?.state !== 'PASSED')
  )
    throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', { checkKey: 'MYSQL_CDC' });
}

function kafkaPartitionIds(
  result: TaskPreflightResult,
  config: PlatformTaskConfig,
  sourceTopicName: string,
): readonly number[] {
  if (config.kind !== 'KAFKA_RAW_FORWARD') {
    return [];
  }
  const topicCheck = preflightCheck(result, 'TOPIC_IDENTITIES')[0];
  const details = topicCheck?.details;
  const detailsRecord =
    typeof details === 'object' && details !== null ? (details as Record<string, unknown>) : null;
  const topics =
    detailsRecord !== null && Array.isArray(detailsRecord.topics) ? detailsRecord.topics : null;
  const sourceTopic = topics?.find((item) => {
    if (typeof item !== 'object' || item === null) {
      return false;
    }
    const record = item as Record<string, unknown>;
    return record.resourceType === 'KAFKA_SOURCE_TOPIC' && record.topic === sourceTopicName;
  });
  const partitionIds =
    typeof sourceTopic === 'object' && sourceTopic !== null
      ? (sourceTopic as Record<string, unknown>).partitionIds
      : null;
  if (
    !Array.isArray(partitionIds) ||
    partitionIds.length === 0 ||
    partitionIds.some(
      (partitionId) => !Number.isSafeInteger(partitionId) || (partitionId as number) < 0,
    ) ||
    new Set(partitionIds).size !== partitionIds.length
  ) {
    throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
      reason: 'KAFKA_PARTITION_BASELINE_MISSING',
    });
  }
  const expected = [...(partitionIds as number[])].sort((left, right) => left - right);
  return expected;
}

function runtimeUnitKeys(
  result: TaskPreflightResult,
  config: PlatformTaskConfig,
): readonly string[] {
  return config.kind !== 'KAFKA_RAW_FORWARD'
    ? ['primary']
    : kafkaTaskMappings(config).flatMap((mapping) =>
        kafkaPartitionIds(result, config, mapping.source).map((partitionId) =>
          kafkaUnitKey(config, mapping.source, partitionId),
        ),
      );
}

function assertKafkaPartitionUnits(
  result: TaskPreflightResult,
  config: PlatformTaskConfig,
  units: readonly RuntimeUnitLaunchRequest[],
): void {
  if (config.kind !== 'KAFKA_RAW_FORWARD') return;
  const expected = [...runtimeUnitKeys(result, config)].sort();
  const actual = units.map((unit) => unit.unitKey).sort();
  if (
    actual.some((unitKey) => kafkaSourceBinding(config, unitKey) === null) ||
    actual.length !== expected.length ||
    actual.some((partitionId, index) => partitionId !== expected[index])
  ) {
    throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
      reason: 'KAFKA_PARTITION_SET_CHANGED',
      expectedPartitions: expected,
      actualPartitions: actual,
    });
  }
}

@Injectable()
export class RuntimeSupervisorService
  implements BeforeApplicationShutdown, OnApplicationShutdown, AutoRecoveryPort
{
  private readonly activeProcesses = new Map<string, Map<string, ManagedRuntimeProcess>>();
  private readonly activeRuntimeFacts = new Map<
    string,
    {
      readonly runId: string;
      readonly operationId: string;
      readonly platformInstanceId: string;
      readonly revisionId: string;
      readonly taskId: string;
      readonly startedAtMilliseconds: number;
      readonly units: Map<
        string,
        { readonly pid: number | undefined; readonly processToken: string }
      >;
    }
  >();
  private readonly memoryTerminations = new Map<string, Promise<MemoryRuntimeTerminationResult>>();
  private readonly failureInProgress = new Set<string>();
  private readonly taskTerminalActions = new Map<string, Promise<void>>();
  private activeStarts = 0;
  private startDrain: Promise<void> = Promise.resolve();
  private resolveStartDrain: (() => void) | null = null;
  private shutdownInProgress = false;
  private readonly autoRecovery: FixedAutoRecoveryCoordinator;
  private readonly mysqlRuns = new Map<string, PreparedStart>();
  private readonly logger = new Logger('RuntimeSupervisor');

  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    private readonly capacity: CapacityReservationService,
    private readonly processes: RuntimeProcessSupervisor,
    private readonly launchFactory: RuntimeLaunchSpecFactory,
    @Optional() private readonly bootstrapState?: BootstrapStateService,
    @Optional()
    @Inject(SYSTEM_RECOVERY_PREFLIGHT)
    private readonly preflightService?: SystemRecoveryPreflightPort,
    @Optional() private readonly metricsPorts?: RuntimeMetricsPortAllocator,
    @Optional() private readonly mysqlState?: MysqlRuntimeStateStore,
    @Optional() private readonly recoverySettings?: RuntimeRecoverySettingsService,
    @Optional() private readonly sources?: DataSourcesService,
    @Optional() private readonly alerts?: AlertsService,
    @Optional() private readonly readiness?: PlatformReadinessService,
  ) {
    this.autoRecovery = new FixedAutoRecoveryCoordinator(this);
  }

  private async assertContinuityReady(
    client: PoolClient,
    taskId: string,
    revisionId: string,
  ): Promise<void> {
    const result = await client.query<{
      decision: string;
      task_revision_id: string;
      row_version: string;
      baseline_version: string | null;
      decision_code: string;
    }>(
      `SELECT observation.decision,baseline.task_revision_id,baseline.row_version::text,
              observation.baseline_version,observation.decision_code
         FROM ape_dts_platform.task_continuity_baselines AS baseline
         JOIN LATERAL (
           SELECT evidence.decision,evidence.decision_code,
             evidence.observation->>'pgTableAdditionBaselineVersion' AS baseline_version
             FROM ape_dts_platform.task_continuity_observations AS evidence
            WHERE evidence.task_id = baseline.task_id
              AND evidence.task_revision_id = $2::uuid
            ORDER BY evidence.observed_at DESC, evidence.id DESC
            LIMIT 1
         ) AS observation ON true
        WHERE baseline.task_id = $1::uuid
        FOR SHARE OF baseline`,
      [taskId, revisionId],
    );
    const row = result.rows[0];
    if (
      row?.decision !== 'MATCHED' ||
      (row.task_revision_id !== revisionId &&
        !(
          row.decision_code === 'PG_TABLE_ADDITION_READY' &&
          row.baseline_version === row.row_version &&
          (await confirmedPgTableExpansion(client, taskId, revisionId))
        ))
    ) {
      throw new RuntimeSupervisorError('RUNTIME_CONTINUITY_BLOCKED');
    }
  }

  private async revalidateExternalContinuity(
    taskId: string,
    revisionId: string,
    rowVersion: string,
    desiredState: 'DELETED' | 'RUNNING' | 'STOPPED',
    reinitializeMysqlSnapshot?: boolean,
  ): Promise<void> {
    if (this.preflightService?.revalidateContinuityBeforeRuntime === undefined) {
      throw new RuntimeSupervisorError('RUNTIME_CONTINUITY_BLOCKED');
    }
    try {
      await this.preflightService.revalidateContinuityBeforeRuntime({
        desiredState,
        id: taskId,
        revisionId,
        rowVersion,
        ...(reinitializeMysqlSnapshot === true ? { reinitializeMysqlSnapshot: true } : {}),
      });
    } catch (error) {
      const reason = recoveryErrorCode(error);
      this.logger.error({
        code: 'RUNTIME_CONTINUITY_VALIDATION_FAILED',
        taskId,
        revisionId,
        reason,
      });
      throw new RuntimeSupervisorError('RUNTIME_CONTINUITY_BLOCKED', { reason });
    }
  }

  listActiveRuntimeTasks(): readonly ActiveRuntimeTaskSnapshot[] {
    return [...this.activeRuntimeFacts.values()]
      .filter((fact) => this.activeProcesses.has(fact.runId))
      .map((fact) => ({
        operationId: fact.operationId,
        platformInstanceId: fact.platformInstanceId,
        revisionId: fact.revisionId,
        runId: fact.runId,
        taskId: fact.taskId,
        units: [...fact.units.entries()]
          .filter(([unitId]) => this.activeProcesses.get(fact.runId)?.has(unitId) === true)
          .map(([unitId, unit]) => ({ ...unit, unitId }))
          .sort((left, right) => left.unitId.localeCompare(right.unitId)),
      }))
      .filter((fact) => fact.units.length > 0)
      .sort((left, right) => left.taskId.localeCompare(right.taskId));
  }

  /** Retire only an owned CDC process, after independently confirming its slot is disconnected. */
  async recoverDisconnectedPostgres(input: {
    taskId: string;
    runId: string;
    revisionId: string;
    collectedAt: string;
  }): Promise<boolean> {
    return this.withTaskTerminalAction(input.taskId, async () => {
      if (this.shutdownInProgress || !this.sources) return false;
      const fact = this.activeRuntimeFacts.get(input.runId);
      const handles = this.activeProcesses.get(input.runId);
      if (
        !fact ||
        fact.taskId !== input.taskId ||
        fact.revisionId !== input.revisionId ||
        !handles?.size
      )
        return false;
      const sampledAt = Date.parse(input.collectedAt);
      if (
        !Number.isFinite(sampledAt) ||
        sampledAt < fact.startedAtMilliseconds ||
        Date.now() - sampledAt < 0 ||
        Date.now() - sampledAt > 45_000
      )
        return false;
      const row = (
        await this.database.requirePool().query<{
          configuration: unknown;
          source_version: string;
        }>(
          `SELECT revision.configuration, source.row_version::text AS source_version
        FROM ape_dts_platform.tasks task
        JOIN ape_dts_platform.task_runs run ON run.task_id=task.id
        JOIN ape_dts_platform.task_revisions revision ON revision.id=run.task_revision_id
        JOIN ape_dts_platform.data_sources source ON source.id=task.source_data_source_id
        WHERE task.id=$1::uuid AND run.id=$2::uuid AND run.task_revision_id=$3::uuid
          AND run.platform_instance_id=$4::uuid AND task.current_applied_revision_id=run.task_revision_id
          AND task.deleted_at IS NULL AND task.desired_state='RUNNING' AND task.observed_state='RUNNING'
          AND run.status='RUNNING' AND run.current_stage IN ('CDC_STARTED','MONITORING_READY')
          AND source.state='ACTIVE' AND source.deleted_at IS NULL
          AND EXISTS (SELECT 1 FROM ape_dts_platform.preflight_runs preflight
            WHERE preflight.task_revision_id=run.task_revision_id AND preflight.state='PASSED'
              AND preflight.started_at<=run.started_at AND preflight.completed_at<=run.started_at
              AND preflight.result #>> '{dataSourceVersions,source}'=source.row_version::text)`,
          [input.taskId, input.runId, input.revisionId, fact.platformInstanceId],
        )
      ).rows[0];
      if (!row) return false;
      const parsed = platformTaskConfigSchema.safeParse(row.configuration);
      if (!parsed.success || parsed.data.kind !== 'PG_TO_KAFKA') return false;
      const confirmation = await this.sources.readProgressForSystem(
        parsed.data.source.dataSourceId,
        { slotName: parsed.data.source.slotName },
        row.source_version,
      );
      if (confirmation.slotStatus !== 'INACTIVE') return false;
      const policy = await this.policy();
      if (
        this.shutdownInProgress ||
        !policy.enabled ||
        (this.readiness && this.readiness.getPhase() !== 'READY')
      )
        return false;
      const [unitId, process] = handles.entries().next().value!;
      this.logger.error({
        code: 'TASK_PG_REPLICATION_DISCONNECTED',
        taskId: input.taskId,
        runId: input.runId,
        slotName: parsed.data.source.slotName,
      });
      const exit = await process.stop();
      await this.observeUnexpectedExitWithinTerminalAction(
        input.taskId,
        input.runId,
        unitId,
        exit,
        'RUNTIME_PG_REPLICATION_DISCONNECTED',
      );
      return true;
    });
  }

  terminateActiveTask(
    taskId: string,
    mode: MemoryRuntimeTerminationMode,
  ): Promise<MemoryRuntimeTerminationResult> {
    const key = `${taskId}:${mode}`;
    const existing = this.memoryTerminations.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const termination = this.terminateActiveTaskOnce(taskId, mode).finally(() => {
      if (this.memoryTerminations.get(key) === termination) {
        this.memoryTerminations.delete(key);
      }
    });
    this.memoryTerminations.set(key, termination);
    return termination;
  }

  private async terminateActiveTaskOnce(
    taskId: string,
    mode: MemoryRuntimeTerminationMode,
  ): Promise<MemoryRuntimeTerminationResult> {
    const facts = [...this.activeRuntimeFacts.values()].filter(
      (candidate) => candidate.taskId === taskId && this.activeProcesses.has(candidate.runId),
    );
    if (facts.length !== 1) {
      throw new RuntimeSupervisorError('RUNTIME_PROCESS_HANDLE_MISSING', {
        activeRunCount: facts.length,
        taskId,
      });
    }
    const fact = facts[0]!;
    const handles = this.activeProcesses.get(fact.runId)!;
    const units = await Promise.all(
      [...fact.units.entries()]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(async ([unitId, unit]) => {
          const handle = handles.get(unitId);
          if (handle === undefined) {
            return {
              ...unit,
              confirmed: false,
              forced: mode === 'FORCE',
              signal: null,
              timeout: true,
              unitId,
            };
          }
          try {
            const exit = mode === 'FORCE' ? await handle.forceKill() : await handle.stop();
            return {
              ...unit,
              confirmed: exit.confirmed,
              forced: exit.forcedKill,
              signal: exit.signal,
              timeout: exit.terminationTimedOut,
              unitId,
            };
          } catch {
            return {
              ...unit,
              confirmed: false,
              forced: mode === 'FORCE',
              signal: null,
              timeout: true,
              unitId,
            };
          }
        }),
    );
    const allConfirmed = units.length > 0 && units.every((unit) => unit.confirmed && !unit.timeout);
    if (allConfirmed) {
      await this.mysqlState?.flush(fact.runId);
      this.processes.finalizeRun?.(fact.runId);
      this.forgetActiveRun(fact.runId);
    }
    return { allConfirmed, mode, runId: fact.runId, taskId, units };
  }

  private rememberActiveRun(
    prepared: PreparedStart,
    handles: Map<string, ManagedRuntimeProcess>,
  ): void {
    this.activeProcesses.set(prepared.runId, handles);
    if (prepared.mysql !== undefined) this.mysqlRuns.set(prepared.runId, prepared);
    this.activeRuntimeFacts.set(prepared.runId, {
      startedAtMilliseconds: Date.now(),
      operationId: prepared.operationId,
      platformInstanceId: prepared.platformInstanceId,
      revisionId: prepared.revisionId,
      runId: prepared.runId,
      taskId: prepared.taskId,
      units: new Map(
        prepared.units
          .filter((unit) => handles.has(unit.id))
          .map((unit) => [
            unit.id,
            { pid: handles.get(unit.id)?.pid, processToken: unit.processToken },
          ]),
      ),
    });
  }

  private forgetActiveRun(runId: string): void {
    this.mysqlState?.unwatch(runId);
    const mysql = this.mysqlRuns.get(runId);
    if (mysql !== undefined)
      for (const unit of mysql.units) {
        void this.mysqlState?.cleanup(unit.launch.runtimeDirectory).catch(() => {
          this.logger.warn({ code: 'MYSQL_RUNTIME_LOG_CLEANUP_FAILED', runId, unitId: unit.id });
        });
      }
    this.mysqlRuns.delete(runId);
    this.activeProcesses.delete(runId);
    this.activeRuntimeFacts.delete(runId);
  }

  /**
   * A new container has no trustworthy handles for processes recorded by an
   * older instance.  Mark those runs BLOCKED instead of pretending they
   * stopped; capacity remains reserved until an operator proves the external
   * process is gone.
   */
  async reconcilePersistedRuns(): Promise<number> {
    return await this.transaction(async (client) => {
      const instanceId = await this.requirePlatformInstanceId(client);
      const active = await client.query<{ id: string; task_id: string }>(
        `SELECT id, task_id
           FROM ape_dts_platform.task_runs
          WHERE status IN ('STARTING', 'RUNNING', 'STOPPING')
            AND platform_instance_id IS DISTINCT FROM $1::uuid
          ORDER BY started_at, id`,
        [instanceId],
      );
      let reconciled = 0;
      for (const run of active.rows) {
        if (this.activeProcesses.has(run.id)) {
          continue;
        }
        if (
          await this.markRunBlockedWithin(client, run.task_id, run.id, {
            failureCode: 'RUNTIME_CONTAINER_RESTART_REQUIRES_RECONCILIATION',
            failureSummary: '容器重建后没有可验证的旧进程句柄，需人工核对旧实例已停止。',
          })
        ) {
          reconciled += 1;
        }
      }
      return reconciled;
    });
  }

  async discardRecovery(
    actor: UserSummary,
    taskId: string,
    expectedTaskRowVersion: string,
  ): Promise<{ discarded: true }> {
    if (actor.role !== 'ADMIN') throw new RuntimeSupervisorError('RUNTIME_ACCESS_DENIED');
    return await this.withTaskTerminalAction(taskId, () =>
      this.transaction(async (client) => {
        const task = (
          await client.query<TaskRow>(
            `SELECT ${TASK_COLUMNS} FROM ape_dts_platform.tasks WHERE id=$1::uuid FOR UPDATE`,
            [taskId],
          )
        ).rows[0];
        if (task === undefined || task.deleted_at !== null)
          throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
        if (task.row_version !== expectedTaskRowVersion)
          throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
        if (task.observed_state !== 'BLOCKED')
          throw new RuntimeSupervisorError('RUNTIME_STOP_NOT_ALLOWED');
        if (this.listActiveRuntimeTasks().some((active) => active.taskId === taskId)) {
          throw new RuntimeSupervisorError('RUNTIME_ALREADY_RUNNING');
        }
        const instanceId = await this.requirePlatformInstanceId(client);
        const owned = await client.query(
          `SELECT id FROM ape_dts_platform.task_runs
          WHERE task_id=$1::uuid AND platform_instance_id=$2::uuid
            AND status IN ('STARTING','RUNNING','STOPPING','BLOCKED') FOR UPDATE`,
          [taskId, instanceId],
        );
        if (owned.rows.length > 0) throw new RuntimeSupervisorError('RUNTIME_ALREADY_RUNNING');
        let draftId = task.current_draft_revision_id;
        if (draftId === null && task.current_applied_revision_id !== null) {
          draftId = randomUUID();
          const copied = await client.query(
            `INSERT INTO ape_dts_platform.task_revisions
             (id, task_id, revision_number, state, configuration_schema_version, configuration,
              artifact_id, preview_ini_sha256, semantic_sha256, created_by_user_id, updated_by_user_id)
           SELECT $1::uuid, task_id,
                  (SELECT max(revision_number)+1 FROM ape_dts_platform.task_revisions WHERE task_id=$2::uuid),
                  'READY_DRAFT', configuration_schema_version, configuration, artifact_id,
                  preview_ini_sha256, semantic_sha256, $4::uuid, $4::uuid
             FROM ape_dts_platform.task_revisions WHERE id=$3::uuid AND task_id=$2::uuid`,
            [draftId, taskId, task.current_applied_revision_id, actor.id],
          );
          if (copied.rowCount !== 1) throw new RuntimeSupervisorError('RUNTIME_REVISION_INVALID');
          await client.query(
            `INSERT INTO ape_dts_platform.pg_kafka_revision_configs
             (task_revision_id, source_database, source_schema, source_table, primary_key_column,
              target_topic, resource_mode, slot_name, publication_name, include_snapshot, mappings)
           SELECT $1::uuid, source_database, source_schema, source_table, primary_key_column,
                  target_topic, resource_mode, slot_name, publication_name, include_snapshot, mappings
             FROM ape_dts_platform.pg_kafka_revision_configs WHERE task_revision_id=$2::uuid`,
            [draftId, task.current_applied_revision_id],
          );
          await client.query(
            `INSERT INTO ape_dts_platform.kafka_kafka_revision_configs
             (task_revision_id, source_topic, target_topic, group_id, required_acks, mappings)
           SELECT $1::uuid, source_topic, target_topic, group_id, required_acks, mappings
             FROM ape_dts_platform.kafka_kafka_revision_configs WHERE task_revision_id=$2::uuid`,
            [draftId, task.current_applied_revision_id],
          );
        }
        // Administrator confirmation closes only old bookkeeping; it never resets CDC positions.
        const runs = await client.query<{ id: string }>(
          `SELECT id FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid FOR UPDATE`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_run_units
            SET status=CASE WHEN status IN ('STARTING','RUNNING','STOPPING') THEN 'BLOCKED' ELSE status END,
                ended_at=COALESCE(ended_at, statement_timestamp()),
                failure_code=CASE WHEN status IN ('STARTING','RUNNING','STOPPING') THEN 'RUNTIME_RECOVERY_DISCARDED' ELSE failure_code END,
                failure_summary=CASE WHEN status IN ('STARTING','RUNNING','STOPPING') THEN '管理员确认旧运行已停止并放弃恢复。' ELSE failure_summary END,
                metrics_port_released_at=COALESCE(metrics_port_released_at, statement_timestamp())
          WHERE task_id=$1::uuid`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_runs SET status='BLOCKED', current_stage='RECONCILIATION',
                ended_at=statement_timestamp(), failure_code='RUNTIME_RECOVERY_DISCARDED',
                failure_summary='管理员确认旧运行已停止并放弃恢复。'
          WHERE task_id=$1::uuid AND status IN ('STARTING','RUNNING','STOPPING')`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_run_stages SET status='PAUSED',
                failure_code='RUNTIME_RECOVERY_DISCARDED', failure_summary='管理员放弃旧运行恢复。',
                heartbeat_at=statement_timestamp()
          WHERE run_id=ANY($1::uuid[]) AND status IN ('PENDING','RUNNING')`,
          [runs.rows.map((run) => run.id)],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_operations SET state='BLOCKED_MANUAL', execution_stage='RECONCILIATION',
                failure_code='RUNTIME_RECOVERY_DISCARDED', failure_summary='管理员放弃旧运行恢复。',
                completed_at=statement_timestamp(), updated_at=statement_timestamp()
          WHERE task_id=$1::uuid AND state IN ('ACCEPTED','PREFLIGHTING','PREPARED','EXECUTING')`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.capacity_reservations SET state='RELEASED', released_at=statement_timestamp()
          WHERE task_id=$1::uuid AND state IN ('RESERVED','ACTIVE')`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.preflight_runs SET state='STALE',
                failure_code='RUNTIME_RECOVERY_DISCARDED', failure_summary='旧运行恢复已放弃，请重新预检。'
          WHERE task_revision_id IN (SELECT id FROM ape_dts_platform.task_revisions WHERE task_id=$1::uuid)
            AND state='PASSED'`,
          [taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.tasks SET desired_state='STOPPED', observed_state='STOPPED',
                current_draft_revision_id=$2::uuid,
                recovery_disposition='NOT_APPLICABLE', manual_gate_reason=NULL,
                observability_health='UNAVAILABLE', row_version=row_version+1, updated_at=statement_timestamp()
          WHERE id=$1::uuid`,
          [taskId, draftId],
        );
        await this.audit.append(client, {
          action: 'TASK_RUNTIME_RECOVERY_DISCARDED',
          actorType: 'USER',
          actorUserId: actor.id,
          details: { oldRuntimeStoppedConfirmed: true, oldRunIds: runs.rows.map((run) => run.id) },
          subjectId: taskId,
          subjectType: 'TASK',
        });
        return { discarded: true };
      }),
    );
  }

  /**
   * Stop every process still owned by this supervisor during application
   * shutdown.  A missing terminal close event is kept as BLOCKED and its
   * capacity is deliberately not released.
   */
  async beforeApplicationShutdown(signal?: string): Promise<void> {
    // Persist process termination while the database and keys are still available.
    await this.onApplicationShutdown(signal);
  }

  async onApplicationShutdown(signal?: string): Promise<void> {
    if (this.shutdownInProgress) {
      return;
    }
    this.shutdownInProgress = true;
    this.autoRecovery.onApplicationShutdown();
    await this.startDrain;
    const entries = [...this.activeProcesses.entries()];
    if (entries.length === 0) {
      if (this.bootstrapState !== undefined && this.database.hasPool()) {
        await this.transaction((client) => this.recordPlatformShutdown(client, true, signal)).catch(
          () => this.logger.warn('RUNTIME_SHUTDOWN_PERSISTENCE_FAILED'),
        );
      }
      return;
    }
    // Process termination must not depend on a reachable metadata database.
    // These facts are captured alongside the handles when the run is admitted.
    const entriesWithTask = entries.map(([runId, processes]) => ({
      processes,
      runId,
      taskId: this.activeRuntimeFacts.get(runId)?.taskId ?? null,
    }));
    const outcomes = await Promise.all(
      entriesWithTask.map(async ({ processes, runId, taskId }) => {
        const stopProcesses = async () => {
          const results = await Promise.allSettled(
            [...processes.entries()].map(async ([unitId, process]) => ({
              result: await process.stop(),
              unitId,
            })),
          );
          const units = results.map((entry) =>
            entry.status === 'fulfilled'
              ? entry.value
              : {
                  result: {
                    confirmed: false,
                    code: null,
                    forcedKill: false,
                    logGapBytes: 0,
                    logRecoveryState: 'NONE',
                    logStorageState: 'HEALTHY',
                    logStreamError: false,
                    signal: null,
                    signals: [] as readonly NodeJS.Signals[],
                    terminationTimedOut: true,
                  } satisfies RuntimeProcessExit,
                  unitId: null,
                },
          );
          return {
            allConfirmed:
              units.length > 0 &&
              units.every(
                (unit) => unit.result.confirmed && unit.result.terminationTimedOut === false,
              ),
            runId,
            taskId,
            units,
          };
        };
        return taskId === null
          ? await stopProcesses()
          : await this.withTaskTerminalAction(taskId, stopProcesses);
      }),
    );
    const settledOutcomes = await Promise.all(
      outcomes.map(async (outcome) => {
        const logResolution = await this.settleRunLogState(
          outcome.runId,
          outcome.units.map((unit) => unit.result),
        ).catch(() => {
          this.logger.warn('RUNTIME_SHUTDOWN_LOG_SETTLEMENT_FAILED');
          return null;
        });
        return { ...outcome, logResolution };
      }),
    );

    try {
      await this.withTaskTerminalActions(
        settledOutcomes.flatMap((outcome) => (outcome.taskId === null ? [] : [outcome.taskId])),
        async () =>
          this.transaction(async (client) => {
            for (const outcome of settledOutcomes) {
              if (outcome.taskId === null || outcome.logResolution === null) {
                continue;
              }
              const hasLogGap = outcome.units.some(
                (unit) => unit.result.logGapBytes > 0 || unit.result.logStreamError === true,
              );
              const configurationCleanupFailed = hasConfigurationCleanupFailure(
                outcome.units.map((unit) => unit.result),
              );
              const cleanShutdown = outcome.allConfirmed && !configurationCleanupFailed;
              const failureCode = !outcome.allConfirmed
                ? 'RUNTIME_SHUTDOWN_PROCESS_UNCONFIRMED'
                : configurationCleanupFailed
                  ? 'RUNTIME_CONFIGURATION_CLEANUP_FAILED'
                  : null;
              const failureSummary = !outcome.allConfirmed
                ? '应用关闭时仍有运行单元未确认退出，容量保持占用并需人工核对。'
                : configurationCleanupFailed
                  ? CONFIGURATION_CLEANUP_FAILURE_SUMMARY
                  : null;
              const logState =
                outcome.logResolution.state ??
                aggregateRuntimeLogState(outcome.units.map((unit) => unit.result));
              const task = await client.query<{ id: string; desired_state: string }>(
                `SELECT id, desired_state FROM ape_dts_platform.tasks
              WHERE id = $1::uuid AND deleted_at IS NULL FOR UPDATE`,
                [outcome.taskId],
              );
              if (task.rows[0] === undefined) {
                continue;
              }
              const resumeAfterRestart = task.rows[0].desired_state === 'RUNNING';
              const releaseCapacity = outcome.allConfirmed && !resumeAfterRestart;
              const pendingRecovery = resumeAfterRestart && cleanShutdown;
              const persistedCode = pendingRecovery
                ? 'RUNTIME_PLATFORM_STOPPED_FOR_RESTART'
                : failureCode;
              const persistedSummary = pendingRecovery
                ? '平台已停止，保留原运行绑定，等待重启恢复。'
                : failureSummary;
              const run = await client.query<{ task_id: string }>(
                `SELECT task_id
               FROM ape_dts_platform.task_runs
              WHERE id = $1::uuid
                AND status IN ('STARTING', 'RUNNING', 'STOPPING')
              FOR UPDATE`,
                [outcome.runId],
              );
              const taskId = run.rows[0]?.task_id;
              if (taskId !== outcome.taskId) {
                continue;
              }
              await client.query(
                `UPDATE ape_dts_platform.task_run_units
                 SET status = CASE WHEN $2::boolean AND NOT $6::boolean THEN 'STOPPED' ELSE 'BLOCKED' END,
                    ended_at = statement_timestamp(), failure_code = $3, failure_summary = $4,
                    metrics_port_released_at = CASE
                      WHEN $5::boolean THEN statement_timestamp()
                      ELSE metrics_port_released_at
                    END
              WHERE run_id = $1::uuid
                AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
                [
                  outcome.runId,
                  outcome.allConfirmed,
                  persistedCode,
                  persistedSummary,
                  releaseCapacity,
                  resumeAfterRestart,
                ],
              );
              await client.query(
                `UPDATE ape_dts_platform.task_runs
                 SET status = CASE WHEN $2::boolean AND NOT $9::boolean THEN 'STOPPED' ELSE 'BLOCKED' END,
                     current_stage = CASE WHEN $9::boolean THEN 'RECONCILIATION' ELSE 'SHUTDOWN' END,
                     ended_at = statement_timestamp(),
                     failure_code = $3, failure_summary = $4,
                      has_gap = has_gap OR $5::boolean,
                      log_storage_state = CASE
                        WHEN NOT $8::boolean THEN log_storage_state
                        WHEN log_storage_state = 'DEGRADED' OR $6 = 'DEGRADED' THEN 'DEGRADED'
                        WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                        ELSE $6
                      END,
                      log_recovery_state = CASE
                        WHEN $8::boolean THEN $7
                        ELSE log_recovery_state
                      END
               WHERE id = $1::uuid
                 AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
                [
                  outcome.runId,
                  cleanShutdown,
                  persistedCode,
                  persistedSummary,
                  hasLogGap,
                  logState.storageState,
                  logState.recoveryState,
                  outcome.logResolution.persistInTerminalTransaction,
                  resumeAfterRestart,
                ],
              );
              await client.query(
                `UPDATE ape_dts_platform.task_run_stages
                 SET status = CASE WHEN $2::boolean AND NOT $5::boolean THEN 'SUCCEEDED' ELSE 'PAUSED' END,
                    ended_at = CASE WHEN $2::boolean AND NOT $5::boolean THEN statement_timestamp() ELSE NULL END,
                    heartbeat_at = statement_timestamp(),
                    failure_code = $3, failure_summary = $4
              WHERE run_id = $1::uuid
                AND status IN ('PENDING', 'RUNNING')`,
                [outcome.runId, cleanShutdown, persistedCode, persistedSummary, resumeAfterRestart],
              );
              if (releaseCapacity) {
                await this.capacity.releaseWithin(client, outcome.runId);
              }
              await client.query(
                `UPDATE ape_dts_platform.tasks
                SET observed_state = CASE WHEN $2::boolean AND NOT $3::boolean THEN 'STOPPED' ELSE 'BLOCKED' END,
                    row_version = row_version + 1, updated_at = statement_timestamp()
              WHERE id = $1::uuid
                AND observed_state IN ('STARTING', 'RUNNING', 'STOPPING')`,
                [taskId, cleanShutdown, resumeAfterRestart],
              );
              await client.query(
                `UPDATE ape_dts_platform.task_operations
                SET state = CASE WHEN $2::boolean THEN 'SUCCEEDED' ELSE 'BLOCKED_MANUAL' END,
                    execution_stage = 'SHUTDOWN',
                    failure_code = $3, failure_summary = $4,
                    completed_at = statement_timestamp(), updated_at = statement_timestamp()
              WHERE run_id = $1::uuid
                AND (
                  state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')
                  OR ($5::boolean AND state = 'SUCCEEDED')
                )`,
                [
                  outcome.runId,
                  cleanShutdown,
                  failureCode,
                  failureSummary,
                  configurationCleanupFailed,
                ],
              );
              await this.audit.append(client, {
                action: cleanShutdown
                  ? 'TASK_RUNTIME_CONTAINER_SHUTDOWN'
                  : 'TASK_RUNTIME_CONTAINER_SHUTDOWN_BLOCKED',
                actorType: 'SYSTEM',
                actorUserId: null,
                details: {
                  runId: outcome.runId,
                  capacityReleased: releaseCapacity,
                  configurationCleanupFailed,
                  shutdownSignal: signal ?? null,
                  unitCount: outcome.units.length,
                },
                subjectId: taskId,
                subjectType: 'TASK',
              });
            }
            if (this.bootstrapState !== undefined) {
              await this.recordPlatformShutdown(
                client,
                settledOutcomes.every(
                  (outcome) =>
                    outcome.logResolution !== null &&
                    outcome.allConfirmed &&
                    !hasConfigurationCleanupFailure(outcome.units.map((unit) => unit.result)),
                ),
                signal,
              );
            }
          }),
      );
    } catch {
      // Keep handles in memory.  A later reconciliation pass may still prove
      // the terminal process state when the database becomes available.
      this.logger.warn('RUNTIME_SHUTDOWN_PERSISTENCE_FAILED');
      return;
    }

    for (const outcome of settledOutcomes) {
      if (outcome.allConfirmed && outcome.logResolution !== null) {
        this.processes.finalizeRun(outcome.runId);
        this.forgetActiveRun(outcome.runId);
      }
    }
  }

  private async recordPlatformShutdown(
    client: PoolClient,
    clean: boolean,
    signal?: string,
  ): Promise<void> {
    await client.query(
      `UPDATE ape_dts_platform.platform_instances AS instance
      SET ended_at=statement_timestamp(), ended_reason=$3,
          clean_shutdown=$2::boolean AND NOT EXISTS (
            SELECT 1 FROM ape_dts_platform.task_runs AS run WHERE run.platform_instance_id=instance.id
              AND (run.status IN ('STARTING','RUNNING','STOPPING') OR
                   (run.status='BLOCKED' AND run.failure_code IS DISTINCT FROM 'RUNTIME_PLATFORM_STOPPED_FOR_RESTART')))
      WHERE instance.id=$1::uuid AND instance.ended_at IS NULL`,
      [this.bootstrapState!.getStatus().platformInstanceId, clean, signal ?? 'APPLICATION_CLOSE'],
    );
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async withTaskTerminalAction<T>(taskId: string, work: () => Promise<T>): Promise<T> {
    const previous = this.taskTerminalActions.get(taskId) ?? Promise.resolve();
    let release: (() => void) | undefined;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.taskTerminalActions.set(taskId, current);
    await previous.catch(() => undefined);
    try {
      return await work();
    } finally {
      release?.();
      if (this.taskTerminalActions.get(taskId) === current) {
        this.taskTerminalActions.delete(taskId);
      }
    }
  }

  private async withTaskTerminalActions<T>(
    taskIds: readonly string[],
    work: () => Promise<T>,
  ): Promise<T> {
    const ordered = [...new Set(taskIds)].sort();
    const acquire = async (index: number): Promise<T> =>
      index === ordered.length
        ? await work()
        : await this.withTaskTerminalAction(ordered[index]!, async () => acquire(index + 1));
    return await acquire(0);
  }

  private async settleRunLogState(
    runId: string,
    results: readonly RuntimeProcessExit[],
  ): Promise<RuntimeLogStateResolution> {
    await this.mysqlState?.flush(runId);
    const settled = await this.processes.settleRunLogs?.(runId);
    if (settled !== undefined) {
      return {
        persistInTerminalTransaction: settled.statePersisted !== true,
        state: settled,
      };
    }
    return {
      persistInTerminalTransaction: results.length > 0,
      state: results.length > 0 ? aggregateRuntimeLogState(results) : null,
    };
  }

  /**
   * Mark a persisted run as requiring manual reconciliation without asserting
   * that its external process has exited.  The task row is locked first to
   * match start/stop's lock order; the run is then re-checked while locked so
   * a concurrent terminal transition is never overwritten.
   */
  private async markRunBlockedWithin(
    client: PoolClient,
    taskId: string,
    runId: string,
    reason: { readonly failureCode: string; readonly failureSummary: string },
  ): Promise<boolean> {
    const task = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.tasks
        WHERE id = $1::uuid
          AND deleted_at IS NULL
        FOR UPDATE`,
      [taskId],
    );
    if (task.rows[0] === undefined) {
      return false;
    }

    const run = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.task_runs
        WHERE id = $1::uuid
          AND task_id = $2::uuid
          AND status IN ('STARTING', 'RUNNING', 'STOPPING')
        FOR UPDATE`,
      [runId, taskId],
    );
    if (run.rows[0] === undefined) {
      return false;
    }

    const units = await client.query(
      `UPDATE ape_dts_platform.task_run_units
          SET status = 'BLOCKED',
              ended_at = statement_timestamp(),
              failure_code = $3,
              failure_summary = $4
        WHERE run_id = $1::uuid
          AND task_id = $2::uuid
          AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
      [runId, taskId, reason.failureCode, reason.failureSummary],
    );
    await client.query(
      `UPDATE ape_dts_platform.task_runs
          SET status = 'BLOCKED',
              current_stage = 'RECONCILIATION',
              ended_at = statement_timestamp(),
              failure_code = $3,
              failure_summary = $4
        WHERE id = $1::uuid
          AND task_id = $2::uuid
          AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
      [runId, taskId, reason.failureCode, reason.failureSummary],
    );
    await client.query(
      `UPDATE ape_dts_platform.task_run_stages
          SET status = 'PAUSED',
              failure_code = $2,
              failure_summary = $3,
              heartbeat_at = statement_timestamp()
        WHERE run_id = $1::uuid
          AND status IN ('PENDING', 'RUNNING')`,
      [runId, reason.failureCode, reason.failureSummary],
    );
    await client.query(
      `UPDATE ape_dts_platform.tasks
          SET observed_state = 'BLOCKED',
              row_version = row_version + 1,
              updated_at = statement_timestamp()
        WHERE id = $1::uuid
          AND deleted_at IS NULL
          AND observed_state IN ('STARTING', 'RUNNING', 'STOPPING')`,
      [taskId],
    );
    await client.query(
      `UPDATE ape_dts_platform.task_operations
          SET state = 'BLOCKED_MANUAL',
              execution_stage = 'RECONCILIATION',
              failure_code = $2,
              failure_summary = $3,
              completed_at = statement_timestamp(),
              updated_at = statement_timestamp()
        WHERE task_id = $1::uuid
          AND run_id = $4::uuid
          AND state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')`,
      [taskId, reason.failureCode, reason.failureSummary, runId],
    );
    await this.audit.append(client, {
      action: 'TASK_RUNTIME_RECONCILIATION_BLOCKED',
      actorType: 'SYSTEM',
      actorUserId: null,
      details: {
        runId,
        unitCount: units.rowCount ?? 0,
        reason: reason.failureSummary,
      },
      subjectId: taskId,
      subjectType: 'TASK',
    });
    return true;
  }

  async start(
    actor: UserSummary,
    request: StartTaskRuntimeRequest,
  ): Promise<RuntimeOperationSummary> {
    const releaseStart = this.acquireStartPermit();
    try {
      return await this.startWithPermit(actor, request);
    } finally {
      releaseStart();
    }
  }

  private acquireStartPermit(): () => void {
    if (this.shutdownInProgress) {
      throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
    }
    if (this.activeStarts === 0) {
      this.startDrain = new Promise<void>((resolve) => {
        this.resolveStartDrain = resolve;
      });
    }
    this.activeStarts += 1;
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      this.activeStarts -= 1;
      if (this.activeStarts === 0) {
        this.resolveStartDrain?.();
        this.resolveStartDrain = null;
      }
    };
  }

  private async startWithPermit(
    actor: UserSummary,
    request: StartTaskRuntimeRequest,
  ): Promise<RuntimeOperationSummary> {
    assertIdempotencyKey(request.idempotencyKey);
    let preparation: PreparedStart | ExistingOperation;
    try {
      preparation = await this.prepareStart(actor, request);
    } catch (error) {
      let rejection: unknown = error;
      if (error instanceof CapacityReservationError) {
        rejection = new RuntimeSupervisorError('RUNTIME_CAPACITY_ERROR', {
          capacityCode: error.code,
          ...error.details,
        });
      }
      if (error instanceof MysqlRuntimeStateError)
        rejection = new RuntimeSupervisorError(error.code);
      if (error instanceof DataSourcesServiceError || error instanceof DataSourceAdapterError)
        rejection = new RuntimeSupervisorError('MYSQL_CDC_NOT_READY');
      if (isPostgresConstraintViolation(error, 'task_run_units_one_active_metrics_port')) {
        rejection = new RuntimeSupervisorError('RUNTIME_METRICS_PORT_CONFLICT');
      }
      if (
        rejection instanceof RuntimeSupervisorError &&
        ![
          'RUNTIME_ACCESS_DENIED',
          'RUNTIME_TASK_NOT_FOUND',
          'RUNTIME_IDEMPOTENCY_KEY_CONFLICT',
          'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED',
        ].includes(rejection.code)
      ) {
        await this.recordStartRejection(actor, request, rejection.code);
      }
      throw rejection;
    }
    if (preparation.kind === 'EXISTING') {
      return preparation.operation;
    }
    return await this.launchPreparedStart(preparation);
  }

  private async recordStartRejection(
    actor: UserSummary,
    request: StartTaskRuntimeRequest,
    failureCode: string,
  ): Promise<void> {
    await this.transaction(async (client) => {
      const task = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS} FROM ape_dts_platform.tasks
          WHERE id = $1::uuid AND deleted_at IS NULL FOR UPDATE`,
        [request.taskId],
      );
      const row = task.rows[0];
      if (row === undefined) throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
      await this.assertTaskAccess(client, actor, row);
      await client.query(
        `INSERT INTO ape_dts_platform.task_operations
          (id, task_id, action, idempotency_key, target_revision_id, state,
           initiated_by_user_id, reason, completed_at, failure_code, failure_summary)
         VALUES ($1::uuid, $2::uuid, 'START', $3, NULL, 'REJECTED', $4::uuid,
                 'runtime start rejected before preparation', statement_timestamp(), $5, $6)
         ON CONFLICT (task_id, idempotency_key) DO NOTHING`,
        [
          randomUUID(),
          request.taskId,
          request.idempotencyKey,
          actor.id,
          failureCode,
          `启动前检查未通过（${failureCode}）。`,
        ],
      );
    });
  }

  private async launchPreparedStart(prepared: PreparedStart): Promise<RuntimeOperationSummary> {
    if (this.recoverySettings) await this.recoverySettings.policy();
    const started: Array<{ readonly unit: PreparedUnit; readonly process: ManagedRuntimeProcess }> =
      [];
    const failures: Array<{ readonly unit: PreparedUnit; readonly error: unknown }> = [];
    await Promise.all(
      prepared.units.map(async (unit) => {
        try {
          const process = await this.processes.start({
            ...unit.launch,
            runId: prepared.runId,
            unitId: unit.id,
          });
          started.push({ process, unit });
          if (process.pid === undefined || !Number.isSafeInteger(process.pid) || process.pid <= 0) {
            throw new Error('运行单元启动后未取得有效 PID。');
          }
        } catch (error) {
          failures.push({ error, unit });
        }
      }),
    );
    const handles = new Map(started.map(({ process, unit }) => [unit.id, process]));
    if (failures.length > 0) {
      const primaryFailure = prepared.units
        .map((unit) => failures.find((failure) => failure.unit.id === unit.id))
        .find((failure) => failure !== undefined);
      const retained = await this.finishStartedProcessFailure(
        prepared,
        started,
        primaryFailure?.error,
      );
      if (retained) {
        this.rememberActiveRun(prepared, handles);
      }
      if (!retained) {
        this.processes.finalizeRun(prepared.runId);
        this.forgetActiveRun(prepared.runId);
      }
      return await this.requireOperation(prepared.operationId);
    }

    this.rememberActiveRun(prepared, handles);
    try {
      await this.finishStartSuccess(prepared, started);
      if (prepared.mysql?.position.phase === 'CDC') this.watchMysqlCheckpoints(prepared);
    } catch (error) {
      const retained = await this.finishStartedProcessFailure(prepared, started, error);
      if (!retained) {
        this.processes.finalizeRun(prepared.runId);
        this.forgetActiveRun(prepared.runId);
      }
      return await this.requireOperation(prepared.operationId);
    }
    for (const { process, unit } of started) {
      void process
        .waitForExit()
        .then((result) =>
          this.observeUnexpectedExit(prepared.taskId, prepared.runId, unit.id, result),
        )
        .catch(() =>
          this.logger.error({ code: 'RUNTIME_EXIT_HANDLING_FAILED', runId: prepared.runId }),
        );
    }
    return await this.requireOperation(prepared.operationId);
  }

  async stop(
    actor: UserSummary,
    request: StopTaskRuntimeRequest,
  ): Promise<RuntimeOperationSummary> {
    assertIdempotencyKey(request.idempotencyKey);
    return await this.withTaskTerminalAction(request.taskId, async () =>
      this.stopWithinTerminalAction(actor, request),
    );
  }

  async preflight(context: AutoRecoveryContext, reserveFreshCapacity = false): Promise<void> {
    if (this.preflightService === undefined) {
      throw new Error('RUNTIME_SYSTEM_PREFLIGHT_UNCONFIGURED');
    }
    const result = await this.preflightService.runForSystemRecovery({
      ...(reserveFreshCapacity ? { reserveFreshCapacity: true } : {}),
      expectedOldRunId: context.runId,
      expectedPlatformInstanceId: context.platformInstanceId,
      expectedTaskRowVersion: context.expectedTaskRowVersion,
      revisionId: context.revisionId,
      taskId: context.taskId,
    });
    if (result.state !== 'PASSED') {
      throw new Error(result.failureCode ?? 'RUNTIME_AUTO_RECOVERY_PREFLIGHT_FAILED');
    }
  }

  policy() {
    return (
      this.recoverySettings?.policy() ??
      Promise.resolve({ enabled: true, maxAttempts: 1, retryDelaySeconds: 30 })
    );
  }

  async restartWithRetainedCapacity(context: AutoRecoveryContext): Promise<AutoRecoveryContext> {
    const prepared = await this.prepareSystemRecoveryStart({
      ...context,
      kind: 'AUTO',
      oldProcessToken: context.processToken,
      oldRunId: context.runId,
    });
    const operation = await this.launchPreparedStart(prepared);
    if (operation.state !== 'SUCCEEDED') {
      const reason = operation.failureCode ?? 'RUNTIME_AUTO_RECOVERY_START_FAILED';
      this.logger.error({
        code: 'TASK_AUTO_RECOVERY_START_FAILED',
        taskId: context.taskId,
        runId: prepared.runId,
        reason,
      });
      // Startup has already terminalized the new run. Never retry against the old run binding.
      throw new AutoRecoveryManualRequiredError(reason);
    }
    return {
      ...context,
      expectedTaskRowVersion: prepared.taskRowVersionAfterPrepare,
      processToken: prepared.units[0]!.processToken,
      runId: prepared.runId,
    };
  }

  /** Retry the applied configuration after a confirmed current-instance failure; never reset source positions. */
  async retryFailedTask(actor: UserSummary, taskId: string, expectedRowVersion: string) {
    if (actor.role !== 'ADMIN') throw new RuntimeSupervisorError('RUNTIME_ACCESS_DENIED');
    return this.withTaskTerminalAction(taskId, async () => {
      if (
        this.shutdownInProgress ||
        this.listActiveRuntimeTasks().some((item) => item.taskId === taskId)
      )
        throw new RuntimeSupervisorError('RUNTIME_ALREADY_RUNNING');
      const context = await this.transaction(async (client): Promise<AutoRecoveryContext> => {
        const task = (
          await client.query<TaskRow & { recovery_disposition: string }>(
            `SELECT ${TASK_COLUMNS}, recovery_disposition FROM ape_dts_platform.tasks WHERE id=$1::uuid AND deleted_at IS NULL FOR UPDATE`,
            [taskId],
          )
        ).rows[0];
        if (
          !task ||
          task.row_version !== expectedRowVersion ||
          task.observed_state !== 'BLOCKED' ||
          !['RUNNING', 'STOPPED'].includes(task.desired_state) ||
          task.recovery_disposition !== 'MANUAL_REQUIRED' ||
          !task.current_applied_revision_id
        )
          throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
        const run = (
          await client.query<{
            id: string;
            task_revision_id: string;
            platform_instance_id: string;
            status: string;
          }>(
            'SELECT id,task_revision_id,platform_instance_id,status FROM ape_dts_platform.task_runs WHERE task_id=$1::uuid ORDER BY started_at DESC,id DESC LIMIT 1 FOR UPDATE',
            [taskId],
          )
        ).rows[0];
        const instanceId = await this.requirePlatformInstanceId(client);
        if (
          !run ||
          run.status !== 'FAILED' ||
          run.platform_instance_id !== instanceId ||
          run.task_revision_id !== task.current_applied_revision_id
        )
          throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
        const units = (
          await client.query<{
            process_token: string;
            status: string;
            metrics_port_released_at: Date | null;
          }>(
            'SELECT process_token,status,metrics_port_released_at FROM ape_dts_platform.task_run_units WHERE run_id=$1::uuid ORDER BY unit_key FOR UPDATE',
            [run.id],
          )
        ).rows;
        if (
          !units.length ||
          units.some(
            (unit) =>
              !['FAILED', 'STOPPED'].includes(unit.status) ||
              unit.metrics_port_released_at === null,
          )
        )
          throw new RuntimeSupervisorError('RUNTIME_ALREADY_RUNNING');
        const active = await client.query(
          `SELECT id FROM ape_dts_platform.task_operations WHERE task_id=$1::uuid AND state IN ('ACCEPTED','PREFLIGHTING','PREPARED','EXECUTING')`,
          [taskId],
        );
        if (active.rows.length) throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
        await client.query(
          "UPDATE ape_dts_platform.tasks SET desired_state='RUNNING',observed_state='FAILED',recovery_disposition='AUTO_ELIGIBLE',manual_gate_reason=NULL,row_version=row_version+1,updated_at=statement_timestamp() WHERE id=$1::uuid",
          [taskId],
        );
        await this.audit.append(client, {
          action: 'TASK_RUNTIME_RECOVERY_RETRY_REQUESTED',
          actorType: 'USER',
          actorUserId: actor.id,
          subjectType: 'TASK',
          subjectId: taskId,
          details: { oldRunId: run.id, revisionId: run.task_revision_id },
        });
        return {
          taskId,
          runId: run.id,
          revisionId: run.task_revision_id,
          platformInstanceId: instanceId,
          processToken: units[0]!.process_token,
          expectedTaskRowVersion: (BigInt(expectedRowVersion) + 1n).toString(),
          actorType: 'SYSTEM',
          firstFailureAt: new Date(),
        };
      });
      this.autoRecovery.cancel(taskId);
      let prepared: PreparedStart;
      try {
        try {
          await this.preflight(context, true);
        } catch (error) {
          throw new RuntimeSupervisorError('RUNTIME_RECOVERY_PREFLIGHT_FAILED', {
            reason: recoveryErrorCode(error),
          });
        }
        prepared = await this.prepareSystemRecoveryStart({
          ...context,
          kind: 'AUTO',
          oldRunId: context.runId,
          oldProcessToken: context.processToken,
          freshCapacity: true,
          actorUserId: actor.id,
        });
      } catch (error) {
        await this.requireManualRecovery(context, recoveryErrorCode(error)).catch(() => {
          this.logger.error({ code: 'TASK_MANUAL_RECOVERY_STATE_PERSIST_FAILED', taskId });
        });
        throw error;
      }
      return await this.launchPreparedStart(prepared);
    });
  }

  private async prepareSystemRecoveryStart(
    context: SystemRecoveryStartBinding,
  ): Promise<PreparedStart> {
    await this.revalidateExternalContinuity(
      context.taskId,
      context.revisionId,
      context.expectedTaskRowVersion,
      'RUNNING',
    );
    return await this.transaction(async (client) => {
      const taskResult = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS} FROM ape_dts_platform.tasks
          WHERE id = $1::uuid AND deleted_at IS NULL
            AND desired_state = 'RUNNING'
            AND observed_state = $4
            AND recovery_disposition = 'AUTO_ELIGIBLE'
            AND row_version = $2::bigint AND current_applied_revision_id = $3::uuid
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_operations AS active_operation
               WHERE active_operation.task_id = tasks.id
                 AND active_operation.state IN ('ACCEPTED','PREFLIGHTING','PREPARED','EXECUTING')
            )
          FOR UPDATE`,
        [
          context.taskId,
          context.expectedTaskRowVersion,
          context.revisionId,
          context.kind === 'AUTO' ? 'FAILED' : 'BLOCKED',
        ],
      );
      if (taskResult.rows[0] === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const oldRun = await client.query<{ id: string }>(
        `SELECT run.id FROM ape_dts_platform.task_runs AS run
          WHERE run.id = $1::uuid AND run.task_id = $2::uuid
            AND run.task_revision_id = $3::uuid AND run.platform_instance_id = $4::uuid
            AND run.status = $5
            AND ($6::text IS NULL OR run.current_stage = $6)
            AND ($7::text IS NULL OR EXISTS (
              SELECT 1 FROM ape_dts_platform.task_run_units AS unit
               WHERE unit.run_id = run.id AND unit.process_token = $7::text))
            AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.task_runs AS newer
                             WHERE newer.task_id = run.task_id
                               AND (newer.started_at, newer.id) > (run.started_at, run.id))
          FOR UPDATE`,
        [
          context.oldRunId,
          context.taskId,
          context.revisionId,
          context.platformInstanceId,
          context.kind === 'AUTO' ? 'FAILED' : 'BLOCKED',
          context.kind === 'CONTAINER' ? 'RECONCILIATION' : null,
          context.oldProcessToken ?? null,
        ],
      );
      if (oldRun.rows[0] === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const revisionResult = await client.query<RevisionRow>(
        `SELECT id, task_id, state, configuration, artifact_id, draft_version::text
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid AND task_id = $2::uuid AND state = 'APPLIED' FOR SHARE`,
        [context.revisionId, context.taskId],
      );
      const revision = revisionResult.rows[0];
      if (revision === undefined) throw new RuntimeSupervisorError('RUNTIME_REVISION_INVALID');
      const configuration = parseRevisionConfiguration(revision.configuration);
      const oldUnits = await client.query<{ metrics_port: number; unit_key: string }>(
        `SELECT unit_key, metrics_port FROM ape_dts_platform.task_run_units
          WHERE run_id = $1::uuid AND task_id = $2::uuid
            AND ($3::boolean = false OR metrics_port_released_at IS NOT NULL)
          ORDER BY unit_key FOR SHARE`,
        [context.oldRunId, context.taskId, context.kind === 'AUTO'],
      );
      if (oldUnits.rows.length === 0)
        throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
      if (
        context.unitKeys !== undefined &&
        (context.unitKeys.length !== oldUnits.rows.length ||
          context.unitKeys.some((key, index) => key !== oldUnits.rows[index]?.unit_key))
      ) {
        throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
      }
      const operationId = randomUUID();
      const runId = randomUUID();
      const platformInstanceId =
        context.kind === 'CONTAINER'
          ? await this.requirePlatformInstanceId(client)
          : context.platformInstanceId;
      await client.query(
        `INSERT INTO ape_dts_platform.task_operations
           (id, task_id, action, idempotency_key, target_revision_id, state,
            initiated_by_user_id, reason, execution_stage, started_at)
         VALUES ($1::uuid, $2::uuid, 'START', $3, $4::uuid, 'PREPARED', $5::uuid,
                 'system auto recovery', 'STARTING_BARRIER', statement_timestamp())`,
        [
          operationId,
          context.taskId,
          `${context.kind === 'AUTO' ? 'system-auto-recovery' : 'container-recovery'}:${context.oldRunId}`,
          context.revisionId,
          context.actorUserId ?? null,
        ],
      );
      await client.query(
        `INSERT INTO ape_dts_platform.task_runs
           (id, task_id, task_revision_id, operation_id, artifact_id, platform_instance_id,
            status, current_stage, started_at)
         VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,$6::uuid,
                 'STARTING','STARTING_BARRIER',statement_timestamp())`,
        [
          runId,
          context.taskId,
          context.revisionId,
          operationId,
          revision.artifact_id,
          platformInstanceId,
        ],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations SET run_id=$2::uuid, state='EXECUTING',
                updated_at=statement_timestamp()
          WHERE id=$1::uuid AND task_id=$3::uuid AND state='PREPARED'`,
        [operationId, runId, context.taskId],
      );
      const mysql =
        configuration.kind === 'MYSQL_TO_KAFKA'
          ? {
              config: configuration,
              position: await this.requireMysqlState().prepareWithin(client, {
                taskId: context.taskId,
                revisionId: context.revisionId,
                runId,
                config: configuration,
                recovery: true,
                actorUserId: null,
              }),
            }
          : undefined;
      const units: PreparedUnit[] = [];
      if (context.kind === 'CONTAINER') {
        const releasedPorts = await client.query(
          `UPDATE ape_dts_platform.task_run_units
              SET metrics_port_released_at = statement_timestamp()
            WHERE run_id = $1::uuid AND task_id = $2::uuid
              AND status = 'BLOCKED' AND metrics_port_released_at IS NULL`,
          [context.oldRunId, context.taskId],
        );
        if ((releasedPorts.rowCount ?? 0) !== oldUnits.rows.length) {
          throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
        }
      }
      const allocatedPorts =
        context.kind === 'AUTO'
          ? await this.metricsPorts?.allocate(client, oldUnits.rows.length)
          : undefined;
      if (context.kind === 'AUTO' && allocatedPorts === undefined)
        throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
      for (const [index, oldUnit] of oldUnits.rows.entries()) {
        const metricsPort = allocatedPorts?.[index] ?? oldUnit.metrics_port;
        const id = randomUUID();
        const processToken = randomUUID();
        const launch = await this.launchFactory.create({
          config: configuration,
          ...(mysql === undefined ? {} : { mysql: mysql.position }),
          metricsPort,
          revisionId: context.revisionId,
          runId,
          taskId: context.taskId,
          unitId: id,
          unitKey: oldUnit.unit_key,
        });
        units.push({
          id,
          launch,
          processToken,
          unitKey: oldUnit.unit_key,
          metricsPort,
        });
        await client.query(
          `INSERT INTO ape_dts_platform.task_run_units
             (id,task_id,run_id,unit_key,status,runtime_directory,ini_path,log4rs_path,
              metrics_port,process_token,started_at)
           VALUES ($1::uuid,$2::uuid,$3::uuid,$4,'STARTING',$5,$6,$7,$8,$9,NULL)`,
          [
            id,
            context.taskId,
            runId,
            oldUnit.unit_key,
            launch.runtimeDirectory,
            launch.iniPath,
            launch.log4rsPath,
            metricsPort,
            processToken,
          ],
        );
      }
      if (context.freshCapacity) {
        await this.capacity.reserveWithin(
          client,
          context.taskId,
          context.revisionId,
          units.map((unit) => unit.unitKey),
          runId,
        );
      } else if (context.kind === 'AUTO') {
        try {
          await this.capacity.transferWithin(
            client,
            {
              expectedTaskRowVersion: context.expectedTaskRowVersion,
              newRunId: runId,
              oldProcessToken: context.oldProcessToken!,
              oldRunId: context.oldRunId,
              platformInstanceId: context.platformInstanceId,
              taskId: context.taskId,
              taskRevisionId: context.revisionId,
            },
            units.length,
          );
        } catch (error) {
          this.logger.error({
            code: 'TASK_RECOVERY_CAPACITY_TRANSFER_FAILED',
            taskId: context.taskId,
            oldRunId: context.oldRunId,
            newRunId: runId,
            reason: recoveryErrorCode(error),
            requested: units.length,
            transferred:
              error instanceof CapacityReservationError ? error.details?.used : undefined,
          });
          throw error;
        }
      } else {
        const transferred = await client.query(
          `UPDATE ape_dts_platform.capacity_reservations AS reservation
              SET run_id = $2::uuid
            WHERE reservation.run_id = $1::uuid
              AND reservation.task_id = $3::uuid
              AND reservation.task_revision_id = $4::uuid
              AND reservation.state IN ('RESERVED','ACTIVE')
              AND EXISTS (SELECT 1 FROM ape_dts_platform.tasks AS task
                           WHERE task.id = reservation.task_id AND task.deleted_at IS NULL
                             AND task.desired_state = 'RUNNING'
                             AND task.recovery_disposition = 'AUTO_ELIGIBLE'
                             AND task.row_version = $6::bigint
                             AND task.current_applied_revision_id = reservation.task_revision_id)
              AND EXISTS (SELECT 1 FROM ape_dts_platform.task_runs AS old_run
                           WHERE old_run.id = reservation.run_id
                             AND old_run.platform_instance_id = $5::uuid
                             AND old_run.status = 'BLOCKED'
                             AND old_run.current_stage = 'RECONCILIATION')
              AND EXISTS (SELECT 1 FROM ape_dts_platform.task_runs AS new_run
                           WHERE new_run.id = $2::uuid AND new_run.task_id = reservation.task_id
                             AND new_run.task_revision_id = reservation.task_revision_id
                             AND new_run.status = 'STARTING'
                             AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.task_runs AS newer
                                              WHERE newer.task_id = new_run.task_id
                                                AND (newer.started_at, newer.id) >
                                                    (new_run.started_at, new_run.id)))`,
          [
            context.oldRunId,
            runId,
            context.taskId,
            context.revisionId,
            context.platformInstanceId,
            context.expectedTaskRowVersion,
          ],
        );
        if ((transferred.rowCount ?? 0) !== units.length) {
          throw new CapacityReservationError('CAPACITY_ALREADY_RESERVED');
        }
      }
      const stageTypes = runtimeStageTypes(configuration, mysql?.position.phase);
      await client.query(
        `INSERT INTO ape_dts_platform.task_run_stages
           (id, run_id, stage_order, stage_type, status, progress_total,
            started_at, ended_at, heartbeat_at)
         SELECT stage.id, $1::uuid, stage.stage_order, stage.stage_type, stage.status,
                NULL,
                CASE WHEN stage.status = 'PENDING' THEN NULL ELSE statement_timestamp() END,
                CASE WHEN stage.status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END,
                CASE WHEN stage.status = 'PENDING' THEN NULL ELSE statement_timestamp() END
           FROM unnest($2::uuid[], $3::integer[], $4::text[], $5::text[])
             AS stage(id, stage_order, stage_type, status)`,
        [
          runId,
          stageTypes.map(() => randomUUID()),
          stageTypes.map((_, index) => index + 1),
          stageTypes,
          stageTypes.map((_, index) =>
            index < 4 ? 'SUCCEEDED' : index === 4 ? 'RUNNING' : 'PENDING',
          ),
        ],
      );
      const updated = await client.query(
        `UPDATE ape_dts_platform.tasks SET observed_state='STARTING', row_version=row_version+1,
                updated_at=statement_timestamp()
          WHERE id=$1::uuid AND deleted_at IS NULL AND desired_state='RUNNING'
            AND observed_state=$4 AND recovery_disposition='AUTO_ELIGIBLE'
            AND row_version=$2::bigint
            AND current_applied_revision_id=$3::uuid`,
        [
          context.taskId,
          context.expectedTaskRowVersion,
          context.revisionId,
          context.kind === 'AUTO' ? 'FAILED' : 'BLOCKED',
        ],
      );
      if ((updated.rowCount ?? 0) !== 1)
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      await this.audit.append(client, {
        action: context.actorUserId
          ? 'TASK_RUNTIME_RECOVERY_START_ACCEPTED'
          : 'TASK_RUNTIME_AUTO_RECOVERY_START_ACCEPTED',
        actorType: context.actorUserId ? 'USER' : 'SYSTEM',
        actorUserId: context.actorUserId ?? null,
        details: { newRunId: runId, oldRunId: context.oldRunId, operationId },
        subjectId: context.taskId,
        subjectType: 'TASK',
      });
      return {
        actorUserId: context.actorUserId ?? null,
        kind: 'PREPARED',
        operation: await this.requireOperationWithin(client, operationId),
        operationId,
        platformInstanceId,
        revisionId: context.revisionId,
        runtimeStage:
          (mysql?.position.phase ?? scenarioForConfiguration(configuration)) === 'SNAPSHOT'
            ? 'SNAPSHOT'
            : 'CDC_STARTED',
        runId,
        taskId: context.taskId,
        taskRowVersionAfterPrepare: (BigInt(context.expectedTaskRowVersion) + 1n).toString(),
        systemRecovery: true,
        units,
        ...(mysql === undefined ? {} : { mysql }),
      };
    });
  }

  async requireManualRecovery(context: AutoRecoveryContext, reason: string): Promise<void> {
    await this.transaction(async (client) => {
      const task = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND desired_state = 'RUNNING'
            AND row_version = $2::bigint
            AND current_applied_revision_id = $3::uuid
          FOR UPDATE`,
        [context.taskId, context.expectedTaskRowVersion, context.revisionId],
      );
      if (task.rows[0] === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const run = await client.query<{ id: string }>(
        `SELECT run.id
           FROM ape_dts_platform.task_runs AS run
          WHERE run.id = $1::uuid
            AND run.task_id = $2::uuid
            AND run.task_revision_id = $3::uuid
            AND run.platform_instance_id = $4::uuid
            AND run.status = 'FAILED'
            AND EXISTS (
              SELECT 1 FROM ape_dts_platform.task_run_units AS unit
               WHERE unit.run_id = run.id AND unit.process_token = $5::text
            )
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_runs AS newer
               WHERE newer.task_id = run.task_id
                 AND (newer.started_at, newer.id) > (run.started_at, run.id)
            )
          FOR UPDATE`,
        [
          context.runId,
          context.taskId,
          context.revisionId,
          context.platformInstanceId,
          context.processToken,
        ],
      );
      if (run.rows[0] === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const released = await this.capacity.releaseWithin(client, context.runId);
      if (released <= 0) {
        const remaining = await client.query<{ count: string }>(
          `SELECT count(*)::text AS count
             FROM ape_dts_platform.capacity_reservations
            WHERE run_id = $1::uuid
              AND task_id = $2::uuid
              AND task_revision_id = $3::uuid
              AND state IN ('RESERVED','ACTIVE')`,
          [context.runId, context.taskId, context.revisionId],
        );
        if (remaining.rows[0]?.count !== '0') {
          throw new CapacityReservationError('CAPACITY_RUN_NOT_FOUND');
        }
      }
      const updated = await client.query(
        `UPDATE ape_dts_platform.tasks
            SET desired_state = 'STOPPED', observed_state = 'BLOCKED',
                recovery_disposition = 'MANUAL_REQUIRED', manual_gate_reason = $4, row_version = row_version + 1,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND desired_state = 'RUNNING'
            AND row_version = $2::bigint
            AND current_applied_revision_id = $3::uuid`,
        [context.taskId, context.expectedTaskRowVersion, context.revisionId, reason],
      );
      if ((updated.rowCount ?? 0) !== 1) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_AUTO_RECOVERY_MANUAL_REQUIRED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: { oldRunId: context.runId, reason },
        subjectId: context.taskId,
        subjectType: 'TASK',
      });
    });
  }

  async warn(
    context: AutoRecoveryContext,
    event: 'FIRST_EXIT' | 'AUTO_RECOVERY_ATTEMPT',
  ): Promise<void> {
    await this.transaction(async (client) => {
      await this.audit.append(client, {
        action:
          event === 'FIRST_EXIT'
            ? 'TASK_RUNTIME_AUTO_RECOVERY_SCHEDULED'
            : 'TASK_RUNTIME_AUTO_RECOVERY_ATTEMPTED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: { oldRunId: context.runId },
        subjectId: context.taskId,
        subjectType: 'TASK',
      });
    });
  }

  async verifyRecoveryArtifact(binding: {
    readonly revisionId: string;
    readonly taskId: string;
  }): Promise<void> {
    const pool = this.database.requirePool();
    const result = await pool.query<{ artifact_id: string; configuration: unknown }>(
      `SELECT artifact.id AS artifact_id, revision.configuration
         FROM ape_dts_platform.tasks AS task
         JOIN ape_dts_platform.task_revisions AS revision
           ON revision.id = task.current_applied_revision_id AND revision.task_id = task.id
         JOIN ape_dts_platform.artifacts AS artifact ON artifact.id = revision.artifact_id
         JOIN ape_dts_platform.stored_objects AS object ON object.id = artifact.stored_object_id
        WHERE task.id = $1::uuid AND task.deleted_at IS NULL
          AND task.current_applied_revision_id = $2::uuid
          AND revision.state = 'APPLIED'
          AND artifact.state <> 'QUARANTINED'
          AND artifact.deleted_at IS NULL
          AND object.state = 'AVAILABLE'
          AND artifact.actual_sha256 = artifact.declared_sha256
          AND object.sha256 = artifact.declared_sha256`,
      [binding.taskId, binding.revisionId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new RuntimeSupervisorError('RUNTIME_ARTIFACT_BINDING_INVALID');
    }
    try {
      await this.launchFactory.verifyArtifact(binding);
    } catch (error) {
      if (error instanceof AuthenticatedRuntimeLaunchSpecError) {
        throw new RuntimeSupervisorError(error.code);
      }
      if (error instanceof RuntimeLaunchSpecError) {
        throw new RuntimeSupervisorError(error.code);
      }
      throw error;
    }
  }

  async recoverPersistedTask(binding: {
    readonly expectedTaskRowVersion: string;
    readonly oldRunId: string;
    readonly platformInstanceId: string;
    readonly revisionId: string;
    readonly taskId: string;
    readonly unitKeys: readonly string[];
  }): Promise<{ readonly runId: string }> {
    const release = this.acquireStartPermit();
    try {
      const prepared = await this.prepareSystemRecoveryStart({ ...binding, kind: 'CONTAINER' });
      const operation = await this.launchPreparedStart(prepared);
      if (operation.state !== 'SUCCEEDED') {
        throw new Error(operation.failureCode ?? 'RECOVERY_RUNTIME_START_FAILED');
      }
      return { runId: prepared.runId };
    } finally {
      release();
    }
  }

  async recoveryObservability(taskIds: readonly string[]): Promise<
    readonly {
      readonly logsReady: boolean;
      readonly metricsReady: boolean;
      readonly runId: string;
      readonly taskId: string;
    }[]
  > {
    if (taskIds.length === 0) return [];
    const result = await this.database.requirePool().query<{
      log_storage_state: string;
      run_id: string;
      task_id: string;
      unit_count: string;
    }>(
      `SELECT task.id AS task_id, run.id AS run_id, run.log_storage_state,
              count(unit.id)::text AS unit_count
         FROM ape_dts_platform.tasks AS task
         JOIN LATERAL (
           SELECT candidate.* FROM ape_dts_platform.task_runs AS candidate
            WHERE candidate.task_id = task.id AND candidate.status = 'RUNNING'
            ORDER BY candidate.started_at DESC, candidate.id DESC LIMIT 1
         ) AS run ON true
         JOIN ape_dts_platform.task_run_units AS unit
           ON unit.run_id = run.id AND unit.status = 'RUNNING'
          AND unit.process_pid IS NOT NULL AND unit.metrics_port_released_at IS NULL
        WHERE task.id = ANY($1::uuid[]) AND task.deleted_at IS NULL
          AND task.desired_state = 'RUNNING' AND task.observed_state = 'RUNNING'
        GROUP BY task.id, run.id, run.log_storage_state`,
      [[...taskIds]],
    );
    return result.rows.map((row) => {
      const active = this.activeProcesses.get(row.run_id);
      const expected = Number(row.unit_count);
      const metricsReady = active !== undefined && active.size === expected && expected > 0;
      return {
        logsReady: metricsReady && row.log_storage_state !== 'UNKNOWN',
        metricsReady,
        runId: row.run_id,
        taskId: row.task_id,
      };
    });
  }

  private async stopWithinTerminalAction(
    actor: UserSummary,
    request: StopTaskRuntimeRequest,
  ): Promise<RuntimeOperationSummary> {
    const preparation = await this.prepareStop(actor, request);
    if (preparation.kind === 'EXISTING') {
      return preparation.operation;
    }
    const prepared = preparation;
    this.autoRecovery.cancel(prepared.taskId);
    const processes = this.activeProcesses.get(prepared.runId);
    if (processes === undefined || processes.size !== prepared.unitIds.length) {
      await this.finishStopBlocked(prepared);
      return await this.requireOperation(prepared.operationId);
    }
    const stopped = await Promise.all(
      prepared.unitIds.map(async (unitId) => {
        const process = processes.get(unitId);
        if (process === undefined) {
          return null;
        }
        return await process.stop();
      }),
    );
    const results = stopped.filter((result): result is RuntimeProcessExit => result !== null);
    if (results.some((result) => !result.confirmed || result.terminationTimedOut)) {
      await this.finishStopBlocked(prepared);
      return await this.requireOperation(prepared.operationId);
    }
    const logState = await this.settleRunLogState(prepared.runId, results);
    const configurationCleanupFailed = hasConfigurationCleanupFailure(results);
    if (!configurationCleanupFailed) {
      await this.finishStopSuccess(prepared, results, logState);
    } else {
      await this.finishStopCleanupBlocked(prepared, results, logState, {
        code: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
        releaseCapacity: true,
        summary: CONFIGURATION_CLEANUP_FAILURE_SUMMARY,
      });
    }
    this.processes.finalizeRun(prepared.runId);
    this.forgetActiveRun(prepared.runId);
    return await this.requireOperation(prepared.operationId);
  }

  private async assertStartPreconditions(
    client: PoolClient,
    task: TaskRow,
    revision: RevisionRow,
    config: PlatformTaskConfig,
    preflight: PreflightRow,
    request: PlannedStartTaskRuntimeRequest,
  ): Promise<void> {
    if (preflight.completed_at === null) {
      throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE');
    }
    const parsed = taskPreflightResultSchema.safeParse(preflight.result);
    if (!parsed.success) {
      throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
        reason: 'PREFLIGHT_RESULT_INVALID',
      });
    }
    const result = parsed.data;
    if (
      result.taskRevision.taskRowVersion !== request.expectedTaskRowVersion ||
      result.taskRevision.draftVersion !== revision.draft_version
    ) {
      throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
        reason: 'TASK_OR_REVISION_VERSION_CHANGED',
      });
    }
    assertPassedPreflightChecks(result, config);
    if (result.capacity.requested !== request.units.length) {
      throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID', {
        preflightRequested: result.capacity.requested,
        requested: request.units.length,
      });
    }
    assertKafkaPartitionUnits(result, config, request.units);

    const sourceIds = [task.source_data_source_id, task.target_data_source_id];
    const dataSources = await client.query<DataSourceRow>(
      `SELECT id, row_version::text, state, deleted_at, configuration
         FROM ape_dts_platform.data_sources
        WHERE id = ANY($1::uuid[])
        FOR SHARE`,
      [sourceIds],
    );
    const dataSourcesById = new Map(dataSources.rows.map((row) => [row.id, row]));
    const source = dataSourcesById.get(task.source_data_source_id);
    const target = dataSourcesById.get(task.target_data_source_id);
    if (
      source === undefined ||
      target === undefined ||
      source.state !== 'ACTIVE' ||
      target.state !== 'ACTIVE' ||
      source.deleted_at !== null ||
      target.deleted_at !== null ||
      source.row_version !== result.dataSourceVersions.source ||
      target.row_version !== result.dataSourceVersions.target
    ) {
      throw new RuntimeSupervisorError('RUNTIME_DATA_SOURCE_VERSION_CONFLICT', {
        expectedSource: result.dataSourceVersions.source,
        expectedTarget: result.dataSourceVersions.target,
        actualSource: source?.row_version ?? null,
        actualTarget: target?.row_version ?? null,
      });
    }
    const identityObservations = await client.query<IdentityObservationRow>(
      `SELECT DISTINCT ON (test.data_source_id)
              test.data_source_id, observation.identity_kind, observation.identity_value
         FROM ape_dts_platform.connection_test_runs AS test
         JOIN ape_dts_platform.data_source_identity_observations AS observation
           ON observation.connection_test_run_id = test.id
        WHERE test.data_source_id = ANY($1::uuid[])
          AND test.state = 'SUCCEEDED'
        ORDER BY test.data_source_id, observation.observed_at DESC`,
      [sourceIds],
    );
    this.assertDataSourceIdentities(result, config, dataSources.rows, identityObservations.rows);

    if (revision.artifact_id === null) {
      throw new RuntimeSupervisorError('RUNTIME_ARTIFACT_BINDING_INVALID', {
        reason: 'ARTIFACT_NOT_BOUND',
      });
    }
    const scenario = scenarioForConfiguration(config);
    const artifactResult = await client.query<ArtifactBindingRow>(
      `SELECT encode(a.declared_sha256, 'hex') AS declared_sha256,
              a.state AS artifact_state,
              encode(a.actual_sha256, 'hex') AS actual_sha256,
              so.state AS stored_object_state,
              encode(so.sha256, 'hex') AS stored_object_sha256
         FROM ape_dts_platform.artifacts AS a
         JOIN ape_dts_platform.stored_objects AS so
           ON so.id = a.stored_object_id
        WHERE a.id = $1::uuid AND a.deleted_at IS NULL FOR SHARE OF a`,
      [revision.artifact_id],
    );
    const artifact = artifactResult.rows[0];
    if (
      artifact === undefined ||
      artifact.artifact_state === 'QUARANTINED' ||
      artifact.stored_object_state !== 'AVAILABLE' ||
      artifact.declared_sha256 === null ||
      artifact.actual_sha256 === null ||
      artifact.declared_sha256 !== artifact.actual_sha256 ||
      artifact.actual_sha256 !== artifact.stored_object_sha256
    ) {
      throw new RuntimeSupervisorError('RUNTIME_ARTIFACT_NOT_ACTIVE');
    }
    const artifactCheck = preflightCheck(result, 'ARTIFACT')[0];
    const adapterId = objectString(artifactCheck?.details, 'adapterId');
    const adapterVersion = objectString(artifactCheck?.details, 'adapterVersion');
    if (adapterId !== 'ape-dts-ini' || adapterVersion !== '1') {
      throw new RuntimeSupervisorError('RUNTIME_ARTIFACT_BINDING_INVALID', {
        scenario,
      });
    }

    const resources = await client.query<ExternalResourceRow>(
      `SELECT data_source_id, resource_type, normalized_name, database_name,
              ownership_mode, stable_identity, lifecycle_state
         FROM ape_dts_platform.external_resources
        WHERE task_id = $1::uuid
          AND lifecycle_state = ANY($2::text[])
        FOR SHARE`,
      [task.id, PREFLIGHT_RESOURCE_STATES],
    );
    this.assertExternalResourceIdentities(result, config, resources.rows, source);
  }

  private assertDataSourceIdentities(
    result: TaskPreflightResult,
    config: PlatformTaskConfig,
    rows: readonly DataSourceRow[],
    observations: readonly IdentityObservationRow[],
  ): void {
    const source = rows.find((row) => row.id === config.source.dataSourceId);
    const target = rows.find((row) => row.id === config.target.dataSourceId);
    const sourceType = objectString(source?.configuration, 'type');
    const targetType = objectString(target?.configuration, 'type');
    const sourceObservation = observations.find((row) => row.data_source_id === source?.id);
    const targetObservation = observations.find((row) => row.data_source_id === target?.id);
    const expectedSourceKind =
      sourceType === 'POSTGRES'
        ? 'POSTGRES_DATABASE'
        : sourceType === 'MYSQL'
          ? 'MYSQL_DATABASE'
          : 'KAFKA_CLUSTER';
    if (
      sourceType === null ||
      targetType === null ||
      targetType !== 'KAFKA' ||
      (config.kind === 'PG_TO_KAFKA' && sourceType !== 'POSTGRES') ||
      (config.kind === 'MYSQL_TO_KAFKA' && sourceType !== 'MYSQL') ||
      (config.kind === 'KAFKA_RAW_FORWARD' && sourceType !== 'KAFKA') ||
      result.identities.source === null ||
      result.identities.target === null ||
      sourceObservation?.identity_kind !== expectedSourceKind ||
      sourceObservation?.identity_value !== result.identities.source ||
      targetObservation?.identity_kind !== 'KAFKA_CLUSTER' ||
      targetObservation?.identity_value !== result.identities.target
    ) {
      throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
        reason: 'DATA_SOURCE_IDENTITY_CHANGED',
      });
    }
  }

  private assertExternalResourceIdentities(
    result: TaskPreflightResult,
    config: PlatformTaskConfig,
    rows: readonly ExternalResourceRow[],
    sourceDataSource: DataSourceRow,
  ): void {
    const expected: Array<{
      readonly dataSourceId: string;
      readonly name: string;
      readonly ownershipMode?: string;
      readonly resourceType: string;
      readonly stableIdentity: string | null;
    }> = [];
    if (config.kind === 'PG_TO_KAFKA') {
      const pgCheck = preflightCheck(result, 'PG_RESOURCES')[0];
      const snapshots =
        typeof pgCheck?.details === 'object' && pgCheck.details !== null
          ? (pgCheck.details as Record<string, unknown>).resources
          : null;
      if (!Array.isArray(snapshots)) {
        throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
          reason: 'PG_RESOURCE_SNAPSHOT_MISSING',
        });
      }
      for (const item of snapshots) {
        if (typeof item !== 'object' || item === null) {
          throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
        }
        const resource = item as Record<string, unknown>;
        const resourceType = objectString(resource, 'resourceType');
        const normalizedName = objectString(resource, 'normalizedName');
        const stableIdentity = objectStringOrNull(resource, 'stableIdentity');
        const ownershipMode = objectStringOrNull(resource, 'ownershipMode');
        if (
          resourceType === null ||
          normalizedName === null ||
          stableIdentity === null ||
          ownershipMode === null
        ) {
          throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
        }
        expected.push({
          dataSourceId: config.source.dataSourceId,
          name: normalizedName,
          ownershipMode,
          resourceType,
          stableIdentity,
        });
      }
      const sourceDatabase = objectString(sourceDataSource.configuration, 'database');
      if (sourceDatabase === null) {
        throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH');
      }
      for (const item of expected) {
        const matches = rows.filter(
          (row) =>
            row.data_source_id === item.dataSourceId &&
            row.resource_type === item.resourceType &&
            row.normalized_name === item.name,
        );
        if (
          matches.length !== 1 ||
          matches[0]?.stable_identity !== item.stableIdentity ||
          matches[0]?.database_name !== sourceDatabase ||
          matches[0]?.ownership_mode !== item.ownershipMode
        ) {
          throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
            resourceType: item.resourceType,
          });
        }
      }
      expected.length = 0;
    }

    if (result.identities.source === null || result.identities.target === null) {
      throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
        reason: 'KAFKA_IDENTITY_MISSING',
      });
    }
    const topicCheck = preflightCheck(result, 'TOPIC_IDENTITIES')[0];
    const snapshots = Array.isArray(topicCheck?.details.topics) ? topicCheck.details.topics : [];
    const requiredTopics = [
      ...(config.kind === 'KAFKA_RAW_FORWARD'
        ? kafkaTaskMappings(config).map((item) => ({
            dataSourceId: config.source.dataSourceId,
            name: item.source,
            resourceType: 'KAFKA_SOURCE_TOPIC',
          }))
        : []),
      ...taskTargetTopics(config).map((item) => ({
        dataSourceId: config.target.dataSourceId,
        name: item.topic,
        resourceType: 'KAFKA_TARGET_TOPIC',
      })),
    ];
    for (const topic of requiredTopics) {
      const matching = snapshots.filter(
        (value) =>
          typeof value === 'object' &&
          value !== null &&
          (value as Record<string, unknown>).resourceType === topic.resourceType &&
          (value as Record<string, unknown>).topic === topic.name,
      );
      const snapshot = matching[0] as Record<string, unknown> | undefined;
      if (
        config.schemaVersion === 2 &&
        (matching.length !== 1 ||
          snapshot === undefined ||
          typeof snapshot.clusterId !== 'string' ||
          !(snapshot.topicId === null || typeof snapshot.topicId === 'string'))
      )
        throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
          reason: 'KAFKA_TOPIC_BASELINE_MISSING',
          topic: topic.name,
        });
      const topicId =
        config.schemaVersion === 1
          ? topic.resourceType === 'KAFKA_SOURCE_TOPIC'
            ? result.identities.sourceTopic
            : result.identities.targetTopic
          : snapshot?.topicId;
      const clusterId =
        topic.resourceType === 'KAFKA_SOURCE_TOPIC'
          ? result.identities.source
          : result.identities.target;
      if (snapshot !== undefined && snapshot.clusterId !== clusterId)
        throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
          reason: 'KAFKA_CLUSTER_BASELINE_MISMATCH',
        });
      expected.push({
        ...topic,
        ownershipMode: 'SELF_MANAGED',
        stableIdentity: topicId == null ? null : `${clusterId}:${String(topicId)}`,
      });
    }
    for (const item of expected) {
      // Older Kafka has no Topic ID; preflight does not persist a fabricated identity either.
      if (item.stableIdentity === null) continue;
      const matches = rows.filter(
        (row) =>
          row.data_source_id === item.dataSourceId &&
          row.resource_type === item.resourceType &&
          row.normalized_name === item.name,
      );
      if (
        matches.length !== 1 ||
        matches[0]?.stable_identity !== item.stableIdentity ||
        matches[0]?.ownership_mode !== item.ownershipMode
      ) {
        throw new RuntimeSupervisorError('RUNTIME_EXTERNAL_IDENTITY_MISMATCH', {
          resourceType: item.resourceType,
          resourceName: item.name,
        });
      }
    }
  }

  private async prepareStart(
    actor: UserSummary,
    request: StartTaskRuntimeRequest,
  ): Promise<PreparedStart | ExistingOperation> {
    const existing = await this.transaction(async (client) => {
      const task = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS} FROM ape_dts_platform.tasks WHERE id=$1::uuid FOR UPDATE`,
        [request.taskId],
      );
      const row = task.rows[0];
      if (row === undefined || row.deleted_at !== null)
        throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
      await this.assertTaskAccess(client, actor, row);
      const operation = await this.findOperationWithin(
        client,
        request.taskId,
        request.idempotencyKey,
      );
      if (operation !== null && operation.action !== 'START')
        throw new RuntimeSupervisorError('RUNTIME_IDEMPOTENCY_KEY_CONFLICT');
      return operation;
    });
    if (existing !== null) return { kind: 'EXISTING', operation: existing };
    let topicPrepared: boolean;
    try {
      topicPrepared =
        (await this.preflightService?.prepareFirstStartTopic?.(actor, request.taskId, {
          revisionId: request.revisionId,
          expectedTaskRowVersion: request.expectedTaskRowVersion,
        })) ?? false;
    } catch (error) {
      if (error instanceof PreflightServiceError) {
        const code =
          error.code === 'PREFLIGHT_ACCESS_DENIED'
            ? 'RUNTIME_ACCESS_DENIED'
            : error.code === 'PREFLIGHT_TASK_NOT_FOUND'
              ? 'RUNTIME_TASK_NOT_FOUND'
              : error.code === 'PREFLIGHT_REVISION_NOT_CURRENT'
                ? 'RUNTIME_REVISION_NOT_CURRENT'
                : 'RUNTIME_TASK_VERSION_CONFLICT';
        throw new RuntimeSupervisorError(code);
      }
      // External creation may have succeeded before its audit or subsequent
      // preflight failed. Never start or delete/recreate the Topic on this path.
      throw new RuntimeSupervisorError('RUNTIME_TOPIC_PREPARATION_FAILED', {
        requiresMetadataRefresh: true,
        ...(error instanceof KafkaTopicAuditError
          ? {
              reason: error.code,
              topic: error.result.topic,
              created: error.result.created,
              completedTopics: error.completedResults,
            }
          : {}),
      });
    }
    if (topicPrepared) throw new RuntimeSupervisorError('RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED');
    await this.revalidateExternalContinuity(
      request.taskId,
      request.revisionId,
      request.expectedTaskRowVersion,
      'STOPPED',
      request.reinitializeMysqlSnapshot,
    );
    return await this.transaction(async (client) => {
      const taskResult = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS}
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
          FOR UPDATE`,
        [request.taskId],
      );
      const task = taskResult.rows[0];
      if (task === undefined || task.deleted_at !== null) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
      }
      // Locking the task before reading the idempotency row serializes concurrent
      // requests for the same task.  A second request therefore observes the
      // committed operation instead of failing with a stale row-version error.
      await this.assertTaskAccess(client, actor, task);
      const existing = await this.findOperationWithin(
        client,
        request.taskId,
        request.idempotencyKey,
      );
      if (existing !== null) {
        if (existing.action !== 'START') {
          throw new RuntimeSupervisorError('RUNTIME_IDEMPOTENCY_KEY_CONFLICT');
        }
        return { kind: 'EXISTING', operation: existing };
      }
      if (task.row_version !== request.expectedTaskRowVersion) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      if (task.desired_state === 'DELETED' || task.observed_state === 'DELETED') {
        throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
      }
      if (
        task.desired_state === 'RUNNING' ||
        ['STARTING', 'RUNNING', 'STOPPING'].includes(task.observed_state)
      ) {
        throw new RuntimeSupervisorError('RUNTIME_ALREADY_RUNNING');
      }
      const revisionResult = await client.query<RevisionRow>(
        `SELECT id, task_id, state, configuration, artifact_id, draft_version::text
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid
            AND task_id = $2::uuid
          FOR SHARE`,
        [request.revisionId, task.id],
      );
      const revision = revisionResult.rows[0];
      if (revision === undefined || task.current_draft_revision_id !== request.revisionId) {
        throw new RuntimeSupervisorError('RUNTIME_REVISION_NOT_CURRENT');
      }
      if (!['PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(revision.state)) {
        throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_REQUIRED');
      }
      const configuration = parseRevisionConfiguration(revision.configuration);
      if (
        actor.role !== 'ADMIN' &&
        configuration.kind === 'PG_TO_KAFKA' &&
        configuration.source.resourceMode === 'APE_DTS_MANAGED'
      ) {
        throw new RuntimeSupervisorError('RUNTIME_ACCESS_DENIED');
      }
      const preflight = await client.query<PreflightRow>(
        `SELECT id, state, completed_at, result
            FROM ape_dts_platform.preflight_runs
           WHERE task_revision_id = $1::uuid
           ORDER BY started_at DESC, id DESC
           LIMIT 1
           FOR SHARE`,
        [request.revisionId],
      );
      if (preflight.rows[0] === undefined || preflight.rows[0].state !== 'PASSED') {
        throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_REQUIRED');
      }
      if (preflight.rows[0].completed_at === null) {
        throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_REQUIRED');
      }
      const parsedPreflight = taskPreflightResultSchema.safeParse(preflight.rows[0].result);
      if (!parsedPreflight.success) {
        throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
          reason: 'PREFLIGHT_RESULT_INVALID',
        });
      }
      const reinitialization =
        parsedPreflight.data.checks.find((check) => check.key === 'CONFIGURATION')?.details
          .reinitializeMysqlSnapshot === true;
      if (
        reinitialization !== (request.reinitializeMysqlSnapshot === true) ||
        (request.reinitializeMysqlSnapshot === true &&
          (configuration.kind !== 'MYSQL_TO_KAFKA' ||
            !configuration.source.includeSnapshot ||
            task.desired_state !== 'STOPPED'))
      )
        throw new RuntimeSupervisorError('RUNTIME_PREFLIGHT_STALE', {
          reason: 'MYSQL_REINITIALIZATION_INTENT_CHANGED',
        });
      if (this.metricsPorts === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED');
      }
      const unitKeys = runtimeUnitKeys(parsedPreflight.data, configuration);
      let metricsPorts: readonly number[];
      try {
        metricsPorts = await this.metricsPorts.allocate(client, unitKeys.length);
      } catch (error) {
        if (error instanceof RuntimeMetricsPortAllocatorError) {
          throw new RuntimeSupervisorError(
            error.code === 'RUNTIME_METRICS_PORTS_UNAVAILABLE'
              ? 'RUNTIME_METRICS_PORTS_UNAVAILABLE'
              : 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED',
          );
        }
        throw error;
      }
      const plannedRequest: PlannedStartTaskRuntimeRequest = {
        ...request,
        units: unitKeys.map((unitKey, index) => ({
          metricsPort: metricsPorts[index]!,
          unitKey,
        })),
      };
      assertUnitRequests(plannedRequest.units);
      await this.assertStartPreconditions(
        client,
        task,
        revision,
        configuration,
        preflight.rows[0],
        plannedRequest,
      );
      await this.assertContinuityReady(client, task.id, request.revisionId);
      const activeOperation = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.task_operations
          WHERE task_id = $1::uuid
            AND state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')
          FOR UPDATE`,
        [task.id],
      );
      if (activeOperation.rows[0] !== undefined) {
        throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
      }
      if (configuration.kind !== 'KAFKA_RAW_FORWARD' && plannedRequest.units.length !== 1) {
        throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
      }
      if (
        configuration.kind === 'KAFKA_RAW_FORWARD' &&
        plannedRequest.units.some(
          (unit) => kafkaSourceBinding(configuration, unit.unitKey) === null,
        )
      ) {
        throw new RuntimeSupervisorError('RUNTIME_UNIT_CONFIGURATION_INVALID');
      }
      const occupiedPorts = await client.query<{ metrics_port: number }>(
        `SELECT metrics_port
             FROM ape_dts_platform.task_run_units
            WHERE metrics_port = ANY($1::integer[])
              AND metrics_port_released_at IS NULL
            FOR SHARE`,
        [plannedRequest.units.map((unit) => unit.metricsPort)],
      );
      if (occupiedPorts.rows.length > 0) {
        throw new RuntimeSupervisorError('RUNTIME_METRICS_PORT_CONFLICT', {
          occupiedMetricsPorts: occupiedPorts.rows.map((row) => row.metrics_port),
        });
      }

      const operationId = randomUUID();
      const runId = randomUUID();
      const platformInstanceId = await this.requirePlatformInstanceId(client);
      await client.query(
        `INSERT INTO ape_dts_platform.task_operations (
           id, task_id, action, idempotency_key, target_revision_id,
           state, initiated_by_user_id, reason, execution_stage, started_at
         ) VALUES ($1::uuid, $2::uuid, 'START', $3, $4::uuid,
                   'PREPARED', $5::uuid, 'runtime start', 'STARTING_BARRIER', statement_timestamp())`,
        [operationId, task.id, request.idempotencyKey, request.revisionId, actor.id],
      );
      await client.query(
        `INSERT INTO ape_dts_platform.task_runs (
           id, task_id, task_revision_id, operation_id, artifact_id,
           platform_instance_id, status, current_stage, started_at
         ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6::uuid,
                   'STARTING', 'STARTING_BARRIER', statement_timestamp())`,
        [runId, task.id, request.revisionId, operationId, revision.artifact_id, platformInstanceId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET run_id = $2::uuid, state = 'EXECUTING', updated_at = statement_timestamp()
          WHERE id = $1::uuid`,
        [operationId, runId],
      );
      const mysql =
        configuration.kind === 'MYSQL_TO_KAFKA'
          ? {
              config: configuration,
              position: await this.requireMysqlState().prepareWithin(client, {
                taskId: task.id,
                revisionId: request.revisionId,
                runId,
                config: configuration,
                recovery: false,
                actorUserId: actor.id,
                ...(request.reinitializeMysqlSnapshot === true ? { reinitialize: true } : {}),
              }),
            }
          : undefined;
      const preparedUnits: PreparedUnit[] = [];
      for (const unit of plannedRequest.units) {
        const unitId = randomUUID();
        const processToken = randomUUID();
        if (this.launchFactory === undefined) {
          throw new RuntimeSupervisorError('RUNTIME_LAUNCH_SPEC_UNCONFIGURED');
        }
        let launch: RuntimeSpawnSpec;
        try {
          launch = await this.launchFactory.create({
            config: configuration,
            ...(mysql === undefined ? {} : { mysql: mysql.position }),
            metricsPort: unit.metricsPort,
            revisionId: request.revisionId,
            runId,
            taskId: task.id,
            unitId,
            unitKey: unit.unitKey,
          });
        } catch (error) {
          if (error instanceof AuthenticatedRuntimeLaunchSpecError) {
            throw new RuntimeSupervisorError(error.code);
          }
          if (error instanceof RuntimeLaunchSpecError) {
            throw new RuntimeSupervisorError(error.code);
          }
          throw error;
        }
        preparedUnits.push({
          id: unitId,
          launch,
          processToken,
          unitKey: unit.unitKey,
          metricsPort: unit.metricsPort,
        });
        await client.query(
          `INSERT INTO ape_dts_platform.task_run_units (
             id, task_id, run_id, unit_key, status, runtime_directory,
             ini_path, log4rs_path, metrics_port, process_token, started_at
           ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, 'STARTING', $5, $6, $7, $8, $9, NULL)`,
          [
            unitId,
            task.id,
            runId,
            unit.unitKey,
            launch.runtimeDirectory,
            launch.iniPath,
            launch.log4rsPath,
            unit.metricsPort,
            processToken,
          ],
        );
      }
      const processBindingHash = createHash('sha256')
        .update(
          preparedUnits
            .map((unit) => `${unit.id}:${unit.processToken}`)
            .sort((left, right) => left.localeCompare(right))
            .join('\n'),
          'utf8',
        )
        .digest('hex');
      await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET process_binding_hash = $2
          WHERE id = $1::uuid
            AND status = 'STARTING'`,
        [runId, processBindingHash],
      );
      await this.capacity.reserveWithin(
        client,
        task.id,
        request.revisionId,
        preparedUnits.map((unit) => unit.unitKey),
        runId,
      );
      const stageTypes = runtimeStageTypes(configuration, mysql?.position.phase);
      await client.query(
        `INSERT INTO ape_dts_platform.task_run_stages (
           id, run_id, stage_order, stage_type, status, progress_total,
           started_at, ended_at, heartbeat_at
         )
         SELECT stage.id, $1::uuid, stage.stage_order, stage.stage_type, stage.status,
                NULL,
                CASE WHEN stage.status = 'PENDING' THEN NULL ELSE statement_timestamp() END,
                CASE WHEN stage.status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END,
                CASE WHEN stage.status = 'PENDING' THEN NULL ELSE statement_timestamp() END
           FROM unnest($2::uuid[], $3::integer[], $4::text[], $5::text[])
             AS stage(id, stage_order, stage_type, status)`,
        [
          runId,
          stageTypes.map(() => randomUUID()),
          stageTypes.map((_, index) => index + 1),
          stageTypes,
          stageTypes.map((_, index) =>
            index < 4 ? 'SUCCEEDED' : index === 4 ? 'RUNNING' : 'PENDING',
          ),
        ],
      );
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET desired_state = 'RUNNING', observed_state = 'STARTING',
                row_version = row_version + 1, updated_by_user_id = $2::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND row_version = $3::bigint`,
        [task.id, actor.id, request.expectedTaskRowVersion],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_START_ACCEPTED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: {
          operationId,
          revisionId: request.revisionId,
          runId,
          unitCount: preparedUnits.length,
        },
        subjectId: task.id,
        subjectType: 'TASK',
      });
      const operation = await this.requireOperationWithin(client, operationId);
      return {
        actorUserId: actor.id,
        kind: 'PREPARED',
        operation,
        operationId,
        platformInstanceId,
        revisionId: request.revisionId,
        runtimeStage:
          (mysql?.position.phase ?? scenarioForConfiguration(configuration)) === 'SNAPSHOT'
            ? 'SNAPSHOT'
            : 'CDC_STARTED',
        runId,
        taskId: task.id,
        taskRowVersionAfterPrepare: incrementRowVersion(task.row_version),
        systemRecovery: false,
        units: preparedUnits,
        ...(mysql === undefined ? {} : { mysql }),
      };
    });
  }

  private async requirePlatformInstanceId(client: PoolClient): Promise<string> {
    const configured = this.bootstrapState?.getStatus().platformInstanceId;
    if (configured !== undefined) {
      return configured;
    }
    // Compatibility for direct service construction in integration harnesses.
    // The application module always injects BootstrapStateService.  Refuse an
    // ambiguous live-instance set instead of guessing which owner to bind.
    const active = await client.query<{ id: string }>(
      `SELECT id
         FROM ape_dts_platform.platform_instances
        WHERE ended_at IS NULL
        ORDER BY started_at DESC, id DESC
        LIMIT 2`,
    );
    if (active.rows.length !== 1) {
      throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS', {
        reason: 'RUNTIME_PLATFORM_INSTANCE_BINDING_UNAVAILABLE',
      });
    }
    return active.rows[0]!.id;
  }

  private async prepareStop(
    actor: UserSummary,
    request: StopTaskRuntimeRequest,
  ): Promise<PreparedStop | ExistingOperation> {
    return await this.transaction(async (client) => {
      const taskResult = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS}
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
          FOR UPDATE`,
        [request.taskId],
      );
      const task = taskResult.rows[0];
      if (task === undefined || task.deleted_at !== null) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_NOT_FOUND');
      }
      // See prepareStart: task locking must precede the idempotency lookup so
      // concurrent STOP calls return the same operation deterministically.
      await this.assertTaskAccess(client, actor, task);
      const existing = await this.findOperationWithin(
        client,
        request.taskId,
        request.idempotencyKey,
      );
      if (existing !== null) {
        if (existing.action !== 'STOP') {
          throw new RuntimeSupervisorError('RUNTIME_IDEMPOTENCY_KEY_CONFLICT');
        }
        return { kind: 'EXISTING', operation: existing };
      }
      if (task.row_version !== request.expectedTaskRowVersion) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const activeOperation = await client.query<{
        action: RuntimeOperationSummary['action'];
        id: string;
      }>(
        `SELECT id, action
           FROM ape_dts_platform.task_operations
          WHERE task_id = $1::uuid
            AND state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')
          FOR UPDATE`,
        [task.id],
      );
      if (activeOperation.rows[0] !== undefined) {
        throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
      }
      if (
        !['STARTING', 'RUNNING'].includes(task.observed_state) ||
        task.desired_state !== 'RUNNING'
      ) {
        throw new RuntimeSupervisorError('RUNTIME_STOP_NOT_ALLOWED');
      }
      const activeRun = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.task_runs
          WHERE task_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING')
          ORDER BY started_at DESC
          LIMIT 1
          FOR UPDATE`,
        [task.id],
      );
      const runId = activeRun.rows[0]?.id;
      if (runId === undefined) {
        throw new RuntimeSupervisorError('RUNTIME_STOP_NOT_ALLOWED');
      }
      const units = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.task_run_units
          WHERE run_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING', 'STOPPING')
          ORDER BY unit_key
          FOR UPDATE`,
        [runId],
      );
      const operationId = randomUUID();
      await client.query(
        `INSERT INTO ape_dts_platform.task_operations (
           id, task_id, action, idempotency_key, target_revision_id,
           run_id, state, initiated_by_user_id, reason, execution_stage, started_at
         ) VALUES ($1::uuid, $2::uuid, 'STOP', $3, $4::uuid,
                   $5::uuid, 'EXECUTING', $6::uuid, 'runtime stop', 'STOPPING', statement_timestamp())`,
        [
          operationId,
          task.id,
          request.idempotencyKey,
          task.current_draft_revision_id ?? task.current_applied_revision_id,
          runId,
          actor.id,
        ],
      );
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET desired_state = 'STOPPED', observed_state = 'STOPPING',
                row_version = row_version + 1, updated_by_user_id = $2::uuid,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND row_version = $3::bigint`,
        [task.id, actor.id, request.expectedTaskRowVersion],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET status = 'STOPPING', current_stage = 'STOPPING'
          WHERE id = $1::uuid
            AND status IN ('STARTING', 'RUNNING')`,
        [runId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_run_units
            SET status = 'STOPPING'
          WHERE run_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING')`,
        [runId],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_STOP_ACCEPTED',
        actorType: 'USER',
        actorUserId: actor.id,
        details: { operationId, runId, unitCount: units.rows.length },
        subjectId: task.id,
        subjectType: 'TASK',
      });
      return {
        kind: 'PREPARED',
        operationId,
        runId,
        taskId: task.id,
        unitIds: units.rows.map((unit) => unit.id),
      };
    });
  }

  private async finishStartedProcessFailure(
    prepared: PreparedStart,
    started: readonly { readonly unit: PreparedUnit; readonly process: ManagedRuntimeProcess }[],
    error: unknown,
  ): Promise<boolean> {
    const stopResults = await Promise.allSettled(started.map(({ process }) => process.stop()));
    const logResults = stopResults.flatMap((result) =>
      result.status === 'fulfilled' ? [result.value] : [],
    );
    const stopUnconfirmed = stopResults.some(
      (result) =>
        result.status === 'rejected' ||
        (result.status === 'fulfilled' &&
          (!result.value.confirmed || result.value.terminationTimedOut)),
    );
    const logState = await this.settleRunLogState(prepared.runId, logResults);
    if (stopUnconfirmed) {
      this.rememberActiveRun(
        prepared,
        new Map(started.map(({ process, unit }) => [unit.id, process])),
      );
      await this.finishStartFailure(
        prepared,
        {
          blockManual: true,
          failureCode: hasConfigurationCleanupFailure(logResults)
            ? 'RUNTIME_CONFIGURATION_CLEANUP_FAILED'
            : 'RUNTIME_PROCESS_HANDLE_MISSING',
          failureSummary: hasConfigurationCleanupFailure(logResults)
            ? CONFIGURATION_CLEANUP_FAILURE_SUMMARY
            : '启动失败后仍有运行单元未确认退出，已进入人工阻塞，容量保持占用。',
          releaseCapacity: false,
        },
        logState,
        logResults,
      );
      return true;
    }

    const configurationCleanupFailed =
      error instanceof RuntimeConfigurationCleanupError ||
      hasConfigurationCleanupFailure(logResults);
    const disposition: StartFailureDisposition = configurationCleanupFailed
      ? {
          auditDetails: {
            originalFailureType:
              error instanceof RuntimeConfigurationCleanupError &&
              error.originalError instanceof Error
                ? error.originalError.name
                : null,
          },
          blockManual: true,
          failureCode: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
          failureSummary: CONFIGURATION_CLEANUP_FAILURE_SUMMARY,
          releaseCapacity: true,
        }
      : {
          blockManual: false,
          failureCode: 'RUNTIME_PROCESS_START_FAILED',
          failureSummary:
            error instanceof Error ? error.message.slice(0, 512) : '运行单元启动失败。',
          releaseCapacity: true,
        };
    await this.finishStartFailure(prepared, disposition, logState, logResults);
    return false;
  }

  private async finishStartSuccess(
    prepared: PreparedStart,
    started: readonly { readonly unit: PreparedUnit; readonly process: ManagedRuntimeProcess }[],
  ): Promise<void> {
    await this.transaction(async (client) => {
      const task = await client.query<Pick<TaskRow, 'desired_state' | 'row_version'>>(
        `SELECT desired_state, row_version::text
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
            AND deleted_at IS NULL
          FOR UPDATE`,
        [prepared.taskId],
      );
      if (
        task.rows[0]?.desired_state !== 'RUNNING' ||
        task.rows[0].row_version !== prepared.taskRowVersionAfterPrepare
      ) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      const revision = await client.query<Pick<RevisionRow, 'state' | 'task_id'>>(
        `SELECT task_id, state
           FROM ape_dts_platform.task_revisions
          WHERE id = $1::uuid
          FOR UPDATE`,
        [prepared.revisionId],
      );
      if (
        revision.rows[0]?.task_id !== prepared.taskId ||
        !(prepared.systemRecovery ? ['APPLIED'] : ['PREFLIGHT_PASSED', 'RISK_CONFIRMED']).includes(
          revision.rows[0].state,
        )
      ) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      for (const { process, unit } of started) {
        await client.query(
          `UPDATE ape_dts_platform.task_run_units
              SET status = 'RUNNING', process_pid = $2, started_at = statement_timestamp()
            WHERE id = $1::uuid
              AND run_id = $3::uuid
              AND status = 'STARTING'`,
          [unit.id, process.pid ?? null, prepared.runId],
        );
      }
      await this.capacity.activateWithin(client, prepared.runId);
      await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET status = 'RUNNING', current_stage = $2
          WHERE id = $1::uuid
            AND status = 'STARTING'`,
        [prepared.runId, prepared.runtimeStage === 'SNAPSHOT' ? 'SNAPSHOT' : 'MONITORING_READY'],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_run_stages
            SET status = 'SUCCEEDED', ended_at = statement_timestamp(), heartbeat_at = statement_timestamp()
          WHERE run_id = $1::uuid
            AND stage_order = 5
            AND status = 'RUNNING'`,
        [prepared.runId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_run_stages
            SET status = CASE WHEN stage_type = 'SNAPSHOT' THEN 'RUNNING' ELSE 'SUCCEEDED' END,
                started_at = statement_timestamp(),
                ended_at = CASE WHEN stage_type = 'SNAPSHOT' THEN NULL ELSE statement_timestamp() END,
                heartbeat_at = statement_timestamp(),
                progress_total = NULL
          WHERE run_id = $1::uuid
            AND stage_order = 6
            AND stage_type = $2
            AND status = 'PENDING'`,
        [prepared.runId, prepared.runtimeStage],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_run_stages
            SET status = 'SUCCEEDED', started_at = statement_timestamp(),
                ended_at = statement_timestamp(), heartbeat_at = statement_timestamp(),
                progress_total = NULL
          WHERE run_id = $1::uuid
            AND stage_type = 'MONITORING_READY'
            AND status = 'PENDING'`,
        [prepared.runId],
      );
      if (!prepared.systemRecovery) {
        await applyPgTableExpansionContinuity(client, prepared.taskId, prepared.revisionId);
        await client.query(
          `UPDATE ape_dts_platform.task_revisions revision SET state='SUPERSEDED',
              row_version=revision.row_version+1,updated_at=statement_timestamp()
            FROM ape_dts_platform.tasks task WHERE task.id=$1::uuid
              AND revision.id=task.current_applied_revision_id AND revision.task_id=task.id
              AND revision.id<>$2::uuid AND revision.state='APPLIED'`,
          [prepared.taskId, prepared.revisionId],
        );
        const appliedRevision = await client.query(
          `UPDATE ape_dts_platform.task_revisions
              SET state = 'APPLIED', row_version = row_version + 1, updated_at = statement_timestamp()
            WHERE id = $1::uuid
              AND task_id = $2::uuid
              AND state IN ('PREFLIGHT_PASSED', 'RISK_CONFIRMED')`,
          [prepared.revisionId, prepared.taskId],
        );
        if (appliedRevision.rowCount !== 1) {
          throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
        }
      }
      const runningTask = await client.query(
        `UPDATE ape_dts_platform.tasks
            SET current_applied_revision_id = $2::uuid,
                current_draft_revision_id = NULL,
                observed_state = 'RUNNING',
                recovery_disposition = CASE
                  WHEN $4::boolean THEN recovery_disposition
                  ELSE 'AUTO_ELIGIBLE'
                END,
                row_version = row_version + 1,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND deleted_at IS NULL
            AND desired_state = 'RUNNING'
            AND row_version = $3::bigint
            AND CASE
                  WHEN $4::boolean THEN current_applied_revision_id = $2::uuid
                  ELSE current_draft_revision_id = $2::uuid
                END`,
        [
          prepared.taskId,
          prepared.revisionId,
          prepared.taskRowVersionAfterPrepare,
          prepared.systemRecovery,
        ],
      );
      if (runningTask.rowCount !== 1) {
        throw new RuntimeSupervisorError('RUNTIME_TASK_VERSION_CONFLICT');
      }
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET state = 'SUCCEEDED', execution_stage = $2,
                completed_at = statement_timestamp(), updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND state = 'EXECUTING'`,
        [
          prepared.operationId,
          prepared.runtimeStage === 'SNAPSHOT' ? 'SNAPSHOT' : 'MONITORING_READY',
        ],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_STARTED',
        actorType: prepared.actorUserId === null ? 'SYSTEM' : 'USER',
        actorUserId: prepared.actorUserId,
        details: {
          operationId: prepared.operationId,
          runId: prepared.runId,
          unitCount: started.length,
        },
        subjectId: prepared.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async finishStartFailure(
    prepared: PreparedStart,
    disposition: StartFailureDisposition,
    logState: RuntimeLogStateResolution = {
      persistInTerminalTransaction: false,
      state: null,
    },
    logResults: readonly RuntimeProcessExit[] = [],
    failureStage: 'PREPARE_RESOURCES' | 'STARTING_BARRIER' = 'STARTING_BARRIER',
  ): Promise<void> {
    const { auditDetails, blockManual, failureCode, failureSummary, releaseCapacity } = disposition;
    await this.transaction(async (client) => {
      await client.query(
        `UPDATE ape_dts_platform.task_run_units
            SET status = CASE
                          WHEN $2::boolean THEN CASE WHEN status IN ('STARTING', 'RUNNING', 'STOPPING') THEN 'BLOCKED' ELSE status END
                          WHEN status IN ('STARTING', 'RUNNING', 'STOPPING') THEN 'FAILED'
                          ELSE status
                        END,
                ended_at = statement_timestamp(), failure_code = $3,
                failure_summary = $4,
                metrics_port_released_at = CASE
                  WHEN $5::boolean THEN statement_timestamp()
                  ELSE metrics_port_released_at
                END
          WHERE run_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
        [prepared.runId, blockManual, failureCode, failureSummary, releaseCapacity],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET status = CASE WHEN $2::boolean THEN 'BLOCKED' ELSE 'FAILED' END,
                 current_stage = $9, ended_at = statement_timestamp(),
                 failure_code = $3, failure_summary = $4,
                 has_gap = has_gap OR $5::boolean,
                 log_storage_state = CASE
                   WHEN NOT $6::boolean THEN log_storage_state
                   WHEN log_storage_state = 'DEGRADED' OR $7 = 'DEGRADED' THEN 'DEGRADED'
                   WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                   ELSE $7
                 END,
                 log_recovery_state = CASE WHEN $6::boolean THEN $8 ELSE log_recovery_state END
          WHERE id = $1::uuid
            AND status IN ('STARTING', 'RUNNING')`,
        [
          prepared.runId,
          blockManual,
          failureCode,
          failureSummary,
          logResults.some((result) => result.logGapBytes > 0 || result.logStreamError === true),
          logState.persistInTerminalTransaction && logState.state !== null,
          logState.state?.storageState ?? null,
          logState.state?.recoveryState ?? null,
          failureStage,
        ],
      );
      if (releaseCapacity) {
        await this.capacity.releaseWithin(client, prepared.runId);
      }
      await client.query(
        `UPDATE ape_dts_platform.task_run_stages
            SET status = CASE WHEN $2::boolean THEN 'PAUSED' ELSE 'FAILED' END,
                ended_at = CASE WHEN $2::boolean THEN NULL ELSE statement_timestamp() END,
                failure_code = $3, failure_summary = $4
          WHERE run_id = $1::uuid
            AND stage_type = $5
            AND status IN ('RUNNING', 'SUCCEEDED')`,
        [prepared.runId, blockManual, failureCode, failureSummary, failureStage],
      );
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET desired_state = 'STOPPED', observed_state = CASE WHEN $2::boolean OR $3::boolean THEN 'BLOCKED' ELSE 'FAILED' END,
                recovery_disposition = CASE WHEN $3::boolean THEN 'MANUAL_REQUIRED' ELSE recovery_disposition END,
                manual_gate_reason = CASE WHEN $3::boolean THEN $4 ELSE manual_gate_reason END,
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND desired_state = 'RUNNING'`,
        [prepared.taskId, blockManual, prepared.systemRecovery, failureCode],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET state = CASE WHEN $2::boolean THEN 'BLOCKED_MANUAL' ELSE 'FAILED_RETRYABLE' END,
                execution_stage = $5,
                failure_code = $3, failure_summary = $4,
                completed_at = statement_timestamp(), updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND state = 'EXECUTING'`,
        [prepared.operationId, blockManual, failureCode, failureSummary, failureStage],
      );
      await this.audit.append(client, {
        action: blockManual ? 'TASK_RUNTIME_START_BLOCKED' : 'TASK_RUNTIME_START_FAILED',
        actorType: prepared.actorUserId === null ? 'SYSTEM' : 'USER',
        actorUserId: prepared.actorUserId,
        details: {
          ...auditDetails,
          operationId: prepared.operationId,
          runId: prepared.runId,
          releaseCapacity,
        },
        subjectId: prepared.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async finishStopSuccess(
    prepared: PreparedStop,
    results: readonly RuntimeProcessExit[],
    logResolution: RuntimeLogStateResolution,
  ): Promise<void> {
    const forcedKillCount = results.filter((result) => result.forcedKill).length;
    const hasLogGap = results.some(
      (result) => result.logGapBytes > 0 || result.logStreamError === true,
    );
    const logState = logResolution.state ?? aggregateRuntimeLogState(results);
    await this.transaction(async (client) => {
      for (const [index, unitId] of prepared.unitIds.entries()) {
        const result = results[index];
        await client.query(
          `UPDATE ape_dts_platform.task_run_units
              SET status = 'STOPPED', ended_at = statement_timestamp(),
                  exit_code = $2, exit_signal = $3,
                  metrics_port_released_at = statement_timestamp()
            WHERE id = $1::uuid
              AND run_id = $4::uuid
              AND status = 'STOPPING'`,
          [unitId, result?.code ?? null, result?.signal ?? null, prepared.runId],
        );
      }
      await client.query(
        `UPDATE ape_dts_platform.task_runs
             SET status = 'STOPPED', current_stage = 'STOPPED', ended_at = statement_timestamp(),
                 has_gap = has_gap OR $2::boolean,
                 log_storage_state = CASE
                   WHEN NOT $5::boolean THEN log_storage_state
                   WHEN log_storage_state = 'DEGRADED' OR $3 = 'DEGRADED' THEN 'DEGRADED'
                   WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                   ELSE $3
                 END,
                 log_recovery_state = CASE WHEN $5::boolean THEN $4 ELSE log_recovery_state END
           WHERE id = $1::uuid
             AND status = 'STOPPING'`,
        [
          prepared.runId,
          hasLogGap,
          logState.storageState,
          logState.recoveryState,
          logResolution.persistInTerminalTransaction,
        ],
      );
      await this.capacity.releaseWithin(client, prepared.runId);
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET observed_state = 'STOPPED', desired_state = 'STOPPED',
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND desired_state = 'STOPPED'`,
        [prepared.taskId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET state = 'SUCCEEDED', execution_stage = 'STOPPED',
                completed_at = statement_timestamp(), updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND state = 'EXECUTING'`,
        [prepared.operationId],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_STOPPED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: { forcedKillCount, operationId: prepared.operationId, runId: prepared.runId },
        subjectId: prepared.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async finishStopCleanupBlocked(
    prepared: PreparedStop,
    results: readonly RuntimeProcessExit[],
    logResolution: RuntimeLogStateResolution,
    failure: { readonly code: string; readonly releaseCapacity: boolean; readonly summary: string },
  ): Promise<void> {
    const failureSummary = failure.summary;
    const hasLogGap = results.some(
      (result) => result.logGapBytes > 0 || result.logStreamError === true,
    );
    const logState = logResolution.state ?? aggregateRuntimeLogState(results);
    const releaseCapacity = failure.releaseCapacity;
    await this.transaction(async (client) => {
      for (const [index, unitId] of prepared.unitIds.entries()) {
        const result = results[index];
        await client.query(
          `UPDATE ape_dts_platform.task_run_units
              SET status = 'STOPPED', ended_at = statement_timestamp(),
                  exit_code = $2, exit_signal = $3,
                  metrics_port_released_at = CASE
                    WHEN $5::boolean THEN statement_timestamp()
                    ELSE metrics_port_released_at
                  END
            WHERE id = $1::uuid
              AND run_id = $4::uuid
              AND status = 'STOPPING'`,
          [unitId, result?.code ?? null, result?.signal ?? null, prepared.runId, releaseCapacity],
        );
      }
      await client.query(
        `UPDATE ape_dts_platform.task_runs
             SET status = 'BLOCKED', current_stage = 'STOPPING', ended_at = statement_timestamp(),
                 failure_code = $2, failure_summary = $3,
                 has_gap = has_gap OR $4::boolean,
                 log_storage_state = CASE
                   WHEN NOT $7::boolean THEN log_storage_state
                   WHEN log_storage_state = 'DEGRADED' OR $5 = 'DEGRADED' THEN 'DEGRADED'
                   WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                   ELSE $5
                 END,
                 log_recovery_state = CASE WHEN $7::boolean THEN $6 ELSE log_recovery_state END
           WHERE id = $1::uuid
             AND status = 'STOPPING'`,
        [
          prepared.runId,
          failure.code,
          failureSummary,
          hasLogGap,
          logState.storageState,
          logState.recoveryState,
          logResolution.persistInTerminalTransaction,
        ],
      );
      if (releaseCapacity) {
        await this.capacity.releaseWithin(client, prepared.runId);
      }
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET observed_state = 'BLOCKED', desired_state = 'STOPPED',
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND desired_state = 'STOPPED'`,
        [prepared.taskId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET state = 'BLOCKED_MANUAL', execution_stage = 'STOPPING',
                failure_code = $2, failure_summary = $3,
                completed_at = statement_timestamp(), updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND state = 'EXECUTING'`,
        [prepared.operationId, failure.code, failureSummary],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_STOP_BLOCKED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: {
          cleanupFailure: failure.code,
          capacityReleased: releaseCapacity,
          operationId: prepared.operationId,
          runId: prepared.runId,
        },
        subjectId: prepared.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async finishStopBlocked(prepared: PreparedStop): Promise<void> {
    await this.transaction(async (client) => {
      await client.query(
        `UPDATE ape_dts_platform.task_run_units
            SET status = 'BLOCKED',
                ended_at = statement_timestamp(),
                failure_code = 'RUNTIME_PROCESS_HANDLE_MISSING',
                failure_summary = '当前容器没有对应的运行单元句柄，不能证明进程已停止。'
          WHERE run_id = $1::uuid
            AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
        [prepared.runId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_runs
            SET status = 'BLOCKED', current_stage = 'STOPPING', ended_at = statement_timestamp(),
                failure_code = 'RUNTIME_PROCESS_HANDLE_MISSING',
                failure_summary = '当前容器没有对应的运行单元句柄，不能证明进程已停止。'
          WHERE id = $1::uuid
            AND status = 'STOPPING'`,
        [prepared.runId],
      );
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET observed_state = 'BLOCKED', desired_state = 'STOPPED',
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND desired_state = 'STOPPED'`,
        [prepared.taskId],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_operations
            SET state = 'BLOCKED_MANUAL', execution_stage = 'STOPPING',
                failure_code = 'RUNTIME_PROCESS_HANDLE_MISSING',
                failure_summary = '当前容器没有对应的运行单元句柄，不能证明进程已停止。',
                completed_at = statement_timestamp(), updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND state = 'EXECUTING'`,
        [prepared.operationId],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_STOP_BLOCKED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: { operationId: prepared.operationId, runId: prepared.runId },
        subjectId: prepared.taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async observeUnexpectedExit(
    taskId: string,
    runId: string,
    unitId: string,
    result: RuntimeProcessExit,
  ): Promise<void> {
    if (this.shutdownInProgress) {
      return;
    }
    await this.withTaskTerminalAction(taskId, async () =>
      this.observeUnexpectedExitWithinTerminalAction(taskId, runId, unitId, result),
    );
  }

  private requireMysqlState(): MysqlRuntimeStateStore {
    if (this.mysqlState === undefined) throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_MISSING');
    return this.mysqlState;
  }

  private watchMysqlCheckpoints(prepared: PreparedStart): void {
    if (prepared.mysql === undefined) return;
    this.requireMysqlState().watch({
      taskId: prepared.taskId,
      revisionId: prepared.revisionId,
      runId: prepared.runId,
      directory: prepared.units[0]!.launch.runtimeDirectory,
      position: prepared.mysql.position,
    });
  }

  private async advanceMysqlSnapshot(runId: string, result: RuntimeProcessExit): Promise<boolean> {
    const prepared = this.mysqlRuns.get(runId);
    if (
      prepared?.mysql?.position.phase !== 'SNAPSHOT' ||
      result.code !== 0 ||
      result.signal !== null ||
      !result.confirmed ||
      result.forcedKill ||
      result.signals.length > 0 ||
      result.terminationTimedOut ||
      result.configurationCleanupFailed
    )
      return false;
    if (this.shutdownInProgress) return true;
    const release = this.acquireStartPermit();
    try {
      const state = this.requireMysqlState();
      const unit = prepared.units[0]!;
      if (!(await readMysqlSnapshotCompleted(unit.launch.runtimeDirectory, prepared.mysql.config)))
        throw new MysqlRuntimeStateError('MYSQL_SNAPSHOT_COMPLETION_UNPROVEN');
      await state.assertAvailable(prepared.mysql.config, prepared.mysql.position);
      const rowVersion = await this.transaction(async (client) => {
        const eligible = await client.query<{ row_version: string }>(
          `SELECT task.row_version::text FROM ape_dts_platform.tasks task
          JOIN ape_dts_platform.task_runs run ON run.task_id=task.id
          WHERE task.id=$1::uuid AND task.deleted_at IS NULL AND task.desired_state='RUNNING'
            AND task.observed_state='RUNNING' AND task.current_applied_revision_id=$3::uuid
            AND run.id=$2::uuid AND run.status='RUNNING' AND run.current_stage='SNAPSHOT'
          FOR UPDATE OF task, run`,
          [prepared.taskId, runId, prepared.revisionId],
        );
        if (eligible.rowCount !== 1)
          throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_BINDING_CHANGED');
        await state.finishSnapshotWithin(client, prepared.taskId, runId);
        return eligible.rows[0]!.row_version;
      });
      const mysql = {
        config: prepared.mysql.config,
        position: { ...prepared.mysql.position, phase: 'CDC' as const },
      };
      const launch = await this.launchFactory.create({
        config: mysql.config,
        mysql: mysql.position,
        metricsPort: unit.metricsPort,
        revisionId: prepared.revisionId,
        runId,
        taskId: prepared.taskId,
        unitId: unit.id,
        unitKey: unit.unitKey,
      });
      // Rendering decrypts current connections; recheck identity/version before using them.
      await this.revalidateExternalContinuity(
        prepared.taskId,
        prepared.revisionId,
        rowVersion,
        'RUNNING',
      );
      await state.assertAvailable(mysql.config, mysql.position);
      let process: ManagedRuntimeProcess | undefined;
      try {
        process = await this.processes.start({ ...launch, runId, unitId: unit.id });
        if (process.pid === undefined || !Number.isSafeInteger(process.pid) || process.pid <= 0)
          throw new MysqlRuntimeStateError('MYSQL_CDC_START_FAILED');
        const next = {
          ...prepared,
          mysql,
          runtimeStage: 'CDC_STARTED' as const,
          units: [{ ...unit, launch }],
        };
        // Keep the newly spawned handle reachable even if persisting the stage fails.
        this.rememberActiveRun(next, new Map([[unit.id, process]]));
        await this.transaction(async (client) => {
          const task = await client.query(
            `SELECT id FROM ape_dts_platform.tasks
            WHERE id=$1::uuid AND deleted_at IS NULL AND desired_state='RUNNING'
              AND observed_state='RUNNING' AND current_applied_revision_id=$2::uuid FOR UPDATE`,
            [prepared.taskId, prepared.revisionId],
          );
          if (task.rowCount !== 1)
            throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_BINDING_CHANGED');
          const running = await client.query(
            `UPDATE ape_dts_platform.task_runs SET current_stage='MONITORING_READY'
            WHERE id=$1::uuid AND task_id=$2::uuid AND status='RUNNING' AND current_stage='SNAPSHOT'`,
            [runId, prepared.taskId],
          );
          if (running.rowCount !== 1)
            throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_BINDING_CHANGED');
          await client.query(
            `UPDATE ape_dts_platform.task_run_units SET process_pid=$3, started_at=statement_timestamp()
            WHERE id=$1::uuid AND run_id=$2::uuid AND status='RUNNING'`,
            [unit.id, runId, process!.pid],
          );
          await client.query(
            `UPDATE ape_dts_platform.task_run_stages SET status='SUCCEEDED',
            started_at=COALESCE(started_at,statement_timestamp()), ended_at=statement_timestamp(),heartbeat_at=statement_timestamp()
            WHERE run_id=$1::uuid AND stage_type IN ('SNAPSHOT','CDC_STARTED','MONITORING_READY')
              AND status IN ('PENDING','RUNNING')`,
            [runId],
          );
          await this.audit.append(client, {
            action: 'TASK_MYSQL_SNAPSHOT_COMPLETED',
            actorType: 'SYSTEM',
            actorUserId: null,
            details: {
              runId,
              binlogFilename: mysql.position.binlogFilename,
              binlogPosition: mysql.position.binlogPosition,
            },
            subjectId: prepared.taskId,
            subjectType: 'TASK',
          });
        });
        this.watchMysqlCheckpoints(next);
        void process
          .waitForExit()
          .then((exit) => this.observeUnexpectedExit(prepared.taskId, runId, unit.id, exit))
          .catch(() => this.logger.error({ code: 'MYSQL_RUNTIME_EXIT_HANDLING_FAILED', runId }));
        return true;
      } catch (error) {
        if (process !== undefined) {
          let exit: RuntimeProcessExit;
          try {
            exit = await process.stop();
          } catch {
            throw new RuntimeSupervisorError('RUNTIME_PROCESS_HANDLE_MISSING');
          }
          if (!exit.confirmed || exit.terminationTimedOut)
            throw new RuntimeSupervisorError('RUNTIME_PROCESS_HANDLE_MISSING');
          if (exit.configurationCleanupFailed)
            throw new RuntimeSupervisorError('RUNTIME_CONFIGURATION_CLEANUP_FAILED');
        }
        if (error instanceof RuntimeConfigurationCleanupError)
          throw new RuntimeSupervisorError('RUNTIME_CONFIGURATION_CLEANUP_FAILED');
        if (error instanceof MysqlRuntimeStateError) throw error;
        throw new MysqlRuntimeStateError('MYSQL_CDC_START_FAILED');
      }
    } finally {
      release();
    }
  }

  private async observeUnexpectedExitWithinTerminalAction(
    taskId: string,
    runId: string,
    unitId: string,
    result: RuntimeProcessExit,
    failureCode = 'RUNTIME_UNEXPECTED_EXIT',
  ): Promise<void> {
    if (this.shutdownInProgress || this.failureInProgress.has(runId)) {
      return;
    }
    // Record the exit before any database I/O; a metadata outage must not erase the evidence.
    if (this.activeProcesses.get(runId)?.has(unitId)) {
      const snapshotCompletion =
        this.mysqlRuns.get(runId)?.mysql?.position.phase === 'SNAPSHOT' && result.code === 0;
      this.logger[snapshotCompletion ? 'log' : 'error']({
        code: snapshotCompletion ? 'MYSQL_SNAPSHOT_PROCESS_EXITED' : 'TASK_RUNTIME_PROCESS_EXITED',
        taskId,
        runId,
        unitId,
        exitCode: result.code,
        signal: result.signal,
        confirmed: result.confirmed,
      });
    }
    this.failureInProgress.add(runId);
    const failureSummary =
      failureCode === 'RUNTIME_PG_REPLICATION_DISCONNECTED'
        ? 'PostgreSQL 复制槽持续未连接，已停止失联进程并进入恢复流程。'
        : '运行单元意外退出。';
    let logResults: readonly RuntimeProcessExit[] = [result];
    let statePersisted = false;
    let failedTaskName: string | null = null;
    let retainHandles = false;
    try {
      if (!(await this.isPersistedRunRunning(runId))) {
        return;
      }
      try {
        if (await this.advanceMysqlSnapshot(runId, result)) return;
      } catch (error) {
        const unconfirmed =
          error instanceof RuntimeSupervisorError &&
          error.code === 'RUNTIME_PROCESS_HANDLE_MISSING';
        retainHandles = unconfirmed;
        await this.finishUnexpectedExitBlocked(
          taskId,
          runId,
          unitId,
          result,
          await this.settleRunLogState(runId, logResults),
          false,
          {
            failureCode:
              error instanceof MysqlRuntimeStateError || error instanceof RuntimeSupervisorError
                ? error.code
                : 'MYSQL_CDC_START_FAILED',
            failureSummary:
              error instanceof RuntimeSupervisorError &&
              error.code === 'RUNTIME_CONFIGURATION_CLEANUP_FAILED'
                ? CONFIGURATION_CLEANUP_FAILURE_SUMMARY
                : unconfirmed
                  ? 'MySQL 阶段衔接失败，新进程未确认停止，已保留运行占用以防止重复启动。'
                  : 'MySQL 全量衔接增量失败，请检查任务运行日志与 Binlog 位点。',
            processesConfirmed: !unconfirmed,
            releaseCapacity: !unconfirmed,
          },
        );
        statePersisted = true;
        return;
      }
      const processes = this.activeProcesses.get(runId);
      if (processes !== undefined) {
        const stopResults = await Promise.allSettled(
          [...processes.entries()]
            .filter(([id]) => id !== unitId)
            .map(([, process]) => process.stop()),
        );
        logResults = [
          result,
          ...stopResults.flatMap((stopResult) =>
            stopResult.status === 'fulfilled' ? [stopResult.value] : [],
          ),
        ];
        if (
          !result.confirmed ||
          result.terminationTimedOut ||
          stopResults.some(
            (stopResult) =>
              stopResult.status === 'rejected' ||
              (stopResult.status === 'fulfilled' &&
                (!stopResult.value.confirmed || stopResult.value.terminationTimedOut)),
          )
        ) {
          retainHandles = true;
          const logResolution = await this.settleRunLogState(runId, logResults);
          await this.finishUnexpectedExitBlocked(
            taskId,
            runId,
            unitId,
            result,
            logResolution,
            logResults.some((item) => item.logGapBytes > 0 || item.logStreamError === true),
            {
              failureCode: hasConfigurationCleanupFailure(logResults)
                ? 'RUNTIME_CONFIGURATION_CLEANUP_FAILED'
                : 'RUNTIME_PROCESS_HANDLE_MISSING',
              failureSummary: hasConfigurationCleanupFailure(logResults)
                ? CONFIGURATION_CLEANUP_FAILURE_SUMMARY
                : '运行单元意外退出后，其余进程未能确认停止。',
              processesConfirmed: false,
              releaseCapacity: false,
            },
          );
          statePersisted = true;
          return;
        }
      }
      const logResolution = await this.settleRunLogState(runId, logResults);
      if (!(await this.isPersistedRunRunning(runId))) {
        return;
      }
      const configurationCleanupFailed = hasConfigurationCleanupFailure(logResults);
      if (configurationCleanupFailed) {
        await this.finishUnexpectedExitBlocked(
          taskId,
          runId,
          unitId,
          result,
          logResolution,
          logResults.some((item) => item.logGapBytes > 0 || item.logStreamError === true),
          {
            failureCode: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
            failureSummary: CONFIGURATION_CLEANUP_FAILURE_SUMMARY,
            processesConfirmed: true,
            releaseCapacity: true,
          },
        );
        statePersisted = true;
        return;
      }
      const logState = logResolution.state ?? aggregateRuntimeLogState(logResults);
      statePersisted = await this.transaction(async (client) => {
        const task = await client.query<{
          display_name: string;
          current_applied_revision_id: string | null;
          id: string;
          row_version: string;
        }>(
          `SELECT id, display_name, current_applied_revision_id, row_version::text
             FROM ape_dts_platform.tasks
            WHERE id = $1::uuid
              AND deleted_at IS NULL
              AND desired_state = 'RUNNING'
            FOR UPDATE`,
          [taskId],
        );
        if (task.rows[0] === undefined) {
          return false;
        }
        failedTaskName = task.rows[0].display_name;
        const run = await client.query<{
          platform_instance_id: string | null;
          status: string;
          task_revision_id: string;
        }>(
          `SELECT status, task_revision_id, platform_instance_id
             FROM ape_dts_platform.task_runs
            WHERE id = $1::uuid
              AND task_id = $2::uuid
            FOR UPDATE`,
          [runId, taskId],
        );
        if (
          run.rows[0]?.status !== 'RUNNING' ||
          run.rows[0].platform_instance_id === null ||
          task.rows[0]?.current_applied_revision_id !== run.rows[0].task_revision_id
        ) {
          return false;
        }
        await client.query(
          `UPDATE ape_dts_platform.task_run_units
              SET status = CASE WHEN id = $2::uuid THEN 'FAILED' ELSE 'STOPPED' END,
                  ended_at = statement_timestamp(),
                  metrics_port_released_at = statement_timestamp(),
                  exit_code = CASE WHEN id = $2::uuid THEN $3 ELSE exit_code END,
                  exit_signal = CASE WHEN id = $2::uuid THEN $4 ELSE exit_signal END,
                  failure_code = CASE WHEN id = $2::uuid THEN $5 ELSE failure_code END,
                  failure_summary = CASE WHEN id = $2::uuid THEN $6 ELSE failure_summary END
            WHERE run_id = $1::uuid
              AND status IN ('RUNNING', 'STOPPING')`,
          [runId, unitId, result.code, result.signal, failureCode, failureSummary],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_runs
               SET status = 'FAILED', current_stage = 'RUNNING', ended_at = statement_timestamp(),
                   failure_code = $6, failure_summary = $7,
                    has_gap = has_gap OR $2::boolean,
                    log_storage_state = CASE
                      WHEN NOT $5::boolean THEN log_storage_state
                      WHEN log_storage_state = 'DEGRADED' OR $3 = 'DEGRADED' THEN 'DEGRADED'
                      WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                      ELSE $3
                    END,
                    log_recovery_state = CASE WHEN $5::boolean THEN $4 ELSE log_recovery_state END
             WHERE id = $1::uuid
               AND status = 'RUNNING'`,
          [
            runId,
            logResults.some((item) => item.logGapBytes > 0 || item.logStreamError === true),
            logState.storageState,
            logState.recoveryState,
            logResolution.persistInTerminalTransaction,
            failureCode,
            failureSummary,
          ],
        );
        await client.query(
          `UPDATE ape_dts_platform.tasks
              SET observed_state = 'FAILED', row_version = row_version + 1,
                  updated_at = statement_timestamp()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
              AND desired_state = 'RUNNING'
              AND observed_state = 'RUNNING'
              AND row_version = $2::bigint
              AND current_applied_revision_id = $3::uuid`,
          [taskId, task.rows[0].row_version, run.rows[0].task_revision_id],
        );
        await this.audit.append(client, {
          action: 'TASK_RUNTIME_UNEXPECTED_EXIT',
          actorType: 'SYSTEM',
          actorUserId: null,
          // Audit detail keys are intentionally separate from sensitive/error-code fields.
          // `code` is rejected by the audit ledger's sensitive-key guard.
          details: {
            exitValue: result.code,
            runId,
            terminationSignal: result.signal,
            unitId,
            reason: failureCode,
          },
          subjectId: taskId,
          subjectType: 'TASK',
        });
        return true;
      });
      if (statePersisted) {
        if (this.alerts && failedTaskName) {
          try {
            await this.alerts.observe(
              taskId,
              failedTaskName,
              'UNEXPECTED_EXIT',
              true,
              true,
              `故障码：${failureCode}`,
            );
          } catch (error) {
            this.logger.error({
              code: 'TASK_FAILURE_NOTIFICATION_FAILED',
              taskId,
              runId,
              reason: recoveryFailureReason(error),
            });
          }
        }
        const binding = await this.loadAutoRecoveryBinding(taskId, runId, unitId);
        if (binding !== null) {
          await this.autoRecovery.observeUnexpectedExit({ ...binding, eligible: true });
        }
      }
    } finally {
      this.failureInProgress.delete(runId);
      if (statePersisted && !retainHandles) {
        this.processes.finalizeRun(runId);
        this.forgetActiveRun(runId);
      }
    }
  }

  private async loadAutoRecoveryBinding(
    taskId: string,
    runId: string,
    unitId: string,
  ): Promise<AutoRecoveryContext | null> {
    const result = await this.database.requirePool().query<{
      platform_instance_id: string;
      process_token: string;
      row_version: string;
      task_revision_id: string;
    }>(
      `SELECT run.platform_instance_id, run.task_revision_id,
              unit.process_token, task.row_version::text
         FROM ape_dts_platform.tasks AS task
         JOIN ape_dts_platform.task_runs AS run
           ON run.task_id = task.id
         JOIN ape_dts_platform.task_run_units AS unit
           ON unit.run_id = run.id
          AND unit.id = $3::uuid
        WHERE task.id = $1::uuid
          AND task.deleted_at IS NULL
          AND task.desired_state = 'RUNNING'
          AND task.current_applied_revision_id = run.task_revision_id
          AND run.id = $2::uuid
          AND run.status = 'FAILED'
          AND run.platform_instance_id IS NOT NULL`,
      [taskId, runId, unitId],
    );
    const row = result.rows[0];
    return row === undefined
      ? null
      : {
          actorType: 'SYSTEM',
          expectedTaskRowVersion: row.row_version,
          firstFailureAt: new Date(),
          platformInstanceId: row.platform_instance_id,
          processToken: row.process_token,
          revisionId: row.task_revision_id,
          runId,
          taskId,
        };
  }

  private async isPersistedRunRunning(runId: string): Promise<boolean> {
    const result = await this.database.requirePool().query<{ status: string }>(
      `SELECT status
         FROM ape_dts_platform.task_runs
        WHERE id = $1::uuid`,
      [runId],
    );
    return result.rows[0]?.status === 'RUNNING';
  }

  private async finishUnexpectedExitBlocked(
    taskId: string,
    runId: string,
    unitId: string,
    result: RuntimeProcessExit,
    logResolution: RuntimeLogStateResolution,
    hasLogGap: boolean,
    reason: UnexpectedExitBlockReason,
  ): Promise<void> {
    const logState = logResolution.state ?? aggregateRuntimeLogState([result]);
    await this.transaction(async (client) => {
      const task = await client.query<{ id: string }>(
        `SELECT id
           FROM ape_dts_platform.tasks
          WHERE id = $1::uuid
            AND deleted_at IS NULL
          FOR UPDATE`,
        [taskId],
      );
      if (task.rows[0] === undefined) {
        return;
      }
      const run = await client.query<{ status: string }>(
        `SELECT status
           FROM ape_dts_platform.task_runs
          WHERE id = $1::uuid
          FOR UPDATE`,
        [runId],
      );
      if (run.rows[0]?.status !== 'RUNNING') {
        return;
      }
      await client.query(
        `UPDATE ape_dts_platform.task_run_units
            SET status = CASE
                          WHEN id = $2::uuid THEN 'FAILED'
                          WHEN $5::boolean THEN 'STOPPED'
                          ELSE 'BLOCKED'
                        END,
                ended_at = statement_timestamp(),
                metrics_port_released_at = CASE
                  WHEN $6::boolean THEN statement_timestamp()
                  ELSE metrics_port_released_at
                END,
                exit_code = CASE WHEN id = $2::uuid THEN $3 ELSE exit_code END,
                exit_signal = CASE WHEN id = $2::uuid THEN $4 ELSE exit_signal END,
                failure_code = CASE
                                 WHEN id = $2::uuid THEN 'RUNTIME_UNEXPECTED_EXIT'
                                 WHEN $5::boolean THEN NULL
                                 ELSE 'RUNTIME_PROCESS_HANDLE_MISSING'
                               END,
                failure_summary = CASE
                                    WHEN id = $2::uuid THEN '运行单元意外退出。'
                                    WHEN $5::boolean THEN NULL
                                    ELSE '其余运行单元未确认退出，已进入人工阻塞。'
                                  END
          WHERE run_id = $1::uuid
            AND status IN ('RUNNING', 'STOPPING')`,
        [
          runId,
          unitId,
          result.code,
          result.signal,
          reason.processesConfirmed,
          reason.releaseCapacity,
        ],
      );
      await client.query(
        `UPDATE ape_dts_platform.task_runs
             SET status = 'BLOCKED', current_stage = 'RUNNING', ended_at = statement_timestamp(),
                 failure_code = $6,
                 failure_summary = $7,
                 has_gap = has_gap OR $2::boolean,
                 log_storage_state = CASE
                   WHEN NOT $5::boolean THEN log_storage_state
                   WHEN log_storage_state = 'DEGRADED' OR $3 = 'DEGRADED' THEN 'DEGRADED'
                   WHEN log_storage_state = 'UNKNOWN' THEN 'UNKNOWN'
                   ELSE $3
                 END,
                 log_recovery_state = CASE
                   WHEN $5::boolean THEN $4
                   ELSE log_recovery_state
                 END
           WHERE id = $1::uuid
             AND status = 'RUNNING'`,
        [
          runId,
          hasLogGap,
          logState.storageState,
          logState.recoveryState,
          logResolution.persistInTerminalTransaction,
          reason.failureCode,
          reason.failureSummary,
        ],
      );
      if (reason.releaseCapacity) {
        await this.capacity.releaseWithin(client, runId);
      }
      if (reason.processesConfirmed) {
        await client.query(
          `UPDATE ape_dts_platform.task_operations
              SET state = 'BLOCKED_MANUAL', execution_stage = 'RUNNING',
                  failure_code = $2, failure_summary = $3,
                  completed_at = statement_timestamp(), updated_at = statement_timestamp()
            WHERE run_id = $1::uuid`,
          [runId, reason.failureCode, reason.failureSummary],
        );
      }
      await client.query(
        `UPDATE ape_dts_platform.tasks
            SET observed_state = 'BLOCKED',
                row_version = row_version + 1,
                updated_at = statement_timestamp()
          WHERE id = $1::uuid
            AND observed_state = 'RUNNING'`,
        [taskId],
      );
      await this.audit.append(client, {
        action: 'TASK_RUNTIME_UNEXPECTED_EXIT_BLOCKED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: {
          exitValue: result.code,
          reason: reason.failureSummary,
          runId,
          terminationSignal: result.signal,
          unitId,
        },
        subjectId: taskId,
        subjectType: 'TASK',
      });
    });
  }

  private async assertTaskAccess(
    client: PoolClient,
    actor: UserSummary,
    task: TaskRow,
  ): Promise<void> {
    if (actor.role === 'ADMIN') {
      return;
    }
    const requiredDataSourceIds = [
      ...new Set([task.source_data_source_id, task.target_data_source_id]),
    ];
    const grants = await client.query<{ count: string }>(
      `SELECT count(DISTINCT data_source_id)::text AS count
         FROM ape_dts_platform.data_source_grants
        WHERE user_id = $1::uuid
          AND data_source_id = ANY($2::uuid[])
          AND revoked_at IS NULL`,
      [actor.id, requiredDataSourceIds],
    );
    if (Number(grants.rows[0]?.count ?? '0') !== requiredDataSourceIds.length) {
      throw new RuntimeSupervisorError('RUNTIME_ACCESS_DENIED');
    }
  }

  private async findOperationWithin(
    client: PoolClient,
    taskId: string,
    idempotencyKey: string,
  ): Promise<RuntimeOperationSummary | null> {
    const result = await client.query<OperationRow>(
      `SELECT ${OPERATION_COLUMNS}
         FROM ape_dts_platform.task_operations
        WHERE task_id = $1::uuid
          AND idempotency_key = $2
        FOR UPDATE`,
      [taskId, idempotencyKey],
    );
    const row = result.rows[0];
    return row === undefined ? null : operationSummary(row);
  }

  private async requireOperation(operationId: string): Promise<RuntimeOperationSummary> {
    const result = await this.database.requirePool().query<OperationRow>(
      `SELECT ${OPERATION_COLUMNS}
         FROM ape_dts_platform.task_operations
        WHERE id = $1::uuid`,
      [operationId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
    }
    return operationSummary(row);
  }

  private async requireOperationWithin(
    client: PoolClient,
    operationId: string,
  ): Promise<RuntimeOperationSummary> {
    const result = await client.query<OperationRow>(
      `SELECT ${OPERATION_COLUMNS}
         FROM ape_dts_platform.task_operations
        WHERE id = $1::uuid`,
      [operationId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new RuntimeSupervisorError('RUNTIME_OPERATION_IN_PROGRESS');
    }
    return operationSummary(row);
  }
}
