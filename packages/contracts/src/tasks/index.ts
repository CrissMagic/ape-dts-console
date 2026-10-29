import { z } from 'zod';

export * from './risk-acceptance.js';
export * from './continuity.js';

const taskNameSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(/^[\p{L}\p{N}][\p{L}\p{N} ._:-]*$/u);
const rowVersionSchema = z.string().regex(/^[1-9]\d*$/u);
const draftConfigurationSchema = z.record(z.string(), z.unknown());

const taskGroupNameSchema = z.string().normalize('NFKC').trim().min(1).max(128);
export const createTaskGroupRequestSchema = z.object({ name: taskGroupNameSchema }).strict();
export const taskGroupIdentifierSchema = z.object({ groupId: z.uuid() }).strict();
export const renameTaskGroupRequestSchema = z
  .object({ name: taskGroupNameSchema, expectedName: taskGroupNameSchema })
  .strict();
export const deleteTaskGroupRequestSchema = z
  .object({ expectedName: taskGroupNameSchema, detachTasks: z.boolean() })
  .strict();
export const deleteTaskGroupResponseSchema = z.object({ deleted: z.literal(true) }).strict();
export const reorderTaskGroupsRequestSchema = z
  .object({ groupIds: z.array(z.uuid()), expectedGroupIds: z.array(z.uuid()) })
  .strict()
  .refine((value) =>
    [value.groupIds, value.expectedGroupIds].every((ids) => new Set(ids).size === ids.length),
  );
export const taskGroupSummarySchema = z
  .object({ id: z.uuid(), name: taskGroupNameSchema })
  .strict();
export const taskGroupListResponseSchema = z
  .object({ groups: z.array(taskGroupSummarySchema) })
  .strict();
export const assignTaskGroupRequestSchema = z
  .object({
    groupId: z.uuid().nullable(),
    expectedGroupId: z.uuid().nullable(),
  })
  .strict();

export const taskTypeSchema = z.enum(['PG_KAFKA', 'KAFKA_KAFKA', 'MYSQL_KAFKA']);
export const taskRevisionStateSchema = z.enum([
  'INCOMPLETE_DRAFT',
  'READY_DRAFT',
  'PREFLIGHT_PASSED',
  'RISK_CONFIRMED',
  'APPLIED',
  'SUPERSEDED',
  'ABANDONED',
  'STALE',
]);
export const taskDesiredStateSchema = z.enum(['STOPPED', 'RUNNING', 'DELETED']);
export const taskObservedStateSchema = z.enum([
  'STOPPED',
  'STARTING',
  'RUNNING',
  'STOPPING',
  'FAILED',
  'BLOCKED',
  'DELETED',
]);
export const taskObservabilityHealthSchema = z.enum(['HEALTHY', 'DEGRADED', 'UNAVAILABLE']);
export const taskRecoveryDispositionSchema = z.enum([
  'AUTO_ELIGIBLE',
  'MANUAL_REQUIRED',
  'CAPACITY_BLOCKED',
  'PERMANENTLY_BLOCKED',
  'NOT_APPLICABLE',
]);

export const taskPreflightCheckKeySchema = z.enum([
  'CONFIGURATION',
  'DATA_SOURCE_ACCESS',
  'SOURCE_CONNECTION',
  'TARGET_CONNECTION',
  'SOURCE_METADATA',
  'TARGET_METADATA',
  'PRIMARY_KEY',
  'TOPIC_IDENTITIES',
  'PG_RESOURCES',
  'MYSQL_CDC',
  'ARTIFACT',
  'CAPACITY',
  'RUNTIME_ENVIRONMENT',
]);
export const taskPreflightCheckStateSchema = z.enum(['PASSED', 'FAILED', 'SKIPPED']);
export const taskPreflightRunStateSchema = z.enum(['RUNNING', 'PASSED', 'FAILED', 'STALE']);
const preflightDetailsSchema = z.record(z.string(), z.unknown());

export const taskPreflightCheckSchema = z
  .object({
    code: z.string().min(1).max(128),
    details: preflightDetailsSchema,
    key: taskPreflightCheckKeySchema,
    state: taskPreflightCheckStateSchema,
    summary: z.string().min(1).max(512),
  })
  .strict();

export const taskPreflightCapacitySchema = z
  .object({
    available: z.number().int().nonnegative().nullable(),
    limit: z.number().int().positive().nullable(),
    requested: z.number().int().nonnegative().nullable(),
    used: z.number().int().nonnegative().nullable(),
  })
  .strict();

export const taskPreflightResultSchema = z
  .object({
    capacity: taskPreflightCapacitySchema,
    checks: z.array(taskPreflightCheckSchema),
    dataSourceVersions: z
      .object({
        source: rowVersionSchema,
        target: rowVersionSchema,
      })
      .strict(),
    identities: z
      .object({
        source: z.string().nullable(),
        sourceTopic: z.string().nullable(),
        target: z.string().nullable(),
        targetTopic: z.string().nullable(),
      })
      .strict(),
    taskRevision: z
      .object({
        draftVersion: rowVersionSchema,
        taskRowVersion: rowVersionSchema,
      })
      .strict(),
  })
  .strict();

export const runTaskPreflightRequestSchema = z
  .object({
    expectedTaskRowVersion: rowVersionSchema,
    revisionId: z.uuid(),
    reinitializeMysqlSnapshot: z.boolean().optional(),
  })
  .strict();

export const taskPreflightRunSummarySchema = z
  .object({
    completedAt: z.iso.datetime({ offset: true }).nullable(),
    failureCode: z.string().min(1).max(128).nullable(),
    failureSummary: z.string().min(1).max(512).nullable(),
    id: z.uuid(),
    result: taskPreflightResultSchema,
    revisionId: z.uuid(),
    startedAt: z.iso.datetime({ offset: true }),
    state: taskPreflightRunStateSchema,
    taskId: z.uuid(),
  })
  .strict();

export const createTaskRequestSchema = z
  .object({
    artifactId: z.uuid().nullable().optional(),
    configuration: draftConfigurationSchema,
    groupId: z.uuid().nullable().optional(),
    name: taskNameSchema,
    sourceDataSourceId: z.uuid(),
    targetDataSourceId: z.uuid(),
    taskType: taskTypeSchema,
  })
  .strict();

export const saveTaskDraftRequestSchema = z
  .object({
    acknowledgePgTableAddition: z.literal(true).optional(),
    artifactId: z.uuid().nullable().optional(),
    configuration: draftConfigurationSchema,
    expectedDraftVersion: rowVersionSchema,
    expectedTaskRowVersion: rowVersionSchema,
    revisionId: z.uuid(),
    taskType: taskTypeSchema,
  })
  .strict();

export const deleteTaskRequestSchema = z.object({ expectedRowVersion: rowVersionSchema }).strict();

export const taskIdentifierParametersSchema = z.object({ taskId: z.uuid() }).strict();

export const taskRevisionSummarySchema = z
  .object({
    artifactId: z.uuid().nullable(),
    configuration: draftConfigurationSchema,
    configurationSchemaVersion: z.number().int().positive().nullable(),
    createdAt: z.iso.datetime({ offset: true }),
    draftVersion: rowVersionSchema,
    id: z.uuid(),
    revisionNumber: rowVersionSchema,
    state: taskRevisionStateSchema,
    updatedAt: z.iso.datetime({ offset: true }),
  })
  .strict();

export const taskSummarySchema = z
  .object({
    createdAt: z.iso.datetime({ offset: true }),
    currentAppliedRevisionId: z.uuid().nullable(),
    deletedAt: z.iso.datetime({ offset: true }).nullable(),
    desiredState: taskDesiredStateSchema,
    draft: taskRevisionSummarySchema.nullable(),
    applied: taskRevisionSummarySchema.nullable().optional(),
    groupId: z.uuid().nullable().optional(),
    id: z.uuid(),
    name: taskNameSchema,
    observabilityHealth: taskObservabilityHealthSchema,
    observedState: taskObservedStateSchema,
    recoveryDisposition: taskRecoveryDispositionSchema,
    rowVersion: rowVersionSchema,
    sourceDataSourceId: z.uuid(),
    targetDataSourceId: z.uuid(),
    taskType: taskTypeSchema,
    updatedAt: z.iso.datetime({ offset: true }),
  })
  .strict();

export const taskListResponseSchema = z.object({ tasks: z.array(taskSummarySchema) }).strict();

export type CreateTaskRequest = z.infer<typeof createTaskRequestSchema>;
export type CreateTaskGroupRequest = z.infer<typeof createTaskGroupRequestSchema>;
export type RenameTaskGroupRequest = z.infer<typeof renameTaskGroupRequestSchema>;
export type DeleteTaskGroupRequest = z.infer<typeof deleteTaskGroupRequestSchema>;
export type ReorderTaskGroupsRequest = z.infer<typeof reorderTaskGroupsRequestSchema>;
export type AssignTaskGroupRequest = z.infer<typeof assignTaskGroupRequestSchema>;
export type TaskGroupSummary = z.infer<typeof taskGroupSummarySchema>;
export type DeleteTaskRequest = z.infer<typeof deleteTaskRequestSchema>;
export type SaveTaskDraftRequest = z.infer<typeof saveTaskDraftRequestSchema>;
export type TaskDesiredState = z.infer<typeof taskDesiredStateSchema>;
export type TaskObservedState = z.infer<typeof taskObservedStateSchema>;
export type TaskObservabilityHealth = z.infer<typeof taskObservabilityHealthSchema>;
export type RunTaskPreflightRequest = z.infer<typeof runTaskPreflightRequestSchema>;
export type TaskPreflightCapacity = z.infer<typeof taskPreflightCapacitySchema>;
export type TaskPreflightCheck = z.infer<typeof taskPreflightCheckSchema>;
export type TaskPreflightCheckKey = z.infer<typeof taskPreflightCheckKeySchema>;
export type TaskPreflightCheckState = z.infer<typeof taskPreflightCheckStateSchema>;
export type TaskPreflightResult = z.infer<typeof taskPreflightResultSchema>;
export type TaskPreflightRunState = z.infer<typeof taskPreflightRunStateSchema>;
export type TaskPreflightRunSummary = z.infer<typeof taskPreflightRunSummarySchema>;
export type TaskRecoveryDisposition = z.infer<typeof taskRecoveryDispositionSchema>;
export type TaskRevisionState = z.infer<typeof taskRevisionStateSchema>;
export type TaskRevisionSummary = z.infer<typeof taskRevisionSummarySchema>;
export type TaskSummary = z.infer<typeof taskSummarySchema>;
export type TaskType = z.infer<typeof taskTypeSchema>;
