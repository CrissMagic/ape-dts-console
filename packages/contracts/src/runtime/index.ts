import { z } from 'zod';

const rowVersionSchema = z.string().regex(/^[1-9]\d*$/u);
const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(255)
  .refine((value) => value.trim().length > 0 && !value.includes('\u0000'));

export const runtimeTaskIdentifierParametersSchema = z.object({ taskId: z.uuid() }).strict();
export const runtimeLogParametersSchema = z.object({ taskId: z.uuid(), runId: z.uuid() }).strict();

export const runtimeUnitLaunchRequestSchema = z
  .object({
    metricsPort: z.number().int().min(1).max(65_535),
    unitKey: z
      .string()
      .min(1)
      .refine((value) => value.trim().length > 0),
  })
  .strict();

export const startTaskRuntimeRequestSchema = z
  .object({
    expectedTaskRowVersion: rowVersionSchema,
    idempotencyKey: idempotencyKeySchema,
    revisionId: z.uuid(),
    reinitializeMysqlSnapshot: z.boolean().optional(),
  })
  .strict();

export const stopTaskRuntimeRequestSchema = z
  .object({
    expectedTaskRowVersion: rowVersionSchema,
    idempotencyKey: idempotencyKeySchema,
  })
  .strict();

export const runtimeOperationSummarySchema = z
  .object({
    action: z.enum([
      'START',
      'STOP',
      'RESTART',
      'APPLY_RESTART',
      'DELETE',
      'RESTORE',
      'EMERGENCY_STOP',
      'EMERGENCY_KILL',
    ]),
    completedAt: z.iso.datetime({ offset: true }).nullable(),
    createdAt: z.iso.datetime({ offset: true }),
    failureCode: z.string().min(1).max(128).nullable(),
    failureSummary: z.string().min(1).max(512).nullable(),
    id: z.uuid(),
    runId: z.uuid().nullable(),
    state: z.enum([
      'ACCEPTED',
      'PREFLIGHTING',
      'PREPARED',
      'EXECUTING',
      'SUCCEEDED',
      'FAILED_RETRYABLE',
      'BLOCKED_MANUAL',
      'BLOCKED_PERMANENT',
      'REJECTED',
    ]),
    taskId: z.uuid(),
    updatedAt: z.iso.datetime({ offset: true }),
  })
  .strict();

const runtimeTimestampSchema = z.iso.datetime({ offset: true }).nullable();
export const logStorageStateSchema = z.enum(['HEALTHY', 'DEGRADED', 'UNKNOWN']);
export const logRecoveryStateSchema = z.enum([
  'NONE',
  'MEMORY_PENDING',
  'DURABLE_PENDING',
  'MIXED_PENDING',
]);

export const runtimeReadStageSchema = z
  .object({
    completed: z.string().regex(/^\d+$/u),
    endedAt: runtimeTimestampSchema,
    failureCode: z.string().nullable(),
    failureSummary: z.string().nullable(),
    heartbeatAt: runtimeTimestampSchema,
    order: z.number().int().positive(),
    retryable: z.boolean(),
    startedAt: runtimeTimestampSchema,
    status: z.enum(['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PAUSED']),
    throughput: z.string().nullable(),
    total: z.string().regex(/^\d+$/u).nullable(),
    type: z.string().min(1),
  })
  .strict();

export const runtimeReadUnitSchema = z
  .object({
    endedAt: runtimeTimestampSchema,
    exitCode: z.number().int().nullable(),
    exitSignal: z.string().nullable(),
    failureCode: z.string().nullable(),
    failureSummary: z.string().nullable(),
    key: z.string().min(1),
    startedAt: runtimeTimestampSchema,
    status: z.enum(['STARTING', 'RUNNING', 'STOPPING', 'STOPPED', 'FAILED', 'BLOCKED']),
  })
  .strict();

export const runtimeReadSchema = z
  .object({
    operation: runtimeOperationSummarySchema.nullable(),
    retryable: z.boolean(),
    run: z
      .object({
        currentStage: z.string().min(1),
        endedAt: runtimeTimestampSchema,
        failureCode: z.string().nullable(),
        failureSummary: z.string().nullable(),
        id: z.uuid(),
        logRecoveryState: logRecoveryStateSchema,
        logStorageState: logStorageStateSchema,
        startedAt: z.iso.datetime({ offset: true }),
        status: z.enum(['STARTING', 'RUNNING', 'STOPPING', 'STOPPED', 'FAILED', 'BLOCKED']),
      })
      .strict()
      .nullable(),
    stages: z.array(runtimeReadStageSchema),
    taskId: z.uuid(),
    units: z.array(runtimeReadUnitSchema),
  })
  .strict();

export const runtimeRunListResponseSchema = z
  .object({
    runs: z.array(
      runtimeReadSchema.shape.run.unwrap().pick({
        id: true,
        status: true,
        startedAt: true,
        endedAt: true,
      }),
    ),
  })
  .strict();
export type RuntimeRunList = z.infer<typeof runtimeRunListResponseSchema>;

const runtimeLogFilterShape = {
  from: z.iso.datetime({ offset: true }).optional(),
  level: z.enum(['ERROR', 'WARN', 'INFO']).optional(),
  stream: z.enum(['STDOUT', 'STDERR']).optional(),
  to: z.iso.datetime({ offset: true }).optional(),
  // A mapped unit includes a 249-character Topic and a safe-integer partition number.
  unitKey: z.string().min(1).max(282).optional(),
};

function validateRuntimeLogTimeRange<
  T extends { readonly from?: string | undefined; readonly to?: string | undefined },
>(value: T, context: z.RefinementCtx<T>): void {
  if (
    value.from !== undefined &&
    value.to !== undefined &&
    Date.parse(value.from) >= Date.parse(value.to)
  ) {
    context.addIssue({
      code: 'custom',
      message: 'from must be earlier than to',
      path: ['to'],
    });
  }
}

export const runtimeLogDownloadQuerySchema = z
  .object(runtimeLogFilterShape)
  .strict()
  .superRefine(validateRuntimeLogTimeRange);

export const runtimeLogQuerySchema = z
  .object({
    ...runtimeLogFilterShape,
    order: z.enum(['ASC', 'DESC']).optional(),
    limit: z.coerce.number().int().min(1).max(1000).default(500),
  })
  .strict()
  .superRefine(validateRuntimeLogTimeRange);
export const runtimeLogFactSchema = z.discriminatedUnion('kind', [
  z
    .object({
      continuation: z.number().int().nonnegative(),
      continues: z.boolean(),
      kind: z.literal('LINE'),
      occurredAt: z.iso.datetime({ offset: true }),
      payload: z.string(),
      stream: z.enum(['STDOUT', 'STDERR']),
      unitId: z.uuid(),
      unitKey: z.string().min(1),
    })
    .strict(),
  z
    .object({
      kind: z.literal('GAP'),
      lostBytes: z.string().regex(/^\d+$/u).nullable(),
      occurredAt: z.iso.datetime({ offset: true }),
      reason: z.string().min(1),
      stream: z.enum(['STDOUT', 'STDERR']).nullable(),
      unitId: z.uuid().nullable(),
      unitKey: z.string().nullable(),
    })
    .strict(),
]);

export function runtimeLogLevel(payload: string): 'ERROR' | 'WARN' | 'INFO' | null {
  const plain = payload.replace(/\p{Cc}\[[0-9;]*m/gu, '').trimStart();
  const match =
    /^(?:\d{4}-\d{2}-\d{2}[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?\s+)?(ERROR|WARN|INFO)\b/iu.exec(plain);
  return match === null ? null : (match[1]!.toUpperCase() as 'ERROR' | 'WARN' | 'INFO');
}
export const runtimeLogReadSchema = z
  .object({
    facts: z.array(runtimeLogFactSchema),
    limited: z.boolean(),
    scanLimited: z.boolean().optional(),
    runId: z.uuid(),
    taskId: z.uuid(),
  })
  .strict();

export const runtimeLogStorageRetryResponseSchema = z
  .object({
    durableRecovery: z
      .object({
        availableSegments: z.number().int().nonnegative(),
        capacityDeletedSegments: z.number().int().nonnegative(),
        completedDeletions: z.number().int().nonnegative(),
        failures: z.number().int().nonnegative(),
        integrityFailedSegments: z.number().int().nonnegative(),
        memoryPendingGaps: z.number().int().nonnegative(),
        missingSegments: z.number().int().nonnegative(),
      })
      .strict()
      .nullable(),
    recoveryState: logRecoveryStateSchema,
    runId: z.uuid(),
    storageState: logStorageStateSchema,
    taskId: z.uuid(),
  })
  .strict();

export type RuntimeOperationSummary = z.infer<typeof runtimeOperationSummarySchema>;
export type RuntimeRead = z.infer<typeof runtimeReadSchema>;
export type RuntimeLogRead = z.infer<typeof runtimeLogReadSchema>;
export type RuntimeLogDownloadQuery = z.infer<typeof runtimeLogDownloadQuerySchema>;
export type RuntimeLogQuery = z.infer<typeof runtimeLogQuerySchema>;
export type RuntimeLogStorageRetryResponse = z.infer<typeof runtimeLogStorageRetryResponseSchema>;
export type RuntimeUnitLaunchRequest = z.infer<typeof runtimeUnitLaunchRequestSchema>;
export type StartTaskRuntimeRequest = z.infer<typeof startTaskRuntimeRequestSchema>;
export type StopTaskRuntimeRequest = z.infer<typeof stopTaskRuntimeRequestSchema>;
