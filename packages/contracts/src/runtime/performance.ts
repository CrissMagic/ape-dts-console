import { z } from 'zod';

export const runtimePerformanceValuesSchema = z
  .object({
    recordsPerSecond: z.number().nonnegative().nullable(),
    bytesPerSecond: z.number().nonnegative().nullable(),
    sinkRecords: z.number().nonnegative().nullable(),
    sinkBytes: z.number().nonnegative().nullable(),
    queueRecords: z.number().nonnegative().nullable(),
    queueBytes: z.number().nonnegative().nullable(),
  })
  .strict();

export const runtimePerformanceSummarySchema = z
  .object({
    taskId: z.uuid(),
    runId: z.uuid().nullable(),
    status: z.enum(['FRESH', 'PARTIAL', 'STALE', 'NO_DATA', 'NOT_RUNNING']),
    collectedAt: z.iso.datetime({ offset: true }).nullable(),
    values: runtimePerformanceValuesSchema,
    delayStatus: z.literal('UNSUPPORTED'),
  })
  .strict();

export const runtimePerformancePointSchema = runtimePerformanceValuesSchema
  .extend({
    collectedAt: z.iso.datetime({ offset: true }),
    gap: z.boolean(),
  })
  .strict();

export const runtimePerformanceWindowSchema = z.enum(['1h', '24h', '7d']);
export const runtimePerformanceDetailSchema = z
  .object({
    summary: runtimePerformanceSummarySchema,
    window: runtimePerformanceWindowSchema,
    points: z.array(runtimePerformancePointSchema),
    runs: z
      .array(
        z
          .object({
            id: z.uuid(),
            status: z.string(),
            startedAt: z.iso.datetime({ offset: true }),
            endedAt: z.iso.datetime({ offset: true }).nullable(),
          })
          .strict(),
      )
      .optional(),
    maintenance: z
      .object({
        status: z.enum(['UNKNOWN', 'HEALTHY', 'FAILED']),
        lastAttemptAt: z.iso.datetime({ offset: true }).nullable(),
      })
      .strict(),
  })
  .strict();

export const runtimePerformanceQueryRequestSchema = z
  .object({
    taskIds: z.array(z.uuid()).max(100),
  })
  .strict();
export const runtimePerformanceQueryResponseSchema = z
  .object({
    items: z.array(runtimePerformanceSummarySchema),
  })
  .strict();

export type RuntimePerformanceValues = z.infer<typeof runtimePerformanceValuesSchema>;
export type RuntimePerformanceSummary = z.infer<typeof runtimePerformanceSummarySchema>;
export type RuntimePerformancePoint = z.infer<typeof runtimePerformancePointSchema>;
export type RuntimePerformanceDetail = z.infer<typeof runtimePerformanceDetailSchema>;
export type RuntimePerformanceWindow = z.infer<typeof runtimePerformanceWindowSchema>;
