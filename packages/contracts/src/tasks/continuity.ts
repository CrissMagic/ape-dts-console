import { z } from 'zod';

const rowVersionSchema = z.string().regex(/^[1-9]\d*$/u);

export const acceptTaskContinuityRequestSchema = z
  .object({
    expectedBaselineRowVersion: rowVersionSchema,
    expectedTaskRowVersion: rowVersionSchema,
    observationId: z.uuid(),
    reason: z.string().trim().min(1).max(512),
  })
  .strict();

export const taskContinuityAcceptanceSchema = z
  .object({
    acceptedAt: z.iso.datetime({ offset: true }),
    baselineRowVersion: rowVersionSchema,
    id: z.uuid(),
    observationId: z.uuid(),
    taskId: z.uuid(),
    taskRowVersion: rowVersionSchema,
  })
  .strict();

export type AcceptTaskContinuityRequest = z.infer<typeof acceptTaskContinuityRequestSchema>;
export type TaskContinuityAcceptance = z.infer<typeof taskContinuityAcceptanceSchema>;
