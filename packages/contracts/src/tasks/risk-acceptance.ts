import { z } from 'zod';

const rowVersionSchema = z.string().regex(/^[1-9]\d*$/u);

export const acceptTaskRevisionRiskRequestSchema = z
  .object({
    expectedDraftVersion: rowVersionSchema,
    expectedTaskRowVersion: rowVersionSchema,
    note: z.string().trim().min(1).max(512).optional(),
    revisionId: z.uuid(),
  })
  .strict();

export const taskRevisionRiskAcceptanceSchema = z
  .object({
    acceptedAt: z.iso.datetime({ offset: true }),
    acceptedByUserId: z.uuid(),
    id: z.uuid(),
    note: z.string().nullable(),
    revisionId: z.uuid(),
    taskId: z.uuid(),
  })
  .strict();

export type AcceptTaskRevisionRiskRequest = z.infer<typeof acceptTaskRevisionRiskRequestSchema>;
export type TaskRevisionRiskAcceptance = z.infer<typeof taskRevisionRiskAcceptanceSchema>;
