import { z } from 'zod';

export const preparePgResourcesRequestSchema = z
  .object({
    revisionId: z.uuid(),
    expectedTaskRowVersion: z.string().regex(/^[1-9]\d*$/u),
    expectedDraftVersion: z.string().regex(/^[1-9]\d*$/u),
    acknowledgeReplicationRisk: z.literal(true),
  })
  .strict();

const preparedResourceSchema = z.object({
  name: z.string(),
  created: z.boolean(),
  stableIdentity: z.string(),
});
export const preparePgResourcesResponseSchema = z.object({
  taskId: z.uuid(),
  revisionId: z.uuid(),
  databaseName: z.string(),
  resourceMode: z.enum(['SELF_MANAGED', 'PLATFORM_MANAGED']),
  slot: preparedResourceSchema,
  publication: preparedResourceSchema.extend({
    extendedTables: z.array(z.object({ schema: z.string(), name: z.string() })).optional(),
  }),
  requiresPreflight: z.literal(true),
});
export type PreparePgResourcesRequest = z.infer<typeof preparePgResourcesRequestSchema>;
export type PreparePgResourcesResponse = z.infer<typeof preparePgResourcesResponseSchema>;
