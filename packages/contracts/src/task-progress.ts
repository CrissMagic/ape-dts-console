import { z } from 'zod';

export const taskProgressSchema = z.object({
  taskId: z.uuid(),
  revisionId: z.uuid(),
  collectedAt: z.iso.datetime(),
  status: z.enum(['AVAILABLE', 'READ_FAILED', 'UNAVAILABLE']),
  sourceType: z.enum(['POSTGRES', 'KAFKA', 'MYSQL']),
  mysqlBinlogFilename: z.string().nullable().optional(),
  mysqlBinlogPosition: z.string().regex(/^\d+$/u).nullable().optional(),
  mysqlCheckpointAt: z.iso.datetime().nullable().optional(),
  currentStage: z.string().nullable().optional(),
  replicationLagMilliseconds: z.number().nonnegative().nullable().optional(),
  slotStatus: z.enum(['ACTIVE', 'INACTIVE', 'MISSING']).nullable(),
  unconfirmedWalBytes: z.string().regex(/^\d+$/u).nullable(),
  retainedWalBytes: z.string().regex(/^\d+$/u).nullable(),
  offsetLag: z.string().regex(/^\d+$/u).nullable(),
  timeDelayMilliseconds: z.number().nonnegative().nullable(),
  timeDelayStatus: z.literal('UNSUPPORTED'),
});
export type TaskProgress = z.infer<typeof taskProgressSchema>;
