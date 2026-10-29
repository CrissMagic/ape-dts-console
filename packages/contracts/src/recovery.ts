import { z } from 'zod';
import { taskTypeSchema } from './tasks/index.js';

export const recoveryStepTypeSchema = z.enum([
  'LOAD_METADATA',
  'UNLOCK_KEYS',
  'ENSURE_ARTIFACTS',
  'VERIFY_CONTINUITY',
  'PLAN_CAPACITY',
  'RESTORE_BATCHES',
  'READY_OBSERVABILITY',
]);
export const recoveryStepStatusSchema = z.enum([
  'PENDING',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
  'PAUSED',
]);
export const recoverySessionStateSchema = z.enum(['RUNNING', 'READY', 'FAILED', 'PAUSED']);

export const recoveryCandidateSchema = z
  .object({
    revisionId: z.uuid(),
    taskId: z.uuid(),
    taskName: z.string().min(1),
    taskType: taskTypeSchema,
    unitKeys: z.array(z.string().min(1)),
    units: z.number().int().nonnegative(),
  })
  .strict();

export const recoveryStepSchema = z
  .object({
    attemptCount: z.number().int().nonnegative(),
    errorSummary: z.string().nullable(),
    failureCode: z.string().nullable(),
    position: z.number().int().positive(),
    progress: z.record(z.string(), z.unknown()),
    status: recoveryStepStatusSchema,
    stepType: recoveryStepTypeSchema,
  })
  .strict();

export const recoverySessionSchema = z
  .object({
    candidates: z.array(recoveryCandidateSchema),
    id: z.uuid(),
    rowVersion: z.string().regex(/^[1-9]\d*$/u),
    state: recoverySessionStateSchema,
    steps: z.array(recoveryStepSchema),
  })
  .strict();

export const retryRecoveryStepRequestSchema = z
  .object({
    stepType: recoveryStepTypeSchema.exclude(['LOAD_METADATA', 'UNLOCK_KEYS']),
  })
  .strict();

export const selectRecoveryTasksRequestSchema = z
  .object({
    expectedSessionRowVersion: z.string().regex(/^[1-9]\d*$/u),
    taskIds: z.array(z.uuid()).min(1),
  })
  .strict();

export const continueRecoveryRequestSchema = z
  .object({
    expectedSessionRowVersion: z.string().regex(/^[1-9]\d*$/u),
  })
  .strict();

export const confirmRecoveryOldRuntimeStoppedRequestSchema = z
  .object({
    expectedSessionRowVersion: z.string().regex(/^[1-9]\d*$/u),
    reason: z.string().trim().min(1).max(1000),
  })
  .strict();

export const recoverySessionParametersSchema = z.object({ sessionId: z.uuid() }).strict();

export type RecoveryCandidate = z.infer<typeof recoveryCandidateSchema>;
export type RecoverySession = z.infer<typeof recoverySessionSchema>;
export type RecoverySessionState = z.infer<typeof recoverySessionStateSchema>;
export type RecoveryStepStatus = z.infer<typeof recoveryStepStatusSchema>;
export type RecoveryStepType = z.infer<typeof recoveryStepTypeSchema>;
