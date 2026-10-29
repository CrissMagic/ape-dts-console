import { z } from 'zod';

export const runtimeRecoveryPolicySchema = z
  .object({
    enabled: z.boolean(),
    maxAttempts: z.number().int().min(1).max(10),
    retryDelaySeconds: z.number().int().min(5).max(3600),
  })
  .strict();
export const runtimeRecoverySettingsSchema = runtimeRecoveryPolicySchema.extend({
  rowVersion: z.string().regex(/^[1-9]\d*$/u),
});
export const updateRuntimeRecoverySettingsSchema = runtimeRecoveryPolicySchema.extend({
  expectedRowVersion: z.string().regex(/^[1-9]\d*$/u),
});
export type RuntimeRecoveryPolicy = z.infer<typeof runtimeRecoveryPolicySchema>;
export type RuntimeRecoverySettings = z.infer<typeof runtimeRecoverySettingsSchema>;
export type UpdateRuntimeRecoverySettings = z.infer<typeof updateRuntimeRecoverySettingsSchema>;
