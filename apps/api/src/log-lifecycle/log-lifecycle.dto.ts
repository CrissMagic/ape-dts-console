import { z } from 'zod';

export const logLifecycleSettingKeySchema = z.enum([
  'logRetentionDays',
  'runLogLimitBytes',
  'logFailureBufferBytes',
  'runRetentionDays',
]);

const baseSettingMutationSchema = z
  .object({
    key: logLifecycleSettingKeySchema,
    rowVersion: z.string().regex(/^[1-9]\d*$/u),
    value: z.number().int().positive().safe(),
  })
  .strict();

export const previewLogLifecycleSettingSchema = baseSettingMutationSchema;
export const updateLogLifecycleSettingSchema = baseSettingMutationSchema
  .extend({ confirmationToken: z.string().min(1).optional() })
  .strict();
