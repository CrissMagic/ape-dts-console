import { z } from 'zod';
import { dataSourceConfigurationSchema } from './data-sources/index.js';
import { taskTypeSchema } from './tasks/index.js';
import { updateAlertSettingsRequestSchema } from './alerts.js';
import { runtimeRecoveryPolicySchema } from './runtime-recovery.js';

export const CONFIG_BUNDLE_MAX_BYTES = 512 * 1024;
const name = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(/^[\p{L}\p{N}][\p{L}\p{N} ._:-]*$/u);
export const configExportOptionsSchema = z
  .object({
    dataSources: z.boolean(),
    tasks: z.boolean(),
    system: z.boolean(),
  })
  .strict()
  .refine((value) => value.dataSources || value.tasks || value.system);
export const configSystemSchema = z
  .object({
    maxRuntimeUnits: z.number().int().min(1).max(2147483647).nullable(),
    recovery: runtimeRecoveryPolicySchema.optional(),
    sessions: z
      .object({
        idleTimeoutMinutes: z.number().int().min(15).max(10080),
        absoluteTimeoutHours: z.number().int().min(1).max(720),
      })
      .strict()
      .refine((value) => value.idleTimeoutMinutes <= value.absoluteTimeoutHours * 60),
    alerts: updateAlertSettingsRequestSchema.omit({
      expectedRowVersion: true,
      webhookUrl: true,
      enabled: true,
    }),
  })
  .strict();
export const configBundleSchema = z
  .object({
    format: z.literal('scs-platform-config'),
    version: z.literal(1),
    exportedAt: z.iso.datetime(),
    dataSources: z
      .array(
        z.object({ id: z.uuid(), name, configuration: dataSourceConfigurationSchema }).strict(),
      )
      .max(500),
    groups: z.array(z.object({ id: z.uuid(), name }).strict()).max(500),
    tasks: z
      .array(
        z
          .object({
            name,
            taskType: taskTypeSchema,
            sourceId: z.uuid(),
            targetId: z.uuid(),
            groupId: z.uuid().nullable(),
            configuration: z.record(z.string(), z.unknown()),
            artifact: z
              .object({ version: z.string().max(128), sha256: z.string().regex(/^[a-f0-9]{64}$/u) })
              .strict()
              .nullable(),
          })
          .strict(),
      )
      .max(1000),
    system: configSystemSchema.nullable(),
  })
  .strict();
export const configImportPreviewSchema = z
  .object({
    previewHash: z.string().regex(/^[a-f0-9]{64}$/u),
    dataSources: z.number().int(),
    tasks: z.number().int(),
    groups: z.number().int(),
    system: z.boolean(),
    conflicts: z.array(z.string()),
    warnings: z.array(z.string()),
  })
  .strict();
export const configImportRequestSchema = z
  .object({
    bundle: configBundleSchema,
    expectedPreviewHash: z.string().regex(/^[a-f0-9]{64}$/u),
  })
  .strict();
export const configImportResultSchema = z
  .object({ dataSources: z.number().int(), tasks: z.number().int() })
  .strict();
export type ConfigBundle = z.infer<typeof configBundleSchema>;
export type ConfigExportOptions = z.infer<typeof configExportOptionsSchema>;
export type ConfigImportPreview = z.infer<typeof configImportPreviewSchema>;
