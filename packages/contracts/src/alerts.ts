export interface AlertSettings {
  channel: 'WECOM';
  enabled: boolean;
  configured: boolean;
  notifyRecovery: boolean;
  durationSeconds: number;
  repeatSeconds: number;
  slotInactive: boolean;
  slotMissing: boolean;
  retainedWalBytes: string | null;
  unconfirmedWalBytes: string | null;
  kafkaOffsetLag: string | null;
  delayMilliseconds: number | null;
  rowVersion: string;
}

export interface UpdateAlertSettingsRequest extends Omit<
  AlertSettings,
  'configured' | 'rowVersion'
> {
  expectedRowVersion: string;
  webhookUrl?: string | undefined;
}

import { z } from 'zod';

const byteThreshold = z
  .string()
  .regex(/^\d{1,30}$/u)
  .nullable();
const rules = {
  channel: z.literal('WECOM'),
  enabled: z.boolean(),
  notifyRecovery: z.boolean(),
  durationSeconds: z.number().int().min(0).max(86400),
  repeatSeconds: z.number().int().min(30).max(604800),
  slotInactive: z.boolean(),
  slotMissing: z.boolean(),
  retainedWalBytes: byteThreshold,
  unconfirmedWalBytes: byteThreshold,
  kafkaOffsetLag: byteThreshold,
  delayMilliseconds: z.number().int().min(1).max(604800000).nullable(),
};
export const alertSettingsSchema = z
  .object({ ...rules, configured: z.boolean(), rowVersion: z.string().regex(/^[1-9]\d*$/u) })
  .strict();
export const updateAlertSettingsRequestSchema = z
  .object({
    ...rules,
    expectedRowVersion: z.string().regex(/^[1-9]\d{0,17}$/u),
    webhookUrl: z.string().max(1024).optional(),
  })
  .strict();
export const alertDeliverySchema = z
  .object({
    id: z.string().uuid(),
    taskId: z.string().uuid().nullable(),
    taskName: z.string().nullable().optional(),
    faultCode: z.string().nullable().optional(),
    rule: z.string(),
    event: z.enum(['ALARM', 'RECOVERY', 'TEST']),
    status: z.enum(['SENT', 'FAILED', 'PENDING']),
    failureCode: z.string().nullable(),
    createdAt: z.string(),
  })
  .strict();
export const alertDeliveryListSchema = z.object({ items: z.array(alertDeliverySchema) }).strict();

export const alertDeliveryContextSchema = z
  .object({
    delivery: alertDeliverySchema,
    association: z.enum(['AUDIT', 'TIME', 'NONE']),
    run: z
      .object({
        id: z.uuid(),
        status: z.string(),
        stage: z.string(),
        failureCode: z.string().nullable(),
        startedAt: z.string(),
        endedAt: z.string().nullable(),
      })
      .nullable(),
    units: z.array(
      z.object({
        unitKey: z.string(),
        status: z.string(),
        exitCode: z.number().nullable(),
        signal: z.string().nullable(),
        failureCode: z.string().nullable(),
      }),
    ),
    events: z.array(
      z.object({ occurredAt: z.string(), action: z.string(), reason: z.string().nullable() }),
    ),
    preflightChecks: z.array(z.object({ key: z.string(), code: z.string(), summary: z.string() })),
    logWindow: z.object({ from: z.string(), to: z.string() }).nullable(),
  })
  .strict();
export type AlertDeliveryContext = z.infer<typeof alertDeliveryContextSchema>;

export interface AlertDelivery {
  id: string;
  taskId: string | null;
  taskName?: string | null | undefined;
  faultCode?: string | null | undefined;
  rule: string;
  event: 'ALARM' | 'RECOVERY' | 'TEST';
  status: 'SENT' | 'FAILED' | 'PENDING';
  failureCode: string | null;
  createdAt: string;
}
