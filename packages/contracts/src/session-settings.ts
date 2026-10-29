import { z } from 'zod';

const policyShape = {
  idleTimeoutMinutes: z.number().int().min(15).max(10_080),
  absoluteTimeoutHours: z.number().int().min(1).max(720),
};
const withinAbsoluteLifetime = (value: {
  idleTimeoutMinutes: number;
  absoluteTimeoutHours: number;
}) => value.idleTimeoutMinutes <= value.absoluteTimeoutHours * 60;
const lifetimeIssue = {
  message: '空闲超时不能超过最长登录时长。',
  path: ['idleTimeoutMinutes'],
};

export const sessionSettingsSchema = z
  .object({
    ...policyShape,
    rowVersion: z.string().regex(/^[1-9]\d*$/u),
  })
  .strict()
  .refine(withinAbsoluteLifetime, lifetimeIssue);

export const updateSessionSettingsRequestSchema = z
  .object({
    ...policyShape,
    expectedRowVersion: z
      .string()
      .refine(
        (value) => /^[1-9]\d{0,18}$/u.test(value) && BigInt(value) <= 9_223_372_036_854_775_807n,
      ),
  })
  .strict()
  .refine(withinAbsoluteLifetime, lifetimeIssue);

export type SessionSettings = z.infer<typeof sessionSettingsSchema>;
export type UpdateSessionSettingsRequest = z.infer<typeof updateSessionSettingsRequestSchema>;
