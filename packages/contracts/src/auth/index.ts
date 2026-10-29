import { z } from 'zod';

import { userSummarySchema } from '../users/index.js';

const passwordSchema = z.string().min(12).max(128);
const usernameSchema = z
  .string()
  .trim()
  .min(3)
  .max(64)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u);
const opaqueTokenSchema = z.string().min(32).max(256);

export const loginRequestSchema = z
  .object({
    password: passwordSchema,
    username: usernameSchema,
  })
  .strict();

export const authenticatedSessionResponseSchema = z
  .object({
    absoluteExpiresAt: z.iso.datetime({ offset: true }),
    csrfToken: opaqueTokenSchema,
    idleExpiresAt: z.iso.datetime({ offset: true }),
    mustChangePassword: z.boolean().default(false),
    status: z.literal('AUTHENTICATED'),
    user: userSummarySchema,
  })
  .strict();

export const loginResponseSchema = z.discriminatedUnion('status', [
  authenticatedSessionResponseSchema,
  z
    .object({
      challengeExpiresAt: z.iso.datetime({ offset: true }),
      challengeToken: opaqueTokenSchema,
      status: z.literal('TOTP_REQUIRED'),
    })
    .strict(),
]);

export const completeLoginRequestSchema = z.discriminatedUnion('method', [
  z
    .object({
      challengeToken: opaqueTokenSchema,
      code: z.string().regex(/^\d{6}$/u),
      method: z.literal('TOTP'),
    })
    .strict(),
  z
    .object({
      challengeToken: opaqueTokenSchema,
      code: z.string().regex(/^scs-rc-v1_[A-Za-z0-9_-]{22}$/u),
      method: z.literal('RECOVERY_CODE'),
    })
    .strict(),
]);

export const changePasswordRequestSchema = z
  .object({
    currentPassword: passwordSchema,
    newPassword: passwordSchema,
  })
  .strict();
export type ChangePasswordRequest = z.infer<typeof changePasswordRequestSchema>;

export type AuthenticatedSessionResponse = z.infer<typeof authenticatedSessionResponseSchema>;
export type CompleteLoginRequest = z.infer<typeof completeLoginRequestSchema>;
export type LoginRequest = z.infer<typeof loginRequestSchema>;
export type LoginResponse = z.infer<typeof loginResponseSchema>;
