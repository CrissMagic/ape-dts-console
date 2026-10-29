import { z } from 'zod';

export const userRoleSchema = z.enum(['ADMIN', 'DEVELOPER']);
export const userStatusSchema = z.enum([
  'INVITED',
  'PASSWORD_REQUIRED',
  'TOTP_REQUIRED',
  'ACTIVE',
  'DISABLED',
]);

const usernameSchema = z
  .string()
  .trim()
  .min(3)
  .max(64)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u);

export const userSummarySchema = z
  .object({
    displayName: z.string().min(1).max(128),
    id: z.uuid(),
    role: userRoleSchema,
    status: userStatusSchema,
    username: z.string().min(1).max(64),
  })
  .strict();

export const createUserRequestSchema = z
  .object({
    displayName: z.string().trim().min(1).max(128),
    role: userRoleSchema,
    username: usernameSchema,
  })
  .strict();

export const activationGrantResponseSchema = z
  .object({
    activationCode: z.string().regex(/^scs-at-v1_[A-Za-z0-9_-]{43}$/u),
    expiresAt: z.iso.datetime({ offset: true }),
    user: userSummarySchema,
  })
  .strict();

export const activateUserRequestSchema = z
  .object({
    activationCode: z.string().regex(/^scs-at-v1_[A-Za-z0-9_-]{43}$/u),
    password: z.string().min(12).max(128),
    username: usernameSchema,
  })
  .strict();

export const activateUserResponseSchema = z.discriminatedUnion('status', [
  z
    .object({
      status: z.literal('ACTIVE'),
      user: userSummarySchema,
    })
    .strict(),
  z
    .object({
      challengeExpiresAt: z.iso.datetime({ offset: true }),
      challengeToken: z.string().min(32).max(256),
      recoveryCodes: z.array(z.string().regex(/^scs-rc-v1_[A-Za-z0-9_-]{22}$/u)).min(1),
      status: z.literal('TOTP_ENROLLMENT_REQUIRED'),
      totpUri: z.string().startsWith('otpauth://totp/'),
      user: userSummarySchema,
    })
    .strict(),
]);

export const confirmTotpEnrollmentRequestSchema = z
  .object({
    challengeToken: z.string().min(32).max(256),
    code: z.string().regex(/^\d{6}$/u),
  })
  .strict();

export const changeUserRoleRequestSchema = z.object({ role: userRoleSchema }).strict();

export const changeUserStatusRequestSchema = z
  .object({ status: z.enum(['ACTIVE', 'DISABLED']) })
  .strict();

export const userListResponseSchema = z.object({ users: z.array(userSummarySchema) }).strict();

export const userMutationResponseSchema = z.object({ user: userSummarySchema }).strict();

export const userIdParametersSchema = z.object({ userId: z.uuid() }).strict();

export type ActivateUserRequest = z.infer<typeof activateUserRequestSchema>;
export type ActivateUserResponse = z.infer<typeof activateUserResponseSchema>;
export type ActivationGrantResponse = z.infer<typeof activationGrantResponseSchema>;
export type ChangeUserRoleRequest = z.infer<typeof changeUserRoleRequestSchema>;
export type ChangeUserStatusRequest = z.infer<typeof changeUserStatusRequestSchema>;
export type CreateUserRequest = z.infer<typeof createUserRequestSchema>;
export type UserListResponse = z.infer<typeof userListResponseSchema>;
export type UserMutationResponse = z.infer<typeof userMutationResponseSchema>;
export type UserRole = z.infer<typeof userRoleSchema>;
export type UserStatus = z.infer<typeof userStatusSchema>;
export type UserSummary = z.infer<typeof userSummarySchema>;
