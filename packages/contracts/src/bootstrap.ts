import { z } from 'zod';

export const bootstrapPhaseSchema = z.enum([
  'SEALED',
  'DB_CONFIGURATION_ALLOWED',
  'SCHEMA_ABSENT',
  'SCHEMA_CURRENT',
  'MIGRATION_REQUIRED',
  'BLOCKED_NEWER_SCHEMA',
  'BLOCKED_CORRUPT_SCHEMA',
  'INITIALIZING',
  'MIGRATING',
  'TOTP_ENROLLMENT_REQUIRED',
  'UNLOCKING',
  'RECOVERING',
  'BLOCKED_CONCURRENT_INSTANCE',
  'READY',
]);

export const bootstrapStatusResponseSchema = z
  .object({
    phase: bootstrapPhaseSchema,
    platformInstanceId: z.uuid(),
    startupIssue: z.string().optional(),
  })
  .strict();

export const bootstrapCodeVerificationRequestSchema = z
  .object({ initializationCode: z.string().trim().min(1).max(64) })
  .strict();

export const bootstrapCodeVerificationResponseSchema = z
  .object({
    expiresAt: z.iso.datetime(),
    sessionToken: z.string().regex(/^[A-Za-z0-9_-]{43}$/u),
  })
  .strict();

export const postgresTlsConfigSchema = z.discriminatedUnion('mode', [
  z.object({ mode: z.literal('DISABLE') }).strict(),
  z.object({ mode: z.literal('REQUIRE') }).strict(),
  z
    .object({
      caCertificate: z
        .string()
        .trim()
        .min(1)
        .max(64 * 1_024),
      mode: z.literal('VERIFY_FULL'),
    })
    .strict(),
]);

export const postgresConnectionConfigSchema = z
  .object({
    database: z.string().trim().min(1).max(128),
    host: z.string().trim().min(1).max(253),
    password: z.string().min(1).max(4_096),
    port: z.number().int().min(1).max(65_535),
    tls: postgresTlsConfigSchema,
    username: z.string().trim().min(1).max(128),
  })
  .strict();

export const inspectBootstrapDatabaseRequestSchema = z
  .object({ connection: postgresConnectionConfigSchema })
  .strict();

export const schemaInspectionStateSchema = z.enum([
  'ABSENT',
  'CURRENT',
  'OLDER',
  'NEWER',
  'CORRUPT',
]);

export const inspectBootstrapDatabaseResponseSchema = z
  .object({
    appliedVersion: z.number().int().positive().nullable(),
    issues: z.array(z.string()),
    pendingMigrations: z.array(
      z
        .object({
          description: z.string(),
          platformVersion: z.string(),
          version: z.number().int().positive(),
        })
        .strict(),
    ),
    phase: bootstrapPhaseSchema,
    state: schemaInspectionStateSchema,
    targetVersion: z.number().int().positive(),
  })
  .strict();

export const initializePlatformRequestSchema = z
  .object({
    administrator: z
      .object({
        displayName: z.string().trim().min(1).max(128),
        password: z.string().min(12).max(128),
        username: z
          .string()
          .trim()
          .min(3)
          .max(64)
          .regex(/^[\p{L}\p{N}._-]+$/u),
      })
      .strict(),
    unlockPassphrase: z.string().min(12).max(256),
  })
  .strict();

export const initializePlatformResponseSchema = z
  .object({
    administratorId: z.uuid(),
    phase: z.literal('TOTP_ENROLLMENT_REQUIRED'),
    recoveryKey: z.string().regex(/^scs-rk-v1_[A-Za-z0-9_-]{43}$/u),
    totpUri: z.string().startsWith('otpauth://totp/'),
  })
  .strict();

export const verifyInitialTotpRequestSchema = z
  .object({ code: z.string().regex(/^\d{6}$/u) })
  .strict();

export const readyBootstrapResponseSchema = z.object({ phase: z.literal('READY') }).strict();

export const verifyInitialTotpResponseSchema = z
  .object({
    phase: z.literal('READY'),
    recoveryCodes: z.array(z.string().regex(/^scs-rc-v1_[A-Za-z0-9_-]{22}$/u)).min(1),
  })
  .strict();

export const unlockPlatformRequestSchema = z
  .object({ unlockPassphrase: z.string().min(1).max(256) })
  .strict();

export const unlockPlatformResponseSchema = z
  .object({
    phase: z.enum(['RECOVERING', 'READY']),
    recoverySessionId: z.uuid(),
    recoveryState: z.enum(['RUNNING', 'READY', 'FAILED', 'PAUSED']),
  })
  .strict();

export const confirmMigrationRequestSchema = z
  .object({ backupConfirmed: z.literal(true) })
  .strict();

export type BootstrapPhase = z.infer<typeof bootstrapPhaseSchema>;
export type BootstrapStatusResponse = z.infer<typeof bootstrapStatusResponseSchema>;
export type ConfirmMigrationRequest = z.infer<typeof confirmMigrationRequestSchema>;
export type InitializePlatformRequest = z.infer<typeof initializePlatformRequestSchema>;
export type InitializePlatformResponse = z.infer<typeof initializePlatformResponseSchema>;
export type InspectBootstrapDatabaseRequest = z.infer<typeof inspectBootstrapDatabaseRequestSchema>;
export type InspectBootstrapDatabaseResponse = z.infer<
  typeof inspectBootstrapDatabaseResponseSchema
>;
export type PostgresConnectionConfig = z.infer<typeof postgresConnectionConfigSchema>;
export type UnlockPlatformRequest = z.infer<typeof unlockPlatformRequestSchema>;
export type UnlockPlatformResponse = z.infer<typeof unlockPlatformResponseSchema>;
export type VerifyInitialTotpRequest = z.infer<typeof verifyInitialTotpRequestSchema>;
export type VerifyInitialTotpResponse = z.infer<typeof verifyInitialTotpResponseSchema>;
