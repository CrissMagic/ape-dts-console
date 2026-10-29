import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';
import type {
  AuthenticatedSessionResponse,
  ChangePasswordRequest,
  CompleteLoginRequest,
  LoginRequest,
  LoginResponse,
  UserRole,
  UserStatus,
  UserSummary,
} from '@scs/contracts';
import {
  generateOpaqueToken,
  hashOpaqueToken,
  hashPassword,
  validateArgon2idParameters,
  verifyOpaqueToken,
  verifyPassword,
  type Argon2idParameters,
  type PasswordHashRecord,
} from '@scs/security';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { ARGON2ID_POLICY } from '../security/security-config.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { SessionSettingsService } from './session-settings.service.js';
import {
  authenticationFingerprint,
  nextAuthenticationFailure,
  sessionDeadlines,
} from './auth-policy.js';

export type AuthServiceErrorCode =
  | 'AUTH_METHOD_RETIRED'
  | 'AUTHENTICATION_FAILED'
  | 'CURRENT_PASSWORD_INVALID'
  | 'PASSWORD_UNCHANGED'
  | 'AUTHENTICATION_RATE_LIMITED'
  | 'CSRF_INVALID'
  | 'SECURITY_POLICY_UNAVAILABLE'
  | 'SESSION_INVALID';

export class AuthServiceError extends Error {
  constructor(
    readonly code: AuthServiceErrorCode,
    readonly retryAfterMs: number | null = null,
  ) {
    super(code);
    this.name = 'AuthServiceError';
  }
}

export interface AuthenticatedRequestContext {
  readonly mustChangePassword?: boolean;
  readonly absoluteExpiresAt: Date;
  readonly csrfTokenHash: Buffer;
  readonly idleExpiresAt: Date;
  readonly sessionGeneration: string;
  readonly sessionId: string;
  readonly sourceFingerprint: Buffer;
  readonly user: UserSummary;
}

export interface AuthenticatedHttpResult {
  readonly response: AuthenticatedSessionResponse;
  readonly sessionToken: string;
}

export interface LoginHttpResult {
  readonly response: LoginResponse;
  readonly sessionToken: string | null;
}

interface SessionResolution {
  readonly context: AuthenticatedRequestContext;
  readonly csrfToken: string | null;
}

type RateLimitFactor = 'PASSWORD' | 'RECOVERY_CODE' | 'TOTP';

interface UserAuthRow extends QueryResultRow {
  must_change_password: boolean;
  display_name: string;
  id: string;
  last_accepted_totp_counter: string | null;
  normalized_username: string;
  password_hash: Buffer | null;
  password_kdf_algorithm: string | null;
  password_kdf_parameters: unknown | null;
  password_salt: Buffer | null;
  role: UserRole;
  session_generation: string;
  status: UserStatus;
  totp_secret_id: string | null;
}

interface SessionRow extends UserAuthRow {
  idle_timeout_minutes: number;
  absolute_expires_at: Date;
  csrf_token_hash: Buffer;
  idle_expires_at: Date;
  persisted_session_generation: string;
  revoked_at: Date | null;
  session_id: string;
  session_source_fingerprint: Buffer;
}

interface RateLimitRow extends QueryResultRow {
  blocked_until: Date | null;
  failure_count: number;
  window_started_at: Date;
}

function normalizeUsername(username: string): string {
  return username.normalize('NFKC').toLocaleLowerCase('en-US');
}

function userSummary(row: UserAuthRow): UserSummary {
  return {
    displayName: row.display_name,
    id: row.id,
    role: row.role,
    status: row.status,
    username: row.normalized_username,
  };
}

function passwordRecord(
  row: Pick<
    UserAuthRow,
    'password_hash' | 'password_kdf_algorithm' | 'password_kdf_parameters' | 'password_salt'
  >,
): PasswordHashRecord | null {
  if (
    row.password_kdf_algorithm !== 'ARGON2ID' ||
    row.password_hash === null ||
    row.password_kdf_parameters === null ||
    row.password_salt === null
  ) {
    return null;
  }
  const parameters = row.password_kdf_parameters as Argon2idParameters;
  validateArgon2idParameters(parameters);
  return {
    algorithm: 'ARGON2ID',
    hash: row.password_hash.toString('base64'),
    parameters,
    salt: row.password_salt.toString('base64'),
  };
}

@Injectable()
export class AuthService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    _unlockedKey: UnlockedKeyService,
    @Inject(ARGON2ID_POLICY)
    private readonly argon2idPolicy: Argon2idParameters | null,
    private readonly sessionSettings: SessionSettingsService,
  ) {}

  private requirePolicy(): Argon2idParameters {
    if (this.argon2idPolicy === null) {
      throw new AuthServiceError('SECURITY_POLICY_UNAVAILABLE');
    }
    return this.argon2idPolicy;
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async assertRateLimitAllowed(
    client: PoolClient,
    accountFingerprint: Buffer,
    sourceFingerprint: Buffer,
    factor: RateLimitFactor,
    now: Date,
  ): Promise<void> {
    const result = await client.query<RateLimitRow>(
      `SELECT failure_count, window_started_at, blocked_until
       FROM ape_dts_platform.authentication_rate_limits
       WHERE account_fingerprint = $1
         AND source_fingerprint = $2
         AND factor = $3
       FOR UPDATE`,
      [accountFingerprint, sourceFingerprint, factor],
    );
    const blockedUntil = result.rows[0]?.blocked_until;
    if (blockedUntil !== undefined && blockedUntil !== null && blockedUntil > now) {
      throw new AuthServiceError(
        'AUTHENTICATION_RATE_LIMITED',
        blockedUntil.getTime() - now.getTime(),
      );
    }
  }

  private async recordAuthenticationFailure(
    client: PoolClient,
    input: {
      readonly accountFingerprint: Buffer;
      readonly factor: RateLimitFactor;
      readonly sourceFingerprint: Buffer;
      readonly subjectUserId: string | null;
    },
    now: Date,
  ): Promise<void> {
    const previous = await client.query<RateLimitRow>(
      `SELECT failure_count, window_started_at, blocked_until
       FROM ape_dts_platform.authentication_rate_limits
       WHERE account_fingerprint = $1
         AND source_fingerprint = $2
         AND factor = $3
       FOR UPDATE`,
      [input.accountFingerprint, input.sourceFingerprint, input.factor],
    );
    const row = previous.rows[0];
    const next = nextAuthenticationFailure(
      row?.failure_count ?? 0,
      row?.window_started_at ?? now,
      now,
    );
    await client.query(
      `INSERT INTO ape_dts_platform.authentication_rate_limits (
         account_fingerprint, source_fingerprint, factor, failure_count,
         window_started_at, last_failed_at, blocked_until
       ) VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (account_fingerprint, source_fingerprint, factor)
       DO UPDATE SET
         failure_count = EXCLUDED.failure_count,
         window_started_at = EXCLUDED.window_started_at,
         last_failed_at = EXCLUDED.last_failed_at,
         blocked_until = EXCLUDED.blocked_until`,
      [
        input.accountFingerprint,
        input.sourceFingerprint,
        input.factor,
        next.failureCount,
        next.windowStartedAt,
        now,
        next.blockedUntil,
      ],
    );
    await this.audit.append(client, {
      action: 'AUTHENTICATION_FAILED',
      actorType: 'SYSTEM',
      actorUserId: null,
      details: {
        factor: input.factor,
        sourceFingerprint: input.sourceFingerprint.toString('hex'),
      },
      subjectId: input.subjectUserId,
      subjectType: 'USER',
    });
  }

  private async clearRateLimit(
    client: PoolClient,
    accountFingerprint: Buffer,
    sourceFingerprint: Buffer,
    factor: RateLimitFactor,
  ): Promise<void> {
    await client.query(
      `DELETE FROM ape_dts_platform.authentication_rate_limits
       WHERE account_fingerprint = $1
         AND source_fingerprint = $2
         AND factor = $3`,
      [accountFingerprint, sourceFingerprint, factor],
    );
  }

  private async loadUserByNormalizedUsername(
    normalizedUsername: string,
  ): Promise<UserAuthRow | undefined> {
    const result = await this.database.requirePool().query<UserAuthRow>(
      `SELECT
         id, normalized_username, display_name, role, status, must_change_password,
         password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
         totp_secret_id, session_generation::text, last_accepted_totp_counter::text
       FROM ape_dts_platform.users
       WHERE normalized_username = $1`,
      [normalizedUsername],
    );
    return result.rows[0];
  }

  private async verifySubmittedPassword(
    submittedPassword: string,
    row: UserAuthRow | undefined,
  ): Promise<boolean> {
    const policy = this.requirePolicy();
    let storedRecord: PasswordHashRecord | null;
    try {
      storedRecord = row === undefined ? null : passwordRecord(row);
    } catch {
      throw new AuthServiceError('SECURITY_POLICY_UNAVAILABLE');
    }
    const record: PasswordHashRecord = storedRecord ?? {
      algorithm: 'ARGON2ID',
      hash: Buffer.alloc(policy.tagLength).toString('base64'),
      parameters: policy,
      salt: Buffer.alloc(16).toString('base64'),
    };
    try {
      return await verifyPassword(submittedPassword, record);
    } catch {
      throw new AuthServiceError('SECURITY_POLICY_UNAVAILABLE');
    }
  }

  private async createSession(
    client: PoolClient,
    row: UserAuthRow,
    sourceFingerprint: Buffer,
    now: Date,
  ): Promise<AuthenticatedHttpResult> {
    const sessionId = randomUUID();
    const sessionToken = generateOpaqueToken('scs-s-v1_', 32);
    const csrfToken = generateOpaqueToken('scs-csrf-v1_', 32);
    const policy = await this.sessionSettings.getWithin(client);
    const deadlines = sessionDeadlines(now, policy);
    await client.query(
      `INSERT INTO ape_dts_platform.auth_sessions (
         id, user_id, token_hash, csrf_token_hash, source_fingerprint,
         session_generation, created_at, last_seen_at, idle_expires_at,
         absolute_expires_at, reauthenticated_at, idle_timeout_minutes
       ) VALUES (
         $1::uuid, $2::uuid, $3, $4, $5, $6::bigint, $7, $7, $8, $9, $7, $10
       )`,
      [
        sessionId,
        row.id,
        hashOpaqueToken(sessionToken),
        hashOpaqueToken(csrfToken),
        sourceFingerprint,
        row.session_generation,
        now,
        deadlines.idleExpiresAt,
        deadlines.absoluteExpiresAt,
        policy.idleTimeoutMinutes,
      ],
    );
    return {
      response: {
        absoluteExpiresAt: deadlines.absoluteExpiresAt.toISOString(),
        csrfToken,
        idleExpiresAt: deadlines.idleExpiresAt.toISOString(),
        mustChangePassword: row.must_change_password,
        status: 'AUTHENTICATED',
        user: userSummary(row),
      },
      sessionToken,
    };
  }

  async login(request: LoginRequest, source: string): Promise<LoginHttpResult> {
    const now = new Date();
    const normalizedUsername = normalizeUsername(request.username);
    const accountFingerprint = authenticationFingerprint(normalizedUsername);
    const sourceFingerprint = authenticationFingerprint(source);
    await this.transaction(async (client) => {
      await this.assertRateLimitAllowed(
        client,
        accountFingerprint,
        sourceFingerprint,
        'PASSWORD',
        now,
      );
    });

    const candidate = await this.loadUserByNormalizedUsername(normalizedUsername);
    const passwordAccepted = await this.verifySubmittedPassword(request.password, candidate);
    if (!passwordAccepted || candidate === undefined || candidate.status !== 'ACTIVE') {
      await this.transaction(async (client) => {
        await this.recordAuthenticationFailure(
          client,
          {
            accountFingerprint,
            factor: 'PASSWORD',
            sourceFingerprint,
            subjectUserId: candidate?.id ?? null,
          },
          now,
        );
      });
      throw new AuthServiceError('AUTHENTICATION_FAILED');
    }

    const result = await this.transaction<LoginHttpResult | null>(async (client) => {
      const locked = await client.query<UserAuthRow>(
        `SELECT
           id, normalized_username, display_name, role, status, must_change_password,
           password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
           totp_secret_id, session_generation::text, last_accepted_totp_counter::text
         FROM ape_dts_platform.users
         WHERE id = $1::uuid
         FOR UPDATE`,
        [candidate.id],
      );
      const current = locked.rows[0];
      if (
        current === undefined ||
        current.status !== 'ACTIVE' ||
        current.session_generation !== candidate.session_generation
      ) {
        await this.recordAuthenticationFailure(
          client,
          {
            accountFingerprint,
            factor: 'PASSWORD',
            sourceFingerprint,
            subjectUserId: candidate.id,
          },
          now,
        );
        return null;
      }
      await this.clearRateLimit(client, accountFingerprint, sourceFingerprint, 'PASSWORD');
      const session = await this.createSession(client, current, sourceFingerprint, now);
      await this.audit.append(client, {
        action: 'AUTH_LOGIN_SUCCEEDED',
        actorType: 'USER',
        actorUserId: current.id,
        details: { factor: 'PASSWORD' },
        subjectId: current.id,
        subjectType: 'USER',
      });
      return { response: session.response, sessionToken: session.sessionToken };
    });
    if (result === null) {
      throw new AuthServiceError('AUTHENTICATION_FAILED');
    }
    return result;
  }

  async completeLogin(
    _request: CompleteLoginRequest,
    _source: string,
  ): Promise<AuthenticatedHttpResult> {
    void _request;
    void _source;
    throw new AuthServiceError('AUTH_METHOD_RETIRED');
  }

  private async resolveSession(
    sessionToken: string,
    source: string,
    rotateCsrf: boolean,
  ): Promise<SessionResolution> {
    if (sessionToken.length === 0) {
      throw new AuthServiceError('SESSION_INVALID');
    }
    const now = new Date();
    const sourceFingerprint = authenticationFingerprint(source);
    const tokenHash = hashOpaqueToken(sessionToken);
    const resolved = await this.transaction<SessionResolution | null>(async (client) => {
      const result = await client.query<SessionRow>(
        `SELECT
           sessions.id AS session_id,
           sessions.source_fingerprint AS session_source_fingerprint,
           sessions.csrf_token_hash,
           sessions.idle_expires_at,
           sessions.idle_timeout_minutes,
           sessions.absolute_expires_at,
           sessions.revoked_at,
           sessions.session_generation::text AS persisted_session_generation,
           users.id,
           users.normalized_username,
           users.display_name,
           users.role,
           users.status,
           users.must_change_password,
           users.password_kdf_algorithm,
           users.password_kdf_parameters,
           users.password_salt,
           users.password_hash,
           users.totp_secret_id,
           users.session_generation::text,
           users.last_accepted_totp_counter::text
         FROM ape_dts_platform.auth_sessions AS sessions
         JOIN ape_dts_platform.users
           ON users.id = sessions.user_id
         WHERE sessions.token_hash = $1
         FOR UPDATE OF sessions`,
        [tokenHash],
      );
      const row = result.rows[0];
      const valid =
        row !== undefined &&
        row.revoked_at === null &&
        row.status === 'ACTIVE' &&
        row.idle_expires_at > now &&
        row.absolute_expires_at > now &&
        row.session_generation === row.persisted_session_generation &&
        row.session_source_fingerprint.equals(sourceFingerprint);
      if (!valid || row === undefined) {
        if (row !== undefined && row.revoked_at === null) {
          await client.query(
            `UPDATE ape_dts_platform.auth_sessions
             SET revoked_at = $2,
                 revocation_reason = 'SESSION_VALIDATION_FAILED'
             WHERE id = $1::uuid
               AND revoked_at IS NULL`,
            [row.session_id, now],
          );
        }
        return null;
      }
      const idleExpiresAt = new Date(
        Math.min(
          row.absolute_expires_at.getTime(),
          now.getTime() + row.idle_timeout_minutes * 60_000,
        ),
      );
      const csrfToken = rotateCsrf ? generateOpaqueToken('scs-csrf-v1_', 32) : null;
      const csrfTokenHash = csrfToken === null ? row.csrf_token_hash : hashOpaqueToken(csrfToken);
      await client.query(
        `UPDATE ape_dts_platform.auth_sessions
         SET last_seen_at = $2,
             idle_expires_at = $3,
             csrf_token_hash = $4
         WHERE id = $1::uuid
           AND revoked_at IS NULL`,
        [row.session_id, now, idleExpiresAt, csrfTokenHash],
      );
      return {
        context: {
          mustChangePassword: row.must_change_password,
          absoluteExpiresAt: row.absolute_expires_at,
          csrfTokenHash,
          idleExpiresAt,
          sessionGeneration: row.session_generation,
          sessionId: row.session_id,
          sourceFingerprint,
          user: userSummary(row),
        },
        csrfToken,
      };
    });
    if (resolved === null) {
      throw new AuthServiceError('SESSION_INVALID');
    }
    return resolved;
  }

  async authenticateSession(
    sessionToken: string,
    source: string,
  ): Promise<AuthenticatedRequestContext> {
    return (await this.resolveSession(sessionToken, source, false)).context;
  }

  async refreshSession(
    sessionToken: string,
    source: string,
  ): Promise<AuthenticatedSessionResponse> {
    const resolved = await this.resolveSession(sessionToken, source, true);
    if (resolved.csrfToken === null) {
      throw new AuthServiceError('SESSION_INVALID');
    }
    return {
      absoluteExpiresAt: resolved.context.absoluteExpiresAt.toISOString(),
      csrfToken: resolved.csrfToken,
      idleExpiresAt: resolved.context.idleExpiresAt.toISOString(),
      mustChangePassword: resolved.context.mustChangePassword ?? false,
      status: 'AUTHENTICATED',
      user: resolved.context.user,
    };
  }

  assertCsrf(context: AuthenticatedRequestContext, csrfToken: string): void {
    if (csrfToken.length === 0 || !verifyOpaqueToken(csrfToken, context.csrfTokenHash)) {
      throw new AuthServiceError('CSRF_INVALID');
    }
  }

  async logout(context: AuthenticatedRequestContext): Promise<void> {
    await this.transaction(async (client) => {
      await client.query(
        `UPDATE ape_dts_platform.auth_sessions
         SET revoked_at = statement_timestamp(),
             revocation_reason = 'USER_LOGOUT'
         WHERE id = $1::uuid
           AND revoked_at IS NULL`,
        [context.sessionId],
      );
      await this.audit.append(client, {
        action: 'AUTH_LOGOUT',
        actorType: 'USER',
        actorUserId: context.user.id,
        details: {},
        subjectId: context.user.id,
        subjectType: 'USER',
      });
    });
  }

  async changePassword(
    context: AuthenticatedRequestContext,
    request: ChangePasswordRequest,
    source: string,
  ): Promise<AuthenticatedHttpResult> {
    const now = new Date();
    const accountFingerprint = authenticationFingerprint(context.user.username);
    const sourceFingerprint = authenticationFingerprint(source);
    await this.transaction((client) =>
      this.assertRateLimitAllowed(client, accountFingerprint, sourceFingerprint, 'PASSWORD', now),
    );
    const candidate = await this.loadUserByNormalizedUsername(context.user.username);
    if (!(await this.verifySubmittedPassword(request.currentPassword, candidate))) {
      await this.transaction((client) =>
        this.recordAuthenticationFailure(
          client,
          {
            accountFingerprint,
            sourceFingerprint,
            factor: 'PASSWORD',
            subjectUserId: context.user.id,
          },
          now,
        ),
      );
      throw new AuthServiceError('CURRENT_PASSWORD_INVALID');
    }
    if (request.currentPassword === request.newPassword) {
      throw new AuthServiceError('PASSWORD_UNCHANGED');
    }
    const password = await hashPassword(request.newPassword, this.requirePolicy());
    return this.transaction(async (client) => {
      // Only this authenticated user and unchanged credential generation may be updated.
      const result = await client.query<UserAuthRow>(
        `UPDATE ape_dts_platform.users
         SET password_kdf_algorithm = 'ARGON2ID', password_kdf_parameters = $3::jsonb,
             password_salt = $4, password_hash = $5, must_change_password = false,
             row_version = row_version + 1, updated_at = statement_timestamp()
         WHERE id = $1::uuid AND session_generation = $2::bigint AND status = 'ACTIVE'
           AND EXISTS (SELECT 1 FROM ape_dts_platform.auth_sessions
                       WHERE id = $6::uuid AND user_id = $1::uuid AND revoked_at IS NULL
                         AND absolute_expires_at > statement_timestamp()
                         AND idle_expires_at > statement_timestamp())
         RETURNING *, session_generation::text`,
        [
          context.user.id,
          context.sessionGeneration,
          JSON.stringify(password.parameters),
          Buffer.from(password.salt, 'base64'),
          Buffer.from(password.hash, 'base64'),
          context.sessionId,
        ],
      );
      const user = result.rows[0];
      if (user === undefined) throw new AuthServiceError('SESSION_INVALID');
      await client.query(
        `UPDATE ape_dts_platform.auth_sessions SET revoked_at = $2, revocation_reason = 'PASSWORD_CHANGED'
         WHERE user_id = $1::uuid AND revoked_at IS NULL`,
        [user.id, now],
      );
      await this.clearRateLimit(client, accountFingerprint, sourceFingerprint, 'PASSWORD');
      await this.audit.append(client, {
        action: 'AUTH_PASSWORD_CHANGED',
        actorType: 'USER',
        actorUserId: user.id,
        details: {},
        subjectId: user.id,
        subjectType: 'USER',
      });
      return this.createSession(client, user, sourceFingerprint, now);
    });
  }
}
