import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';
import type {
  ActivateUserRequest,
  ActivateUserResponse,
  ActivationGrantResponse,
  ChangeUserRoleRequest,
  ChangeUserStatusRequest,
  CreateUserRequest,
  UserMutationResponse,
  UserRole,
  UserStatus,
  UserSummary,
} from '@scs/contracts';
import {
  generateOpaqueToken,
  hashOpaqueToken,
  hashPassword,
  type Argon2idParameters,
} from '@scs/security';
import type { PoolClient, QueryResultRow } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { AUTH_ACTIVATION_MILLISECONDS } from '../auth/auth-policy.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { ARGON2ID_POLICY } from '../security/security-config.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';

export type UsersServiceErrorCode =
  | 'AUTH_METHOD_RETIRED'
  | 'ACTIVATION_FAILED'
  | 'FINAL_ACTIVE_ADMINISTRATOR'
  | 'FIXED_ADMIN_PROTECTED'
  | 'INVALID_USER_STATE'
  | 'ROLE_UNCHANGED'
  | 'SECURITY_POLICY_UNAVAILABLE'
  | 'TOTP_ENROLLMENT_FAILED'
  | 'USERNAME_ALREADY_EXISTS'
  | 'USER_NOT_FOUND';

export class UsersServiceError extends Error {
  constructor(readonly code: UsersServiceErrorCode) {
    super(code);
    this.name = 'UsersServiceError';
  }
}

interface UserRow extends QueryResultRow {
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

interface ActivationRow extends UserRow {
  activation_id: string;
  created_by_user_id: string;
  expires_at: Date;
  purpose: 'CREDENTIAL_RESET' | 'INVITATION' | 'ROLE_ELEVATION';
}

function normalizeUsername(username: string): string {
  return username.normalize('NFKC').toLocaleLowerCase('en-US');
}

function userSummary(row: UserRow): UserSummary {
  return {
    displayName: row.display_name,
    id: row.id,
    role: row.role,
    status: row.status,
    username: row.normalized_username,
  };
}

function postgresCode(error: unknown): string | null {
  if (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string'
  ) {
    return error.code;
  }
  return null;
}

@Injectable()
export class UsersService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    _unlockedKey: UnlockedKeyService,
    @Inject(ARGON2ID_POLICY)
    private readonly argon2idPolicy: Argon2idParameters | null,
  ) {}

  private requirePolicy(): Argon2idParameters {
    if (this.argon2idPolicy === null) {
      throw new UsersServiceError('SECURITY_POLICY_UNAVAILABLE');
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

  async listUsers(): Promise<readonly UserSummary[]> {
    const result = await this.database.requirePool().query<UserRow>(
      `SELECT
         id, normalized_username, display_name, role, status,
         password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
         totp_secret_id, session_generation::text, last_accepted_totp_counter::text
       FROM ape_dts_platform.users
       ORDER BY normalized_username, id`,
    );
    return result.rows.map(userSummary);
  }

  private async createActivationGrant(
    client: PoolClient,
    input: {
      readonly actorUserId: string;
      readonly purpose: ActivationRow['purpose'];
      readonly user: UserRow;
    },
    now: Date,
  ): Promise<ActivationGrantResponse> {
    const activationCode = generateOpaqueToken('scs-at-v1_', 32);
    const expiresAt = new Date(now.getTime() + AUTH_ACTIVATION_MILLISECONDS);
    await client.query(
      `UPDATE ape_dts_platform.user_activation_tokens
       SET consumed_at = $2
       WHERE user_id = $1::uuid
         AND consumed_at IS NULL`,
      [input.user.id, now],
    );
    await client.query(
      `INSERT INTO ape_dts_platform.user_activation_tokens (
         id, user_id, purpose, token_hash, created_by_user_id, created_at, expires_at
       ) VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6, $7)`,
      [
        randomUUID(),
        input.user.id,
        input.purpose,
        hashOpaqueToken(activationCode),
        input.actorUserId,
        now,
        expiresAt,
      ],
    );
    return {
      activationCode,
      expiresAt: expiresAt.toISOString(),
      user: userSummary(input.user),
    };
  }

  async createUser(
    actorUserId: string,
    request: CreateUserRequest,
  ): Promise<ActivationGrantResponse> {
    const now = new Date();
    try {
      return await this.transaction(async (client) => {
        const inserted = await client.query<UserRow>(
          `INSERT INTO ape_dts_platform.users (
             id, normalized_username, display_name, role, status,
             password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash
           ) VALUES (
             $1::uuid, $2, $3, $4, 'INVITED', NULL, NULL, NULL, NULL
           )
           RETURNING
             id, normalized_username, display_name, role, status,
             password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
             totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
          [randomUUID(), normalizeUsername(request.username), request.displayName, request.role],
        );
        const user = inserted.rows[0];
        if (user === undefined) {
          throw new Error('USER_INSERT_FAILED');
        }
        const grant = await this.createActivationGrant(
          client,
          { actorUserId, purpose: 'INVITATION', user },
          now,
        );
        await this.audit.append(client, {
          action: 'USER_INVITED',
          actorType: 'USER',
          actorUserId,
          details: { role: user.role },
          subjectId: user.id,
          subjectType: 'USER',
        });
        return grant;
      });
    } catch (error) {
      if (postgresCode(error) === '23505') {
        throw new UsersServiceError('USERNAME_ALREADY_EXISTS');
      }
      throw error;
    }
  }

  private async revokeSecondFactors(client: PoolClient, row: UserRow, now: Date): Promise<void> {
    if (row.totp_secret_id !== null) {
      await client.query(
        `UPDATE ape_dts_platform.secrets
         SET destroyed_at = $2
         WHERE id = $1::uuid
           AND destroyed_at IS NULL`,
        [row.totp_secret_id, now],
      );
    }
    await client.query(
      `UPDATE ape_dts_platform.user_recovery_code_batches
       SET state = 'REVOKED',
           revoked_at = $2
       WHERE user_id = $1::uuid
         AND state = 'ACTIVE'`,
      [row.id, now],
    );
    await client.query(
      `UPDATE ape_dts_platform.authentication_challenges
       SET consumed_at = $2
       WHERE user_id = $1::uuid
         AND consumed_at IS NULL`,
      [row.id, now],
    );
  }

  async activateUser(request: ActivateUserRequest, _source: string): Promise<ActivateUserResponse> {
    void _source;
    const now = new Date();
    const password = await hashPassword(request.password, this.requirePolicy());
    const tokenHash = hashOpaqueToken(request.activationCode);
    const normalizedUsername = normalizeUsername(request.username);
    const response = await this.transaction<ActivateUserResponse | null>(async (client) => {
      const result = await client.query<ActivationRow>(
        `SELECT
             token.id AS activation_id,
             token.purpose,
             token.created_by_user_id,
             token.expires_at,
             users.id,
             users.normalized_username,
             users.display_name,
             users.role,
             users.status,
             users.password_kdf_algorithm,
             users.password_kdf_parameters,
             users.password_salt,
             users.password_hash,
             users.totp_secret_id,
             users.session_generation::text,
             users.last_accepted_totp_counter::text
           FROM ape_dts_platform.user_activation_tokens AS token
           JOIN ape_dts_platform.users
             ON users.id = token.user_id
           WHERE token.token_hash = $1
             AND token.consumed_at IS NULL
           FOR UPDATE OF token, users`,
        [tokenHash],
      );
      const activation = result.rows[0];
      const expectedState =
        activation?.purpose === 'INVITATION'
          ? 'INVITED'
          : activation?.purpose === 'CREDENTIAL_RESET'
            ? 'PASSWORD_REQUIRED'
            : 'TOTP_REQUIRED';
      if (
        activation === undefined ||
        activation.expires_at <= now ||
        activation.normalized_username !== normalizedUsername ||
        activation.status !== expectedState ||
        (activation.purpose === 'ROLE_ELEVATION' && activation.role !== 'ADMIN')
      ) {
        return null;
      }
      await client.query(
        `UPDATE ape_dts_platform.user_activation_tokens
           SET consumed_at = $2
           WHERE id = $1::uuid
             AND consumed_at IS NULL`,
        [activation.activation_id, now],
      );
      const nextStatus = 'ACTIVE';
      const updated = await client.query<UserRow>(
        `UPDATE ape_dts_platform.users
           SET password_kdf_algorithm = 'ARGON2ID',
               password_kdf_parameters = $2::jsonb,
               password_salt = $3,
               password_hash = $4,
               status = $5,
               must_change_password = false,
               disabled_at = NULL,
               totp_secret_id = NULL,
               last_accepted_totp_counter = NULL,
               row_version = row_version + 1,
               updated_at = statement_timestamp()
           WHERE id = $1::uuid
           RETURNING
             id, normalized_username, display_name, role, status,
             password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
             totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
        [
          activation.id,
          JSON.stringify(password.parameters),
          Buffer.from(password.salt, 'base64'),
          Buffer.from(password.hash, 'base64'),
          nextStatus,
        ],
      );
      const user = updated.rows[0];
      if (user === undefined) {
        return null;
      }
      const activationResponse: ActivateUserResponse = {
        status: 'ACTIVE',
        user: userSummary(user),
      };
      await this.audit.append(client, {
        action: 'USER_ACTIVATED',
        actorType: 'USER',
        actorUserId: user.id,
        details: { purpose: activation.purpose, role: user.role },
        subjectId: user.id,
        subjectType: 'USER',
      });
      return activationResponse;
    });
    if (response === null) {
      throw new UsersServiceError('ACTIVATION_FAILED');
    }
    return response;
  }

  async confirmTotpEnrollment(
    _challengeToken: string,
    _code: string,
    _source: string,
  ): Promise<UserMutationResponse> {
    void _challengeToken;
    void _code;
    void _source;
    throw new UsersServiceError('AUTH_METHOD_RETIRED');
  }

  private async lockUser(client: PoolClient, userId: string): Promise<UserRow> {
    const result = await client.query<UserRow>(
      `SELECT
         id, normalized_username, display_name, role, status,
         password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
         totp_secret_id, session_generation::text, last_accepted_totp_counter::text
       FROM ape_dts_platform.users
       WHERE id = $1::uuid
       FOR UPDATE`,
      [userId],
    );
    const user = result.rows[0];
    if (user === undefined) {
      throw new UsersServiceError('USER_NOT_FOUND');
    }
    return user;
  }

  async changeStatus(
    actorUserId: string,
    userId: string,
    request: ChangeUserStatusRequest,
  ): Promise<UserMutationResponse> {
    try {
      return await this.transaction(async (client) => {
        const current = await this.lockUser(client, userId);
        if (
          (request.status === 'DISABLED' && current.status !== 'ACTIVE') ||
          (request.status === 'ACTIVE' &&
            (current.status !== 'DISABLED' || current.password_hash === null))
        ) {
          throw new UsersServiceError('INVALID_USER_STATE');
        }
        const updated = await client.query<UserRow>(
          `UPDATE ape_dts_platform.users
           SET status = $2,
               disabled_at = CASE
                 WHEN $2 = 'DISABLED' THEN statement_timestamp()
                 ELSE NULL
               END,
               row_version = row_version + 1,
               updated_at = statement_timestamp()
           WHERE id = $1::uuid
             AND status = $3
           RETURNING
             id, normalized_username, display_name, role, status,
             password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
             totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
          [userId, request.status, request.status === 'ACTIVE' ? 'DISABLED' : 'ACTIVE'],
        );
        const user = updated.rows[0];
        if (user === undefined) {
          throw new UsersServiceError('INVALID_USER_STATE');
        }
        await this.audit.append(client, {
          action: request.status === 'ACTIVE' ? 'USER_ENABLED' : 'USER_DISABLED',
          actorType: 'USER',
          actorUserId,
          details: {},
          subjectId: user.id,
          subjectType: 'USER',
        });
        return { user: userSummary(user) };
      });
    } catch (error) {
      if (postgresCode(error) === '23514' && error instanceof UsersServiceError === false) {
        throw new UsersServiceError('FINAL_ACTIVE_ADMINISTRATOR');
      }
      throw error;
    }
  }

  async changeRole(
    actorUserId: string,
    userId: string,
    request: ChangeUserRoleRequest,
  ): Promise<ActivationGrantResponse | UserMutationResponse> {
    try {
      return await this.transaction(async (client) => {
        const current = await this.lockUser(client, userId);
        if (
          current.normalized_username === 'admin' &&
          current.role === 'ADMIN' &&
          request.role !== 'ADMIN'
        ) {
          throw new UsersServiceError('FIXED_ADMIN_PROTECTED');
        }
        if (current.status !== 'ACTIVE') {
          throw new UsersServiceError('INVALID_USER_STATE');
        }
        if (current.role === request.role) {
          throw new UsersServiceError('ROLE_UNCHANGED');
        }
        if (request.role === 'ADMIN') {
          const updated = await client.query<UserRow>(
            `UPDATE ape_dts_platform.users
             SET role = 'ADMIN',
                 status = 'ACTIVE',
                 totp_secret_id = NULL,
                 last_accepted_totp_counter = NULL,
                 row_version = row_version + 1,
                 updated_at = statement_timestamp()
             WHERE id = $1::uuid
               AND role = 'DEVELOPER'
               AND status = 'ACTIVE'
             RETURNING
               id, normalized_username, display_name, role, status,
               password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
               totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
            [userId],
          );
          const user = updated.rows[0];
          if (user === undefined) {
            throw new UsersServiceError('INVALID_USER_STATE');
          }

          await this.audit.append(client, {
            action: 'USER_ROLE_CHANGED',
            actorType: 'USER',
            actorUserId,
            details: { fromRole: current.role, toRole: user.role },
            subjectId: user.id,
            subjectType: 'USER',
          });
          return { user: userSummary(user) };
        }

        const now = new Date();
        await this.revokeSecondFactors(client, current, now);
        const updated = await client.query<UserRow>(
          `UPDATE ape_dts_platform.users
           SET role = 'DEVELOPER',
               status = 'ACTIVE',
               totp_secret_id = NULL,
               last_accepted_totp_counter = NULL,
               row_version = row_version + 1,
               updated_at = statement_timestamp()
           WHERE id = $1::uuid
             AND role = 'ADMIN'
             AND status = 'ACTIVE'
           RETURNING
             id, normalized_username, display_name, role, status,
             password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
             totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
          [userId],
        );
        const user = updated.rows[0];
        if (user === undefined) {
          throw new UsersServiceError('INVALID_USER_STATE');
        }
        await this.audit.append(client, {
          action: 'USER_ROLE_CHANGED',
          actorType: 'USER',
          actorUserId,
          details: { fromRole: current.role, toRole: user.role },
          subjectId: user.id,
          subjectType: 'USER',
        });
        return { user: userSummary(user) };
      });
    } catch (error) {
      if (postgresCode(error) === '23514') {
        throw new UsersServiceError('FINAL_ACTIVE_ADMINISTRATOR');
      }
      throw error;
    }
  }

  async resetCredentials(actorUserId: string, userId: string): Promise<ActivationGrantResponse> {
    return await this.transaction(async (client) => {
      const current = await this.lockUser(client, userId);
      if (current.status === 'DISABLED') {
        throw new UsersServiceError('INVALID_USER_STATE');
      }
      const now = new Date();
      if (current.status === 'INVITED') {
        const grant = await this.createActivationGrant(
          client,
          { actorUserId, purpose: 'INVITATION', user: current },
          now,
        );
        await this.audit.append(client, {
          action: 'USER_ACTIVATION_REISSUED',
          actorType: 'USER',
          actorUserId,
          details: {},
          subjectId: current.id,
          subjectType: 'USER',
        });
        return grant;
      }
      await this.revokeSecondFactors(client, current, now);
      const updated = await client.query<UserRow>(
        `UPDATE ape_dts_platform.users
         SET status = 'PASSWORD_REQUIRED',
             totp_secret_id = NULL,
             last_accepted_totp_counter = NULL,
             row_version = row_version + 1,
             updated_at = statement_timestamp()
         WHERE id = $1::uuid
           AND status IN ('ACTIVE', 'PASSWORD_REQUIRED', 'TOTP_REQUIRED')
         RETURNING
           id, normalized_username, display_name, role, status,
           password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash,
           totp_secret_id, session_generation::text, last_accepted_totp_counter::text`,
        [userId],
      );
      const user = updated.rows[0];
      if (user === undefined) {
        throw new UsersServiceError('INVALID_USER_STATE');
      }
      const grant = await this.createActivationGrant(
        client,
        { actorUserId, purpose: 'CREDENTIAL_RESET', user },
        now,
      );
      await this.audit.append(client, {
        action: 'USER_CREDENTIAL_RESET_REQUIRED',
        actorType: 'USER',
        actorUserId,
        details: {},
        subjectId: user.id,
        subjectType: 'USER',
      });
      return grant;
    });
  }

  async revokeSessions(actorUserId: string, userId: string): Promise<void> {
    await this.transaction(async (client) => {
      await this.lockUser(client, userId);
      await client.query(
        `UPDATE ape_dts_platform.users
         SET session_generation = session_generation + 1,
             row_version = row_version + 1,
             updated_at = statement_timestamp()
         WHERE id = $1::uuid`,
        [userId],
      );
      await client.query(
        `UPDATE ape_dts_platform.auth_sessions
         SET revoked_at = statement_timestamp(),
             revocation_reason = 'ADMINISTRATIVE_REVOCATION'
         WHERE user_id = $1::uuid
           AND revoked_at IS NULL`,
        [userId],
      );
      await this.audit.append(client, {
        action: 'USER_SESSIONS_REVOKED',
        actorType: 'USER',
        actorUserId,
        details: {},
        subjectId: userId,
        subjectType: 'USER',
      });
    });
  }
}
