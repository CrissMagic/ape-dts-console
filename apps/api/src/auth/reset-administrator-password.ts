import { randomBytes } from 'node:crypto';
import { hashPassword, type Argon2idParameters } from '@scs/security';
import type { Pool } from 'pg';
import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { authenticationFingerprint } from './auth-policy.js';

/** Host/container-authorized recovery. Callers must print the returned secret only once. */
export async function resetAdministratorPassword(
  pool: Pool,
  audit: Pick<AuditLedgerService, 'append'>,
  policy: Argon2idParameters,
): Promise<string> {
  const temporaryPassword = randomBytes(24).toString('base64url');
  const password = await hashPassword(temporaryPassword, policy);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Recovery is strictly scoped to the fixed admin identity; no other account is modified.
    const result = await client.query<{ id: string }>(
      `UPDATE ape_dts_platform.users
       SET password_kdf_algorithm = 'ARGON2ID', password_kdf_parameters = $1::jsonb,
           password_salt = $2, password_hash = $3, must_change_password = true,
           status = 'ACTIVE', disabled_at = NULL, row_version = row_version + 1,
           updated_at = statement_timestamp()
       WHERE normalized_username = 'admin' AND role = 'ADMIN'
       RETURNING id`,
      [
        JSON.stringify(password.parameters),
        Buffer.from(password.salt, 'base64'),
        Buffer.from(password.hash, 'base64'),
      ],
    );
    const admin = result.rows[0];
    if (admin === undefined) throw new Error('ADMIN_ACCOUNT_NOT_FOUND');
    await client.query(
      `UPDATE ape_dts_platform.auth_sessions SET revoked_at = statement_timestamp(),
         revocation_reason = 'ADMINISTRATOR_PASSWORD_RESET'
       WHERE user_id = $1::uuid AND revoked_at IS NULL`,
      [admin.id],
    );
    await client.query(
      `UPDATE ape_dts_platform.user_activation_tokens SET consumed_at = statement_timestamp()
       WHERE user_id = $1::uuid AND consumed_at IS NULL`,
      [admin.id],
    );
    await client.query(
      `UPDATE ape_dts_platform.authentication_challenges SET consumed_at = statement_timestamp()
       WHERE user_id = $1::uuid AND consumed_at IS NULL`,
      [admin.id],
    );
    await client.query(
      'DELETE FROM ape_dts_platform.authentication_rate_limits WHERE account_fingerprint = $1',
      [authenticationFingerprint('admin')],
    );
    await audit.append(client, {
      action: 'ADMINISTRATOR_PASSWORD_RESET',
      actorType: 'DEPLOYMENT_ADMIN',
      actorUserId: null,
      details: { method: 'CONTAINER_CLI' },
      subjectId: admin.id,
      subjectType: 'USER',
    });
    await client.query('COMMIT');
    return temporaryPassword;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
