import { randomUUID } from 'node:crypto';

import type { MigrationTransaction } from '@scs/database';

import { calculateAuditEventHash, type AuditHashInput } from '../audit/audit-hash.js';

type Query = Pick<MigrationTransaction, 'query'>;

/** Read-only preflight runs before persisting a key file or applying schema changes. */
export async function inspectAdministratorIdentity(
  database: Query,
  selected?: string,
): Promise<string> {
  const username = selected ?? 'admin';
  const result = await database.query(
    `SELECT id, normalized_username, role FROM ape_dts_platform.users
     WHERE normalized_username IN ($1, 'admin')`,
    [username],
  );
  const fixed = result.rows.find((row) => row.normalized_username === 'admin');
  if (fixed !== undefined && (fixed.role !== 'ADMIN' || username !== 'admin')) {
    throw new Error('ADMIN_USERNAME_CONFLICT');
  }
  const chosen = result.rows.find(
    (row) => row.normalized_username === username && row.role === 'ADMIN',
  );
  if (chosen === undefined)
    throw new Error(
      selected === undefined
        ? 'ADMIN_IDENTITY_MIGRATION_REQUIRED'
        : 'SELECTED_ADMINISTRATOR_NOT_FOUND',
    );
  return username;
}

/** Caller owns the transaction. Only the explicitly selected ADMIN row is renamed. */
export async function migrateAdministratorIdentity(
  transaction: Query,
  selected: string,
): Promise<void> {
  await transaction.query('LOCK TABLE ape_dts_platform.users IN SHARE ROW EXCLUSIVE MODE');
  await inspectAdministratorIdentity(transaction, selected);
  if (selected === 'admin') return;
  await transaction.query('SELECT pg_advisory_xact_lock($1, $2)', [1_096_048_964, 1_096_104_660]);
  const updated = await transaction.query(
    `UPDATE ape_dts_platform.users
     SET normalized_username = 'admin', session_generation = session_generation + 1,
         row_version = row_version + 1, updated_at = statement_timestamp()
     WHERE normalized_username = $1 AND role = 'ADMIN'
       AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.users WHERE normalized_username = 'admin')
     RETURNING id`,
    [selected],
  );
  const id = updated.rows[0]?.id;
  if (typeof id !== 'string' || updated.rows.length !== 1)
    throw new Error('ADMIN_IDENTITY_MIGRATION_CONFLICT');
  await transaction.query(
    `UPDATE ape_dts_platform.auth_sessions SET revoked_at = statement_timestamp(),
       revocation_reason = 'ADMINISTRATOR_IDENTITY_MIGRATED'
     WHERE user_id = $1::uuid AND revoked_at IS NULL`,
    [id],
  );
  const previous = await transaction.query(
    "SELECT encode(event_hash, 'base64') AS hash FROM ape_dts_platform.audit_events ORDER BY sequence DESC LIMIT 1",
  );
  const encoded = previous.rows[0]?.hash;
  const previousHash = typeof encoded === 'string' ? Buffer.from(encoded, 'base64') : null;
  const event: AuditHashInput = {
    action: 'ADMINISTRATOR_IDENTITY_MIGRATED',
    actorType: 'DEPLOYMENT_ADMIN',
    actorUserId: null,
    details: { previousUsername: selected, username: 'admin', method: 'CONTAINER_CLI' },
    eventId: randomUUID(),
    occurredAt: new Date().toISOString(),
    subjectId: id,
    subjectType: 'USER',
  };
  await transaction.query(
    `INSERT INTO ape_dts_platform.audit_events
       (event_id, actor_type, actor_user_id, action, subject_type, subject_id,
        canonical_details, previous_hash, event_hash, occurred_at)
     VALUES ($1::uuid, $2, NULL, $3, $4, $5::uuid, $6::jsonb, $7, $8, $9::timestamptz)`,
    [
      event.eventId,
      event.actorType,
      event.action,
      event.subjectType,
      id,
      JSON.stringify(event.details),
      previousHash,
      calculateAuditEventHash(previousHash, event),
      event.occurredAt,
    ],
  );
}
