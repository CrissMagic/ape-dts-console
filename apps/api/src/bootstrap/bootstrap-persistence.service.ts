import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { MigrationTransaction } from '@scs/database';
import {
  validateArgon2idParameters,
  type EncryptedSecret,
  type PasswordHashRecord,
  type RecoveryCodeHashRecord,
  type WrappedKey,
} from '@scs/security';
import type { Pool, PoolClient, QueryResultRow } from 'pg';

import { calculateAuditEventHash, type AuditHashInput } from '../audit/audit-hash.js';

const PLATFORM_VERSION = '0.0.0';
const AUDIT_LOCK_NAMESPACE = 1_096_048_964;
const AUDIT_LOCK_KEY = 1_096_104_660;

const RECOVERY_STEPS = [
  'LOAD_METADATA',
  'UNLOCK_KEYS',
  'ENSURE_ARTIFACTS',
  'VERIFY_CONTINUITY',
  'PLAN_CAPACITY',
  'RESTORE_BATCHES',
  'READY_OBSERVABILITY',
] as const;

interface InitialPlatformRows {
  readonly passwordOnly?: boolean;
  readonly administrator: {
    readonly displayName: string;
    readonly id: string;
    readonly normalizedUsername: string;
    readonly password: PasswordHashRecord;
  };
  readonly instanceId: string;
  readonly passphraseWrapper: WrappedKey;
  readonly recoverySessionId: string;
  readonly recoveryWrapper: WrappedKey;
  readonly startedAt: string;
}

interface CompleteAdministratorInput {
  readonly acceptedTotpCounter: number;
  readonly administratorId: string;
  readonly encryptedTotpSecret: EncryptedSecret;
  readonly recoveryCodeHashes: readonly RecoveryCodeHashRecord[];
  readonly recoverySessionId: string;
  readonly secretId: string;
}

interface WrapperRow extends QueryResultRow {
  ciphertext: Buffer;
  kdf_algorithm: string;
  kdf_parameters: unknown;
  nonce: Buffer;
  salt: Buffer;
  tag: Buffer;
}

interface PreviousHashRow extends QueryResultRow {
  event_hash: Buffer;
}

function wrapperKdfParameters(wrapper: WrappedKey): Readonly<Record<string, unknown>> {
  return wrapper.kdf.algorithm === 'ARGON2ID'
    ? { ...wrapper.kdf.parameters }
    : { info: wrapper.kdf.info };
}

function wrapperSalt(wrapper: WrappedKey): Buffer {
  return Buffer.from(wrapper.kdf.salt, 'base64');
}

async function insertWrapper(
  transaction: MigrationTransaction,
  wrapper: WrappedKey,
): Promise<void> {
  await transaction.query(
    `INSERT INTO ape_dts_platform.key_wrappers
      (id, wrapper_type, version, kdf_algorithm, kdf_parameters, salt,
       cipher_algorithm, nonce, ciphertext, tag, active)
     VALUES ($1::uuid, $2, 1, $3, $4::jsonb, $5, $6, $7, $8, $9, true)`,
    [
      randomUUID(),
      wrapper.wrapperType,
      wrapper.kdf.algorithm,
      JSON.stringify(wrapperKdfParameters(wrapper)),
      wrapperSalt(wrapper),
      wrapper.cipherAlgorithm,
      Buffer.from(wrapper.nonce, 'base64'),
      Buffer.from(wrapper.ciphertext, 'base64'),
      Buffer.from(wrapper.tag, 'base64'),
    ],
  );
}

function initializationAudit(rows: InitialPlatformRows): AuditHashInput {
  return {
    action: 'PLATFORM_INITIALIZED',
    actorType: 'DEPLOYMENT_ADMIN',
    actorUserId: null,
    details: {
      administratorUsername: rows.administrator.normalizedUsername,
      schemaVersion: 1,
    },
    eventId: randomUUID(),
    occurredAt: rows.startedAt,
    subjectId: rows.instanceId,
    subjectType: 'PLATFORM_INSTANCE',
  };
}

@Injectable()
export class BootstrapPersistenceService {
  async ensureDefaultLocalStorage(pool: Pool): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
        AUDIT_LOCK_NAMESPACE,
        AUDIT_LOCK_KEY,
      ]);
      // Serialize the empty-table decision against all concurrent profile writes.
      await client.query(
        'LOCK TABLE ape_dts_platform.storage_profiles IN SHARE ROW EXCLUSIVE MODE',
      );
      const id = randomUUID();
      const inserted = await client.query(
        `INSERT INTO ape_dts_platform.storage_profiles
          (id, profile_key, display_name, version, driver, config, state,
           created_by_actor_type, created_by_user_id, activated_at)
         SELECT $1::uuid, 'default-local', '默认本地存储', 1, 'LOCAL',
                '{"relative_prefix":""}'::jsonb, 'ACTIVE_WRITE', 'SYSTEM', NULL,
                statement_timestamp()
         WHERE NOT EXISTS (SELECT 1 FROM ape_dts_platform.storage_profiles)
         RETURNING id`,
        [id],
      );
      if (inserted.rowCount === 1) {
        await this.appendAuditEvent(client, {
          action: 'DEFAULT_LOCAL_STORAGE_CREATED',
          actorType: 'SYSTEM',
          actorUserId: null,
          details: { driver: 'LOCAL', relativePrefix: '' },
          eventId: randomUUID(),
          occurredAt: new Date().toISOString(),
          subjectId: id,
          subjectType: 'STORAGE_PROFILE',
        });
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async insertInitialPlatformRows(
    transaction: MigrationTransaction,
    rows: InitialPlatformRows,
  ): Promise<void> {
    await transaction.query(
      `INSERT INTO ape_dts_platform.platform_instances
        (id, platform_version, started_at, last_heartbeat_at, process_id, current_recovery_session_id)
       VALUES ($1::uuid, $2, $3::timestamptz, $3::timestamptz, $4, NULL)`,
      [rows.instanceId, PLATFORM_VERSION, rows.startedAt, process.pid],
    );
    await transaction.query(
      `INSERT INTO ape_dts_platform.recovery_sessions
        (id, platform_instance_id, state)
       VALUES ($1::uuid, $2::uuid, 'RUNNING')`,
      [rows.recoverySessionId, rows.instanceId],
    );
    await transaction.query(
      `UPDATE ape_dts_platform.platform_instances
       SET current_recovery_session_id = $1::uuid
       WHERE id = $2::uuid AND current_recovery_session_id IS NULL`,
      [rows.recoverySessionId, rows.instanceId],
    );

    const stepIds = RECOVERY_STEPS.map(() => randomUUID());
    const positions = RECOVERY_STEPS.map((_, index) => index + 1);
    const statuses = RECOVERY_STEPS.map((_, index) => (index < 2 ? 'SUCCEEDED' : 'PENDING'));
    await transaction.query(
      `INSERT INTO ape_dts_platform.recovery_steps
        (id, recovery_session_id, step_type, position, status, progress, started_at, finished_at)
       SELECT id, $1::uuid, step_type, position, status,
              CASE WHEN status = 'SUCCEEDED' THEN '{"candidateCount":0}'::jsonb ELSE '{}'::jsonb END,
              CASE WHEN status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END,
              CASE WHEN status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END
       FROM unnest($2::uuid[], $3::text[], $4::smallint[], $5::text[])
         AS steps(id, step_type, position, status)`,
      [rows.recoverySessionId, stepIds, [...RECOVERY_STEPS], positions, statuses],
    );

    await insertWrapper(transaction, rows.passphraseWrapper);
    await insertWrapper(transaction, rows.recoveryWrapper);
    await transaction.query(
      `INSERT INTO ape_dts_platform.users
        (id, normalized_username, display_name, role, status,
         password_kdf_algorithm, password_kdf_parameters, password_salt, password_hash${rows.passwordOnly === true ? ', must_change_password' : ''})
       VALUES ($1::uuid, $2, $3, 'ADMIN', '${rows.passwordOnly === true ? 'ACTIVE' : 'TOTP_REQUIRED'}', 'ARGON2ID', $4::jsonb, $5, $6${rows.passwordOnly === true ? ', true' : ''})`,
      [
        rows.administrator.id,
        rows.administrator.normalizedUsername,
        rows.administrator.displayName,
        JSON.stringify(rows.administrator.password.parameters),
        Buffer.from(rows.administrator.password.salt, 'base64'),
        Buffer.from(rows.administrator.password.hash, 'base64'),
      ],
    );
    await transaction.query(
      `INSERT INTO ape_dts_platform.platform_settings (singleton_id, updated_by)
       VALUES (1, $1::uuid)`,
      [rows.administrator.id],
    );

    const audit = initializationAudit(rows);
    await transaction.query(
      `INSERT INTO ape_dts_platform.audit_events
        (event_id, actor_type, actor_user_id, action, subject_type, subject_id,
         canonical_details, previous_hash, event_hash, occurred_at)
       VALUES ($1::uuid, $2, NULL, $3, $4, $5::uuid, $6::jsonb, NULL, $7, $8::timestamptz)`,
      [
        audit.eventId,
        audit.actorType,
        audit.action,
        audit.subjectType,
        audit.subjectId,
        JSON.stringify(audit.details),
        calculateAuditEventHash(null, audit),
        audit.occurredAt,
      ],
    );
  }

  async completeInitialAdministrator(pool: Pool, input: CompleteAdministratorInput): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
        AUDIT_LOCK_NAMESPACE,
        AUDIT_LOCK_KEY,
      ]);
      await client.query(
        `INSERT INTO ape_dts_platform.secrets
          (id, owner_type, owner_id, purpose, algorithm, dek_version, aad_version,
           nonce, ciphertext, tag)
         VALUES ($1::uuid, 'USER', $2::uuid, 'TOTP_SEED', $3, 1, $4, $5, $6, $7)`,
        [
          input.secretId,
          input.administratorId,
          input.encryptedTotpSecret.algorithm,
          input.encryptedTotpSecret.aadVersion,
          Buffer.from(input.encryptedTotpSecret.nonce, 'base64'),
          Buffer.from(input.encryptedTotpSecret.ciphertext, 'base64'),
          Buffer.from(input.encryptedTotpSecret.tag, 'base64'),
        ],
      );
      const updated = await client.query<{ id: string } & QueryResultRow>(
        `UPDATE ape_dts_platform.users
         SET totp_secret_id = $1::uuid,
             status = 'ACTIVE',
             last_accepted_totp_counter = $3::bigint,
             session_generation = session_generation + 1,
             row_version = row_version + 1,
             updated_at = statement_timestamp()
         WHERE id = $2::uuid
           AND role = 'ADMIN'
           AND status = 'TOTP_REQUIRED'
           AND totp_secret_id IS NULL
           AND disabled_at IS NULL
         RETURNING id`,
        [input.secretId, input.administratorId, input.acceptedTotpCounter],
      );
      if (updated.rowCount !== 1) {
        throw new Error('INITIAL_ADMINISTRATOR_STATE_CHANGED');
      }
      const recoveryCodeBatchId = randomUUID();
      await client.query(
        `INSERT INTO ape_dts_platform.user_recovery_code_batches (
           id, user_id, state
         ) VALUES ($1::uuid, $2::uuid, 'ACTIVE')`,
        [recoveryCodeBatchId, input.administratorId],
      );
      for (const [index, recoveryCode] of input.recoveryCodeHashes.entries()) {
        await client.query(
          `INSERT INTO ape_dts_platform.user_recovery_codes (
             id, batch_id, ordinal, code_salt, code_hash
           ) VALUES ($1::uuid, $2::uuid, $3, $4, $5)`,
          [randomUUID(), recoveryCodeBatchId, index + 1, recoveryCode.salt, recoveryCode.hash],
        );
      }
      await client.query(
        `UPDATE ape_dts_platform.recovery_steps
         SET status = 'SUCCEEDED',
             progress = '{"candidateCount":0}'::jsonb,
             started_at = COALESCE(started_at, statement_timestamp()),
             finished_at = statement_timestamp(),
             updated_at = statement_timestamp()
         WHERE recovery_session_id = $1::uuid
           AND status IN ('PENDING', 'RUNNING', 'PAUSED')`,
        [input.recoverySessionId],
      );
      await client.query(
        `UPDATE ape_dts_platform.recovery_sessions
         SET state = 'READY', ended_at = statement_timestamp(), row_version = row_version + 1
         WHERE id = $1::uuid AND state = 'RUNNING'`,
        [input.recoverySessionId],
      );
      await this.appendAuditEvent(client, {
        action: 'INITIAL_ADMINISTRATOR_TOTP_ENROLLED',
        actorType: 'DEPLOYMENT_ADMIN',
        actorUserId: null,
        details: {},
        eventId: randomUUID(),
        occurredAt: new Date().toISOString(),
        subjectId: input.administratorId,
        subjectType: 'USER',
      });
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async loadActivePassphraseWrapper(pool: Pool): Promise<WrappedKey> {
    const result = await pool.query<WrapperRow>(
      `SELECT kdf_algorithm, kdf_parameters, salt, nonce, ciphertext, tag
       FROM ape_dts_platform.key_wrappers
       WHERE wrapper_type = 'PASSPHRASE' AND active = true`,
    );
    if (result.rowCount !== 1) {
      throw new Error('ACTIVE_PASSPHRASE_WRAPPER_NOT_UNIQUE');
    }
    const row = result.rows[0];
    if (row === undefined || row.kdf_algorithm !== 'ARGON2ID') {
      throw new Error('ACTIVE_PASSPHRASE_WRAPPER_INVALID');
    }
    const parameters = row.kdf_parameters as Parameters<typeof validateArgon2idParameters>[0];
    validateArgon2idParameters(parameters);
    return {
      cipherAlgorithm: 'AES_256_GCM',
      ciphertext: row.ciphertext.toString('base64'),
      kdf: {
        algorithm: 'ARGON2ID',
        parameters,
        salt: row.salt.toString('base64'),
      },
      nonce: row.nonce.toString('base64'),
      tag: row.tag.toString('base64'),
      version: 1,
      wrapperType: 'PASSPHRASE',
    };
  }

  async startExistingRecovery(pool: Pool, instanceId: string): Promise<string> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
        AUDIT_LOCK_NAMESPACE,
        AUDIT_LOCK_KEY,
      ]);
      const recoverySessionId = randomUUID();
      const startedAt = new Date().toISOString();
      await client.query(
        `INSERT INTO ape_dts_platform.platform_instances
          (id, platform_version, started_at, last_heartbeat_at, process_id)
         VALUES ($1::uuid, $2, $3::timestamptz, $3::timestamptz, $4)`,
        [instanceId, PLATFORM_VERSION, startedAt, process.pid],
      );
      await client.query(
        `INSERT INTO ape_dts_platform.recovery_sessions
          (id, platform_instance_id, state)
         VALUES ($1::uuid, $2::uuid, 'RUNNING')`,
        [recoverySessionId, instanceId],
      );
      await client.query(
        `UPDATE ape_dts_platform.platform_instances
         SET current_recovery_session_id = $1::uuid
         WHERE id = $2::uuid AND current_recovery_session_id IS NULL`,
        [recoverySessionId, instanceId],
      );
      const stepIds = RECOVERY_STEPS.map(() => randomUUID());
      const positions = RECOVERY_STEPS.map((_, index) => index + 1);
      const statuses = RECOVERY_STEPS.map((_, index) => (index < 2 ? 'SUCCEEDED' : 'PENDING'));
      await client.query(
        `INSERT INTO ape_dts_platform.recovery_steps
          (id, recovery_session_id, step_type, position, status, progress, started_at, finished_at)
         SELECT id, $1::uuid, step_type, position, status,
                CASE step_type
                  WHEN 'LOAD_METADATA' THEN '{"metadataLoaded":true}'::jsonb
                  WHEN 'UNLOCK_KEYS' THEN '{"keysUnlocked":true}'::jsonb
                  ELSE '{}'::jsonb
                END,
                CASE WHEN status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END,
                CASE WHEN status = 'SUCCEEDED' THEN statement_timestamp() ELSE NULL END
         FROM unnest($2::uuid[], $3::text[], $4::smallint[], $5::text[])
           AS steps(id, step_type, position, status)`,
        [recoverySessionId, stepIds, [...RECOVERY_STEPS], positions, statuses],
      );
      await this.appendAuditEvent(client, {
        action: 'PLATFORM_UNLOCKED',
        actorType: 'DEPLOYMENT_ADMIN',
        actorUserId: null,
        details: { recoverySessionId },
        eventId: randomUUID(),
        occurredAt: startedAt,
        subjectId: instanceId,
        subjectType: 'PLATFORM_INSTANCE',
      });
      await client.query('COMMIT');
      return recoverySessionId;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async recordMigrationConfirmation(
    transaction: MigrationTransaction,
    instanceId: string,
    pendingVersions: readonly number[],
  ): Promise<void> {
    const previous = await transaction.query(
      `SELECT encode(event_hash, 'base64') AS event_hash
       FROM ape_dts_platform.audit_events
       ORDER BY sequence DESC LIMIT 1`,
    );
    const previousEncoded = previous.rows[0]?.event_hash;
    const previousHash =
      typeof previousEncoded === 'string' ? Buffer.from(previousEncoded, 'base64') : null;
    const event: AuditHashInput = {
      action: 'SCHEMA_MIGRATION_CONFIRMED',
      actorType: 'DEPLOYMENT_ADMIN',
      actorUserId: null,
      details: { backupConfirmed: true, pendingVersions: [...pendingVersions] },
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      subjectId: instanceId,
      subjectType: 'PLATFORM_INSTANCE',
    };
    await transaction.query(
      `INSERT INTO ape_dts_platform.audit_events
        (event_id, actor_type, action, subject_type, subject_id, canonical_details,
         previous_hash, event_hash, occurred_at)
       VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6::jsonb, $7, $8, $9::timestamptz)`,
      [
        event.eventId,
        event.actorType,
        event.action,
        event.subjectType,
        event.subjectId,
        JSON.stringify(event.details),
        previousHash,
        calculateAuditEventHash(previousHash, event),
        event.occurredAt,
      ],
    );
  }

  private async appendAuditEvent(client: PoolClient, event: AuditHashInput): Promise<void> {
    const previous = await client.query<PreviousHashRow>(
      'SELECT event_hash FROM ape_dts_platform.audit_events ORDER BY sequence DESC LIMIT 1',
    );
    const previousHash = previous.rows[0]?.event_hash ?? null;
    await client.query(
      `INSERT INTO ape_dts_platform.audit_events
        (event_id, actor_type, actor_user_id, action, subject_type, subject_id,
         canonical_details, previous_hash, event_hash, occurred_at)
       VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6::uuid, $7::jsonb, $8, $9, $10::timestamptz)`,
      [
        event.eventId,
        event.actorType,
        event.actorUserId,
        event.action,
        event.subjectType,
        event.subjectId,
        JSON.stringify(event.details),
        previousHash,
        calculateAuditEventHash(previousHash, event),
        event.occurredAt,
      ],
    );
  }
}
