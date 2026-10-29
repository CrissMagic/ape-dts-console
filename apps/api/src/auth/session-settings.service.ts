import { ConflictException, Injectable, ServiceUnavailableException } from '@nestjs/common';
import {
  sessionSettingsSchema,
  updateSessionSettingsRequestSchema,
  type SessionSettings,
  type UpdateSessionSettingsRequest,
} from '@scs/contracts';
import type { PoolClient } from 'pg';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';

@Injectable()
export class SessionSettingsService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
  ) {}

  async getWithin(client: Pick<PoolClient, 'query'>): Promise<SessionSettings> {
    const result = await client.query<{
      idle_timeout_minutes: number;
      absolute_timeout_hours: number;
      row_version: string;
    }>(`SELECT idle_timeout_minutes, absolute_timeout_hours, row_version::text
          FROM ape_dts_platform.auth_session_settings WHERE singleton_id=1`);
    const row = result.rows[0];
    const parsed = sessionSettingsSchema.safeParse(
      row === undefined
        ? null
        : {
            idleTimeoutMinutes: row.idle_timeout_minutes,
            absoluteTimeoutHours: row.absolute_timeout_hours,
            rowVersion: row.row_version,
          },
    );
    if (!parsed.success)
      throw new ServiceUnavailableException({ code: 'SESSION_SETTINGS_UNAVAILABLE' });
    return parsed.data;
  }

  get(): Promise<SessionSettings> {
    return this.getWithin(this.database.requirePool());
  }

  async update(
    actorUserId: string,
    request: UpdateSessionSettingsRequest,
  ): Promise<SessionSettings> {
    const input = updateSessionSettingsRequestSchema.parse(request);
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'SELECT singleton_id FROM ape_dts_platform.auth_session_settings WHERE singleton_id=1 FOR UPDATE',
      );
      const previous = await this.getWithin(client);
      if (previous.rowVersion !== input.expectedRowVersion)
        throw new ConflictException({ code: 'SESSION_SETTINGS_VERSION_CONFLICT' });
      await client.query(
        `UPDATE ape_dts_platform.auth_session_settings
        SET idle_timeout_minutes=$1, absolute_timeout_hours=$2, row_version=row_version+1,
            updated_by_user_id=$3::uuid, updated_at=statement_timestamp() WHERE singleton_id=1`,
        [input.idleTimeoutMinutes, input.absoluteTimeoutHours, actorUserId],
      );
      const updated = await this.getWithin(client);
      await this.audit.append(client, {
        action: 'SESSION_SETTINGS_UPDATED',
        actorType: 'USER',
        actorUserId,
        subjectType: 'SESSION_SETTINGS',
        subjectId: null,
        details: { previous, updated, appliesTo: 'NEW_SESSIONS' },
      });
      await client.query('COMMIT');
      return updated;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
