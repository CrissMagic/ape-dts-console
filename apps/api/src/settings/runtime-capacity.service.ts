import { ConflictException, Injectable, ServiceUnavailableException } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';

interface SettingsRow {
  max_runtime_units: number | null;
  row_version: string;
}

@Injectable()
export class RuntimeCapacityService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
  ) {}

  private async snapshot(client: PoolClient) {
    const settings = await client.query<SettingsRow>(
      `SELECT max_runtime_units, row_version::text FROM ape_dts_platform.platform_settings
       WHERE singleton_id = 1 FOR UPDATE`,
    );
    const row = settings.rows[0];
    if (!row) throw new ServiceUnavailableException({ code: 'PLATFORM_SETTINGS_UNAVAILABLE' });
    const reservations = await client.query<{ used: string }>(
      `SELECT count(*)::text AS used FROM ape_dts_platform.capacity_reservations
       WHERE state IN ('RESERVED', 'ACTIVE')`,
    );
    const used = Number(reservations.rows[0]!.used);
    return { maxRuntimeUnits: row.max_runtime_units, rowVersion: row.row_version, used };
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

  get() {
    return this.transaction((client) => this.snapshot(client));
  }

  update(actorUserId: string, input: { maxRuntimeUnits: number; expectedRowVersion: string }) {
    return this.transaction(async (client) => {
      // This is the same singleton lock acquired by capacity reservation.
      const current = await this.snapshot(client);
      if (current.rowVersion !== input.expectedRowVersion) {
        throw new ConflictException({ code: 'CAPACITY_VERSION_CONFLICT' });
      }
      if (input.maxRuntimeUnits < current.used) {
        throw new ConflictException({ code: 'CAPACITY_BELOW_USAGE', used: current.used });
      }
      const updated = await client.query<{ row_version: string }>(
        `UPDATE ape_dts_platform.platform_settings
         SET max_runtime_units = $1, row_version = row_version + 1,
             updated_by = $2::uuid, updated_at = statement_timestamp()
         WHERE singleton_id = 1 AND row_version = $3::bigint
         RETURNING row_version::text`,
        [input.maxRuntimeUnits, actorUserId, input.expectedRowVersion],
      );
      if (!updated.rows[0]) throw new ConflictException({ code: 'CAPACITY_VERSION_CONFLICT' });
      await this.audit.append(client, {
        action: 'RUNTIME_CAPACITY_UPDATED',
        actorType: 'USER',
        actorUserId,
        subjectType: 'PLATFORM_SETTINGS',
        subjectId: null,
        details: {
          previousLimit: current.maxRuntimeUnits,
          limit: input.maxRuntimeUnits,
          used: current.used,
        },
      });
      return {
        maxRuntimeUnits: input.maxRuntimeUnits,
        rowVersion: updated.rows[0].row_version,
        used: current.used,
      };
    });
  }
}
