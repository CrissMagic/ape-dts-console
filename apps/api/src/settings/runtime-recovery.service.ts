import { ConflictException, Injectable, Logger } from '@nestjs/common';
import {
  runtimeRecoverySettingsSchema,
  type RuntimeRecoveryPolicy,
  type UpdateRuntimeRecoverySettings,
} from '@scs/contracts';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';

@Injectable()
export class RuntimeRecoverySettingsService {
  private cached: RuntimeRecoveryPolicy | undefined;
  private readonly logger = new Logger(RuntimeRecoverySettingsService.name);
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
  ) {}
  async get() {
    const row = (
      await this.database
        .requirePool()
        .query(
          'SELECT enabled,max_attempts AS "maxAttempts",retry_delay_seconds AS "retryDelaySeconds",row_version::text AS "rowVersion" FROM ape_dts_platform.runtime_recovery_settings WHERE singleton_id=1',
        )
    ).rows[0];
    const settings = runtimeRecoverySettingsSchema.parse(row);
    this.cached = settings;
    return settings;
  }
  async policy(): Promise<RuntimeRecoveryPolicy> {
    try {
      return await this.get();
    } catch {
      this.logger.warn({
        code: 'RUNTIME_RECOVERY_SETTINGS_UNAVAILABLE',
        usingCachedPolicy: this.cached !== undefined,
      });
      // Never enable unconfigured recovery when the database/key state is unknown.
      return this.cached ?? { enabled: false, maxAttempts: 1, retryDelaySeconds: 30 };
    }
  }
  async update(actorId: string, input: UpdateRuntimeRecoverySettings) {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const row = (
        await client.query(
          `UPDATE ape_dts_platform.runtime_recovery_settings SET enabled=$1,max_attempts=$2,retry_delay_seconds=$3,row_version=row_version+1,updated_at=statement_timestamp()
         WHERE singleton_id=1 AND row_version=$4::bigint
         RETURNING enabled,max_attempts AS "maxAttempts",retry_delay_seconds AS "retryDelaySeconds",row_version::text AS "rowVersion"`,
          [input.enabled, input.maxAttempts, input.retryDelaySeconds, input.expectedRowVersion],
        )
      ).rows[0];
      if (!row) throw new ConflictException({ code: 'RUNTIME_RECOVERY_SETTINGS_CONFLICT' });
      const result = runtimeRecoverySettingsSchema.parse(row);
      await this.audit.append(client, {
        actorType: 'USER',
        actorUserId: actorId,
        action: 'RUNTIME_RECOVERY_SETTINGS_UPDATED',
        subjectType: 'PLATFORM_SETTINGS',
        subjectId: null,
        details: {
          enabled: result.enabled,
          maxAttempts: result.maxAttempts,
          retryDelaySeconds: result.retryDelaySeconds,
        },
      });
      await client.query('COMMIT');
      this.cached = result;
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
