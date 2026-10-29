import { Inject, Injectable, Logger, type OnApplicationShutdown } from '@nestjs/common';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { PostgresLogSegmentStore } from './postgres-log-segment-store.js';
import { POSTGRES_LOG_SEGMENT_STORE } from './storage.tokens.js';

@Injectable()
export class LogStorageRecoveryService implements OnApplicationShutdown {
  private readonly logger = new Logger(LogStorageRecoveryService.name);
  private readonly unregister: () => void;
  private recovery: Promise<void> = Promise.resolve();

  constructor(
    database: PlatformDatabaseService,
    @Inject(POSTGRES_LOG_SEGMENT_STORE) private readonly store: PostgresLogSegmentStore,
  ) {
    this.unregister = database.registerSchemaCurrentListener(() => this.scheduleRecovery());
  }

  async onApplicationShutdown(): Promise<void> {
    this.unregister();
    await this.recovery;
  }

  private scheduleRecovery(): void {
    this.recovery = this.recovery
      .then(async () => {
        const result = await this.store.recoverPendingOperations();
        if (result.failures > 0) {
          this.logger.warn(
            `Log storage recovery left ${result.failures} operation(s) pending for an explicit retry`,
          );
        }
        const recovered =
          result.availableSegments +
          result.completedDeletions +
          result.memoryPendingGaps +
          result.missingSegments +
          result.integrityFailedSegments;
        if (recovered > 0) {
          this.logger.log(
            `Recovered ${recovered} log storage operation(s): ${JSON.stringify(result)}`,
          );
        }
      })
      .catch((error: unknown) => {
        const reason =
          error instanceof Error ? error.message : 'unknown log storage recovery error';
        this.logger.error(reason);
      });
  }
}
