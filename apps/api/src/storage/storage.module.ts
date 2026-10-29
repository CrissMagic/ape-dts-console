import { Module } from '@nestjs/common';
import { LogPipeline } from '@scs/observability';
import {
  ArtifactCache,
  createLocalObjectStorage,
  LOCAL_STORAGE_ROOT,
  type LogSegmentStore,
} from '@scs/storage';

import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { LogStorageRecoveryService } from './log-storage-recovery.service.js';
import { PostgresLogSegmentStore } from './postgres-log-segment-store.js';
import {
  ARTIFACT_CACHE,
  LOCAL_LOG_STORAGE_ROOT,
  LOCAL_OBJECT_STORAGE,
  LOG_SEGMENT_STORE,
  LOG_PIPELINE,
  POSTGRES_LOG_SEGMENT_STORE,
} from './storage.tokens.js';

export const ARTIFACT_CACHE_ROOT = '/var/cache/ape-dts-platform/artifacts';

@Module({
  exports: [
    ARTIFACT_CACHE,
    LOCAL_LOG_STORAGE_ROOT,
    LOCAL_OBJECT_STORAGE,
    LOG_SEGMENT_STORE,
    LOG_PIPELINE,
    POSTGRES_LOG_SEGMENT_STORE,
  ],
  imports: [BootstrapModule],
  providers: [
    LogStorageRecoveryService,
    {
      provide: ARTIFACT_CACHE,
      useFactory: () => new ArtifactCache(ARTIFACT_CACHE_ROOT),
    },
    {
      provide: LOCAL_OBJECT_STORAGE,
      useFactory: () => createLocalObjectStorage(),
    },
    {
      provide: LOCAL_LOG_STORAGE_ROOT,
      useValue: LOCAL_STORAGE_ROOT,
    },
    {
      inject: [PlatformDatabaseService, LOCAL_LOG_STORAGE_ROOT],
      provide: POSTGRES_LOG_SEGMENT_STORE,
      useFactory: (database: PlatformDatabaseService, localRoot: string) =>
        new PostgresLogSegmentStore(database, localRoot),
    },
    {
      provide: LOG_SEGMENT_STORE,
      useExisting: POSTGRES_LOG_SEGMENT_STORE,
    },
    {
      inject: [LOG_SEGMENT_STORE],
      provide: LOG_PIPELINE,
      useFactory: (store: LogSegmentStore) => new LogPipeline({ store }),
    },
  ],
})
export class StorageModule {}
