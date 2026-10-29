import { Module } from '@nestjs/common';
import { CertifiedAdapterRegistry } from '@scs/config-model';
import { LogPipeline, RuntimeLogPipelineSink } from '@scs/observability';
import { ArtifactCache, type ObjectStorageDriver } from '@scs/storage';

import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { LogLifecycleModule } from '../log-lifecycle/log-lifecycle.module.js';
import { LogPipelineCoordinatorService } from '../log-lifecycle/log-pipeline-coordinator.service.js';
import { PgResourcesModule } from '../pg-resources/pg-resources.module.js';
import { PreflightCoreModule } from '../preflight/preflight-core.module.js';
import { PREFLIGHT_PLATFORM_VERSION } from '../preflight/preflight.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { StorageModule } from '../storage/storage.module.js';
import { ARTIFACT_CACHE, LOCAL_OBJECT_STORAGE, LOG_PIPELINE } from '../storage/storage.tokens.js';
import { AuthenticatedRuntimeLaunchSpecFactory } from './authenticated-runtime-launch-spec.js';
import { redactRuntimeLog, RuntimeProcessSupervisor } from './process-supervisor.js';
import { RuntimeLaunchSpecFactory } from './runtime-launch-spec.js';
import { RuntimeMetricsPortAllocator } from './runtime-metrics-port-allocator.js';
import { RuntimeSupervisorService } from './runtime-supervisor.service.js';
import { RuntimeController } from './runtime.controller.js';
import { RuntimeReadService } from './runtime-read.service.js';
import { RuntimeLogReadService } from './runtime-log-read.service.js';
import { RuntimePerformanceCollector } from './performance-collector.js';
import { createRuntimePerformanceCollector } from './performance-collection.js';
import { RuntimePerformanceStore } from './performance-store.js';
import { RuntimePerformanceReadService } from './performance-read.service.js';
import { RuntimePerformanceController } from './performance.controller.js';
import { DataSourcesModule } from '../data-sources/data-sources.module.js';
import { MysqlRuntimeStateStore } from './mysql-runtime-state.js';
import { RuntimeRecoverySettingsModule } from '../settings/runtime-recovery.module.js';
import { TaskProgressModule } from '../task-progress/task-progress.module.js';
import { PostgresReplicationWatchdog } from './postgres-replication-watchdog.service.js';
import { AlertsModule } from '../alerts/alerts.module.js';

export function runtimeMetricsPortAllocatorFromEnvironment(
  environment: NodeJS.ProcessEnv,
): RuntimeMetricsPortAllocator | undefined {
  const minimum = environment.SCS_RUNTIME_METRICS_PORT_MIN;
  const maximum = environment.SCS_RUNTIME_METRICS_PORT_MAX;
  if (minimum === undefined || maximum === undefined) return undefined;
  try {
    return new RuntimeMetricsPortAllocator({
      maxPort: Number(maximum),
      minPort: Number(minimum),
    });
  } catch {
    // Runtime configuration is evaluated again at task start. Keeping the API
    // available allows administrators to diagnose and correct invalid values.
    return undefined;
  }
}

@Module({
  controllers: [RuntimeController, RuntimePerformanceController],
  exports: [RuntimeLaunchSpecFactory, RuntimeSupervisorService],
  imports: [
    AuditModule,
    AuthModule,
    BootstrapModule,
    DataSourcesModule,
    LogLifecycleModule,
    PgResourcesModule,
    PreflightCoreModule,
    RuntimeRecoverySettingsModule,
    StorageModule,
    TaskProgressModule,
    AlertsModule,
  ],
  providers: [
    PostgresReplicationWatchdog,
    RuntimeSupervisorService,
    MysqlRuntimeStateStore,
    RuntimeReadService,
    RuntimeLogReadService,
    RuntimePerformanceStore,
    RuntimePerformanceReadService,
    {
      provide: RuntimePerformanceCollector,
      inject: [PlatformDatabaseService, RuntimeSupervisorService, RuntimePerformanceStore],
      useFactory: createRuntimePerformanceCollector,
    },
    {
      provide: RuntimeMetricsPortAllocator,
      useFactory: () => runtimeMetricsPortAllocatorFromEnvironment(process.env),
    },
    {
      inject: [
        ARTIFACT_CACHE,
        PlatformDatabaseService,
        LOCAL_OBJECT_STORAGE,
        PREFLIGHT_PLATFORM_VERSION,
        CertifiedAdapterRegistry,
        UnlockedKeyService,
      ],
      provide: RuntimeLaunchSpecFactory,
      useFactory: (
        artifactCache: ArtifactCache,
        database: PlatformDatabaseService,
        localObjectStorage: ObjectStorageDriver,
        platformVersion: string,
        adapters: CertifiedAdapterRegistry,
        unlockedKey: UnlockedKeyService,
      ) =>
        new AuthenticatedRuntimeLaunchSpecFactory({
          adapters,
          artifactCache,
          database,
          localObjectStorage,
          platformVersion,
          unlockedKey,
        }),
    },
    {
      inject: [LOG_PIPELINE, LogPipelineCoordinatorService],
      provide: RuntimeProcessSupervisor,
      useFactory: (pipeline: LogPipeline, coordinator: LogPipelineCoordinatorService) =>
        new RuntimeProcessSupervisor({
          awaitSpawn: true,
          beforeStart: async () => {
            await coordinator.synchronizeFromDatabase();
          },
          logSink: new RuntimeLogPipelineSink(pipeline),
          redact: redactRuntimeLog,
        }),
    },
  ],
})
export class RuntimeModule {}
