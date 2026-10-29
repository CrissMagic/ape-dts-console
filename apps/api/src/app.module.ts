import { Module } from '@nestjs/common';

import { ArtifactsModule } from './artifacts/artifacts.module.js';
import { AuditQueryModule } from './audit-query/audit-query.module.js';
import { AuthModule } from './auth/auth.module.js';
import { BootstrapModule } from './bootstrap/bootstrap.module.js';
import { BootstrapFlowModule } from './bootstrap/bootstrap-flow.module.js';
import { DataSourcesModule } from './data-sources/data-sources.module.js';
import { EmergencyModule } from './emergency/emergency.module.js';
import { HealthModule } from './health/health.module.js';
import { LogLifecycleModule } from './log-lifecycle/log-lifecycle.module.js';
import { MetadataAvailabilityModule } from './metadata-availability/metadata-availability.module.js';
import { PreflightModule } from './preflight/preflight.module.js';
import { RuntimeModule } from './runtime/runtime.module.js';
import { RecoveryModule } from './recovery/recovery.module.js';
import { TasksModule } from './tasks/tasks.module.js';
import { UsersModule } from './users/users.module.js';
import { RuntimeCapacityModule } from './settings/runtime-capacity.module.js';
import { AlertsModule } from './alerts/alerts.module.js';
import { TaskProgressModule } from './task-progress/task-progress.module.js';
import { ConfigTransferModule } from './config-transfer/config-transfer.module.js';
import { RuntimeRecoverySettingsModule } from './settings/runtime-recovery.module.js';

@Module({
  imports: [
    HealthModule,
    LogLifecycleModule,
    BootstrapModule,
    BootstrapFlowModule,
    MetadataAvailabilityModule,
    ArtifactsModule,
    AuditQueryModule,
    AuthModule,
    UsersModule,
    RuntimeCapacityModule,
    RuntimeRecoverySettingsModule,
    AlertsModule,
    TaskProgressModule,
    ConfigTransferModule,
    DataSourcesModule,
    TasksModule,
    PreflightModule,
    RuntimeModule,
    RecoveryModule,
    EmergencyModule,
  ],
})
export class AppModule {}
