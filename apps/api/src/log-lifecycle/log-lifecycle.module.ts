import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { StorageModule } from '../storage/storage.module.js';
import { LogLifecycleSettingsService } from './log-lifecycle-settings.service.js';
import { LogLifecycleController } from './log-lifecycle.controller.js';
import { LogPipelineCoordinatorService } from './log-pipeline-coordinator.service.js';
import { LogRetentionService } from './log-retention.service.js';

@Module({
  controllers: [LogLifecycleController],
  exports: [LogPipelineCoordinatorService],
  imports: [AuditModule, AuthModule, BootstrapModule, StorageModule],
  providers: [LogLifecycleSettingsService, LogPipelineCoordinatorService, LogRetentionService],
})
export class LogLifecycleModule {}
