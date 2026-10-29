import { Module } from '@nestjs/common';

import { RecoveryModule } from '../recovery/recovery.module.js';
import { BootstrapCoordinatorService } from './bootstrap-coordinator.service.js';
import { BootstrapController } from './bootstrap.controller.js';
import { BootstrapModule } from './bootstrap.module.js';
import { LocalPlatformStartupService } from './local-platform-startup.service.js';

/** Bootstrap HTTP/orchestration layer.  Domain modules import BootstrapModule only. */
@Module({
  controllers: [BootstrapController],
  exports: [BootstrapCoordinatorService],
  imports: [BootstrapModule, RecoveryModule],
  providers: [BootstrapCoordinatorService, LocalPlatformStartupService],
})
export class BootstrapFlowModule {}
