import { Module } from '@nestjs/common';

import { HealthController } from './health.controller.js';
import { PlatformReadinessService } from './platform-readiness.service.js';

@Module({
  controllers: [HealthController],
  exports: [PlatformReadinessService],
  providers: [PlatformReadinessService],
})
export class HealthModule {}
