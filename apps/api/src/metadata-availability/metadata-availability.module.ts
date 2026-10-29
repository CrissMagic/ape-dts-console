import { Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';

import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { HealthModule } from '../health/health.module.js';
import { MetadataAvailabilityGuard } from './metadata-availability.guard.js';
import { MetadataAvailabilityService } from './metadata-availability.service.js';
import { MetadataConnectivityExceptionFilter } from './metadata-connectivity-exception.filter.js';

@Module({
  exports: [MetadataAvailabilityService],
  imports: [BootstrapModule, HealthModule],
  providers: [
    MetadataAvailabilityService,
    {
      provide: APP_GUARD,
      useClass: MetadataAvailabilityGuard,
    },
    {
      provide: APP_FILTER,
      useClass: MetadataConnectivityExceptionFilter,
    },
  ],
})
export class MetadataAvailabilityModule {}
