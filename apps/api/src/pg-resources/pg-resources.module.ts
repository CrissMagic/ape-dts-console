import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { CapacityReservationService } from './capacity-reservation.service.js';
import { PgResourceManager } from './pg-resource-manager.js';
import { PgResourcePreparationController } from './pg-resource-preparation.controller.js';
import { PgResourcePreparationService } from './pg-resource-preparation.service.js';
import { AuthModule } from '../auth/auth.module.js';
import { DataSourcesModule } from '../data-sources/data-sources.module.js';

@Module({
  controllers: [PgResourcePreparationController],
  exports: [CapacityReservationService, PgResourceManager],
  imports: [AuditModule, BootstrapModule, AuthModule, DataSourcesModule],
  providers: [CapacityReservationService, PgResourceManager, PgResourcePreparationService],
})
export class PgResourcesModule {}
