import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { PgResourcesModule } from '../pg-resources/pg-resources.module.js';
import { RiskAcceptanceController } from './risk-acceptance.controller.js';
import { RiskAcceptanceService } from './risk-acceptance.service.js';
import { TaskGroupsController, TasksController } from './tasks.controller.js';
import { TasksService } from './tasks.service.js';

@Module({
  controllers: [RiskAcceptanceController, TasksController, TaskGroupsController],
  exports: [TasksService],
  imports: [AuditModule, AuthModule, BootstrapModule, PgResourcesModule],
  providers: [RiskAcceptanceService, TasksService],
})
export class TasksModule {}
