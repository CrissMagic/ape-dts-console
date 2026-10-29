import { Module } from '@nestjs/common';
import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { DataSourcesModule } from '../data-sources/data-sources.module.js';
import { TasksModule } from '../tasks/tasks.module.js';
import { ConfigTransferService } from './config-transfer.service.js';
import { ConfigTransferController } from './config-transfer.controller.js';

@Module({
  imports: [AuthModule, AuditModule, BootstrapModule, DataSourcesModule, TasksModule],
  providers: [ConfigTransferService],
  controllers: [ConfigTransferController],
})
export class ConfigTransferModule {}
