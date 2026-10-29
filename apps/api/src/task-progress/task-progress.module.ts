import { Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { DataSourcesModule } from '../data-sources/data-sources.module.js';
import { TaskProgressController } from './task-progress.controller.js';
import { TaskProgressService } from './task-progress.service.js';

@Module({
  imports: [AuthModule, BootstrapModule, DataSourcesModule],
  controllers: [TaskProgressController],
  providers: [TaskProgressService],
  exports: [TaskProgressService],
})
export class TaskProgressModule {}
