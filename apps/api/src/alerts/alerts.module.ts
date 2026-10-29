import { Module } from '@nestjs/common';
import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { TaskProgressModule } from '../task-progress/task-progress.module.js';
import { AlertsController } from './alerts.controller.js';
import { AlertsService } from './alerts.service.js';
import { AlertsMonitorService } from './alerts-monitor.service.js';
import { PlatformHealthMonitorService } from './platform-health-monitor.service.js';
import { MetadataAvailabilityModule } from '../metadata-availability/metadata-availability.module.js';

@Module({
  imports: [
    AuthModule,
    BootstrapModule,
    AuditModule,
    TaskProgressModule,
    MetadataAvailabilityModule,
  ],
  controllers: [AlertsController],
  providers: [AlertsService, AlertsMonitorService, PlatformHealthMonitorService],
  exports: [AlertsService],
})
export class AlertsModule {}
