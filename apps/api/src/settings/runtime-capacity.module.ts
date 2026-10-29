import { Module } from '@nestjs/common';
import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { RuntimeCapacityController } from './runtime-capacity.controller.js';
import { RuntimeCapacityService } from './runtime-capacity.service.js';
import {
  RUNTIME_ENVIRONMENT_CHECK,
  RuntimeEnvironmentInspector,
} from '../preflight/runtime-environment-check.js';

@Module({
  imports: [AuthModule, BootstrapModule, AuditModule],
  controllers: [RuntimeCapacityController],
  providers: [
    RuntimeCapacityService,
    { provide: RUNTIME_ENVIRONMENT_CHECK, useFactory: () => new RuntimeEnvironmentInspector() },
  ],
})
export class RuntimeCapacityModule {}
