import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { AuthController } from './auth.controller.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from './auth.guards.js';
import { AuthService } from './auth.service.js';
import { SessionSettingsController } from './session-settings.controller.js';
import { SessionSettingsService } from './session-settings.service.js';

@Module({
  controllers: [AuthController, SessionSettingsController],
  exports: [AdminGuard, AuthService, CsrfGuard, SessionAuthGuard],
  imports: [AuditModule, BootstrapModule],
  providers: [AdminGuard, AuthService, CsrfGuard, SessionAuthGuard, SessionSettingsService],
})
export class AuthModule {}
