import { Module } from '@nestjs/common';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { AuditModule } from '../audit/audit.module.js';
import { RuntimeRecoverySettingsController } from './runtime-recovery.controller.js';
import { RuntimeRecoverySettingsService } from './runtime-recovery.service.js';
@Module({
  imports: [BootstrapModule, AuthModule, AuditModule],
  controllers: [RuntimeRecoverySettingsController],
  providers: [RuntimeRecoverySettingsService],
  exports: [RuntimeRecoverySettingsService],
})
export class RuntimeRecoverySettingsModule {}
