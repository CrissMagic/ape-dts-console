import { Module } from '@nestjs/common';

import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { AuditQueryController } from './audit-query.controller.js';
import { AuditQueryService } from './audit-query.service.js';

@Module({
  controllers: [AuditQueryController],
  imports: [AuthModule, BootstrapModule],
  providers: [AuditQueryService],
})
export class AuditQueryModule {}
