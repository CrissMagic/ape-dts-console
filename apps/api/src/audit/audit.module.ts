import { Module } from '@nestjs/common';

import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { AuditLedgerService } from './audit-ledger.service.js';

@Module({
  exports: [AuditLedgerService],
  imports: [BootstrapModule],
  providers: [AuditLedgerService],
})
export class AuditModule {}
