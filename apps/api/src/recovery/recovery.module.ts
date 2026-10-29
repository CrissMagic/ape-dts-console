import { Module } from '@nestjs/common';

import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { PreflightCoreModule } from '../preflight/preflight-core.module.js';
import { RuntimeModule } from '../runtime/runtime.module.js';
import { RuntimeSupervisorService } from '../runtime/runtime-supervisor.service.js';
import { RecoveryCoordinatorService } from './recovery-coordinator.service.js';
import {
  PostgresRecoveryExternalActions,
  RECOVERY_RUNTIME_PORT,
} from './recovery-external-actions.js';
import { PostgresRecoveryStore } from './postgres-recovery.store.js';
import { RecoveryController } from './recovery.controller.js';
import { TaskRecoveryService } from './task-recovery.service.js';
import { AuditModule } from '../audit/audit.module.js';

@Module({
  controllers: [RecoveryController],
  exports: [RecoveryCoordinatorService],
  imports: [AuditModule, AuthModule, BootstrapModule, PreflightCoreModule, RuntimeModule],
  providers: [
    TaskRecoveryService,
    PostgresRecoveryStore,
    PostgresRecoveryExternalActions,
    { provide: RECOVERY_RUNTIME_PORT, useExisting: RuntimeSupervisorService },
    {
      inject: [PostgresRecoveryStore, PostgresRecoveryExternalActions],
      provide: RecoveryCoordinatorService,
      useFactory: (store: PostgresRecoveryStore, actions: PostgresRecoveryExternalActions) =>
        new RecoveryCoordinatorService(store, actions),
    },
  ],
})
export class RecoveryModule {}
