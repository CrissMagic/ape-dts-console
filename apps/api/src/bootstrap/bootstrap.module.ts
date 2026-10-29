import { Module } from '@nestjs/common';

import { MigrationBundleService } from '../database/migration-bundle.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { EmergencyStopCodeLifecycleService } from '../emergency/emergency-stop-code-lifecycle.service.js';
import { EmergencyStopCodeService } from '../emergency/emergency-stop-code.service.js';
import { HealthModule } from '../health/health.module.js';
import { ARGON2ID_POLICY, readArgon2idPolicy } from '../security/security-config.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import {
  BootstrapAccessService,
  generateInitializationCode,
  PLATFORM_BOOTSTRAP_ACCESS_POLICY,
} from './bootstrap-access.service.js';
import { BootstrapPersistenceService } from './bootstrap-persistence.service.js';
import { BootstrapRecoveryCompletionService } from './bootstrap-recovery-completion.service.js';
import { PendingEnrollmentService } from './pending-enrollment.service.js';
import { BootstrapStateService } from './bootstrap-state.service.js';

@Module({
  exports: [
    ARGON2ID_POLICY,
    BootstrapAccessService,
    BootstrapPersistenceService,
    BootstrapRecoveryCompletionService,
    BootstrapStateService,
    EmergencyStopCodeLifecycleService,
    EmergencyStopCodeService,
    HealthModule,
    MigrationBundleService,
    PendingEnrollmentService,
    PlatformDatabaseService,
    UnlockedKeyService,
  ],
  imports: [HealthModule],
  providers: [
    {
      provide: BootstrapAccessService,
      useFactory: () => {
        const initializationCode = generateInitializationCode();
        return new BootstrapAccessService(initializationCode, PLATFORM_BOOTSTRAP_ACCESS_POLICY);
      },
    },
    {
      provide: ARGON2ID_POLICY,
      useFactory: () => readArgon2idPolicy(),
    },
    BootstrapPersistenceService,
    BootstrapRecoveryCompletionService,
    EmergencyStopCodeLifecycleService,
    EmergencyStopCodeService,
    {
      provide: BootstrapStateService,
      useFactory: () => new BootstrapStateService(),
    },
    MigrationBundleService,
    PendingEnrollmentService,
    PlatformDatabaseService,
    UnlockedKeyService,
  ],
})
export class BootstrapModule {}
