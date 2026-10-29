import { Module } from '@nestjs/common';
import { ApeDtsV1Adapter, CertifiedAdapterRegistry } from '@scs/config-model';

import { AuditModule } from '../audit/audit.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { DataSourcesModule } from '../data-sources/data-sources.module.js';
import { PgResourcesModule } from '../pg-resources/pg-resources.module.js';
import { platformVersion } from '../platform-version.js';
import {
  PREFLIGHT_PLATFORM_VERSION,
  PreflightService,
  SYSTEM_RECOVERY_PREFLIGHT,
} from './preflight.service.js';
import { ContinuityService } from './continuity.service.js';
import {
  RUNTIME_ENVIRONMENT_CHECK,
  RuntimeEnvironmentInspector,
} from './runtime-environment-check.js';

/** Domain preflight providers without the authenticated HTTP controller. */
@Module({
  exports: [
    CertifiedAdapterRegistry,
    PREFLIGHT_PLATFORM_VERSION,
    PreflightService,
    ContinuityService,
    SYSTEM_RECOVERY_PREFLIGHT,
  ],
  imports: [AuditModule, BootstrapModule, DataSourcesModule, PgResourcesModule],
  providers: [
    { provide: RUNTIME_ENVIRONMENT_CHECK, useFactory: () => new RuntimeEnvironmentInspector() },
    PreflightService,
    ContinuityService,
    { provide: SYSTEM_RECOVERY_PREFLIGHT, useExisting: PreflightService },
    {
      provide: CertifiedAdapterRegistry,
      useFactory: () => new CertifiedAdapterRegistry([new ApeDtsV1Adapter()]),
    },
    { provide: PREFLIGHT_PLATFORM_VERSION, useFactory: platformVersion },
  ],
})
export class PreflightCoreModule {}
