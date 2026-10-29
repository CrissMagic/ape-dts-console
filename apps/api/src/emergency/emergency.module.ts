import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { MetadataAvailabilityModule } from '../metadata-availability/metadata-availability.module.js';
import { RuntimeModule } from '../runtime/runtime.module.js';
import { EmergencyController } from './emergency.controller.js';
import { EmergencyMetadataAdapter } from './emergency-metadata.adapter.js';
import { EmergencyPendingEventQueue } from './emergency-pending-events.js';
import { EmergencyReconciliationService } from './emergency-reconciliation.service.js';
import {
  EMERGENCY_METADATA_STATE_PORT,
  EMERGENCY_RUNTIME_PORT,
  EMERGENCY_STRUCTURED_LOG_PORT,
  type EmergencyStructuredLogPort,
} from './emergency-ports.js';
import { EmergencyRuntimeAdapter } from './emergency-runtime.adapter.js';
import { EmergencyService } from './emergency.service.js';
import { EmergencyStructuredLoggerAdapter } from './emergency-structured-logger.adapter.js';

const EMERGENCY_PENDING_EVENT_CAPACITY = 1_000;

@Module({
  controllers: [EmergencyController],
  exports: [EmergencyPendingEventQueue],
  imports: [AuditModule, BootstrapModule, MetadataAvailabilityModule, RuntimeModule],
  providers: [
    EmergencyService,
    EmergencyReconciliationService,
    EmergencyRuntimeAdapter,
    EmergencyMetadataAdapter,
    EmergencyStructuredLoggerAdapter,
    { provide: EMERGENCY_RUNTIME_PORT, useExisting: EmergencyRuntimeAdapter },
    { provide: EMERGENCY_METADATA_STATE_PORT, useExisting: EmergencyMetadataAdapter },
    { provide: EMERGENCY_STRUCTURED_LOG_PORT, useExisting: EmergencyStructuredLoggerAdapter },
    {
      inject: [EMERGENCY_STRUCTURED_LOG_PORT],
      provide: EmergencyPendingEventQueue,
      useFactory: (log: EmergencyStructuredLogPort) =>
        new EmergencyPendingEventQueue(EMERGENCY_PENDING_EVENT_CAPACITY, log),
    },
  ],
})
export class EmergencyModule {}
