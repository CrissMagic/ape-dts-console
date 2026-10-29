import { Injectable, Logger, type OnApplicationShutdown } from '@nestjs/common';

import { EmergencyStopCodeService } from './emergency-stop-code.service.js';

@Injectable()
export class EmergencyStopCodeLifecycleService implements OnApplicationShutdown {
  constructor(private readonly stopCode: EmergencyStopCodeService) {}

  activateForReadyPlatform(): void {
    this.stopCode.activate();
    const emergencyStopCode = this.stopCode.consumePlaintextOnce();
    if (emergencyStopCode === undefined) {
      throw new Error('EMERGENCY_CODE_PLAINTEXT_UNAVAILABLE');
    }
    Logger.warn(
      JSON.stringify({
        emergencyStopCode,
        event: 'emergency.stop_code_activated',
        message: 'Store this process-lifetime code for metadata-outage emergency stops',
      }),
      'EmergencyAccess',
    );
  }

  onApplicationShutdown(): void {
    this.stopCode.clear();
  }
}
