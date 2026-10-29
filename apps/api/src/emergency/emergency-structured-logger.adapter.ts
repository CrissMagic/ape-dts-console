import { Injectable, Logger } from '@nestjs/common';

import type { EmergencyStructuredLogEvent, EmergencyStructuredLogPort } from './emergency-ports.js';

@Injectable()
export class EmergencyStructuredLoggerAdapter implements EmergencyStructuredLogPort {
  private readonly logger = new Logger('Emergency');

  write(event: EmergencyStructuredLogEvent): void {
    this.logger.warn(JSON.stringify({ event: 'emergency.runtime_action', ...event }));
  }
}
