import { Injectable } from '@nestjs/common';

import { MetadataAvailabilityService } from '../metadata-availability/metadata-availability.service.js';
import type { EmergencyMetadataStatePort } from './emergency-ports.js';

@Injectable()
export class EmergencyMetadataAdapter implements EmergencyMetadataStatePort {
  constructor(private readonly availability: MetadataAvailabilityService) {}

  isMetadataUnavailable(): boolean {
    return this.availability.isMetadataUnavailable();
  }
}
