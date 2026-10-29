import { Controller, Get } from '@nestjs/common';
import type { BootstrapStatusResponse } from '@scs/contracts';

import { AllowWhenMetadataUnavailable } from '../metadata-availability/metadata-availability.decorator.js';
import { BootstrapCoordinatorService } from './bootstrap-coordinator.service.js';

/** Deployment configuration is local-only; HTTP cannot initialize or unlock a platform. */
@AllowWhenMetadataUnavailable()
@Controller('bootstrap')
export class BootstrapController {
  constructor(private readonly coordinator: BootstrapCoordinatorService) {}

  @Get('status')
  status(): BootstrapStatusResponse {
    return this.coordinator.getStatus();
  }
}
