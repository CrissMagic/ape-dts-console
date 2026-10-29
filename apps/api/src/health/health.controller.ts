import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import type {
  LiveHealthResponse,
  ReadyHealthResponse,
  UnavailableHealthResponse,
} from '@scs/contracts';

import { PlatformReadinessService } from './platform-readiness.service.js';
import { AllowWhenMetadataUnavailable } from '../metadata-availability/metadata-availability.decorator.js';

@AllowWhenMetadataUnavailable()
@Controller('health')
export class HealthController {
  constructor(
    @Inject(PlatformReadinessService)
    private readonly readiness: PlatformReadinessService,
  ) {}

  @Get('live')
  live(): LiveHealthResponse {
    return { status: 'ok' };
  }

  @Get('ready')
  ready(): ReadyHealthResponse {
    const phase = this.readiness.getPhase();
    if (phase !== 'READY') {
      const response: UnavailableHealthResponse = {
        code: 'PLATFORM_NOT_READY',
        phase,
        status: 'not_ready',
      };
      throw new ServiceUnavailableException(response);
    }

    return {
      phase,
      status: 'ready',
    };
  }
}
