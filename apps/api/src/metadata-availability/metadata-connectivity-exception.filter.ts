import { ArgumentsHost, Catch, Injectable, ServiceUnavailableException } from '@nestjs/common';
import { BaseExceptionFilter, HttpAdapterHost } from '@nestjs/core';

import { MetadataAvailabilityService } from './metadata-availability.service.js';

@Catch()
@Injectable()
export class MetadataConnectivityExceptionFilter extends BaseExceptionFilter {
  constructor(
    adapterHost: HttpAdapterHost,
    private readonly availability: MetadataAvailabilityService,
  ) {
    super(adapterHost.httpAdapter);
  }

  override catch(exception: unknown, host: ArgumentsHost): void {
    if (this.availability.reportFailure(exception, 'request_failure')) {
      super.catch(
        new ServiceUnavailableException({
          code: 'METADATA_DATABASE_UNAVAILABLE',
          phase: 'METADATA_DEGRADED',
          status: 'unavailable',
        }),
        host,
      );
      return;
    }
    super.catch(exception, host);
  }
}
