import {
  type CanActivate,
  type ExecutionContext,
  Injectable,
  ServiceUnavailableException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';

import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import {
  METADATA_AVAILABILITY_BYPASS,
  METADATA_AVAILABILITY_RECOVERY_ACCESS,
} from './metadata-availability.decorator.js';
import {
  MetadataAvailabilityError,
  MetadataAvailabilityService,
} from './metadata-availability.service.js';

@Injectable()
export class MetadataAvailabilityGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly availability: MetadataAvailabilityService,
    private readonly bootstrapState: BootstrapStateService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const bypass = this.reflector.getAllAndOverride<boolean>(METADATA_AVAILABILITY_BYPASS, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (bypass === true) {
      return true;
    }
    const recoveryAccess = this.reflector.getAllAndOverride<boolean>(
      METADATA_AVAILABILITY_RECOVERY_ACCESS,
      [context.getHandler(), context.getClass()],
    );
    if (recoveryAccess === true && this.bootstrapState.getStatus().phase === 'RECOVERING') {
      return true;
    }

    try {
      await this.availability.ensureManagementAvailable();
      return true;
    } catch (error) {
      if (error instanceof MetadataAvailabilityError) {
        throw new ServiceUnavailableException({
          code: error.code,
          phase: error.phase,
          status: 'unavailable',
        });
      }
      throw error;
    }
  }
}
