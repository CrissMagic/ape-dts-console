import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  HttpException,
  NotFoundException,
  Param,
  Post,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { z, ZodError } from 'zod';

import { AllowWhenMetadataUnavailable } from '../metadata-availability/metadata-availability.decorator.js';
import type { EmergencyRuntimeTask } from './emergency-ports.js';
import { EmergencyError, EmergencyService } from './emergency.service.js';

const forceKillBodySchema = z
  .object({
    confirmed: z.literal(true),
    reason: z.string().trim().min(1).max(2_000),
  })
  .strict();

function mapEmergencyError(error: unknown): never {
  if (error instanceof HttpException) {
    throw error;
  }
  if (error instanceof EmergencyError) {
    switch (error.code) {
      case 'EMERGENCY_ACCESS_DENIED':
        throw new UnauthorizedException({ code: error.code });
      case 'EMERGENCY_TASK_NOT_RUNNING':
        throw new NotFoundException({ code: error.code });
      case 'EMERGENCY_METADATA_AVAILABLE':
        throw new ConflictException({ code: error.code });
      case 'EMERGENCY_TERMINATION_UNCONFIRMED':
        throw new ServiceUnavailableException({ code: error.code });
      case 'EMERGENCY_FORCE_CONFIRMATION_REQUIRED':
      case 'EMERGENCY_FORCE_REASON_REQUIRED':
        throw new BadRequestException({ code: error.code });
    }
  }
  throw error;
}

function parseForceKillBody(body: unknown): { confirmed: true; reason: string } {
  try {
    return forceKillBodySchema.parse(body);
  } catch (error) {
    if (error instanceof ZodError) {
      throw new BadRequestException({
        code: 'INVALID_REQUEST',
        issues: error.issues.map(({ code, path }) => ({ code, path })),
      });
    }
    throw error;
  }
}

@AllowWhenMetadataUnavailable()
@Controller('emergency')
export class EmergencyController {
  constructor(private readonly emergency: EmergencyService) {}

  @Get('runtime-tasks')
  list(
    @Headers('x-emergency-stop-code') emergencyCode: string | undefined,
  ): readonly EmergencyRuntimeTask[] {
    try {
      return this.emergency.list(emergencyCode ?? '');
    } catch (error) {
      mapEmergencyError(error);
    }
  }

  @Post('runtime-tasks/:taskId/stop')
  async stop(
    @Param('taskId') taskId: string,
    @Headers('x-emergency-stop-code') emergencyCode: string | undefined,
  ): Promise<{ status: 'stop_requested' }> {
    try {
      await this.emergency.stop({ emergencyCode: emergencyCode ?? '', taskId });
      return { status: 'stop_requested' };
    } catch (error) {
      mapEmergencyError(error);
    }
  }

  @Post('runtime-tasks/:taskId/force-kill')
  async forceKill(
    @Param('taskId') taskId: string,
    @Headers('x-emergency-stop-code') emergencyCode: string | undefined,
    @Body() body: unknown,
  ): Promise<{ status: 'force_kill_requested' }> {
    const request = parseForceKillBody(body);
    try {
      await this.emergency.forceKill({ emergencyCode: emergencyCode ?? '', taskId, ...request });
      return { status: 'force_kill_requested' };
    } catch (error) {
      mapEmergencyError(error);
    }
  }
}
