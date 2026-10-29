import {
  BadRequestException,
  ConflictException,
  Controller,
  ForbiddenException,
  NotFoundException,
  Param,
  Post,
  Body,
  Req,
  UseGuards,
} from '@nestjs/common';
import {
  acceptTaskContinuityRequestSchema,
  runTaskPreflightRequestSchema,
  taskIdentifierParametersSchema,
} from '@scs/contracts';
import { ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { PreflightService, PreflightServiceError } from './preflight.service.js';
import { ContinuityService, ContinuityServiceError } from './continuity.service.js';

function parse<T>(schema: ZodType<T>, value: unknown): T {
  try {
    return schema.parse(value);
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

function mapPreflightError(error: unknown): never {
  if (error instanceof ContinuityServiceError) {
    switch (error.code) {
      case 'CONTINUITY_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code });
      case 'CONTINUITY_OBSERVATION_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      default:
        throw new ConflictException({ code: error.code });
    }
  }
  if (error instanceof PreflightServiceError) {
    switch (error.code) {
      case 'PREFLIGHT_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code });
      case 'PREFLIGHT_TASK_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      case 'PREFLIGHT_REVISION_NOT_CURRENT':
      case 'PREFLIGHT_ALREADY_RUNNING':
      case 'PREFLIGHT_TASK_VERSION_CONFLICT':
        throw new ConflictException({ code: error.code });
    }
  }
  throw error;
}

@Controller('tasks')
export class PreflightController {
  constructor(
    private readonly preflight: PreflightService,
    private readonly continuity: ContinuityService,
  ) {}

  @Post(':taskId/continuity-acceptances')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async acceptContinuity(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.continuity.accept(
        requireAuthContext(request).user,
        taskId,
        parse(acceptTaskContinuityRequestSchema, body),
      );
    } catch (error) {
      mapPreflightError(error);
    }
  }

  @Post(':taskId/preflight')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async run(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.preflight.run(
        requireAuthContext(request).user,
        taskId,
        parse(runTaskPreflightRequestSchema, body),
      );
    } catch (error) {
      mapPreflightError(error);
    }
  }
}
