import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  ForbiddenException,
  NotFoundException,
  Param,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import {
  acceptTaskRevisionRiskRequestSchema,
  taskIdentifierParametersSchema,
} from '@scs/contracts';
import { ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { RiskAcceptanceService, RiskAcceptanceServiceError } from './risk-acceptance.service.js';

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

function mapRiskAcceptanceError(error: unknown): never {
  if (error instanceof RiskAcceptanceServiceError) {
    switch (error.code) {
      case 'RISK_ACCEPTANCE_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code });
      case 'RISK_ACCEPTANCE_TASK_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      case 'RISK_ACCEPTANCE_ALREADY_EXISTS':
      case 'RISK_ACCEPTANCE_PREFLIGHT_REQUIRED':
      case 'RISK_ACCEPTANCE_REVISION_NOT_CURRENT':
      case 'RISK_ACCEPTANCE_REVISION_VERSION_CONFLICT':
      case 'RISK_ACCEPTANCE_TASK_VERSION_CONFLICT':
        throw new ConflictException({ code: error.code });
    }
  }
  throw error;
}

@Controller('tasks')
export class RiskAcceptanceController {
  constructor(private readonly riskAcceptance: RiskAcceptanceService) {}

  @Post(':taskId/risk-acceptance')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async accept(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.riskAcceptance.accept(
        requireAuthContext(request).user,
        taskId,
        parse(acceptTaskRevisionRiskRequestSchema, body),
      );
    } catch (error) {
      mapRiskAcceptanceError(error);
    }
  }
}
