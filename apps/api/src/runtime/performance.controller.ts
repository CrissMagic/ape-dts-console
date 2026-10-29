import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  NotFoundException,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import {
  runtimePerformanceQueryRequestSchema,
  runtimePerformanceWindowSchema,
  runtimeTaskIdentifierParametersSchema,
} from '@scs/contracts';
import { ZodError } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  RuntimePerformanceNotFoundError,
  RuntimePerformanceReadService,
} from './performance-read.service.js';

function parse<T>(schema: { parse(value: unknown): T }, value: unknown): T {
  try {
    return schema.parse(value);
  } catch (error) {
    if (error instanceof ZodError)
      throw new BadRequestException({
        code: 'INVALID_REQUEST',
        issues: error.issues.map(({ code, path }) => ({ code, path })),
      });
    throw error;
  }
}

@Controller('tasks')
export class RuntimePerformanceController {
  constructor(private readonly reads: RuntimePerformanceReadService) {}

  @Post('performance/query')
  @HttpCode(200)
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async query(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const { taskIds } = parse(runtimePerformanceQueryRequestSchema, body);
    return this.reads.query(requireAuthContext(request).user, taskIds);
  }

  @Get(':taskId/performance')
  @UseGuards(SessionAuthGuard)
  async detail(
    @Param() parameters: unknown,
    @Query() query: Record<string, unknown>,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    const window = parse(runtimePerformanceWindowSchema, query.window ?? '1h');
    const runId =
      query.runId === undefined
        ? undefined
        : parse(runtimeTaskIdentifierParametersSchema, { taskId: query.runId }).taskId;
    if (Object.keys(query).some((key) => key !== 'window' && key !== 'runId'))
      throw new BadRequestException({ code: 'INVALID_REQUEST' });
    try {
      return await this.reads.detail(requireAuthContext(request).user, taskId, window, runId);
    } catch (error) {
      if (error instanceof RuntimePerformanceNotFoundError)
        throw new NotFoundException({ code: 'RUNTIME_TASK_NOT_FOUND' });
      throw error;
    }
  }
}
