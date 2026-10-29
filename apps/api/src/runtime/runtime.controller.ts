import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  ForbiddenException,
  NotFoundException,
  Param,
  Post,
  Get,
  Req,
  Query,
  Res,
  ServiceUnavailableException,
  UseGuards,
} from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import {
  runtimeTaskIdentifierParametersSchema,
  runtimeLogDownloadQuerySchema,
  runtimeLogParametersSchema,
  runtimeLogQuerySchema,
  startTaskRuntimeRequestSchema,
  stopTaskRuntimeRequestSchema,
} from '@scs/contracts';
import { z, ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  LogPipelineCoordinatorError,
  LogPipelineCoordinatorService,
} from '../log-lifecycle/log-pipeline-coordinator.service.js';
import { RuntimeSupervisorError, RuntimeSupervisorService } from './runtime-supervisor.service.js';
import { RuntimeReadNotFoundError, RuntimeReadService } from './runtime-read.service.js';
import { RuntimeLogReadError, RuntimeLogReadService } from './runtime-log-read.service.js';

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

function mapRuntimeError(error: unknown): never {
  if (
    error instanceof Error &&
    'code' in error &&
    error.code === '23514' &&
    error.message === 'artifact unavailable'
  ) {
    throw new ConflictException({ code: 'RUNTIME_ARTIFACT_NOT_ACTIVE' });
  }
  if (error instanceof RuntimeSupervisorError) {
    switch (error.code) {
      case 'RUNTIME_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code, details: error.details });
      case 'RUNTIME_TASK_NOT_FOUND':
        throw new NotFoundException({ code: error.code, details: error.details });
      case 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED':
      case 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED':
        throw new ServiceUnavailableException({ code: error.code, details: error.details });
      default:
        throw new ConflictException({ code: error.code, details: error.details });
    }
  }
  throw error;
}

@Controller('tasks')
export class RuntimeController {
  constructor(
    private readonly runtime: RuntimeSupervisorService,
    private readonly reads: RuntimeReadService,
    private readonly logs: RuntimeLogReadService,
    private readonly logPipeline: LogPipelineCoordinatorService,
  ) {}

  @Get(':taskId/runtime')
  @UseGuards(SessionAuthGuard)
  async get(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    try {
      return await this.reads.get(requireAuthContext(request).user, taskId);
    } catch (error) {
      if (error instanceof RuntimeReadNotFoundError)
        throw new NotFoundException({ code: 'RUNTIME_TASK_NOT_FOUND' });
      throw error;
    }
  }

  @Get(':taskId/runtime/runs')
  @UseGuards(SessionAuthGuard)
  async runs(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    try {
      return await this.reads.listRuns(requireAuthContext(request).user, taskId);
    } catch (error) {
      if (error instanceof RuntimeReadNotFoundError)
        throw new NotFoundException({ code: 'RUNTIME_TASK_NOT_FOUND' });
      throw error;
    }
  }

  @Post(':taskId/runtime/:runId/logs/retry')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async logsRetry(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const ids = parse(runtimeLogParametersSchema, parameters);
    try {
      return await this.logPipeline.retryStorage(
        requireAuthContext(request).user,
        ids.taskId,
        ids.runId,
      );
    } catch (error) {
      if (
        error instanceof LogPipelineCoordinatorError &&
        error.code === 'RUNTIME_LOG_RUN_NOT_FOUND'
      ) {
        throw new NotFoundException({ code: error.code });
      }
      if (error instanceof LogPipelineCoordinatorError) {
        throw new ServiceUnavailableException({ code: error.code });
      }
      throw error;
    }
  }

  @Get(':taskId/runtime/:runId/logs')
  @UseGuards(SessionAuthGuard)
  async logsList(
    @Param() parameters: unknown,
    @Query() query: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const ids = parse(runtimeLogParametersSchema, parameters);
    const filter = parse(runtimeLogQuerySchema, query);
    try {
      return await this.logs.list(requireAuthContext(request).user, ids.taskId, ids.runId, filter);
    } catch (error) {
      if (error instanceof RuntimeReadNotFoundError)
        throw new NotFoundException({ code: 'RUNTIME_TASK_NOT_FOUND' });
      if (error instanceof RuntimeLogReadError) throw new ConflictException({ code: error.code });
      throw error;
    }
  }

  @Get(':taskId/runtime/:runId/logs/download')
  @UseGuards(SessionAuthGuard)
  async logsDownload(
    @Param() parameters: unknown,
    @Query() query: unknown,
    @Req() request: AuthenticatedFastifyRequest,
    @Res() reply: FastifyReply,
  ) {
    const ids = parse(runtimeLogParametersSchema, parameters);
    const filter = parse(runtimeLogDownloadQuerySchema, query);
    try {
      const download = await this.logs.download(
        requireAuthContext(request).user,
        ids.taskId,
        ids.runId,
        filter,
      );
      reply.header('Content-Type', 'text/plain; charset=utf-8');
      reply.header('X-Content-Type-Options', 'nosniff');
      reply.header('Content-Disposition', `attachment; filename="${download.filename}"`);
      return reply.send(download.content);
    } catch (error) {
      if (error instanceof RuntimeReadNotFoundError)
        throw new NotFoundException({ code: 'RUNTIME_TASK_NOT_FOUND' });
      if (error instanceof RuntimeLogReadError) throw new ConflictException({ code: error.code });
      throw error;
    }
  }

  @Post(':taskId/runtime/start')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async start(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    try {
      return await this.runtime.start(requireAuthContext(request).user, {
        ...parse(startTaskRuntimeRequestSchema, body),
        taskId,
      });
    } catch (error) {
      mapRuntimeError(error);
    }
  }

  @Post(':taskId/runtime/stop')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async stop(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    try {
      return await this.runtime.stop(requireAuthContext(request).user, {
        ...parse(stopTaskRuntimeRequestSchema, body),
        taskId,
      });
    } catch (error) {
      mapRuntimeError(error);
    }
  }

  @Post(':taskId/runtime/discard-recovery')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async discardRecovery(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(runtimeTaskIdentifierParametersSchema, parameters);
    const input = parse(
      z
        .object({
          expectedTaskRowVersion: z.string().regex(/^[1-9]\d*$/u),
          oldRuntimeStoppedConfirmed: z.literal(true),
        })
        .strict(),
      body,
    );
    try {
      return await this.runtime.discardRecovery(
        requireAuthContext(request).user,
        taskId,
        input.expectedTaskRowVersion,
      );
    } catch (error) {
      mapRuntimeError(error);
    }
  }
}
