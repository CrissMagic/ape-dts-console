import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  ForbiddenException,
  Get,
  NotFoundException,
  Param,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import {
  continueRecoveryRequestSchema,
  confirmRecoveryOldRuntimeStoppedRequestSchema,
  recoverySessionParametersSchema,
  retryRecoveryStepRequestSchema,
  selectRecoveryTasksRequestSchema,
  runtimeTaskIdentifierParametersSchema,
} from '@scs/contracts';
import { z, ZodError, type ZodType } from 'zod';
import { TaskRecoveryService } from './task-recovery.service.js';
import { RuntimeSupervisorError } from '../runtime/runtime-supervisor.service.js';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { BootstrapRecoveryCompletionService } from '../bootstrap/bootstrap-recovery-completion.service.js';
import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import { AllowDuringBootstrapRecovery } from '../metadata-availability/metadata-availability.decorator.js';
import {
  RecoveryCoordinatorError,
  RecoveryCoordinatorService,
} from './recovery-coordinator.service.js';

function parse<T>(schema: ZodType<T>, value: unknown): T {
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
function mapError(error: unknown): never {
  if (error instanceof RuntimeSupervisorError)
    throw new ConflictException({ code: error.code, details: error.details });
  if (error instanceof RecoveryCoordinatorError) {
    if (error.code === 'RECOVERY_ACCESS_DENIED') throw new ForbiddenException({ code: error.code });
    if (error.code === 'RECOVERY_SESSION_NOT_FOUND')
      throw new NotFoundException({ code: error.code });
    throw new ConflictException({ code: error.code, details: error.details });
  }
  throw error;
}

@AllowDuringBootstrapRecovery()
@Controller('recovery-sessions')
export class RecoveryController {
  constructor(
    private readonly coordinator: RecoveryCoordinatorService,
    private readonly bootstrapState: BootstrapStateService,
    private readonly bootstrapCompletion: BootstrapRecoveryCompletionService,
    private readonly taskRecovery: TaskRecoveryService,
  ) {}

  @Post('tasks/:taskId/retry')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async retryTask(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const actor = this.requireAdmin(request);
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
      return await this.taskRecovery.retry(actor, taskId, input.expectedTaskRowVersion);
    } catch (error) {
      mapError(error);
    }
  }

  @Get('current')
  @UseGuards(SessionAuthGuard)
  async current(@Req() request: AuthenticatedFastifyRequest) {
    this.requireAdmin(request);
    try {
      return await this.coordinator.getCurrent(this.bootstrapState.getStatus().platformInstanceId);
    } catch (error) {
      mapError(error);
    }
  }

  @Get(':sessionId')
  @UseGuards(SessionAuthGuard)
  async get(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    this.requireAdmin(request);
    try {
      return await this.coordinator.get(
        parse(recoverySessionParametersSchema, parameters).sessionId,
      );
    } catch (error) {
      mapError(error);
    }
  }

  @Post(':sessionId/run')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async run(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    this.requireAdmin(request);
    try {
      const session = await this.coordinator.run(
        parse(recoverySessionParametersSchema, parameters).sessionId,
      );
      await this.completeBootstrap(session.id, session.state);
      return session;
    } catch (error) {
      mapError(error);
    }
  }

  @Post(':sessionId/retry')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async retry(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const user = this.requireAdmin(request);
    const stepType = parse(retryRecoveryStepRequestSchema, body).stepType;
    try {
      const session = await this.coordinator.retry(
        parse(recoverySessionParametersSchema, parameters).sessionId,
        stepType,
        user.id,
      );
      await this.completeBootstrap(session.id, session.state);
      return session;
    } catch (error) {
      mapError(error);
    }
  }

  @Post(':sessionId/capacity-selection')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async select(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const user = this.requireAdmin(request);
    const input = parse(selectRecoveryTasksRequestSchema, body);
    try {
      const session = await this.coordinator.selectCapacityCandidates(
        parse(recoverySessionParametersSchema, parameters).sessionId,
        input.taskIds,
        input.expectedSessionRowVersion,
        user.id,
      );
      await this.completeBootstrap(session.id, session.state);
      return session;
    } catch (error) {
      mapError(error);
    }
  }

  @Post(':sessionId/continue')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async continueRecovery(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const user = this.requireAdmin(request);
    const input = parse(continueRecoveryRequestSchema, body);
    try {
      const session = await this.coordinator.continueAfterFailedTask(
        parse(recoverySessionParametersSchema, parameters).sessionId,
        input.expectedSessionRowVersion,
        user.id,
      );
      await this.completeBootstrap(session.id, session.state);
      return session;
    } catch (error) {
      mapError(error);
    }
  }

  @Post(':sessionId/confirm-old-runtime-stopped')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async confirmOldRuntimeStopped(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const user = this.requireAdmin(request);
    const input = parse(confirmRecoveryOldRuntimeStoppedRequestSchema, body);
    try {
      return await this.coordinator.confirmOldRuntimeStopped(
        parse(recoverySessionParametersSchema, parameters).sessionId,
        input.expectedSessionRowVersion,
        input.reason,
        user.id,
      );
    } catch (error) {
      mapError(error);
    }
  }

  private requireAdmin(request: AuthenticatedFastifyRequest) {
    const user = requireAuthContext(request).user;
    if (user.role !== 'ADMIN') throw new ForbiddenException({ code: 'RECOVERY_ACCESS_DENIED' });
    return user;
  }

  private async completeBootstrap(
    sessionId: string,
    state: 'FAILED' | 'PAUSED' | 'READY' | 'RUNNING',
  ): Promise<void> {
    if (state !== 'RUNNING') {
      if (!(await this.bootstrapCompletion.complete(sessionId))) {
        throw new ConflictException({ code: 'RECOVERY_SESSION_NOT_READY' });
      }
    }
  }
}
