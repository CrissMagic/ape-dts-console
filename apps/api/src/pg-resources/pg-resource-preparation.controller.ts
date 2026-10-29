import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  ForbiddenException,
  HttpCode,
  NotFoundException,
  Param,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import { preparePgResourcesRequestSchema } from '@scs/contracts';
import { z } from 'zod';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  DataSourceAdapterError,
  PostgresResourcePreparationError,
} from '../data-sources/adapters/types.js';
import { DataSourcesServiceError } from '../data-sources/data-sources.service.js';
import {
  PgResourcePreparationError,
  PgResourcePreparationService,
} from './pg-resource-preparation.service.js';
import { PgResourceManagerError } from './pg-resource-manager.js';

@Controller('tasks')
export class PgResourcePreparationController {
  constructor(private readonly preparations: PgResourcePreparationService) {}
  @Post(':taskId/pg-resources/prepare')
  @HttpCode(200)
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async prepare(
    @Param('taskId') taskId: string,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const input = preparePgResourcesRequestSchema.safeParse(body);
    if (!z.uuid().safeParse(taskId).success || !input.success)
      throw new BadRequestException({ code: 'INVALID_REQUEST' });
    try {
      return await this.preparations.prepare(requireAuthContext(request).user, taskId, input.data);
    } catch (error) {
      if (error instanceof PostgresResourcePreparationError)
        throw new ConflictException({ code: error.code, details: error.progress });
      if (error instanceof PgResourcePreparationError) {
        if (error.code === 'PG_RESOURCE_ACCESS_DENIED')
          throw new ForbiddenException({ code: error.code });
        if (error.code === 'PG_RESOURCE_TASK_NOT_FOUND')
          throw new NotFoundException({ code: error.code });
        throw new ConflictException({ code: error.code });
      }
      if (
        error instanceof DataSourceAdapterError ||
        error instanceof DataSourcesServiceError ||
        error instanceof PgResourceManagerError
      )
        throw new ConflictException({ code: error.code });
      throw error;
    }
  }
}
