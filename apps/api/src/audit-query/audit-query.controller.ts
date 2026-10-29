import { BadRequestException, Controller, Get, Query, UseGuards } from '@nestjs/common';
import { auditEventListQuerySchema } from '@scs/contracts';
import { ZodError } from 'zod';

import { AdminGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { AuditQueryError, AuditQueryService } from './audit-query.service.js';

@Controller('audit/events')
export class AuditQueryController {
  constructor(private readonly audit: AuditQueryService) {}

  @Get()
  @UseGuards(SessionAuthGuard, AdminGuard)
  async list(@Query() query: unknown) {
    try {
      return await this.audit.list(auditEventListQuerySchema.parse(query));
    } catch (error) {
      if (error instanceof ZodError) {
        throw new BadRequestException({
          code: 'INVALID_REQUEST',
          issues: error.issues.map(({ code, path }) => ({ code, path })),
        });
      }
      if (error instanceof AuditQueryError) {
        throw new BadRequestException({ code: error.code });
      }
      throw error;
    }
  }
}
