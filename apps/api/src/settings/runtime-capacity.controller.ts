import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Inject,
  Patch,
  Req,
  UseGuards,
} from '@nestjs/common';
import { z } from 'zod';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { RuntimeCapacityService } from './runtime-capacity.service.js';
import {
  RUNTIME_ENVIRONMENT_CHECK,
  type RuntimeEnvironmentCheck,
} from '../preflight/runtime-environment-check.js';

const updateSchema = z
  .object({
    maxRuntimeUnits: z.number().int().min(1).max(2_147_483_647),
    expectedRowVersion: z
      .string()
      .regex(/^[1-9]\d*$/u)
      .refine((value) => BigInt(value) <= 9_223_372_036_854_775_807n),
  })
  .strict();

@Controller('settings/runtime-capacity')
export class RuntimeCapacityController {
  constructor(
    private readonly capacity: RuntimeCapacityService,
    @Inject(RUNTIME_ENVIRONMENT_CHECK) private readonly environment: RuntimeEnvironmentCheck,
  ) {}

  @Get('environment')
  @UseGuards(SessionAuthGuard, AdminGuard)
  environmentReadiness() {
    return this.environment.inspect();
  }

  @Get()
  @UseGuards(SessionAuthGuard, AdminGuard)
  get() {
    return this.capacity.get();
  }

  @Patch()
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  update(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const input = updateSchema.safeParse(body);
    if (!input.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    return this.capacity.update(requireAuthContext(request).user.id, input.data);
  }
}
