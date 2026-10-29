import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Patch,
  Post,
  Req,
  ServiceUnavailableException,
  UseGuards,
} from '@nestjs/common';
import { ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  previewLogLifecycleSettingSchema,
  updateLogLifecycleSettingSchema,
} from './log-lifecycle.dto.js';
import {
  LogLifecycleSettingsError,
  LogLifecycleSettingsService,
} from './log-lifecycle-settings.service.js';

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

function mapError(error: unknown): never {
  if (error instanceof LogLifecycleSettingsError) {
    switch (error.code) {
      case 'LOG_SETTINGS_VALUE_INVALID':
      case 'LOG_SETTINGS_CONFIRMATION_REQUIRED':
      case 'LOG_SETTINGS_CONFIRMATION_INVALID':
        throw new BadRequestException({ code: error.code });
      case 'LOG_SETTINGS_VERSION_CONFLICT':
        throw new ConflictException({ code: error.code });
      case 'LOG_RUNTIME_LIMIT_COORDINATOR_UNAVAILABLE':
      case 'LOG_SETTINGS_UNAVAILABLE':
        throw new ServiceUnavailableException({ code: error.code });
    }
  }
  throw error;
}

const READ_GUARDS = [SessionAuthGuard, AdminGuard] as const;
const HIGH_RISK_GUARDS = [SessionAuthGuard, CsrfGuard, AdminGuard] as const;

@Controller('admin/log-lifecycle/settings')
export class LogLifecycleController {
  constructor(private readonly settings: LogLifecycleSettingsService) {}

  @Get()
  @UseGuards(...READ_GUARDS)
  async get() {
    try {
      return await this.settings.get();
    } catch (error) {
      mapError(error);
    }
  }

  @Post('preview')
  @UseGuards(...HIGH_RISK_GUARDS)
  async preview(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.settings.preview(
        requireAuthContext(request).user,
        parse(previewLogLifecycleSettingSchema, body),
      );
    } catch (error) {
      mapError(error);
    }
  }

  @Patch()
  @UseGuards(...HIGH_RISK_GUARDS)
  async update(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.settings.save(
        requireAuthContext(request).user,
        parse(updateLogLifecycleSettingSchema, body),
      );
    } catch (error) {
      mapError(error);
    }
  }
}
