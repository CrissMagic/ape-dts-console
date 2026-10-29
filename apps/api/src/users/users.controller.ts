import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  GoneException,
  HttpCode,
  HttpException,
  HttpStatus,
  NotFoundException,
  Param,
  Patch,
  Post,
  Req,
  ServiceUnavailableException,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import {
  activateUserRequestSchema,
  changeUserRoleRequestSchema,
  changeUserStatusRequestSchema,
  confirmTotpEnrollmentRequestSchema,
  createUserRequestSchema,
  userIdParametersSchema,
} from '@scs/contracts';
import type { FastifyRequest } from 'fastify';
import { ZodError, type ZodType } from 'zod';

import {
  requestSource,
  requireAuthContext,
  type AuthenticatedFastifyRequest,
} from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { UsersService, UsersServiceError } from './users.service.js';

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

function mapUsersError(error: unknown): never {
  if (error instanceof HttpException) {
    throw error;
  }
  if (error instanceof UsersServiceError) {
    switch (error.code) {
      case 'AUTH_METHOD_RETIRED':
        throw new GoneException({ code: error.code });
      case 'ACTIVATION_FAILED':
      case 'TOTP_ENROLLMENT_FAILED':
        throw new UnauthorizedException({ code: error.code });
      case 'USER_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      case 'FINAL_ACTIVE_ADMINISTRATOR':
      case 'FIXED_ADMIN_PROTECTED':
      case 'INVALID_USER_STATE':
      case 'ROLE_UNCHANGED':
      case 'USERNAME_ALREADY_EXISTS':
        throw new ConflictException({ code: error.code });
      case 'SECURITY_POLICY_UNAVAILABLE':
        throw new ServiceUnavailableException({ code: error.code });
    }
  }
  throw error;
}

const HIGH_RISK_GUARDS = [SessionAuthGuard, CsrfGuard, AdminGuard] as const;

@Controller('users')
export class UsersController {
  constructor(private readonly users: UsersService) {}

  @Get()
  @UseGuards(SessionAuthGuard, AdminGuard)
  async list() {
    return { users: await this.users.listUsers() };
  }

  @Post()
  @UseGuards(...HIGH_RISK_GUARDS)
  async create(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.users.createUser(
        requireAuthContext(request).user.id,
        parse(createUserRequestSchema, body),
      );
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Post('activate')
  @HttpCode(HttpStatus.OK)
  async activate(@Body() body: unknown, @Req() request: FastifyRequest) {
    try {
      return await this.users.activateUser(
        parse(activateUserRequestSchema, body),
        requestSource(request),
      );
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Post('totp/confirm')
  @HttpCode(HttpStatus.OK)
  async confirmTotp(@Body() body: unknown, @Req() request: FastifyRequest) {
    const parsed = parse(confirmTotpEnrollmentRequestSchema, body);
    try {
      return await this.users.confirmTotpEnrollment(
        parsed.challengeToken,
        parsed.code,
        requestSource(request),
      );
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Patch(':userId/status')
  @UseGuards(...HIGH_RISK_GUARDS)
  async changeStatus(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { userId } = parse(userIdParametersSchema, parameters);
    try {
      return await this.users.changeStatus(
        requireAuthContext(request).user.id,
        userId,
        parse(changeUserStatusRequestSchema, body),
      );
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Patch(':userId/role')
  @UseGuards(...HIGH_RISK_GUARDS)
  async changeRole(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { userId } = parse(userIdParametersSchema, parameters);
    try {
      return await this.users.changeRole(
        requireAuthContext(request).user.id,
        userId,
        parse(changeUserRoleRequestSchema, body),
      );
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Post(':userId/credentials/reset')
  @UseGuards(...HIGH_RISK_GUARDS)
  async resetCredentials(
    @Param() parameters: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { userId } = parse(userIdParametersSchema, parameters);
    try {
      return await this.users.resetCredentials(requireAuthContext(request).user.id, userId);
    } catch (error) {
      mapUsersError(error);
    }
  }

  @Post(':userId/sessions/revoke')
  @HttpCode(HttpStatus.NO_CONTENT)
  @UseGuards(...HIGH_RISK_GUARDS)
  async revokeSessions(
    @Param() parameters: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ): Promise<void> {
    const { userId } = parse(userIdParametersSchema, parameters);
    try {
      await this.users.revokeSessions(requireAuthContext(request).user.id, userId);
    } catch (error) {
      mapUsersError(error);
    }
  }
}
