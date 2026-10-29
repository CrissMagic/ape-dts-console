import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Post,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import {
  completeLoginRequestSchema,
  changePasswordRequestSchema,
  loginRequestSchema,
} from '@scs/contracts';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { ZodError, type ZodType } from 'zod';

import { AllowDuringBootstrapRecovery } from '../metadata-availability/metadata-availability.decorator.js';
import {
  clearSessionCookie,
  mapAuthError,
  requestSource,
  requireAuthContext,
  sessionToken,
  setSessionCookie,
  type AuthenticatedFastifyRequest,
} from './auth-http.js';
import { AllowPasswordChange, CsrfGuard, SessionAuthGuard } from './auth.guards.js';
import { AuthService } from './auth.service.js';

function parse<T>(schema: ZodType<T>, body: unknown): T {
  try {
    return schema.parse(body);
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

@AllowDuringBootstrapRecovery()
@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('login')
  @HttpCode(HttpStatus.OK)
  async login(
    @Body() body: unknown,
    @Req() request: FastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
  ) {
    try {
      const result = await this.auth.login(parse(loginRequestSchema, body), requestSource(request));
      if (result.sessionToken !== null && result.response.status === 'AUTHENTICATED') {
        setSessionCookie(reply, result.sessionToken, new Date(result.response.absoluteExpiresAt));
      }
      return result.response;
    } catch (error) {
      mapAuthError(error);
    }
  }

  @Post('login/complete')
  @HttpCode(HttpStatus.OK)
  async completeLogin(
    @Body() body: unknown,
    @Req() request: FastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
  ) {
    try {
      const result = await this.auth.completeLogin(
        parse(completeLoginRequestSchema, body),
        requestSource(request),
      );
      setSessionCookie(reply, result.sessionToken, new Date(result.response.absoluteExpiresAt));
      return result.response;
    } catch (error) {
      mapAuthError(error);
    }
  }

  @Get('session')
  async session(@Req() request: FastifyRequest) {
    try {
      return await this.auth.refreshSession(sessionToken(request), requestSource(request));
    } catch (error) {
      mapAuthError(error);
    }
  }

  @Post('logout')
  @AllowPasswordChange()
  @HttpCode(HttpStatus.NO_CONTENT)
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async logout(
    @Req() request: AuthenticatedFastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
  ): Promise<void> {
    try {
      await this.auth.logout(requireAuthContext(request));
      clearSessionCookie(reply);
    } catch (error) {
      mapAuthError(error);
    }
  }

  @Post('password/change')
  @HttpCode(HttpStatus.OK)
  @AllowPasswordChange()
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async changePassword(
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
  ) {
    try {
      const result = await this.auth.changePassword(
        requireAuthContext(request),
        parse(changePasswordRequestSchema, body),
        requestSource(request),
      );
      setSessionCookie(reply, result.sessionToken, new Date(result.response.absoluteExpiresAt));
      return result.response;
    } catch (error) {
      mapAuthError(error);
    }
  }
}
