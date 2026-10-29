import {
  ForbiddenException,
  GoneException,
  BadRequestException,
  HttpException,
  HttpStatus,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import type { FastifyReply, FastifyRequest } from 'fastify';

import { AuthServiceError, type AuthenticatedRequestContext } from './auth.service.js';

export const AUTH_SESSION_COOKIE_NAME = 'scs_session';

export interface AuthenticatedFastifyRequest extends FastifyRequest {
  authContext?: AuthenticatedRequestContext;
}

export function requestSource(request: FastifyRequest): string {
  return request.ip;
}

export function readCookie(cookieHeader: string | undefined, name: string): string | undefined {
  if (cookieHeader === undefined) {
    return undefined;
  }
  for (const part of cookieHeader.split(';')) {
    const separator = part.indexOf('=');
    if (separator < 0 || part.slice(0, separator).trim() !== name) {
      continue;
    }
    try {
      return decodeURIComponent(part.slice(separator + 1).trim());
    } catch {
      return undefined;
    }
  }
  return undefined;
}

export function sessionToken(request: FastifyRequest): string {
  return readCookie(request.headers.cookie, AUTH_SESSION_COOKIE_NAME) ?? '';
}

export function setSessionCookie(
  reply: FastifyReply,
  token: string,
  absoluteExpiresAt: Date,
): void {
  const maxAge = Math.max(0, Math.floor((absoluteExpiresAt.getTime() - Date.now()) / 1_000));
  reply.header(
    'set-cookie',
    `${AUTH_SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/api; Max-Age=${maxAge}`,
  );
}

export function clearSessionCookie(reply: FastifyReply): void {
  reply.header(
    'set-cookie',
    `${AUTH_SESSION_COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/api; Max-Age=0`,
  );
}

export function requireAuthContext(
  request: AuthenticatedFastifyRequest,
): AuthenticatedRequestContext {
  if (request.authContext === undefined) {
    throw new UnauthorizedException({ code: 'SESSION_INVALID' });
  }
  return request.authContext;
}

export function mapAuthError(error: unknown): never {
  if (error instanceof HttpException) {
    throw error;
  }
  if (error instanceof AuthServiceError) {
    switch (error.code) {
      case 'AUTH_METHOD_RETIRED':
        throw new GoneException({ code: error.code });
      case 'CURRENT_PASSWORD_INVALID':
      case 'PASSWORD_UNCHANGED':
        throw new BadRequestException({ code: error.code });
      case 'AUTHENTICATION_FAILED':
      case 'SESSION_INVALID':
        throw new UnauthorizedException({ code: error.code });
      case 'AUTHENTICATION_RATE_LIMITED':
        throw new HttpException(
          { code: error.code, retryAfterMs: error.retryAfterMs },
          HttpStatus.TOO_MANY_REQUESTS,
        );
      case 'CSRF_INVALID':
        throw new ForbiddenException({ code: error.code });
      case 'SECURITY_POLICY_UNAVAILABLE':
        throw new ServiceUnavailableException({ code: error.code });
    }
  }
  throw error;
}
