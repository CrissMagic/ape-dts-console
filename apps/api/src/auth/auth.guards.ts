import {
  type CanActivate,
  type ExecutionContext,
  ForbiddenException,
  Injectable,
  SetMetadata,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';

import {
  mapAuthError,
  requestSource,
  requireAuthContext,
  sessionToken,
  type AuthenticatedFastifyRequest,
} from './auth-http.js';
import { AuthService } from './auth.service.js';
import { authorizationSubject, hasPlatformCapability } from './auth-policy.js';

const ALLOW_PASSWORD_CHANGE = 'allow-password-change';
export const AllowPasswordChange = () => SetMetadata(ALLOW_PASSWORD_CHANGE, true);

@Injectable()
export class SessionAuthGuard implements CanActivate {
  constructor(private readonly auth: AuthService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<AuthenticatedFastifyRequest>();
    try {
      request.authContext = await this.auth.authenticateSession(
        sessionToken(request),
        requestSource(request),
      );
      if (
        request.authContext.mustChangePassword &&
        !new Reflector().getAllAndOverride<boolean>(ALLOW_PASSWORD_CHANGE, [
          context.getHandler(),
          context.getClass(),
        ])
      ) {
        throw new ForbiddenException({ code: 'PASSWORD_CHANGE_REQUIRED' });
      }
      return true;
    } catch (error) {
      mapAuthError(error);
    }
  }
}

@Injectable()
export class CsrfGuard implements CanActivate {
  constructor(private readonly auth: AuthService) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedFastifyRequest>();
    const header = request.headers['x-csrf-token'];
    try {
      this.auth.assertCsrf(requireAuthContext(request), typeof header === 'string' ? header : '');
      return true;
    } catch (error) {
      mapAuthError(error);
    }
  }
}

@Injectable()
export class AdminGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedFastifyRequest>();
    if (
      !hasPlatformCapability(authorizationSubject(requireAuthContext(request).user), 'MANAGE_USERS')
    ) {
      throw new ForbiddenException({ code: 'ADMIN_ROLE_REQUIRED' });
    }
    return true;
  }
}
