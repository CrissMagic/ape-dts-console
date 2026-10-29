import { BadRequestException, Body, Controller, Get, Patch, Req, UseGuards } from '@nestjs/common';
import { updateSessionSettingsRequestSchema } from '@scs/contracts';
import { requireAuthContext, type AuthenticatedFastifyRequest } from './auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from './auth.guards.js';
import { SessionSettingsService } from './session-settings.service.js';

@Controller('settings/sessions')
@UseGuards(SessionAuthGuard, AdminGuard)
export class SessionSettingsController {
  constructor(private readonly settings: SessionSettingsService) {}

  @Get()
  get() {
    return this.settings.get();
  }

  @Patch()
  @UseGuards(CsrfGuard)
  update(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const parsed = updateSessionSettingsRequestSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    return this.settings.update(requireAuthContext(request).user.id, parsed.data);
  }
}
