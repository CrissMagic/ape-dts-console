import { BadRequestException, Body, Controller, Get, Patch, Req, UseGuards } from '@nestjs/common';
import { updateRuntimeRecoverySettingsSchema } from '@scs/contracts';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { RuntimeRecoverySettingsService } from './runtime-recovery.service.js';

@Controller('settings/runtime-recovery')
@UseGuards(SessionAuthGuard, AdminGuard)
export class RuntimeRecoverySettingsController {
  constructor(private readonly settings: RuntimeRecoverySettingsService) {}
  @Get() get() {
    return this.settings.get();
  }
  @Patch()
  @UseGuards(CsrfGuard)
  update(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const parsed = updateRuntimeRecoverySettingsSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    return this.settings.update(requireAuthContext(request).user.id, parsed.data);
  }
}
