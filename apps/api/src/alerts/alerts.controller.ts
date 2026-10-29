import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Patch,
  Param,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import { updateAlertSettingsRequestSchema } from '@scs/contracts';
import { z } from 'zod';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AlertsService } from './alerts.service.js';

@Controller('settings/alerts')
@UseGuards(SessionAuthGuard, AdminGuard)
export class AlertsController {
  constructor(private readonly alerts: AlertsService) {}
  @Get() get() {
    return this.alerts.get();
  }
  @Get('deliveries') deliveries() {
    return this.alerts.deliveries();
  }
  @Get('deliveries/:deliveryId/context')
  deliveryContext(@Param('deliveryId') deliveryId: string) {
    if (!z.uuid().safeParse(deliveryId).success)
      throw new BadRequestException({ code: 'INVALID_REQUEST' });
    return this.alerts.deliveryContext(deliveryId);
  }
  @Patch()
  @UseGuards(CsrfGuard)
  update(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const input = updateAlertSettingsRequestSchema.safeParse(body);
    if (!input.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    return this.alerts.update(requireAuthContext(request).user.id, input.data);
  }
  @Post('test')
  @UseGuards(CsrfGuard)
  test(@Req() request: AuthenticatedFastifyRequest) {
    return this.alerts.test(requireAuthContext(request).user.id);
  }
}
