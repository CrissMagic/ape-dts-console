import { BadRequestException, Body, Controller, Post, Req, UseGuards } from '@nestjs/common';
import {
  configBundleSchema,
  configExportOptionsSchema,
  configImportRequestSchema,
} from '@scs/contracts';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { ConfigTransferService } from './config-transfer.service.js';

@Controller('settings/config-transfer')
@UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
export class ConfigTransferController {
  constructor(private readonly transfer: ConfigTransferService) {}
  @Post('export')
  export(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const input = configExportOptionsSchema.safeParse(body);
    if (!input.success) throw new BadRequestException({ code: 'CONFIG_EXPORT_OPTIONS_INVALID' });
    return this.transfer.export(requireAuthContext(request).user, input.data);
  }
  @Post('preview')
  preview(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const input = configBundleSchema.safeParse(body);
    if (!input.success) throw new BadRequestException({ code: 'CONFIG_BUNDLE_INVALID' });
    return this.transfer.preview(requireAuthContext(request).user, input.data);
  }
  @Post('import')
  import(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const input = configImportRequestSchema.safeParse(body);
    if (!input.success) throw new BadRequestException({ code: 'CONFIG_BUNDLE_INVALID' });
    return this.transfer.import(
      requireAuthContext(request).user,
      input.data.bundle,
      input.data.expectedPreviewHash,
    );
  }
}
