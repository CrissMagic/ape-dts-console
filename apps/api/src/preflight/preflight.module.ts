import { Module } from '@nestjs/common';

import { AuthModule } from '../auth/auth.module.js';
import { PreflightCoreModule } from './preflight-core.module.js';
import { PreflightController } from './preflight.controller.js';

@Module({
  controllers: [PreflightController],
  exports: [PreflightCoreModule],
  imports: [AuthModule, PreflightCoreModule],
})
export class PreflightModule {}
