import { Module } from '@nestjs/common';
import { ProcessProbeRunner } from '@scs/artifact-probe';

import { StorageModule } from '../storage/storage.module.js';
import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { ArtifactsController } from './artifacts.controller.js';
import { ArtifactsService } from './artifacts.service.js';
import { ArtifactProbeService } from './artifact-probe.service.js';
import { ArtifactFiles } from './artifact-files.js';
import { ArtifactUploadParser } from './artifact-upload-parser.js';

@Module({
  controllers: [ArtifactsController],
  exports: [ArtifactProbeService],
  imports: [StorageModule, AuditModule, AuthModule, BootstrapModule],
  providers: [
    ArtifactUploadParser,
    ArtifactProbeService,
    ArtifactsService,
    { provide: ArtifactFiles, useFactory: () => new ArtifactFiles() },
    { provide: ProcessProbeRunner, useFactory: () => new ProcessProbeRunner() },
  ],
})
export class ArtifactsModule {}
