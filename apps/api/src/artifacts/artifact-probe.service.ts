import { Inject, Injectable } from '@nestjs/common';
import {
  ProcessProbeRunner,
  type ProcessProbeOptions,
  type ProcessProbeResult,
} from '@scs/artifact-probe';
import { ArtifactCache } from '@scs/storage';

import { ARTIFACT_CACHE } from '../storage/storage.tokens.js';

export interface CachedArtifactProbeRequest extends Omit<
  ProcessProbeOptions,
  'allowPathFallbackForNonLinuxTests'
> {
  readonly expectedSha256: string;
  readonly expectedSize: number;
}

@Injectable()
export class ArtifactProbeService {
  constructor(
    @Inject(ARTIFACT_CACHE) private readonly cache: ArtifactCache,
    private readonly runner: ProcessProbeRunner,
  ) {}

  async runCachedProbe(request: CachedArtifactProbeRequest): Promise<ProcessProbeResult> {
    const binding = await this.cache.bind(request.expectedSha256, request.expectedSize);
    try {
      return await this.runner.run(binding, {
        arguments: request.arguments,
        environment: request.environment,
        killGraceMilliseconds: request.killGraceMilliseconds,
        maxOutputBytesPerStream: request.maxOutputBytesPerStream,
        redact: request.redact,
        timeoutMilliseconds: request.timeoutMilliseconds,
        workingDirectory: request.workingDirectory,
      });
    } finally {
      await binding.close();
    }
  }
}
