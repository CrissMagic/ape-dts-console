import { Injectable } from '@nestjs/common';
import type { PlatformRuntimePhase } from '@scs/contracts';

@Injectable()
export class PlatformReadinessService {
  private phase: PlatformRuntimePhase = 'SEALED';

  getPhase(): PlatformRuntimePhase {
    return this.phase;
  }

  beginBootstrap(): void {
    this.phase = 'BOOTSTRAPPING';
  }

  markSealed(): void {
    this.phase = 'SEALED';
  }

  markReady(): void {
    this.phase = 'READY';
  }

  markMetadataDegraded(): void {
    this.phase = 'METADATA_DEGRADED';
  }

  markReconciling(): void {
    this.phase = 'RECONCILING';
  }

  markConcurrentInstanceBlocked(): void {
    this.phase = 'BLOCKED_CONCURRENT_INSTANCE';
  }
}
