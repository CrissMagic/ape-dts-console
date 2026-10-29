import { Injectable } from '@nestjs/common';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { EmergencyStopCodeLifecycleService } from '../emergency/emergency-stop-code-lifecycle.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { BootstrapAccessService } from './bootstrap-access.service.js';
import { BootstrapStateService } from './bootstrap-state.service.js';

/**
 * Task recovery outcomes do not gate the control plane once metadata and keys
 * are ready for the recovery session bound to this exact platform instance.
 *
 * This service deliberately reads the persisted recovery fact directly.  It
 * belongs to the Bootstrap core and therefore does not depend on the Recovery
 * orchestration module, which keeps the Nest module graph acyclic.
 */
@Injectable()
export class BootstrapRecoveryCompletionService {
  constructor(
    private readonly access: BootstrapAccessService,
    private readonly state: BootstrapStateService,
    private readonly database: PlatformDatabaseService,
    private readonly readiness: PlatformReadinessService,
    private readonly emergencyStopCode: EmergencyStopCodeLifecycleService,
  ) {}

  async complete(recoverySessionId: string): Promise<boolean> {
    const status = this.state.getStatus();
    if (status.phase === 'READY') return true;
    if (status.phase !== 'RECOVERING') return false;

    const persisted = await this.database.requirePool().query(
      `SELECT 1
         FROM ape_dts_platform.platform_instances AS instance
         JOIN ape_dts_platform.recovery_sessions AS recovery
           ON recovery.id = instance.current_recovery_session_id
          AND recovery.platform_instance_id = instance.id
        WHERE instance.id = $1::uuid
          AND recovery.id = $2::uuid
          AND recovery.state IN ('RUNNING', 'READY', 'FAILED', 'PAUSED')
          AND (SELECT count(*) FROM ape_dts_platform.recovery_steps AS step
                WHERE step.recovery_session_id = recovery.id
                  AND step.step_type IN ('LOAD_METADATA', 'UNLOCK_KEYS')
                  AND step.status = 'SUCCEEDED') = 2`,
      [status.platformInstanceId, recoverySessionId],
    );
    if (persisted.rowCount !== 1) return false;
    await this.database.probe();

    this.emergencyStopCode.activateForReadyPlatform();
    this.state.markReady();
    this.readiness.markReady();
    this.access.invalidateSession();
    return true;
  }
}
