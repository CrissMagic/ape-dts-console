import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { inspectControllerLockHolders } from '@scs/database';

export async function waitForUpgradeControllerLock(
  database: PlatformDatabaseService,
): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (!(await database.acquireControllerLock())) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      let holders: Awaited<ReturnType<typeof inspectControllerLockHolders>> = [];
      let diagnosticsAvailable = true;
      try {
        holders = await inspectControllerLockHolders(database.requirePool());
      } catch {
        diagnosticsAvailable = false;
      }
      process.stderr.write(
        `${JSON.stringify({
          event: 'PLATFORM_CONTROLLER_LOCK_TIMEOUT',
          waitSeconds: 60,
          diagnosticsAvailable,
          holders,
        })}\n`,
      );
      throw new Error('STOP_PLATFORM_BEFORE_SECURITY_MIGRATION');
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(1000, remaining)));
  }
}
