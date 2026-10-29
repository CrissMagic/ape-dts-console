import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it, vi } from 'vitest';

import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PostgresLogSegmentStore } from './postgres-log-segment-store.js';

const runId = '22222222-2222-4222-8222-222222222222';

describe('PostgresLogSegmentStore recovery scope', () => {
  it('limits state reconciliation to the requested run and separates capacity deletions', async () => {
    const calls: Array<{ readonly parameters: readonly unknown[]; readonly sql: string }> = [];
    let availableCount = 2;
    const query = vi.fn(async (sql: string, parameters: readonly unknown[] = []) => {
      calls.push({ parameters, sql });
      if (sql.includes('SELECT count(*)::text AS count')) {
        const count = availableCount;
        availableCount -= 1;
        return { rowCount: 1, rows: [{ count: String(count) }] };
      }
      if (sql.includes('SELECT run_log_limit_bytes::text AS max_bytes')) {
        return { rowCount: 1, rows: [{ max_bytes: '1024' }] };
      }
      return { rowCount: 0, rows: [] };
    });
    const store = new PostgresLogSegmentStore(
      { requirePool: () => ({ query }) } as unknown as PlatformDatabaseService,
      path.join(tmpdir(), 'ape-dts-console-log-recovery-test'),
    );
    vi.spyOn(store, 'enforceRunCapacity').mockResolvedValue(undefined);

    await expect(store.recoverPendingOperations(runId)).resolves.toEqual({
      availableSegments: 0,
      capacityDeletedSegments: 1,
      completedDeletions: 0,
      failures: 0,
      integrityFailedSegments: 0,
      memoryPendingGaps: 0,
      missingSegments: 0,
    });

    const stateUpdates = calls.filter((call) =>
      call.sql.includes('UPDATE ape_dts_platform.task_runs AS runs'),
    );
    expect(stateUpdates).toHaveLength(2);
    for (const update of stateUpdates) {
      expect(update.sql).toContain('($1::uuid IS NULL OR runs.id = $1::uuid)');
      expect(update.parameters).toEqual([runId]);
    }
    expect(store.enforceRunCapacity).toHaveBeenCalledWith({ maxBytes: 1024, runId });
  });

  it('serializes durable recovery scans so one object is not processed concurrently', async () => {
    let releaseFirstScan!: () => void;
    const firstScanGate = new Promise<void>((resolve) => {
      releaseFirstScan = resolve;
    });
    let recoveryScans = 0;
    const query = vi.fn(async (sql: string) => {
      if (
        sql.includes('SELECT id') &&
        sql.includes("log_recovery_state IN ('MEMORY_PENDING', 'MIXED_PENDING')")
      ) {
        recoveryScans += 1;
        if (recoveryScans === 1) await firstScanGate;
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes('SELECT run_log_limit_bytes::text AS max_bytes')) {
        return { rowCount: 1, rows: [{ max_bytes: '1024' }] };
      }
      if (sql.includes('SELECT count(*)::text AS count')) {
        return { rowCount: 1, rows: [{ count: '0' }] };
      }
      return { rowCount: 0, rows: [] };
    });
    const store = new PostgresLogSegmentStore(
      { requirePool: () => ({ query }) } as unknown as PlatformDatabaseService,
      path.join(tmpdir(), 'ape-dts-console-log-recovery-test'),
    );

    const first = store.recoverPendingOperations(runId);
    await Promise.resolve();
    const second = store.recoverPendingOperations(runId);
    await Promise.resolve();
    const scansBeforeRelease = recoveryScans;
    releaseFirstScan();
    await Promise.all([first, second]);

    expect(scansBeforeRelease).toBe(1);
    expect(recoveryScans).toBe(2);
  });
});
