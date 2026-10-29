import type { UserSummary } from '@scs/contracts';
import { describe, expect, it, vi } from 'vitest';

import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { LogLifecycleSettingsService } from './log-lifecycle-settings.service.js';
import type { LogPipelineCoordinatorService } from './log-pipeline-coordinator.service.js';

const actor: UserSummary = {
  displayName: 'Administrator',
  id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f5',
  role: 'ADMIN',
  status: 'ACTIVE',
  username: 'admin',
};

function setup() {
  let rowVersion = '7';
  let logRetentionDays = 30;
  let runLogLimitBytes = '1073741824';
  let logFailureBufferBytes = '67108864';
  const query = vi.fn(async (sql: string, values?: readonly unknown[]) => {
    if (sql.includes('SELECT log_retention_days')) {
      return {
        rows: [
          {
            log_failure_buffer_bytes: logFailureBufferBytes,
            log_retention_days: logRetentionDays,
            row_version: rowVersion,
            run_log_limit_bytes: runLogLimitBytes,
            run_retention_days: 365,
          },
        ],
      };
    }
    if (sql.includes('count(DISTINCT run.id)')) {
      return { rows: [{ affected_runs: '2', bytes: '4096', segments: '3' }] };
    }
    if (sql.includes('WITH affected AS')) {
      return { rows: [{ affected_runs: '1', bytes: '12288', segments: '4' }] };
    }
    if (sql.includes('UPDATE ape_dts_platform.platform_settings')) {
      if (sql.includes('log_retention_days=')) logRetentionDays = values?.[0] as number;
      if (sql.includes('run_log_limit_bytes=')) runLogLimitBytes = String(values?.[0]);
      if (sql.includes('log_failure_buffer_bytes=')) logFailureBufferBytes = String(values?.[0]);
      rowVersion = '8';
      return { rowCount: 1, rows: [] };
    }
    return { rowCount: null, rows: [] };
  });
  const client = { query, release: vi.fn() };
  const pool = { connect: vi.fn(async () => client), query };
  const append = vi.fn(async () => '10');
  const applyLimits = vi.fn(async () => ({
    current: {},
    droppedBufferedBytes: 0,
    droppedBufferedSegments: 0,
    previous: {},
    sealedSegments: 0,
  }));
  const snapshotLimits = vi.fn(() => ({
    activeRuns: 2,
    maxBufferedBytes: 67_108_864,
    maxRunBytes: 1_073_741_824,
    pendingBufferedBytes: 10_000,
  }));
  const service = new LogLifecycleSettingsService(
    { requirePool: () => pool } as unknown as PlatformDatabaseService,
    { append } as unknown as AuditLedgerService,
    { applyLimits, snapshotLimits } as unknown as LogPipelineCoordinatorService,
  );
  return { append, applyLimits, query, service };
}

describe('LogLifecycleSettingsService', () => {
  it('previews and applies a lower per-run limit against persistent segments', async () => {
    const test = setup();
    const preview = (await test.service.preview(actor, {
      key: 'runLogLimitBytes',
      rowVersion: '7',
      value: 536_870_912,
    })) as { confirmationToken: string; impact: { affectedRuns: number } };
    expect(preview.impact.affectedRuns).toBe(1);

    await expect(
      test.service.save(actor, {
        confirmationToken: preview.confirmationToken,
        key: 'runLogLimitBytes',
        rowVersion: '7',
        value: 536_870_912,
      }),
    ).resolves.toMatchObject({
      rowVersion: '8',
      runLogLimitBytes: '536870912',
    });
    expect(test.applyLimits).toHaveBeenCalledWith({
      maxBufferedBytes: 67_108_864,
      maxRunBytes: 536_870_912,
      rowVersion: '8',
    });
  });

  it('previews the exact redacted bytes discarded by a failure-buffer reduction', async () => {
    const test = setup();
    await expect(
      test.service.preview(actor, {
        key: 'logFailureBufferBytes',
        rowVersion: '7',
        value: 4096,
      }),
    ).resolves.toMatchObject({
      confirmationRequired: true,
      impact: { affectedRuns: 2, bufferedBytes: '10000', bytes: '5904', segments: null },
      oldValue: 67_108_864,
    });
  });

  it('binds a one-time confirmation to the actor and exact impact summary, then audits without the token', async () => {
    const test = setup();
    const preview = (await test.service.preview(actor, {
      key: 'logRetentionDays',
      rowVersion: '7',
      value: 15,
    })) as { confirmationToken: string };
    await expect(
      test.service.save(actor, {
        confirmationToken: preview.confirmationToken,
        key: 'logRetentionDays',
        rowVersion: '7',
        value: 15,
      }),
    ).resolves.toMatchObject({ logRetentionDays: 15, rowVersion: '8' });

    expect(test.append).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: 'LOG_LIFECYCLE_SETTING_UPDATED',
        actorUserId: actor.id,
        details: {
          key: 'logRetentionDays',
          newValue: 15,
          oldValue: 30,
          previousRowVersion: '7',
        },
      }),
    );
    expect(JSON.stringify(test.append.mock.calls)).not.toContain(preview.confirmationToken);

    await expect(
      test.service.save(actor, {
        confirmationToken: preview.confirmationToken,
        key: 'logRetentionDays',
        rowVersion: '7',
        value: 15,
      }),
    ).rejects.toMatchObject({ code: 'LOG_SETTINGS_VERSION_CONFLICT' });
  });

  it('invalidates a confirmation after an actor mismatch instead of allowing reuse', async () => {
    const test = setup();
    const preview = (await test.service.preview(actor, {
      key: 'logRetentionDays',
      rowVersion: '7',
      value: 15,
    })) as { confirmationToken: string };
    const anotherActor = { ...actor, id: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f6' };

    await expect(
      test.service.save(anotherActor, {
        confirmationToken: preview.confirmationToken,
        key: 'logRetentionDays',
        rowVersion: '7',
        value: 15,
      }),
    ).rejects.toMatchObject({ code: 'LOG_SETTINGS_CONFIRMATION_INVALID' });
    await expect(
      test.service.save(actor, {
        confirmationToken: preview.confirmationToken,
        key: 'logRetentionDays',
        rowVersion: '7',
        value: 15,
      }),
    ).rejects.toMatchObject({ code: 'LOG_SETTINGS_CONFIRMATION_INVALID' });
  });
});
