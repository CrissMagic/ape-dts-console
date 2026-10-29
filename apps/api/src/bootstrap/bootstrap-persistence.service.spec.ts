import { describe, expect, it, vi } from 'vitest';

import { BootstrapPersistenceService } from './bootstrap-persistence.service.js';

describe('BootstrapPersistenceService existing-platform recovery', () => {
  it('creates a RUNNING session with only bootstrap-owned steps completed', async () => {
    const queries: Array<{ text: string; values?: readonly unknown[] }> = [];
    const client = {
      query: vi.fn(async (text: string, values?: readonly unknown[]) => {
        queries.push(values === undefined ? { text } : { text, values });
        return { rowCount: 0, rows: [] };
      }),
      release: vi.fn(),
    };
    const pool = { connect: vi.fn(async () => client) };

    const sessionId = await new BootstrapPersistenceService().startExistingRecovery(
      pool as never,
      '00000000-0000-4000-8000-000000000001',
    );

    expect(sessionId).toMatch(/^[0-9a-f-]{36}$/u);
    const sessionInsert = queries.find((query) =>
      query.text.includes('INSERT INTO ape_dts_platform.recovery_sessions'),
    );
    expect(sessionInsert?.text).toContain("VALUES ($1::uuid, $2::uuid, 'RUNNING')");

    const stepInsert = queries.find((query) =>
      query.text.includes('INSERT INTO ape_dts_platform.recovery_steps'),
    );
    expect(stepInsert?.values?.[4]).toEqual([
      'SUCCEEDED',
      'SUCCEEDED',
      'PENDING',
      'PENDING',
      'PENDING',
      'PENDING',
      'PENDING',
    ]);
    expect(client.query).toHaveBeenLastCalledWith('COMMIT');
  });
});
