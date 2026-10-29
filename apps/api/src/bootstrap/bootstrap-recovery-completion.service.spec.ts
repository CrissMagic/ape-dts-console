import { describe, expect, it, vi } from 'vitest';

import { BootstrapRecoveryCompletionService } from './bootstrap-recovery-completion.service.js';

function fixture(
  options: { readonly phase?: 'READY' | 'RECOVERING'; readonly rowCount?: number } = {},
) {
  let phase = options.phase ?? 'RECOVERING';
  const access = { invalidateSession: vi.fn() };
  const state = {
    getStatus: vi.fn(() => ({ phase, platformInstanceId: 'instance-1' })),
    markReady: vi.fn(() => {
      phase = 'READY';
    }),
  };
  const query = vi.fn(async (_sql: string, _parameters: unknown[]) => {
    void _sql;
    void _parameters;
    return { rowCount: options.rowCount ?? 1, rows: [] };
  });
  const probe = vi.fn(async () => undefined);
  const readiness = { markReady: vi.fn() };
  const emergency = { activateForReadyPlatform: vi.fn() };
  const service = new BootstrapRecoveryCompletionService(
    access as never,
    state as never,
    { requirePool: () => ({ query }), probe } as never,
    readiness as never,
    emergency as never,
  );
  return { access, emergency, query, readiness, service, state, probe };
}

describe('BootstrapRecoveryCompletionService', () => {
  it('opens management for a settled session bound to this platform with metadata and keys ready', async () => {
    const context = fixture();

    await expect(context.service.complete('recovery-1')).resolves.toBe(true);

    expect(context.query).toHaveBeenCalledWith(
      expect.stringContaining('recovery.id = instance.current_recovery_session_id'),
      ['instance-1', 'recovery-1'],
    );
    expect(context.emergency.activateForReadyPlatform).toHaveBeenCalledOnce();
    expect(context.state.markReady).toHaveBeenCalledOnce();
    expect(context.readiness.markReady).toHaveBeenCalledOnce();
    expect(context.access.invalidateSession).toHaveBeenCalledOnce();
    expect(context.query.mock.calls[0]?.[0]).toContain(
      "recovery.state IN ('RUNNING', 'READY', 'FAILED', 'PAUSED')",
    );
  });

  it('keeps the platform frozen when the binding or metadata/key readiness is missing', async () => {
    const context = fixture({ rowCount: 0 });

    await expect(context.service.complete('stale-recovery')).resolves.toBe(false);

    expect(context.emergency.activateForReadyPlatform).not.toHaveBeenCalled();
    expect(context.readiness.markReady).not.toHaveBeenCalled();
    expect(context.access.invalidateSession).not.toHaveBeenCalled();
  });

  it('does not mark a recovering platform ready after losing its controller connection', async () => {
    const context = fixture();
    context.probe.mockRejectedValueOnce(new Error('Controller connection closed'));

    await expect(context.service.complete('recovery-1')).rejects.toThrow(
      'Controller connection closed',
    );

    expect(context.state.markReady).not.toHaveBeenCalled();
    expect(context.readiness.markReady).not.toHaveBeenCalled();
    expect(context.emergency.activateForReadyPlatform).not.toHaveBeenCalled();
    expect(context.access.invalidateSession).not.toHaveBeenCalled();
  });

  it('is idempotent after this process is already READY', async () => {
    const context = fixture({ phase: 'READY' });

    await expect(context.service.complete('recovery-1')).resolves.toBe(true);

    expect(context.query).not.toHaveBeenCalled();
    expect(context.emergency.activateForReadyPlatform).not.toHaveBeenCalled();
  });
});
