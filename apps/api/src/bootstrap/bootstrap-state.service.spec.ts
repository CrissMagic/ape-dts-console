import { describe, expect, it } from 'vitest';

import { BootstrapStateError, BootstrapStateService } from './bootstrap-state.service.js';

describe('BootstrapStateService', () => {
  it('maps every database classification to one explicit user-visible phase', () => {
    const state = new BootstrapStateService('018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3');
    state.markCodeVerified();

    expect(state.recordSchemaInspection('ABSENT')).toBe('SCHEMA_ABSENT');
    expect(state.recordSchemaInspection('CURRENT')).toBe('SCHEMA_CURRENT');
    expect(state.recordSchemaInspection('OLDER')).toBe('MIGRATION_REQUIRED');
    expect(state.recordSchemaInspection('NEWER')).toBe('BLOCKED_NEWER_SCHEMA');
    expect(state.recordSchemaInspection('CORRUPT')).toBe('BLOCKED_CORRUPT_SCHEMA');
  });

  it('rejects a transition that skips initialization or unlock', () => {
    const state = new BootstrapStateService('018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3');

    expect(() => state.markReady()).toThrow(BootstrapStateError);
    expect(state.getStatus().phase).toBe('SEALED');
  });

  it('returns to the actionable schema phase after a failed transactional operation', () => {
    const state = new BootstrapStateService('018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3');
    state.markCodeVerified();
    state.recordSchemaInspection('ABSENT');
    state.beginInitialization();
    state.markInitializationFailed();

    expect(state.getStatus().phase).toBe('SCHEMA_ABSENT');
  });
});
