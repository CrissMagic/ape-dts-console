import { describe, expect, it } from 'vitest';

import { RuntimeEnvironmentInspector } from './runtime-environment-check.js';

describe('runtime environment preflight', () => {
  it('reports missing Metrics configuration without requiring task resource isolation', async () => {
    const result = await new RuntimeEnvironmentInspector({}).inspect();
    expect(result.state).toBe('FAILED');
    expect(result.details.issues).toEqual([
      expect.objectContaining({ code: 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED' }),
    ]);
  });

  it('passes with valid Metrics ports and no cgroup delegation or per-task hard limits', async () => {
    const result = await new RuntimeEnvironmentInspector({
      SCS_RUNTIME_METRICS_PORT_MIN: '36001',
      SCS_RUNTIME_METRICS_PORT_MAX: '36100',
    }).inspect();
    expect(result.state).toBe('PASSED');
    expect(result.code).toBe('RUNTIME_ENVIRONMENT_READY');
    expect(result.details.issues).toEqual([]);
    expect(result.summary).toContain('共享容器');
  });
});
