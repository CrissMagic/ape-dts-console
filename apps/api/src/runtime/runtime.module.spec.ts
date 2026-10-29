import { describe, expect, it } from 'vitest';

import { RuntimeMetricsPortAllocator } from './runtime-metrics-port-allocator.js';
import { runtimeMetricsPortAllocatorFromEnvironment } from './runtime.module.js';

describe('runtimeMetricsPortAllocatorFromEnvironment', () => {
  it('creates an allocator only when both bounds form a valid range', () => {
    expect(
      runtimeMetricsPortAllocatorFromEnvironment({
        SCS_RUNTIME_METRICS_PORT_MAX: '20100',
        SCS_RUNTIME_METRICS_PORT_MIN: '20000',
      }),
    ).toBeInstanceOf(RuntimeMetricsPortAllocator);
  });

  it.each([
    {},
    { SCS_RUNTIME_METRICS_PORT_MIN: '20000' },
    { SCS_RUNTIME_METRICS_PORT_MAX: '20100' },
    {
      SCS_RUNTIME_METRICS_PORT_MAX: '20000',
      SCS_RUNTIME_METRICS_PORT_MIN: '20100',
    },
    {
      SCS_RUNTIME_METRICS_PORT_MAX: 'not-a-port',
      SCS_RUNTIME_METRICS_PORT_MIN: '20000',
    },
  ])('defers missing or invalid configuration to task start (%o)', (environment) => {
    expect(runtimeMetricsPortAllocatorFromEnvironment(environment)).toBeUndefined();
  });
});
