import { describe, expect, it } from 'vitest';

import {
  RuntimePerformanceCollector,
  type PerformanceCollectionRepository,
} from './performance-collector.js';
import type { RuntimePerformanceBinding, RuntimePerformanceSample } from './performance-store.js';

const binding: RuntimePerformanceBinding = {
  taskId: 'task',
  runId: 'run',
  unitId: 'unit',
  processToken: 'token',
  pid: 10,
  port: 19001,
};
class MemoryMetricsDatabase implements PerformanceCollectionRepository {
  bindings = [binding];
  samples: RuntimePerformanceSample[] = [];
  gaps: { startedAt: Date; endedAt: Date; reason: string }[] = [];
  unavailable = false;
  accept = true;
  maintenanceFailed = false;
  async list() {
    if (this.unavailable) throw new Error('database secret');
    return this.bindings;
  }
  async append(_binding: RuntimePerformanceBinding, sample: RuntimePerformanceSample) {
    if (this.unavailable) throw new Error('database secret');
    if (!this.accept) return false;
    this.samples.push(sample);
    return true;
  }
  async recordGap(
    _binding: RuntimePerformanceBinding,
    gap: { startedAt: Date; endedAt: Date; reason: string },
  ) {
    if (this.unavailable) throw new Error('database secret');
    this.gaps.push(gap);
  }
  async maintain() {
    if (this.maintenanceFailed) throw new Error('database secret');
  }
}

describe('runtime performance collection', () => {
  it('coalesces overlapping polls and waits for collection on shutdown', async () => {
    const repository = new MemoryMetricsDatabase();
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      read: async () => {
        await ready;
        return { sinkRecords: 1, sinkBytes: null, queueRecords: null, queueBytes: null };
      },
    });
    const first = collector.runOnce();
    expect(collector.runOnce()).toBe(first);
    let closed = false;
    const shutdown = collector.onModuleDestroy().then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(closed).toBe(false);
    release();
    await shutdown;
    expect(repository.samples).toHaveLength(1);
  });

  it('drops counters if listener ownership is lost after the scrape', async () => {
    const repository = new MemoryMetricsDatabase();
    let scraped = false;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      verifyPort: async () => {
        if (scraped) throw new Error('METRICS_PORT_OWNERSHIP_UNVERIFIED');
      },
      read: async () => {
        scraped = true;
        return { sinkRecords: 999, sinkBytes: null, queueRecords: null, queueBytes: null };
      },
    });
    await collector.runOnce();
    expect(repository.samples.at(-1)).toMatchObject({
      gapReason: 'METRICS_PORT_OWNERSHIP_UNVERIFIED',
      values: { sinkRecords: null },
    });
  });

  it('does not reuse a baseline after a rejected durable binding', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => ({
        sinkRecords: now,
        sinkBytes: null,
        queueRecords: null,
        queueBytes: null,
      }),
    });
    await collector.runOnce();
    now = 15000;
    repository.accept = false;
    await collector.runOnce();
    now = 30000;
    repository.accept = true;
    await collector.runOnce();
    expect(repository.samples.map((sample) => sample.values.recordsPerSecond)).toEqual([
      null,
      null,
    ]);
  });

  it('records a failed write as a gap and resumes from a new baseline', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => {
        if (now === 15000) repository.unavailable = true;
        return { sinkRecords: now, sinkBytes: null, queueRecords: null, queueBytes: null };
      },
    });
    await collector.runOnce();
    now = 15000;
    await collector.runOnce();
    now = 30000;
    repository.unavailable = false;
    await collector.runOnce();
    expect(repository.samples.map((sample) => sample.values.recordsPerSecond)).toEqual([
      null,
      null,
    ]);
    expect(repository.gaps).toHaveLength(1);
  });

  it('rejects a port claimed by another process even when database binding is unchanged', async () => {
    const repository = new MemoryMetricsDatabase();
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      verifyPort: async () => {
        throw new Error('METRICS_PORT_OWNERSHIP_UNVERIFIED');
      },
      read: async () => ({
        sinkRecords: 999,
        sinkBytes: null,
        queueRecords: null,
        queueBytes: null,
      }),
    });
    await collector.runOnce();
    expect(
      repository.samples.map((sample) => [sample.values.sinkRecords, sample.gapReason]),
    ).toEqual([[null, 'METRICS_PORT_OWNERSHIP_UNVERIFIED']]);
  });

  it('resets the baseline when the PID changes, even if a token was accidentally reused', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => ({
        sinkRecords: now === 0 ? 100 : 500,
        sinkBytes: null,
        queueRecords: null,
        queueBytes: null,
      }),
    });
    await collector.runOnce();
    now = 15000;
    repository.bindings = [{ ...binding, pid: 11 }];
    await collector.runOnce();
    expect(repository.samples.at(-1)?.values.recordsPerSecond).toBeNull();
  });

  it('leaves a gap after a scrape failure and resets the rate baseline', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => {
        if (now === 15000) throw new Error('METRICS_TIMEOUT');
        return {
          sinkRecords: now === 0 ? 100 : 400,
          sinkBytes: null,
          queueRecords: 0,
          queueBytes: null,
        };
      },
    });
    await collector.runOnce();
    now = 15000;
    await collector.runOnce();
    now = 30000;
    await collector.runOnce();
    expect(
      repository.samples.map((sample) => [sample.values.recordsPerSecond, sample.gapReason]),
    ).toEqual([
      [null, null],
      [null, 'METRICS_TIMEOUT'],
      [null, null],
    ]);
  });

  it('discards a response when process ownership changes during the HTTP request', async () => {
    const repository = new MemoryMetricsDatabase();
    let owned = true;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => owned,
      read: async () => {
        owned = false;
        return { sinkRecords: 1, sinkBytes: null, queueRecords: null, queueBytes: null };
      },
    });
    await collector.runOnce();
    expect(repository.samples).toEqual([]);
  });

  it('records a database outage on recovery without backfilling values, even after the task stops', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => ({ sinkRecords: 1, sinkBytes: null, queueRecords: null, queueBytes: null }),
    });
    await collector.runOnce();
    repository.unavailable = true;
    now = 15000;
    await collector.runOnce();
    repository.unavailable = false;
    repository.bindings = [];
    now = 30000;
    await collector.runOnce();
    expect(repository.gaps).toEqual([
      {
        startedAt: new Date(15000),
        endedAt: new Date(30000),
        reason: 'METRICS_STORAGE_UNAVAILABLE',
      },
    ]);
    expect(repository.samples).toHaveLength(1);
  });

  it('does not let a failed unit or retention job prevent other units from collecting', async () => {
    const repository = new MemoryMetricsDatabase();
    repository.maintenanceFailed = true;
    repository.bindings = [binding, { ...binding, unitId: 'second', port: 19002 }];
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      read: async (port) => {
        if (port === 19001) throw new Error('sensitive URL');
        return { sinkRecords: 9, sinkBytes: null, queueRecords: null, queueBytes: null };
      },
    });
    await collector.runOnce();
    expect(repository.samples.some((sample) => sample.values.sinkRecords === 9)).toBe(true);
    expect(
      repository.samples.some((sample) => sample.gapReason === 'METRICS_HTTP_UNAVAILABLE'),
    ).toBe(true);
  });

  it('persists measured counters and subsequent rates only for owned processes', async () => {
    const repository = new MemoryMetricsDatabase();
    let now = 0;
    const collector = new RuntimePerformanceCollector({
      repository,
      owns: () => true,
      now: () => new Date(now),
      read: async () => ({
        sinkRecords: now === 0 ? 100 : 250,
        sinkBytes: null,
        queueRecords: 0,
        queueBytes: null,
      }),
    });
    await collector.runOnce();
    now = 15000;
    await collector.runOnce();
    expect(repository.samples.map((sample) => sample.values.recordsPerSecond)).toEqual([null, 10]);
  });
});
