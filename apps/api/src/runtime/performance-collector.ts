import { Logger, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';

import { readPerformanceMetrics } from './performance-http.js';
import {
  performanceSample,
  type PerformanceBaseline,
  type PerformanceCounters,
} from './performance-metrics.js';
import type { RuntimePerformanceBinding, RuntimePerformanceSample } from './performance-store.js';

export interface PerformanceCollectionRepository {
  list(): Promise<RuntimePerformanceBinding[]>;
  append(binding: RuntimePerformanceBinding, sample: RuntimePerformanceSample): Promise<boolean>;
  recordGap(
    binding: RuntimePerformanceBinding,
    gap: { startedAt: Date; endedAt: Date; reason: string },
  ): Promise<void>;
  maintain(now?: Date): Promise<void>;
}

interface CollectorOptions {
  repository: PerformanceCollectionRepository;
  owns(binding: RuntimePerformanceBinding): boolean;
  now?: () => Date;
  read?: (port: number) => Promise<PerformanceCounters>;
  verifyPort?: (binding: RuntimePerformanceBinding) => Promise<void>;
}

export class RuntimePerformanceCollector implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RuntimePerformanceCollector.name);
  private readonly baselines = new Map<string, PerformanceBaseline>();
  private readonly lastBindings = new Map<string, RuntimePerformanceBinding>();
  private readonly pendingGaps = new Map<
    string,
    { binding: RuntimePerformanceBinding; startedAt: Date }
  >();
  private lastMaintenance: number | undefined;
  private timer: NodeJS.Timeout | undefined;
  private inFlight: Promise<void> | undefined;

  constructor(private readonly options: CollectorOptions) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.runOnce(), 15000);
    this.timer.unref();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.timer !== undefined) clearInterval(this.timer);
    await this.inFlight;
  }

  runOnce(): Promise<void> {
    if (this.inFlight !== undefined) return this.inFlight;
    this.inFlight = this.collect()
      .catch(() => {
        this.logger.warn('METRICS_COLLECTION_FAILED');
      })
      .finally(() => {
        this.inFlight = undefined;
      });
    return this.inFlight;
  }

  private async collect(): Promise<void> {
    const now = this.now();
    for (const [key, gap] of this.pendingGaps) {
      try {
        await this.options.repository.recordGap(gap.binding, {
          startedAt: gap.startedAt,
          endedAt: now,
          reason: 'METRICS_STORAGE_UNAVAILABLE',
        });
        this.pendingGaps.delete(key);
      } catch {
        this.logger.warn('METRICS_GAP_PERSIST_FAILED');
      }
    }
    let bindings: RuntimePerformanceBinding[];
    try {
      bindings = await this.options.repository.list();
    } catch {
      for (const binding of this.lastBindings.values()) this.storageFailed(binding, now);
      this.logger.warn('METRICS_STORAGE_UNAVAILABLE');
      return;
    }
    const activeKeys = new Set(bindings.map((binding) => this.key(binding)));
    for (const key of this.baselines.keys()) if (!activeKeys.has(key)) this.baselines.delete(key);
    this.lastBindings.clear();
    for (const binding of bindings) this.lastBindings.set(this.key(binding), binding);

    // Bound parallel HTTP calls. One slow or failed unit cannot abort the others.
    let next = 0;
    await Promise.all(
      Array.from({ length: Math.min(4, bindings.length) }, async () => {
        while (next < bindings.length) {
          const binding = bindings[next++]!;
          try {
            await this.collectUnit(binding);
          } catch {
            this.storageFailed(binding, this.now());
            this.logger.warn('METRICS_STORAGE_UNAVAILABLE');
          }
        }
      }),
    );
    if (this.lastMaintenance === undefined || now.getTime() - this.lastMaintenance >= 60000) {
      this.lastMaintenance = now.getTime();
      try {
        await this.options.repository.maintain(now);
      } catch {
        this.logger.warn('METRICS_MAINTENANCE_FAILED');
      }
    }
  }

  private async collectUnit(binding: RuntimePerformanceBinding): Promise<void> {
    const key = this.key(binding);
    if (!this.options.owns(binding) || this.pendingGaps.has(key)) {
      this.baselines.delete(key);
      return;
    }
    let counters: PerformanceCounters;
    try {
      await this.options.verifyPort?.(binding);
      counters = await (this.options.read ?? readPerformanceMetrics)(binding.port);
      await this.options.verifyPort?.(binding);
    } catch (error) {
      this.baselines.delete(key);
      if (!this.options.owns(binding)) return;
      const message = error instanceof Error ? error.message : '';
      const gapReason = [
        'METRICS_TIMEOUT',
        'METRICS_INVALID_PAYLOAD',
        'METRICS_RESPONSE_TOO_LARGE',
        'METRICS_PORT_OWNERSHIP_UNVERIFIED',
      ].includes(message)
        ? message
        : 'METRICS_HTTP_UNAVAILABLE';
      await this.options.repository.append(binding, {
        collectedAt: this.now(),
        gapReason,
        values: {
          recordsPerSecond: null,
          bytesPerSecond: null,
          sinkRecords: null,
          sinkBytes: null,
          queueRecords: null,
          queueBytes: null,
        },
      });
      return;
    }
    const collectedAt = this.now();
    if (!this.options.owns(binding)) {
      this.baselines.delete(key);
      return;
    }
    const values = performanceSample(counters, collectedAt, this.baselines.get(key));
    if (await this.options.repository.append(binding, { collectedAt, values, gapReason: null })) {
      this.baselines.set(key, { counters, collectedAt });
    } else {
      this.baselines.delete(key);
    }
  }

  private now(): Date {
    return (this.options.now ?? (() => new Date()))();
  }
  private key(binding: RuntimePerformanceBinding): string {
    return JSON.stringify([
      binding.taskId,
      binding.runId,
      binding.unitId,
      binding.processToken,
      binding.pid,
      binding.port,
    ]);
  }
  private storageFailed(binding: RuntimePerformanceBinding, at: Date): void {
    const key = this.key(binding);
    this.baselines.delete(key);
    if (!this.pendingGaps.has(key)) this.pendingGaps.set(key, { binding, startedAt: at });
  }
}
