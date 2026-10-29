import { describe, expect, it } from 'vitest';

import { parsePerformanceMetrics, performanceSample } from './performance-metrics.js';

describe('runtime performance measurements', () => {
  it.each([
    'sinker_sinked_records NaN',
    'sinker_sinked_records +Inf',
    'sinker_sinked_records -1',
    'sinker_sinked_records 9007199254740992',
    'sinker_sinked_records 1\nsinker_sinked_records 2',
    'sinker_sinked_records{worker="a"} 2',
    'sinker_sinked_records broken value',
    '# HELP something else\nsinker_rps_avg 5',
  ])('rejects unusable or ambiguous counters: %s', (body) => {
    expect(() => parsePerformanceMetrics(body)).toThrow('METRICS_INVALID_PAYLOAD');
  });

  it('keeps absent fields unknown and handles Prometheus exponent notation', () => {
    expect(
      parsePerformanceMetrics(
        '# HELP sinker_sinked_records count\nsinker_sinked_records 1.5e3\nother_metric NaN',
      ),
    ).toEqual({
      sinkRecords: 1500,
      sinkBytes: null,
      queueRecords: null,
      queueBytes: null,
    });
  });

  it.each([0, -15, 46])('does not interpolate across a %s second invalid interval', (seconds) => {
    const before = parsePerformanceMetrics('sinker_sinked_records 100');
    const after = parsePerformanceMetrics('sinker_sinked_records 250');
    expect(
      performanceSample(after, new Date(100000 + seconds * 1000), {
        counters: before,
        collectedAt: new Date(100000),
      }).recordsPerSecond,
    ).toBeNull();
  });

  it('does not report negative throughput after a counter reset', () => {
    const previous = {
      counters: parsePerformanceMetrics('sinker_sinked_records 100'),
      collectedAt: new Date(0),
    };
    expect(
      performanceSample(
        parsePerformanceMetrics('sinker_sinked_records 2'),
        new Date(15000),
        previous,
      ).recordsPerSecond,
    ).toBeNull();
  });

  it('reports zero only from two valid equal counters, and no rate for the first sample', () => {
    const counters = parsePerformanceMetrics('sinker_sinked_records 100');
    expect(performanceSample(counters, new Date(0)).recordsPerSecond).toBeNull();
    expect(
      performanceSample(counters, new Date(15000), { counters, collectedAt: new Date(0) })
        .recordsPerSecond,
    ).toBe(0);
  });

  it('derives wall-clock throughput from successful writes, not engine averages or timestamps', () => {
    const first = parsePerformanceMetrics(
      'sinker_sinked_records 100\nsinker_sinked_bytes 1000\npipeline_queue_size 3\nsinker_rps_avg 999\ntimestamp 0',
    );
    const second = parsePerformanceMetrics(
      'sinker_sinked_records 250\nsinker_sinked_bytes 4000\npipeline_queue_size 0',
    );
    expect(
      performanceSample(second, new Date('2026-09-30T00:00:15Z'), {
        counters: first,
        collectedAt: new Date('2026-09-30T00:00:00Z'),
      }),
    ).toEqual({
      recordsPerSecond: 10,
      bytesPerSecond: 200,
      sinkRecords: 250,
      sinkBytes: 4000,
      queueRecords: 0,
      queueBytes: null,
    });
  });
});
