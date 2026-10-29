export interface PerformanceCounters {
  sinkRecords: number | null;
  sinkBytes: number | null;
  queueRecords: number | null;
  queueBytes: number | null;
}

export interface PerformanceBaseline {
  counters: PerformanceCounters;
  collectedAt: Date;
}

const names = new Map<string, keyof PerformanceCounters>([
  ['sinker_sinked_records', 'sinkRecords'],
  ['sinker_sinked_bytes', 'sinkBytes'],
  ['pipeline_queue_size', 'queueRecords'],
  ['pipeline_queue_bytes', 'queueBytes'],
]);

export function parsePerformanceMetrics(text: string): PerformanceCounters {
  const counters: PerformanceCounters = {
    sinkRecords: null,
    sinkBytes: null,
    queueRecords: null,
    queueBytes: null,
  };
  const seen = new Set<string>();
  for (const line of text.split('\n')) {
    const name = /^(\w+)/u.exec(line.trim())?.[1];
    if (name === undefined || !names.has(name)) continue;
    const match = /^(\w+)\s+(\S+)\s*$/u.exec(line.trim());
    if (
      match === null ||
      seen.has(name) ||
      !/^[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/u.test(match[2]!)
    ) {
      throw new Error('METRICS_INVALID_PAYLOAD');
    }
    const value = Number(match[2]);
    if (!Number.isSafeInteger(value) || value < 0) throw new Error('METRICS_INVALID_PAYLOAD');
    seen.add(name);
    counters[names.get(name)!] = value;
  }
  if (seen.size === 0) throw new Error('METRICS_INVALID_PAYLOAD');
  return counters;
}

export function performanceSample(
  counters: PerformanceCounters,
  collectedAt: Date,
  previous?: PerformanceBaseline,
) {
  const seconds =
    previous === undefined ? 0 : (collectedAt.getTime() - previous.collectedAt.getTime()) / 1000;
  const rate = (key: 'sinkRecords' | 'sinkBytes'): number | null => {
    const before = previous?.counters[key];
    const after = counters[key];
    return seconds > 0 &&
      seconds <= 45 &&
      before !== null &&
      before !== undefined &&
      after !== null &&
      after >= before
      ? (after - before) / seconds
      : null;
  };
  return { ...counters, recordsPerSecond: rate('sinkRecords'), bytesPerSecond: rate('sinkBytes') };
}
