import { describe, expect, it } from 'vitest';
import { parsePlatformTaskConfig } from '@scs/config-model';
import { kafkaSourceBinding, kafkaUnitKey } from './kafka-source-units.js';

const base = {
  kind: 'KAFKA_RAW_FORWARD',
  schemaVersion: 2,
  source: { dataSourceId: crypto.randomUUID(), groupId: 'task-group' },
  target: { dataSourceId: crypto.randomUUID() },
  mappings: [
    { source: 'events', targets: [{ topic: 'sink' }] },
    { source: 'orders', targets: [{ topic: 'sink' }] },
  ],
  tuning: {
    batchSize: 100,
    bufferSize: 100,
    checkpointIntervalSeconds: 5,
    ackIntervalSeconds: 5,
    requiredAcks: 'all',
  },
};

describe('Kafka source unit binding', () => {
  it('keeps equal partition numbers on different source topics distinct and round-trippable', () => {
    const config = parsePlatformTaskConfig(base);
    if (config.kind !== 'KAFKA_RAW_FORWARD') throw new Error('Kafka expected');
    const first = kafkaUnitKey(config, 'events', 0);
    const second = kafkaUnitKey(config, 'orders', 0);
    expect(first).not.toBe(second);
    expect(kafkaSourceBinding(config, first)).toEqual({
      sourceTopic: 'events',
      sourcePartition: 0,
    });
    expect(kafkaSourceBinding(config, second)).toEqual({
      sourceTopic: 'orders',
      sourcePartition: 0,
    });
    expect(kafkaSourceBinding(config, 'topic:other:partition:0')).toBeNull();
    expect(kafkaSourceBinding(config, 'partition:0')).toBeNull();
    expect(kafkaSourceBinding(config, 'topic:events:partition:01')).toBeNull();
  });

  it('retains legacy partition keys and rejects non-canonical or unsafe partitions', () => {
    const config = parsePlatformTaskConfig({
      kind: base.kind,
      tuning: base.tuning,
      schemaVersion: 1,
      source: { ...base.source, topic: 'events' },
      target: { ...base.target, topic: 'sink' },
    });
    if (config.kind !== 'KAFKA_RAW_FORWARD') throw new Error('Kafka expected');
    expect(kafkaUnitKey(config, 'events', 1)).toBe('partition:1');
    expect(kafkaSourceBinding(config, 'partition:1')).toEqual({
      sourceTopic: 'events',
      sourcePartition: 1,
    });
    expect(kafkaSourceBinding(config, 'topic:events:partition:1')).toBeNull();
    expect(kafkaSourceBinding(config, 'partition:9007199254740992')).toBeNull();
  });
});
