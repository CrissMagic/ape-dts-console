import { describe, expect, it } from 'vitest';
import { kafkaOffsetLag } from '../data-sources/adapters/kafka-data-source.adapter.js';

describe('Kafka committed offset progress', () => {
  const ends = [
    { partition: 0, low: '0', high: '9007199254741000' },
    { partition: 1, low: '3', high: '10' },
  ];
  it('aggregates exact lag over all partitions', () => {
    expect(
      kafkaOffsetLag(ends, [
        { partition: 0, offset: '1' },
        { partition: 1, offset: '5' },
      ]),
    ).toBe('9007199254741004');
  });
  it('does not treat missing/uncommitted or expired partition offsets as zero', () => {
    expect(kafkaOffsetLag(ends, [{ partition: 0, offset: '1' }])).toBeNull();
    expect(
      kafkaOffsetLag(ends, [
        { partition: 0, offset: '-1' },
        { partition: 1, offset: '5' },
      ]),
    ).toBeNull();
    expect(
      kafkaOffsetLag(ends, [
        { partition: 0, offset: '1' },
        { partition: 1, offset: '2' },
      ]),
    ).toBeNull();
  });
});
