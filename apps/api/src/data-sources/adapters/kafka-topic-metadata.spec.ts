import { createServer } from 'node:net';
import { once } from 'node:events';
import { describe, expect, it } from 'vitest';

import { fetchKafkaTopicMetadata } from './kafka-topic-metadata.js';

function i16(value: number): Buffer {
  const result = Buffer.alloc(2);
  result.writeInt16BE(value);
  return result;
}
function i32(value: number): Buffer {
  const result = Buffer.alloc(4);
  result.writeInt32BE(value);
  return result;
}
function str(value: string): Buffer {
  const bytes = Buffer.from(value);
  return Buffer.concat([i16(bytes.length), bytes]);
}

describe('Kafka legacy metadata', () => {
  it('negotiates Kafka 2.1 metadata without UUIDs and returns actual partition IDs', async () => {
    const versions: number[] = [];
    const server = createServer((socket) => {
      let input = Buffer.alloc(0);
      socket.on('data', (chunk) => {
        input = Buffer.concat([input, chunk]);
        while (input.length >= 4 && input.length >= input.readInt32BE(0) + 4) {
          const length = input.readInt32BE(0);
          const request = input.subarray(4, length + 4);
          input = input.subarray(length + 4);
          const key = request.readInt16BE(0);
          const correlation = request.readInt32BE(4);
          let body: Buffer;
          if (key === 18) {
            body = Buffer.concat([i16(0), i32(1), i16(3), i16(0), i16(7), i32(0)]);
          } else {
            versions.push(request.readInt16BE(2));
            // v4: throttle, brokers, cluster, controller, topics; partition 3 led by broker 9.
            body = Buffer.concat([
              i32(0),
              i32(1),
              i32(9),
              str('localhost'),
              i32(9092),
              i16(-1),
              str('legacy-cluster'),
              i32(9),
              i32(1),
              i16(0),
              str('events'),
              Buffer.from([0]),
              i32(1),
              i16(0),
              i32(3),
              i32(9),
              i32(1),
              i32(9),
              i32(1),
              i32(9),
            ]);
          }
          const frame = Buffer.concat([i32(correlation), body]);
          socket.write(Buffer.concat([i32(frame.length), frame]));
        }
      });
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const address = server.address();
    if (address === null || typeof address === 'string') throw new Error('missing address');
    try {
      const topics = await fetchKafkaTopicMetadata([`127.0.0.1:${address.port}`]);
      expect(versions).toEqual([4]);
      expect(topics).toEqual([
        { name: 'events', topicId: null, partitions: [{ partitionId: 3, leader: 9 }] },
      ]);
    } finally {
      server.close();
      await once(server, 'close');
    }
  });
});
