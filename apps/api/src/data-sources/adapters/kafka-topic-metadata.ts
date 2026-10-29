import { createConnection, type Socket } from 'node:net';

import { DataSourceAdapterError } from './types.js';

const API_VERSIONS_KEY = 18;
const API_VERSIONS_VERSION = 2;
const MAX_FRAME_BYTES = 16 * 1_024 * 1_024;
const METADATA_KEY = 3;
const METADATA_VERSION = 10;
const REQUEST_TIMEOUT_MS = 10_000;

interface KafkaPartitionMetadata {
  readonly leader: number;
  readonly partitionId: number;
}

export interface KafkaTopicMetadata {
  readonly name: string;
  readonly partitions: readonly KafkaPartitionMetadata[];
  readonly topicId: string | null;
}

class KafkaDecoder {
  private offset = 0;

  constructor(private readonly buffer: Buffer) {}

  private requireBytes(length: number): void {
    if (length < 0 || this.offset + length > this.buffer.length) {
      throw new Error('KAFKA_METADATA_RESPONSE_TRUNCATED');
    }
  }

  readInt8(): number {
    this.requireBytes(1);
    const value = this.buffer.readInt8(this.offset);
    this.offset += 1;
    return value;
  }

  readInt16(): number {
    this.requireBytes(2);
    const value = this.buffer.readInt16BE(this.offset);
    this.offset += 2;
    return value;
  }

  readInt32(): number {
    this.requireBytes(4);
    const value = this.buffer.readInt32BE(this.offset);
    this.offset += 4;
    return value;
  }

  readString(): string | null {
    const length = this.readInt16();
    if (length === -1) return null;
    this.requireBytes(length);
    const value = this.buffer.toString('utf8', this.offset, this.offset + length);
    this.offset += length;
    return value;
  }

  readArray<T>(readItem: () => T): readonly T[] | null {
    const count = this.readInt32();
    if (count === -1) return null;
    if (count < 0 || count > this.buffer.length - this.offset)
      throw new Error('KAFKA_METADATA_ARRAY_LENGTH_INVALID');
    return Array.from({ length: count }, readItem);
  }

  readUnsignedVarInt(): number {
    let result = 0;
    for (let shift = 0; shift <= 28; shift += 7) {
      this.requireBytes(1);
      const current = this.buffer[this.offset++]!;
      result |= (current & 0x7f) << shift;
      if ((current & 0x80) === 0) {
        return result >>> 0;
      }
    }
    throw new Error('KAFKA_METADATA_VARINT_INVALID');
  }

  readCompactNullableString(): string | null {
    const encodedLength = this.readUnsignedVarInt();
    if (encodedLength === 0) {
      return null;
    }
    const length = encodedLength - 1;
    this.requireBytes(length);
    const value = this.buffer.toString('utf8', this.offset, this.offset + length);
    this.offset += length;
    return value;
  }

  readCompactArray<T>(readItem: () => T): readonly T[] | null {
    const encodedLength = this.readUnsignedVarInt();
    if (encodedLength === 0) {
      return null;
    }
    const itemCount = encodedLength - 1;
    if (itemCount > this.buffer.length - this.offset) {
      throw new Error('KAFKA_METADATA_ARRAY_LENGTH_INVALID');
    }
    const items: T[] = [];
    for (let index = 0; index < itemCount; index += 1) {
      items.push(readItem());
    }
    return items;
  }

  readUuid(): string | null {
    this.requireBytes(16);
    const bytes = this.buffer.subarray(this.offset, this.offset + 16);
    this.offset += 16;
    return bytes.every((value) => value === 0) ? null : bytes.toString('base64url');
  }

  readTaggedFields(): void {
    const count = this.readUnsignedVarInt();
    for (let index = 0; index < count; index += 1) {
      this.readUnsignedVarInt();
      const size = this.readUnsignedVarInt();
      this.requireBytes(size);
      this.offset += size;
    }
  }
}

class KafkaFrameReader {
  private buffered = Buffer.alloc(0);
  private readonly chunks: AsyncIterator<Buffer>;

  constructor(socket: Socket) {
    this.chunks = socket[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
  }

  async read(correlationId: number): Promise<Buffer> {
    while (this.buffered.length < 4) {
      await this.readChunk();
    }
    const frameLength = this.buffered.readInt32BE(0);
    if (frameLength < 4 || frameLength > MAX_FRAME_BYTES) {
      throw new Error('KAFKA_METADATA_FRAME_LENGTH_INVALID');
    }
    while (this.buffered.length < frameLength + 4) {
      await this.readChunk();
    }
    const frame = this.buffered.subarray(4, frameLength + 4);
    this.buffered = this.buffered.subarray(frameLength + 4);
    if (frame.readInt32BE(0) !== correlationId) {
      throw new Error('KAFKA_METADATA_CORRELATION_ID_MISMATCH');
    }
    return frame.subarray(4);
  }

  private async readChunk(): Promise<void> {
    const next = await this.chunks.next();
    if (next.done) {
      const error = new Error('KAFKA_METADATA_CONNECTION_CLOSED') as Error & { code?: string };
      error.code = 'ECONNRESET';
      throw error;
    }
    this.buffered = Buffer.concat([this.buffered, next.value]);
  }
}

function int16(value: number): Buffer {
  const buffer = Buffer.allocUnsafe(2);
  buffer.writeInt16BE(value);
  return buffer;
}

function int32(value: number): Buffer {
  const buffer = Buffer.allocUnsafe(4);
  buffer.writeInt32BE(value);
  return buffer;
}

function kafkaString(value: string): Buffer {
  const bytes = Buffer.from(value, 'utf8');
  return Buffer.concat([int16(bytes.length), bytes]);
}

function requestFrame(
  apiKey: number,
  apiVersion: number,
  correlationId: number,
  flexibleHeader: boolean,
  body: Buffer,
): Buffer {
  const request = Buffer.concat([
    int16(apiKey),
    int16(apiVersion),
    int32(correlationId),
    kafkaString('scs-data-sync-platform-topic-id'),
    ...(flexibleHeader ? [Buffer.from([0])] : []),
    body,
  ]);
  return Buffer.concat([int32(request.length), request]);
}

function requireArray<T>(value: readonly T[] | null): readonly T[] {
  if (value === null) {
    throw new Error('KAFKA_METADATA_REQUIRED_ARRAY_IS_NULL');
  }
  return value;
}

function throwKafkaError(errorCode: number): never {
  if (errorCode === 29) {
    const error = new Error('TOPIC_AUTHORIZATION_FAILED') as Error & { type?: string };
    error.type = 'TOPIC_AUTHORIZATION_FAILED';
    throw error;
  }
  throw new Error(`KAFKA_METADATA_ERROR:${errorCode}`);
}

function metadataMaxVersion(response: Buffer): number {
  const decoder = new KafkaDecoder(response);
  const errorCode = decoder.readInt16();
  if (errorCode !== 0) {
    throwKafkaError(errorCode);
  }
  const apiCount = decoder.readInt32();
  if (apiCount < 0) {
    throw new Error('KAFKA_API_VERSIONS_RESPONSE_INVALID');
  }
  let maximum = -1;
  for (let index = 0; index < apiCount; index += 1) {
    const apiKey = decoder.readInt16();
    decoder.readInt16();
    const maxVersion = decoder.readInt16();
    if (apiKey === METADATA_KEY) {
      maximum = maxVersion;
    }
  }
  decoder.readInt32();
  return maximum;
}

function decodePartition(decoder: KafkaDecoder): KafkaPartitionMetadata {
  const errorCode = decoder.readInt16();
  if (errorCode !== 0) {
    throwKafkaError(errorCode);
  }
  const partitionId = decoder.readInt32();
  const leader = decoder.readInt32();
  decoder.readInt32();
  requireArray(decoder.readCompactArray(() => decoder.readInt32()));
  requireArray(decoder.readCompactArray(() => decoder.readInt32()));
  requireArray(decoder.readCompactArray(() => decoder.readInt32()));
  decoder.readTaggedFields();
  return { leader, partitionId };
}

function decodeTopic(decoder: KafkaDecoder): KafkaTopicMetadata {
  const errorCode = decoder.readInt16();
  if (errorCode !== 0) {
    throwKafkaError(errorCode);
  }
  const name = decoder.readCompactNullableString();
  const topicId = decoder.readUuid();
  decoder.readInt8();
  const partitions = requireArray(decoder.readCompactArray(() => decodePartition(decoder)));
  decoder.readInt32();
  decoder.readTaggedFields();
  if (name === null) {
    throw new Error('KAFKA_METADATA_TOPIC_NAME_MISSING');
  }
  return { name, partitions, topicId };
}

// Metadata v4 is supported by Kafka 2.1 and returns real partition facts, but no Topic UUID.
function decodeLegacyMetadata(response: Buffer): readonly KafkaTopicMetadata[] {
  const decoder = new KafkaDecoder(response);
  decoder.readInt32();
  requireArray(
    decoder.readArray(() => {
      decoder.readInt32();
      decoder.readString();
      decoder.readInt32();
      decoder.readString();
    }),
  );
  decoder.readString();
  decoder.readInt32();
  return requireArray(
    decoder.readArray(() => {
      const error = decoder.readInt16();
      if (error !== 0) throwKafkaError(error);
      const name = decoder.readString();
      if (name === null) throw new Error('KAFKA_METADATA_TOPIC_NAME_MISSING');
      decoder.readInt8();
      const partitions = requireArray(
        decoder.readArray(() => {
          const partitionError = decoder.readInt16();
          if (partitionError !== 0) throwKafkaError(partitionError);
          const partitionId = decoder.readInt32();
          const leader = decoder.readInt32();
          requireArray(decoder.readArray(() => decoder.readInt32()));
          requireArray(decoder.readArray(() => decoder.readInt32()));
          return { partitionId, leader };
        }),
      );
      return { name, partitions, topicId: null };
    }),
  );
}

function decodeMetadata(response: Buffer): readonly KafkaTopicMetadata[] {
  const decoder = new KafkaDecoder(response);
  decoder.readTaggedFields();
  decoder.readInt32();
  requireArray(
    decoder.readCompactArray(() => {
      decoder.readInt32();
      decoder.readCompactNullableString();
      decoder.readInt32();
      decoder.readCompactNullableString();
      decoder.readTaggedFields();
    }),
  );
  decoder.readCompactNullableString();
  decoder.readInt32();
  const topics = requireArray(decoder.readCompactArray(() => decodeTopic(decoder)));
  decoder.readInt32();
  decoder.readTaggedFields();
  return topics;
}

function parseBootstrapServer(server: string): { readonly host: string; readonly port: number } {
  const separator = server.lastIndexOf(':');
  return { host: server.slice(0, separator), port: Number(server.slice(separator + 1)) };
}

async function connect(server: string): Promise<Socket> {
  const { host, port } = parseBootstrapServer(server);
  const socket = createConnection({ host, port });
  socket.setTimeout(REQUEST_TIMEOUT_MS, () => {
    const error = new Error('KAFKA_METADATA_REQUEST_TIMEOUT') as Error & { code?: string };
    error.code = 'ETIMEDOUT';
    socket.destroy(error);
  });
  await new Promise<void>((resolve, reject) => {
    const connected = () => {
      socket.off('error', failed);
      resolve();
    };
    const failed = (error: Error) => {
      socket.off('connect', connected);
      reject(error);
    };
    socket.once('connect', connected);
    socket.once('error', failed);
  });
  return socket;
}

async function fetchFromBroker(server: string): Promise<readonly KafkaTopicMetadata[]> {
  const socket = await connect(server);
  const reader = new KafkaFrameReader(socket);
  try {
    socket.write(requestFrame(API_VERSIONS_KEY, API_VERSIONS_VERSION, 1, false, Buffer.alloc(0)));
    const maximum = metadataMaxVersion(await reader.read(1));
    if (maximum < 4) {
      throw new DataSourceAdapterError('METADATA_UNSUPPORTED', 'Kafka Broker 不支持 Metadata v4。');
    }

    if (maximum < METADATA_VERSION) {
      socket.write(
        requestFrame(
          METADATA_KEY,
          4,
          2,
          false,
          Buffer.concat([
            int32(-1),
            Buffer.from([0]), // all topics; never auto-create
          ]),
        ),
      );
      return decodeLegacyMetadata(await reader.read(2));
    }

    const metadataBody = Buffer.from([
      0, // nullable Topics array: null means all Topics
      0, // allow_auto_topic_creation
      0, // include_cluster_authorized_operations
      0, // include_topic_authorized_operations
      0, // request tagged fields
    ]);
    socket.write(requestFrame(METADATA_KEY, METADATA_VERSION, 2, true, metadataBody));
    return decodeMetadata(await reader.read(2));
  } finally {
    socket.destroy();
  }
}

export async function fetchKafkaTopicMetadata(
  bootstrapServers: readonly string[],
): Promise<readonly KafkaTopicMetadata[]> {
  let lastError: unknown;
  for (const server of bootstrapServers) {
    try {
      return await fetchFromBroker(server);
    } catch (error) {
      if (error instanceof DataSourceAdapterError) {
        throw error;
      }
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error('KAFKA_BOOTSTRAP_SERVERS_EMPTY');
}
