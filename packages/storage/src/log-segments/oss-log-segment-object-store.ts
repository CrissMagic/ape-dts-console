import { createHash } from 'node:crypto';

import type { StoredObjectMetadata } from '../drivers/types.js';
import {
  OssObjectStorage,
  type OssObjectClient,
  type OssObjectRequest,
  type OssProfileConfig,
} from '../drivers/oss/oss-storage.js';

export interface OssLogObjectPutRequest extends OssObjectRequest {
  readonly acl: 'private';
  readonly content: Buffer;
  readonly contentType: 'application/x-ndjson';
  readonly ifNoneMatch: '*';
  readonly serverSideEncryption: 'AES256';
}

export interface OssLogObjectClient extends OssObjectClient {
  deleteObject(request: OssObjectRequest): Promise<void>;
  putObject(request: OssLogObjectPutRequest): Promise<void>;
}

/**
 * Immutable OSS log segment adapter. The concrete client is responsible for
 * authenticated HTTPS transport; every write explicitly requests a private
 * object ACL, server-side encryption and create-only semantics.
 */
export class OssLogSegmentObjectStore {
  private readonly objects: OssObjectStorage;

  constructor(
    config: OssProfileConfig,
    private readonly client: OssLogObjectClient,
  ) {
    this.objects = new OssObjectStorage(config, client);
  }

  async put(input: { readonly content: Buffer; readonly objectKey: string }): Promise<void> {
    const request = this.objects.objectRequest(input.objectKey);
    try {
      await this.client.putObject({
        ...request,
        acl: 'private',
        content: input.content,
        contentType: 'application/x-ndjson',
        ifNoneMatch: '*',
        serverSideEncryption: 'AES256',
      });
    } catch (error) {
      if (!isPreconditionFailure(error)) throw error;
      try {
        await this.read({
          objectKey: input.objectKey,
          sha256: sha256(input.content),
          sizeBytes: input.content.length,
        });
      } catch (readError) {
        throw new Error('LOG_SEGMENT_OBJECT_CONFLICT', { cause: readError });
      }
    }
  }

  async delete(objectKey: string): Promise<void> {
    await this.client.deleteObject(this.objects.objectRequest(objectKey));
  }

  async inspect(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<'MATCH' | 'MISSING' | 'MISMATCH'> {
    try {
      await this.read(input);
      return 'MATCH';
    } catch (error) {
      return isObjectMissing(error) ? 'MISSING' : 'MISMATCH';
    }
  }

  async read(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<Buffer> {
    validateExpectedMetadata(input);
    let metadata: StoredObjectMetadata;
    try {
      metadata = await this.objects.headObject(input.objectKey);
    } catch (error) {
      if (isObjectMissing(error)) throw new Error('LOG_SEGMENT_OBJECT_MISSING', { cause: error });
      throw error;
    }
    if (metadata.size !== input.sizeBytes) throw new Error('LOG_SEGMENT_OBJECT_SIZE_MISMATCH');
    const stream = await this.objects.openObject(input.objectKey);
    const chunks: Buffer[] = [];
    let received = 0;
    for await (const chunk of stream) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      received += bytes.length;
      if (received > input.sizeBytes) {
        stream.destroy();
        throw new Error('LOG_SEGMENT_OBJECT_SIZE_MISMATCH');
      }
      chunks.push(bytes);
    }
    const content = Buffer.concat(chunks, received);
    if (received !== input.sizeBytes) throw new Error('LOG_SEGMENT_OBJECT_SIZE_MISMATCH');
    if (sha256(content) !== input.sha256) throw new Error('LOG_SEGMENT_OBJECT_HASH_MISMATCH');
    return content;
  }
}

function validateExpectedMetadata(input: { readonly sha256: string; readonly sizeBytes: number }) {
  if (
    !Number.isSafeInteger(input.sizeBytes) ||
    input.sizeBytes < 0 ||
    !/^[a-f0-9]{64}$/u.test(input.sha256)
  ) {
    throw new Error('LOG_SEGMENT_EXPECTED_METADATA_INVALID');
  }
}

function sha256(content: Uint8Array): string {
  return createHash('sha256').update(content).digest('hex');
}

function errorCode(error: unknown): string | number | undefined {
  if (typeof error !== 'object' || error === null || !('code' in error)) return undefined;
  return (error as { code?: string | number }).code;
}

function isObjectMissing(error: unknown): boolean {
  return (
    (error instanceof Error && error.message === 'LOG_SEGMENT_OBJECT_MISSING') ||
    ['NoSuchKey', 'NotFound', 404].includes(errorCode(error) as never)
  );
}

function isPreconditionFailure(error: unknown): boolean {
  return ['PreconditionFailed', 412].includes(errorCode(error) as never);
}
