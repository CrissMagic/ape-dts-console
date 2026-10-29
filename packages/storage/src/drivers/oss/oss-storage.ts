import type { Readable } from 'node:stream';

import type { ObjectStorageDriver, StoredObjectMetadata } from '../types.js';

export interface OssProfileConfig {
  readonly bucket: string;
  readonly endpoint: string;
  readonly prefix: string;
}

export interface OssObjectRequest {
  readonly bucket: string;
  readonly key: string;
}

export interface OssObjectClient {
  headObject(request: OssObjectRequest): Promise<StoredObjectMetadata>;
  openObject(request: OssObjectRequest): Promise<Readable>;
}

export class OssObjectKeyError extends Error {
  readonly code = 'OSS_OBJECT_KEY_INVALID';

  constructor() {
    super('OSS_OBJECT_KEY_INVALID');
    this.name = 'OssObjectKeyError';
  }
}

function validateNonEmptyScalar(value: string): void {
  if (value.trim().length === 0 || hasControlCharacter(value)) {
    throw new OssObjectKeyError();
  }
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 0x1f || code === 0x7f;
  });
}

function validatePrefix(prefix: string): void {
  if (prefix.length === 0) {
    return;
  }
  validateNonEmptyScalar(prefix);
  if (
    prefix.startsWith('/') ||
    prefix.endsWith('/') ||
    prefix.split('/').some((segment) => segment.length === 0 || segment === '.' || segment === '..')
  ) {
    throw new OssObjectKeyError();
  }
}

function validateObjectKey(objectKey: string): void {
  validateNonEmptyScalar(objectKey);
  if (objectKey.startsWith('/')) {
    throw new OssObjectKeyError();
  }
}

export class OssObjectStorage implements ObjectStorageDriver {
  readonly driver = 'OSS' as const;
  private readonly config: OssProfileConfig;

  constructor(
    config: OssProfileConfig,
    private readonly client: OssObjectClient,
  ) {
    validateNonEmptyScalar(config.bucket);
    validateNonEmptyScalar(config.endpoint);
    validatePrefix(config.prefix);
    this.config = Object.freeze({ ...config });
  }

  objectRequest(objectKey: string): OssObjectRequest {
    validateObjectKey(objectKey);
    return {
      bucket: this.config.bucket,
      key: this.config.prefix.length === 0 ? objectKey : `${this.config.prefix}/${objectKey}`,
    };
  }

  async headObject(objectKey: string): Promise<StoredObjectMetadata> {
    const metadata = await this.client.headObject(this.objectRequest(objectKey));
    if (!Number.isSafeInteger(metadata.size) || metadata.size < 0) {
      throw new Error('OSS_OBJECT_SIZE_INVALID');
    }
    return metadata;
  }

  async openObject(objectKey: string): Promise<Readable> {
    return await this.client.openObject(this.objectRequest(objectKey));
  }
}
