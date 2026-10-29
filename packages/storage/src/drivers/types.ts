import type { Readable } from 'node:stream';

export type StorageDriverKind = 'LOCAL' | 'OSS';

export interface StoredObjectMetadata {
  readonly contentType?: string;
  readonly size: number;
}

export interface ObjectStorageDriver {
  readonly driver: StorageDriverKind;
  headObject(objectKey: string): Promise<StoredObjectMetadata>;
  openObject(objectKey: string): Promise<Readable>;
}
