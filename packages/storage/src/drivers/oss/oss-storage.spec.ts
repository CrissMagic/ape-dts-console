import { Readable } from 'node:stream';

import { describe, expect, it } from 'vitest';

import { OssObjectStorage, type OssObjectClient } from './oss-storage.js';

describe('OssObjectStorage', () => {
  it('treats the object key as opaque remote data and applies the immutable profile prefix', async () => {
    const requestedKeys: string[] = [];
    const client: OssObjectClient = {
      headObject: async ({ bucket, key }) => {
        expect(bucket).toBe('private-bucket');
        requestedKeys.push(key);
        return { contentType: 'application/octet-stream', size: 3 };
      },
      openObject: async ({ bucket, key }) => {
        expect(bucket).toBe('private-bucket');
        requestedKeys.push(key);
        return Readable.from([Buffer.from('elf')]);
      },
    };
    const storage = new OssObjectStorage(
      {
        bucket: 'private-bucket',
        endpoint: 'https://oss.example.invalid',
        prefix: 'tenant/version-3',
      },
      client,
    );

    await expect(storage.headObject('candidate/../../dt-main')).resolves.toMatchObject({ size: 3 });
    await storage.openObject('candidate/../../dt-main');
    expect(requestedKeys).toEqual([
      'tenant/version-3/candidate/../../dt-main',
      'tenant/version-3/candidate/../../dt-main',
    ]);
  });

  it.each(['', '/leading-slash', 'line\nbreak', 'nul\0byte'])(
    'rejects a non-canonical OSS key: %s',
    async (objectKey) => {
      const client: OssObjectClient = {
        headObject: async () => ({ size: 0 }),
        openObject: async () => Readable.from([]),
      };
      const storage = new OssObjectStorage(
        { bucket: 'bucket', endpoint: 'https://oss.example.invalid', prefix: '' },
        client,
      );

      await expect(storage.headObject(objectKey)).rejects.toMatchObject({
        code: 'OSS_OBJECT_KEY_INVALID',
      });
    },
  );
});
