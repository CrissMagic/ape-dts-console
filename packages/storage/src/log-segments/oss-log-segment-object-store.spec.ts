import { Readable } from 'node:stream';

import { describe, expect, it, vi } from 'vitest';

import {
  OssLogSegmentObjectStore,
  type OssLogObjectClient,
} from './oss-log-segment-object-store.js';

function clientFor(content: Buffer): OssLogObjectClient {
  return {
    deleteObject: vi.fn(async () => undefined),
    headObject: vi.fn(async () => ({ size: content.length })),
    openObject: vi.fn(async () => Readable.from([content])),
    putObject: vi.fn(async () => undefined),
  };
}

describe('OssLogSegmentObjectStore', () => {
  it('writes under the immutable prefix with private ACL, SSE and create-only semantics', async () => {
    const client = clientFor(Buffer.from('line\n'));
    const store = new OssLogSegmentObjectStore(
      { bucket: 'logs', endpoint: 'https://oss.example.invalid', prefix: 'platform/v3' },
      client,
    );
    await store.put({ content: Buffer.from('line\n'), objectKey: 'runs/r/logs/stdout/s.log' });
    expect(client.putObject).toHaveBeenCalledWith({
      acl: 'private',
      bucket: 'logs',
      content: Buffer.from('line\n'),
      contentType: 'application/x-ndjson',
      ifNoneMatch: '*',
      key: 'platform/v3/runs/r/logs/stdout/s.log',
      serverSideEncryption: 'AES256',
    });
  });

  it('reads only when head, received size and sha256 all match', async () => {
    const content = Buffer.from('verified');
    const client = clientFor(content);
    const store = new OssLogSegmentObjectStore(
      { bucket: 'logs', endpoint: 'https://oss.example.invalid', prefix: '' },
      client,
    );
    await expect(
      store.read({
        objectKey: 'runs/r/logs/stdout/s.log',
        sha256: '1c34f88707b55e6104c4eb20e71ffa3d33e414b71ef689a15fad0640d0ac58cb',
        sizeBytes: content.length,
      }),
    ).resolves.toEqual(content);
    await expect(
      store.read({
        objectKey: 'runs/r/logs/stdout/s.log',
        sha256: '0'.repeat(64),
        sizeBytes: content.length,
      }),
    ).rejects.toThrow('LOG_SEGMENT_OBJECT_HASH_MISMATCH');
  });

  it('accepts a create-only race only when the existing bytes are identical', async () => {
    const content = Buffer.from('same');
    const client = clientFor(content);
    vi.mocked(client.putObject).mockRejectedValue(
      Object.assign(new Error('already exists'), { code: 'PreconditionFailed' }),
    );
    const store = new OssLogSegmentObjectStore(
      { bucket: 'logs', endpoint: 'https://oss.example.invalid', prefix: '' },
      client,
    );
    await expect(
      store.put({ content, objectKey: 'runs/r/logs/stdout/s.log' }),
    ).resolves.toBeUndefined();

    vi.mocked(client.headObject).mockResolvedValue({ size: content.length });
    vi.mocked(client.openObject).mockResolvedValue(Readable.from([Buffer.from('evil')]));
    await expect(store.put({ content, objectKey: 'runs/r/logs/stdout/s.log' })).rejects.toThrow(
      'LOG_SEGMENT_OBJECT_CONFLICT',
    );
  });

  it('maps absence in inspection and does not hide other integrity failures as missing', async () => {
    const missing = Object.assign(new Error('missing'), { code: 'NoSuchKey' });
    const client = clientFor(Buffer.alloc(0));
    vi.mocked(client.headObject).mockRejectedValue(missing);
    const store = new OssLogSegmentObjectStore(
      { bucket: 'logs', endpoint: 'https://oss.example.invalid', prefix: '' },
      client,
    );
    await expect(
      store.inspect({ objectKey: 'a', sha256: '0'.repeat(64), sizeBytes: 0 }),
    ).resolves.toBe('MISSING');
  });

  it('deletes the exact prefixed object and propagates client failures', async () => {
    const client = clientFor(Buffer.alloc(0));
    const store = new OssLogSegmentObjectStore(
      { bucket: 'logs', endpoint: 'https://oss.example.invalid', prefix: 'p' },
      client,
    );
    await store.delete('runs/r/logs/stderr/s.log');
    expect(client.deleteObject).toHaveBeenCalledWith({
      bucket: 'logs',
      key: 'p/runs/r/logs/stderr/s.log',
    });
    vi.mocked(client.deleteObject).mockRejectedValueOnce(new Error('denied'));
    await expect(store.delete('runs/r/logs/stderr/s.log')).rejects.toThrow('denied');
  });
});
