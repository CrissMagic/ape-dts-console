import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import {
  LocalLogSegmentObjectStore,
  segmentObjectKey,
  segmentSha256,
} from './log-segment-store.js';

describe('LocalLogSegmentObjectStore', () => {
  const roots: string[] = [];

  afterEach(async () => {
    await Promise.all(roots.splice(0).map((root) => rm(root, { force: true, recursive: true })));
  });

  it('publishes atomically below the configured root and deletes only the named object', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-log-segments-'));
    roots.push(root);
    const store = new LocalLogSegmentObjectStore(root);
    const key = 'runs/run-1/logs/stdout/segment.log';

    await store.put({ content: Buffer.from('safe log'), objectKey: key });
    await store.put({ content: Buffer.from('safe log'), objectKey: key });
    await expect(
      store.inspect({
        objectKey: key,
        sha256: segmentSha256(Buffer.from('safe log')),
        sizeBytes: 8,
      }),
    ).resolves.toBe('MATCH');
    await expect(
      store.inspect({ objectKey: key, sha256: segmentSha256(Buffer.from('other')), sizeBytes: 8 }),
    ).resolves.toBe('MISMATCH');
    await expect(
      store.read({ objectKey: key, sha256: segmentSha256(Buffer.from('safe log')), sizeBytes: 8 }),
    ).resolves.toEqual(Buffer.from('safe log'));
    await expect(
      store.read({ objectKey: key, sha256: segmentSha256(Buffer.from('other')), sizeBytes: 8 }),
    ).rejects.toThrow('LOG_SEGMENT_OBJECT_HASH_MISMATCH');
    await expect(
      store.read({ objectKey: key, sha256: segmentSha256(Buffer.from('safe log')), sizeBytes: 7 }),
    ).rejects.toThrow('LOG_SEGMENT_OBJECT_SIZE_MISMATCH');
    await expect(store.put({ content: Buffer.from('different'), objectKey: key })).rejects.toThrow(
      'LOG_SEGMENT_OBJECT_CONFLICT',
    );
    await expect(readFile(path.join(root, ...key.split('/')), 'utf8')).resolves.toBe('safe log');
    await store.delete(key);
    await expect(readFile(path.join(root, ...key.split('/')), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(
      store.inspect({
        objectKey: key,
        sha256: segmentSha256(Buffer.from('safe log')),
        sizeBytes: 8,
      }),
    ).resolves.toBe('MISSING');
    await expect(
      store.read({ objectKey: key, sha256: segmentSha256(Buffer.from('safe log')), sizeBytes: 8 }),
    ).rejects.toThrow('LOG_SEGMENT_OBJECT_MISSING');
  });

  it('rejects absolute, traversal, and empty object keys', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-log-segments-'));
    roots.push(root);
    const store = new LocalLogSegmentObjectStore(root);
    await expect(store.put({ content: Buffer.from('x'), objectKey: '../escape' })).rejects.toThrow(
      'LOG_SEGMENT_OBJECT_KEY_INVALID',
    );
    await expect(store.put({ content: Buffer.from('x'), objectKey: '/absolute' })).rejects.toThrow(
      'LOG_SEGMENT_OBJECT_KEY_INVALID',
    );
    await expect(store.put({ content: Buffer.from('x'), objectKey: '' })).rejects.toThrow(
      'LOG_SEGMENT_OBJECT_KEY_INVALID',
    );
  });

  it('uses stable content hashes and controlled object keys', () => {
    expect(segmentSha256(Buffer.from('x'))).toMatch(/^[a-f0-9]{64}$/u);
    expect(segmentObjectKey({ id: 'segment', runId: 'run', stream: 'STDERR' })).toBe(
      'runs/run/logs/stderr/segment.log',
    );
  });

  it('rejects symbolic-link roots and ancestors instead of writing or deleting outside', async () => {
    const base = await mkdtemp(path.join(tmpdir(), 'scs-log-segment-links-'));
    roots.push(base);
    const realRoot = path.join(base, 'real-root');
    const linkedRoot = path.join(base, 'linked-root');
    const outside = path.join(base, 'outside');
    await mkdir(realRoot);
    await mkdir(outside);
    await symlink(realRoot, linkedRoot, process.platform === 'win32' ? 'junction' : 'dir');

    const rootLinkStore = new LocalLogSegmentObjectStore(linkedRoot);
    await expect(
      rootLinkStore.put({ content: Buffer.from('x'), objectKey: 'runs/run/logs/stdout/root.log' }),
    ).rejects.toThrow('LOG_SEGMENT_SYMLINK_REJECTED');

    const store = new LocalLogSegmentObjectStore(realRoot);
    const outsideObject = path.join(outside, 'run', 'logs', 'stdout', 'outside.log');
    await mkdir(path.dirname(outsideObject), { recursive: true });
    await writeFile(outsideObject, 'outside');
    await symlink(
      outside,
      path.join(realRoot, 'runs'),
      process.platform === 'win32' ? 'junction' : 'dir',
    );
    const key = 'runs/run/logs/stdout/outside.log';

    await expect(
      store.put({ content: Buffer.from('replacement'), objectKey: key }),
    ).rejects.toThrow('LOG_SEGMENT_SYMLINK_REJECTED');
    await expect(store.delete(key)).rejects.toThrow('LOG_SEGMENT_SYMLINK_REJECTED');
    await expect(
      store.read({ objectKey: key, sha256: segmentSha256(Buffer.from('outside')), sizeBytes: 7 }),
    ).rejects.toThrow('LOG_SEGMENT_SYMLINK_REJECTED');
    await expect(readFile(outsideObject, 'utf8')).resolves.toBe('outside');
  });
});
