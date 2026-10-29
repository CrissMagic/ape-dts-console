import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { LocalObjectStorage, LocalStoragePathError } from './local-storage.js';

const cleanupPaths: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'scs-local-storage-'));
  cleanupPaths.push(directory);
  return directory;
}

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    if (typeof chunk === 'string' || chunk instanceof Uint8Array) {
      chunks.push(Buffer.from(chunk));
      continue;
    }
    throw new Error('unexpected stream chunk type');
  }
  return Buffer.concat(chunks);
}

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map(async (directory) => {
      await rm(directory, { force: true, recursive: true });
    }),
  );
});

describe('LocalObjectStorage', () => {
  it('reads only a canonical relative object path below the configured root', async () => {
    const root = await temporaryDirectory();
    await mkdir(path.join(root, 'artifacts'), { recursive: true });
    await writeFile(path.join(root, 'artifacts', 'dt-main'), 'verified-binary');
    const storage = new LocalObjectStorage(root);

    await expect(storage.headObject('artifacts/dt-main')).resolves.toMatchObject({ size: 15 });
    await expect(readAll(await storage.openObject('artifacts/dt-main'))).resolves.toEqual(
      Buffer.from('verified-binary'),
    );
  });

  it.each([
    '',
    '/absolute',
    'C:\\absolute',
    'C:drive-relative',
    '../escape',
    'a/../escape',
    './relative',
    'a//b',
    'a/',
    'a\\b',
    'a\0b',
  ])('rejects a non-canonical local object path: %s', async (objectKey) => {
    const storage = new LocalObjectStorage(await temporaryDirectory());

    await expect(storage.headObject(objectKey)).rejects.toBeInstanceOf(LocalStoragePathError);
  });

  it('rejects a symbolic-link component instead of following it outside the root', async () => {
    const base = await temporaryDirectory();
    const root = path.join(base, 'root');
    const outside = path.join(base, 'outside');
    await mkdir(root);
    await mkdir(outside);
    await writeFile(path.join(outside, 'dt-main'), 'outside');
    await symlink(
      outside,
      path.join(root, 'escape'),
      process.platform === 'win32' ? 'junction' : 'dir',
    );
    const storage = new LocalObjectStorage(root);

    await expect(storage.openObject('escape/dt-main')).rejects.toMatchObject({
      code: 'LOCAL_STORAGE_SYMLINK_REJECTED',
    });
  });
});
