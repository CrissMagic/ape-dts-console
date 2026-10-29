import { constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import path from 'node:path';
import type { Readable } from 'node:stream';

import type { ObjectStorageDriver, StoredObjectMetadata } from '../types.js';

export const LOCAL_STORAGE_ROOT = '/data/storage';

export class LocalStoragePathError extends Error {
  constructor(
    readonly code:
      | 'LOCAL_STORAGE_PATH_INVALID'
      | 'LOCAL_STORAGE_PATH_ESCAPE'
      | 'LOCAL_STORAGE_SYMLINK_REJECTED'
      | 'LOCAL_STORAGE_NOT_REGULAR_FILE',
  ) {
    super(code);
    this.name = 'LocalStoragePathError';
  }
}

function segmentsFor(objectKey: string): readonly string[] {
  if (
    objectKey.length === 0 ||
    objectKey.includes('\\') ||
    hasControlCharacter(objectKey) ||
    path.posix.isAbsolute(objectKey) ||
    path.win32.isAbsolute(objectKey) ||
    /^[a-zA-Z]:/u.test(objectKey)
  ) {
    throw new LocalStoragePathError('LOCAL_STORAGE_PATH_INVALID');
  }
  const segments = objectKey.split('/');
  if (segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')) {
    throw new LocalStoragePathError('LOCAL_STORAGE_PATH_INVALID');
  }
  return segments;
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 0x1f || code === 0x7f;
  });
}

function isBelowRoot(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return (
    relative.length > 0 &&
    !relative.startsWith(`..${path.sep}`) &&
    relative !== '..' &&
    !path.isAbsolute(relative)
  );
}

export class LocalObjectStorage implements ObjectStorageDriver {
  readonly driver = 'LOCAL' as const;
  private readonly root: string;

  constructor(root: string) {
    if (root.trim().length === 0) {
      throw new LocalStoragePathError('LOCAL_STORAGE_PATH_INVALID');
    }
    this.root = path.resolve(root);
  }

  private async openVerified(objectKey: string) {
    const segments = segmentsFor(objectKey);
    const resolvedRoot = await realpath(this.root);
    let candidate = this.root;
    for (const segment of segments) {
      candidate = path.join(candidate, segment);
      const status = await lstat(candidate);
      if (status.isSymbolicLink()) {
        throw new LocalStoragePathError('LOCAL_STORAGE_SYMLINK_REJECTED');
      }
    }

    const resolvedCandidate = await realpath(candidate);
    if (!isBelowRoot(resolvedRoot, resolvedCandidate)) {
      throw new LocalStoragePathError('LOCAL_STORAGE_PATH_ESCAPE');
    }

    const noFollow = typeof constants.O_NOFOLLOW === 'number' ? constants.O_NOFOLLOW : 0;
    const handle = await open(candidate, constants.O_RDONLY | noFollow);
    try {
      const status = await handle.stat();
      if (!status.isFile()) {
        throw new LocalStoragePathError('LOCAL_STORAGE_NOT_REGULAR_FILE');
      }
      const openedPath =
        process.platform === 'linux'
          ? await realpath(`/proc/self/fd/${handle.fd}`)
          : await realpath(candidate);
      if (!isBelowRoot(resolvedRoot, openedPath)) {
        throw new LocalStoragePathError('LOCAL_STORAGE_PATH_ESCAPE');
      }
      return { handle, size: status.size };
    } catch (error) {
      await handle.close();
      throw error;
    }
  }

  async headObject(objectKey: string): Promise<StoredObjectMetadata> {
    const opened = await this.openVerified(objectKey);
    await opened.handle.close();
    return { size: opened.size };
  }

  async openObject(objectKey: string): Promise<Readable> {
    const opened = await this.openVerified(objectKey);
    return opened.handle.createReadStream({ autoClose: true });
  }
}

export function createLocalObjectStorage(): LocalObjectStorage {
  return new LocalObjectStorage(LOCAL_STORAGE_ROOT);
}
