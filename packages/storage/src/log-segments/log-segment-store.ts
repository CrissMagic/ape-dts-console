import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { link, lstat, mkdir, open, realpath, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';

import type { LogSegmentRecord } from './types.js';

export interface FileLogSegmentObjectStore {
  readonly root: string;
  delete(objectKey: string): Promise<void>;
  inspect(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<'MATCH' | 'MISSING' | 'MISMATCH'>;
  put(input: { readonly content: Buffer; readonly objectKey: string }): Promise<void>;
  read(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<Buffer>;
}

/**
 * Small LOCAL object writer used by the log pipeline.  It only accepts object
 * keys relative to its configured root and publishes through a same-directory
 * hard link so a reader never observes a partial segment or an overwrite.
 */
export class LocalLogSegmentObjectStore implements FileLogSegmentObjectStore {
  readonly root: string;

  constructor(root: string) {
    if (root.trim().length === 0 || root.includes('\u0000')) {
      throw new Error('LOG_SEGMENT_STORAGE_ROOT_INVALID');
    }
    this.root = path.resolve(root);
  }

  async put(input: { readonly content: Buffer; readonly objectKey: string }): Promise<void> {
    const finalPath = this.resolve(input.objectKey);
    const parent = path.dirname(finalPath);
    await this.ensureSafeDirectory(parent, true);
    const temporaryPath = `${finalPath}.${randomUUID()}.staging`;
    try {
      await writeFile(temporaryPath, input.content, {
        encoding: undefined,
        flag: 'wx',
        mode: 0o600,
      });
      try {
        await this.ensureSafeDirectory(parent, false);
        await link(temporaryPath, finalPath);
      } catch (error) {
        if (!isFileExistsError(error)) {
          throw error;
        }
        const existing = await this.readVerified(finalPath);
        if (existing === undefined) {
          throw error;
        }
        if (!existing.equals(input.content)) {
          throw new Error('LOG_SEGMENT_OBJECT_CONFLICT', { cause: error });
        }
      }
    } finally {
      await rm(temporaryPath, { force: true });
    }
  }

  async delete(objectKey: string): Promise<void> {
    const finalPath = this.resolve(objectKey);
    if (!(await this.ensureSafeDirectory(path.dirname(finalPath), false))) {
      return;
    }
    let status;
    try {
      status = await lstat(finalPath);
    } catch (error) {
      if (isFileMissingError(error)) {
        return;
      }
      throw error;
    }
    if (status.isSymbolicLink()) {
      throw new Error('LOG_SEGMENT_SYMLINK_REJECTED');
    }
    if (!status.isFile()) {
      throw new Error('LOG_SEGMENT_NOT_REGULAR_FILE');
    }
    try {
      await rm(finalPath);
    } catch (error) {
      if (!isFileMissingError(error)) {
        throw error;
      }
    }
  }

  async inspect(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<'MATCH' | 'MISSING' | 'MISMATCH'> {
    const content = await this.readVerified(this.resolve(input.objectKey));
    if (content === undefined) {
      return 'MISSING';
    }
    if (
      content.length !== input.sizeBytes ||
      createHash('sha256').update(content).digest('hex') !== input.sha256
    ) {
      return 'MISMATCH';
    }
    return 'MATCH';
  }

  async read(input: {
    readonly objectKey: string;
    readonly sha256: string;
    readonly sizeBytes: number;
  }): Promise<Buffer> {
    const content = await this.readVerified(this.resolve(input.objectKey));
    if (content === undefined) throw new Error('LOG_SEGMENT_OBJECT_MISSING');
    if (content.length !== input.sizeBytes) throw new Error('LOG_SEGMENT_OBJECT_SIZE_MISMATCH');
    if (createHash('sha256').update(content).digest('hex') !== input.sha256) {
      throw new Error('LOG_SEGMENT_OBJECT_HASH_MISMATCH');
    }
    return content;
  }

  private resolve(objectKey: string): string {
    if (
      objectKey.length === 0 ||
      objectKey.includes('\\') ||
      objectKey.includes('\u0000') ||
      path.posix.isAbsolute(objectKey) ||
      path.win32.isAbsolute(objectKey) ||
      objectKey
        .split('/')
        .some((segment) => segment.length === 0 || segment === '.' || segment === '..')
    ) {
      throw new Error('LOG_SEGMENT_OBJECT_KEY_INVALID');
    }
    const candidate = path.resolve(this.root, ...objectKey.split('/'));
    const relative = path.relative(this.root, candidate);
    if (
      relative.length === 0 ||
      relative === '..' ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative)
    ) {
      throw new Error('LOG_SEGMENT_OBJECT_KEY_ESCAPE');
    }
    return candidate;
  }

  private async ensureSafeDirectory(directory: string, create: boolean): Promise<boolean> {
    const root = path.parse(directory).root;
    const relative = path.relative(root, directory);
    let current = root;
    for (const segment of relative.split(path.sep).filter((value) => value.length > 0)) {
      current = path.join(current, segment);
      let status;
      try {
        status = await lstat(current);
      } catch (error) {
        if (!isFileMissingError(error) || !create) {
          if (isFileMissingError(error)) {
            return false;
          }
          throw error;
        }
        await mkdir(current, { mode: 0o700 });
        status = await lstat(current);
      }
      if (status.isSymbolicLink()) {
        throw new Error('LOG_SEGMENT_SYMLINK_REJECTED');
      }
      if (!status.isDirectory()) {
        throw new Error('LOG_SEGMENT_NOT_DIRECTORY');
      }
    }

    const resolvedRoot = await realpath(this.root);
    const resolvedDirectory = await realpath(directory);
    const relativeToRoot = path.relative(resolvedRoot, resolvedDirectory);
    if (
      relativeToRoot === '..' ||
      relativeToRoot.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relativeToRoot)
    ) {
      throw new Error('LOG_SEGMENT_OBJECT_KEY_ESCAPE');
    }
    return true;
  }

  private async readVerified(finalPath: string): Promise<Buffer | undefined> {
    if (!(await this.ensureSafeDirectory(path.dirname(finalPath), false))) {
      return undefined;
    }
    let status;
    try {
      status = await lstat(finalPath);
    } catch (error) {
      if (isFileMissingError(error)) {
        return undefined;
      }
      throw error;
    }
    if (status.isSymbolicLink()) {
      throw new Error('LOG_SEGMENT_SYMLINK_REJECTED');
    }

    const noFollow = typeof constants.O_NOFOLLOW === 'number' ? constants.O_NOFOLLOW : 0;
    let handle;
    try {
      handle = await open(finalPath, constants.O_RDONLY | noFollow);
    } catch (error) {
      if (isFileMissingError(error)) {
        return undefined;
      }
      throw error;
    }
    try {
      const openedStatus = await handle.stat();
      if (!openedStatus.isFile()) {
        throw new Error('LOG_SEGMENT_NOT_REGULAR_FILE');
      }
      const resolvedRoot = await realpath(this.root);
      const openedPath =
        process.platform === 'linux'
          ? await realpath(`/proc/self/fd/${handle.fd}`)
          : await realpath(finalPath);
      const relative = path.relative(resolvedRoot, openedPath);
      if (
        relative.length === 0 ||
        relative === '..' ||
        relative.startsWith(`..${path.sep}`) ||
        path.isAbsolute(relative)
      ) {
        throw new Error('LOG_SEGMENT_OBJECT_KEY_ESCAPE');
      }
      return await handle.readFile();
    } finally {
      await handle.close();
    }
  }
}

function isFileExistsError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && 'code' in error && error.code === 'EEXIST';
}

function isFileMissingError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && 'code' in error && error.code === 'ENOENT';
}

export function segmentObjectKey(
  segment: Pick<LogSegmentRecord, 'id' | 'runId' | 'stream'>,
): string {
  return `runs/${segment.runId}/logs/${segment.stream.toLowerCase()}/${segment.id}.log`;
}

export function segmentSha256(content: Uint8Array): string {
  return createHash('sha256').update(content).digest('hex');
}
