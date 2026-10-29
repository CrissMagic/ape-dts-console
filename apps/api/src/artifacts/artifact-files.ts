import { createReadStream, createWriteStream, constants } from 'node:fs';
import {
  copyFile,
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readdir,
  realpath,
  rm,
} from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { Transform, type Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createGunzip } from 'node:zlib';
import tar from 'tar-stream';
import { LocalObjectStorage } from '@scs/storage';

const MAX_FILE = 512 * 1024 * 1024;
const MAX_EXPANDED = 2 * 1024 * 1024 * 1024;
export class ArtifactFileError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'ArtifactFileError';
  }
}
export interface ArtifactFileCandidate {
  name: string;
  sizeBytes: number;
  modifiedAt: string;
}
export interface PreparedArtifactFile {
  path: string;
  sha256: string;
  sizeBytes: number;
  fileName: string;
  suggestedVersion: string;
}
function fail(code: string): never {
  throw new ArtifactFileError(code);
}
function segments(value: string, empty = false): string[] {
  if (empty && value === '') return [];
  if (
    !value ||
    /[\\:]/u.test(value) ||
    [...value].some(
      (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
    ) ||
    path.posix.isAbsolute(value)
  )
    fail('ARTIFACT_PATH_INVALID');
  const result = value.split('/');
  if (result.some((part) => !part || part === '.' || part === '..')) fail('ARTIFACT_PATH_INVALID');
  return result;
}
function limiter(max: number) {
  let size = 0;
  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      size += chunk.length;
      callback(size > max ? new ArtifactFileError('ARTIFACT_SIZE_LIMIT') : null, chunk);
    },
  });
}
async function elf(file: string) {
  const handle = await open(file, 'r');
  try {
    const bytes = Buffer.alloc(4);
    const result = await handle.read(bytes, 0, 4, 0);
    return result.bytesRead === 4 && bytes.equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]));
  } finally {
    await handle.close();
  }
}
function kind(name: string): 'TAR_GZ' | 'TAR_XZ' | 'BINARY' {
  return name.endsWith('.tar.gz') ? 'TAR_GZ' : name.endsWith('.tar.xz') ? 'TAR_XZ' : 'BINARY';
}

export class ArtifactFiles {
  private preparing = false;
  constructor(private readonly root = '/data/storage') {}
  async upload<T>(
    prefix: string,
    name: string,
    input: Readable,
    inspect: (binary: PreparedArtifactFile) => Promise<T>,
    signal?: AbortSignal,
  ) {
    if (this.preparing) fail('ARTIFACT_FILE_BUSY');
    if (segments(name).length !== 1 || Buffer.byteLength(name) > 255) fail('ARTIFACT_PATH_INVALID');
    this.preparing = true;
    let temporary: string | undefined;
    let published: string | undefined;
    try {
      const parts = [...segments(prefix, true), 'artifact-inbox'];
      const directory = await this.directory(parts);
      const staging = await this.directory([...parts, '.uploads']);
      temporary = await mkdtemp(path.join(staging, 'upload-'));
      const snapshot = path.join(temporary, 'input');
      await pipeline(
        input,
        limiter(MAX_FILE),
        createWriteStream(snapshot, { flags: 'wx', mode: 0o600 }),
        { signal },
      );
      if ((await lstat(snapshot)).size === 0) fail('ARTIFACT_BINARY_INVALID');
      const prepared = await this.inspectSnapshot(snapshot, name, temporary, inspect, signal);
      signal?.throwIfAborted();
      // Re-check the configured directory before atomically linking the completed file.
      await this.directory(parts);
      const targetName = name;
      const target = path.join(directory, targetName);
      await link(snapshot, target).catch((error: NodeJS.ErrnoException) => {
        if (error.code === 'EEXIST') fail('ARTIFACT_FILE_ALREADY_EXISTS');
        throw error;
      });
      published = target;
      signal?.throwIfAborted();
      const status = await lstat(target);
      const result = {
        file: {
          name: targetName,
          sizeBytes: status.size,
          modifiedAt: status.mtime.toISOString(),
          kind: kind(name),
        },
        prepared,
      };
      signal?.throwIfAborted();
      published = undefined;
      return result;
    } finally {
      try {
        if (published) await rm(published, { force: true });
        if (temporary) await rm(temporary, { recursive: true, force: true });
      } finally {
        this.preparing = false;
      }
    }
  }
  private async directory(parts: string[]) {
    const root = await realpath(this.root);
    if ((await lstat(this.root)).isSymbolicLink()) fail('ARTIFACT_PATH_INVALID');
    let current = root;
    for (const part of parts) {
      current = path.join(current, part);
      await mkdir(current, { mode: 0o700 }).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== 'EEXIST') throw error;
      });
      const status = await lstat(current);
      if (status.isSymbolicLink() || !status.isDirectory() || (await realpath(current)) !== current)
        fail('ARTIFACT_PATH_INVALID');
    }
    return current;
  }
  async scan(prefix: string) {
    const directory = await this.directory([...segments(prefix, true), 'artifact-inbox']);
    const files: (ArtifactFileCandidate & { kind: 'BINARY' | 'TAR_GZ' | 'TAR_XZ' })[] = [];
    const entries = await readdir(directory, { withFileTypes: true });
    if (entries.length > 10000) fail('ARTIFACT_DIRECTORY_LIMIT');
    for (const entry of entries) {
      if (!entry.isFile() || entry.isSymbolicLink() || entry.name.length > 255) continue;
      try {
        segments(entry.name);
      } catch {
        continue;
      }
      const file = path.join(directory, entry.name);
      const status = await lstat(file);
      if (
        !status.isFile() ||
        status.isSymbolicLink() ||
        status.size === 0 ||
        status.size > MAX_FILE
      )
        continue;
      const fileKind = kind(entry.name);
      if (fileKind === 'BINARY') {
        const stream = await new LocalObjectStorage(this.root).openObject(
          [...segments(prefix, true), 'artifact-inbox', entry.name].join('/'),
        );
        const bytes: Buffer[] = [];
        let size = 0;
        for await (const chunk of stream) {
          const buffer = Buffer.from(chunk as Uint8Array);
          bytes.push(buffer.subarray(0, 4 - size));
          size += bytes.at(-1)!.length;
          if (size >= 4) break;
        }
        if (!Buffer.concat(bytes).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) continue;
      }
      files.push({
        name: entry.name,
        sizeBytes: status.size,
        modifiedAt: status.mtime.toISOString(),
        kind: fileKind,
      });
    }
    return { directory, files: files.sort((a, b) => a.name.localeCompare(b.name)) };
  }
  async withPrepared<T>(
    prefix: string,
    candidate: ArtifactFileCandidate,
    work: (binary: PreparedArtifactFile) => Promise<T>,
  ): Promise<T> {
    if (this.preparing) fail('ARTIFACT_FILE_BUSY');
    this.preparing = true;
    try {
      return await this.prepare(prefix, candidate, work);
    } finally {
      this.preparing = false;
    }
  }
  private async prepare<T>(
    prefix: string,
    candidate: ArtifactFileCandidate,
    work: (binary: PreparedArtifactFile) => Promise<T>,
  ): Promise<T> {
    if (segments(candidate.name).length !== 1) fail('ARTIFACT_PATH_INVALID');
    const directory = await this.directory([...segments(prefix, true), 'artifact-inbox']);
    const source = path.join(directory, candidate.name);
    const verify = async () => {
      const status = await lstat(source).catch(() => fail('ARTIFACT_FILE_CHANGED'));
      if (
        !status.isFile() ||
        status.isSymbolicLink() ||
        status.size !== candidate.sizeBytes ||
        status.mtime.toISOString() !== candidate.modifiedAt
      )
        fail('ARTIFACT_FILE_CHANGED');
      if (status.size > MAX_FILE) fail('ARTIFACT_SIZE_LIMIT');
      return status;
    };
    const before = await verify();
    const temporary = await mkdtemp(path.join(tmpdir(), 'scs-artifact-import-'));
    try {
      const snapshot = path.join(temporary, 'input');
      const key = [...segments(prefix, true), 'artifact-inbox', candidate.name].join('/');
      await pipeline(
        await new LocalObjectStorage(this.root).openObject(key),
        limiter(MAX_FILE),
        createWriteStream(snapshot, { flags: 'wx', mode: 0o600 }),
      );
      const after = await verify();
      if (
        before.ino !== after.ino ||
        before.ctimeMs !== after.ctimeMs ||
        (await lstat(snapshot)).size !== candidate.sizeBytes
      )
        fail('ARTIFACT_FILE_CHANGED');
      return await this.inspectSnapshot(snapshot, candidate.name, temporary, work);
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  }
  private async inspectSnapshot<T>(
    snapshot: string,
    name: string,
    temporary: string,
    work: (binary: PreparedArtifactFile) => Promise<T>,
    signal?: AbortSignal,
  ) {
    signal?.throwIfAborted();
    const prepared =
      kind(name) === 'BINARY'
        ? { path: snapshot, fileName: name }
        : await this.unpack(snapshot, kind(name), temporary, signal).catch((error: unknown) => {
            signal?.throwIfAborted();
            if (error instanceof ArtifactFileError) throw error;
            throw new ArtifactFileError('ARTIFACT_ARCHIVE_INVALID');
          });
    const binary = prepared.path;
    signal?.throwIfAborted();
    if (!(await elf(binary))) fail('ARTIFACT_BINARY_INVALID');
    const hash = createHash('sha256');
    for await (const chunk of createReadStream(binary, { signal })) hash.update(chunk);
    signal?.throwIfAborted();
    return await work({
      path: binary,
      sha256: hash.digest('hex'),
      sizeBytes: (await lstat(binary)).size,
      fileName: prepared.fileName,
      suggestedVersion: name.replace(/\.tar\.(gz|xz)$/u, '').slice(0, 128),
    });
  }
  private async unpack(snapshot: string, format: string, temporary: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    // tar-stream caps PAX/GNU extended headers at 4 MiB before buffering them.
    const extract = tar.extract();
    const seen = new Set<string>();
    let count = 0;
    let candidates = 0;
    let hasCli = false;
    let fileName = '';
    const binary = path.join(temporary, 'ape-dts');
    extract.on('entry', (header, stream, next) => {
      void (async () => {
        if (++count > 10000) fail('ARTIFACT_ARCHIVE_LIMIT');
        const normalized = header.name.replace(/^(\.\/)+/u, '');
        if (header.type === 'directory' && (normalized === '' || normalized === '.')) {
          stream.resume();
          next();
          return;
        }
        const name = header.type === 'directory' ? normalized.replace(/\/$/u, '') : normalized;
        const parts = segments(name);
        if (seen.has(name)) fail('ARTIFACT_ARCHIVE_INVALID');
        seen.add(name);
        if (header.type !== 'file' && header.type !== 'directory') fail('ARTIFACT_ARCHIVE_INVALID');
        if (header.type === 'file' && parts.at(-1) === 'dtscli') hasCli = true;
        const isCandidate =
          header.type === 'file' && ['ape-dts', 'ape_dts', 'dt-main'].includes(parts.at(-1)!);
        if (isCandidate) {
          const target = path.join(temporary, `candidate-${count}`);
          await pipeline(
            stream,
            limiter(MAX_FILE),
            createWriteStream(target, { flags: 'wx', mode: 0o600 }),
            { signal },
          );
          if (await elf(target)) {
            if (++candidates > 1) fail('ARTIFACT_ARCHIVE_AMBIGUOUS');
            fileName = name;
            await copyFile(target, binary, constants.COPYFILE_EXCL);
          }
          await rm(target);
        } else {
          for await (const _chunk of stream) {
            void _chunk;
            /* Drain without extracting unrelated members. */
          }
        }
        next();
      })().catch((error: Error) => extract.destroy(error));
    });
    if (format === 'TAR_GZ') {
      const timer = setTimeout(
        () => extract.destroy(new ArtifactFileError('ARTIFACT_ARCHIVE_TIMEOUT')),
        60000,
      );
      try {
        await pipeline(createReadStream(snapshot), createGunzip(), limiter(MAX_EXPANDED), extract, {
          signal,
        });
      } finally {
        clearTimeout(timer);
      }
    } else {
      const child = spawn(
        'xz',
        ['--decompress', '--stdout', '--memlimit-decompress=256MiB', '--', snapshot],
        { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], shell: false },
      );
      child.stderr.resume();
      const completion = new Promise<void>((resolve, reject) => {
        child.once('error', () => reject(new ArtifactFileError('ARTIFACT_XZ_UNAVAILABLE')));
        child.once('close', (code) =>
          code === 0 ? resolve() : reject(new ArtifactFileError('ARTIFACT_ARCHIVE_INVALID')),
        );
      });
      const timer = setTimeout(() => {
        child.kill();
        extract.destroy(new ArtifactFileError('ARTIFACT_ARCHIVE_TIMEOUT'));
      }, 60000);
      try {
        await Promise.all([
          completion,
          pipeline(child.stdout, limiter(MAX_EXPANDED), extract, { signal }),
        ]);
      } finally {
        clearTimeout(timer);
        child.kill();
        await completion.catch(() => undefined);
      }
    }
    if (candidates !== 1)
      fail(hasCli ? 'ARTIFACT_ARCHIVE_CLI_ONLY' : 'ARTIFACT_ARCHIVE_BINARY_MISSING');
    return { path: binary, fileName };
  }
  async publish(prefix: string, binary: PreparedArtifactFile) {
    const parts = [...segments(prefix, true), 'artifacts', 'imported', randomUUID()];
    const directory = await this.directory(parts);
    const target = path.join(directory, 'ape-dts');
    try {
      await copyFile(binary.path, target, constants.COPYFILE_EXCL);
    } catch (error) {
      await rm(directory, { recursive: true, force: true });
      throw error;
    }
    return {
      objectKey: [...parts.slice(segments(prefix, true).length), 'ape-dts'].join('/'),
      remove: async () => {
        await rm(directory, { recursive: true, force: true });
      },
    };
  }
}
