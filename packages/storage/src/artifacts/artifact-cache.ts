import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import { chmod, link, mkdir, open, rm } from 'node:fs/promises';
import path from 'node:path';

import type { ObjectStorageDriver } from '../drivers/types.js';
import type {
  ArtifactExecutionBinding,
  ArtifactSignaturePolicy,
  ArtifactSignatureResult,
  StaticArtifactInspection,
  StaticArtifactInspector,
  StaticInspectionPolicy,
} from './types.js';

const SHA256_PATTERN = /^[a-f0-9]{64}$/u;

export class ArtifactIntegrityError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'ArtifactIntegrityError';
  }
}

class OpenArtifactBinding implements ArtifactExecutionBinding {
  readonly descriptor: number;
  private closed = false;

  constructor(
    private readonly handle: FileHandle,
    readonly path: string,
    readonly sha256: string,
    readonly size: number,
  ) {
    this.descriptor = handle.fd;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.handle.close();
  }
}

async function sha256FileHandle(handle: FileHandle): Promise<string> {
  const digest = createHash('sha256');
  const buffer = Buffer.allocUnsafe(64 * 1024);
  let position = 0;
  while (true) {
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, position);
    if (bytesRead === 0) {
      break;
    }
    digest.update(buffer.subarray(0, bytesRead));
    position += bytesRead;
  }
  return digest.digest('hex');
}

function validateExpectedObject(sha256: string, size: number): void {
  if (!SHA256_PATTERN.test(sha256)) {
    throw new ArtifactIntegrityError('ARTIFACT_SHA256_INVALID');
  }
  if (!Number.isSafeInteger(size) || size <= 0) {
    throw new ArtifactIntegrityError('ARTIFACT_SIZE_INVALID');
  }
}

export class ArtifactCache {
  readonly root: string;
  private readonly objectsDirectory: string;
  private readonly stagingDirectory: string;

  constructor(root: string) {
    if (root.trim().length === 0) {
      throw new ArtifactIntegrityError('ARTIFACT_CACHE_ROOT_INVALID');
    }
    this.root = path.resolve(root);
    this.objectsDirectory = path.join(this.root, 'objects');
    this.stagingDirectory = path.join(this.root, '.staging');
  }

  async ensureDirectories(): Promise<void> {
    await mkdir(this.objectsDirectory, { mode: 0o700, recursive: true });
    await mkdir(this.stagingDirectory, { mode: 0o700, recursive: true });
  }

  async createStagingPath(): Promise<string> {
    await this.ensureDirectories();
    return path.join(this.stagingDirectory, randomUUID());
  }

  objectPath(sha256: string): string {
    if (!SHA256_PATTERN.test(sha256)) {
      throw new ArtifactIntegrityError('ARTIFACT_SHA256_INVALID');
    }
    return path.join(this.objectsDirectory, sha256);
  }

  async publish(stagingPath: string, sha256: string): Promise<string> {
    await this.ensureDirectories();
    const finalPath = this.objectPath(sha256);
    await chmod(stagingPath, 0o500);
    try {
      await link(stagingPath, finalPath);
    } catch (error) {
      if (!isNodeError(error) || error.code !== 'EEXIST') {
        throw error;
      }
    } finally {
      await rm(stagingPath, { force: true });
    }
    return finalPath;
  }

  async bind(sha256: string, expectedSize: number): Promise<ArtifactExecutionBinding> {
    validateExpectedObject(sha256, expectedSize);
    await this.ensureDirectories();
    const objectPath = this.objectPath(sha256);
    const noFollow = typeof constants.O_NOFOLLOW === 'number' ? constants.O_NOFOLLOW : 0;
    const handle = await open(objectPath, constants.O_RDONLY | noFollow);
    try {
      const before = await handle.stat();
      if (!before.isFile()) {
        throw new ArtifactIntegrityError('CACHE_OBJECT_NOT_REGULAR_FILE');
      }
      if (before.size !== expectedSize) {
        throw new ArtifactIntegrityError('CACHE_OBJECT_SIZE_MISMATCH');
      }
      if (process.platform !== 'win32' && (before.mode & 0o222) !== 0) {
        throw new ArtifactIntegrityError('CACHE_OBJECT_WRITABLE');
      }
      const actualSha256 = await sha256FileHandle(handle);
      const after = await handle.stat();
      if (
        before.dev !== after.dev ||
        before.ino !== after.ino ||
        before.size !== after.size ||
        before.mtimeMs !== after.mtimeMs
      ) {
        throw new ArtifactIntegrityError('CACHE_OBJECT_CHANGED_DURING_VERIFICATION');
      }
      if (actualSha256 !== sha256) {
        throw new ArtifactIntegrityError('CACHE_OBJECT_SHA256_MISMATCH');
      }
      return new OpenArtifactBinding(handle, objectPath, sha256, expectedSize);
    } catch (error) {
      await handle.close();
      throw error;
    }
  }
}

function isNodeError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && 'code' in error;
}

async function verifySignature(
  policy: ArtifactSignaturePolicy,
  sha256: string,
): Promise<ArtifactSignatureResult> {
  if (policy.mode === 'DISABLED') {
    return { mode: 'DISABLED', signerId: null, verified: false };
  }
  if (!policy.allowedSignerIds.includes(policy.signerId)) {
    throw new ArtifactIntegrityError('ARTIFACT_SIGNER_NOT_ALLOWED');
  }
  if (
    !(await policy.verifier.verifyDigest({
      algorithm: policy.algorithm,
      sha256,
      signature: policy.signature,
      signerId: policy.signerId,
    }))
  ) {
    throw new ArtifactIntegrityError('ARTIFACT_SIGNATURE_INVALID');
  }
  return { mode: 'REQUIRED', signerId: policy.signerId, verified: true };
}

export interface ArtifactLoadRequest {
  readonly expectedSha256: string;
  readonly expectedSize: number;
  readonly objectKey: string;
  readonly signaturePolicy: ArtifactSignaturePolicy;
  readonly staticPolicy: StaticInspectionPolicy;
}

export interface LoadedArtifact {
  readonly binding: ArtifactExecutionBinding;
  readonly signature: ArtifactSignatureResult;
  readonly staticInspection: StaticArtifactInspection;
}

export class ArtifactLoader {
  constructor(
    private readonly cache: ArtifactCache,
    private readonly inspector: StaticArtifactInspector,
  ) {}

  async load(source: ObjectStorageDriver, request: ArtifactLoadRequest): Promise<LoadedArtifact> {
    validateExpectedObject(request.expectedSha256, request.expectedSize);
    const metadata = await source.headObject(request.objectKey);
    if (metadata.size !== request.expectedSize) {
      throw new ArtifactIntegrityError('ARTIFACT_SOURCE_SIZE_MISMATCH');
    }

    const stagingPath = await this.cache.createStagingPath();
    let stagingExists = false;
    try {
      const output = await open(stagingPath, 'wx', 0o600);
      stagingExists = true;
      const digest = createHash('sha256');
      let size = 0;
      try {
        const input = await source.openObject(request.objectKey);
        for await (const rawChunk of input) {
          const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk as Uint8Array);
          size += chunk.length;
          if (size > request.expectedSize) {
            throw new ArtifactIntegrityError('ARTIFACT_SOURCE_SIZE_MISMATCH');
          }
          digest.update(chunk);
          await output.write(chunk);
        }
        await output.sync();
      } finally {
        await output.close();
      }

      if (size !== request.expectedSize) {
        throw new ArtifactIntegrityError('ARTIFACT_SOURCE_SIZE_MISMATCH');
      }
      const actualSha256 = digest.digest('hex');
      if (actualSha256 !== request.expectedSha256) {
        throw new ArtifactIntegrityError('ARTIFACT_SHA256_MISMATCH');
      }

      const staticInspection = await this.inspector.inspect(stagingPath, request.staticPolicy);
      const signature = await verifySignature(request.signaturePolicy, actualSha256);
      await this.cache.publish(stagingPath, actualSha256);
      stagingExists = false;
      const binding = await this.cache.bind(actualSha256, size);
      return { binding, signature, staticInspection };
    } catch (error) {
      if (stagingExists) {
        await rm(stagingPath, { force: true });
      }
      throw error;
    }
  }
}

export type {
  ArtifactExecutionBinding,
  ArtifactSignaturePolicy,
  ArtifactSignatureResult,
  StaticArtifactInspection,
  StaticArtifactInspector,
  StaticInspectionPolicy,
} from './types.js';
