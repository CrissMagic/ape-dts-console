import { createHash } from 'node:crypto';
import { chmod, mkdtemp, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';

import { afterEach, describe, expect, it } from 'vitest';

import type { ObjectStorageDriver } from '../drivers/types.js';
import { ArtifactCache, ArtifactLoader, type StaticArtifactInspector } from './artifact-cache.js';

const cleanupPaths: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'scs-artifact-cache-'));
  cleanupPaths.push(directory);
  return directory;
}

function sha256(contents: Buffer): string {
  return createHash('sha256').update(contents).digest('hex');
}

function source(contents: Buffer): ObjectStorageDriver {
  return {
    driver: 'OSS',
    headObject: async () => ({ size: contents.length }),
    openObject: async () => Readable.from([contents]),
  };
}

const inspector: StaticArtifactInspector = {
  inspect: async () => ({
    architecture: 'x86_64',
    dynamicLibraries: [],
    elfClass: 64,
    format: 'ELF',
    interpreter: null,
  }),
};

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map(async (directory) => {
      await rm(directory, { force: true, recursive: true });
    }),
  );
});

describe('ArtifactLoader and ArtifactCache', () => {
  it('publishes by hash from an isolated staging file and never reuses the OSS key as a filename', async () => {
    const contents = Buffer.from('candidate-binary');
    const digest = sha256(contents);
    const cache = new ArtifactCache(await temporaryDirectory());
    const loader = new ArtifactLoader(cache, inspector);

    const loaded = await loader.load(source(contents), {
      expectedSha256: digest,
      expectedSize: contents.length,
      objectKey: 'customer/../../user-controlled-name',
      signaturePolicy: { mode: 'DISABLED' },
      staticPolicy: {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      },
    });

    expect(path.basename(loaded.binding.path)).toBe(digest);
    expect(loaded.binding.path).not.toContain('user-controlled-name');
    expect(await readFile(loaded.binding.path)).toEqual(contents);
    expect((await stat(loaded.binding.path)).mode & 0o222).toBe(0);
    await loaded.binding.close();
  });

  it('allows concurrent publication of identical content without overwriting an existing object', async () => {
    const contents = Buffer.from('same-binary');
    const digest = sha256(contents);
    const cache = new ArtifactCache(await temporaryDirectory());
    const loader = new ArtifactLoader(cache, inspector);
    const request = {
      expectedSha256: digest,
      expectedSize: contents.length,
      objectKey: 'one/object',
      signaturePolicy: { mode: 'DISABLED' } as const,
      staticPolicy: {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' } as const,
        allowedInterpreters: { mode: 'REPORT_ONLY' } as const,
        expectedArchitecture: 'x86_64' as const,
      },
    };

    const [first, second] = await Promise.all([
      loader.load(source(contents), request),
      loader.load(source(contents), request),
    ]);

    expect(first.binding.path).toBe(second.binding.path);
    expect(await readFile(first.binding.path)).toEqual(contents);
    await first.binding.close();
    await second.binding.close();
  });

  it('removes failed staging content and never publishes a size or hash mismatch', async () => {
    const cacheRoot = await temporaryDirectory();
    const cache = new ArtifactCache(cacheRoot);
    const loader = new ArtifactLoader(cache, inspector);
    const contents = Buffer.from('changed');

    await expect(
      loader.load(source(contents), {
        expectedSha256: '0'.repeat(64),
        expectedSize: contents.length,
        objectKey: 'artifact',
        signaturePolicy: { mode: 'DISABLED' },
        staticPolicy: {
          allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
          allowedInterpreters: { mode: 'REPORT_ONLY' },
          expectedArchitecture: 'x86_64',
        },
      }),
    ).rejects.toMatchObject({ code: 'ARTIFACT_SHA256_MISMATCH' });

    expect(await readdir(path.join(cacheRoot, '.staging'))).toEqual([]);
    expect(await readdir(path.join(cacheRoot, 'objects'))).toEqual([]);
  });

  it('revalidates size and hash when binding the exact cache object for execution', async () => {
    const contents = Buffer.from('verified');
    const digest = sha256(contents);
    const cache = new ArtifactCache(await temporaryDirectory());
    const loaded = await new ArtifactLoader(cache, inspector).load(source(contents), {
      expectedSha256: digest,
      expectedSize: contents.length,
      objectKey: 'artifact',
      signaturePolicy: { mode: 'DISABLED' },
      staticPolicy: {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      },
    });
    const cachePath = loaded.binding.path;
    await loaded.binding.close();
    await chmod(cachePath, 0o700);
    await writeFile(cachePath, 'tampered');
    await chmod(cachePath, 0o500);

    await expect(cache.bind(digest, contents.length)).rejects.toMatchObject({
      code: 'CACHE_OBJECT_SHA256_MISMATCH',
    });
  });
});
