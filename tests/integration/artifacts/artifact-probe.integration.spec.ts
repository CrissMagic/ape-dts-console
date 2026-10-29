import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { copyFile, mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  ArtifactCache,
  ArtifactLoader,
  LocalObjectStorage,
  type StaticArtifactInspector,
} from '../../../packages/storage/src/index.js';
import { ProcessProbeRunner } from '../../../tools/artifact-probe/src/index.js';

let testRoot: string;

async function sha256(file: string): Promise<string> {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(file)) {
    digest.update(chunk);
  }
  return digest.digest('hex');
}

beforeAll(async () => {
  testRoot = await mkdtemp(path.join(tmpdir(), 'scs-artifact-integration-'));
});

afterAll(async () => {
  await rm(testRoot, { force: true, recursive: true });
});

describe('artifact load and controlled behavior probe', () => {
  it('executes the verified cache object after the mutable source object changes', async () => {
    const storageRoot = path.join(testRoot, 'storage');
    const sourceDirectory = path.join(storageRoot, 'artifacts');
    const cacheRoot = path.join(testRoot, 'cache');
    const runtimeDirectory = path.join(testRoot, 'runtime');
    await mkdir(sourceDirectory, { recursive: true });
    await mkdir(runtimeDirectory, { recursive: true });
    const sourceArtifact = path.join(sourceDirectory, 'controlled-runtime');
    await copyFile(process.execPath, sourceArtifact);
    const digest = await sha256(sourceArtifact);
    const sourceSize = (await stat(sourceArtifact)).size;
    const controlledInspector: StaticArtifactInspector = {
      inspect: async () => ({
        architecture: 'x86_64',
        dynamicLibraries: [],
        elfClass: 64,
        format: 'ELF',
        interpreter: null,
      }),
    };
    const loaded = await new ArtifactLoader(new ArtifactCache(cacheRoot), controlledInspector).load(
      new LocalObjectStorage(storageRoot),
      {
        expectedSha256: digest,
        expectedSize: sourceSize,
        objectKey: 'artifacts/controlled-runtime',
        signaturePolicy: { mode: 'DISABLED' },
        staticPolicy: {
          allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
          allowedInterpreters: { mode: 'REPORT_ONLY' },
          expectedArchitecture: 'x86_64',
        },
      },
    );
    await writeFile(sourceArtifact, 'source object changed after verified load');
    const script = path.join(runtimeDirectory, 'probe.cjs');
    await writeFile(script, "process.stdout.write('verified-cache-executed\\n');");

    try {
      const result = await new ProcessProbeRunner().run(loaded.binding, {
        allowPathFallbackForNonLinuxTests: process.platform !== 'linux',
        arguments: [script],
        environment: {},
        killGraceMilliseconds: 100,
        maxOutputBytesPerStream: 4_096,
        redact: (value) => value,
        timeoutMilliseconds: 2_000,
        workingDirectory: runtimeDirectory,
      });

      expect(result).toMatchObject({
        exitCode: 0,
        outcome: 'EXITED',
        stdout: 'verified-cache-executed\n',
      });
      expect(path.basename(loaded.binding.path)).toBe(digest);
      expect(loaded.binding.path).not.toBe(sourceArtifact);
    } finally {
      await loaded.binding.close();
    }
  });
});
