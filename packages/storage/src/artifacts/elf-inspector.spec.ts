import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { ElfArtifactInspector } from './elf-inspector.js';

const cleanupPaths: string[] = [];

async function writeTemporaryFile(contents: Buffer): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'scs-elf-inspector-'));
  cleanupPaths.push(directory);
  const file = path.join(directory, 'artifact');
  await writeFile(file, contents);
  return file;
}

function writeProgramHeader(
  buffer: Buffer,
  index: number,
  values: { fileSize: number; offset: number; type: number; virtualAddress: number },
): void {
  const start = 64 + index * 56;
  buffer.writeUInt32LE(values.type, start);
  buffer.writeUInt32LE(4, start + 4);
  buffer.writeBigUInt64LE(BigInt(values.offset), start + 8);
  buffer.writeBigUInt64LE(BigInt(values.virtualAddress), start + 16);
  buffer.writeBigUInt64LE(BigInt(values.virtualAddress), start + 24);
  buffer.writeBigUInt64LE(BigInt(values.fileSize), start + 32);
  buffer.writeBigUInt64LE(BigInt(values.fileSize), start + 40);
  buffer.writeBigUInt64LE(8n, start + 48);
}

function dynamicElf(machine = 62): Buffer {
  const buffer = Buffer.alloc(512);
  buffer.set([0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, 0], 0);
  buffer.writeUInt16LE(3, 16);
  buffer.writeUInt16LE(machine, 18);
  buffer.writeUInt32LE(1, 20);
  buffer.writeBigUInt64LE(64n, 32);
  buffer.writeUInt16LE(64, 52);
  buffer.writeUInt16LE(56, 54);
  buffer.writeUInt16LE(2, 56);
  writeProgramHeader(buffer, 0, {
    fileSize: buffer.length,
    offset: 0,
    type: 1,
    virtualAddress: 0x40_0000,
  });
  writeProgramHeader(buffer, 1, {
    fileSize: 80,
    offset: 256,
    type: 2,
    virtualAddress: 0x40_0100,
  });
  const dynamicEntries: Array<[bigint, bigint]> = [
    [5n, 0x40_0180n],
    [10n, 23n],
    [1n, 1n],
    [1n, 11n],
    [0n, 0n],
  ];
  dynamicEntries.forEach(([tag, value], index) => {
    buffer.writeBigInt64LE(tag, 256 + index * 16);
    buffer.writeBigUInt64LE(value, 264 + index * 16);
  });
  buffer.write('\0libc.so.6\0libssl.so.3\0', 384, 'utf8');
  return buffer;
}

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map(async (directory) => {
      await rm(directory, { force: true, recursive: true });
    }),
  );
});

describe('ElfArtifactInspector', () => {
  it('reports the ELF architecture and dynamic dependencies without executing the file', async () => {
    const inspection = await new ElfArtifactInspector().inspect(
      await writeTemporaryFile(dynamicElf()),
      {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      },
    );

    expect(inspection).toMatchObject({
      architecture: 'x86_64',
      dynamicLibraries: ['libc.so.6', 'libssl.so.3'],
      elfClass: 64,
      format: 'ELF',
    });
  });

  it('rejects a wrong architecture and a dependency outside an explicit allowlist', async () => {
    const inspector = new ElfArtifactInspector();
    await expect(
      inspector.inspect(await writeTemporaryFile(dynamicElf(183)), {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      }),
    ).rejects.toMatchObject({ code: 'ELF_ARCHITECTURE_MISMATCH' });

    await expect(
      inspector.inspect(await writeTemporaryFile(dynamicElf()), {
        allowedDynamicLibraries: { allowedNames: ['libc.so.6'], mode: 'ALLOWLIST' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      }),
    ).rejects.toMatchObject({ code: 'ELF_DYNAMIC_LIBRARY_NOT_ALLOWED' });
  });

  it('rejects a non-ELF object', async () => {
    await expect(
      new ElfArtifactInspector().inspect(await writeTemporaryFile(Buffer.from('not-an-elf')), {
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
        expectedArchitecture: 'x86_64',
      }),
    ).rejects.toMatchObject({ code: 'ARTIFACT_NOT_ELF' });
  });
});
