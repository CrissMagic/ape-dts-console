import type { FileHandle } from 'node:fs/promises';
import { open } from 'node:fs/promises';

import type {
  ArtifactArchitecture,
  NamePolicy,
  StaticArtifactInspection,
  StaticArtifactInspector,
  StaticInspectionPolicy,
} from './types.js';

const ELF_HEADER_SIZE = 64;
const PROGRAM_HEADER_SIZE = 56;
const PT_LOAD = 1;
const PT_DYNAMIC = 2;
const PT_INTERP = 3;
const DT_NULL = 0n;
const DT_NEEDED = 1n;
const DT_STRTAB = 5n;
const DT_STRSZ = 10n;

export class ElfInspectionError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = 'ElfInspectionError';
  }
}

interface ProgramHeader {
  readonly fileSize: number;
  readonly offset: number;
  readonly type: number;
  readonly virtualAddress: number;
}

function safeNumber(value: bigint, code = 'ELF_OFFSET_INVALID'): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new ElfInspectionError(code);
  }
  return Number(value);
}

function ensureRange(offset: number, length: number, fileSize: number): void {
  if (
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(length) ||
    offset < 0 ||
    length < 0 ||
    offset > fileSize ||
    length > fileSize - offset
  ) {
    throw new ElfInspectionError('ELF_RANGE_INVALID');
  }
}

async function readRange(
  handle: FileHandle,
  offset: number,
  length: number,
  fileSize: number,
): Promise<Buffer> {
  ensureRange(offset, length, fileSize);
  const buffer = Buffer.alloc(length);
  const { bytesRead } = await handle.read(buffer, 0, length, offset);
  if (bytesRead !== length) {
    throw new ElfInspectionError('ELF_TRUNCATED');
  }
  return buffer;
}

function architectureFor(machine: number): ArtifactArchitecture {
  if (machine === 62) {
    return 'x86_64';
  }
  if (machine === 183) {
    return 'aarch64';
  }
  throw new ElfInspectionError('ELF_ARCHITECTURE_UNSUPPORTED');
}

function enforceNamePolicy(value: string, policy: NamePolicy, code: string): void {
  if (policy.mode === 'ALLOWLIST' && !policy.allowedNames.includes(value)) {
    throw new ElfInspectionError(code);
  }
}

function virtualAddressToOffset(
  virtualAddress: number,
  length: number,
  programHeaders: readonly ProgramHeader[],
): number {
  const loadSegment = programHeaders.find(
    (header) =>
      header.type === PT_LOAD &&
      virtualAddress >= header.virtualAddress &&
      virtualAddress - header.virtualAddress <= header.fileSize - length,
  );
  if (loadSegment === undefined) {
    throw new ElfInspectionError('ELF_VIRTUAL_ADDRESS_UNMAPPED');
  }
  return loadSegment.offset + (virtualAddress - loadSegment.virtualAddress);
}

function readNullTerminatedString(table: Buffer, offset: number): string {
  if (!Number.isSafeInteger(offset) || offset < 0 || offset >= table.length) {
    throw new ElfInspectionError('ELF_STRING_OFFSET_INVALID');
  }
  const end = table.indexOf(0, offset);
  if (end < 0) {
    throw new ElfInspectionError('ELF_STRING_UNTERMINATED');
  }
  const value = table.toString('utf8', offset, end);
  if (value.length === 0 || hasControlCharacter(value)) {
    throw new ElfInspectionError('ELF_STRING_INVALID');
  }
  return value;
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 0x1f || code === 0x7f;
  });
}

async function readProgramHeaders(
  handle: FileHandle,
  header: Buffer,
  fileSize: number,
): Promise<readonly ProgramHeader[]> {
  const tableOffset = safeNumber(header.readBigUInt64LE(32));
  const entrySize = header.readUInt16LE(54);
  const entryCount = header.readUInt16LE(56);
  if (entrySize < PROGRAM_HEADER_SIZE || entryCount > 4096) {
    throw new ElfInspectionError('ELF_PROGRAM_HEADER_INVALID');
  }
  ensureRange(tableOffset, entrySize * entryCount, fileSize);
  const programHeaders: ProgramHeader[] = [];
  for (let index = 0; index < entryCount; index += 1) {
    const entry = await readRange(
      handle,
      tableOffset + index * entrySize,
      PROGRAM_HEADER_SIZE,
      fileSize,
    );
    const programHeader = {
      fileSize: safeNumber(entry.readBigUInt64LE(32)),
      offset: safeNumber(entry.readBigUInt64LE(8)),
      type: entry.readUInt32LE(0),
      virtualAddress: safeNumber(entry.readBigUInt64LE(16)),
    };
    ensureRange(programHeader.offset, programHeader.fileSize, fileSize);
    programHeaders.push(programHeader);
  }
  return programHeaders;
}

async function readInterpreter(
  handle: FileHandle,
  programHeaders: readonly ProgramHeader[],
  fileSize: number,
): Promise<string | null> {
  const interpreterHeader = programHeaders.find((header) => header.type === PT_INTERP);
  if (interpreterHeader === undefined) {
    return null;
  }
  const contents = await readRange(
    handle,
    interpreterHeader.offset,
    interpreterHeader.fileSize,
    fileSize,
  );
  const nullIndex = contents.indexOf(0);
  if (nullIndex < 1) {
    throw new ElfInspectionError('ELF_INTERPRETER_INVALID');
  }
  return contents.toString('utf8', 0, nullIndex);
}

async function readDynamicLibraries(
  handle: FileHandle,
  programHeaders: readonly ProgramHeader[],
  fileSize: number,
): Promise<readonly string[]> {
  const dynamicHeader = programHeaders.find((header) => header.type === PT_DYNAMIC);
  if (dynamicHeader === undefined) {
    return [];
  }
  if (dynamicHeader.fileSize % 16 !== 0 || dynamicHeader.fileSize / 16 > 65_536) {
    throw new ElfInspectionError('ELF_DYNAMIC_SECTION_INVALID');
  }
  const dynamic = await readRange(handle, dynamicHeader.offset, dynamicHeader.fileSize, fileSize);
  const neededOffsets: number[] = [];
  let stringTableVirtualAddress: number | undefined;
  let stringTableSize: number | undefined;
  for (let offset = 0; offset < dynamic.length; offset += 16) {
    const tag = dynamic.readBigInt64LE(offset);
    const value = dynamic.readBigUInt64LE(offset + 8);
    if (tag === DT_NULL) {
      break;
    }
    if (tag === DT_NEEDED) {
      neededOffsets.push(safeNumber(value, 'ELF_STRING_OFFSET_INVALID'));
    } else if (tag === DT_STRTAB) {
      stringTableVirtualAddress = safeNumber(value);
    } else if (tag === DT_STRSZ) {
      stringTableSize = safeNumber(value, 'ELF_STRING_TABLE_SIZE_INVALID');
    }
  }
  if (neededOffsets.length === 0) {
    return [];
  }
  if (
    stringTableVirtualAddress === undefined ||
    stringTableSize === undefined ||
    stringTableSize === 0
  ) {
    throw new ElfInspectionError('ELF_STRING_TABLE_MISSING');
  }
  const stringTableOffset = virtualAddressToOffset(
    stringTableVirtualAddress,
    stringTableSize,
    programHeaders,
  );
  const stringTable = await readRange(handle, stringTableOffset, stringTableSize, fileSize);
  return [...new Set(neededOffsets.map((offset) => readNullTerminatedString(stringTable, offset)))];
}

export class ElfArtifactInspector implements StaticArtifactInspector {
  async inspect(path: string, policy: StaticInspectionPolicy): Promise<StaticArtifactInspection> {
    const handle = await open(path, 'r');
    try {
      const status = await handle.stat();
      if (!status.isFile() || status.size < ELF_HEADER_SIZE) {
        throw new ElfInspectionError('ARTIFACT_NOT_ELF');
      }
      const header = await readRange(handle, 0, ELF_HEADER_SIZE, status.size);
      if (!header.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
        throw new ElfInspectionError('ARTIFACT_NOT_ELF');
      }
      if (header[4] !== 2) {
        throw new ElfInspectionError('ELF_CLASS_UNSUPPORTED');
      }
      if (header[5] !== 1) {
        throw new ElfInspectionError('ELF_ENDIANNESS_UNSUPPORTED');
      }
      const type = header.readUInt16LE(16);
      if (type !== 2 && type !== 3) {
        throw new ElfInspectionError('ELF_TYPE_NOT_EXECUTABLE');
      }
      const architecture = architectureFor(header.readUInt16LE(18));
      if (architecture !== policy.expectedArchitecture) {
        throw new ElfInspectionError('ELF_ARCHITECTURE_MISMATCH');
      }
      const programHeaders = await readProgramHeaders(handle, header, status.size);
      const interpreter = await readInterpreter(handle, programHeaders, status.size);
      const dynamicLibraries = await readDynamicLibraries(handle, programHeaders, status.size);
      if (interpreter !== null) {
        enforceNamePolicy(interpreter, policy.allowedInterpreters, 'ELF_INTERPRETER_NOT_ALLOWED');
      }
      for (const library of dynamicLibraries) {
        enforceNamePolicy(
          library,
          policy.allowedDynamicLibraries,
          'ELF_DYNAMIC_LIBRARY_NOT_ALLOWED',
        );
      }
      return {
        architecture,
        dynamicLibraries,
        elfClass: 64,
        format: 'ELF',
        interpreter,
      };
    } finally {
      await handle.close();
    }
  }
}
