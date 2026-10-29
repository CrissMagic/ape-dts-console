import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

import type {
  MigrationBundle,
  MigrationManifest,
  MigrationManifestEntry,
  MigrationSentinel,
} from './types.js';

const CHECKSUM_PATTERN = /^[a-f0-9]{64}$/u;
const FILENAME_PATTERN = /^\d{6}_[a-z0-9_]+\.sql$/u;
const IDENTIFIER_PATTERN = /^[a-z][a-z0-9_]*$/u;

export class MigrationManifestError extends Error {
  readonly code = 'INVALID_MIGRATION_MANIFEST';

  constructor(message: string) {
    super(message);
    this.name = 'MigrationManifestError';
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new MigrationManifestError(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function string(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new MigrationManifestError(`${label} must be a non-empty string`);
  }
  return value;
}

function identifiers(value: unknown, label: string): readonly string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || value.length === 0) {
    throw new MigrationManifestError(`${label} must be a non-empty array when configured`);
  }
  return value.map((item, index) => {
    const parsed = string(item, `${label}[${index}]`);
    if (!IDENTIFIER_PATTERN.test(parsed)) {
      throw new MigrationManifestError(`${label}[${index}] is not a safe identifier`);
    }
    return parsed;
  });
}

function parseSentinel(value: unknown, label: string): MigrationSentinel {
  const input = record(value, label);
  const table = string(input.table, `${label}.table`);
  if (!IDENTIFIER_PATTERN.test(table)) {
    throw new MigrationManifestError(`${label}.table is not a safe identifier`);
  }
  if (!Array.isArray(input.columns) || input.columns.length === 0) {
    throw new MigrationManifestError(`${label}.columns must be a non-empty array`);
  }
  const columns = input.columns.map((column, index) => {
    const parsed = string(column, `${label}.columns[${index}]`);
    if (!IDENTIFIER_PATTERN.test(parsed)) {
      throw new MigrationManifestError(`${label}.columns[${index}] is not a safe identifier`);
    }
    return parsed;
  });
  const constraints = identifiers(input.constraints, `${label}.constraints`);
  const indexes = identifiers(input.indexes, `${label}.indexes`);
  return {
    columns,
    ...(constraints === undefined ? {} : { constraints }),
    ...(indexes === undefined ? {} : { indexes }),
    table,
  };
}

function parseEntry(value: unknown, index: number): MigrationManifestEntry {
  const label = `migrations[${index}]`;
  const input = record(value, label);
  const version = input.version;
  if (!Number.isSafeInteger(version) || (version as number) <= 0) {
    throw new MigrationManifestError(`${label}.version must be a positive safe integer`);
  }
  const checksum = string(input.checksum, `${label}.checksum`);
  if (!CHECKSUM_PATTERN.test(checksum)) {
    throw new MigrationManifestError(`${label}.checksum must be lowercase SHA-256`);
  }
  const filename = string(input.filename, `${label}.filename`);
  if (!FILENAME_PATTERN.test(filename)) {
    throw new MigrationManifestError(`${label}.filename is invalid`);
  }
  if (!Array.isArray(input.sentinels) || input.sentinels.length === 0) {
    throw new MigrationManifestError(`${label}.sentinels must be a non-empty array`);
  }
  return {
    checksum,
    description: string(input.description, `${label}.description`),
    filename,
    platformVersion: string(input.platformVersion, `${label}.platformVersion`),
    sentinels: input.sentinels.map((sentinel, sentinelIndex) =>
      parseSentinel(sentinel, `${label}.sentinels[${sentinelIndex}]`),
    ),
    version: version as number,
  };
}

function parseManifest(value: unknown): MigrationManifest {
  const input = record(value, 'manifest');
  const schema = string(input.schema, 'manifest.schema');
  if (!IDENTIFIER_PATTERN.test(schema)) {
    throw new MigrationManifestError('manifest.schema is not a safe identifier');
  }
  if (!Array.isArray(input.migrations) || input.migrations.length === 0) {
    throw new MigrationManifestError('manifest.migrations must be a non-empty array');
  }
  const migrations = input.migrations.map(parseEntry);
  let previousVersion = 0;
  const filenames = new Set<string>();
  for (const migration of migrations) {
    if (migration.version <= previousVersion) {
      throw new MigrationManifestError('migration versions must be strictly increasing');
    }
    if (filenames.has(migration.filename)) {
      throw new MigrationManifestError(`duplicate migration filename: ${migration.filename}`);
    }
    previousVersion = migration.version;
    filenames.add(migration.filename);
  }
  return { migrations, schema };
}

export async function loadMigrationBundle(
  migrationsDirectory = new URL('../../migrations/', import.meta.url),
): Promise<MigrationBundle> {
  const manifestText = await readFile(new URL('manifest.json', migrationsDirectory), 'utf8');
  let parsed: unknown;
  try {
    parsed = JSON.parse(manifestText) as unknown;
  } catch {
    throw new MigrationManifestError('manifest.json is not valid JSON');
  }
  const manifest = parseManifest(parsed);
  const migrations = await Promise.all(
    manifest.migrations.map(async (entry) => {
      const sql = await readFile(new URL(entry.filename, migrationsDirectory), 'utf8');
      const checksum = createHash('sha256').update(sql, 'utf8').digest('hex');
      if (checksum !== entry.checksum) {
        throw new MigrationManifestError(`checksum mismatch for ${entry.filename}`);
      }
      return { ...entry, sql };
    }),
  );
  return { manifest, migrations };
}
