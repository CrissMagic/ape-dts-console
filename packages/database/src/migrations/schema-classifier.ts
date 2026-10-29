import type {
  MigrationManifest,
  MigrationManifestEntry,
  SchemaClassification,
  SchemaSnapshot,
} from './types.js';

function corrupt(
  targetVersion: number,
  appliedVersion: number | null,
  issues: readonly string[],
): SchemaClassification {
  return { appliedVersion, issues, pendingVersions: [], state: 'CORRUPT', targetVersion };
}

function validateManifest(manifest: MigrationManifest): void {
  if (manifest.migrations.length === 0) {
    throw new Error('migration manifest must not be empty');
  }
  let previousVersion = 0;
  for (const migration of manifest.migrations) {
    if (!Number.isSafeInteger(migration.version) || migration.version <= previousVersion) {
      throw new Error('migration manifest versions must be strictly increasing positive integers');
    }
    previousVersion = migration.version;
  }
}

function sentinelIssues(
  migration: MigrationManifestEntry,
  columnsByTable: Readonly<Record<string, readonly string[]>>,
  constraintsByTable: Readonly<Record<string, readonly string[]>>,
  indexesByTable: Readonly<Record<string, readonly string[]>>,
): string[] {
  const issues: string[] = [];
  for (const sentinel of migration.sentinels) {
    const actualColumns = columnsByTable[sentinel.table];
    if (actualColumns === undefined) {
      issues.push(`MISSING_TABLE:${sentinel.table}`);
      continue;
    }
    const actualColumnSet = new Set(actualColumns);
    for (const expectedColumn of sentinel.columns) {
      if (!actualColumnSet.has(expectedColumn)) {
        issues.push(`MISSING_COLUMN:${sentinel.table}.${expectedColumn}`);
      }
    }
    if (sentinel.constraints !== undefined) {
      const actualConstraints = new Set(constraintsByTable[sentinel.table] ?? []);
      for (const expectedConstraint of sentinel.constraints) {
        if (!actualConstraints.has(expectedConstraint)) {
          issues.push(`MISSING_CONSTRAINT:${sentinel.table}.${expectedConstraint}`);
        }
      }
    }
    if (sentinel.indexes !== undefined) {
      const actualIndexes = new Set(indexesByTable[sentinel.table] ?? []);
      for (const expectedIndex of sentinel.indexes) {
        if (!actualIndexes.has(expectedIndex)) {
          issues.push(`MISSING_INDEX:${sentinel.table}.${expectedIndex}`);
        }
      }
    }
  }
  return issues;
}

export function classifySchemaSnapshot(
  snapshot: SchemaSnapshot,
  manifest: MigrationManifest,
): SchemaClassification {
  validateManifest(manifest);
  const targetVersion = manifest.migrations.at(-1)?.version ?? 0;

  if (!snapshot.schemaExists) {
    return {
      appliedVersion: null,
      issues: [],
      pendingVersions: manifest.migrations.map(({ version }) => version),
      state: 'ABSENT',
      targetVersion,
    };
  }
  if (!snapshot.migrationsTableExists) {
    return corrupt(targetVersion, null, ['MISSING_MIGRATIONS_TABLE']);
  }

  const applied = [...snapshot.appliedMigrations].sort(
    (left, right) => left.version - right.version,
  );
  if (applied.length === 0) {
    return corrupt(targetVersion, null, ['EMPTY_MIGRATION_LEDGER']);
  }
  const appliedVersion = applied.at(-1)?.version ?? null;
  if (applied.some(({ version }) => version > targetVersion)) {
    return {
      appliedVersion,
      issues: [],
      pendingVersions: [],
      state: 'NEWER',
      targetVersion,
    };
  }

  const issues: string[] = [];
  for (let index = 0; index < applied.length; index += 1) {
    const actual = applied[index];
    const expected = manifest.migrations[index];
    if (actual === undefined || expected === undefined || actual.version !== expected.version) {
      issues.push(`NON_PREFIX_MIGRATION_LEDGER:${actual?.version ?? 'unknown'}`);
      continue;
    }
    if (actual.checksum !== expected.checksum) {
      issues.push(`CHECKSUM_MISMATCH:${actual.version}`);
    }
    if (actual.platformVersion !== expected.platformVersion) {
      issues.push(`PLATFORM_VERSION_MISMATCH:${actual.version}`);
    }
    issues.push(
      ...sentinelIssues(
        expected,
        snapshot.columnsByTable,
        snapshot.constraintsByTable ?? {},
        snapshot.indexesByTable ?? {},
      ),
    );
  }
  if (issues.length > 0) {
    return corrupt(targetVersion, appliedVersion, [...new Set(issues)]);
  }

  const pendingVersions = manifest.migrations.slice(applied.length).map(({ version }) => version);
  return {
    appliedVersion,
    issues: [],
    pendingVersions,
    state: pendingVersions.length === 0 ? 'CURRENT' : 'OLDER',
    targetVersion,
  };
}
