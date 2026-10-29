export type SchemaState = 'ABSENT' | 'CURRENT' | 'OLDER' | 'NEWER' | 'CORRUPT';

export interface MigrationSentinel {
  readonly columns: readonly string[];
  readonly constraints?: readonly string[];
  readonly indexes?: readonly string[];
  readonly table: string;
}

export interface MigrationManifestEntry {
  readonly checksum: string;
  readonly description: string;
  readonly filename: string;
  readonly platformVersion: string;
  readonly sentinels: readonly MigrationSentinel[];
  readonly version: number;
}

export interface MigrationManifest {
  readonly migrations: readonly MigrationManifestEntry[];
  readonly schema: string;
}

export interface MigrationDefinition extends MigrationManifestEntry {
  readonly sql: string;
}

export interface MigrationBundle {
  readonly manifest: MigrationManifest;
  readonly migrations: readonly MigrationDefinition[];
}

export interface AppliedMigration {
  readonly checksum: string;
  readonly platformVersion: string;
  readonly version: number;
}

export interface SchemaSnapshot {
  readonly appliedMigrations: readonly AppliedMigration[];
  readonly columnsByTable: Readonly<Record<string, readonly string[]>>;
  readonly constraintsByTable?: Readonly<Record<string, readonly string[]>>;
  readonly indexesByTable?: Readonly<Record<string, readonly string[]>>;
  readonly migrationsTableExists: boolean;
  readonly schemaExists: boolean;
}

export interface SchemaClassification {
  readonly appliedVersion: number | null;
  readonly issues: readonly string[];
  readonly pendingVersions: readonly number[];
  readonly state: SchemaState;
  readonly targetVersion: number;
}

export interface MigrationQueryResult {
  readonly rows: readonly Record<string, unknown>[];
}

export interface MigrationTransaction {
  createPlatformSchema(schema: string): Promise<void>;
  executeMigration(migration: MigrationDefinition): Promise<void>;
  inspectSchema(manifest: MigrationManifest): Promise<SchemaClassification>;
  query(sql: string, values?: readonly unknown[]): Promise<MigrationQueryResult>;
  recordMigration(migration: MigrationDefinition, instanceId: string): Promise<void>;
  tryAcquireMigrationLock(): Promise<boolean>;
}

export interface MigrationStore {
  transaction<T>(work: (transaction: MigrationTransaction) => Promise<T>): Promise<T>;
}
