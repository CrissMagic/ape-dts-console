import type {
  MigrationBundle,
  MigrationStore,
  MigrationTransaction,
  SchemaState,
} from './types.js';

export class MigrationLockUnavailableError extends Error {
  readonly code = 'MIGRATION_LOCK_UNAVAILABLE';

  constructor() {
    super('MIGRATION_LOCK_UNAVAILABLE');
    this.name = 'MigrationLockUnavailableError';
  }
}

export class SchemaNotMigratableError extends Error {
  readonly code = 'SCHEMA_NOT_MIGRATABLE';

  constructor(
    readonly actualState: SchemaState,
    readonly expectedState: SchemaState,
  ) {
    super(`SCHEMA_NOT_MIGRATABLE:${actualState}:EXPECTED:${expectedState}`);
    this.name = 'SchemaNotMigratableError';
  }
}

export interface MigrationOptions {
  readonly afterMigrations?: (transaction: MigrationTransaction) => Promise<void> | void;
  readonly expectedState: Extract<SchemaState, 'ABSENT' | 'OLDER'>;
}

export interface MigrationResult {
  readonly appliedVersions: readonly number[];
  readonly previousState: Extract<SchemaState, 'ABSENT' | 'OLDER'>;
}

export class MigrationRunner {
  constructor(private readonly store: MigrationStore) {}

  async migrate(
    bundle: MigrationBundle,
    instanceId: string,
    options: MigrationOptions,
  ): Promise<MigrationResult> {
    return await this.store.transaction(async (transaction) => {
      if (!(await transaction.tryAcquireMigrationLock())) {
        throw new MigrationLockUnavailableError();
      }

      const classification = await transaction.inspectSchema(bundle.manifest);
      if (classification.state !== options.expectedState) {
        throw new SchemaNotMigratableError(classification.state, options.expectedState);
      }

      if (classification.state === 'ABSENT') {
        await transaction.createPlatformSchema(bundle.manifest.schema);
      }

      const pending = new Set(classification.pendingVersions);
      const appliedVersions: number[] = [];
      for (const migration of bundle.migrations) {
        if (!pending.has(migration.version)) {
          continue;
        }
        await transaction.executeMigration(migration);
        await transaction.recordMigration(migration, instanceId);
        appliedVersions.push(migration.version);
      }

      await options.afterMigrations?.(transaction);
      return { appliedVersions, previousState: classification.state };
    });
  }
}
