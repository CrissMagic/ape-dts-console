import { describe, expect, it } from 'vitest';

import {
  classifySchemaSnapshot,
  MigrationLockUnavailableError,
  MigrationRunner,
  SchemaNotMigratableError,
  type AppliedMigration,
  type MigrationBundle,
  type MigrationStore,
  type MigrationTransaction,
  type SchemaSnapshot,
} from '../index.js';

const bundle: MigrationBundle = {
  manifest: {
    migrations: [
      {
        checksum: 'a'.repeat(64),
        description: 'core',
        filename: '000001_platform_core.sql',
        platformVersion: '0.1.0',
        sentinels: [{ columns: ['version', 'checksum'], table: 'schema_migrations' }],
        version: 1,
      },
      {
        checksum: 'b'.repeat(64),
        description: 'next',
        filename: '000002_next.sql',
        platformVersion: '0.2.0',
        sentinels: [{ columns: ['id'], table: 'next_table' }],
        version: 2,
      },
    ],
    schema: 'ape_dts_platform',
  },
  migrations: [
    {
      checksum: 'a'.repeat(64),
      description: 'core',
      filename: '000001_platform_core.sql',
      platformVersion: '0.1.0',
      sentinels: [{ columns: ['version', 'checksum'], table: 'schema_migrations' }],
      sql: 'create core',
      version: 1,
    },
    {
      checksum: 'b'.repeat(64),
      description: 'next',
      filename: '000002_next.sql',
      platformVersion: '0.2.0',
      sentinels: [{ columns: ['id'], table: 'next_table' }],
      sql: 'create next',
      version: 2,
    },
  ],
};

function absentSnapshot(): SchemaSnapshot {
  return {
    appliedMigrations: [],
    columnsByTable: {},
    migrationsTableExists: false,
    schemaExists: false,
  };
}

function applied(version: number, checksum: string): AppliedMigration {
  return { checksum, platformVersion: `0.${version}.0`, version };
}

describe('schema classification', () => {
  it('distinguishes absent, older, current, newer and corrupt without filling gaps', () => {
    expect(classifySchemaSnapshot(absentSnapshot(), bundle.manifest).state).toBe('ABSENT');

    const older: SchemaSnapshot = {
      appliedMigrations: [applied(1, 'a'.repeat(64))],
      columnsByTable: { schema_migrations: ['version', 'checksum'] },
      migrationsTableExists: true,
      schemaExists: true,
    };
    expect(classifySchemaSnapshot(older, bundle.manifest)).toMatchObject({
      pendingVersions: [2],
      state: 'OLDER',
    });

    const current: SchemaSnapshot = {
      appliedMigrations: [applied(1, 'a'.repeat(64)), applied(2, 'b'.repeat(64))],
      columnsByTable: {
        next_table: ['id'],
        schema_migrations: ['version', 'checksum'],
      },
      migrationsTableExists: true,
      schemaExists: true,
    };
    expect(classifySchemaSnapshot(current, bundle.manifest).state).toBe('CURRENT');

    expect(
      classifySchemaSnapshot(
        {
          ...current,
          appliedMigrations: [...current.appliedMigrations, applied(3, 'c'.repeat(64))],
        },
        bundle.manifest,
      ).state,
    ).toBe('NEWER');

    expect(
      classifySchemaSnapshot(
        { ...older, appliedMigrations: [applied(1, 'f'.repeat(64))] },
        bundle.manifest,
      ),
    ).toMatchObject({ issues: ['CHECKSUM_MISMATCH:1'], state: 'CORRUPT' });

    expect(
      classifySchemaSnapshot(
        { ...current, columnsByTable: { schema_migrations: ['version', 'checksum'] } },
        bundle.manifest,
      ),
    ).toMatchObject({ issues: ['MISSING_TABLE:next_table'], state: 'CORRUPT' });
  });

  it('treats an existing schema without the migration ledger as corrupt, not absent', () => {
    expect(
      classifySchemaSnapshot({ ...absentSnapshot(), schemaExists: true }, bundle.manifest),
    ).toMatchObject({ issues: ['MISSING_MIGRATIONS_TABLE'], state: 'CORRUPT' });
  });
});

class InMemoryMigrationStore implements MigrationStore {
  readonly events: string[] = [];
  lockAvailable = true;

  constructor(public snapshot: SchemaSnapshot) {}

  async transaction<T>(work: (transaction: MigrationTransaction) => Promise<T>): Promise<T> {
    const before = structuredClone(this.snapshot);
    this.events.push('BEGIN');
    const transaction: MigrationTransaction = {
      createPlatformSchema: async () => {
        this.events.push('CREATE_SCHEMA');
        this.snapshot = { ...this.snapshot, schemaExists: true };
      },
      executeMigration: async (migration) => {
        this.events.push(`EXECUTE:${migration.version}`);
        const columnsByTable = { ...this.snapshot.columnsByTable };
        for (const sentinel of migration.sentinels) {
          columnsByTable[sentinel.table] = [...sentinel.columns];
        }
        this.snapshot = {
          ...this.snapshot,
          columnsByTable,
          migrationsTableExists: true,
        };
      },
      inspectSchema: async (manifest) => classifySchemaSnapshot(this.snapshot, manifest),
      query: async () => ({ rows: [] }),
      recordMigration: async (migration) => {
        this.events.push(`RECORD:${migration.version}`);
        this.snapshot = {
          ...this.snapshot,
          appliedMigrations: [
            ...this.snapshot.appliedMigrations,
            {
              checksum: migration.checksum,
              platformVersion: migration.platformVersion,
              version: migration.version,
            },
          ],
        };
      },
      tryAcquireMigrationLock: async () => {
        this.events.push('LOCK');
        return this.lockAvailable;
      },
    };

    try {
      const result = await work(transaction);
      this.events.push('COMMIT');
      return result;
    } catch (error) {
      this.snapshot = before;
      this.events.push('ROLLBACK');
      throw error;
    }
  }
}

describe('MigrationRunner', () => {
  it('initializes an absent schema and invokes the bootstrap write inside the same transaction', async () => {
    const store = new InMemoryMigrationStore(absentSnapshot());
    const runner = new MigrationRunner(store);

    const result = await runner.migrate(bundle, '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', {
      afterMigrations: async (transaction) => {
        store.events.push('BOOTSTRAP_ROWS');
        await transaction.query('insert bootstrap rows');
      },
      expectedState: 'ABSENT',
    });

    expect(result.appliedVersions).toEqual([1, 2]);
    expect(store.events).toEqual([
      'BEGIN',
      'LOCK',
      'CREATE_SCHEMA',
      'EXECUTE:1',
      'RECORD:1',
      'EXECUTE:2',
      'RECORD:2',
      'BOOTSTRAP_ROWS',
      'COMMIT',
    ]);
    expect(classifySchemaSnapshot(store.snapshot, bundle.manifest).state).toBe('CURRENT');
  });

  it('rolls back every DDL and migration row when the bootstrap write fails', async () => {
    const store = new InMemoryMigrationStore(absentSnapshot());
    const runner = new MigrationRunner(store);

    await expect(
      runner.migrate(bundle, '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', {
        afterMigrations: () => {
          throw new Error('admin insert failed');
        },
        expectedState: 'ABSENT',
      }),
    ).rejects.toThrow('admin insert failed');

    expect(store.events.at(-1)).toBe('ROLLBACK');
    expect(store.snapshot).toEqual(absentSnapshot());
  });

  it('does not wait for or take over a held migration lock', async () => {
    const store = new InMemoryMigrationStore(absentSnapshot());
    store.lockAvailable = false;

    await expect(
      new MigrationRunner(store).migrate(bundle, '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', {
        expectedState: 'ABSENT',
      }),
    ).rejects.toBeInstanceOf(MigrationLockUnavailableError);
    expect(store.events).toEqual(['BEGIN', 'LOCK', 'ROLLBACK']);
  });

  it('refuses stale confirmations and corrupt or newer schemas before executing DDL', async () => {
    const store = new InMemoryMigrationStore({ ...absentSnapshot(), schemaExists: true });

    await expect(
      new MigrationRunner(store).migrate(bundle, '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3', {
        expectedState: 'ABSENT',
      }),
    ).rejects.toBeInstanceOf(SchemaNotMigratableError);
    expect(store.events).toEqual(['BEGIN', 'LOCK', 'ROLLBACK']);
  });
});
