import type { Pool, PoolClient, QueryResultRow } from 'pg';

import type {
  MigrationDefinition,
  MigrationManifest,
  MigrationQueryResult,
  MigrationStore,
  MigrationTransaction,
  SchemaClassification,
} from '../migrations/types.js';
import { qualifiedTable, quoteDatabaseIdentifier } from './identifiers.js';
import { PgSchemaInspector } from './schema-inspector.js';

const MIGRATION_LOCK_NAMESPACE = 1_096_048_964;
const MIGRATION_LOCK_KEY = 1_297_307_463;

interface LockRow extends QueryResultRow {
  acquired: boolean;
}

class PgMigrationTransaction implements MigrationTransaction {
  private readonly inspector = new PgSchemaInspector();

  constructor(private readonly client: PoolClient) {}

  async tryAcquireMigrationLock(): Promise<boolean> {
    const result = await this.client.query<LockRow>(
      'SELECT pg_try_advisory_xact_lock($1, $2) AS acquired',
      [MIGRATION_LOCK_NAMESPACE, MIGRATION_LOCK_KEY],
    );
    return result.rows[0]?.acquired === true;
  }

  async inspectSchema(manifest: MigrationManifest): Promise<SchemaClassification> {
    return await this.inspector.inspect(this.client, manifest);
  }

  async createPlatformSchema(schema: string): Promise<void> {
    await this.client.query(`CREATE SCHEMA ${quoteDatabaseIdentifier(schema)}`);
  }

  async executeMigration(migration: MigrationDefinition): Promise<void> {
    await this.client.query(migration.sql);
  }

  async recordMigration(migration: MigrationDefinition, instanceId: string): Promise<void> {
    await this.client.query(
      `INSERT INTO ${qualifiedTable('ape_dts_platform', 'schema_migrations')}
        (version, checksum, platform_version, applied_by_instance_id)
       VALUES ($1, $2, $3, $4::uuid)`,
      [migration.version, migration.checksum, migration.platformVersion, instanceId],
    );
  }

  async query(sql: string, values: readonly unknown[] = []): Promise<MigrationQueryResult> {
    const result = await this.client.query(sql, [...values]);
    return { rows: result.rows as Record<string, unknown>[] };
  }
}

export class PgMigrationStore implements MigrationStore {
  constructor(private readonly pool: Pool) {}

  async transaction<T>(work: (transaction: MigrationTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work(new PgMigrationTransaction(client));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // The original migration error is the actionable failure; a broken connection is discarded by pg.
      }
      throw error;
    } finally {
      client.release();
    }
  }
}
