import type { PoolClient, QueryResultRow } from 'pg';

import { classifySchemaSnapshot } from '../migrations/schema-classifier.js';
import type {
  AppliedMigration,
  MigrationManifest,
  SchemaClassification,
  SchemaSnapshot,
} from '../migrations/types.js';
import { qualifiedTable } from './identifiers.js';

interface SchemaExistsRow extends QueryResultRow {
  exists: boolean;
}

interface AppliedMigrationRow extends QueryResultRow {
  checksum: string;
  platform_version: string;
  version: string;
}

interface ColumnRow extends QueryResultRow {
  column_name: string;
  table_name: string;
}

interface DatabaseObjectRow extends QueryResultRow {
  object_name: string;
  table_name: string;
}

export class PgSchemaInspector {
  async inspect(client: PoolClient, manifest: MigrationManifest): Promise<SchemaClassification> {
    return classifySchemaSnapshot(await this.snapshot(client, manifest.schema), manifest);
  }

  async snapshot(client: PoolClient, schema: string): Promise<SchemaSnapshot> {
    const schemaResult = await client.query<SchemaExistsRow>(
      'SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1) AS exists',
      [schema],
    );
    const schemaExists = schemaResult.rows[0]?.exists === true;
    if (!schemaExists) {
      return {
        appliedMigrations: [],
        columnsByTable: {},
        constraintsByTable: {},
        indexesByTable: {},
        migrationsTableExists: false,
        schemaExists: false,
      };
    }

    const migrationTableResult = await client.query<SchemaExistsRow>(
      "SELECT to_regclass(format('%I.%I', $1::text, 'schema_migrations')) IS NOT NULL AS exists",
      [schema],
    );
    const migrationsTableExists = migrationTableResult.rows[0]?.exists === true;
    if (!migrationsTableExists) {
      return {
        appliedMigrations: [],
        columnsByTable: {},
        constraintsByTable: {},
        indexesByTable: {},
        migrationsTableExists: false,
        schemaExists: true,
      };
    }

    const migrationsResult = await client.query<AppliedMigrationRow>(
      `SELECT version::text, checksum, platform_version FROM ${qualifiedTable(schema, 'schema_migrations')} ORDER BY version`,
    );
    const columnsResult = await client.query<ColumnRow>(
      'SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = $1 ORDER BY table_name, ordinal_position',
      [schema],
    );
    const columnsByTable: Record<string, string[]> = {};
    for (const row of columnsResult.rows) {
      (columnsByTable[row.table_name] ??= []).push(row.column_name);
    }
    const constraintsResult = await client.query<DatabaseObjectRow>(
      `SELECT relation.relname AS table_name, constraint_.conname AS object_name
         FROM pg_catalog.pg_constraint AS constraint_
         JOIN pg_catalog.pg_class AS relation ON relation.oid = constraint_.conrelid
         JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = $1
        ORDER BY relation.relname, constraint_.conname`,
      [schema],
    );
    const constraintsByTable: Record<string, string[]> = {};
    for (const row of constraintsResult.rows) {
      (constraintsByTable[row.table_name] ??= []).push(row.object_name);
    }
    const indexesResult = await client.query<DatabaseObjectRow>(
      `SELECT tablename AS table_name, indexname AS object_name
         FROM pg_catalog.pg_indexes
        WHERE schemaname = $1
        ORDER BY tablename, indexname`,
      [schema],
    );
    const indexesByTable: Record<string, string[]> = {};
    for (const row of indexesResult.rows) {
      (indexesByTable[row.table_name] ??= []).push(row.object_name);
    }
    const appliedMigrations: AppliedMigration[] = migrationsResult.rows.map((row) => ({
      checksum: row.checksum,
      platformVersion: row.platform_version,
      version: Number(row.version),
    }));
    return {
      appliedMigrations,
      columnsByTable,
      constraintsByTable,
      indexesByTable,
      migrationsTableExists: true,
      schemaExists: true,
    };
  }
}
