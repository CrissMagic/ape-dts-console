import type { ConnectionOptions } from 'node:tls';

import type {
  DataSourceConfiguration,
  DataSourceIdentity,
  MetadataItem,
  MetadataQuery,
} from '@scs/contracts';
import { Pool, type PoolConfig, type QueryResultRow } from 'pg';
import {
  preparePostgresReplicationResources,
  readPostgresReplicationResources,
} from './postgres-replication-resources.js';
import type { PostgresResourceInspection, PostgresResourcePreparationRequest } from './types.js';

import {
  DataSourceAdapterError,
  type DataSourceAdapter,
  type DataSourceAdapterInput,
  requireMetadataContext,
  type PostgresContinuityFact,
  toAdapterError,
} from './types.js';

const CONNECTION_TIMEOUT_MILLISECONDS = 10_000;
const QUERY_TIMEOUT_MILLISECONDS = 10_000;

type PostgresConfiguration = Extract<DataSourceConfiguration, { type: 'POSTGRES' }>;

interface ServerRow extends QueryResultRow {
  database_name: string;
  server_version: string;
}

interface SystemIdentifierRow extends QueryResultRow {
  system_identifier: string;
}

interface DatabaseRow extends QueryResultRow {
  database_name: string;
  database_oid: string;
}

interface SchemaRow extends QueryResultRow {
  schema_name: string;
  schema_oid: string;
}

interface TableRow extends QueryResultRow {
  table_name: string;
  table_oid: string;
}

interface ColumnRow extends QueryResultRow {
  column_name: string;
  data_type: string;
  is_nullable: 'NO' | 'YES';
  is_primary_key: boolean;
  ordinal_position: string;
}

function requirePostgresConfiguration(
  configuration: DataSourceConfiguration,
): PostgresConfiguration {
  if (configuration.type !== 'POSTGRES') {
    throw new Error('POSTGRES_ADAPTER_CONFIGURATION_TYPE_MISMATCH');
  }
  return configuration;
}

function sslConfiguration(configuration: PostgresConfiguration): false | ConnectionOptions {
  switch (configuration.tls.mode) {
    case 'DISABLE':
      return false;
    case 'REQUIRE':
      return { rejectUnauthorized: false };
    case 'VERIFY_FULL':
      return { ca: configuration.tls.caCertificate, rejectUnauthorized: true };
  }
}

function poolConfiguration(
  configuration: PostgresConfiguration,
  password: string,
  database = configuration.database,
): PoolConfig {
  return {
    application_name: 'scs-data-sync-platform-data-source-test',
    connectionTimeoutMillis: CONNECTION_TIMEOUT_MILLISECONDS,
    database,
    host: configuration.host,
    idleTimeoutMillis: 1_000,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    max: 1,
    password,
    port: configuration.port,
    query_timeout: QUERY_TIMEOUT_MILLISECONDS,
    ssl: sslConfiguration(configuration),
    statement_timeout: QUERY_TIMEOUT_MILLISECONDS,
    user: configuration.username,
  };
}

function requirePassword(input: DataSourceAdapterInput): string {
  if (input.password === null) {
    throw new Error('POSTGRES_DATA_SOURCE_PASSWORD_MISSING');
  }
  return input.password;
}

export class PostgresDataSourceAdapter implements DataSourceAdapter {
  readonly type = 'POSTGRES' as const;
  async readProgress(input: DataSourceAdapterInput, request: { slotName?: string }) {
    if (!request.slotName) throw new Error('PROGRESS_SLOT_REQUIRED');
    return this.withPool(input, undefined, async (pool) => {
      const result = await pool.query<{
        active: boolean;
        unconfirmed: string | null;
        retained: string | null;
        replication_lag_ms: number | null;
      }>(
        `SELECT slot.active,
                CASE WHEN slot.confirmed_flush_lsn <= pg_current_wal_lsn() THEN
                  pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn)::text END AS unconfirmed,
                CASE WHEN slot.restart_lsn <= pg_current_wal_lsn() THEN
                  pg_wal_lsn_diff(pg_current_wal_lsn(), slot.restart_lsn)::text END AS retained,
                CASE WHEN slot.active AND replication.replay_lag >= interval '0 seconds' THEN
                  (extract(epoch FROM replication.replay_lag) * 1000)::double precision END AS replication_lag_ms
           FROM pg_replication_slots AS slot
           LEFT JOIN pg_stat_replication AS replication ON replication.pid=slot.active_pid
          WHERE slot.slot_name=$1 AND slot.database=current_database() AND slot.slot_type='logical'`,
        [request.slotName],
      );
      const row = result.rows[0];
      return {
        slotStatus: row
          ? row.active
            ? ('ACTIVE' as const)
            : ('INACTIVE' as const)
          : ('MISSING' as const),
        unconfirmedWalBytes: row?.unconfirmed ?? null,
        retainedWalBytes: row?.retained ?? null,
        replicationLagMilliseconds: row?.active === true ? (row.replication_lag_ms ?? null) : null,
        offsetLag: null,
      };
    });
  }
  async readPostgresResources(
    input: DataSourceAdapterInput,
    request: Omit<PostgresResourcePreparationRequest, 'resourceMode'>,
  ) {
    return this.withPool(input, undefined, (pool) =>
      readPostgresReplicationResources(pool, request),
    );
  }

  async preparePostgresResources(
    input: DataSourceAdapterInput,
    request: PostgresResourcePreparationRequest,
    beforeWrite: (inspection: PostgresResourceInspection) => Promise<void>,
  ) {
    return this.withPool(input, undefined, (pool) =>
      preparePostgresReplicationResources(pool, request, beforeWrite),
    );
  }

  async readPostgresContinuity(
    input: DataSourceAdapterInput,
    slotName: string,
  ): Promise<PostgresContinuityFact> {
    return await this.withPool(input, undefined, async (pool) => {
      const result = await pool.query<{
        confirmed_flush_lsn: string | null;
        database_name: string;
        database_oid: string;
        plugin: string | null;
        active: boolean | null;
        slot_name: string | null;
        system_identifier: string;
      }>(
        `SELECT control.system_identifier::text,
                current_database() AS database_name,
                database.oid::text AS database_oid,
                slot.slot_name, slot.plugin, slot.active, slot.confirmed_flush_lsn::text
           FROM pg_control_system() AS control
           JOIN pg_database AS database ON database.datname = current_database()
           LEFT JOIN pg_replication_slots AS slot
             ON slot.slot_name = $1 AND slot.database = current_database()`,
        [slotName],
      );
      const row = result.rows[0];
      if (row === undefined || row.slot_name === null)
        throw new DataSourceAdapterError('POSTGRES_SLOT_MISSING', '指定复制槽不存在。');
      if (row.active !== false)
        throw new DataSourceAdapterError(
          'POSTGRES_SLOT_ACTIVE',
          '指定复制槽当前仍被其他进程占用。',
        );
      if (row.plugin === null)
        throw new DataSourceAdapterError(
          'POSTGRES_CONTINUITY_IDENTITY_UNAVAILABLE',
          '复制槽 plugin 不可用。',
        );
      if (row.confirmed_flush_lsn === null)
        throw new DataSourceAdapterError(
          'POSTGRES_CONTINUITY_IDENTITY_UNAVAILABLE',
          '复制槽 confirmed_flush_lsn 不可用。',
        );
      return {
        confirmedFlushLsn: row.confirmed_flush_lsn,
        databaseName: row.database_name,
        databaseOid: row.database_oid,
        slotName: row.slot_name,
        slotPlugin: row.plugin,
        slotActive: false,
        systemIdentifier: row.system_identifier,
      };
    });
  }

  private async withPool<T>(
    input: DataSourceAdapterInput,
    database: string | undefined,
    operation: (pool: Pool) => Promise<T>,
  ): Promise<T> {
    const configuration = requirePostgresConfiguration(input.configuration);
    const pool = new Pool(poolConfiguration(configuration, requirePassword(input), database));
    try {
      return await operation(pool);
    } catch (error) {
      throw toAdapterError(error);
    } finally {
      await pool.end().catch(() => undefined);
    }
  }

  async test(input: DataSourceAdapterInput): Promise<DataSourceIdentity> {
    return await this.withPool(input, undefined, async (pool) => {
      const serverResult = await pool.query<ServerRow>(
        "SELECT current_database() AS database_name, current_setting('server_version') AS server_version",
      );
      const server = serverResult.rows[0];
      if (server === undefined) {
        throw new Error('POSTGRES_SERVER_IDENTIFICATION_EMPTY');
      }
      try {
        const identityResult = await pool.query<SystemIdentifierRow>(
          'SELECT system_identifier::text FROM pg_control_system()',
        );
        const identity = identityResult.rows[0];
        if (identity === undefined) {
          return {
            kind: 'UNAVAILABLE',
            serverVersion: server.server_version,
            value: null,
          };
        }
        return {
          kind: 'POSTGRES_DATABASE',
          serverVersion: server.server_version,
          value: `${identity.system_identifier}:${server.database_name}`,
        };
      } catch (error) {
        const classified = toAdapterError(error);
        if (classified.code === 'PERMISSION_DENIED') {
          return {
            kind: 'UNAVAILABLE',
            serverVersion: server.server_version,
            value: null,
          };
        }
        throw classified;
      }
    });
  }

  async metadata(
    input: DataSourceAdapterInput,
    query: MetadataQuery,
  ): Promise<readonly MetadataItem[]> {
    const selectedDatabase =
      query.resource === 'DATABASES'
        ? undefined
        : requireMetadataContext(query.database, 'database');
    return await this.withPool(input, selectedDatabase, async (pool) => {
      switch (query.resource) {
        case 'DATABASES': {
          const result = await pool.query<DatabaseRow>(
            `SELECT datname AS database_name, oid::text AS database_oid
             FROM pg_database
             WHERE datallowconn
             ORDER BY datname`,
          );
          return result.rows.map((row) => ({
            attributes: {},
            identity: row.database_oid,
            name: row.database_name,
          }));
        }
        case 'SCHEMAS': {
          const result = await pool.query<SchemaRow>(
            `SELECT nspname AS schema_name, oid::text AS schema_oid
             FROM pg_namespace
             WHERE has_schema_privilege(nspname, 'USAGE')
             ORDER BY nspname`,
          );
          return result.rows.map((row) => ({
            attributes: {},
            identity: row.schema_oid,
            name: row.schema_name,
          }));
        }
        case 'TABLES': {
          const schema = requireMetadataContext(query.schema, 'schema');
          const result = await pool.query<TableRow>(
            `SELECT tables.table_name, classes.oid::text AS table_oid
             FROM information_schema.tables AS tables
             JOIN pg_namespace AS namespaces
               ON namespaces.nspname = tables.table_schema
             JOIN pg_class AS classes
               ON classes.relnamespace = namespaces.oid
              AND classes.relname = tables.table_name
             WHERE tables.table_catalog = current_database()
               AND tables.table_schema = $1
               AND tables.table_type = 'BASE TABLE'
             ORDER BY tables.table_name`,
            [schema],
          );
          return result.rows.map((row) => ({
            attributes: {},
            identity: row.table_oid,
            name: row.table_name,
          }));
        }
        case 'COLUMNS': {
          const schema = requireMetadataContext(query.schema, 'schema');
          const table = requireMetadataContext(query.table, 'table');
          const result = await pool.query<ColumnRow>(
            `SELECT
               columns.column_name,
               columns.data_type,
               columns.is_nullable,
               columns.ordinal_position::text,
               EXISTS (
                 SELECT 1
                 FROM information_schema.table_constraints AS constraints
                 JOIN information_schema.key_column_usage AS key_usage
                   ON key_usage.constraint_name = constraints.constraint_name
                  AND key_usage.constraint_schema = constraints.constraint_schema
                  AND key_usage.table_name = constraints.table_name
                 WHERE constraints.constraint_type = 'PRIMARY KEY'
                   AND constraints.table_schema = columns.table_schema
                   AND constraints.table_name = columns.table_name
                   AND key_usage.column_name = columns.column_name
               ) AS is_primary_key
             FROM information_schema.columns AS columns
             WHERE columns.table_catalog = current_database()
               AND columns.table_schema = $1
               AND columns.table_name = $2
             ORDER BY columns.ordinal_position`,
            [schema, table],
          );
          return result.rows.map((row) => ({
            attributes: {
              dataType: row.data_type,
              isNullable: row.is_nullable === 'YES' ? 'true' : 'false',
              isPrimaryKey: row.is_primary_key ? 'true' : 'false',
              ordinalPosition: row.ordinal_position,
            },
            identity: row.ordinal_position,
            name: row.column_name,
          }));
        }
        case 'TOPICS':
        case 'PARTITIONS':
          throw new DataSourceAdapterError(
            'METADATA_UNSUPPORTED',
            'PostgreSQL 数据源不支持该元数据资源。',
          );
      }
    });
  }
}
