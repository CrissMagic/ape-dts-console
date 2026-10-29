import type {
  DataSourceConfiguration,
  DataSourceIdentity,
  DataSourceType,
  MetadataItem,
  MetadataQuery,
  MysqlCdcInspection,
} from '@scs/contracts';
import mysql, { type Connection, type ConnectionOptions, type RowDataPacket } from 'mysql2/promise';

import {
  DataSourceAdapterError,
  type DataSourceAdapter,
  type DataSourceAdapterInput,
  type MysqlBinlogFile,
  requireMetadataContext,
  toAdapterError,
} from './types.js';

type MysqlCompatibleConfiguration = Extract<DataSourceConfiguration, { type: 'MYSQL' | 'DORIS' }>;

interface VersionRow extends RowDataPacket {
  server_version: string;
}

interface DatabaseRow extends RowDataPacket {
  database_name: string;
}

interface TableRow extends RowDataPacket {
  table_name: string;
}

interface ColumnRow extends RowDataPacket {
  column_name: string;
  column_type: string;
  is_nullable: 'NO' | 'YES';
  ordinal_position: number;
  is_primary_key?: number;
}

interface MysqlIdentityRow extends RowDataPacket {
  server_uuid: string;
  database_name: string | null;
}

interface MysqlVariableRow extends RowDataPacket {
  Variable_name: string;
  Value: string;
}

interface BinlogStatusRow extends RowDataPacket {
  File: string;
  Position: number | string;
}

async function readMysqlRows<T extends RowDataPacket>(
  connection: Connection,
  sql: string,
): Promise<T[]> {
  try {
    const [rows] = await connection.query<T[]>({ sql, timeout: 10_000 });
    return rows;
  } catch (error) {
    if (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      error.code === 'PROTOCOL_SEQUENCE_TIMEOUT'
    )
      connection.destroy();
    throw error;
  }
}

async function mysqlIdentity(
  connection: Connection,
  version: string | null,
): Promise<DataSourceIdentity> {
  const unavailable: DataSourceIdentity = {
    kind: 'UNAVAILABLE',
    serverVersion: version,
    value: null,
  };
  try {
    const rows = await readMysqlRows<MysqlIdentityRow>(
      connection,
      'SELECT @@server_uuid AS server_uuid, DATABASE() AS database_name',
    );
    const row = rows[0];
    if (
      row === undefined ||
      !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/iu.test(row.server_uuid) ||
      !row.database_name
    )
      return unavailable;
    return {
      kind: 'MYSQL_DATABASE',
      serverVersion: version,
      value: `${row.server_uuid.toLowerCase()}:${row.database_name}`,
    };
  } catch (error) {
    // MySQL-compatible engines may connect successfully without a MySQL instance UUID.
    if (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      error.code === 'ER_UNKNOWN_SYSTEM_VARIABLE'
    )
      return unavailable;
    throw error;
  }
}

function requireMysqlCompatibleConfiguration(
  configuration: DataSourceConfiguration,
  type: 'MYSQL' | 'DORIS',
): MysqlCompatibleConfiguration {
  if (configuration.type !== type) {
    throw new Error('MYSQL_COMPATIBLE_ADAPTER_CONFIGURATION_TYPE_MISMATCH');
  }
  return configuration;
}

function connectionOptions(
  configuration: MysqlCompatibleConfiguration,
  password: string,
  database = configuration.database,
): ConnectionOptions {
  const ssl =
    configuration.tls.mode === 'DISABLE'
      ? undefined
      : configuration.tls.mode === 'VERIFY_FULL'
        ? { ca: configuration.tls.caCertificate, rejectUnauthorized: true }
        : { rejectUnauthorized: false };
  return {
    connectTimeout: 10_000,
    database,
    host: configuration.host,
    password,
    port: configuration.port,
    user: configuration.username,
    ...(ssl === undefined ? {} : { ssl }),
  };
}

export class MysqlCompatibleDataSourceAdapter implements DataSourceAdapter {
  constructor(readonly type: Extract<DataSourceType, 'MYSQL' | 'DORIS'>) {}

  private async withConnection<T>(
    input: DataSourceAdapterInput,
    database: string | undefined,
    operation: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    const configuration = requireMysqlCompatibleConfiguration(input.configuration, this.type);
    if (input.password === null) {
      throw new Error('MYSQL_COMPATIBLE_DATA_SOURCE_PASSWORD_MISSING');
    }
    let connection: Connection | undefined;
    try {
      connection = await mysql.createConnection(
        connectionOptions(configuration, input.password, database),
      );
      return await operation(connection);
    } catch (error) {
      throw toAdapterError(error);
    } finally {
      await connection?.end().catch(() => undefined);
    }
  }

  async test(input: DataSourceAdapterInput): Promise<DataSourceIdentity> {
    return await this.withConnection(input, undefined, async (connection) => {
      const [rows] = await connection.query<VersionRow[]>('SELECT VERSION() AS server_version');
      if (this.type === 'MYSQL')
        return await mysqlIdentity(connection, rows[0]?.server_version ?? null);
      return {
        kind: 'UNAVAILABLE',
        serverVersion: rows[0]?.server_version ?? null,
        value: null,
      };
    });
  }

  async inspectMysqlCdc(input: DataSourceAdapterInput): Promise<MysqlCdcInspection> {
    if (this.type !== 'MYSQL')
      throw new DataSourceAdapterError('METADATA_UNSUPPORTED', '仅 MySQL 数据源支持 Binlog 检查。');
    return await this.withConnection(input, undefined, async (connection) => {
      const versions = await readMysqlRows<VersionRow>(
        connection,
        'SELECT VERSION() AS server_version',
      );
      const version = versions[0]?.server_version ?? null;
      const identity = await mysqlIdentity(connection, version);
      const variables = await readMysqlRows<MysqlVariableRow>(
        connection,
        `SHOW GLOBAL VARIABLES WHERE Variable_name IN (
          'server_id', 'log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode'
        )`,
      );
      const values = new Map(variables.map((row) => [row.Variable_name.toLowerCase(), row.Value]));
      const rawServerId = values.get('server_id');
      const parsedServerId =
        rawServerId !== undefined && /^\d+$/u.test(rawServerId) ? Number(rawServerId) : NaN;
      const serverId =
        Number.isSafeInteger(parsedServerId) &&
        parsedServerId > 0 &&
        parsedServerId <= 4_294_967_295
          ? parsedServerId
          : null;
      const enabled = ['ON', '1'].includes(values.get('log_bin') ?? '')
        ? true
        : ['OFF', '0'].includes(values.get('log_bin') ?? '')
          ? false
          : null;
      const format = values.get('binlog_format') ?? null;
      const rowImage = values.get('binlog_row_image') ?? null;
      const majorMinor = /^(\d+)\.(\d+)\./u.exec(version ?? '');
      const major = Number(majorMinor?.[1]);
      const minor = Number(majorMinor?.[2]);
      const supportedVersion =
        majorMinor !== null &&
        !version?.toLowerCase().includes('mariadb') &&
        (major > 5 || (major === 5 && minor >= 7));
      let position: MysqlCdcInspection['position'] = null;
      if (enabled === true && supportedVersion) {
        const status = await readMysqlRows<BinlogStatusRow>(
          connection,
          major > 8 || (major === 8 && minor >= 4)
            ? 'SHOW BINARY LOG STATUS'
            : 'SHOW MASTER STATUS',
        );
        const row = status[0];
        const parsedPosition =
          row !== undefined && /^\d+$/u.test(String(row.Position)) ? Number(row.Position) : NaN;
        if (
          row !== undefined &&
          /^[A-Za-z0-9._-]{1,255}$/u.test(row.File) &&
          Number.isSafeInteger(parsedPosition) &&
          parsedPosition >= 4 &&
          parsedPosition <= 4_294_967_295
        )
          position = { filename: row.File, position: parsedPosition };
      }
      const check = (
        key: MysqlCdcInspection['checks'][number]['key'],
        passed: boolean,
        summary: string,
      ): MysqlCdcInspection['checks'][number] => ({
        key,
        state: passed ? 'PASSED' : 'FAILED',
        summary,
      });
      return {
        identity,
        binlog: { enabled, format, rowImage, serverId, gtidMode: values.get('gtid_mode') ?? null },
        position,
        checks: [
          check(
            'MYSQL_VERSION',
            supportedVersion,
            supportedVersion
              ? `MySQL ${version}`
              : '需要 MySQL 5.7 或更高版本；MariaDB 不在本次接入范围内。',
          ),
          check(
            'MYSQL_IDENTITY',
            identity.kind === 'MYSQL_DATABASE',
            identity.kind === 'MYSQL_DATABASE'
              ? '已读取实例 UUID 与数据库身份。'
              : '未能读取稳定实例 UUID 与数据库身份。',
          ),
          check(
            'SERVER_ID',
            serverId !== null,
            serverId !== null
              ? `服务端 server_id：${serverId}`
              : '服务端需要设置有效且非零的 server_id。',
          ),
          check(
            'BINLOG_ENABLED',
            enabled === true,
            enabled === true ? 'Binlog 已开启。' : '需要在 MySQL 服务端开启 log_bin。',
          ),
          check(
            'BINLOG_FORMAT',
            format === 'ROW',
            format === 'ROW' ? 'Binlog 格式为 ROW。' : '需要将 binlog_format 设置为 ROW。',
          ),
          check(
            'BINLOG_ROW_IMAGE',
            rowImage === 'FULL',
            rowImage === 'FULL'
              ? 'Binlog 行镜像为 FULL。'
              : '需要将 binlog_row_image 设置为 FULL。',
          ),
          check(
            'BINLOG_POSITION',
            position !== null,
            position !== null
              ? '已读取当前 Binlog 文件与位置。'
              : '未能读取有效 Binlog 位点；请检查 Binlog 配置及 REPLICATION CLIENT 权限。',
          ),
        ],
      };
    });
  }

  async readMysqlBinlogs(input: DataSourceAdapterInput): Promise<readonly MysqlBinlogFile[]> {
    if (this.type !== 'MYSQL')
      throw new DataSourceAdapterError('METADATA_UNSUPPORTED', '仅 MySQL 数据源支持 Binlog 检查。');
    return await this.withConnection(input, undefined, async (connection) => {
      const rows = await readMysqlRows<RowDataPacket>(connection, 'SHOW BINARY LOGS');
      return rows.map((row) => {
        const filename = String(row.Log_name);
        const sizeBytes = String(row.File_size);
        if (
          !/^[A-Za-z0-9._-]{1,255}$/u.test(filename) ||
          !/^\d+$/u.test(sizeBytes) ||
          (typeof row.File_size === 'number' && !Number.isSafeInteger(row.File_size))
        )
          throw new DataSourceAdapterError(
            'CONNECTION_FAILED',
            'MySQL 返回的 Binlog 文件信息无效。',
          );
        return { filename, sizeBytes };
      });
    });
  }

  async metadata(
    input: DataSourceAdapterInput,
    query: MetadataQuery,
  ): Promise<readonly MetadataItem[]> {
    const database =
      query.resource === 'DATABASES'
        ? undefined
        : requireMetadataContext(query.database, 'database');
    return await this.withConnection(input, database, async (connection) => {
      switch (query.resource) {
        case 'DATABASES': {
          const [rows] = await connection.query<DatabaseRow[]>(
            `SELECT schema_name AS database_name
             FROM information_schema.schemata
             ORDER BY schema_name`,
          );
          return rows.map((row) => ({
            attributes: {},
            identity: null,
            name: row.database_name,
          }));
        }
        case 'TABLES': {
          const [rows] = await connection.query<TableRow[]>(
            `SELECT table_name AS table_name
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND table_type = 'BASE TABLE'
             ORDER BY table_name`,
          );
          return rows.map((row) => ({ attributes: {}, identity: null, name: row.table_name }));
        }
        case 'COLUMNS': {
          const table = requireMetadataContext(query.table, 'table');
          const [rows] = await connection.query<ColumnRow[]>(
            `SELECT cols.column_name AS column_name,
                    cols.column_type AS column_type,
                    cols.is_nullable AS is_nullable,
                    cols.ordinal_position AS ordinal_position
                    ${
                      this.type === 'MYSQL'
                        ? `, EXISTS (
                      SELECT 1 FROM information_schema.table_constraints AS tc
                      JOIN information_schema.key_column_usage AS ku
                        ON ku.constraint_schema = tc.constraint_schema
                       AND ku.constraint_name = tc.constraint_name
                       AND ku.table_schema = tc.table_schema
                       AND ku.table_name = tc.table_name
                      WHERE tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = cols.table_schema
                        AND tc.table_name = cols.table_name
                        AND ku.column_name = cols.column_name
                    ) AS is_primary_key`
                        : ''
                    }
             FROM information_schema.columns AS cols
             WHERE cols.table_schema = DATABASE()
               AND cols.table_name = ?
             ORDER BY cols.ordinal_position`,
            [table],
          );
          return rows.map((row) => ({
            attributes: {
              dataType: row.column_type,
              isNullable: row.is_nullable === 'YES' ? 'true' : 'false',
              ordinalPosition: String(row.ordinal_position),
              ...(this.type === 'MYSQL'
                ? { isPrimaryKey: row.is_primary_key === 1 ? 'true' : 'false' }
                : {}),
            },
            identity: String(row.ordinal_position),
            name: row.column_name,
          }));
        }
        case 'SCHEMAS':
        case 'TOPICS':
        case 'PARTITIONS':
          throw new DataSourceAdapterError(
            'METADATA_UNSUPPORTED',
            `${this.type} 数据源不支持该元数据资源。`,
          );
      }
    });
  }
}
