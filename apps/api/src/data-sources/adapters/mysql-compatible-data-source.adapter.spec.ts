import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MysqlCompatibleDataSourceAdapter } from './mysql-compatible-data-source.adapter.js';

const { createConnection, query, end, destroy } = vi.hoisted(() => ({
  createConnection: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  destroy: vi.fn(),
}));
vi.mock('mysql2/promise', () => ({ default: { createConnection } }));

const serverUuid = '2195a93c-721c-11ee-8a48-000c29f49242';
const input = {
  configuration: {
    database: 'demo',
    host: 'mysql.test',
    port: 3306,
    tls: { mode: 'DISABLE' as const, riskAcknowledged: true as const },
    type: 'MYSQL' as const,
    username: 'reader',
  },
  password: 'test-only-password',
};

beforeEach(() => {
  vi.resetAllMocks();
  createConnection.mockResolvedValue({ query, end, destroy });
  end.mockResolvedValue(undefined);
});

function sqlText(input: string | { sql: string }): string {
  return typeof input === 'string' ? input : input.sql;
}

function server(version = '8.0.36', variables: Record<string, string> = {}) {
  query.mockImplementation(async (input: string | { sql: string }) => {
    const sql = sqlText(input);
    if (sql.includes('VERSION()')) return [[{ server_version: version }]];
    if (sql.includes('@@server_uuid'))
      return [[{ server_uuid: serverUuid, database_name: 'demo' }]];
    if (sql.includes('SHOW GLOBAL VARIABLES'))
      return [
        Object.entries({
          server_uuid: serverUuid,
          server_id: '1',
          log_bin: 'ON',
          binlog_format: 'ROW',
          binlog_row_image: 'FULL',
          gtid_mode: 'OFF',
          ...variables,
        }).map(([Variable_name, Value]) => ({ Variable_name, Value })),
      ];
    if (sql === 'SHOW MASTER STATUS' || sql === 'SHOW BINARY LOG STATUS')
      return [[{ File: 'mysql-bin.000001', Position: 157 }]];
    throw new Error(`Unexpected query: ${sql}`);
  });
}

describe('MySQL source inspection', () => {
  it('reads retained Binlog files with exact sizes and sanitizes failed reads', async () => {
    query.mockResolvedValue([[{ Log_name: 'mysql-bin.000001', File_size: '4294967296' }]]);
    const adapter = new MysqlCompatibleDataSourceAdapter('MYSQL');
    await expect(adapter.readMysqlBinlogs(input)).resolves.toEqual([
      { filename: 'mysql-bin.000001', sizeBytes: '4294967296' },
    ]);
    expect(query).toHaveBeenCalledWith({ sql: 'SHOW BINARY LOGS', timeout: 10000 });
    query.mockResolvedValue([[{ Log_name: '../unexpected', File_size: 'NaN' }]]);
    await expect(adapter.readMysqlBinlogs(input)).rejects.toMatchObject({
      code: 'CONNECTION_FAILED',
    });
    expect(end).toHaveBeenCalledTimes(2);
  });
  it('identifies an instance and database without requiring replication privileges', async () => {
    server();
    await expect(new MysqlCompatibleDataSourceAdapter('MYSQL').test(input)).resolves.toEqual({
      kind: 'MYSQL_DATABASE',
      serverVersion: '8.0.36',
      value: `${serverUuid}:demo`,
    });
    expect(query.mock.calls.map(([sql]) => sqlText(sql))).not.toEqual(
      expect.arrayContaining(['SHOW MASTER STATUS']),
    );
    expect(query.mock.calls.map(([sql]) => sqlText(sql))).not.toEqual(
      expect.arrayContaining(['SHOW BINARY LOG STATUS']),
    );
    expect(end).toHaveBeenCalledOnce();
  });

  it('keeps Doris connection tests independent of MySQL-only variables', async () => {
    query.mockResolvedValue([[{ server_version: '5.7.99 Doris' }]]);
    await expect(
      new MysqlCompatibleDataSourceAdapter('DORIS').test({
        ...input,
        configuration: { ...input.configuration, type: 'DORIS' },
      }),
    ).resolves.toMatchObject({ kind: 'UNAVAILABLE', value: null });
    expect(query).toHaveBeenCalledExactlyOnceWith('SELECT VERSION() AS server_version');
  });

  it('does not invent an identity for a MySQL-compatible server without server_uuid', async () => {
    query.mockImplementation(async (input: string | { sql: string }) => {
      const sql = sqlText(input);
      if (sql.includes('VERSION()')) return [[{ server_version: '10.11.0-MariaDB' }]];
      throw Object.assign(new Error('Unknown system variable'), {
        code: 'ER_UNKNOWN_SYSTEM_VARIABLE',
      });
    });
    await expect(new MysqlCompatibleDataSourceAdapter('MYSQL').test(input)).resolves.toEqual({
      kind: 'UNAVAILABLE',
      serverVersion: '10.11.0-MariaDB',
      value: null,
    });
  });

  it.each(['5.7.44', '8.0.36', '8.4.7'])(
    'inspects Binlog settings and the current position on MySQL %s using read-only SQL',
    async (version) => {
      server(version);
      const result = await new MysqlCompatibleDataSourceAdapter('MYSQL').inspectMysqlCdc(input);
      expect(result.identity.value).toBe(`${serverUuid}:demo`);
      expect(result.binlog).toEqual({
        enabled: true,
        format: 'ROW',
        rowImage: 'FULL',
        serverId: 1,
        gtidMode: 'OFF',
      });
      expect(result.position).toEqual({ filename: 'mysql-bin.000001', position: 157 });
      expect(result.checks.every((check) => check.state === 'PASSED')).toBe(true);
      expect(query).toHaveBeenCalledWith({
        sql: version.startsWith('8.4') ? 'SHOW BINARY LOG STATUS' : 'SHOW MASTER STATUS',
        timeout: 10_000,
      });
      expect(end).toHaveBeenCalledOnce();
    },
  );

  it('reports disabled Binlog, incompatible settings and missing position without defaults', async () => {
    server('8.0.36', { log_bin: 'OFF', binlog_format: 'STATEMENT', binlog_row_image: 'MINIMAL' });
    const result = await new MysqlCompatibleDataSourceAdapter('MYSQL').inspectMysqlCdc(input);
    expect(
      result.checks.filter((check) => check.state === 'FAILED').map((check) => check.key),
    ).toEqual(['BINLOG_ENABLED', 'BINLOG_FORMAT', 'BINLOG_ROW_IMAGE', 'BINLOG_POSITION']);
    expect(result.position).toBeNull();
    expect(query.mock.calls.map(([sql]) => sqlText(sql))).not.toEqual(
      expect.arrayContaining(['SHOW MASTER STATUS']),
    );
    expect(query.mock.calls.map(([sql]) => sqlText(sql))).not.toEqual(
      expect.arrayContaining(['SHOW BINARY LOG STATUS']),
    );
  });

  it('does not accept a malformed Binlog position as safe', async () => {
    server();
    const implementation = query.getMockImplementation()!;
    query.mockImplementation(async (sql: string) =>
      sqlText(sql) === 'SHOW MASTER STATUS'
        ? [[{ File: 'mysql-bin.000001', Position: 'not-a-position' }]]
        : await implementation(sql),
    );
    const result = await new MysqlCompatibleDataSourceAdapter('MYSQL').inspectMysqlCdc(input);
    expect(result.position).toBeNull();
    expect(result.checks.find((check) => check.key === 'BINLOG_POSITION')?.state).toBe('FAILED');
  });

  it('sanitizes a denied position read and always closes the connection', async () => {
    server();
    const implementation = query.getMockImplementation()!;
    query.mockImplementation(async (sql: string) => {
      if (sqlText(sql) === 'SHOW MASTER STATUS')
        throw Object.assign(new Error('replication permission failure: secret'), {
          code: 'ER_SPECIFIC_ACCESS_DENIED_ERROR',
          errno: 1227,
        });
      return await implementation(sql);
    });
    await expect(
      new MysqlCompatibleDataSourceAdapter('MYSQL').inspectMysqlCdc(input),
    ).rejects.toMatchObject({
      code: 'PERMISSION_DENIED',
      summary: expect.not.stringContaining('secret'),
    });
    expect(end).toHaveBeenCalledOnce();
  });

  it('destroys an unresponsive inspection connection and reports a timeout', async () => {
    query.mockRejectedValue(
      Object.assign(new Error('Query inactivity timeout'), { code: 'PROTOCOL_SEQUENCE_TIMEOUT' }),
    );
    await expect(
      new MysqlCompatibleDataSourceAdapter('MYSQL').inspectMysqlCdc(input),
    ).rejects.toMatchObject({ code: 'TIMEOUT' });
    expect(query).toHaveBeenCalledWith({
      sql: 'SELECT VERSION() AS server_version',
      timeout: 10_000,
    });
    expect(destroy).toHaveBeenCalledOnce();
    expect(end).toHaveBeenCalledOnce();
  });

  it('reads primary key membership for the selected database and table', async () => {
    query.mockResolvedValue([
      [
        {
          column_name: 'id',
          column_type: 'bigint',
          is_nullable: 'NO',
          ordinal_position: 1,
          column_key: 'PRI',
          is_primary_key: 1,
        },
        {
          column_name: 'name',
          column_type: 'varchar(64)',
          is_nullable: 'YES',
          ordinal_position: 2,
          column_key: '',
          is_primary_key: 0,
        },
      ],
    ]);
    const result = await new MysqlCompatibleDataSourceAdapter('MYSQL').metadata(input, {
      database: 'demo',
      table: 'source_rows',
      resource: 'COLUMNS',
    });
    expect(result.map((column) => [column.name, column.attributes.isPrimaryKey])).toEqual([
      ['id', 'true'],
      ['name', 'false'],
    ]);
    expect(query).toHaveBeenCalledWith(expect.stringContaining('table_schema = DATABASE()'), [
      'source_rows',
    ]);
  });

  it('does not mistake an implicit unique key for an explicit primary key', async () => {
    query.mockResolvedValue([
      [
        {
          column_name: 'unique_id',
          column_type: 'bigint',
          is_nullable: 'NO',
          ordinal_position: 1,
          column_key: 'PRI',
          is_primary_key: 0,
        },
      ],
    ]);
    const columns = await new MysqlCompatibleDataSourceAdapter('MYSQL').metadata(input, {
      database: 'demo',
      table: 'unique_only',
      resource: 'COLUMNS',
    });
    expect(columns[0]?.attributes.isPrimaryKey).toBe('false');
  });
});
