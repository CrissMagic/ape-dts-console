import type { Pool, PoolClient } from 'pg';
import {
  DataSourceAdapterError,
  PostgresResourcePreparationError,
  type PreparedPostgresResources,
  type PostgresResourceInspection,
  type PostgresResourcePreparationRequest,
} from './types.js';

function identifier(value: string): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    Buffer.byteLength(value) > 63 ||
    value.includes('\0')
  )
    throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', 'PostgreSQL 标识符无效。');
  return `"${value.replaceAll('"', '""')}"`;
}

type PostgresResourceReadRequest = Omit<PostgresResourcePreparationRequest, 'resourceMode'>;

interface PublicationState {
  readonly oid: string;
  readonly pubinsert: boolean;
  readonly pubupdate: boolean;
  readonly pubdelete: boolean;
  readonly pubtruncate: boolean;
  readonly includes_table: boolean;
}

interface ResourceState {
  readonly databaseName: string;
  readonly identityPrefix: string;
  readonly publication: PublicationState | undefined;
  readonly slot:
    | {
        readonly database: string | null;
        readonly plugin: string | null;
        readonly active: boolean;
        readonly slot_type: string;
      }
    | undefined;
}

function validateRequest(request: PostgresResourceReadRequest) {
  identifier(request.publicationName);
  if (!/^[a-z0-9_]{1,63}$/u.test(request.slotName))
    throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', '复制槽名称无效。');
  if (request.tables !== undefined && (request.schema !== undefined || request.table !== undefined))
    throw new DataSourceAdapterError(
      'PG_RESOURCE_CONFLICT',
      '源表配置不能同时使用单表和多表格式。',
    );
  const tables =
    request.tables ??
    (request.schema !== undefined && request.table !== undefined
      ? [{ schema: request.schema, name: request.table }]
      : []);
  if (tables.length === 0)
    throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', '至少需要选择一张源表。');
  const selected = new Set<string>();
  for (const table of tables) {
    const name = `${identifier(table.schema)}.${identifier(table.name)}`;
    if (selected.has(name))
      throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', '源表不能重复。');
    selected.add(name);
  }
  return tables;
}

async function inspectPostgresReplicationResources(
  client: Pick<PoolClient, 'query'>,
  request: PostgresResourceReadRequest,
  allowMissingPublicationTables = false,
): Promise<ResourceState> {
  const tables = validateRequest(request);
  const identity = await client.query<{
    database_name: string;
    database_oid: string;
    system_identifier: string;
  }>(
    `SELECT current_database() AS database_name, d.oid::text AS database_oid,
            c.system_identifier::text FROM pg_database d CROSS JOIN pg_control_system() c
      WHERE d.datname = current_database()`,
  );
  const database = identity.rows[0];
  if (!database) throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', '数据库身份不可用。');
  const identityPrefix = `${database.system_identifier}:${database.database_oid}:`;
  for (const selected of tables) {
    const table = await client.query<{ oid: string; relreplident: string; primary_key: boolean }>(
      `SELECT c.oid::text, c.relreplident,
      EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary) AS primary_key
     FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
     WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r'`,
      [selected.schema, selected.name],
    );
    if (!table.rows[0])
      throw new DataSourceAdapterError('PG_RESOURCE_NOT_FOUND', '目标源表不存在或不是普通表。');
    if (
      !['f', 'i'].includes(table.rows[0].relreplident) &&
      !(table.rows[0].relreplident === 'd' && table.rows[0].primary_key)
    )
      throw new DataSourceAdapterError(
        'PG_RESOURCE_CONFLICT',
        `源表 ${selected.schema}.${selected.name} 没有可支持 UPDATE/DELETE 的复制标识。`,
      );
  }
  const slots = await client.query<{
    database: string | null;
    plugin: string | null;
    active: boolean;
    slot_type: string;
  }>('SELECT database, plugin, active, slot_type FROM pg_replication_slots WHERE slot_name=$1', [
    request.slotName,
  ]);
  const slot = slots.rows[0];
  if (
    slot &&
    (slot.database !== database.database_name ||
      slot.plugin !== 'pgoutput' ||
      slot.active ||
      slot.slot_type !== 'logical')
  )
    throw new DataSourceAdapterError(
      'PG_RESOURCE_CONFLICT',
      '已有复制槽必须属于当前数据库、使用 pgoutput 且未被占用。',
    );
  const publication = (
    await client.query<PublicationState>(
      `SELECT p.oid::text,p.pubinsert,p.pubupdate,p.pubdelete,p.pubtruncate,
      NOT EXISTS (
        SELECT 1 FROM unnest($2::text[], $3::text[]) selected(schema_name, table_name)
         WHERE NOT EXISTS (SELECT 1 FROM pg_publication_tables t
           WHERE t.pubname=p.pubname AND t.schemaname=selected.schema_name AND t.tablename=selected.table_name)
      ) AS includes_table
      FROM pg_publication p WHERE p.pubname=$1`,
      [
        request.publicationName,
        tables.map((table) => table.schema),
        tables.map((table) => table.name),
      ],
    )
  ).rows[0];
  if (
    publication &&
    ((!publication.includes_table && !allowMissingPublicationTables) ||
      !publication.pubinsert ||
      !publication.pubupdate ||
      !publication.pubdelete ||
      !publication.pubtruncate)
  )
    throw new DataSourceAdapterError(
      'PG_RESOURCE_CONFLICT',
      '已有发布必须包含全部所选源表并启用 INSERT/UPDATE/DELETE/TRUNCATE；平台不会修改已有发布。',
    );
  return {
    databaseName: database.database_name,
    identityPrefix,
    publication,
    slot,
  };
}

function preparedResources(
  state: ResourceState & {
    readonly publication: PublicationState;
    readonly slot: NonNullable<ResourceState['slot']>;
  },
  request: PostgresResourceReadRequest,
  created: { readonly slot: boolean; readonly publication: boolean },
): PreparedPostgresResources {
  return {
    databaseName: state.databaseName,
    slot: {
      name: request.slotName,
      created: created.slot,
      stableIdentity: `${state.identityPrefix}slot:${request.slotName}`,
    },
    publication: {
      name: request.publicationName,
      created: created.publication,
      stableIdentity: `${state.identityPrefix}publication:${state.publication.oid}`,
    },
  };
}

export async function readPostgresReplicationResources(
  pool: Pool,
  request: PostgresResourceReadRequest,
): Promise<PreparedPostgresResources> {
  const client = await pool.connect();
  try {
    const state = await inspectPostgresReplicationResources(client, request);
    if (!state.slot || !state.publication) {
      throw new DataSourceAdapterError(
        'PG_RESOURCE_NOT_FOUND',
        '指定的复制槽或发布不存在，请先准备或恢复资源。',
      );
    }
    return preparedResources(
      { ...state, slot: state.slot, publication: state.publication },
      request,
      { slot: false, publication: false },
    );
  } finally {
    client.release(true);
  }
}

/** Autocommit is intentional: logical slots are not transactional database objects. */
export async function preparePostgresReplicationResources(
  pool: Pool,
  request: PostgresResourcePreparationRequest,
  beforeWrite: (inspection: PostgresResourceInspection) => Promise<void>,
): Promise<PreparedPostgresResources> {
  const publicationSql = identifier(request.publicationName);
  const tables = validateRequest(request);
  const tableSql = tables
    .map((table) => `${identifier(table.schema)}.${identifier(table.name)}`)
    .join(', ');
  const client = await pool.connect();
  const created = { slot: false, publication: false };
  let extendedTables: { schema: string; name: string }[] = [];
  const extending = request.extendPublication !== undefined;
  try {
    // Serialize cooperating preparations, including aliases of the same database.
    await client.query(
      "SELECT pg_advisory_lock(hashtextextended(current_database() || ':scs-resource-prepare', 0))",
    );
    const state = await inspectPostgresReplicationResources(
      client,
      request,
      extending && request.resourceMode === 'PLATFORM_MANAGED',
    );
    if (extending && (!state.slot || !state.publication))
      throw new DataSourceAdapterError(
        'PG_RESOURCE_NOT_FOUND',
        '历史任务的复制槽和发布必须已存在，不会重新创建。',
      );
    if (request.resourceMode === 'SELF_MANAGED' && (!state.slot || !state.publication))
      throw new DataSourceAdapterError(
        'PG_RESOURCE_NOT_FOUND',
        '自行管理模式要求复制槽和发布均已存在。',
      );
    await beforeWrite({
      databaseName: state.databaseName,
      identityPrefix: state.identityPrefix,
      slotIdentity: state.slot ? `${state.identityPrefix}slot:${request.slotName}` : null,
      publicationIdentity: state.publication
        ? `${state.identityPrefix}publication:${state.publication.oid}`
        : null,
    });
    if (extending && request.resourceMode === 'PLATFORM_MANAGED') {
      const current = await inspectPostgresReplicationResources(client, request, true);
      if (
        !current.slot ||
        !current.publication ||
        current.identityPrefix !== state.identityPrefix ||
        current.publication.oid !== state.publication?.oid
      )
        throw new DataSourceAdapterError('PG_RESOURCE_CONFLICT', '复制资源在扩展前已变化。');
      const missing = await client.query<{ schema: string; name: string }>(
        `SELECT selected.schema_name AS schema, selected.table_name AS name
           FROM unnest($2::text[], $3::text[]) selected(schema_name, table_name)
          WHERE NOT EXISTS (SELECT 1 FROM pg_publication_tables t
            WHERE t.pubname=$1 AND t.schemaname=selected.schema_name AND t.tablename=selected.table_name)`,
        [
          request.publicationName,
          tables.map((table) => table.schema),
          tables.map((table) => table.name),
        ],
      );
      const allowed = new Set(
        request.extendPublication!.addedTables.map(
          (table) => `${identifier(table.schema)}.${identifier(table.name)}`,
        ),
      );
      if (
        missing.rows.some(
          (table) => !allowed.has(`${identifier(table.schema)}.${identifier(table.name)}`),
        )
      )
        throw new DataSourceAdapterError(
          'PG_RESOURCE_CONFLICT',
          '原来源表已从 Publication 移除，请核实复制连续性；新增表确认不会自动修复旧表。',
        );
      if (missing.rows.length > 0) {
        const addedSql = missing.rows
          .map((table) => `${identifier(table.schema)}.${identifier(table.name)}`)
          .join(', ');
        await client.query(`ALTER PUBLICATION ${publicationSql} ADD TABLE ${addedSql}`);
        extendedTables = missing.rows;
      }
    }
    // Initial preparation may create resources; an extension never enters these branches.
    if (!state.publication) {
      await client.query(
        `CREATE PUBLICATION ${publicationSql} FOR TABLE ${tableSql} WITH (publish = 'insert, update, delete, truncate')`,
      );
      created.publication = true;
    }
    if (!state.slot) {
      await client.query(
        "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
        [request.slotName],
      );
      created.slot = true;
    }
    const observed =
      created.slot || created.publication || extending
        ? await inspectPostgresReplicationResources(client, request)
        : state;
    if (!observed.slot || !observed.publication)
      throw new Error('POSTGRES_RESOURCES_CREATED_BUT_UNAVAILABLE');
    const result = preparedResources(
      { ...observed, slot: observed.slot, publication: observed.publication },
      request,
      created,
    );
    return extending
      ? { ...result, publication: { ...result.publication, extendedTables } }
      : result;
  } catch (error) {
    if (created.slot || created.publication || extendedTables.length > 0)
      throw new PostgresResourcePreparationError(
        {
          slotName: request.slotName,
          publicationName: request.publicationName,
          created,
          requiresInspection: true,
          ...(extendedTables.length > 0 ? { publicationExtendedTables: extendedTables } : {}),
        },
        { cause: error },
      );
    throw error;
  } finally {
    // The dedicated pool is closed by the adapter; discard the session so session
    // advisory locks cannot leak to a future caller even if the connection failed.
    client.release(true);
  }
}
