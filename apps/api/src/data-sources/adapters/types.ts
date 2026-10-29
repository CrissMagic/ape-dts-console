import type {
  CreateKafkaTopicRequest,
  CreateKafkaTopicResponse,
  DataSourceConfiguration,
  DataSourceIdentity,
  DataSourceType,
  MetadataItem,
  MetadataQuery,
  MysqlCdcInspection,
  TaskProgress,
} from '@scs/contracts';

export type DataSourceAdapterErrorCode =
  | 'PG_RESOURCE_CONFLICT'
  | 'PG_RESOURCE_NOT_FOUND'
  | 'PG_RESOURCE_PREPARATION_FAILED'
  | 'KAFKA_TOPIC_PARAMETERS_INVALID'
  | 'CONNECTION_FAILED'
  | 'INVALID_METADATA_QUERY'
  | 'METADATA_UNSUPPORTED'
  | 'PERMISSION_DENIED'
  | 'POSTGRES_CONTINUITY_IDENTITY_UNAVAILABLE'
  | 'POSTGRES_SLOT_ACTIVE'
  | 'POSTGRES_SLOT_MISSING'
  | 'TIMEOUT';

export class DataSourceAdapterError extends Error {
  constructor(
    readonly code: DataSourceAdapterErrorCode,
    readonly summary: string,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'DataSourceAdapterError';
  }
}

export interface DataSourceAdapterInput {
  readonly configuration: DataSourceConfiguration;
  readonly password: string | null;
}

export interface PostgresResourcePreparationRequest {
  readonly extendPublication?: {
    readonly addedTables: readonly { readonly schema: string; readonly name: string }[];
  };
  readonly slotName: string;
  readonly publicationName: string;
  readonly schema?: string;
  readonly table?: string;
  readonly tables?: readonly { readonly schema: string; readonly name: string }[];
  readonly resourceMode: 'SELF_MANAGED' | 'PLATFORM_MANAGED';
}
export interface PreparedPostgresResources {
  readonly databaseName: string;
  readonly slot: {
    readonly name: string;
    readonly created: boolean;
    readonly stableIdentity: string;
  };
  readonly publication: {
    readonly name: string;
    readonly created: boolean;
    readonly stableIdentity: string;
    readonly extendedTables?: { schema: string; name: string }[];
  };
}
export interface PostgresResourceInspection {
  readonly databaseName: string;
  readonly identityPrefix: string;
  readonly slotIdentity: string | null;
  readonly publicationIdentity: string | null;
}
export class PostgresResourcePreparationError extends DataSourceAdapterError {
  constructor(
    readonly progress: {
      readonly slotName: string;
      readonly publicationName: string;
      readonly created: { readonly slot: boolean; readonly publication: boolean };
      readonly requiresInspection: true;
      readonly publicationExtendedTables?: readonly {
        readonly schema: string;
        readonly name: string;
      }[];
    },
    options?: ErrorOptions,
  ) {
    super(
      'PG_RESOURCE_PREPARATION_FAILED',
      '外部复制资源操作未完整完成或绑定失败，请核实已创建资源后重试。',
      options,
    );
  }
}

export interface PostgresContinuityFact {
  readonly confirmedFlushLsn: string;
  readonly databaseName: string;
  readonly databaseOid: string;
  readonly slotName: string;
  readonly slotPlugin: string;
  readonly slotActive: false;
  readonly systemIdentifier: string;
}

export interface MysqlBinlogFile {
  readonly filename: string;
  readonly sizeBytes: string;
}

export interface DataSourceAdapter {
  readonly type: DataSourceType;
  readPostgresResources?(
    input: DataSourceAdapterInput,
    request: Omit<PostgresResourcePreparationRequest, 'resourceMode'>,
  ): Promise<PreparedPostgresResources>;
  preparePostgresResources?(
    input: DataSourceAdapterInput,
    request: PostgresResourcePreparationRequest,
    beforeWrite: (inspection: PostgresResourceInspection) => Promise<void>,
  ): Promise<PreparedPostgresResources>;

  metadata(input: DataSourceAdapterInput, query: MetadataQuery): Promise<readonly MetadataItem[]>;
  test(input: DataSourceAdapterInput): Promise<DataSourceIdentity>;
  inspectMysqlCdc?(input: DataSourceAdapterInput): Promise<MysqlCdcInspection>;
  readMysqlBinlogs?(input: DataSourceAdapterInput): Promise<readonly MysqlBinlogFile[]>;
  createKafkaTopic?(
    input: DataSourceAdapterInput,
    request: CreateKafkaTopicRequest,
  ): Promise<CreateKafkaTopicResponse>;
  readPostgresContinuity?(
    input: DataSourceAdapterInput,
    slotName: string,
  ): Promise<PostgresContinuityFact>;
  readProgress?(
    input: DataSourceAdapterInput,
    request: { slotName?: string; topic?: string; groupId?: string },
  ): Promise<
    Pick<
      TaskProgress,
      | 'slotStatus'
      | 'unconfirmedWalBytes'
      | 'retainedWalBytes'
      | 'offsetLag'
      | 'replicationLagMilliseconds'
    >
  >;
}

export class DataSourceAdapterRegistry {
  private readonly byType: ReadonlyMap<DataSourceType, DataSourceAdapter>;

  constructor(adapters: readonly DataSourceAdapter[]) {
    this.byType = new Map(adapters.map((adapter) => [adapter.type, adapter]));
  }

  require(type: DataSourceType): DataSourceAdapter {
    const adapter = this.byType.get(type);
    if (adapter === undefined) {
      throw new Error(`DATA_SOURCE_ADAPTER_MISSING:${type}`);
    }
    return adapter;
  }
}

const KAFKA_AUTHORIZATION_ERROR_TYPES: ReadonlySet<string> = new Set([
  'TOPIC_AUTHORIZATION_FAILED',
  'GROUP_AUTHORIZATION_FAILED',
  'CLUSTER_AUTHORIZATION_FAILED',
  'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
  'DELEGATION_TOKEN_AUTHORIZATION_FAILED',
]);

export function toAdapterError(error: unknown): DataSourceAdapterError {
  if (error instanceof DataSourceAdapterError) {
    return error;
  }
  const errorObject = typeof error === 'object' && error !== null ? error : null;
  const code =
    errorObject !== null && 'code' in errorObject && typeof errorObject.code === 'string'
      ? errorObject.code
      : null;
  const type =
    errorObject !== null && 'type' in errorObject && typeof errorObject.type === 'string'
      ? errorObject.type
      : null;
  const message = error instanceof Error ? error.message.toLowerCase() : '';
  if (
    code === '42501' ||
    code === '28000' ||
    code === '1044' ||
    code === '1045' ||
    code === 'ER_ACCESS_DENIED_ERROR' ||
    code === 'ER_DBACCESS_DENIED_ERROR' ||
    code === 'ER_TABLEACCESS_DENIED_ERROR' ||
    code === 'ER_COLUMNACCESS_DENIED_ERROR' ||
    code === 'ER_SPECIFIC_ACCESS_DENIED_ERROR' ||
    message.includes('permission denied') ||
    message.includes('access denied') ||
    (type !== null && KAFKA_AUTHORIZATION_ERROR_TYPES.has(type))
  ) {
    return new DataSourceAdapterError('PERMISSION_DENIED', '当前账号没有执行此操作的权限。', {
      cause: error,
    });
  }
  if (
    code === 'ETIMEDOUT' ||
    code === 'ESOCKETTIMEDOUT' ||
    code === 'PROTOCOL_SEQUENCE_TIMEOUT' ||
    message.includes('timeout') ||
    message.includes('timed out')
  ) {
    return new DataSourceAdapterError('TIMEOUT', '连接或读取在限定时间内未完成。', {
      cause: error,
    });
  }
  return new DataSourceAdapterError('CONNECTION_FAILED', '连接或读取失败。', { cause: error });
}

export function requireMetadataContext(
  value: string | undefined,
  field: 'database' | 'schema' | 'table' | 'topic',
): string {
  if (value === undefined) {
    throw new DataSourceAdapterError('INVALID_METADATA_QUERY', `读取该层级必须明确提供 ${field}。`);
  }
  return value;
}
