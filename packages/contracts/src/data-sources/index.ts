import { z } from 'zod';

import { userSummarySchema } from '../users/index.js';

const dataSourceNameSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(/^[\p{L}\p{N}][\p{L}\p{N} ._:-]*$/u);
const databaseNameSchema = z.string().trim().min(1).max(128);
const hostSchema = z.string().trim().min(1).max(253);
const portSchema = z.number().int().min(1).max(65_535);
const usernameSchema = z.string().trim().min(1).max(128);
const passwordSchema = z.string().min(1).max(4_096);
const rowVersionSchema = z.string().regex(/^[1-9]\d*$/u);

export const createKafkaTopicRequestSchema = z
  .object({
    topic: z
      .string()
      .trim()
      .min(1)
      .max(249)
      .regex(/^[a-zA-Z0-9._-]+$/u)
      .refine((name) => name !== '.' && name !== '..'),
    numPartitions: z.number().int().min(1).max(2_147_483_647),
    replicationFactor: z.number().int().min(1).max(32_767),
    expectedRowVersion: rowVersionSchema,
  })
  .strict();
export const createKafkaTopicResponseSchema = z
  .object({
    topic: z.string(),
    created: z.boolean(),
  })
  .strict();
export type CreateKafkaTopicRequest = z.infer<typeof createKafkaTopicRequestSchema>;
export type CreateKafkaTopicResponse = z.infer<typeof createKafkaTopicResponseSchema>;

export const dataSourceTypeSchema = z.enum(['POSTGRES', 'KAFKA', 'MYSQL', 'DORIS']);
export const dataSourceStateSchema = z.enum(['ACTIVE', 'DELETED']);

export const databaseTlsConfigSchema = z.discriminatedUnion('mode', [
  z.object({ mode: z.literal('REQUIRE') }).strict(),
  z
    .object({
      caCertificate: z
        .string()
        .trim()
        .min(1)
        .max(64 * 1_024),
      mode: z.literal('VERIFY_FULL'),
    })
    .strict(),
  z
    .object({
      mode: z.literal('DISABLE'),
      riskAcknowledged: z.literal(true),
    })
    .strict(),
]);

const postgresConfigurationSchema = z
  .object({
    database: databaseNameSchema,
    host: hostSchema,
    port: portSchema,
    tls: databaseTlsConfigSchema,
    type: z.literal('POSTGRES'),
    username: usernameSchema,
  })
  .strict();

const mysqlCompatibleConfiguration = (type: 'MYSQL' | 'DORIS') =>
  z
    .object({
      database: databaseNameSchema,
      host: hostSchema,
      port: portSchema,
      tls: databaseTlsConfigSchema,
      type: z.literal(type),
      username: usernameSchema,
    })
    .strict();

const kafkaBootstrapServerSchema = z
  .string()
  .trim()
  .min(1)
  .max(253)
  .regex(/^[^\s,:]+:\d{1,5}$/u)
  .superRefine((server, context) => {
    const port = Number(server.slice(server.lastIndexOf(':') + 1));
    if (!Number.isInteger(port) || port < 1 || port > 65_535) {
      context.addIssue({
        code: 'custom',
        message: 'Kafka bootstrap server port must be between 1 and 65535',
      });
    }
  });

const kafkaConfigurationSchema = z
  .object({
    bootstrapServers: z.array(kafkaBootstrapServerSchema).min(1).max(16),
    plaintextRiskAcknowledged: z.literal(true),
    type: z.literal('KAFKA'),
  })
  .strict()
  .superRefine((configuration, context) => {
    if (new Set(configuration.bootstrapServers).size !== configuration.bootstrapServers.length) {
      context.addIssue({
        code: 'custom',
        message: 'bootstrapServers must not contain duplicates',
        path: ['bootstrapServers'],
      });
    }
  });

export const dataSourceConfigurationSchema = z.discriminatedUnion('type', [
  postgresConfigurationSchema,
  kafkaConfigurationSchema,
  mysqlCompatibleConfiguration('MYSQL'),
  mysqlCompatibleConfiguration('DORIS'),
]);

const setDatabaseCredentialsSchema = z
  .object({
    mode: z.literal('SET'),
    password: passwordSchema,
  })
  .strict();
const replaceDatabaseCredentialsSchema = z
  .object({
    mode: z.literal('REPLACE'),
    password: passwordSchema,
  })
  .strict();
const retainDatabaseCredentialsSchema = z.object({ mode: z.literal('RETAIN') }).strict();
const noCredentialsSchema = z.object({ mode: z.literal('NONE') }).strict();

const createPostgresDataSourceSchema = z
  .object({
    configuration: postgresConfigurationSchema,
    credentials: setDatabaseCredentialsSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const createKafkaDataSourceSchema = z
  .object({
    configuration: kafkaConfigurationSchema,
    credentials: noCredentialsSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const createMysqlDataSourceSchema = z
  .object({
    configuration: mysqlCompatibleConfiguration('MYSQL'),
    credentials: setDatabaseCredentialsSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const createDorisDataSourceSchema = z
  .object({
    configuration: mysqlCompatibleConfiguration('DORIS'),
    credentials: setDatabaseCredentialsSchema,
    name: dataSourceNameSchema,
  })
  .strict();

export const createDataSourceRequestSchema = z.union([
  createPostgresDataSourceSchema,
  createKafkaDataSourceSchema,
  createMysqlDataSourceSchema,
  createDorisDataSourceSchema,
]);

const updatePostgresDataSourceSchema = z
  .object({
    configuration: postgresConfigurationSchema,
    credentials: z.union([retainDatabaseCredentialsSchema, replaceDatabaseCredentialsSchema]),
    expectedRowVersion: rowVersionSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const updateKafkaDataSourceSchema = z
  .object({
    configuration: kafkaConfigurationSchema,
    credentials: noCredentialsSchema,
    expectedRowVersion: rowVersionSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const updateMysqlDataSourceSchema = z
  .object({
    configuration: mysqlCompatibleConfiguration('MYSQL'),
    credentials: z.union([retainDatabaseCredentialsSchema, replaceDatabaseCredentialsSchema]),
    expectedRowVersion: rowVersionSchema,
    name: dataSourceNameSchema,
  })
  .strict();
const updateDorisDataSourceSchema = z
  .object({
    configuration: mysqlCompatibleConfiguration('DORIS'),
    credentials: z.union([retainDatabaseCredentialsSchema, replaceDatabaseCredentialsSchema]),
    expectedRowVersion: rowVersionSchema,
    name: dataSourceNameSchema,
  })
  .strict();

export const updateDataSourceRequestSchema = z.union([
  updatePostgresDataSourceSchema,
  updateKafkaDataSourceSchema,
  updateMysqlDataSourceSchema,
  updateDorisDataSourceSchema,
]);

export const deleteDataSourceRequestSchema = z
  .object({ expectedRowVersion: rowVersionSchema })
  .strict();

export const copyDataSourceRequestSchema = z
  .object({
    name: dataSourceNameSchema,
    expectedRowVersion: rowVersionSchema,
  })
  .strict();
export type CopyDataSourceRequest = z.infer<typeof copyDataSourceRequestSchema>;

export const dataSourceIdentifierParametersSchema = z.object({ dataSourceId: z.uuid() }).strict();

export const dataSourceSummarySchema = z
  .object({
    credentialsRequired: z.boolean().optional(),
    configuration: dataSourceConfigurationSchema,
    createdAt: z.iso.datetime({ offset: true }),
    deletedAt: z.iso.datetime({ offset: true }).nullable(),
    id: z.uuid(),
    name: dataSourceNameSchema,
    rowVersion: rowVersionSchema,
    state: dataSourceStateSchema,
    updatedAt: z.iso.datetime({ offset: true }),
  })
  .strict();

export const dataSourceListResponseSchema = z
  .object({ dataSources: z.array(dataSourceSummarySchema) })
  .strict();

export const dataSourceCatalogQuerySchema = z
  .object({
    page: z.coerce.number().int().min(1).max(1_000_000).default(1),
    pageSize: z.coerce
      .number()
      .pipe(z.union([z.literal(10), z.literal(20), z.literal(50)]))
      .default(10),
    q: z.string().trim().max(200).default(''),
    type: dataSourceTypeSchema.optional(),
  })
  .strict();

export const dataSourceCatalogResponseSchema = z
  .object({
    dataSources: z.array(dataSourceSummarySchema),
    total: z.number().int().nonnegative(),
    page: z.number().int().positive(),
    pageSize: z.union([z.literal(10), z.literal(20), z.literal(50)]),
  })
  .strict();

export type DataSourceCatalogQuery = z.infer<typeof dataSourceCatalogQuerySchema>;
export type DataSourceCatalogResponse = z.infer<typeof dataSourceCatalogResponseSchema>;

export const dataSourceGrantSchema = z
  .object({
    dataSourceId: z.uuid(),
    grantedAt: z.iso.datetime({ offset: true }),
    grantedBy: userSummarySchema,
    user: userSummarySchema,
  })
  .strict();

export const dataSourceGrantListResponseSchema = z
  .object({ grants: z.array(dataSourceGrantSchema) })
  .strict();

export const createDataSourceGrantRequestSchema = z.object({ userId: z.uuid() }).strict();
export const dataSourceGrantParametersSchema = z
  .object({ dataSourceId: z.uuid(), userId: z.uuid() })
  .strict();

export const connectionTestPurposeSchema = z.enum(['MANUAL', 'PREFLIGHT']);
export const connectionTestStateSchema = z.enum(['SUCCEEDED', 'FAILED']);
export const dataSourceIdentitySchema = z
  .object({
    kind: z.enum(['KAFKA_CLUSTER', 'POSTGRES_DATABASE', 'MYSQL_DATABASE', 'UNAVAILABLE']),
    serverVersion: z.string().min(1).max(256).nullable(),
    value: z.string().min(1).max(512).nullable(),
  })
  .strict();

export const mysqlCdcInspectionSchema = z
  .object({
    identity: dataSourceIdentitySchema,
    binlog: z
      .object({
        enabled: z.boolean().nullable(),
        format: z.string().nullable(),
        rowImage: z.string().nullable(),
        serverId: z.number().int().min(1).max(4_294_967_295).nullable(),
        gtidMode: z.string().nullable(),
      })
      .strict(),
    position: z
      .object({
        filename: z.string().min(1).max(255),
        position: z.number().int().min(4).max(4_294_967_295),
      })
      .strict()
      .nullable(),
    checks: z.array(
      z
        .object({
          key: z.enum([
            'MYSQL_VERSION',
            'MYSQL_IDENTITY',
            'SERVER_ID',
            'BINLOG_ENABLED',
            'BINLOG_FORMAT',
            'BINLOG_ROW_IMAGE',
            'BINLOG_POSITION',
          ]),
          state: z.enum(['PASSED', 'FAILED']),
          summary: z.string().min(1).max(512),
        })
        .strict(),
    ),
  })
  .strict();

export const mysqlCdcCheckRequestSchema = z
  .object({ expectedRowVersion: rowVersionSchema })
  .strict();

export const connectionTestResponseSchema = z
  .object({
    completedAt: z.iso.datetime({ offset: true }),
    dataSourceId: z.uuid(),
    durationMs: z.number().int().nonnegative(),
    errorCode: z.string().min(1).max(128).nullable(),
    errorSummary: z.string().min(1).max(512).nullable(),
    id: z.uuid(),
    identity: dataSourceIdentitySchema.nullable(),
    purpose: connectionTestPurposeSchema,
    startedAt: z.iso.datetime({ offset: true }),
    state: connectionTestStateSchema,
  })
  .strict();

export const metadataResourceSchema = z.enum([
  'DATABASES',
  'SCHEMAS',
  'TABLES',
  'COLUMNS',
  'TOPICS',
  'PARTITIONS',
]);

export const metadataQuerySchema = z
  .object({
    database: databaseNameSchema.optional(),
    resource: metadataResourceSchema,
    schema: databaseNameSchema.optional(),
    table: databaseNameSchema.optional(),
    topic: z.string().trim().min(1).max(249).optional(),
  })
  .strict();

const metadataAttributesSchema = z.record(z.string(), z.string());
export const metadataItemSchema = z
  .object({
    attributes: metadataAttributesSchema,
    identity: z.string().min(1).max(512).nullable(),
    name: z.string().min(1).max(512),
  })
  .strict();

const metadataBaseSchema = z.object({
  attemptedAt: z.iso.datetime({ offset: true }),
  dataSourceId: z.uuid(),
  resource: metadataResourceSchema,
});

export const metadataResponseSchema = z.discriminatedUnion('state', [
  metadataBaseSchema
    .extend({
      items: z.array(metadataItemSchema).min(1),
      refreshedAt: z.iso.datetime({ offset: true }),
      state: z.literal('SUCCESS'),
    })
    .strict(),
  metadataBaseSchema
    .extend({
      refreshedAt: z.iso.datetime({ offset: true }),
      state: z.literal('EMPTY'),
    })
    .strict(),
  metadataBaseSchema
    .extend({
      errorCode: z.string().min(1).max(128),
      errorSummary: z.string().min(1).max(512),
      items: z.array(metadataItemSchema).min(1),
      refreshedAt: z.iso.datetime({ offset: true }),
      state: z.literal('PARTIAL'),
    })
    .strict(),
  metadataBaseSchema
    .extend({
      errorCode: z.string().min(1).max(128),
      errorSummary: z.string().min(1).max(512),
      state: z.literal('PERMISSION_DENIED'),
    })
    .strict(),
  metadataBaseSchema
    .extend({
      errorCode: z.string().min(1).max(128),
      errorSummary: z.string().min(1).max(512),
      state: z.literal('TIMEOUT'),
    })
    .strict(),
  metadataBaseSchema
    .extend({
      errorCode: z.string().min(1).max(128),
      errorSummary: z.string().min(1).max(512),
      state: z.literal('ERROR'),
    })
    .strict(),
]);

export type ConnectionTestResponse = z.infer<typeof connectionTestResponseSchema>;
export type ConnectionTestPurpose = z.infer<typeof connectionTestPurposeSchema>;
export type ConnectionTestState = z.infer<typeof connectionTestStateSchema>;
export type CreateDataSourceGrantRequest = z.infer<typeof createDataSourceGrantRequestSchema>;
export type CreateDataSourceRequest = z.infer<typeof createDataSourceRequestSchema>;
export type DataSourceConfiguration = z.infer<typeof dataSourceConfigurationSchema>;
export type DataSourceGrant = z.infer<typeof dataSourceGrantSchema>;
export type DataSourceIdentity = z.infer<typeof dataSourceIdentitySchema>;
export type DataSourceState = z.infer<typeof dataSourceStateSchema>;
export type DataSourceSummary = z.infer<typeof dataSourceSummarySchema>;
export type DataSourceType = z.infer<typeof dataSourceTypeSchema>;
export type MetadataQuery = z.infer<typeof metadataQuerySchema>;
export type MetadataItem = z.infer<typeof metadataItemSchema>;
export type MetadataResponse = z.infer<typeof metadataResponseSchema>;
export type MetadataResource = z.infer<typeof metadataResourceSchema>;
export type MysqlCdcInspection = z.infer<typeof mysqlCdcInspectionSchema>;
export type UpdateDataSourceRequest = z.infer<typeof updateDataSourceRequestSchema>;
