import { z } from 'zod';

const safePgComponent = z
  .string()
  .trim()
  .min(1)
  .max(63)
  .regex(/^[\p{L}\p{N}_$-]+$/u);
const slotName = z.string().regex(/^[a-z0-9_]{1,63}$/u);
const publicationName = z
  .string()
  .min(1)
  .max(63)
  .regex(/^[A-Za-z_][A-Za-z0-9_]*$/u);
const kafkaName = z
  .string()
  .min(1)
  .max(249)
  .regex(/^[A-Za-z0-9._-]+$/u);
const positiveInteger = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);

const commonTuning = {
  batchSize: positiveInteger,
  bufferSize: positiveInteger,
  checkpointIntervalSeconds: positiveInteger,
};

const topicCreationSchema = z
  .object({
    numPartitions: z.number().int().min(1).max(2_147_483_647),
    replicationFactor: z.number().int().min(1).max(32_767),
  })
  .strict();

const rdbMessageOptions = {
  messageFormat: z.enum(['avro', 'json']).optional(),
  jsonTemplate: z.enum(['standard', 'cloudcanal']).optional(),
  withFieldDefs: z.boolean().optional(),
};

function validateMessageFormat(
  target: { jsonTemplate?: string | undefined; messageFormat?: string | undefined },
  context: z.RefinementCtx,
): void {
  if (target.jsonTemplate !== undefined && target.messageFormat !== 'json') {
    context.addIssue({
      code: 'custom',
      message: 'JSON template requires JSON message format',
      path: ['jsonTemplate'],
    });
  }
}

export const rdbKafkaTargetSchema = z
  .object({ dataSourceId: z.uuid(), ...rdbMessageOptions })
  .strict()
  .superRefine(validateMessageFormat);

export const rdbKafkaTuningSchema = z
  .object({ ...commonTuning, requiredAcks: z.enum(['one', 'all']) })
  .strict();

// Optional platform policy: absent on existing revisions, never passed to ape-dts INI.
const kafkaTargetSchema = z
  .object({
    dataSourceId: z.uuid(),
    topic: kafkaName,
    topicCreation: topicCreationSchema.optional(),
  })
  .strict()
  .superRefine((target, context) => {
    if (target.topicCreation !== undefined && (target.topic === '.' || target.topic === '..')) {
      context.addIssue({ code: 'custom', message: 'Invalid Kafka topic name', path: ['topic'] });
    }
  });

export const pgResourceModeSchema = z.enum(['SELF_MANAGED', 'PLATFORM_MANAGED', 'APE_DTS_MANAGED']);

const pgToKafkaTaskConfigV1Schema = z
  .object({
    kind: z.literal('PG_TO_KAFKA'),
    schemaVersion: z.literal(1),
    source: z
      .object({
        dataSourceId: z.uuid(),
        includeSnapshot: z.boolean(),
        publicationName,
        resourceMode: pgResourceModeSchema,
        slotName,
        table: z.object({ name: safePgComponent, schema: safePgComponent }).strict(),
      })
      .strict(),
    target: kafkaTargetSchema.safeExtend(rdbMessageOptions).superRefine(validateMessageFormat),
    tuning: rdbKafkaTuningSchema,
  })
  .strict()
  .superRefine((config, context) => {
    if (config.source.resourceMode === 'APE_DTS_MANAGED' && config.source.includeSnapshot) {
      context.addIssue({
        code: 'custom',
        message: 'APE_DTS_MANAGED does not support snapshot',
        path: ['source', 'includeSnapshot'],
      });
    }
  });

const kafkaRawForwardTaskConfigV1Schema = z
  .object({
    kind: z.literal('KAFKA_RAW_FORWARD'),
    schemaVersion: z.literal(1),
    source: z
      .object({
        dataSourceId: z.uuid(),
        groupId: kafkaName.max(255),
        topic: kafkaName,
      })
      .strict(),
    target: kafkaTargetSchema,
    tuning: z
      .object({
        ...commonTuning,
        ackIntervalSeconds: positiveInteger,
        requiredAcks: z.enum(['one', 'all']),
      })
      .strict(),
  })
  .strict();

const mappingTopicName = kafkaName.refine((topic) => topic !== '.' && topic !== '..', {
  message: 'Invalid Kafka topic name',
});

export const mappingTargetSchema = z
  .object({ topic: mappingTopicName, topicCreation: topicCreationSchema.optional() })
  .strict();

export type TaskTargetTopic = z.infer<typeof mappingTargetSchema>;
export interface PgTaskMapping {
  readonly source: { readonly schema: string; readonly name: string };
  readonly targets: readonly TaskTargetTopic[];
}
export interface KafkaTaskMapping {
  readonly source: string;
  readonly targets: readonly TaskTargetTopic[];
}

export function validateMappings(
  config: { mappings: readonly (PgTaskMapping | KafkaTaskMapping)[] },
  context: z.RefinementCtx,
): void {
  const sources = new Set<string>();
  const topicPolicies = new Map<string, TaskTargetTopic['topicCreation']>();
  config.mappings.forEach((mapping, row) => {
    const source =
      typeof mapping.source === 'string'
        ? mapping.source
        : `${mapping.source.schema}.${mapping.source.name}`;
    if (sources.has(source)) {
      context.addIssue({
        code: 'custom',
        message: 'Duplicate mapping source',
        path: ['mappings', row, 'source'],
      });
    }
    sources.add(source);
    const targets = new Set<string>();
    mapping.targets.forEach((target, column) => {
      const path = ['mappings', row, 'targets', column];
      if (targets.has(target.topic)) {
        context.addIssue({ code: 'custom', message: 'Duplicate mapping target', path });
      }
      targets.add(target.topic);
      const previous = topicPolicies.get(target.topic);
      if (
        topicPolicies.has(target.topic) &&
        (previous?.numPartitions !== target.topicCreation?.numPartitions ||
          previous?.replicationFactor !== target.topicCreation?.replicationFactor)
      ) {
        context.addIssue({ code: 'custom', message: 'Conflicting topic creation policies', path });
      }
      topicPolicies.set(target.topic, target.topicCreation);
    });
  });
}

const pgToKafkaTaskConfigV2Schema = z
  .object({
    kind: z.literal('PG_TO_KAFKA'),
    schemaVersion: z.literal(2),
    source: pgToKafkaTaskConfigV1Schema.shape.source.omit({ table: true }),
    target: rdbKafkaTargetSchema,
    mappings: z
      .array(
        z
          .object({
            source: pgToKafkaTaskConfigV1Schema.shape.source.shape.table,
            targets: z.array(mappingTargetSchema).min(1),
          })
          .strict(),
      )
      .min(1),
    tuning: pgToKafkaTaskConfigV1Schema.shape.tuning,
  })
  .strict()
  .superRefine(validateMappings)
  .superRefine((config, context) => {
    if (config.source.resourceMode === 'APE_DTS_MANAGED' && config.source.includeSnapshot) {
      context.addIssue({
        code: 'custom',
        message: 'APE_DTS_MANAGED does not support snapshot',
        path: ['source', 'includeSnapshot'],
      });
    }
  });

const kafkaRawForwardTaskConfigV2Schema = z
  .object({
    kind: z.literal('KAFKA_RAW_FORWARD'),
    schemaVersion: z.literal(2),
    source: kafkaRawForwardTaskConfigV1Schema.shape.source.omit({ topic: true }),
    target: z.object({ dataSourceId: z.uuid() }).strict(),
    mappings: z
      .array(
        z
          .object({
            source: mappingTopicName,
            targets: z.array(mappingTargetSchema).min(1),
          })
          .strict(),
      )
      .min(1),
    tuning: kafkaRawForwardTaskConfigV1Schema.shape.tuning,
  })
  .strict()
  .superRefine(validateMappings);

export const pgToKafkaTaskConfigSchema = z.union([
  pgToKafkaTaskConfigV1Schema,
  pgToKafkaTaskConfigV2Schema,
]);
export const kafkaRawForwardTaskConfigSchema = z.union([
  kafkaRawForwardTaskConfigV1Schema,
  kafkaRawForwardTaskConfigV2Schema,
]);

const mysqlComponent = z
  .string()
  .trim()
  .min(1)
  .max(64)
  .regex(/^[\p{L}\p{N}_$-]+$/u);

// MySQL's schema is its database name, matching the engine's schema.table routing.
export const mysqlToKafkaTaskConfigSchema = z
  .object({
    kind: z.literal('MYSQL_TO_KAFKA'),
    schemaVersion: z.literal(2),
    source: z.object({ dataSourceId: z.uuid(), includeSnapshot: z.boolean() }).strict(),
    target: rdbKafkaTargetSchema,
    mappings: z
      .array(
        z
          .object({
            source: z.object({ schema: mysqlComponent, name: mysqlComponent }).strict(),
            targets: z.array(mappingTargetSchema).min(1),
          })
          .strict(),
      )
      .min(1),
    tuning: rdbKafkaTuningSchema,
  })
  .strict()
  .superRefine(validateMappings);

export type MysqlToKafkaTaskConfig = z.infer<typeof mysqlToKafkaTaskConfigSchema>;

export const platformTaskConfigSchema = z.union([
  pgToKafkaTaskConfigSchema,
  kafkaRawForwardTaskConfigSchema,
  mysqlToKafkaTaskConfigSchema,
]);

export type PgToKafkaTaskConfig = z.infer<typeof pgToKafkaTaskConfigSchema>;
export type KafkaRawForwardTaskConfig = z.infer<typeof kafkaRawForwardTaskConfigSchema>;
export type PlatformTaskConfig = z.infer<typeof platformTaskConfigSchema>;

export function pgTaskMappings(config: PgToKafkaTaskConfig): readonly PgTaskMapping[] {
  return config.schemaVersion === 2
    ? config.mappings
    : [
        {
          source: config.source.table,
          targets: [
            {
              topic: config.target.topic,
              ...(config.target.topicCreation === undefined
                ? {}
                : { topicCreation: config.target.topicCreation }),
            },
          ],
        },
      ];
}

/** Compare physical table names exactly: quoted PostgreSQL identifiers are case-sensitive. */
export function pgTableAddition(
  previous: unknown,
  candidate: unknown,
): {
  addedTables: readonly { schema: string; name: string }[];
  eligible: boolean;
} {
  const old = pgToKafkaTaskConfigSchema.safeParse(previous);
  const next = pgToKafkaTaskConfigSchema.safeParse(candidate);
  if (!old.success || !next.success) return { addedTables: [], eligible: false };
  const tableKey = (table: { schema: string; name: string }) =>
    JSON.stringify([table.schema, table.name]);
  const before = pgTaskMappings(old.data);
  const after = pgTaskMappings(next.data);
  const oldNames = new Set(before.map((row) => tableKey(row.source)));
  const addedTables = after
    .filter((row) => !oldNames.has(tableKey(row.source)))
    .map((row) => row.source);
  const eligible =
    addedTables.length > 0 &&
    !old.data.source.includeSnapshot &&
    old.data.source.dataSourceId === next.data.source.dataSourceId &&
    old.data.target.dataSourceId === next.data.target.dataSourceId &&
    old.data.source.resourceMode === next.data.source.resourceMode &&
    old.data.source.slotName === next.data.source.slotName &&
    old.data.source.publicationName === next.data.source.publicationName &&
    before.every((row) => {
      const matching = after.find((item) => tableKey(item.source) === tableKey(row.source));
      return (
        matching !== undefined &&
        matching.targets.length === row.targets.length &&
        row.targets.every((target) => matching.targets.some((item) => item.topic === target.topic))
      );
    });
  return { addedTables, eligible };
}

export function kafkaTaskMappings(config: KafkaRawForwardTaskConfig): readonly KafkaTaskMapping[] {
  return config.schemaVersion === 2
    ? config.mappings
    : [
        {
          source: config.source.topic,
          targets: [
            {
              topic: config.target.topic,
              ...(config.target.topicCreation === undefined
                ? {}
                : { topicCreation: config.target.topicCreation }),
            },
          ],
        },
      ];
}

export function taskTargetTopics(config: PlatformTaskConfig): readonly TaskTargetTopic[] {
  const mappings =
    config.kind === 'PG_TO_KAFKA'
      ? pgTaskMappings(config)
      : config.kind === 'MYSQL_TO_KAFKA'
        ? config.mappings
        : kafkaTaskMappings(config);
  return [
    ...new Map(
      mappings.flatMap((mapping) => mapping.targets).map((target) => [target.topic, target]),
    ).values(),
  ];
}

export function parsePlatformTaskConfig(value: unknown): PlatformTaskConfig {
  return platformTaskConfigSchema.parse(value);
}
