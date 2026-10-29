import type { TaskType } from '@scs/contracts';

export interface TaskMappingTarget {
  readonly topic: string;
  readonly autoCreateTopic: boolean;
  readonly numPartitions: number | null;
  readonly replicationFactor: number | null;
}

export interface TaskMappingDraft {
  readonly sourceSchema: string;
  readonly sourceTable: string;
  readonly sourceTopic: string;
  readonly targets: readonly TaskMappingTarget[];
}

export function emptyTaskMapping(): TaskMappingDraft {
  return { sourceSchema: '', sourceTable: '', sourceTopic: '', targets: [] };
}

export function newMappingTarget(topic: string): TaskMappingTarget {
  return { topic, autoCreateTopic: false, numPartitions: null, replicationFactor: null };
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : '';
}
function positive(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : null;
}

export function readTaskMappings(value: unknown): TaskMappingDraft[] {
  const config = record(value);
  const source = record(config.source);
  const target = record(config.target);
  const rows =
    config.schemaVersion === 2
      ? Array.isArray(config.mappings)
        ? config.mappings
        : []
      : [
          {
            source:
              config.kind === 'PG_TO_KAFKA' || source.table !== undefined
                ? source.table
                : source.topic,
            targets: text(target.topic) ? [target] : [],
          },
        ];
  return rows.map((value) => {
    const row = record(value);
    const table = record(row.source);
    return {
      sourceSchema: text(table.schema),
      sourceTable: text(table.name),
      sourceTopic: text(row.source),
      targets: (Array.isArray(row.targets) ? row.targets : []).map((value) => {
        const item = record(value);
        const creation = record(item.topicCreation);
        return {
          topic: text(item.topic),
          autoCreateTopic: item.topicCreation !== undefined,
          numPartitions: positive(creation.numPartitions),
          replicationFactor: positive(creation.replicationFactor),
        };
      }),
    };
  });
}

export function mappingSourceLabel(row: TaskMappingDraft, type: TaskType): string {
  return type !== 'KAFKA_KAFKA'
    ? [row.sourceSchema, row.sourceTable].filter(Boolean).join('.')
    : row.sourceTopic;
}

export function mappingTargets(rows: readonly TaskMappingDraft[]): TaskMappingTarget[] {
  return [
    ...new Map(rows.flatMap((row) => row.targets).map((target) => [target.topic, target])).values(),
  ];
}

export function serializeTaskMappings(
  rows: readonly TaskMappingDraft[],
  type: TaskType,
  complete = false,
) {
  if (complete && rows.length === 0) throw new Error('请至少添加一个来源。');
  const sources = new Set<string>();
  const policies = new Map<string, string>();
  const validate = (value: string, label: string, pattern: RegExp) => {
    const normalized = value.trim();
    if (
      (complete && !normalized) ||
      (normalized && !pattern.test(normalized)) ||
      normalized === '.' ||
      normalized === '..'
    ) {
      throw new Error(`${label}${normalized ? '格式不正确' : '不能为空'}。`);
    }
    return normalized;
  };
  return rows.map((row, index) => {
    const label = `第 ${index + 1} 行`;
    const source =
      type !== 'KAFKA_KAFKA'
        ? {
            schema: validate(
              row.sourceSchema,
              `${label}${type === 'MYSQL_KAFKA' ? '来源数据库' : '源 Schema'}`,
              type === 'MYSQL_KAFKA' ? /^[\p{L}\p{N}_$-]{1,64}$/u : /^[\p{L}\p{N}_$-]{1,63}$/u,
            ),
            name: validate(
              row.sourceTable,
              `${label}源表`,
              type === 'MYSQL_KAFKA' ? /^[\p{L}\p{N}_$-]{1,64}$/u : /^[\p{L}\p{N}_$-]{1,63}$/u,
            ),
          }
        : validate(row.sourceTopic, `${label}源 Topic`, /^[A-Za-z0-9._-]{1,249}$/u);
    const key = JSON.stringify(source);
    if (sources.has(key)) throw new Error('来源不能重复。');
    sources.add(key);
    if (complete && row.targets.length === 0) throw new Error(`${label}请至少选择一个目标 Topic。`);
    const names = new Set<string>();
    const targets = row.targets.map((target) => {
      const topic = validate(target.topic, '目标 Topic', /^[A-Za-z0-9._-]{1,249}$/u);
      if (names.has(topic)) throw new Error('同一来源的目标 Topic 不能重复。');
      names.add(topic);
      for (const [field, name] of [
        [target.numPartitions, '分区数'],
        [target.replicationFactor, '副本数'],
      ] as const) {
        if (target.autoCreateTopic && (field === null || !Number.isSafeInteger(field) || field < 1))
          throw new Error(`${name}必须是正整数。`);
      }
      const topicCreation = target.autoCreateTopic
        ? { numPartitions: target.numPartitions!, replicationFactor: target.replicationFactor! }
        : undefined;
      const policy = JSON.stringify(topicCreation) ?? 'existing';
      if (policies.has(topic) && policies.get(topic) !== policy)
        throw new Error('相同目标 Topic 的创建策略必须一致。');
      policies.set(topic, policy);
      return { topic, ...(topicCreation === undefined ? {} : { topicCreation }) };
    });
    return { source, targets };
  });
}
