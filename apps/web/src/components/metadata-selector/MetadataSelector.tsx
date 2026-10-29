import { useEffect, useMemo, useRef, useState } from 'react';
import type {
  DataSourceSummary,
  MetadataItem,
  MetadataQuery,
  MetadataResource,
  MetadataResponse,
} from '@scs/contracts';

import { dataSourcesApi, PlatformApiError } from '../../api/platform-api';
import { formatDateTime } from '../../lib/date-time';

type SelectorPhase = 'NOT_REQUESTED' | 'LOADING' | 'STALE' | MetadataResponse['state'];

export interface MetadataSelection {
  readonly identity: string | null;
  readonly name: string;
}

const resourcesByType: Readonly<
  Record<DataSourceSummary['configuration']['type'], readonly MetadataResource[]>
> = {
  DORIS: ['DATABASES', 'TABLES', 'COLUMNS'],
  KAFKA: ['TOPICS', 'PARTITIONS'],
  MYSQL: ['DATABASES', 'TABLES', 'COLUMNS'],
  POSTGRES: ['DATABASES', 'SCHEMAS', 'TABLES', 'COLUMNS'],
};

function resourceLabel(resource: MetadataResource): string {
  switch (resource) {
    case 'DATABASES':
      return '数据库';
    case 'SCHEMAS':
      return 'Schema';
    case 'TABLES':
      return '表';
    case 'COLUMNS':
      return '字段';
    case 'TOPICS':
      return 'Topic';
    case 'PARTITIONS':
      return 'Partition';
  }
}

function phaseText(
  phase: SelectorPhase,
  response: MetadataResponse | null,
  error: string | null,
): string {
  const refreshedAt = response !== null && 'refreshedAt' in response ? response.refreshedAt : null;
  const errorSummary =
    response !== null && 'errorSummary' in response ? response.errorSummary : null;
  switch (phase) {
    case 'NOT_REQUESTED':
      return '尚未读取元数据。首次加载不会用示例数据或历史缓存替代。';
    case 'LOADING':
      return '正在从当前数据源实时读取元数据。';
    case 'STALE':
      return '查询条件已改变，当前结果已过期；请手动刷新后再选择。';
    case 'SUCCESS':
      return `读取成功，最后刷新于 ${formatDateTime(refreshedAt)}。`;
    case 'EMPTY':
      return `读取成功，但当前范围内没有可选${resourceLabel(response?.resource ?? 'TABLES')}。`;
    case 'PARTIAL':
      return `部分读取成功：${errorSummary ?? '部分层级读取失败。'}`;
    case 'PERMISSION_DENIED':
      return `数据源账号无权读取：${errorSummary ?? '权限不足。'}`;
    case 'TIMEOUT':
      return `读取超时：${errorSummary ?? '请检查网络或服务负载后重试。'}`;
    case 'ERROR':
      return error ?? `读取失败：${errorSummary ?? '未知错误。'}`;
  }
}

function itemsOf(response: MetadataResponse | null): readonly MetadataItem[] {
  return response !== null && 'items' in response ? response.items : [];
}

function requiresDatabase(resource: MetadataResource): boolean {
  return resource === 'SCHEMAS' || resource === 'TABLES' || resource === 'COLUMNS';
}

function buildQuery(
  resource: MetadataResource,
  database: string,
  schema: string,
  table: string,
  topic: string,
): MetadataQuery {
  return {
    ...(database.trim().length === 0 ? {} : { database: database.trim() }),
    ...(schema.trim().length === 0 ? {} : { schema: schema.trim() }),
    ...(table.trim().length === 0 ? {} : { table: table.trim() }),
    ...(topic.trim().length === 0 ? {} : { topic: topic.trim() }),
    resource,
  };
}

export function MetadataSelector({
  dataSource,
  onSelectionChange,
  onSelectionInvalidated,
}: {
  dataSource: DataSourceSummary;
  onSelectionChange?: (selection: MetadataSelection | null) => void;
  onSelectionInvalidated?: (selection: MetadataSelection) => void;
}) {
  const resources = resourcesByType[dataSource.configuration.type];
  const [resource, setResource] = useState<MetadataResource>(resources[0]!);
  const [database, setDatabase] = useState('');
  const [schema, setSchema] = useState('');
  const [table, setTable] = useState('');
  const [topic, setTopic] = useState('');
  const [phase, setPhase] = useState<SelectorPhase>('NOT_REQUESTED');
  const [response, setResponse] = useState<MetadataResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selection, setSelection] = useState<MetadataSelection | null>(null);
  const selectionChangeRef = useRef(onSelectionChange);
  const requestSequenceRef = useRef(0);

  useEffect(() => {
    selectionChangeRef.current = onSelectionChange;
  }, [onSelectionChange]);

  const query = useMemo(
    () => buildQuery(resource, database, schema, table, topic),
    [database, resource, schema, table, topic],
  );

  useEffect(() => {
    requestSequenceRef.current += 1;
    const nextResource = resourcesByType[dataSource.configuration.type][0]!;
    setResource(nextResource);
    setDatabase('');
    setSchema('');
    setTable('');
    setTopic('');
    setPhase('NOT_REQUESTED');
    setResponse(null);
    setError(null);
    setSelection(null);
    selectionChangeRef.current?.(null);
  }, [dataSource.configuration.type, dataSource.id, dataSource.rowVersion]);

  function markStale() {
    requestSequenceRef.current += 1;
    setPhase((current) => (current === 'NOT_REQUESTED' ? 'NOT_REQUESTED' : 'STALE'));
    setResponse(null);
    setError(null);
  }

  function changeSelection(item: MetadataItem) {
    const next = { identity: item.identity, name: item.name };
    setSelection(next);
    onSelectionChange?.(next);
  }

  function clearSelectionAfterRefresh(previous: MetadataSelection, items: readonly MetadataItem[]) {
    const matching = items.find((item) => item.name === previous.name);
    if (
      matching === undefined ||
      previous.identity === null ||
      matching.identity !== previous.identity
    ) {
      setSelection(null);
      onSelectionChange?.(null);
      onSelectionInvalidated?.(previous);
    }
  }

  function refresh() {
    const previous = selection;
    const requestSequence = requestSequenceRef.current + 1;
    requestSequenceRef.current = requestSequence;
    setPhase('LOADING');
    setError(null);
    void dataSourcesApi
      .metadata(dataSource, query)
      .then((next) => {
        if (requestSequence !== requestSequenceRef.current) {
          return;
        }
        const items = itemsOf(next);
        if (
          previous !== null &&
          (next.state === 'SUCCESS' || next.state === 'PARTIAL' || next.state === 'EMPTY')
        ) {
          clearSelectionAfterRefresh(previous, items);
        }
        setResponse(next);
        setPhase(next.state);
      })
      .catch((caught: unknown) => {
        if (requestSequence !== requestSequenceRef.current) {
          return;
        }
        setResponse(null);
        setPhase('ERROR');
        setError(
          caught instanceof PlatformApiError ? `读取失败：${caught.code}` : '读取元数据失败。',
        );
      });
  }

  const databaseRequired = requiresDatabase(resource);
  const schemaRequired =
    dataSource.configuration.type === 'POSTGRES' &&
    (resource === 'TABLES' || resource === 'COLUMNS');
  const tableRequired = resource === 'COLUMNS';
  const topicRequired = resource === 'PARTITIONS';
  const selectable = phase === 'SUCCESS' || phase === 'PARTIAL';

  return (
    <section aria-labelledby={`metadata-title-${dataSource.id}`} className="metadata-selector">
      <div className="metadata-selector-heading">
        <div>
          <p className="eyebrow">LIVE METADATA</p>
          <h3 id={`metadata-title-${dataSource.id}`}>元数据选择</h3>
        </div>
        <span className="status-row">{dataSource.name}</span>
      </div>
      <div className="form-grid metadata-query-form">
        <label>
          <span>读取对象</span>
          <select
            onChange={(event) => {
              markStale();
              setResource(event.target.value as MetadataResource);
            }}
            value={resource}
          >
            {resources.map((candidate) => (
              <option key={candidate} value={candidate}>
                {resourceLabel(candidate)}
              </option>
            ))}
          </select>
        </label>
        {databaseRequired && (
          <label>
            <span>数据库</span>
            <input
              onChange={(event) => {
                markStale();
                setDatabase(event.target.value);
              }}
              required
              value={database}
            />
          </label>
        )}
        {schemaRequired && (
          <label>
            <span>Schema</span>
            <input
              onChange={(event) => {
                markStale();
                setSchema(event.target.value);
              }}
              required
              value={schema}
            />
          </label>
        )}
        {tableRequired && (
          <label>
            <span>表</span>
            <input
              onChange={(event) => {
                markStale();
                setTable(event.target.value);
              }}
              required
              value={table}
            />
          </label>
        )}
        {topicRequired && (
          <label>
            <span>Topic</span>
            <input
              onChange={(event) => {
                markStale();
                setTopic(event.target.value);
              }}
              required
              value={topic}
            />
          </label>
        )}
        <button
          className="secondary-button metadata-refresh-button"
          disabled={
            phase === 'LOADING' ||
            (databaseRequired && database.trim().length === 0) ||
            (schemaRequired && schema.trim().length === 0) ||
            (tableRequired && table.trim().length === 0) ||
            (topicRequired && topic.trim().length === 0)
          }
          onClick={refresh}
          type="button"
        >
          {phase === 'LOADING' ? '正在读取' : '手动刷新'}
        </button>
      </div>
      <p
        aria-live="polite"
        className={`metadata-state metadata-state-${phase.toLowerCase()}`}
        role="status"
      >
        {phaseText(phase, response, error)}
      </p>
      {(phase === 'SUCCESS' || phase === 'PARTIAL') && (
        <div className="metadata-item-list" role="list" aria-label="可选元数据">
          {itemsOf(response).map((item) => {
            const selected = selection?.name === item.name && selection.identity === item.identity;
            return (
              <div key={`${item.name}:${item.identity ?? 'identity-unavailable'}`} role="listitem">
                <button
                  aria-pressed={selected}
                  className="metadata-item"
                  disabled={!selectable}
                  onClick={() => changeSelection(item)}
                  type="button"
                >
                  <span>{item.name}</span>
                  {Object.keys(item.attributes).length > 0 && (
                    <small>
                      {Object.entries(item.attributes)
                        .map(([key, value]) => `${key}: ${value}`)
                        .join(' · ')}
                    </small>
                  )}
                </button>
              </div>
            );
          })}
        </div>
      )}
      {selection !== null && selection.identity === null && (
        <p className="field-warning">
          当前对象未提供稳定 identity；下一次成功刷新会清除本次选择，避免同名对象被静默复用。
        </p>
      )}
    </section>
  );
}
