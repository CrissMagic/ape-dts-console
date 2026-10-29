import { useCallback, useEffect, useId, useRef, useState } from 'react';
import { Button, Select, Table, Tag, Tooltip } from 'antd';
import type { DataSourceSummary, MetadataItem, TaskType } from '@scs/contracts';
import { ArrowRight, Info, Plus, RefreshCw, Trash2 } from 'lucide-react';
import { dataSourcesApi, PlatformApiError } from '../../api/platform-api';
import { Input } from '../../components/ui/input';
import {
  emptyTaskMapping,
  mappingTargets,
  newMappingTarget,
  type TaskMappingDraft,
  type TaskMappingTarget,
} from './task-mappings';
import './source-sink-mappings.css';

function useMetadata(
  dataSource: DataSourceSummary | null,
  resource: 'SCHEMAS' | 'TABLES' | 'TOPICS',
  database?: string,
  schema?: string,
) {
  const [items, setItems] = useState<readonly MetadataItem[]>([]);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(false);
  const [complete, setComplete] = useState(false);
  const sequence = useRef(0);
  const enabled =
    dataSource !== null &&
    (resource === 'TOPICS' || Boolean(database)) &&
    (resource !== 'TABLES' || dataSource.configuration.type === 'MYSQL' || Boolean(schema));
  const load = useCallback(async () => {
    const request = ++sequence.current;
    setItems([]);
    setStatus('');
    setLoading(false);
    setComplete(false);
    if (!enabled || dataSource === null) return;
    setLoading(true);
    try {
      const result = await dataSourcesApi.metadata(dataSource, {
        resource,
        ...(database === undefined ? {} : { database }),
        ...(schema === undefined ? {} : { schema }),
      });
      if (request !== sequence.current) return;
      setItems('items' in result ? result.items : []);
      setComplete(result.state === 'SUCCESS' || result.state === 'EMPTY');
      setStatus(
        result.state === 'SUCCESS'
          ? ''
          : result.state === 'EMPTY'
            ? '暂无可选项，可手动填写。'
            : `元数据读取不完整${'errorSummary' in result && result.errorSummary ? `：${result.errorSummary}` : ''}，可手动填写。`,
      );
    } catch (error) {
      if (request !== sequence.current) return;
      setStatus(
        `元数据读取失败${error instanceof PlatformApiError ? `：${error.code}` : ''}，可手动填写。`,
      );
    } finally {
      if (request === sequence.current) setLoading(false);
    }
  }, [dataSource, database, enabled, resource, schema]);
  useEffect(() => {
    void load();
    return () => {
      sequence.current += 1;
    };
  }, [load]);
  return { items, status, loading, load, enabled, complete };
}

function SourceInput({
  dataSource,
  database,
  resource,
  schema,
  label,
  value,
  onChange,
}: {
  dataSource: DataSourceSummary | null;
  database?: string | undefined;
  resource: 'SCHEMAS' | 'TABLES' | 'TOPICS';
  schema?: string | undefined;
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  const metadata = useMetadata(dataSource, resource, database, schema);
  const id = useId();
  return (
    <div className="task-mapping-source-input">
      <div className="flex min-w-0 gap-1">
        <Input
          aria-label={label}
          placeholder={label.replace(/ \d+$/u, '')}
          value={value}
          list={id}
          disabled={dataSource === null}
          onChange={(event) => onChange(event.target.value)}
        />
        <datalist id={id}>
          {metadata.items.map((item) => (
            <option key={item.name} value={item.name} />
          ))}
        </datalist>
        <Button
          aria-label={`刷新${label}`}
          disabled={!metadata.enabled || metadata.loading}
          onClick={() => void metadata.load()}
          icon={<RefreshCw size={14} className={metadata.loading ? 'animate-spin' : ''} />}
        />
      </div>
      {metadata.status && (
        <span className="text-xs text-muted-foreground" role="status">
          {metadata.status}
        </span>
      )}
    </div>
  );
}

export function SourceSinkMappings({
  rows,
  taskType,
  source,
  target,
  onChange,
  canCreate,
  onCreateTopic,
  refreshKey,
  creating,
}: {
  rows: readonly TaskMappingDraft[];
  taskType: TaskType;
  source: DataSourceSummary | null;
  target: DataSourceSummary | null;
  onChange: (rows: readonly TaskMappingDraft[]) => void;
  canCreate: boolean;
  creating: boolean;
  onCreateTopic: (target: TaskMappingTarget) => void;
  refreshKey: number;
}) {
  const metadata = useMetadata(target, 'TOPICS');
  const refreshTopics = metadata.load;
  const previousRefresh = useRef(refreshKey);
  useEffect(() => {
    if (previousRefresh.current !== refreshKey) {
      previousRefresh.current = refreshKey;
      void refreshTopics();
    }
  }, [refreshTopics, refreshKey]);
  const uniqueTargets = mappingTargets(rows);
  const existing = new Set(metadata.items.map((item) => item.name));
  const database =
    source?.configuration.type === 'POSTGRES' || source?.configuration.type === 'MYSQL'
      ? source.configuration.database
      : undefined;
  function updateRow(index: number, changes: Partial<TaskMappingDraft>) {
    onChange(rows.map((row, rowIndex) => (rowIndex === index ? { ...row, ...changes } : row)));
  }
  function updateTarget(topic: string, changes: Partial<TaskMappingTarget>) {
    onChange(
      rows.map((row) => ({
        ...row,
        targets: row.targets.map((item) => (item.topic === topic ? { ...item, ...changes } : item)),
      })),
    );
  }
  return (
    <section className="task-mappings" aria-label="来源与目标映射">
      <div className="flex flex-wrap items-center justify-between gap-3 pb-3">
        <div className="flex items-center gap-2">
          <Tag variant="filled">{rows.length} 个来源</Tag>
          <Tooltip title="每行一个来源，其数据将分别写入所有已选目标 Topic。输入 Topic 名称后按 Enter 可手动添加。">
            <button type="button" aria-label="映射说明" className="text-muted-foreground">
              <Info size={15} />
            </button>
          </Tooltip>
        </div>
        <Button
          icon={<Plus size={15} />}
          disabled={source === null}
          onClick={() =>
            onChange([
              ...rows,
              {
                ...emptyTaskMapping(),
                ...(taskType === 'MYSQL_KAFKA' ? { sourceSchema: database ?? '' } : {}),
              },
            ])
          }
        >
          添加来源
        </Button>
      </div>
      <Table
        pagination={false}
        size="small"
        rowKey="key"
        dataSource={rows.map((row, key) => ({ ...row, key }))}
        scroll={{ x: 520, ...(rows.length > 3 ? { y: 220 } : {}) }}
        locale={{ emptyText: '请添加来源并配置目标 Topic' }}
        columns={[
          {
            title: taskType !== 'KAFKA_KAFKA' ? '来源表' : '来源 Topic',
            width: taskType !== 'KAFKA_KAFKA' ? '45%' : '38%',
            render: (_, row) => {
              const suffix = row.key === 0 ? '' : ` ${row.key + 1}`;
              return taskType !== 'KAFKA_KAFKA' ? (
                <div className="grid min-w-0 gap-2 sm:grid-cols-2">
                  {taskType === 'MYSQL_KAFKA' ? (
                    <Input aria-label={`来源数据库${suffix}`} value={row.sourceSchema} readOnly />
                  ) : (
                    <SourceInput
                      dataSource={source}
                      database={database}
                      resource="SCHEMAS"
                      label={`源 Schema${suffix}`}
                      value={row.sourceSchema}
                      onChange={(value) =>
                        updateRow(row.key, { sourceSchema: value, sourceTable: '' })
                      }
                    />
                  )}
                  <SourceInput
                    dataSource={source}
                    database={database}
                    schema={taskType === 'MYSQL_KAFKA' ? undefined : row.sourceSchema}
                    resource="TABLES"
                    label={`源表${suffix}`}
                    value={row.sourceTable}
                    onChange={(value) => updateRow(row.key, { sourceTable: value })}
                  />
                </div>
              ) : (
                <SourceInput
                  dataSource={source}
                  resource="TOPICS"
                  label={`源 Topic${suffix}`}
                  value={row.sourceTopic}
                  onChange={(value) => updateRow(row.key, { sourceTopic: value })}
                />
              );
            },
          },
          {
            title: '',
            width: 35,
            render: () => <ArrowRight size={17} className="text-muted-foreground" />,
          },
          {
            title: '目标 Topic · 可多选',
            render: (_, row) => (
              <Select<string[]>
                aria-label={row.key === 0 ? '目标 Topic' : `目标 Topic ${row.key + 1}`}
                mode="tags"
                style={{ width: '100%' }}
                disabled={target === null}
                loading={metadata.loading}
                value={row.targets.map((item) => item.topic)}
                placeholder={target ? '选择或输入 Topic，按 Enter 添加' : '请先选择目标 Kafka'}
                options={metadata.items.map((item) => ({ value: item.name, label: item.name }))}
                onChange={(topics) =>
                  updateRow(row.key, {
                    targets: topics.map(
                      (topic) =>
                        uniqueTargets.find((item) => item.topic === topic) ??
                        newMappingTarget(topic),
                    ),
                  })
                }
                tagRender={({ label, closable, onClose }) => (
                  <Tag
                    closable={closable}
                    onClose={onClose}
                    onMouseDown={(event) => {
                      event.preventDefault();
                      event.stopPropagation();
                    }}
                    style={{
                      marginBlock: 2,
                      maxWidth: '100%',
                      whiteSpace: 'normal',
                      wordBreak: 'break-all',
                    }}
                    title={String(label)}
                  >
                    {label}
                    {metadata.complete && !existing.has(String(label)) && (
                      <span className="ml-1 text-amber-700 dark:text-amber-300">
                        {row.targets.find((item) => item.topic === String(label))?.autoCreateTopic
                          ? '待创建'
                          : '未发现'}
                      </span>
                    )}
                  </Tag>
                )}
              />
            ),
          },
          {
            title: '操作',
            width: 62,
            render: (_, row) => (
              <Button
                type="text"
                danger
                aria-label={`删除来源 ${row.key + 1}`}
                icon={<Trash2 size={16} />}
                onClick={() => onChange(rows.filter((_, index) => index !== row.key))}
              />
            ),
          },
        ]}
      />
      <div className="flex flex-wrap items-center justify-between gap-2 py-2 text-xs text-muted-foreground">
        <span>
          {rows.reduce((sum, row) => sum + row.targets.length, 0)} 条映射 · {uniqueTargets.length}{' '}
          个目标 Topic
        </span>
        <Button
          size="small"
          type="text"
          disabled={!metadata.enabled || metadata.loading}
          onClick={() => void metadata.load()}
          icon={<RefreshCw size={13} />}
        >
          刷新目标 Topic
        </Button>
      </div>
      {metadata.status && (
        <p className="text-xs text-muted-foreground" role="status">
          {metadata.status}
        </p>
      )}
      {uniqueTargets.length > 0 && (
        <details className="rounded-lg border p-3">
          <summary className="cursor-pointer text-sm font-medium">Topic 创建设置</summary>
          <p className="my-2 text-xs text-muted-foreground">
            已有 Topic 不修改。相同 Topic 的设置适用于全部映射。
          </p>
          <div className="max-h-56 space-y-3 overflow-auto">
            {uniqueTargets.map((item, index) => (
              <div key={item.topic} className="task-mapping-topic-policy">
                <span className="min-w-0 break-all text-sm font-medium">{item.topic}</span>
                <label className="flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={item.autoCreateTopic}
                    onChange={(event) =>
                      updateTarget(item.topic, { autoCreateTopic: event.target.checked })
                    }
                  />
                  首次启动时创建缺失的 Topic{index > 0 ? `（${item.topic}）` : ''}
                </label>
                {(item.autoCreateTopic || canCreate) && (
                  <div className="flex items-center gap-2">
                    <Input
                      type="number"
                      min={1}
                      step={1}
                      aria-label={index === 0 ? '分区数' : `${item.topic}分区数`}
                      placeholder="分区数"
                      value={item.numPartitions ?? ''}
                      onChange={(event) =>
                        updateTarget(item.topic, {
                          numPartitions:
                            event.target.value === '' ? null : Number(event.target.value),
                        })
                      }
                    />
                    <Input
                      type="number"
                      min={1}
                      step={1}
                      aria-label={index === 0 ? '副本数' : `${item.topic}副本数`}
                      placeholder="副本数"
                      value={item.replicationFactor ?? ''}
                      onChange={(event) =>
                        updateTarget(item.topic, {
                          replicationFactor:
                            event.target.value === '' ? null : Number(event.target.value),
                        })
                      }
                    />
                    {canCreate && (
                      <Button
                        disabled={target === null || creating}
                        onClick={() => onCreateTopic(item)}
                      >
                        立即创建 Topic{index > 0 ? ` ${item.topic}` : ''}
                      </Button>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </details>
      )}
    </section>
  );
}
