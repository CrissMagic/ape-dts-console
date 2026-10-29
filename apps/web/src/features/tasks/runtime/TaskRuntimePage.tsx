import { useCallback, useEffect, useState } from 'react';
import { Button, Card, Drawer, Empty, Spin, Table, Tag } from 'antd';
import type { TaskSummary } from '@scs/contracts';

import { PlatformApiError, tasksApi } from '../../../api/platform-api';
import { operationName } from '../../../lib/operation-labels';
import { TaskSetupGuide } from '../TaskSetupGuide';
import { PerformanceSummary } from './RuntimePerformance';
import { TaskRuntimeDetails } from './TaskRuntimeDetails';
import { usePerformanceSummaries } from './usePerformanceSummaries';

function operationLabel(code: string) {
  return <span title={code}>{operationName(code)}</span>;
}

function errorCode(error: unknown) {
  return error instanceof PlatformApiError ? error.code : 'UNKNOWN_ERROR';
}

export function TaskRuntimePage({
  onSessionInvalidated,
  onOpenDataSources,
  onOpenTasks,
}: {
  onSessionInvalidated: () => void;
  onOpenDataSources?: () => void;
  onOpenTasks?: () => void;
}) {
  const [tasks, setTasks] = useState<readonly TaskSummary[]>([]);
  const [detailOpen, setDetailOpen] = useState(false);
  const performance = usePerformanceSummaries(
    tasks.map((task) => task.id),
    onSessionInvalidated,
    !detailOpen,
  );
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await tasksApi.list();
      setTasks(result.tasks);
      setSelectedId((current) =>
        current !== null && result.tasks.some((task) => task.id === current) ? current : null,
      );
    } catch (cause) {
      if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      setError(errorCode(cause));
    } finally {
      setLoading(false);
    }
  }, [onSessionInvalidated]);

  useEffect(() => void load(), [load]);
  const selected = tasks.find((task) => task.id === selectedId) ?? null;

  return (
    <section>
      <div className="mb-4 flex justify-end">
        <Button disabled={loading} onClick={() => void load()}>
          刷新
        </Button>
      </div>
      {error !== null && (
        <p className="error-banner" role="alert">
          操作失败：{error}
        </p>
      )}
      {performance.error && <p role="status">性能摘要读取失败，不影响任务操作。</p>}
      {loading ? (
        <Card className="runtime-empty-card">
          <Spin />
          <p role="status">正在读取任务运行记录…</p>
        </Card>
      ) : tasks.length === 0 && error !== null ? (
        <Card>
          <p>任务列表未能读取，暂时无法确认是否有可访问任务。请使用上方“刷新”重试。</p>
        </Card>
      ) : tasks.length === 0 ? (
        <Card className="runtime-empty-card">
          <Empty description="当前没有可访问任务。" />
          <TaskSetupGuide onOpenDataSources={onOpenDataSources} onOpenTasks={onOpenTasks} />
        </Card>
      ) : (
        <>
          <Table<TaskSummary>
            columns={[
              { dataIndex: 'name', key: 'name', title: '任务名称' },
              { dataIndex: 'taskType', key: 'taskType', title: '任务类型', render: operationLabel },
              {
                dataIndex: 'desiredState',
                key: 'desiredState',
                render: (state: string) => <Tag>{operationLabel(state)}</Tag>,
                title: '用户期望',
              },
              {
                dataIndex: 'observedState',
                key: 'observedState',
                render: (state: string) => (
                  <Tag
                    color={
                      state === 'RUNNING'
                        ? 'green'
                        : ['FAILED', 'BLOCKED'].includes(state)
                          ? 'red'
                          : 'default'
                    }
                  >
                    {operationLabel(state)}
                  </Tag>
                ),
                title: '运行状态',
              },
              {
                dataIndex: 'observabilityHealth',
                key: 'health',
                title: '可观测健康',
                render: operationLabel,
              },
              {
                key: 'performance',
                title: '当前吞吐',
                render: (_, task) => (
                  <PerformanceSummary
                    running={task.observedState === 'RUNNING'}
                    summary={performance.items.find((item) => item.taskId === task.id)}
                  />
                ),
              },
              {
                key: 'actions',
                fixed: 'right',
                width: 80,
                render: (_, task) => (
                  <Button
                    aria-label={`${task.name} 查看运行详情`}
                    onClick={() => {
                      setSelectedId(task.id);
                      setDetailOpen(true);
                    }}
                    type="link"
                  >
                    查看
                  </Button>
                ),
                title: '操作',
              },
            ]}
            dataSource={[...tasks]}
            pagination={false}
            rowKey="id"
            scroll={{ x: 1150 }}
          />
          <Drawer
            destroyOnHidden
            onClose={() => setDetailOpen(false)}
            open={detailOpen}
            size="large"
            styles={{ body: { display: 'flex', overflow: 'hidden', minHeight: 0 } }}
            title={selected === null ? '运行详情' : `${selected.name} · 运行详情`}
          >
            {detailOpen && selected !== null && (
              <TaskRuntimeDetails
                key={selected.id}
                task={selected}
                onSessionInvalidated={onSessionInvalidated}
                onTaskChanged={async () => {
                  const result = await tasksApi.list();
                  setTasks(result.tasks);
                }}
              />
            )}
          </Drawer>
        </>
      )}
    </section>
  );
}
