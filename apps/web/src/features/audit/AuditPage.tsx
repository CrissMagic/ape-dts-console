import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  AuditActorType,
  AuditEvent,
  AuditEventListQuery,
  AuditEventListResponse,
} from '@scs/contracts';
import {
  Alert,
  AutoComplete,
  Button,
  Card,
  Descriptions,
  Drawer,
  Empty,
  Input,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  type TableColumnsType,
} from 'antd';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';

import { auditApi, PlatformApiError } from '../../api/platform-api';
import { formatDateTime } from '../../lib/date-time';
import { auditActionLabels } from '../../lib/operation-labels';

interface AuditFilters {
  readonly action: string;
  readonly actorType: AuditActorType | '';
  readonly subjectId: string;
  readonly subjectType: string;
}

const emptyFilters: AuditFilters = {
  action: '',
  actorType: '',
  subjectId: '',
  subjectType: '',
};

function normalizedQuery(
  filters: AuditFilters,
  limit: number,
  cursor?: string,
): AuditEventListQuery {
  const action = filters.action.trim();
  const subjectId = filters.subjectId.trim();
  const subjectType = filters.subjectType.trim();
  return {
    ...(action.length === 0 ? {} : { action }),
    ...(filters.actorType === '' ? {} : { actorType: filters.actorType }),
    ...(cursor === undefined ? {} : { cursor }),
    limit,
    ...(subjectId.length === 0 ? {} : { subjectId }),
    ...(subjectType.length === 0 ? {} : { subjectType }),
  };
}

function actor(event: AuditEvent) {
  if (event.actorType === 'USER') {
    return (
      <Space size={6} wrap>
        <Tag color="blue">用户</Tag>
        <Typography.Text code copyable={{ text: event.actorUserId ?? '' }}>
          {event.actorUserId}
        </Typography.Text>
      </Space>
    );
  }
  return (
    <Tag color={event.actorType === 'SYSTEM' ? 'purple' : 'gold'}>
      {event.actorType === 'SYSTEM' ? '系统' : '部署管理员'}
    </Tag>
  );
}

function auditColumns(onViewDetails: (event: AuditEvent) => void): TableColumnsType<AuditEvent> {
  return [
    {
      dataIndex: 'sequence',
      key: 'sequence',
      title: '序号',
      width: 100,
    },
    {
      dataIndex: 'occurredAt',
      key: 'occurredAt',
      render: (value: string) => formatDateTime(value),
      title: '发生时间',
      width: 190,
    },
    {
      key: 'actor',
      render: (_value, event) => actor(event),
      title: '操作主体',
      width: 300,
    },
    {
      dataIndex: 'action',
      key: 'action',
      render: (value: string) => (
        <Space orientation="vertical" size={0}>
          {auditActionLabels[value] && <span>{auditActionLabels[value]}</span>}
          <Typography.Text type="secondary" code>
            {value}
          </Typography.Text>
        </Space>
      ),
      title: '动作',
      width: 260,
    },
    {
      key: 'subject',
      render: (_value, event) => (
        <Space orientation="vertical" size={0}>
          <span>{event.subjectType}</span>
          {event.subjectId !== null && (
            <Typography.Text type="secondary" copyable={{ text: event.subjectId }}>
              {event.subjectId}
            </Typography.Text>
          )}
        </Space>
      ),
      title: '对象',
      width: 300,
    },
    {
      fixed: 'right',
      key: 'actions',
      render: (_value, event) => (
        <Button type="link" onClick={() => onViewDetails(event)}>
          详情
        </Button>
      ),
      title: '操作',
      width: 90,
    },
  ];
}

export function AuditPage({ onSessionInvalidated }: { onSessionInvalidated: () => void }) {
  const [filters, setFilters] = useState<AuditFilters>(emptyFilters);
  const [appliedFilters, setAppliedFilters] = useState<AuditFilters>(emptyFilters);
  const [pages, setPages] = useState<readonly AuditEventListResponse[]>([]);
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<AuditEvent | null>(null);
  const requestGeneration = useRef(0);

  const load = useCallback(
    async (nextFilters: AuditFilters, cursor?: string, targetPage = 0, limit = 10) => {
      const nextPage = cursor !== undefined;
      const generation = requestGeneration.current + 1;
      requestGeneration.current = generation;
      if (nextPage) {
        setLoadingMore(true);
      } else {
        setLoading(true);
        setLoadingMore(false);
        setPages([]);
        setPageIndex(0);
      }
      setError(null);
      try {
        const result = await auditApi.list(normalizedQuery(nextFilters, limit, cursor));
        if (generation !== requestGeneration.current) return;
        setPages((current) => [...current.slice(0, targetPage), result]);
        setPageIndex(targetPage);
      } catch (caught) {
        if (generation !== requestGeneration.current) return;
        if (
          caught instanceof PlatformApiError &&
          (caught.status === 401 || caught.code === 'SESSION_INVALID')
        ) {
          onSessionInvalidated();
          return;
        }
        setError(
          caught instanceof PlatformApiError
            ? `读取审计记录失败：${caught.code}`
            : '读取审计记录失败。',
        );
      } finally {
        if (generation === requestGeneration.current) {
          if (nextPage) setLoadingMore(false);
          else setLoading(false);
        }
      }
    },
    [onSessionInvalidated],
  );

  useEffect(() => {
    void load(emptyFilters);
  }, [load]);

  const apply = () => {
    setAppliedFilters(filters);
    void load(filters, undefined, 0, pageSize);
  };
  const reset = () => {
    setFilters(emptyFilters);
    setAppliedFilters(emptyFilters);
    void load(emptyFilters, undefined, 0, pageSize);
  };
  const page = pages[pageIndex];
  const events = page?.events ?? [];
  const nextCursor = page?.nextCursor ?? null;
  const next = () => {
    if (pages[pageIndex + 1] !== undefined) {
      setError(null);
      setPageIndex(pageIndex + 1);
    } else if (nextCursor !== null) {
      void load(appliedFilters, nextCursor, pageIndex + 1, pageSize);
    }
  };

  return (
    <Space orientation="vertical" size={16} style={{ width: '100%' }}>
      {error !== null && <Alert showIcon type="error" title={error} />}
      <Card>
        <div className="audit-filter-grid">
          <label className="audit-filter-field">
            <span>操作主体</span>
            <Select<AuditActorType | ''>
              aria-label="操作主体"
              value={filters.actorType}
              style={{ width: 180 }}
              options={[
                { label: '全部', value: '' },
                { label: '用户', value: 'USER' },
                { label: '系统', value: 'SYSTEM' },
                { label: '部署管理员', value: 'DEPLOYMENT_ADMIN' },
              ]}
              onChange={(value) => setFilters((current) => ({ ...current, actorType: value }))}
            />
          </label>
          <label className="audit-filter-field">
            <span>动作</span>
            <AutoComplete
              aria-label="动作"
              placeholder="选择动作或输入代码"
              style={{ width: '100%' }}
              options={Object.entries(auditActionLabels).map(([value, label]) => ({
                value,
                label: `${label}（${value}）`,
              }))}
              filterOption={(input, option) =>
                String(option?.label).toLowerCase().includes(input.toLowerCase())
              }
              value={filters.action}
              onChange={(value) => setFilters((current) => ({ ...current, action: value }))}
            />
          </label>
          <label className="audit-filter-field">
            <span>对象类型</span>
            <Input
              aria-label="对象类型"
              placeholder="例如 TASK"
              value={filters.subjectType}
              onChange={(event) =>
                setFilters((current) => ({ ...current, subjectType: event.target.value }))
              }
            />
          </label>
          <label className="audit-filter-field audit-filter-id">
            <span>对象 ID</span>
            <Input
              aria-label="对象 ID"
              placeholder="UUID"
              value={filters.subjectId}
              onChange={(event) =>
                setFilters((current) => ({ ...current, subjectId: event.target.value }))
              }
            />
          </label>
          <div className="audit-filter-actions">
            <Button type="primary" icon={<SearchOutlined />} disabled={loading} onClick={apply}>
              查询
            </Button>
            <Button icon={<ReloadOutlined />} disabled={loading} onClick={reset}>
              重置
            </Button>
          </div>
        </div>
      </Card>
      <Card>
        <Table<AuditEvent>
          columns={auditColumns(setSelectedEvent)}
          dataSource={[...events]}
          locale={{ emptyText: <Empty description="暂无符合条件的审计记录" /> }}
          loading={loading}
          pagination={false}
          rowKey="eventId"
          scroll={{ x: 1150 }}
        />
        <div className="flex flex-wrap items-center justify-between gap-3 pt-4">
          <Space>
            <Select
              aria-label="审计每页条数"
              value={pageSize}
              disabled={loading}
              options={[10, 20, 50].map((size) => ({ value: size, label: `${size} 条/页` }))}
              onChange={(size) => {
                setPageSize(size);
                void load(appliedFilters, undefined, 0, size);
              }}
            />
            <Typography.Text type="secondary">本页 {events.length} 条</Typography.Text>
          </Space>
          <Space aria-label="审计分页">
            <Button
              aria-label="上一页"
              disabled={loading || loadingMore || pageIndex === 0}
              onClick={() => {
                setError(null);
                setPageIndex(pageIndex - 1);
              }}
            >
              上一页
            </Button>
            <span aria-live="polite">第 {pageIndex + 1} 页</span>
            <Button
              aria-label="下一页"
              disabled={loading || loadingMore || nextCursor === null}
              loading={loadingMore}
              onClick={next}
            >
              下一页
            </Button>
          </Space>
        </div>
      </Card>
      <Drawer
        destroyOnHidden
        open={selectedEvent !== null}
        size="large"
        title="审计详情"
        onClose={() => setSelectedEvent(null)}
      >
        {selectedEvent !== null && (
          <Space orientation="vertical" size={20} style={{ width: '100%' }}>
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="事件序号">{selectedEvent.sequence}</Descriptions.Item>
              <Descriptions.Item label="发生时间">
                {formatDateTime(selectedEvent.occurredAt)}
              </Descriptions.Item>
              <Descriptions.Item label="操作主体">{actor(selectedEvent)}</Descriptions.Item>
              <Descriptions.Item label="动作">
                {auditActionLabels[selectedEvent.action] && (
                  <p>{auditActionLabels[selectedEvent.action]}</p>
                )}
                <Typography.Text code copyable={{ text: selectedEvent.action }}>
                  {selectedEvent.action}
                </Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label="对象类型">{selectedEvent.subjectType}</Descriptions.Item>
              <Descriptions.Item label="对象 ID">
                {selectedEvent.subjectId === null ? (
                  <Typography.Text type="secondary">无</Typography.Text>
                ) : (
                  <Typography.Text copyable={{ text: selectedEvent.subjectId }}>
                    {selectedEvent.subjectId}
                  </Typography.Text>
                )}
              </Descriptions.Item>
            </Descriptions>
            <section aria-labelledby="audit-event-details-title">
              <Typography.Title id="audit-event-details-title" level={5}>
                事件明细
              </Typography.Title>
              {Object.keys(selectedEvent.details).length === 0 ? (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="无事件明细" />
              ) : (
                <pre className="audit-details">
                  {JSON.stringify(selectedEvent.details, null, 2)}
                </pre>
              )}
            </section>
          </Space>
        )}
      </Drawer>
    </Space>
  );
}
