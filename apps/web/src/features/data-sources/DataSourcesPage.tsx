import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { useOperationNotification } from '../../components/useOperationNotification';
import { Alert, Button, Card, Input, Modal, Select, Space, Table, Tag } from 'antd';
import { ArrowLeft, CircleCheck, Database, Link2, LockKeyhole, Users } from 'lucide-react';
import type {
  AuthenticatedSessionResponse,
  ConnectionTestResponse,
  CreateDataSourceRequest,
  DataSourceConfiguration,
  DataSourceCatalogQuery,
  DataSourceGrant,
  DataSourceSummary,
  DataSourceType,
  MysqlCdcInspection,
  UpdateDataSourceRequest,
  UserSummary,
} from '@scs/contracts';
import { copyDataSourceRequestSchema } from '@scs/contracts';

import { dataSourcesApi, PlatformApiError, usersApi } from '../../api/platform-api';
import { useDraftGuard, type LeaveGuard } from '../../components/useDraftGuard';
import { InfoTip } from '../../components/InfoTip';
import { Button as NavigationButton } from '../../components/ui/button';
import './data-source-editor.css';

type TlsMode = '' | 'DISABLE' | 'REQUIRE' | 'VERIFY_FULL';

interface SourceDraft {
  readonly bootstrapServers: string;
  readonly caCertificate: string;
  readonly connectionMode: 'REPLACE' | 'RETAIN';
  readonly database: string;
  readonly disableRiskAcknowledged: boolean;
  readonly host: string;
  readonly name: string;
  readonly password: string;
  readonly plaintextRiskAcknowledged: boolean;
  readonly port: string;
  readonly sourceType: DataSourceType | '';
  readonly tlsMode: TlsMode;
  readonly username: string;
}

type PendingAction =
  | {
      readonly description: string;
      readonly kind: 'CREATE';
      readonly request: CreateDataSourceRequest;
    }
  | {
      readonly dataSource: DataSourceSummary;
      readonly description: string;
      readonly kind: 'UPDATE';
      readonly request: UpdateDataSourceRequest;
    }
  | {
      readonly dataSource: DataSourceSummary;
      readonly description: string;
      readonly kind: 'DELETE';
    }
  | {
      readonly dataSource: DataSourceSummary;
      readonly description: string;
      readonly kind: 'GRANT';
      readonly user: UserSummary;
    }
  | {
      readonly dataSource: DataSourceSummary;
      readonly description: string;
      readonly grant: DataSourceGrant;
      readonly kind: 'REVOKE';
    };

interface ConnectionTestResult {
  readonly dataSourceId: string;
  readonly dataSourceRowVersion: string;
  readonly result: ConnectionTestResponse;
}

function emptyDraft(): SourceDraft {
  return {
    bootstrapServers: '',
    caCertificate: '',
    connectionMode: 'RETAIN',
    database: '',
    disableRiskAcknowledged: false,
    host: '',
    name: '',
    password: '',
    plaintextRiskAcknowledged: false,
    port: '',
    sourceType: '',
    tlsMode: '',
    username: '',
  };
}

function draftFromDataSource(dataSource: DataSourceSummary): SourceDraft {
  const configuration = dataSource.configuration;
  if (configuration.type === 'KAFKA') {
    return {
      ...emptyDraft(),
      bootstrapServers: configuration.bootstrapServers.join(', '),
      name: dataSource.name,
      plaintextRiskAcknowledged: configuration.plaintextRiskAcknowledged,
      sourceType: configuration.type,
    };
  }
  return {
    ...emptyDraft(),
    caCertificate: configuration.tls.mode === 'VERIFY_FULL' ? configuration.tls.caCertificate : '',
    database: configuration.database,
    connectionMode: dataSource.credentialsRequired ? 'REPLACE' : 'RETAIN',
    disableRiskAcknowledged:
      configuration.tls.mode === 'DISABLE' && configuration.tls.riskAcknowledged,
    host: configuration.host,
    name: dataSource.name,
    port: String(configuration.port),
    sourceType: configuration.type,
    tlsMode: configuration.tls.mode,
    username: configuration.username,
  };
}

function typeLabel(type: DataSourceType): string {
  switch (type) {
    case 'POSTGRES':
      return 'PostgreSQL';
    case 'KAFKA':
      return 'Kafka';
    case 'MYSQL':
      return 'MySQL';
    case 'DORIS':
      return 'Doris';
  }
}

function requireText(value: string, field: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`请明确填写${field}。`);
  }
  return trimmed;
}

function requiredPort(value: string): number {
  if (!/^\d+$/u.test(value) || Number(value) < 1 || Number(value) > 65_535) {
    throw new Error('请明确填写 1 到 65535 之间的端口。');
  }
  return Number(value);
}

function tlsFromDraft(draft: SourceDraft) {
  switch (draft.tlsMode) {
    case 'DISABLE':
      if (!draft.disableRiskAcknowledged) {
        throw new Error('明文数据库连接必须明确确认风险。');
      }
      return { mode: 'DISABLE' as const, riskAcknowledged: true as const };
    case 'REQUIRE':
      return { mode: 'REQUIRE' as const };
    case 'VERIFY_FULL':
      return {
        caCertificate: requireText(draft.caCertificate, 'CA 证书'),
        mode: 'VERIFY_FULL' as const,
      };
    case '':
      throw new Error('请明确选择数据库 TLS 模式。');
  }
}

function configurationFromDraft(draft: SourceDraft): DataSourceConfiguration {
  if (draft.sourceType === '') {
    throw new Error('请明确选择数据源类型。');
  }
  if (draft.sourceType === 'KAFKA') {
    const bootstrapServers = draft.bootstrapServers
      .split(',')
      .map((server) => server.trim())
      .filter((server) => server.length > 0);
    if (bootstrapServers.length === 0) {
      throw new Error('请明确填写至少一个 Kafka bootstrap server。');
    }
    if (!draft.plaintextRiskAcknowledged) {
      throw new Error('Kafka PLAINTEXT 必须明确确认风险。');
    }
    return {
      bootstrapServers,
      plaintextRiskAcknowledged: true,
      type: 'KAFKA',
    };
  }
  const databaseConfiguration = {
    database: requireText(draft.database, '数据库名'),
    host: requireText(draft.host, '主机地址'),
    port: requiredPort(draft.port),
    tls: tlsFromDraft(draft),
    username: requireText(draft.username, '用户名'),
  };
  switch (draft.sourceType) {
    case 'POSTGRES':
      return { ...databaseConfiguration, type: 'POSTGRES' };
    case 'MYSQL':
      return { ...databaseConfiguration, type: 'MYSQL' };
    case 'DORIS':
      return { ...databaseConfiguration, type: 'DORIS' };
  }
}

function createRequestFromDraft(draft: SourceDraft): CreateDataSourceRequest {
  const configuration = configurationFromDraft(draft);
  const name = requireText(draft.name, '数据源名称');
  if (configuration.type === 'KAFKA') {
    return { configuration, credentials: { mode: 'NONE' }, name };
  }
  const credentials = { mode: 'SET' as const, password: requireText(draft.password, '连接密码') };
  switch (configuration.type) {
    case 'POSTGRES':
      return { configuration, credentials, name };
    case 'MYSQL':
      return { configuration, credentials, name };
    case 'DORIS':
      return { configuration, credentials, name };
  }
}

function updateRequestFromDraft(
  dataSource: DataSourceSummary,
  draft: SourceDraft,
): UpdateDataSourceRequest {
  const configuration = configurationFromDraft(draft);
  if (configuration.type !== dataSource.configuration.type) {
    throw new Error('已登记数据源的类型不能在编辑中变更；请新建正确类型的数据源。');
  }
  const name = requireText(draft.name, '数据源名称');
  if (configuration.type === 'KAFKA') {
    return {
      configuration,
      credentials: { mode: 'NONE' },
      expectedRowVersion: dataSource.rowVersion,
      name,
    };
  }
  const credentials =
    draft.connectionMode === 'REPLACE'
      ? { mode: 'REPLACE' as const, password: requireText(draft.password, '新连接密码') }
      : { mode: 'RETAIN' as const };
  switch (configuration.type) {
    case 'POSTGRES':
      return { configuration, credentials, expectedRowVersion: dataSource.rowVersion, name };
    case 'MYSQL':
      return { configuration, credentials, expectedRowVersion: dataSource.rowVersion, name };
    case 'DORIS':
      return { configuration, credentials, expectedRowVersion: dataSource.rowVersion, name };
  }
}

export function DataSourcesPage({
  onLeaveGuardChange,
  onSessionInvalidated,
  session,
}: {
  onLeaveGuardChange?: (guard: LeaveGuard | null) => void;
  onSessionInvalidated: () => void;
  session: AuthenticatedSessionResponse;
}) {
  const administrator = session.user.role === 'ADMIN';
  const [dataSources, setDataSources] = useState<readonly DataSourceSummary[]>([]);
  const [draft, setDraft] = useState<SourceDraft>(emptyDraft);
  const [selectedDataSource, setSelectedDataSource] = useState<DataSourceSummary | null>(null);
  const [catalogQuery, setCatalogQuery] = useState<DataSourceCatalogQuery>({
    page: 1,
    pageSize: 10,
    q: '',
  });
  const [catalogTotal, setCatalogTotal] = useState(0);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const catalogRequest = useRef(0);
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorSection, setEditorSection] = useState('connection');
  const [grants, setGrants] = useState<readonly DataSourceGrant[]>([]);
  const [grantsLoading, setGrantsLoading] = useState(false);
  const [users, setUsers] = useState<readonly UserSummary[]>([]);
  const [grantUserId, setGrantUserId] = useState('');
  const [pendingAction, setPendingAction] = useState<PendingAction | null>(null);
  const [pendingActionConfirmed, setPendingActionConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [copySource, setCopySource] = useState<DataSourceSummary | null>(null);
  const [copyName, setCopyName] = useState('');
  const copying = useRef(false);
  const { holder, setError, notify } = useOperationNotification();
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null);
  const [connectionTesting, setConnectionTesting] = useState(false);
  const connectionTestRequest = useRef(0);
  useEffect(
    () => () => {
      connectionTestRequest.current += 1;
    },
    [],
  );
  const [mysqlInspection, setMysqlInspection] = useState<{
    dataSourceId: string;
    rowVersion: string;
    result: MysqlCdcInspection;
  } | null>(null);
  const [mysqlInspectionBusy, setMysqlInspectionBusy] = useState(false);
  const mysqlInspectionRequest = useRef(0);
  const invalidateMysqlInspection = useCallback(() => {
    mysqlInspectionRequest.current += 1;
    setMysqlInspection(null);
    setMysqlInspectionBusy(false);
  }, []);

  const baseline =
    selectedDataSource === null ? emptyDraft() : draftFromDataSource(selectedDataSource);
  const dirty = editorOpen && administrator && JSON.stringify(draft) !== JSON.stringify(baseline);
  useEffect(() => {
    invalidateMysqlInspection();
    return () => {
      mysqlInspectionRequest.current += 1;
    };
  }, [invalidateMysqlInspection, selectedDataSource?.id, selectedDataSource?.rowVersion, dirty]);
  const { requestLeave, prompt } = useDraftGuard(dirty, busy && pendingAction !== null);
  useEffect(() => {
    onLeaveGuardChange?.(requestLeave);
    return () => onLeaveGuardChange?.(null);
  }, [onLeaveGuardChange, requestLeave]);
  const selectedTestResult =
    selectedDataSource !== null &&
    testResult?.dataSourceId === selectedDataSource.id &&
    testResult.dataSourceRowVersion === selectedDataSource.rowVersion
      ? testResult.result
      : null;
  const selectedMysqlInspection =
    selectedDataSource !== null &&
    !dirty &&
    mysqlInspection?.dataSourceId === selectedDataSource.id &&
    mysqlInspection.rowVersion === selectedDataSource.rowVersion
      ? mysqlInspection.result
      : null;
  const grantCandidates = users.filter(
    (user) => user.role === 'DEVELOPER' && user.status === 'ACTIVE',
  );

  const loadDataSources = useCallback(async () => {
    const requestId = ++catalogRequest.current;
    setCatalogLoading(true);
    try {
      const result = await dataSourcesApi.catalog(catalogQuery);
      if (requestId !== catalogRequest.current) return;
      setDataSources(result.dataSources);
      setCatalogTotal(result.total);
      setCatalogQuery((current) =>
        current.page === result.page ? current : { ...current, page: result.page },
      );
    } finally {
      if (requestId === catalogRequest.current) setCatalogLoading(false);
    }
  }, [catalogQuery]);

  const loadGrants = useCallback(async (dataSource: DataSourceSummary) => {
    const result = await dataSourcesApi.listGrants(dataSource);
    setGrants(result.grants);
  }, []);

  const reportFailure = useCallback(
    (prefix: string, caught: unknown) => {
      if (caught instanceof PlatformApiError) {
        if (caught.status === 401 || caught.code === 'SESSION_INVALID') {
          onSessionInvalidated();
          return;
        }
        const messages: Record<string, string> = {
          DATA_SOURCE_REFERENCED_BY_TASK: '该数据源仍被任务引用，请先调整或删除相关任务。',
          DATA_SOURCE_REFERENCED_BY_RESOURCE: '该数据源仍关联受跟踪的复制资源，请先处理相关资源。',
          DATA_SOURCE_NAME_ALREADY_EXISTS: '数据源名称已存在，请使用其他名称。',
          DATA_SOURCE_VERSION_CONFLICT: '数据源已被其他操作更新，请刷新后重试。',
        };
        setError(messages[caught.code] ?? `${prefix}：${caught.code}`);
        return;
      }
      setError(`${prefix}。`);
    },
    [onSessionInvalidated, setError],
  );

  const run = useCallback(
    (action: () => Promise<void>, failurePrefix: string) => {
      setBusy(true);
      setError(null);
      void action()
        .catch((caught: unknown) => reportFailure(failurePrefix, caught))
        .finally(() => setBusy(false));
    },
    [reportFailure, setError],
  );

  useEffect(() => {
    run(async () => {
      if (administrator) {
        const result = await usersApi.list();
        setUsers(result.users);
      }
    }, '读取数据源失败');
  }, [administrator, run]);

  useEffect(() => {
    let active = true;
    const timer = setTimeout(() => {
      void loadDataSources().catch((caught: unknown) => {
        if (active) reportFailure('读取数据源失败', caught);
      });
    }, 200);
    return () => {
      active = false;
      clearTimeout(timer);
      catalogRequest.current += 1;
    };
  }, [loadDataSources, reportFailure]);

  useEffect(() => {
    setGrants([]);
    if (!administrator || selectedDataSource === null) {
      setGrantsLoading(false);
      return;
    }
    let active = true;
    setGrantsLoading(true);
    void dataSourcesApi
      .listGrants(selectedDataSource)
      .then((result) => {
        if (active) setGrants(result.grants);
      })
      .catch((error: unknown) => {
        if (active) reportFailure('读取授权失败', error);
      })
      .finally(() => {
        if (active) setGrantsLoading(false);
      });
    return () => {
      active = false;
    };
  }, [administrator, reportFailure, selectedDataSource]);

  function selectDataSource(dataSource: DataSourceSummary) {
    setEditorSection('connection');
    setSelectedDataSource(dataSource);
    setDraft(draftFromDataSource(dataSource));
    resetConnectionTest();
    invalidateMysqlInspection();
    setError(null);
    setEditorOpen(true);
  }

  function beginCreate() {
    setEditorSection('connection');
    invalidateMysqlInspection();
    setSelectedDataSource(null);
    setDraft(emptyDraft());
    resetConnectionTest();
    setPendingAction(null);
    setError(null);
    setEditorOpen(true);
  }

  function closeEditor() {
    requestLeave(() => {
      invalidateMysqlInspection();
      setEditorOpen(false);
      setSelectedDataSource(null);
      setDraft(emptyDraft());
      resetConnectionTest();
      setError(null);
    });
  }

  function requestDelete(dataSource: DataSourceSummary) {
    setPendingAction({
      dataSource,
      description: `将删除平台中的数据源“${dataSource.name}”，销毁其保存的连接密码并撤销用户授权。存在任务或受跟踪复制资源引用时无法删除；不会删除外部数据库、Topic 或复制资源。`,
      kind: 'DELETE',
    });
    setPendingActionConfirmed(false);
    setError(null);
  }

  function requestCopy(dataSource: DataSourceSummary) {
    setCopySource(dataSource);
    setCopyName(`${dataSource.name.slice(0, 125)}-副本`);
  }

  function confirmCopy() {
    if (copySource === null || copying.current) return;
    const parsed = copyDataSourceRequestSchema.safeParse({
      name: copyName,
      expectedRowVersion: copySource.rowVersion,
    });
    if (!parsed.success) {
      setError(
        '请输入有效的数据源名称：以文字或数字开头，最长 128 字符，可包含空格、点、下划线、冒号和短横线。',
      );
      return;
    }
    copying.current = true;
    run(async () => {
      try {
        await dataSourcesApi.copy(copySource, { name: parsed.data.name });
        setCopySource(null);
        notify('success', '数据源已复制，未复制原数据源的用户授权。');
        await loadDataSources();
      } finally {
        copying.current = false;
      }
    }, '复制数据源失败');
  }

  function submitEditor(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    try {
      if (selectedDataSource === null) {
        const request = createRequestFromDraft(draft);
        setPendingAction({
          description: `将创建 ${request.name}（${typeLabel(request.configuration.type)}）。${request.configuration.type === 'KAFKA' ? '当前连接使用明文传输。' : '连接密码将加密保存。'}创建后请执行连接测试，确认平台可以访问该数据源。`,
          kind: 'CREATE',
          request,
        });
      } else {
        const request = updateRequestFromDraft(selectedDataSource, draft);
        setPendingAction({
          dataSource: selectedDataSource,
          description:
            request.credentials.mode === 'REPLACE'
              ? `将更新 ${selectedDataSource.name} 并替换连接密码。旧密码将从平台中销毁，操作后需重新测试。`
              : `将更新 ${selectedDataSource.name} 的连接配置，保留现有连接凭据。`,
          kind: 'UPDATE',
          request,
        });
      }
      setPendingActionConfirmed(false);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : '无法构建数据源请求。');
    }
  }

  function confirmPendingAction() {
    if (pendingAction === null || !pendingActionConfirmed) {
      return;
    }
    run(async () => {
      switch (pendingAction.kind) {
        case 'CREATE': {
          const created = await dataSourcesApi.create(pendingAction.request);
          await loadDataSources();
          setSelectedDataSource(created);
          setDraft(draftFromDataSource(created));
          notify('success', '数据源已创建，可继续测试连接。');
          break;
        }
        case 'UPDATE': {
          const updated = await dataSourcesApi.update(
            pendingAction.dataSource,
            pendingAction.request,
          );
          await loadDataSources();
          setSelectedDataSource(updated);
          setDraft(draftFromDataSource(updated));
          setTestResult(null);
          notify('success', '数据源配置已保存，请重新测试连接。');
          break;
        }
        case 'DELETE':
          await dataSourcesApi.remove(pendingAction.dataSource);
          await loadDataSources();
          setEditorOpen(false);
          setSelectedDataSource(null);
          setDraft(emptyDraft());
          setTestResult(null);
          break;
        case 'GRANT':
          await dataSourcesApi.grant(pendingAction.dataSource, pendingAction.user.id);
          await loadGrants(pendingAction.dataSource);
          setGrantUserId('');
          break;
        case 'REVOKE':
          await dataSourcesApi.revoke(pendingAction.dataSource, pendingAction.grant.user.id);
          await loadGrants(pendingAction.dataSource);
          break;
      }
      setPendingAction(null);
      setPendingActionConfirmed(false);
    }, '操作失败');
  }

  function resetConnectionTest() {
    connectionTestRequest.current += 1;
    setTestResult(null);
    setConnectionTesting(false);
  }

  function runConnectionTest() {
    if (selectedDataSource === null || dirty || connectionTesting) {
      return;
    }
    const testedSource = selectedDataSource;
    const requestId = ++connectionTestRequest.current;
    setConnectionTesting(true);
    setTestResult(null);
    void dataSourcesApi
      .test(testedSource)
      .then((result) => {
        if (requestId === connectionTestRequest.current)
          setTestResult({
            dataSourceId: testedSource.id,
            dataSourceRowVersion: testedSource.rowVersion,
            result,
          });
      })
      .catch((error: unknown) => {
        if (requestId === connectionTestRequest.current) reportFailure('连接测试失败', error);
      })
      .finally(() => {
        if (requestId === connectionTestRequest.current) setConnectionTesting(false);
      });
  }

  function inspectMysqlCdc() {
    if (selectedDataSource === null || dirty || mysqlInspectionBusy) return;
    const source = selectedDataSource;
    const requestId = ++mysqlInspectionRequest.current;
    setMysqlInspection(null);
    setMysqlInspectionBusy(true);
    setError(null);
    void dataSourcesApi
      .inspectMysqlCdc(source)
      .then((result) => {
        if (mysqlInspectionRequest.current === requestId)
          setMysqlInspection({ dataSourceId: source.id, rowVersion: source.rowVersion, result });
      })
      .catch((error: unknown) => {
        if (mysqlInspectionRequest.current !== requestId) return;
        if (error instanceof PlatformApiError && error.code === 'DATA_SOURCE_VERSION_CONFLICT') {
          setError('连接配置已变更，请刷新数据源后重新检查。');
        } else if (error instanceof PlatformApiError && error.code === 'PERMISSION_DENIED') {
          setError('Binlog 检查失败：请检查 MySQL 账号的 REPLICATION CLIENT 权限。');
        } else {
          reportFailure('Binlog 检查失败', error);
        }
      })
      .finally(() => {
        if (mysqlInspectionRequest.current === requestId) setMysqlInspectionBusy(false);
      });
  }

  function requestGrant() {
    if (selectedDataSource === null || grantUserId.length === 0) {
      return;
    }
    const user = grantCandidates.find((candidate) => candidate.id === grantUserId);
    if (user === undefined) {
      setError('请选择当前有效的开发者。');
      return;
    }
    setPendingAction({
      dataSource: selectedDataSource,
      description: `将授予 ${user.displayName} 使用 ${selectedDataSource.name} 的权限，使其可以读取元数据和主动测试连接。不会授予任意 SQL 或 DDL 权限。`,
      kind: 'GRANT',
      user,
    });
    setPendingActionConfirmed(false);
  }

  const databaseSource = draft.sourceType !== '' && draft.sourceType !== 'KAFKA';

  return (
    <section
      aria-label="数据源目录"
      className={editorOpen ? 'data-sources-page is-editing' : 'data-sources-page'}
    >
      {!editorOpen && (
        <>
          <div className="data-source-catalog-toolbar">
            <Input
              aria-label="搜索数据源"
              placeholder="搜索名称、地址或数据库…"
              value={catalogQuery.q}
              maxLength={200}
              allowClear
              onChange={(event) => {
                catalogRequest.current += 1;
                setCatalogQuery((current) => ({ ...current, q: event.target.value, page: 1 }));
              }}
            />
            <Select<DataSourceType | undefined>
              aria-label="数据源类型筛选"
              placeholder="全部类型"
              allowClear
              value={catalogQuery.type}
              options={(['POSTGRES', 'MYSQL', 'KAFKA', 'DORIS'] as const).map((type) => ({
                value: type,
                label: typeLabel(type),
              }))}
              onChange={(type) => {
                catalogRequest.current += 1;
                setCatalogQuery((current) => ({ ...current, type, page: 1 }));
              }}
            />
            <span className="text-sm text-muted-foreground">{catalogTotal} 个数据源</span>
            {administrator && (
              <Button type="primary" disabled={busy} onClick={() => requestLeave(beginCreate)}>
                新建数据源
              </Button>
            )}
          </div>

          <Card className="data-source-catalog-card">
            <Table<DataSourceSummary>
              aria-label="数据源列表"
              loading={catalogLoading}
              columns={[
                {
                  key: 'name',
                  title: '名称',
                  render: (_, source) => (
                    <Space>
                      {source.name}
                      {source.credentialsRequired && <Tag color="orange">待配置密码</Tag>}
                    </Space>
                  ),
                },
                {
                  key: 'type',
                  title: '类型',
                  render: (_, dataSource) => <Tag>{typeLabel(dataSource.configuration.type)}</Tag>,
                },
                {
                  key: 'host',
                  title: 'IP / 主机',
                  render: (_, { configuration }) =>
                    configuration.type === 'KAFKA'
                      ? configuration.bootstrapServers.join(', ')
                      : configuration.host,
                },
                {
                  key: 'port',
                  title: '端口',
                  width: 100,
                  render: (_, { configuration }) =>
                    configuration.type === 'KAFKA'
                      ? [
                          ...new Set(
                            configuration.bootstrapServers.map((server) =>
                              server.slice(server.lastIndexOf(':') + 1),
                            ),
                          ),
                        ].join(', ')
                      : configuration.port,
                },
                {
                  key: 'actions',
                  title: '操作',
                  width: administrator ? 220 : 100,
                  render: (_, dataSource) => (
                    <div className="flex items-center gap-1 whitespace-nowrap">
                      <Button
                        aria-label={`${dataSource.name} ${administrator ? '编辑' : '详情'}`}
                        type="link"
                        onClick={() => requestLeave(() => selectDataSource(dataSource))}
                      >
                        {administrator ? '编辑' : '详情'}
                      </Button>
                      {administrator && (
                        <Button
                          aria-label={`${dataSource.name} 复制`}
                          type="link"
                          disabled={busy}
                          onClick={() => requestLeave(() => requestCopy(dataSource))}
                        >
                          复制
                        </Button>
                      )}
                      {administrator && (
                        <Button
                          aria-label={`${dataSource.name} 删除`}
                          type="link"
                          danger
                          disabled={busy}
                          onClick={() => requestLeave(() => requestDelete(dataSource))}
                        >
                          删除
                        </Button>
                      )}
                    </div>
                  ),
                },
              ]}
              dataSource={[...dataSources]}
              locale={{
                emptyText:
                  catalogQuery.q.trim() !== '' || catalogQuery.type !== undefined
                    ? '没有匹配的数据源'
                    : administrator
                      ? '当前尚未登记数据源。'
                      : '管理员尚未向你授予可用数据源。',
              }}
              pagination={{
                current: catalogQuery.page,
                pageSize: catalogQuery.pageSize,
                total: catalogTotal,
                pageSizeOptions: [10, 20, 50],
                showSizeChanger: { 'aria-label': '数据源每页条数' },
                onChange: (page, pageSize) => {
                  catalogRequest.current += 1;
                  setCatalogQuery((current) => ({
                    ...current,
                    page: pageSize === current.pageSize ? page : 1,
                    pageSize: pageSize as 10 | 20 | 50,
                  }));
                },
              }}
              rowKey="id"
              scroll={{ x: 850 }}
            />
          </Card>
        </>
      )}
      {holder}

      {editorOpen && (
        <div className="data-source-editor">
          <header className="data-source-editor-heading">
            <NavigationButton variant="outline" onClick={closeEditor}>
              <ArrowLeft aria-hidden className="size-4" />
              返回列表
            </NavigationButton>
            <div className="data-source-editor-title">
              <h2>
                {selectedDataSource === null
                  ? '新建数据源'
                  : administrator
                    ? '编辑数据源'
                    : '数据源详情'}
              </h2>
              <InfoTip
                label="数据源配置说明"
                text="保存后再测试连接。连接地址须可从平台容器访问，localhost 指平台容器自身。离开前会提醒保存尚未提交的修改。"
              />
              {selectedDataSource !== null && (
                <Tag icon={<Database aria-hidden size={14} />}>
                  {typeLabel(selectedDataSource.configuration.type)}
                  <LockKeyhole aria-hidden size={12} />
                </Tag>
              )}
              {selectedDataSource !== null && (
                <span className="data-source-context">{selectedDataSource.name}</span>
              )}
            </div>
          </header>
          <div className="data-source-editor-layout">
            <nav aria-label="数据源设置分组" className="data-source-editor-nav">
              {[
                { key: 'connection', label: '连接配置', icon: Link2, disabled: false },
                ...(administrator
                  ? [
                      {
                        key: 'grants',
                        label: '使用授权',
                        icon: Users,
                        disabled: selectedDataSource === null,
                      },
                    ]
                  : []),
              ].map(({ key, label, icon: Icon, disabled }) => (
                <NavigationButton
                  key={key}
                  variant="ghost"
                  aria-current={editorSection === key ? 'page' : undefined}
                  disabled={disabled}
                  className={editorSection === key ? 'is-active' : ''}
                  onClick={() => setEditorSection(key)}
                >
                  <Icon aria-hidden className="size-4" />
                  {label}
                </NavigationButton>
              ))}
              {selectedDataSource === null && <p>保存后可管理使用授权。</p>}
            </nav>
            <div className="data-source-editor-content">
              <div hidden={editorSection !== 'connection'}>
                <form
                  id="data-source-configuration"
                  autoComplete="off"
                  className="data-source-form"
                  onSubmit={submitEditor}
                >
                  <fieldset disabled={!administrator || (busy && pendingAction !== null)}>
                    <legend>基本信息</legend>
                    <div className="data-source-fields">
                      <label>
                        <span>名称</span>
                        <Input
                          onChange={(event) => setDraft({ ...draft, name: event.target.value })}
                          required
                          value={draft.name}
                        />
                      </label>
                      {selectedDataSource === null && (
                        <label>
                          <span>类型</span>
                          <select
                            disabled={selectedDataSource !== null}
                            onChange={(event) =>
                              setDraft({
                                ...draft,
                                sourceType: event.target.value as DataSourceType | '',
                              })
                            }
                            required
                            value={draft.sourceType}
                          >
                            <option value="">请选择类型</option>
                            <option value="POSTGRES">PostgreSQL</option>
                            <option value="KAFKA">Kafka</option>
                            <option value="MYSQL">MySQL</option>
                            <option value="DORIS">Doris</option>
                          </select>
                        </label>
                      )}
                    </div>
                  </fieldset>
                  <fieldset disabled={!administrator || (busy && pendingAction !== null)}>
                    <legend>连接信息</legend>
                    <div className="data-source-fields">
                      {draft.sourceType === 'KAFKA' ? (
                        <>
                          <label className="full-width">
                            <span>Kafka 服务地址</span>
                            <Input
                              onChange={(event) =>
                                setDraft({ ...draft, bootstrapServers: event.target.value })
                              }
                              placeholder="kafka-a.example:9092, kafka-b.example:9092"
                              required
                              value={draft.bootstrapServers}
                            />
                          </label>
                        </>
                      ) : (
                        <>
                          <div className="data-source-host-port full-width">
                            <label>
                              <span>主机地址</span>
                              <Input
                                onChange={(event) =>
                                  setDraft({ ...draft, host: event.target.value })
                                }
                                required
                                value={draft.host}
                              />
                            </label>
                            <label>
                              <span>端口</span>
                              <Input
                                inputMode="numeric"
                                onChange={(event) =>
                                  setDraft({ ...draft, port: event.target.value })
                                }
                                required
                                value={draft.port}
                              />
                            </label>
                          </div>
                          <label>
                            <span>数据库</span>
                            <Input
                              onChange={(event) =>
                                setDraft({ ...draft, database: event.target.value })
                              }
                              required
                              value={draft.database}
                            />
                          </label>
                          <label>
                            <span>用户名</span>
                            <Input
                              autoComplete="off"
                              name="data-source-username"
                              onChange={(event) =>
                                setDraft({ ...draft, username: event.target.value })
                              }
                              required
                              value={draft.username}
                            />
                          </label>
                          {databaseSource && selectedDataSource !== null && (
                            <div className="full-width data-source-password">
                              <span>连接密码</span>
                              {draft.connectionMode === 'RETAIN' ? (
                                <div className="data-source-saved-password">
                                  <span>
                                    <LockKeyhole aria-hidden size={16} />
                                    已保存连接密码
                                  </span>
                                  <Button
                                    type="link"
                                    onClick={() =>
                                      setDraft({
                                        ...draft,
                                        connectionMode: 'REPLACE',
                                        password: '',
                                      })
                                    }
                                  >
                                    更换密码
                                  </Button>
                                </div>
                              ) : (
                                <div className="data-source-password-input">
                                  <Input.Password
                                    aria-label="新连接密码"
                                    autoComplete="new-password"
                                    required
                                    value={draft.password}
                                    onChange={(event) =>
                                      setDraft({ ...draft, password: event.target.value })
                                    }
                                  />
                                  <Button
                                    type="link"
                                    disabled={selectedDataSource.credentialsRequired === true}
                                    onClick={() =>
                                      setDraft({ ...draft, connectionMode: 'RETAIN', password: '' })
                                    }
                                  >
                                    保留原密码
                                  </Button>
                                </div>
                              )}
                            </div>
                          )}
                          {selectedDataSource === null && (
                            <label className="full-width">
                              <span>连接密码</span>
                              <Input.Password
                                autoComplete="new-password"
                                required
                                value={draft.password}
                                onChange={(event) =>
                                  setDraft({ ...draft, password: event.target.value })
                                }
                              />
                            </label>
                          )}
                        </>
                      )}
                    </div>
                  </fieldset>
                  <fieldset disabled={!administrator || (busy && pendingAction !== null)}>
                    <legend>安全设置</legend>
                    {draft.sourceType === 'KAFKA' ? (
                      <label className="checkbox-label">
                        <input
                          type="checkbox"
                          checked={draft.plaintextRiskAcknowledged}
                          onChange={(event) =>
                            setDraft({ ...draft, plaintextRiskAcknowledged: event.target.checked })
                          }
                        />
                        我确认当前 Kafka 使用明文传输（PLAINTEXT），并接受该风险。
                      </label>
                    ) : (
                      <div className="data-source-fields">
                        <label className="data-source-tls">
                          <span>TLS 模式</span>
                          <select
                            onChange={(event) =>
                              setDraft({ ...draft, tlsMode: event.target.value as TlsMode })
                            }
                            required
                            value={draft.tlsMode}
                          >
                            <option value="">请选择 TLS 模式</option>
                            <option value="REQUIRE">加密连接（REQUIRE）</option>
                            <option value="VERIFY_FULL">
                              加密并验证证书与主机名（VERIFY_FULL）
                            </option>
                            <option value="DISABLE">不加密（DISABLE，需确认风险）</option>
                          </select>
                        </label>
                        {draft.tlsMode === 'VERIFY_FULL' && (
                          <label className="full-width">
                            <span>CA 证书</span>
                            <textarea
                              onChange={(event) =>
                                setDraft({ ...draft, caCertificate: event.target.value })
                              }
                              required
                              rows={5}
                              value={draft.caCertificate}
                            />
                          </label>
                        )}
                        {draft.tlsMode === 'DISABLE' && (
                          <label className="checkbox-label full-width">
                            <input
                              checked={draft.disableRiskAcknowledged}
                              onChange={(event) =>
                                setDraft({
                                  ...draft,
                                  disableRiskAcknowledged: event.target.checked,
                                })
                              }
                              type="checkbox"
                            />
                            确认使用明文连接
                            <InfoTip
                              label="明文连接风险"
                              text="未启用 TLS，数据将以明文方式传输；请确认网络环境安全，并接受数据泄露风险。"
                            />
                          </label>
                        )}
                      </div>
                    )}
                  </fieldset>
                </form>

                {selectedDataSource !== null && (
                  <>
                    <section className="data-source-connection-test">
                      <div className="data-source-test-toolbar">
                        <span>连接测试</span>
                        <div className="data-source-test-action">
                          <Button
                            loading={connectionTesting}
                            disabled={busy || dirty}
                            onClick={runConnectionTest}
                          >
                            测试连接
                          </Button>
                          {!dirty &&
                            !connectionTesting &&
                            selectedTestResult?.state === 'SUCCEEDED' && (
                              <span role="status" aria-label="连接测试成功" title="连接测试成功">
                                <CircleCheck
                                  size={18}
                                  className="text-green-600 dark:text-green-400"
                                  aria-hidden="true"
                                />
                              </span>
                            )}
                        </div>
                        <span className="text-sm text-muted-foreground">
                          {dirty ? '请先保存后测试' : '使用已保存的连接配置'}
                          <InfoTip
                            label="连接测试说明"
                            text="每次测试重新连接并记录脱敏结果；历史成功不代表当前可用。"
                          />
                        </span>
                      </div>
                      {!dirty && !connectionTesting && selectedTestResult?.state === 'FAILED' && (
                        <Alert
                          type="error"
                          showIcon
                          role="status"
                          title={`连接测试失败：${selectedTestResult.errorSummary}`}
                        />
                      )}
                    </section>

                    {selectedDataSource.configuration.type === 'MYSQL' && (
                      <section className="data-source-mysql-check">
                        <div className="metadata-selector-heading">
                          <h3>
                            MySQL CDC 基础检查
                            <InfoTip
                              label="MySQL CDC 检查说明"
                              text="检查已保存连接的实例身份、server_id、Binlog、ROW/FULL 配置与当前位点，不修改数据库配置。任务启动时仍需验证表读取及 REPLICATION SLAVE 权限；基础检查通过不代表同步任务已运行。"
                            />
                          </h3>
                          <Button
                            disabled={busy || dirty || mysqlInspectionBusy}
                            loading={mysqlInspectionBusy}
                            onClick={inspectMysqlCdc}
                          >
                            检查 Binlog
                          </Button>
                        </div>
                        {dirty && <p>请先保存连接配置，再执行检查。</p>}
                        {selectedMysqlInspection !== null && (
                          <>
                            <div className="metadata-selector-heading">
                              <Tag
                                color={
                                  selectedMysqlInspection.checks.every(
                                    (check) => check.state === 'PASSED',
                                  )
                                    ? 'success'
                                    : 'warning'
                                }
                              >
                                {selectedMysqlInspection.checks.every(
                                  (check) => check.state === 'PASSED',
                                )
                                  ? '基础检查通过'
                                  : '需要调整'}
                              </Tag>
                              <code>
                                {selectedMysqlInspection.position === null
                                  ? '未取得 Binlog 位点'
                                  : `${selectedMysqlInspection.position.filename}:${selectedMysqlInspection.position.position}`}
                              </code>
                            </div>
                            <Table
                              size="small"
                              pagination={false}
                              rowKey="key"
                              dataSource={selectedMysqlInspection.checks}
                              columns={[
                                {
                                  title: '状态',
                                  dataIndex: 'state',
                                  width: 88,
                                  render: (state: string) => (
                                    <Tag color={state === 'PASSED' ? 'success' : 'error'}>
                                      {state === 'PASSED' ? '通过' : '未通过'}
                                    </Tag>
                                  ),
                                },
                                { title: '检查结果', dataIndex: 'summary' },
                              ]}
                            />
                          </>
                        )}
                      </section>
                    )}
                  </>
                )}
              </div>

              {selectedDataSource !== null && (
                <>
                  {administrator && editorSection === 'grants' && (
                    <section className="data-source-grants">
                      <div className="data-source-grant-controls">
                        <Select
                          aria-label="开发者"
                          placeholder="选择开发者"
                          showSearch={{ optionFilterProp: 'label' }}
                          allowClear
                          disabled={busy || grantsLoading}
                          value={grantUserId || undefined}
                          onChange={(value: string | undefined) => setGrantUserId(value ?? '')}
                          options={grantCandidates.map((user) => ({
                            value: user.id,
                            label: `${user.displayName}（${user.username}）`,
                          }))}
                          notFoundContent="暂无可选开发者"
                        />
                        <Button
                          type="primary"
                          disabled={busy || grantsLoading || grantUserId.length === 0}
                          onClick={requestGrant}
                          htmlType="button"
                        >
                          授予使用权
                        </Button>
                      </div>
                      <div className="grant-list" role="list" aria-label="当前数据源授权">
                        {grantsLoading ? (
                          <p role="status">正在读取授权…</p>
                        ) : grants.length === 0 ? (
                          <p>当前没有开发者授权。</p>
                        ) : (
                          grants.map((grant) => (
                            <div key={grant.user.id} role="listitem">
                              <span>
                                {grant.user.displayName}（{grant.user.username}）
                              </span>
                              <Button
                                type="link"
                                danger
                                disabled={busy}
                                onClick={() => {
                                  setPendingAction({
                                    dataSource: selectedDataSource,
                                    description: `将立即撤销 ${grant.user.displayName} 对 ${selectedDataSource.name} 的使用权；不会自动停止现有任务。`,
                                    grant,
                                    kind: 'REVOKE',
                                  });
                                  setPendingActionConfirmed(false);
                                }}
                                htmlType="button"
                              >
                                撤销
                              </Button>
                            </div>
                          ))
                        )}
                      </div>
                    </section>
                  )}
                </>
              )}
            </div>
          </div>
          <footer className="data-source-editor-footer">
            <span className="data-source-save-state">
              {dirty
                ? '有未保存的修改'
                : selectedDataSource === null
                  ? '尚未创建'
                  : '当前配置已保存'}
            </span>
            <div className="flex items-center gap-2">
              <Button disabled={busy && pendingAction !== null} onClick={closeEditor}>
                {administrator ? '取消' : '关闭详情'}
              </Button>
              {administrator &&
                (editorSection === 'connection' ? (
                  <Button
                    type="primary"
                    htmlType="submit"
                    form="data-source-configuration"
                    disabled={busy || !dirty}
                  >
                    {selectedDataSource === null ? '创建数据源' : '保存修改'}
                  </Button>
                ) : (
                  <Button onClick={() => setEditorSection('connection')}>返回连接配置</Button>
                ))}
            </div>
          </footer>
        </div>
      )}

      {copySource !== null && (
        <Modal
          open
          title="复制数据源"
          okText="确认复制"
          cancelText="取消"
          confirmLoading={busy}
          closable={!busy}
          mask={{ closable: false }}
          keyboard={!busy}
          onCancel={() => {
            if (!busy) setCopySource(null);
          }}
          onOk={confirmCopy}
        >
          <p className="mb-4 text-sm text-muted-foreground">
            沿用连接配置和密码，不复制用户授权。复制后可通过编辑调整连接配置。
          </p>
          <label htmlFor="copy-data-source-name">新数据源名称</label>
          <Input
            id="copy-data-source-name"
            value={copyName}
            maxLength={128}
            disabled={busy}
            onChange={(event) => setCopyName(event.target.value)}
            onPressEnter={() => {
              if (!busy) confirmCopy();
            }}
          />
        </Modal>
      )}

      {pendingAction !== null && (
        <Modal
          open
          title={pendingAction.kind === 'CREATE' ? '确认创建数据源' : '确认数据源操作'}
          footer={null}
          closable={!busy}
          mask={{ closable: false }}
          keyboard={!busy}
          onCancel={() => {
            if (busy) return;
            setPendingAction(null);
            setPendingActionConfirmed(false);
          }}
        >
          <p>{pendingAction.description}</p>
          {(pendingAction.kind === 'CREATE' || pendingAction.kind === 'UPDATE') && (
            <p>
              目标地址：
              {pendingAction.request.configuration.type === 'KAFKA'
                ? pendingAction.request.configuration.bootstrapServers.join(', ')
                : `${pendingAction.request.configuration.host}:${pendingAction.request.configuration.port}/${pendingAction.request.configuration.database}`}
            </p>
          )}
          <label className="checkbox-label">
            <input
              checked={pendingActionConfirmed}
              onChange={(event) => setPendingActionConfirmed(event.target.checked)}
              type="checkbox"
            />
            我已了解此操作的影响，并确认继续。
          </label>
          <div className="user-actions confirmation-actions">
            <button
              className="secondary-button"
              disabled={busy}
              onClick={() => {
                setPendingAction(null);
                setPendingActionConfirmed(false);
              }}
              type="button"
            >
              取消
            </button>
            <button
              className="primary-button"
              disabled={busy || !pendingActionConfirmed}
              onClick={confirmPendingAction}
              type="button"
            >
              {busy ? '正在处理…' : pendingAction.kind === 'CREATE' ? '确认创建' : '确认执行'}
            </button>
          </div>
        </Modal>
      )}
      {prompt}
    </section>
  );
}
