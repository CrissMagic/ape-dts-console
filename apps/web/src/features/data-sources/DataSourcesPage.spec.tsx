import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import type { AuthenticatedSessionResponse, DataSourceSummary } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfigProvider } from 'antd';

import { dataSourcesApi, PlatformApiError, usersApi } from '../../api/platform-api';
import { DataSourcesPage } from './DataSourcesPage';

const administrator: AuthenticatedSessionResponse = {
  mustChangePassword: false,
  absoluteExpiresAt: '2026-09-02T12:00:00.000Z',
  csrfToken: 'scs-csrf-v1_test-token',
  idleExpiresAt: '2026-09-02T10:30:00.000Z',
  status: 'AUTHENTICATED',
  user: {
    displayName: 'Platform administrator',
    id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c3001',
    role: 'ADMIN',
    status: 'ACTIVE',
    username: 'platform-admin',
  },
};

const createdSource: DataSourceSummary = {
  configuration: {
    database: 'business',
    host: 'postgres.internal',
    port: 5432,
    tls: { mode: 'REQUIRE' },
    type: 'POSTGRES',
    username: 'reader',
  },
  createdAt: '2026-09-02T08:00:00.000Z',
  deletedAt: null,
  id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c3002',
  name: 'business-postgres',
  rowVersion: '1',
  state: 'ACTIVE',
  updatedAt: '2026-09-02T08:00:00.000Z',
};

const otherSource: DataSourceSummary = {
  ...createdSource,
  id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c3003',
  name: 'other-postgres',
};

const mysqlSource: DataSourceSummary = {
  ...createdSource,
  configuration: {
    database: 'business',
    host: 'mysql.test',
    port: 3306,
    tls: { mode: 'REQUIRE' },
    type: 'MYSQL',
    username: 'reader',
  },
  name: 'mysql-source',
};

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('DataSourcesPage', () => {
  it('marks imported sources as needing credentials and does not offer to retain a nonexistent password', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [{ ...createdSource, credentialsRequired: true }],
    });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    await screen.findByText('待配置密码');
    fireEvent.click(screen.getByRole('button', { name: 'business-postgres 编辑' }));
    expect(screen.getByLabelText('新连接密码')).toBeInTheDocument();
    expect(screen.queryByText('已保存连接密码')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '保留原密码' })).toBeDisabled();
  });
  it('uses a dedicated settings page, retains the draft between sections and only tests saved configuration', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const test = vi.spyOn(dataSourcesApi, 'test');
    const metadata = vi.spyOn(dataSourcesApi, 'metadata');
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
    expect(screen.getAllByRole('heading', { name: '编辑数据源' })).toHaveLength(1);
    expect(screen.queryByRole('table', { name: '数据源列表' })).not.toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: /编辑/u })).not.toBeInTheDocument();
    expect(screen.queryByLabelText('类型')).not.toBeInTheDocument();
    expect(screen.getByText('已保存连接密码')).toBeInTheDocument();
    expect(screen.queryByLabelText('新连接密码')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '保存修改' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('主机地址'), { target: { value: 'new-host.internal' } });
    expect(screen.getByRole('button', { name: '测试连接' })).toBeDisabled();
    expect(screen.getByText('请先保存后测试')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '元数据' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '使用授权' }));
    expect(screen.getByRole('combobox', { name: '开发者' })).toBeInTheDocument();
    expect(screen.queryByText('LIVE METADATA')).not.toBeInTheDocument();
    expect(metadata).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '连接配置' }));
    expect(screen.getByLabelText('主机地址')).toHaveValue('new-host.internal');
    expect(test).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '继续编辑' }));
    expect(screen.getByLabelText('主机地址')).toHaveValue('new-host.internal');
  });

  it('replaces a password only when explicitly requested and clears the replacement when retaining it again', async () => {
    const updated = { ...createdSource, name: 'renamed-source', rowVersion: '2' };
    vi.spyOn(dataSourcesApi, 'list')
      .mockResolvedValueOnce({ dataSources: [createdSource] })
      .mockResolvedValue({ dataSources: [updated] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const update = vi.spyOn(dataSourcesApi, 'update').mockResolvedValue(updated);
    const test = vi.spyOn(dataSourcesApi, 'test');
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
    fireEvent.click(screen.getByRole('button', { name: '更换密码' }));
    const password = screen.getByLabelText('新连接密码');
    fireEvent.change(password, { target: { value: 'discarded-test-password' } });
    expect(password).toHaveAttribute('type', 'password');
    fireEvent.click(screen.getByRole('button', { name: '使用授权' }));
    fireEvent.click(screen.getByRole('button', { name: '连接配置' }));
    expect(screen.getByLabelText('新连接密码')).toHaveValue('discarded-test-password');
    fireEvent.click(screen.getByRole('button', { name: '保留原密码' }));
    expect(screen.queryByLabelText('新连接密码')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '保存修改' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '更换密码' }));
    expect(screen.getByLabelText('新连接密码')).toHaveValue('');
    fireEvent.click(screen.getByRole('button', { name: '保留原密码' }));
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: updated.name } });
    await waitFor(() => expect(screen.getByRole('button', { name: '保存修改' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: '保存修改' }));
    expect(update).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认执行' }));
    await waitFor(() =>
      expect(update).toHaveBeenCalledExactlyOnceWith(createdSource, {
        configuration: createdSource.configuration,
        credentials: { mode: 'RETAIN' },
        expectedRowVersion: '1',
        name: updated.name,
      }),
    );
    expect(await screen.findByText('数据源配置已保存，请重新测试连接。')).toBeInTheDocument();
    expect(test).not.toHaveBeenCalled();
  });

  it('shows readonly connection details without password replacement or grants for developers', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    render(
      <DataSourcesPage
        onSessionInvalidated={vi.fn()}
        session={{ ...administrator, user: { ...administrator.user, role: 'DEVELOPER' } }}
      />,
    );
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 详情' }));
    expect(screen.getByLabelText('名称')).toBeDisabled();
    expect(screen.getByLabelText('主机地址')).toBeDisabled();
    expect(screen.getByRole('button', { name: '更换密码' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: '使用授权' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '保存修改' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '元数据' })).not.toBeInTheDocument();
  });

  it('submits an explicitly entered replacement password only after confirmation', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const update = vi
      .spyOn(dataSourcesApi, 'update')
      .mockResolvedValue({ ...createdSource, rowVersion: '2' });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
    fireEvent.click(screen.getByRole('button', { name: '更换密码' }));
    fireEvent.change(screen.getByLabelText('新连接密码'), {
      target: { value: 'replacement-test-password' },
    });
    await waitFor(() => expect(screen.getByRole('button', { name: '保存修改' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: '保存修改' }));
    expect(screen.getByText(/旧密码将从平台中销毁/u)).toBeInTheDocument();
    expect(update).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认执行' }));
    await waitFor(() =>
      expect(update).toHaveBeenCalledExactlyOnceWith(createdSource, {
        name: createdSource.name,
        configuration: createdSource.configuration,
        expectedRowVersion: '1',
        credentials: { mode: 'REPLACE', password: 'replacement-test-password' },
      }),
    );
    await waitFor(() => expect(screen.queryByLabelText('新连接密码')).not.toBeInTheDocument());
  });

  it('keeps CA verification settings when editing a certificate-backed source', async () => {
    const source: DataSourceSummary = {
      ...createdSource,
      configuration: {
        ...createdSource.configuration,
        type: 'POSTGRES',
        database: 'business',
        host: 'postgres.internal',
        port: 5432,
        username: 'reader',
        tls: { mode: 'VERIFY_FULL', caCertificate: 'test-ca-certificate' },
      },
    };
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [source] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
    expect(screen.getByLabelText('CA 证书')).toHaveValue('test-ca-certificate');
    fireEvent.change(screen.getByLabelText('CA 证书'), { target: { value: 'new-test-ca' } });
    fireEvent.click(screen.getByRole('button', { name: '使用授权' }));
    fireEvent.click(screen.getByRole('button', { name: '连接配置' }));
    expect(screen.getByLabelText('CA 证书')).toHaveValue('new-test-ca');
    expect(screen.getByLabelText('TLS 模式')).toHaveValue('VERIFY_FULL');
  });

  it('confirms developer grants in their own section without discarding connection edits', async () => {
    const user = {
      ...administrator.user,
      id: otherSource.id,
      role: 'DEVELOPER' as const,
      displayName: 'Developer',
      username: 'developer',
    };
    const granted = {
      dataSourceId: createdSource.id,
      user,
      grantedBy: administrator.user,
      grantedAt: createdSource.createdAt,
    };
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    vi.spyOn(dataSourcesApi, 'listGrants')
      .mockResolvedValueOnce({ grants: [] })
      .mockResolvedValue({ grants: [granted] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [user] });
    const grant = vi.spyOn(dataSourcesApi, 'grant').mockResolvedValue(granted);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: 'unsaved-name' } });
    fireEvent.click(screen.getByRole('button', { name: '使用授权' }));
    await waitFor(() => expect(screen.getByRole('combobox', { name: '开发者' })).toBeEnabled());
    expect(screen.getByRole('button', { name: '授予使用权' })).toBeDisabled();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '开发者' }));
    fireEvent.click(await screen.findByText('Developer（developer）'));
    fireEvent.click(screen.getByRole('button', { name: '授予使用权' }));
    expect(grant).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认执行' }));
    await waitFor(() => expect(grant).toHaveBeenCalledExactlyOnceWith(createdSource, user.id));
    await waitFor(() =>
      expect(screen.getByRole('list', { name: '当前数据源授权' })).toHaveTextContent('Developer'),
    );
    fireEvent.click(screen.getByRole('button', { name: '连接配置' }));
    expect(screen.getByLabelText('名称')).toHaveValue('unsaved-name');
  });

  it('keeps Kafka bootstrap servers and plaintext acknowledgement when editing its name', async () => {
    const kafka: DataSourceSummary = {
      ...createdSource,
      name: 'kafka-source',
      configuration: {
        type: 'KAFKA',
        bootstrapServers: ['broker-a:9092', 'broker-b:9092'],
        plaintextRiskAcknowledged: true,
      },
    };
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [kafka] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const update = vi
      .spyOn(dataSourcesApi, 'update')
      .mockResolvedValue({ ...kafka, name: 'new-kafka', rowVersion: '2' });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'kafka-source 编辑' }));
    expect(screen.getByLabelText('Kafka 服务地址')).toHaveValue('broker-a:9092, broker-b:9092');
    expect(screen.queryByLabelText('主机地址')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '更换密码' })).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: 'new-kafka' } });
    await waitFor(() => expect(screen.getByRole('button', { name: '保存修改' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: '保存修改' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认执行' }));
    await waitFor(() =>
      expect(update).toHaveBeenCalledExactlyOnceWith(kafka, {
        name: 'new-kafka',
        configuration: kafka.configuration,
        credentials: { mode: 'NONE' },
        expectedRowVersion: '1',
      }),
    );
  });

  it('copies a source with a new name and without requesting its password', async () => {
    const copied = { ...createdSource, id: otherSource.id, name: 'business-copy' };
    vi.spyOn(dataSourcesApi, 'list')
      .mockResolvedValueOnce({ dataSources: [createdSource] })
      .mockResolvedValue({ dataSources: [createdSource, copied] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const result = deferred<DataSourceSummary>();
    const copy = vi.spyOn(dataSourcesApi, 'copy').mockReturnValue(result.promise);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    const copyButton = await screen.findByRole('button', { name: 'business-postgres 复制' });
    await waitFor(() => expect(copyButton).toBeEnabled());
    fireEvent.click(copyButton);
    expect(screen.getByLabelText('新数据源名称')).toHaveValue('business-postgres-副本');
    expect(screen.queryByLabelText('连接密码')).not.toBeInTheDocument();
    expect(copy).not.toHaveBeenCalled();
    fireEvent.change(screen.getByLabelText('新数据源名称'), { target: { value: 'business-copy' } });
    fireEvent.click(screen.getByRole('button', { name: '确认复制' }));
    fireEvent.click(screen.getByRole('button', { name: /确认复制/u }));
    expect(copy).toHaveBeenCalledExactlyOnceWith(createdSource, { name: 'business-copy' });
    await act(async () => result.resolve(copied));
    expect(await screen.findByText('business-copy')).toBeInTheDocument();
    expect(screen.getByText('business-postgres')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: '复制数据源' })).not.toBeInTheDocument(),
    );
  });

  it('preserves the copy name on conflict so the user can correct and retry', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const copy = vi
      .spyOn(dataSourcesApi, 'copy')
      .mockRejectedValueOnce(new PlatformApiError('DATA_SOURCE_NAME_ALREADY_EXISTS', 409))
      .mockResolvedValue(otherSource);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    const copyButton = await screen.findByRole('button', { name: 'business-postgres 复制' });
    await waitFor(() => expect(copyButton).toBeEnabled());
    fireEvent.click(copyButton);
    fireEvent.click(screen.getByRole('button', { name: '确认复制' }));
    expect(await screen.findByText('数据源名称已存在，请使用其他名称。')).toBeInTheDocument();
    expect(screen.getByLabelText('新数据源名称')).toHaveValue('business-postgres-副本');
    fireEvent.change(screen.getByLabelText('新数据源名称'), {
      target: { value: 'other-postgres' },
    });
    await waitFor(() => expect(screen.getByRole('button', { name: /确认复制/u })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: /确认复制/u }));
    await waitFor(() =>
      expect(copy).toHaveBeenLastCalledWith(createdSource, { name: 'other-postgres' }),
    );
  });

  it('only offers details to non-administrators', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    render(
      <DataSourcesPage
        onSessionInvalidated={vi.fn()}
        session={{ ...administrator, user: { ...administrator.user, role: 'DEVELOPER' } }}
      />,
    );
    expect(await screen.findByRole('button', { name: 'business-postgres 详情' })).toBeEnabled();
    expect(
      screen.queryByRole('button', { name: 'business-postgres 复制' }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: 'business-postgres 删除' }),
    ).not.toBeInTheDocument();
  });

  it('deletes a source from its table row only after confirmation and hides the version column', async () => {
    vi.spyOn(dataSourcesApi, 'list')
      .mockResolvedValueOnce({ dataSources: [createdSource] })
      .mockResolvedValue({ dataSources: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const remove = vi
      .spyOn(dataSourcesApi, 'remove')
      .mockResolvedValue({ ...createdSource, state: 'DELETED' });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    const button = await screen.findByRole('button', { name: 'business-postgres 删除' });
    expect(screen.queryByRole('columnheader', { name: '版本' })).not.toBeInTheDocument();
    fireEvent.click(button);
    expect(remove).not.toHaveBeenCalled();
    expect(
      screen.queryByRole('dialog', { name: '编辑 business-postgres' }),
    ).not.toBeInTheDocument();
    const confirm = screen.getByRole('button', { name: '确认执行' });
    expect(confirm).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(confirm);
    await waitFor(() => expect(remove).toHaveBeenCalledExactlyOnceWith(createdSource));
    await waitFor(() => expect(screen.queryByText('business-postgres')).not.toBeInTheDocument());
  });

  it('keeps a referenced source and explains why deletion is blocked', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    vi.spyOn(dataSourcesApi, 'remove').mockRejectedValue(
      new PlatformApiError('DATA_SOURCE_REFERENCED_BY_TASK', 409),
    );
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 删除' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认执行' }));
    expect(await screen.findByText(/该数据源仍被任务引用/u)).toBeInTheDocument();
    expect(screen.getByText('business-postgres')).toBeInTheDocument();
  });

  it('can reveal and hide a connection password without submitting the form', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const create = vi.spyOn(dataSourcesApi, 'create');
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: '新建数据源' }));
    fireEvent.change(screen.getByLabelText('类型'), { target: { value: 'POSTGRES' } });
    const password = screen.getByLabelText('连接密码');
    fireEvent.change(password, { target: { value: 'database-secret' } });
    expect(password).toHaveAttribute('type', 'password');
    fireEvent.click(screen.getByRole('img', { name: 'eye-invisible' }));
    expect(password).toHaveAttribute('type', 'text');
    expect(password).toHaveValue('database-secret');
    fireEvent.click(screen.getByRole('img', { name: 'eye' }));
    expect(password).toHaveAttribute('type', 'password');
    expect(create).not.toHaveBeenCalled();
  });

  it('shows database hosts and ports plus complete Kafka bootstrap addresses in the source list', async () => {
    const kafkaSource: DataSourceSummary = {
      ...otherSource,
      name: 'business-kafka',
      configuration: {
        type: 'KAFKA',
        bootstrapServers: ['broker-a.internal:9092', 'broker-b.internal:9093'],
        plaintextRiskAcknowledged: true,
      },
    };
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [createdSource, kafkaSource],
    });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);

    const databaseRow = (await screen.findByText('business-postgres')).closest('tr');
    const kafkaRow = screen.getByText('business-kafka').closest('tr');
    expect(databaseRow).not.toBeNull();
    expect(kafkaRow).not.toBeNull();
    expect(within(databaseRow!).getByText('postgres.internal')).toBeInTheDocument();
    expect(within(databaseRow!).getByText('5432')).toBeInTheDocument();
    expect(
      within(kafkaRow!).getByText('broker-a.internal:9092, broker-b.internal:9093'),
    ).toBeInTheDocument();
    expect(within(kafkaRow!).getByText('9092, 9093')).toBeInTheDocument();
    expect(screen.queryByText('已登记数据源')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '数据源目录' })).not.toBeInTheDocument();
  });

  it('runs a separate MySQL Binlog check and shows formatted facts plus actionable failures', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const inspect = vi.spyOn(dataSourcesApi, 'inspectMysqlCdc').mockResolvedValue({
      identity: { kind: 'MYSQL_DATABASE', serverVersion: '8.0.36', value: 'instance:business' },
      binlog: { enabled: true, format: 'ROW', rowImage: 'MINIMAL', serverId: 1, gtidMode: 'OFF' },
      position: { filename: 'mysql-bin.000003', position: 1234 },
      checks: [
        { key: 'BINLOG_FORMAT', state: 'PASSED', summary: 'Binlog 格式为 ROW。' },
        {
          key: 'BINLOG_ROW_IMAGE',
          state: 'FAILED',
          summary: '需要将 binlog_row_image 设置为 FULL。',
        },
      ],
    });
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: /^mysql-source 编辑$/ }));
    const button = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);
    expect(await screen.findByText('需要将 binlog_row_image 设置为 FULL。')).toBeInTheDocument();
    expect(inspect).toHaveBeenCalledExactlyOnceWith(mysqlSource);
    expect(screen.getByText('mysql-bin.000003:1234')).toBeInTheDocument();
    expect(screen.getByText('需要调整')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('数据库'), { target: { value: 'unsaved_database' } });
    expect(button).toBeDisabled();
  });

  it('does not display a MySQL check result after selecting a different source', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, otherSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const pending = deferred<Awaited<ReturnType<typeof dataSourcesApi.inspectMysqlCdc>>>();
    vi.spyOn(dataSourcesApi, 'inspectMysqlCdc').mockReturnValue(pending.promise);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
    fireEvent.click(await screen.findByRole('button', { name: /^mysql-source 编辑$/ }));
    const button = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^other-postgres 编辑$/ }));
    await act(async () => {
      pending.resolve({
        identity: { kind: 'MYSQL_DATABASE', serverVersion: '8.0.36', value: 'instance:business' },
        binlog: { enabled: true, format: 'ROW', rowImage: 'FULL', serverId: 1, gtidMode: 'OFF' },
        position: { filename: 'mysql-bin.000003', position: 1234 },
        checks: [{ key: 'BINLOG_FORMAT', state: 'PASSED', summary: 'Binlog 格式为 ROW。' }],
      });
      await pending.promise;
    });
    expect(screen.queryByText('mysql-bin.000003:1234')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '检查 Binlog' })).not.toBeInTheDocument();
  });

  it('keeps the latest same-source check when an earlier request completes after reopening', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const oldCheck = deferred<Awaited<ReturnType<typeof dataSourcesApi.inspectMysqlCdc>>>();
    const inspection = {
      identity: {
        kind: 'MYSQL_DATABASE' as const,
        serverVersion: '8.0.36',
        value: 'instance:business',
      },
      binlog: { enabled: true, format: 'ROW', rowImage: 'FULL', serverId: 1, gtidMode: 'OFF' },
      position: { filename: 'mysql-bin.000003', position: 1234 },
      checks: [
        { key: 'BINLOG_FORMAT' as const, state: 'PASSED' as const, summary: 'Binlog 格式为 ROW。' },
      ],
    };
    const inspect = vi
      .spyOn(dataSourcesApi, 'inspectMysqlCdc')
      .mockReturnValueOnce(oldCheck.promise)
      .mockResolvedValue(inspection);
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />
      </ConfigProvider>,
    );
    fireEvent.click(await screen.findByRole('button', { name: /^mysql-source 编辑$/ }));
    const firstButton = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(firstButton).toBeEnabled());
    fireEvent.click(firstButton);
    await waitFor(() => expect(inspect).toHaveBeenCalledOnce());
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^mysql-source 编辑$/ }));
    const secondButton = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(secondButton).toBeEnabled());
    fireEvent.click(secondButton);
    expect(await screen.findByText('mysql-bin.000003:1234')).toBeInTheDocument();
    await act(async () => {
      oldCheck.resolve({
        ...inspection,
        position: { filename: 'mysql-bin.000001', position: 157 },
      });
      await oldCheck.promise;
    });
    expect(screen.getByText('mysql-bin.000003:1234')).toBeInTheDocument();
    expect(screen.queryByText('mysql-bin.000001:157')).not.toBeInTheDocument();
  });

  it('ignores a failed check after leaving its data source and allows retry after a current failure', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [mysqlSource, otherSource] });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    let rejectOld: (error: Error) => void = () => undefined;
    const oldCheck = new Promise<never>((_resolve, reject) => {
      rejectOld = reject;
    });
    const inspect = vi
      .spyOn(dataSourcesApi, 'inspectMysqlCdc')
      .mockReturnValueOnce(oldCheck)
      .mockRejectedValue(new Error('offline'));
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />
      </ConfigProvider>,
    );
    fireEvent.click(await screen.findByRole('button', { name: /^mysql-source 编辑$/ }));
    const button = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^other-postgres 编辑$/ }));
    await act(async () => {
      rejectOld(new Error('old request failed'));
      await oldCheck.catch(() => undefined);
    });
    expect(screen.queryByText('Binlog 检查失败。')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^mysql-source 编辑$/ }));
    const retry = await screen.findByRole('button', { name: '检查 Binlog' });
    await waitFor(() => expect(retry).toBeEnabled());
    fireEvent.click(retry);
    expect(await screen.findByText('Binlog 检查失败。')).toBeInTheDocument();
    await waitFor(() => expect(retry).toBeEnabled());
    expect(inspect).toHaveBeenCalledTimes(2);
  });

  it('protects refresh and reset, and retains a draft after a failed creation', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const create = vi.spyOn(dataSourcesApi, 'create').mockRejectedValue(new Error('offline'));
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />
      </ConfigProvider>,
    );
    fireEvent.click(await screen.findByRole('button', { name: '新建数据源' }));
    expect(screen.getByRole('button', { name: '创建数据源' })).toBeDisabled();
    const cleanUnload = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(cleanUnload);
    expect(cleanUnload.defaultPrevented).toBe(false);
    for (const [label, value] of Object.entries({
      类型: 'POSTGRES',
      名称: 'UX draft',
      主机地址: 'postgres.internal',
      端口: '5432',
      数据库: 'business',
      用户名: 'reader',
      'TLS 模式': 'REQUIRE',
      连接密码: 'test-only-password',
    }))
      fireEvent.change(screen.getByLabelText(label), { target: { value } });
    const dirtyUnload = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(dirtyUnload);
    expect(dirtyUnload.defaultPrevented).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '继续编辑' }));
    await waitFor(() =>
      expect(
        screen.queryByRole('dialog', { name: '放弃未保存的数据源配置？' }),
      ).not.toBeInTheDocument(),
    );
    expect(screen.getByLabelText('名称')).toHaveValue('UX draft');
    fireEvent.click(screen.getByRole('button', { name: '创建数据源' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));
    await waitFor(() => expect(create).toHaveBeenCalledOnce());
    // Ant Design's test IDs are shared by retained hidden dialogs; scope by the visible confirmation content.
    const confirmation = screen
      .getByText('目标地址：postgres.internal:5432/business')
      .closest<HTMLElement>('[role="dialog"]')!;
    await waitFor(() =>
      expect(within(confirmation).getByRole('button', { name: '取消' })).toBeEnabled(),
    );
    const failure = await screen.findByText('操作失败');
    expect(failure.closest('.ant-notification-notice')).toHaveClass('operation-notification-error');
    fireEvent.click(within(confirmation).getByRole('button', { name: '取消' }));
    await waitFor(() =>
      expect(
        screen.queryByText('目标地址：postgres.internal:5432/business'),
      ).not.toBeInTheDocument(),
    );
    expect(screen.getByLabelText('连接密码')).toHaveValue('test-only-password');
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '放弃修改并继续' }));
    fireEvent.click(screen.getByRole('button', { name: '新建数据源' }));
    await waitFor(() => expect(screen.getByLabelText('名称')).toHaveValue(''));
    expect(screen.getByLabelText('连接密码')).toHaveValue('');
    const resetUnload = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(resetUnload);
    expect(resetUnload.defaultPrevented).toBe(false);
  });
  it('requires an explicit confirmation before creating a source with an encrypted connection secret', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const create = vi.spyOn(dataSourcesApi, 'create').mockResolvedValue(createdSource);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);

    await waitFor(() => expect(dataSourcesApi.list).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: '新建数据源' }));
    fireEvent.change(screen.getByLabelText('类型'), { target: { value: 'POSTGRES' } });
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: 'Business PostgreSQL' } });
    fireEvent.change(screen.getByLabelText('主机地址'), { target: { value: 'postgres.internal' } });
    fireEvent.change(screen.getByLabelText('端口'), { target: { value: '5432' } });
    fireEvent.change(screen.getByLabelText('数据库'), { target: { value: 'business' } });
    fireEvent.change(screen.getByLabelText('用户名'), { target: { value: 'reader' } });
    fireEvent.change(screen.getByLabelText('TLS 模式'), { target: { value: 'REQUIRE' } });
    fireEvent.change(screen.getByLabelText('连接密码'), { target: { value: 'secret-password' } });

    fireEvent.click(screen.getByRole('button', { name: '创建数据源' }));

    expect(create).not.toHaveBeenCalled();
    const createDescription = screen.getByText('目标地址：postgres.internal:5432/business');
    expect(createDescription.closest('.ant-modal')).toHaveTextContent('确认创建数据源');

    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认创建' }));

    await waitFor(() =>
      expect(create).toHaveBeenCalledWith({
        configuration: {
          database: 'business',
          host: 'postgres.internal',
          port: 5432,
          tls: { mode: 'REQUIRE' },
          type: 'POSTGRES',
          username: 'reader',
        },
        credentials: { mode: 'SET', password: 'secret-password' },
        name: 'Business PostgreSQL',
      }),
    );
  });

  it.each([true, false])(
    'shows an inline success icon without internal identity (available: %s)',
    async (identityAvailable) => {
      vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [createdSource] });
      vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
      vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
      const success: Awaited<ReturnType<typeof dataSourcesApi.test>> = {
        completedAt: '2026-09-02T08:02:00.000Z',
        dataSourceId: createdSource.id,
        durationMs: 10,
        errorCode: null,
        errorSummary: null,
        id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c3004',
        identity: identityAvailable
          ? { kind: 'POSTGRES_DATABASE', serverVersion: '12.22', value: 'preview-instance' }
          : null,
        purpose: 'MANUAL',
        startedAt: '2026-09-02T08:01:59.990Z',
        state: 'SUCCEEDED',
      };
      const pending = deferred<typeof success>();
      vi.spyOn(dataSourcesApi, 'test')
        .mockResolvedValueOnce(success)
        .mockReturnValueOnce(pending.promise)
        .mockResolvedValueOnce(success);
      render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);
      fireEvent.click(await screen.findByRole('button', { name: 'business-postgres 编辑' }));
      const testButton = screen.getByRole('button', { name: '测试连接' });
      fireEvent.click(testButton);
      const status = await screen.findByRole('status', { name: '连接测试成功' });
      expect(status.parentElement).toBe(testButton.parentElement);
      expect(status.querySelector('svg')).toBeInTheDocument();
      expect(status.closest('.ant-alert')).toBeNull();
      expect(screen.queryByText(/POSTGRES_DATABASE|preview-instance/u)).not.toBeInTheDocument();
      // Ant Design keeps its loading animation briefly after the request finishes.
      await waitFor(() => expect(testButton).not.toHaveClass('ant-btn-loading'));
      fireEvent.click(testButton);
      expect(screen.queryByRole('status', { name: '连接测试成功' })).not.toBeInTheDocument();
      await act(async () => {
        pending.resolve({
          ...success,
          state: 'FAILED',
          identity: null,
          errorCode: 'CONNECTION_FAILED',
          errorSummary: '连接被拒绝',
        });
      });
      expect(await screen.findByText('连接测试失败：连接被拒绝')).toBeInTheDocument();
      expect(screen.queryByRole('status', { name: '连接测试成功' })).not.toBeInTheDocument();
      await waitFor(() => expect(testButton).not.toHaveClass('ant-btn-loading'));
      fireEvent.click(testButton);
      await screen.findByRole('status', { name: '连接测试成功' });
      fireEvent.change(screen.getByLabelText('主机地址'), {
        target: { value: 'changed.internal' },
      });
      expect(screen.queryByRole('status', { name: '连接测试成功' })).not.toBeInTheDocument();
      expect(testButton).toBeDisabled();
    },
  );

  it('ignores a previous visit connection test after switching sources and reopening', async () => {
    const pending = deferred<Awaited<ReturnType<typeof dataSourcesApi.test>>>();
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [createdSource, otherSource],
    });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const connectionTest = vi.spyOn(dataSourcesApi, 'test').mockReturnValue(pending.promise);
    render(<DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />);

    await waitFor(() => expect(dataSourcesApi.list).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: /^business-postgres 编辑$/ }));
    await waitFor(() => expect(dataSourcesApi.listGrants).toHaveBeenCalledWith(createdSource));
    await waitFor(() =>
      expect(screen.getByRole('button', { name: '测试连接' })).not.toBeDisabled(),
    );
    fireEvent.click(screen.getByRole('button', { name: '测试连接' }));
    await waitFor(() => expect(connectionTest).toHaveBeenCalledWith(createdSource));
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^other-postgres 编辑$/ }));

    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(screen.getByRole('button', { name: /^business-postgres 编辑$/ }));

    await act(async () => {
      pending.resolve({
        completedAt: '2026-09-02T08:02:00.000Z',
        dataSourceId: createdSource.id,
        durationMs: 10,
        errorCode: null,
        errorSummary: null,
        id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c3004',
        identity: {
          kind: 'POSTGRES_DATABASE',
          serverVersion: '12.22',
          value: '123456789:business',
        },
        purpose: 'MANUAL',
        startedAt: '2026-09-02T08:01:59.990Z',
        state: 'SUCCEEDED',
      });
      await pending.promise;
    });

    expect(screen.getByRole('heading', { name: '编辑数据源' })).toBeInTheDocument();
    expect(screen.getByLabelText('名称')).toHaveValue('business-postgres');
    expect(screen.queryByRole('status', { name: '连接测试成功' })).not.toBeInTheDocument();
  });

  it('does not warn for an unchanged existing source but protects edits when selecting another', async () => {
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({
      dataSources: [createdSource, otherSource],
    });
    vi.spyOn(dataSourcesApi, 'listGrants').mockResolvedValue({ grants: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <DataSourcesPage onSessionInvalidated={vi.fn()} session={administrator} />
      </ConfigProvider>,
    );
    fireEvent.click(await screen.findByRole('button', { name: /^business-postgres 编辑$/ }));
    const unchanged = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(unchanged);
    expect(unchanged.defaultPrevented).toBe(false);
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: 'edited name' } });
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '继续编辑' }));
    await waitFor(() =>
      expect(
        screen.queryByRole('dialog', { name: '放弃未保存的数据源配置？' }),
      ).not.toBeInTheDocument(),
    );
    expect(screen.getByLabelText('名称')).toHaveValue('edited name');
    fireEvent.click(screen.getByRole('button', { name: '返回列表' }));
    fireEvent.click(await screen.findByRole('button', { name: '放弃修改并继续' }));
    fireEvent.click(screen.getByRole('button', { name: /^other-postgres 编辑$/ }));
    await waitFor(() => expect(screen.getByLabelText('名称')).toHaveValue('other-postgres'));
  });
});
