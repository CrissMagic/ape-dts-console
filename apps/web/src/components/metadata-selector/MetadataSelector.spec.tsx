import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { DataSourceSummary, MetadataResponse } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { dataSourcesApi } from '../../api/platform-api';
import { MetadataSelector } from './MetadataSelector';

const postgresSource: DataSourceSummary = {
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
  id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c2001',
  name: 'business-postgres',
  rowVersion: '1',
  state: 'ACTIVE',
  updatedAt: '2026-09-02T08:00:00.000Z',
};

const replacementPostgresSource: DataSourceSummary = {
  ...postgresSource,
  id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c2002',
  name: 'replacement-postgres',
};

function databases(items: readonly { identity: string; name: string }[]): MetadataResponse {
  return {
    attemptedAt: '2026-09-02T08:01:00.000Z',
    dataSourceId: postgresSource.id,
    items: items.map((item) => ({ attributes: {}, identity: item.identity, name: item.name })),
    refreshedAt: '2026-09-02T08:01:01.000Z',
    resource: 'DATABASES',
    state: 'SUCCESS',
  };
}

function emptyDatabases(): MetadataResponse {
  return {
    attemptedAt: '2026-09-02T08:01:00.000Z',
    dataSourceId: postgresSource.id,
    refreshedAt: '2026-09-02T08:01:01.000Z',
    resource: 'DATABASES',
    state: 'EMPTY',
  };
}

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

describe('MetadataSelector', () => {
  it('keeps the initial state distinct from a successful empty collection', async () => {
    const metadata = vi.spyOn(dataSourcesApi, 'metadata').mockResolvedValue(emptyDatabases());
    render(<MetadataSelector dataSource={postgresSource} />);

    expect(screen.getByRole('status')).toHaveTextContent('尚未读取元数据');
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));

    await waitFor(() =>
      expect(metadata).toHaveBeenCalledWith(postgresSource, { resource: 'DATABASES' }),
    );
    expect(screen.getByRole('status')).toHaveTextContent('没有可选数据库');
    expect(screen.queryByRole('list', { name: '可选元数据' })).not.toBeInTheDocument();
  });

  it('does not let a completed stale request overwrite a changed query', async () => {
    const pending = deferred<MetadataResponse>();
    vi.spyOn(dataSourcesApi, 'metadata').mockReturnValueOnce(pending.promise);
    render(<MetadataSelector dataSource={postgresSource} />);

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));
    expect(screen.getByRole('status')).toHaveTextContent('正在从当前数据源实时读取');
    fireEvent.change(screen.getByLabelText('读取对象'), { target: { value: 'SCHEMAS' } });

    await act(async () => {
      pending.resolve(databases([{ identity: '16384', name: 'business' }]));
      await pending.promise;
    });

    expect(screen.getByRole('status')).toHaveTextContent('查询条件已改变');
    expect(screen.queryByRole('button', { name: 'business' })).not.toBeInTheDocument();
  });

  it('does not let a completed request for a replaced data source overwrite the new source state', async () => {
    const pending = deferred<MetadataResponse>();
    vi.spyOn(dataSourcesApi, 'metadata').mockReturnValueOnce(pending.promise);
    const view = render(<MetadataSelector dataSource={postgresSource} />);

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));
    view.rerender(<MetadataSelector dataSource={replacementPostgresSource} />);

    await act(async () => {
      pending.resolve(databases([{ identity: '16384', name: 'business' }]));
      await pending.promise;
    });

    expect(screen.getByRole('status')).toHaveTextContent('尚未读取元数据');
    expect(screen.getByText('replacement-postgres')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'business' })).not.toBeInTheDocument();
  });

  it('clears a selected object when a refresh cannot prove the same stable identity', async () => {
    const onSelectionInvalidated = vi.fn();
    vi.spyOn(dataSourcesApi, 'metadata')
      .mockResolvedValueOnce(databases([{ identity: '16384', name: 'business' }]))
      .mockResolvedValueOnce(databases([{ identity: '16385', name: 'business' }]));
    render(
      <MetadataSelector
        dataSource={postgresSource}
        onSelectionInvalidated={onSelectionInvalidated}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));
    const item = await screen.findByRole('button', { name: 'business' });
    fireEvent.click(item);
    expect(item).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));
    await waitFor(() =>
      expect(onSelectionInvalidated).toHaveBeenCalledWith({ identity: '16384', name: 'business' }),
    );
    expect(screen.getByRole('button', { name: 'business' })).toHaveAttribute(
      'aria-pressed',
      'false',
    );
  });

  it('clears a selected object when a successful refresh is empty', async () => {
    const onSelectionChange = vi.fn();
    const onSelectionInvalidated = vi.fn();
    vi.spyOn(dataSourcesApi, 'metadata')
      .mockResolvedValueOnce(databases([{ identity: '16384', name: 'business' }]))
      .mockResolvedValueOnce(emptyDatabases());
    render(
      <MetadataSelector
        dataSource={postgresSource}
        onSelectionChange={onSelectionChange}
        onSelectionInvalidated={onSelectionInvalidated}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));
    fireEvent.click(await screen.findByRole('button', { name: 'business' }));
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));

    await waitFor(() =>
      expect(onSelectionInvalidated).toHaveBeenCalledWith({ identity: '16384', name: 'business' }),
    );
    expect(onSelectionChange).toHaveBeenLastCalledWith(null);
    expect(screen.getByRole('status')).toHaveTextContent('没有可选数据库');
  });

  it('shows source permission failure instead of representing it as an empty collection', async () => {
    vi.spyOn(dataSourcesApi, 'metadata').mockResolvedValue({
      attemptedAt: '2026-09-02T08:01:00.000Z',
      dataSourceId: postgresSource.id,
      errorCode: 'PERMISSION_DENIED',
      errorSummary: '当前账号没有执行此操作的权限。',
      resource: 'DATABASES',
      state: 'PERMISSION_DENIED',
    });
    render(<MetadataSelector dataSource={postgresSource} />);

    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }));

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('无权读取'));
    expect(screen.getByRole('status')).not.toHaveTextContent('没有可选');
  });
});
