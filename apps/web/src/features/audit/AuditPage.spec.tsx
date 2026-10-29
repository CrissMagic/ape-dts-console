import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { ConfigProvider } from 'antd';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { auditApi, PlatformApiError } from '../../api/platform-api';
import { AuditPage } from './AuditPage';

const systemEvent = {
  action: 'TASK_RUNTIME_RECOVERED',
  actorType: 'SYSTEM' as const,
  actorUserId: null,
  details: { runId: '33333333-3333-4333-8333-333333333333' },
  eventId: '11111111-1111-4111-8111-111111111111',
  occurredAt: '2026-09-18T08:00:00.000Z',
  sequence: '12',
  subjectId: '22222222-2222-4222-8222-222222222222',
  subjectType: 'TASK',
};

type AuditListResult = Awaited<ReturnType<typeof auditApi.list>>;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('AuditPage', () => {
  it.each([20, 50])(
    'changes the page size to %s and resets cursor history while retaining applied filters',
    async (size) => {
      const list = vi
        .spyOn(auditApi, 'list')
        .mockResolvedValue({ events: [systemEvent], nextCursor: 'cursor-1' });
      render(<AuditPage onSessionInvalidated={vi.fn()} />);
      await screen.findByText('12');
      expect(list).toHaveBeenLastCalledWith({ limit: 10 });
      fireEvent.change(screen.getByLabelText('动作'), {
        target: { value: 'TASK_RUNTIME_RECOVERED' },
      });
      fireEvent.click(screen.getByRole('button', { name: /查询$/u }));
      await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
      fireEvent.click(screen.getByRole('button', { name: '下一页' }));
      await screen.findByText('第 2 页');
      fireEvent.change(screen.getByLabelText('动作'), { target: { value: 'UNAPPLIED_ACTION' } });
      fireEvent.mouseDown(screen.getByRole('combobox', { name: '审计每页条数' }));
      expect(
        screen.getByText('10 条/页', { selector: '.ant-select-item-option-content' }),
      ).toBeInTheDocument();
      expect(
        screen.getByText('20 条/页', { selector: '.ant-select-item-option-content' }),
      ).toBeInTheDocument();
      expect(
        screen.getByText('50 条/页', { selector: '.ant-select-item-option-content' }),
      ).toBeInTheDocument();
      fireEvent.click(
        screen.getByText(`${size} 条/页`, { selector: '.ant-select-item-option-content' }),
      );
      await waitFor(() => expect(list).toHaveBeenCalledTimes(4));
      expect(list).toHaveBeenLastCalledWith({ action: 'TASK_RUNTIME_RECOVERED', limit: size });
      expect(await screen.findByText('第 1 页')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: '上一页' })).toBeDisabled();
      fireEvent.click(screen.getByRole('button', { name: '下一页' }));
      await screen.findByText('第 2 页');
      expect(list).toHaveBeenLastCalledWith({
        action: 'TASK_RUNTIME_RECOVERED',
        cursor: 'cursor-1',
        limit: size,
      });
      fireEvent.click(screen.getByRole('button', { name: /重置$/u }));
      await waitFor(() => expect(list).toHaveBeenCalledTimes(6));
      expect(list).toHaveBeenLastCalledWith({ limit: size });
    },
  );

  it('ignores an in-flight old-size page after changing the page size', async () => {
    const stale = deferred<AuditListResult>();
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValueOnce({ events: [systemEvent], nextCursor: 'old-cursor' })
      .mockImplementationOnce(() => stale.promise)
      .mockResolvedValueOnce({ events: [{ ...systemEvent, sequence: '30' }], nextCursor: null });
    render(<AuditPage onSessionInvalidated={vi.fn()} />);
    await screen.findByText('12');
    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '审计每页条数' }));
    fireEvent.click(screen.getByText('20 条/页', { selector: '.ant-select-item-option-content' }));
    await screen.findByText('30');
    await act(async () => {
      stale.resolve({ events: [{ ...systemEvent, sequence: '11' }], nextCursor: 'stale-cursor' });
    });
    expect(screen.queryByText('11')).not.toBeInTheDocument();
    expect(screen.getByText('30')).toBeInTheDocument();
    expect(screen.getByText('第 1 页')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '下一页' })).toBeDisabled();
    expect(list).toHaveBeenLastCalledWith({ limit: 20 });
  });

  it('keeps system events distinct and applies exact filters', async () => {
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValueOnce({ events: [systemEvent], nextCursor: null })
      .mockResolvedValueOnce({ events: [], nextCursor: null });

    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <AuditPage onSessionInvalidated={vi.fn()} />
      </ConfigProvider>,
    );

    expect(await screen.findByText('系统')).toBeInTheDocument();
    expect(screen.queryByText('审计查询')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '审计查询说明' })).not.toBeInTheDocument();
    expect(screen.queryByText('用户')).not.toBeInTheDocument();
    expect(screen.getByText('TASK_RUNTIME_RECOVERED')).toBeInTheDocument();
    expect(screen.getByText('任务已恢复')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '详情' }));
    const drawerTitle = await screen.findByText('审计详情');
    const drawer = drawerTitle.closest<HTMLElement>('.ant-drawer');
    expect(drawer).not.toBeNull();
    expect(drawer).toHaveTextContent('TASK_RUNTIME_RECOVERED');
    expect(drawer).toHaveTextContent('33333333-3333-4333-8333-333333333333');
    expect(screen.queryByRole('button', { name: 'Expand row' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));

    fireEvent.mouseDown(screen.getByRole('combobox', { name: '操作主体' }));
    fireEvent.click(await screen.findByText('用户'));
    fireEvent.change(screen.getByLabelText('动作'), {
      target: { value: ' TASK_RUNTIME_START_ACCEPTED ' },
    });
    fireEvent.change(screen.getByLabelText('对象类型'), { target: { value: ' TASK ' } });
    fireEvent.click(screen.getByRole('button', { name: /查询$/u }));

    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    expect(list.mock.calls[1]?.[0]).toEqual({
      action: 'TASK_RUNTIME_START_ACCEPTED',
      actorType: 'USER',
      limit: 10,
      subjectType: 'TASK',
    });
  });

  it('shows one cursor page at a time and returns to an already loaded page', async () => {
    const next = {
      ...systemEvent,
      eventId: '44444444-4444-4444-8444-444444444444',
      sequence: '11',
    };
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValueOnce({ events: [systemEvent], nextCursor: 'cursor-1' })
      .mockResolvedValueOnce({ events: [next], nextCursor: null });

    render(<AuditPage onSessionInvalidated={vi.fn()} />);

    await screen.findByText('12');
    expect(screen.getByRole('button', { name: '上一页' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    expect(list.mock.calls[1]?.[0]).toEqual({ cursor: 'cursor-1', limit: 10 });
    expect(await screen.findByText('11')).toBeInTheDocument();
    expect(screen.queryByText('12')).not.toBeInTheDocument();
    expect(screen.getByText('第 2 页')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '下一页' })).toBeDisabled();

    fireEvent.click(screen.getByRole('button', { name: '上一页' }));
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.queryByText('11')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    expect(screen.getByText('11')).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(2);
  });

  it('retains the current page after a next-page failure and retries its cursor', async () => {
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValueOnce({ events: [systemEvent], nextCursor: 'cursor-1' })
      .mockRejectedValueOnce(new PlatformApiError('AUDIT_UNAVAILABLE', 503))
      .mockResolvedValueOnce({ events: [], nextCursor: null });

    render(<AuditPage onSessionInvalidated={vi.fn()} />);

    await screen.findByText('12');
    fireEvent.click(await screen.findByRole('button', { name: '下一页' }));
    expect(await screen.findByText('读取审计记录失败：AUDIT_UNAVAILABLE')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.getByText('第 1 页')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
    expect(list.mock.calls[2]?.[0]).toEqual({ cursor: 'cursor-1', limit: 10 });
    expect(await screen.findByText('第 2 页')).toBeInTheDocument();
  });

  it('resets cursor history when applying filters and clearing them', async () => {
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValue({ events: [systemEvent], nextCursor: 'cursor-1' });
    render(<AuditPage onSessionInvalidated={vi.fn()} />);

    await screen.findByText('12');
    fireEvent.click(await screen.findByRole('button', { name: '下一页' }));
    expect(await screen.findByText('第 2 页')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('动作'), {
      target: { value: 'TASK_RUNTIME_RECOVERED' },
    });
    fireEvent.click(screen.getByRole('button', { name: /查询$/u }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
    expect(list.mock.calls[2]?.[0]).toEqual({ action: 'TASK_RUNTIME_RECOVERED', limit: 10 });
    expect(await screen.findByText('第 1 页')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '上一页' })).toBeDisabled();

    fireEvent.click(screen.getByRole('button', { name: '下一页' }));
    expect(await screen.findByText('第 2 页')).toBeInTheDocument();
    expect(list.mock.calls[3]?.[0]).toEqual({
      action: 'TASK_RUNTIME_RECOVERED',
      cursor: 'cursor-1',
      limit: 10,
    });
    fireEvent.click(screen.getByRole('button', { name: /重置$/u }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(5));
    expect(list.mock.calls[4]?.[0]).toEqual({ limit: 10 });
    expect(await screen.findByText('第 1 页')).toBeInTheDocument();
    expect(screen.getByLabelText('动作')).toHaveValue('');
    expect(screen.getByRole('button', { name: '上一页' })).toBeDisabled();
  });

  it('discards an older next-page response after a fresh filtered query', async () => {
    const stalePage = deferred<AuditListResult>();
    const freshPage = deferred<AuditListResult>();
    const staleEvent = {
      ...systemEvent,
      action: 'STALE_PAGE_EVENT',
      eventId: '44444444-4444-4444-8444-444444444444',
      sequence: '11',
    };
    const freshEvent = {
      ...systemEvent,
      action: 'FRESH_FILTERED_EVENT',
      eventId: '55555555-5555-4555-8555-555555555555',
      sequence: '20',
    };
    const list = vi
      .spyOn(auditApi, 'list')
      .mockResolvedValueOnce({ events: [systemEvent], nextCursor: 'cursor-1' })
      .mockImplementationOnce(() => stalePage.promise)
      .mockImplementationOnce(() => freshPage.promise);

    render(<AuditPage onSessionInvalidated={vi.fn()} />);

    await screen.findByText('12');
    fireEvent.click(await screen.findByRole('button', { name: '下一页' }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));

    fireEvent.change(screen.getByLabelText('动作'), {
      target: { value: 'FRESH_FILTERED_EVENT' },
    });
    fireEvent.click(screen.getByRole('button', { name: /查询$/u }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(3));

    expect(screen.getByRole('button', { name: '下一页' })).toBeDisabled();
    expect(screen.getByRole('button', { name: /查询$/u })).toBeDisabled();
    expect(screen.getByRole('button', { name: /重置$/u })).toBeDisabled();

    await act(async () => {
      freshPage.resolve({ events: [freshEvent], nextCursor: null });
      await freshPage.promise;
    });
    expect(
      await screen.findByText('FRESH_FILTERED_EVENT', { selector: 'code' }),
    ).toBeInTheDocument();

    await act(async () => {
      stalePage.resolve({ events: [staleEvent], nextCursor: 'stale-cursor' });
      await stalePage.promise;
    });

    expect(screen.queryByText('STALE_PAGE_EVENT')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '下一页' })).toBeDisabled();
    expect(screen.getByRole('button', { name: /查询$/u })).toBeEnabled();
  });
});
