import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import type { AlertDeliveryContext, RuntimeLogRead } from '@scs/contracts';
import { AlertContextDialog } from './AlertContextDialog';

afterEach(cleanup);
const context: AlertDeliveryContext = {
  delivery: {
    id: 'delivery-old',
    taskId: 'task',
    taskName: '测试同步',
    rule: 'UNEXPECTED_EXIT',
    event: 'RECOVERY',
    status: 'SENT',
    failureCode: null,
    faultCode: null,
    createdAt: '2026-10-27T00:39:00Z',
  },
  association: 'AUDIT',
  run: {
    id: 'old-run',
    status: 'FAILED',
    stage: 'CDC',
    failureCode: 'RUNTIME_UNEXPECTED_EXIT',
    startedAt: '2026-10-26T00:00:00Z',
    endedAt: '2026-10-26T21:11:35Z',
  },
  units: [
    {
      unitKey: 'source',
      status: 'FAILED',
      exitCode: 1,
      signal: null,
      failureCode: 'RUNTIME_UNEXPECTED_EXIT',
    },
  ],
  events: [],
  preflightChecks: [],
  logWindow: { from: '2026-10-26T21:09:35Z', to: '2026-10-26T21:13:35Z' },
};
const logs: RuntimeLogRead = { taskId: 'task', runId: 'old-run', facts: [], limited: false };
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it('reads only on open and uses the failed historical run for a recovery notification', async () => {
  const loadContext = vi.fn(async () => context);
  const loadLogs = vi.fn(async () => logs);
  const props = { onClose: vi.fn(), onSessionInvalidated: vi.fn(), loadContext, loadLogs };
  const view = render(<AlertContextDialog {...props} deliveryId={null} />);
  expect(loadContext).not.toHaveBeenCalled();
  expect(loadLogs).not.toHaveBeenCalled();
  view.rerender(<AlertContextDialog {...props} deliveryId="delivery-old" />);
  const dialog = await screen.findByRole('dialog', { name: '告警上下文' });
  await waitFor(() => expect(loadLogs).toHaveBeenCalledOnce());
  expect(loadLogs).toHaveBeenCalledWith('task', 'old-run', {
    ...context.logWindow,
    order: 'DESC',
    stream: 'STDERR',
  });
  expect(within(dialog).getByText('测试同步')).toBeInTheDocument();
  expect(within(dialog).getByText(/这条记录表示此前告警已解除/u)).toBeInTheDocument();
  expect(await screen.findByText(/该时段没有可用日志/u)).toBeInTheDocument();
  fireEvent.mouseDown(screen.getByRole('combobox', { name: '上下文日志流' }));
  fireEvent.click(
    await screen.findByText('标准输出', { selector: '.ant-select-item-option-content' }),
  );
  await waitFor(() =>
    expect(loadLogs).toHaveBeenLastCalledWith('task', 'old-run', {
      ...context.logWindow,
      order: 'DESC',
      stream: 'STDOUT',
    }),
  );
  view.rerender(<AlertContextDialog {...props} deliveryId={null} />);
  const count = loadLogs.mock.calls.length;
  await act(async () => {});
  expect(loadLogs).toHaveBeenCalledTimes(count);
});

it('ignores an old context response after switching notification or closing', async () => {
  const pending = deferred<AlertDeliveryContext>();
  const loadContext = vi
    .fn()
    .mockReturnValueOnce(pending.promise)
    .mockResolvedValueOnce({
      ...context,
      delivery: { ...context.delivery, id: 'new', taskName: '新任务' },
      run: null,
      association: 'NONE',
    });
  const loadLogs = vi.fn(async () => logs);
  const props = { onClose: vi.fn(), onSessionInvalidated: vi.fn(), loadContext, loadLogs };
  const view = render(<AlertContextDialog {...props} deliveryId="delivery-old" />);
  view.rerender(<AlertContextDialog {...props} deliveryId="new" />);
  expect(await screen.findByText('新任务')).toBeInTheDocument();
  await act(async () => pending.resolve(context));
  expect(screen.queryByText('测试同步')).not.toBeInTheDocument();
  expect(loadLogs).not.toHaveBeenCalled();
  view.rerender(<AlertContextDialog {...props} deliveryId={null} />);
  expect(loadLogs).not.toHaveBeenCalled();
});

it('keeps context visible when logs cannot be read and allows a manual retry', async () => {
  const loadContext = vi.fn(async () => context);
  const loadLogs = vi
    .fn()
    .mockRejectedValueOnce(new Error('unavailable'))
    .mockResolvedValueOnce({
      ...logs,
      facts: [
        {
          kind: 'LINE',
          payload: 'connection reset by peer',
          stream: 'STDERR',
          unitId: 'unit',
          unitKey: 'source',
          continuation: 0,
          continues: false,
          occurredAt: '2026-10-26T21:11:14Z',
        },
      ],
    });
  render(
    <AlertContextDialog
      deliveryId="delivery-old"
      onClose={vi.fn()}
      onSessionInvalidated={vi.fn()}
      loadContext={loadContext}
      loadLogs={loadLogs}
    />,
  );
  expect(await screen.findByText(/该运行日志暂不可读/u)).toBeInTheDocument();
  expect(screen.getByText('测试同步')).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: '刷新日志' }));
  expect(await screen.findByText(/connection reset by peer/u)).toBeInTheDocument();
  expect(loadContext).toHaveBeenCalledOnce();
});

it('does not show a late log response from another notification', async () => {
  const pending = deferred<RuntimeLogRead>();
  const loadContext = vi
    .fn()
    .mockResolvedValueOnce(context)
    .mockResolvedValueOnce({
      ...context,
      delivery: { ...context.delivery, id: 'new', taskName: '新任务' },
      run: null,
      association: 'NONE',
    });
  const loadLogs = vi.fn(() => pending.promise);
  const props = { onClose: vi.fn(), onSessionInvalidated: vi.fn(), loadContext, loadLogs };
  const view = render(<AlertContextDialog {...props} deliveryId="delivery-old" />);
  await waitFor(() => expect(loadLogs).toHaveBeenCalledOnce());
  view.rerender(<AlertContextDialog {...props} deliveryId="new" />);
  await screen.findByText('新任务');
  await act(async () =>
    pending.resolve({
      ...logs,
      facts: [
        {
          kind: 'LINE',
          payload: 'old private log',
          stream: 'STDERR',
          unitId: 'unit',
          unitKey: 'source',
          continuation: 0,
          continues: false,
          occurredAt: '2026-10-26T21:11:14Z',
        },
      ],
    }),
  );
  expect(screen.queryByText(/old private log/u)).not.toBeInTheDocument();
  expect(loadLogs).toHaveBeenCalledOnce();
});
