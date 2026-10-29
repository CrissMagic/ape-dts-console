import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import type { AlertDelivery, AlertSettings, UpdateAlertSettingsRequest } from '@scs/contracts';
import { AlertSettingsCard } from './AlertSettingsCard';

const settings: AlertSettings = {
  channel: 'WECOM',
  enabled: false,
  configured: true,
  notifyRecovery: true,
  durationSeconds: 30,
  repeatSeconds: 300,
  slotInactive: true,
  slotMissing: true,
  retainedWalBytes: null,
  unconfirmedWalBytes: null,
  kafkaOffsetLag: null,
  delayMilliseconds: null,
  rowVersion: '1',
};
const delivery: AlertDelivery = {
  id: '0199c617-249d-7459-bbcb-f97ad47a3111',
  taskId: null,
  rule: 'TEST',
  event: 'TEST',
  status: 'SENT',
  failureCode: null,
  createdAt: '2026-10-15T01:00:00Z',
};
function setup() {
  const api = {
    get: vi.fn(async () => settings),
    update: vi
      .fn<(input: UpdateAlertSettingsRequest) => Promise<AlertSettings>>()
      .mockResolvedValue({ ...settings, rowVersion: '2' }),
    test: vi.fn(async () => delivery),
    deliveries: vi.fn(async () => ({ items: [] as AlertDelivery[] })),
  };
  render(<AlertSettingsCard api={api} onSessionInvalidated={vi.fn()} />);
  return api;
}

it('defaults notification history to ten rows and allows twenty or fifty rows per page', async () => {
  const items = Array.from({ length: 25 }, (_, index) => ({
    ...delivery,
    id: `delivery-${index}`,
    taskId: `task-${index + 1}`,
    taskName: `task-${index + 1}`,
  }));
  const api = {
    get: vi.fn(async () => settings),
    update: vi.fn(),
    test: vi.fn(),
    deliveries: vi.fn(async () => ({ items })),
  };
  render(<AlertSettingsCard api={api} onSessionInvalidated={vi.fn()} />);
  await screen.findByRole('button', { name: '发送测试通知' });
  fireEvent.click(screen.getByRole('tab', { name: '通知记录' }));
  const table = within(screen.getByRole('table'));
  expect(table.getByText('task-10')).toBeInTheDocument();
  expect(table.queryByText('task-11')).not.toBeInTheDocument();
  fireEvent.mouseDown(screen.getByRole('combobox'));
  fireEvent.click(
    await screen.findByText(/^20\s*\//u, { selector: '.ant-select-item-option-content' }),
  );
  expect(table.getByText('task-20')).toBeInTheDocument();
  expect(table.queryByText('task-21')).not.toBeInTheDocument();
  fireEvent.mouseDown(screen.getByRole('combobox'));
  fireEvent.click(
    await screen.findByText(/^50\s*\//u, { selector: '.ant-select-item-option-content' }),
  );
  expect(table.getByText('task-25')).toBeInTheDocument();
  expect(api.deliveries).toHaveBeenCalledTimes(1);
});

it('protects unsaved alert settings through the registered navigation guard', async () => {
  let guard: ((proceed: () => void) => void) | null = null;
  const api = {
    get: vi.fn(async () => settings),
    update: vi.fn(),
    test: vi.fn(),
    deliveries: vi.fn(async () => ({ items: [] })),
  };
  render(
    <AlertSettingsCard
      api={api}
      onSessionInvalidated={vi.fn()}
      onLeaveGuardChange={(next) => {
        guard = next;
      }}
    />,
  );
  await screen.findByRole('button', { name: '发送测试通知' });
  fireEvent.change(screen.getByLabelText('企业微信机器人 Webhook'), {
    target: { value: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=example' },
  });
  const proceed = vi.fn();
  fireEvent.click(screen.getByRole('tab', { name: '通知记录' }));
  // Switching an inner tab keeps the draft; only leaving the section needs confirmation.
  act(() => guard?.(proceed));
  expect(await screen.findByRole('dialog', { name: '放弃未保存的告警配置？' })).toBeInTheDocument();
  expect(proceed).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
  fireEvent.click(screen.getByRole('tab', { name: '配置与规则' }));
  expect(screen.getByLabelText('企业微信机器人 Webhook')).toHaveValue(
    'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=example',
  );
  expect(api.update).not.toHaveBeenCalled();
});

it('shows a task name and concise category, opening context only on demand', async () => {
  const api = {
    get: vi.fn(async () => settings),
    update: vi.fn(),
    test: vi.fn(),
    deliveries: vi.fn(async () => ({
      items: [
        {
          ...delivery,
          taskId: 'private-task-id',
          taskName: '订单同步',
          rule: 'UNEXPECTED_EXIT',
          event: 'ALARM' as const,
        },
      ],
    })),
    context: vi.fn().mockResolvedValue({
      delivery: { ...delivery, taskName: '订单同步' },
      association: 'NONE',
      run: null,
      units: [],
      events: [],
      preflightChecks: [],
      logWindow: null,
    }),
  };
  render(<AlertSettingsCard api={api} onSessionInvalidated={vi.fn()} />);
  await screen.findByRole('button', { name: '发送测试通知' });
  fireEvent.click(screen.getByRole('tab', { name: '通知记录' }));
  const table = within(screen.getByRole('table'));
  expect(table.getByText('订单同步')).toBeInTheDocument();
  expect(table.queryByText('private-task-id')).not.toBeInTheDocument();
  expect(table.getByText('进程异常退出')).toBeInTheDocument();
  expect(api.context).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: '订单同步 查看告警上下文' }));
  await waitFor(() => expect(api.context).toHaveBeenCalledWith(delivery.id));
  // rc-dialog uses the fixed "test-id" in test mode, shared with the hidden confirmation modal.
  const dialog = await screen.findByRole('dialog');
  expect(within(dialog).getByText('告警上下文')).toBeInTheDocument();
  expect(within(dialog).getByText('订单同步')).toBeInTheDocument();
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

it('keeps the configured webhook private and sends only after explicit test confirmation', async () => {
  const api = setup();
  const button = await screen.findByRole('button', { name: '发送测试通知' });
  expect(screen.queryByText('告警配置', { exact: true })).not.toBeInTheDocument();
  expect(screen.getByLabelText('企业微信机器人 Webhook')).toHaveValue('');
  expect(api.test).not.toHaveBeenCalled();
  fireEvent.click(button);
  const dialog = await screen.findByRole('dialog', { name: '发送测试通知' });
  expect(api.test).not.toHaveBeenCalled();
  fireEvent.click(within(dialog).getByRole('button', { name: /发\s*送/u }));
  await waitFor(() => expect(api.test).toHaveBeenCalledOnce());
  expect(await screen.findByText('测试通知已发送，请在企业微信群中确认。')).toBeInTheDocument();
  fireEvent.click(screen.getByRole('tab', { name: '通知记录' }));
  expect(await screen.findByText('2026年10月15日 09:00:00')).toBeInTheDocument();
});

it('saves rules with optimistic versioning without replacing a blank webhook or sending a notification', async () => {
  const api = setup();
  await screen.findByRole('button', { name: '发送测试通知' });
  fireEvent.change(screen.getByLabelText('保留 WAL 阈值（字节）'), {
    target: { value: '1073741824' },
  });
  expect(screen.getByRole('button', { name: '发送测试通知' })).toBeDisabled();
  fireEvent.click(screen.getByRole('button', { name: '保存配置' }));
  await waitFor(() => expect(api.update).toHaveBeenCalledOnce());
  expect(api.update).toHaveBeenCalledWith(
    expect.objectContaining({
      expectedRowVersion: '1',
      retainedWalBytes: '1073741824',
      enabled: false,
      channel: 'WECOM',
    }),
  );
  expect(api.update.mock.calls[0]?.[0]).not.toHaveProperty('webhookUrl');
  expect(api.test).not.toHaveBeenCalled();
  expect(await screen.findByText('告警配置已保存。')).toBeInTheDocument();
});

it('records a rejected test delivery as a failure instead of success', async () => {
  const api = setup();
  api.test.mockResolvedValue({ ...delivery, status: 'FAILED', failureCode: 'CHANNEL_REJECTED' });
  fireEvent.click(await screen.findByRole('button', { name: '发送测试通知' }));
  const dialog = await screen.findByRole('dialog', { name: '发送测试通知' });
  fireEvent.click(within(dialog).getByRole('button', { name: /发\s*送/u }));
  expect(await screen.findByText('测试通知发送失败（CHANNEL_REJECTED）。')).toBeInTheDocument();
  expect(screen.queryByText('测试通知已发送，请在企业微信群中确认。')).not.toBeInTheDocument();
});
