import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { AuthenticatedSessionResponse, SessionSettings } from '@scs/contracts';
import { afterEach, expect, it, vi } from 'vitest';
import { PlatformApiError, sessionSettingsApi } from '../../api/platform-api';
import type { LeaveGuard } from '../../components/useDraftGuard';
import { SessionSettingsCard } from './SessionSettingsCard';
import { SystemSettingsPage } from './SystemSettingsPage';

const settings: SessionSettings = {
  idleTimeoutMinutes: 480,
  absoluteTimeoutHours: 168,
  rowVersion: '1',
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function setup() {
  const get = vi.spyOn(sessionSettingsApi, 'get').mockResolvedValue(settings);
  const update = vi.spyOn(sessionSettingsApi, 'update').mockImplementation(async (input) => ({
    idleTimeoutMinutes: input.idleTimeoutMinutes,
    absoluteTimeoutHours: input.absoluteTimeoutHours,
    rowVersion: '2',
  }));
  const onSessionInvalidated = vi.fn();
  render(
    <SessionSettingsCard
      onSessionInvalidated={onSessionInvalidated}
      onLeaveGuardChange={vi.fn()}
    />,
  );
  return { get, update, onSessionInvalidated };
}

it('loads the saved policy and saves the edited durations with its version', async () => {
  const { update } = setup();
  const idle = await screen.findByRole('spinbutton', { name: '空闲超时（分钟）' });
  expect(idle).toHaveValue('480');
  expect(screen.getByRole('spinbutton', { name: '最长登录时长（小时）' })).toHaveValue('168');
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
  fireEvent.change(idle, { target: { value: '60' } });
  fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  await waitFor(() =>
    expect(update).toHaveBeenCalledExactlyOnceWith({
      idleTimeoutMinutes: 60,
      absoluteTimeoutHours: 168,
      expectedRowVersion: '1',
    }),
  );
  expect(await screen.findByText('登录与会话设置已保存，将于下一次登录生效。')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
});

it('rejects an idle timeout longer than the absolute lifetime or an empty duration', async () => {
  const { update } = setup();
  const absolute = await screen.findByRole('spinbutton', { name: '最长登录时长（小时）' });
  fireEvent.change(absolute, { target: { value: '1' } });
  expect(screen.getByRole('alert')).toHaveTextContent('空闲超时不能超过最长登录时长');
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
  fireEvent.change(absolute, { target: { value: '' } });
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
  expect(update).not.toHaveBeenCalled();
});

it('preserves an edit on conflict and requires confirmation before reloading it', async () => {
  const { get, update } = setup();
  update.mockRejectedValue(new PlatformApiError('SESSION_SETTINGS_VERSION_CONFLICT', 409));
  const idle = await screen.findByRole('spinbutton', { name: '空闲超时（分钟）' });
  fireEvent.change(idle, { target: { value: '60' } });
  fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  expect(
    await screen.findByText('配置已被其他管理员修改，请重新加载后再保存。'),
  ).toBeInTheDocument();
  expect(idle).toHaveValue('60');
  get.mockResolvedValue({ ...settings, idleTimeoutMinutes: 120, rowVersion: '2' });
  fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
  expect(await screen.findByRole('dialog', { name: '放弃未保存的会话配置？' })).toBeInTheDocument();
  expect(get).toHaveBeenCalledTimes(1);
  fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
  expect(idle).toHaveValue('60');
  fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
  fireEvent.click(screen.getByRole('button', { name: '放弃修改并继续' }));
  await waitFor(() => expect(idle).toHaveValue('120'));
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
});

it('allows leaving during the initial request and ignores its failure after unmount', async () => {
  let reject!: (reason: unknown) => void;
  vi.spyOn(sessionSettingsApi, 'get').mockReturnValue(
    new Promise((_resolve, fail) => {
      reject = fail;
    }),
  );
  let guard: LeaveGuard | null = null;
  const onSessionInvalidated = vi.fn();
  const view = render(
    <SessionSettingsCard
      onSessionInvalidated={onSessionInvalidated}
      onLeaveGuardChange={(next) => {
        guard = next;
      }}
    />,
  );
  const proceed = vi.fn();
  act(() => guard?.(proceed));
  expect(proceed).toHaveBeenCalledOnce();
  view.unmount();
  await act(async () => reject(new PlatformApiError('SESSION_INVALID', 401)));
  expect(onSessionInvalidated).not.toHaveBeenCalled();
});

it.each(['load', 'save'])('returns to login on an expired session during %s', async (phase) => {
  const { get, update, onSessionInvalidated } = setup();
  const idle = await screen.findByRole('spinbutton', { name: '空闲超时（分钟）' });
  const expired = new PlatformApiError('SESSION_INVALID', 401);
  if (phase === 'load') {
    get.mockRejectedValue(expired);
    fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
  } else {
    update.mockRejectedValue(expired);
    fireEvent.change(idle, { target: { value: '60' } });
    fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  }
  await waitFor(() => expect(onSessionInvalidated).toHaveBeenCalledOnce());
});

it('does not show assumed defaults when loading fails and can retry', async () => {
  vi.spyOn(sessionSettingsApi, 'get')
    .mockRejectedValueOnce(new Error('offline'))
    .mockResolvedValue(settings);
  render(<SessionSettingsCard onSessionInvalidated={vi.fn()} onLeaveGuardChange={vi.fn()} />);
  expect(await screen.findByText('登录与会话配置操作失败，请稍后重试。')).toBeInTheDocument();
  expect(screen.queryByRole('spinbutton')).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: /保存设置/u })).toBeDisabled();
  fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
  expect(await screen.findByRole('spinbutton', { name: '空闲超时（分钟）' })).toHaveValue('480');
});

it('opens the policy from system settings and protects edits when changing groups', async () => {
  vi.spyOn(sessionSettingsApi, 'get').mockResolvedValue(settings);
  const session: AuthenticatedSessionResponse = {
    status: 'AUTHENTICATED',
    mustChangePassword: false,
    csrfToken: 'scs-csrf-v1_test',
    absoluteExpiresAt: '2026-10-29T00:00:00Z',
    idleExpiresAt: '2026-10-22T08:00:00Z',
    user: {
      id: '0199c617-249d-7459-bbcb-f97ad47a3120',
      username: 'admin',
      displayName: 'Admin',
      role: 'ADMIN',
      status: 'ACTIVE',
    },
  };
  render(
    <SystemSettingsPage
      session={session}
      statusText="管理服务正常"
      onSessionInvalidated={vi.fn()}
      onLeaveGuardChange={vi.fn()}
    />,
  );
  fireEvent.click(screen.getByRole('button', { name: '登录与会话' }));
  const idle = await screen.findByRole('spinbutton', { name: '空闲超时（分钟）' });
  fireEvent.change(idle, { target: { value: '60' } });
  fireEvent.click(screen.getByRole('button', { name: '平台信息' }));
  expect(await screen.findByRole('dialog', { name: '放弃未保存的会话配置？' })).toBeInTheDocument();
  expect(idle).toHaveValue('60');
  fireEvent.click(screen.getByRole('button', { name: '放弃修改并继续' }));
  expect(await screen.findByRole('region', { name: '平台信息' })).toBeInTheDocument();
  expect(screen.queryByRole('spinbutton')).not.toBeInTheDocument();
});
