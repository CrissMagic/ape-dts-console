import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { authApi, PlatformApiError } from '../api/platform-api';
import { bootstrapApi } from '../api/bootstrap-api';
import { App } from './App';

vi.mock('../features/auth/AuthenticatedConsole', () => ({
  AuthenticatedConsole: ({ onChangePassword }: { onChangePassword: () => void }) => (
    <div>
      业务控制台<button onClick={onChangePassword}>修改密码</button>
    </div>
  ),
}));
const session = {
  absoluteExpiresAt: '2026-09-28T12:00:00.000Z',
  csrfToken: 'scs-csrf-v1_test',
  idleExpiresAt: '2026-09-28T10:00:00.000Z',
  status: 'AUTHENTICATED' as const,
  mustChangePassword: true,
  user: {
    displayName: 'Admin',
    id: '0199c617-249d-7459-bbcb-f97ad47a3120',
    role: 'ADMIN' as const,
    status: 'ACTIVE' as const,
    username: 'admin',
  },
};
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});
function ready() {
  vi.spyOn(bootstrapApi, 'getStatus').mockResolvedValue({
    phase: 'READY',
    platformInstanceId: session.user.id,
  });
}
function passwords(confirmation = 'new-password-123') {
  fireEvent.change(screen.getByLabelText('当前密码'), { target: { value: 'old-password-123' } });
  fireEvent.change(screen.getByLabelText('新密码（至少 12 位）'), {
    target: { value: 'new-password-123' },
  });
  fireEvent.change(screen.getByLabelText('确认新密码'), { target: { value: confirmation } });
}
describe('App password-only entry', () => {
  it('requires login if the server confirms session expiry after reconnection', async () => {
    vi.useFakeTimers();
    ready();
    vi.spyOn(authApi, 'session')
      .mockRejectedValueOnce(new PlatformApiError('NETWORK_ERROR', 0))
      .mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    render(<App />);
    await act(async () => {});
    expect(screen.getByText('服务连接已中断')).toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(screen.getByRole('heading', { name: '登录管理平台' })).toBeInTheDocument();
    expect(screen.queryByText('业务控制台')).not.toBeInTheDocument();
  });
  it('retries session verification after a tunnel disconnect without treating it as logout or bootstrap failure', async () => {
    vi.useFakeTimers();
    ready();
    const readSession = vi
      .spyOn(authApi, 'session')
      .mockRejectedValueOnce(new PlatformApiError('NETWORK_ERROR', 0))
      .mockResolvedValue({ ...session, mustChangePassword: false });
    render(<App />);
    await act(async () => {});
    expect(screen.getByText('服务连接已中断')).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '登录管理平台' })).not.toBeInTheDocument();
    expect(screen.queryByText('等待平台启动')).not.toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    expect(screen.getByText('业务控制台')).toBeInTheDocument();
    expect(readSession).toHaveBeenCalledTimes(2);
  });
  it('does not expose bootstrap secrets or interactive unlock', async () => {
    vi.spyOn(bootstrapApi, 'getStatus').mockResolvedValue({
      phase: 'SEALED',
      platformInstanceId: session.user.id,
    });
    render(<App />);
    expect(await screen.findByText('等待平台启动')).toBeInTheDocument();
    expect(screen.queryByLabelText('初始化码')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('平台解锁口令')).not.toBeInTheDocument();
    expect(screen.queryByText(/未配置数据库时/)).not.toBeInTheDocument();
  });
  it('polls startup until ready and enters login without a second factor', async () => {
    vi.spyOn(bootstrapApi, 'getStatus')
      .mockResolvedValueOnce({ phase: 'SEALED', platformInstanceId: session.user.id })
      .mockResolvedValue({ phase: 'READY', platformInstanceId: session.user.id });
    vi.spyOn(authApi, 'session').mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    vi.spyOn(authApi, 'login').mockResolvedValue({ ...session, mustChangePassword: false });
    render(<App />);
    await screen.findByText('等待平台启动');
    fireEvent.click(screen.getByRole('button', { name: '刷新状态' }));
    await screen.findByRole('heading', { name: '登录管理平台' });
    fireEvent.change(screen.getByLabelText('用户名'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('密码'), { target: { value: 'old-password-123' } });
    fireEvent.click(screen.getByRole('button', { name: /登\s*录/u }));
    expect(await screen.findByText('业务控制台')).toBeInTheDocument();
    expect(authApi.login).toHaveBeenCalledWith({ username: 'admin', password: 'old-password-123' });
  });
  it('blocks business until required password change succeeds and refreshes session', async () => {
    ready();
    vi.spyOn(authApi, 'session').mockResolvedValue(session);
    const change = vi
      .spyOn(authApi, 'changePassword')
      .mockResolvedValue({ ...session, mustChangePassword: false });
    render(<App />);
    await screen.findByRole('heading', { name: '设置新密码' });
    expect(screen.queryByText('业务控制台')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /取\s*消/u })).not.toBeInTheDocument();
    passwords('different-password');
    fireEvent.click(screen.getByRole('button', { name: '保存密码' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('两次输入的新密码不一致');
    expect(change).not.toHaveBeenCalled();
    passwords();
    fireEvent.click(screen.getByRole('button', { name: '保存密码' }));
    expect(await screen.findByText('业务控制台')).toBeInTheDocument();
    expect(change).toHaveBeenCalledWith({
      currentPassword: 'old-password-123',
      newPassword: 'new-password-123',
    });
    fireEvent.click(screen.getByRole('button', { name: '修改密码' }));
    expect(await screen.findByRole('heading', { name: '修改密码' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /取\s*消/u }));
    expect(screen.getByText('业务控制台')).toBeInTheDocument();
  });
  it('allows logout from the required password screen', async () => {
    ready();
    vi.spyOn(authApi, 'session').mockResolvedValue(session);
    const logout = vi.spyOn(authApi, 'logout').mockResolvedValue();
    render(<App />);
    await screen.findByRole('heading', { name: '设置新密码' });
    fireEvent.click(screen.getByRole('button', { name: '退出登录' }));
    await waitFor(() => expect(logout).toHaveBeenCalled());
    expect(await screen.findByRole('heading', { name: '登录管理平台' })).toBeInTheDocument();
  });
  it.each([
    ['CURRENT_PASSWORD_INVALID', '当前密码错误。'],
    ['PASSWORD_UNCHANGED', '新密码不能与当前密码相同。'],
    ['PASSWORD_CHANGE_REQUIRED', '请先修改临时密码。'],
  ])('keeps the change screen when the server rejects %s', async (code, message) => {
    ready();
    vi.spyOn(authApi, 'session').mockResolvedValue(session);
    vi.spyOn(authApi, 'changePassword').mockRejectedValue(new PlatformApiError(code, 403));
    render(<App />);
    await screen.findByRole('heading', { name: '设置新密码' });
    passwords();
    fireEvent.click(screen.getByRole('button', { name: '保存密码' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(message);
    expect(screen.queryByText('业务控制台')).not.toBeInTheDocument();
  });
  it('routes recovering startup to login', async () => {
    vi.spyOn(bootstrapApi, 'getStatus').mockResolvedValue({
      phase: 'RECOVERING',
      platformInstanceId: session.user.id,
    });
    vi.spyOn(authApi, 'session').mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
    render(<App />);
    expect(await screen.findByRole('heading', { name: '登录管理平台' })).toBeInTheDocument();
  });
});
