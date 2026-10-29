import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { AuthenticatedSessionResponse, UserSummary } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { PlatformApiError, usersApi } from '../../api/platform-api';
import { AdminUsersPage } from './AdminUsersPage';

const administrator: AuthenticatedSessionResponse = {
  mustChangePassword: false,
  absoluteExpiresAt: '2026-09-02T12:00:00.000Z',
  csrfToken: 'scs-csrf-v1_test-token',
  idleExpiresAt: '2026-09-02T10:30:00.000Z',
  status: 'AUTHENTICATED',
  user: {
    displayName: 'Platform administrator',
    id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c1001',
    role: 'ADMIN',
    status: 'ACTIVE',
    username: 'platform-admin',
  },
};

const developer: UserSummary = {
  displayName: 'Unit five developer',
  id: '01990f5d-b5f4-7a51-8e2d-e7f1b32c1002',
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: 'unit-five-developer',
};

const activationGrant = {
  activationCode: `scs-at-v1_${'A'.repeat(43)}`,
  expiresAt: '2026-09-03T09:00:00.000Z',
  user: developer,
};

function renderPage(users: readonly UserSummary[] = [developer], onSessionInvalidated = vi.fn()) {
  vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [...users] });
  return {
    onSessionInvalidated,
    ...render(
      <AdminUsersPage onSessionInvalidated={onSessionInvalidated} session={administrator} />,
    ),
  };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('AdminUsersPage', () => {
  it('returns to login when a user-management action encounters an expired session', async () => {
    vi.spyOn(usersApi, 'revokeSessions').mockRejectedValue(
      new PlatformApiError('SESSION_INVALID', 401),
    );
    const { onSessionInvalidated } = renderPage();
    fireEvent.click(await screen.findByRole('button', { name: `管理 ${developer.username}` }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '使登录会话失效' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认使登录会话失效' }));
    await waitFor(() => expect(onSessionInvalidated).toHaveBeenCalledOnce());
    expect(screen.queryByLabelText('当前密码')).not.toBeInTheDocument();
  });
  it('does not offer demotion for the fixed admin account', async () => {
    vi.spyOn(usersApi, 'list').mockResolvedValue({
      users: [{ ...administrator.user, username: 'admin' }, developer],
    });
    const changeRole = vi.spyOn(usersApi, 'changeRole');
    render(<AdminUsersPage session={administrator} onSessionInvalidated={() => {}} />);
    fireEvent.click(await screen.findByRole('button', { name: '管理 admin' }));
    const action = await screen.findByRole('menuitem', { name: '固定管理员不可降级' });
    expect(action).toHaveAttribute('aria-disabled', 'true');
    fireEvent.click(action);
    expect(changeRole).not.toHaveBeenCalled();
    expect(screen.queryByRole('dialog', { name: '确认高风险操作' })).not.toBeInTheDocument();
  });
  it('uses a table for the user list and opens the invitation form in a drawer', async () => {
    const create = vi.spyOn(usersApi, 'create').mockResolvedValue(activationGrant);
    renderPage();

    expect(await screen.findByRole('table', { name: '平台用户' })).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '用户管理' })).not.toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: '邀请用户' })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '邀请用户' }));
    const drawer = await screen.findByRole('dialog', { name: '邀请用户' });
    fireEvent.change(screen.getByRole('textbox', { name: '用户名' }), {
      target: { value: 'new-developer' },
    });
    fireEvent.change(screen.getByRole('textbox', { name: '显示名称' }), {
      target: { value: 'New developer' },
    });
    fireEvent.click(screen.getByRole('button', { name: '创建一次性邀请' }));

    expect(create).not.toHaveBeenCalled();
    expect(
      await screen.findByText('将创建 New developer（new-developer）并生成一次性激活码。'),
    ).toBeInTheDocument();
    expect(drawer).toBeInTheDocument();
  });

  it('requires explicit confirmation before elevating a user and displays the activation grant', async () => {
    const changeRole = vi.spyOn(usersApi, 'changeRole').mockResolvedValue(activationGrant);
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: `管理 ${developer.username}` }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '升为管理员' }));

    expect(screen.getByRole('dialog', { name: '确认高风险操作' })).toBeInTheDocument();
    expect(changeRole).not.toHaveBeenCalled();
    const confirm = screen.getByRole('button', { name: '确认提升为管理员' });
    expect(confirm).toBeDisabled();

    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(confirm);

    await waitFor(() => expect(changeRole).toHaveBeenCalledWith(developer, 'ADMIN'));
    expect(await screen.findByText(activationGrant.activationCode)).toBeInTheDocument();
  });

  it('confirms credential resets and session revocation independently', async () => {
    const resetCredentials = vi
      .spyOn(usersApi, 'resetCredentials')
      .mockResolvedValue(activationGrant);
    const revokeSessions = vi.spyOn(usersApi, 'revokeSessions').mockResolvedValue(undefined);
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: `管理 ${developer.username}` }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '重置凭据' }));
    expect(resetCredentials).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认重置凭据' }));
    await waitFor(() => expect(resetCredentials).toHaveBeenCalledWith(developer));

    fireEvent.click(await screen.findByRole('button', { name: '我已安全保存，关闭' }));
    fireEvent.click(screen.getByRole('button', { name: `管理 ${developer.username}` }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '使登录会话失效' }));
    expect(revokeSessions).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认使登录会话失效' }));
    await waitFor(() => expect(revokeSessions).toHaveBeenCalledWith(developer));
  });

  it('returns to the authentication boundary after revoking the current administrator session', async () => {
    const revokeSessions = vi.spyOn(usersApi, 'revokeSessions').mockResolvedValue(undefined);
    const { onSessionInvalidated } = renderPage([administrator.user]);

    fireEvent.click(
      await screen.findByRole('button', { name: `管理 ${administrator.user.username}` }),
    );
    fireEvent.click(await screen.findByRole('menuitem', { name: '使登录会话失效' }));
    fireEvent.click(screen.getByRole('checkbox', { name: '我已了解此操作的影响，并确认继续。' }));
    fireEvent.click(screen.getByRole('button', { name: '确认使登录会话失效' }));

    await waitFor(() => expect(revokeSessions).toHaveBeenCalledWith(administrator.user));
    expect(onSessionInvalidated).toHaveBeenCalledOnce();
  });
});
