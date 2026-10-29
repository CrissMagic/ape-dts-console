import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { usersApi } from '../../api/platform-api';
import { ActivationPage } from './ActivationPage';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
it('activates an administrator with a confirmed password without TOTP enrollment', async () => {
  const activate = vi.spyOn(usersApi, 'activate').mockResolvedValue({
    status: 'ACTIVE',
    user: {
      id: '0199c617-249d-7459-bbcb-f97ad47a3120',
      username: 'admin2',
      displayName: '管理员',
      role: 'ADMIN',
      status: 'ACTIVE',
    },
  });
  render(<ActivationPage />);
  fireEvent.change(screen.getByLabelText('用户名'), { target: { value: 'admin2' } });
  fireEvent.change(screen.getByLabelText('一次性激活码'), { target: { value: 'activation-code' } });
  fireEvent.change(screen.getByLabelText('新密码（至少 12 位）'), {
    target: { value: 'new-password-123' },
  });
  fireEvent.change(screen.getByLabelText('确认新密码'), {
    target: { value: 'different-password' },
  });
  fireEvent.click(screen.getByRole('button', { name: '激活账号' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('两次输入的新密码不一致');
  expect(activate).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText('确认新密码'), { target: { value: 'new-password-123' } });
  fireEvent.click(screen.getByRole('button', { name: '激活账号' }));
  expect(await screen.findByRole('heading', { name: '账号已激活' })).toBeInTheDocument();
  expect(activate).toHaveBeenCalledWith({
    username: 'admin2',
    activationCode: 'activation-code',
    password: 'new-password-123',
  });
  expect(screen.queryByText(/TOTP/u)).not.toBeInTheDocument();
});
