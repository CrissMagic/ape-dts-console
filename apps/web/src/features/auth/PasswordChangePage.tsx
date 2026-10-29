import { useState, type FormEvent } from 'react';
import type { AuthenticatedSessionResponse } from '@scs/contracts';
import { Button, Input, Space } from 'antd';
import { authApi, PlatformApiError } from '../../api/platform-api';

export function PasswordChangePage({
  required = false,
  onChanged,
  onCancel,
  onLoggedOut,
}: {
  required?: boolean;
  onChanged: (session: AuthenticatedSessionResponse) => void;
  onCancel?: () => void;
  onLoggedOut: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const data = new FormData(event.currentTarget);
    const newPassword = String(data.get('newPassword'));
    if (newPassword !== data.get('confirmation')) {
      setError('两次输入的新密码不一致。');
      return;
    }
    setBusy(true);
    setError(null);
    try {
      onChanged(
        await authApi.changePassword({
          currentPassword: String(data.get('currentPassword')),
          newPassword,
        }),
      );
    } catch (caught) {
      const messages: Record<string, string> = {
        CURRENT_PASSWORD_INVALID: '当前密码错误。',
        PASSWORD_UNCHANGED: '新密码不能与当前密码相同。',
        PASSWORD_CHANGE_REQUIRED: '请先修改临时密码。',
        AUTHENTICATION_RATE_LIMITED: '尝试过多，请稍后重试。',
      };
      setError(
        caught instanceof PlatformApiError
          ? (messages[caught.code] ?? `修改失败：${caught.code}`)
          : '修改失败，请稍后重试。',
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <main className="platform-shell">
      <section className="bootstrap-card auth-card">
        <header className="platform-header">
          <div>
            <h1>{required ? '设置新密码' : '修改密码'}</h1>
            <p className="summary">
              {required ? '临时密码必须修改后才能使用平台。' : '修改后，其他已登录会话将失效。'}
            </p>
          </div>
        </header>
        {error && (
          <div className="error-banner" role="alert">
            {error}
          </div>
        )}
        <form className="form-grid compact-form" onSubmit={(event) => void submit(event)}>
          <label>
            <span>当前密码</span>
            <Input.Password name="currentPassword" autoComplete="current-password" required />
          </label>
          <label>
            <span>新密码（至少 12 位）</span>
            <Input.Password
              name="newPassword"
              autoComplete="new-password"
              minLength={12}
              maxLength={128}
              required
            />
          </label>
          <label>
            <span>确认新密码</span>
            <Input.Password
              name="confirmation"
              autoComplete="new-password"
              minLength={12}
              maxLength={128}
              required
            />
          </label>
          <Button type="primary" htmlType="submit" loading={busy} disabled={busy}>
            保存密码
          </Button>
          <Space>
            {!required && (
              <Button onClick={onCancel} disabled={busy}>
                取消
              </Button>
            )}
            <Button
              disabled={busy}
              onClick={() => {
                setBusy(true);
                void authApi
                  .logout()
                  .then(onLoggedOut)
                  .catch(() => {
                    setError('退出失败，请重试。');
                    setBusy(false);
                  });
              }}
            >
              退出登录
            </Button>
          </Space>
        </form>
      </section>
    </main>
  );
}
