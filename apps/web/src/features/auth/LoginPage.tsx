import { useState, type FormEvent } from 'react';
import type { AuthenticatedSessionResponse } from '@scs/contracts';
import { Button, Input, Tooltip } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { authApi, PlatformApiError } from '../../api/platform-api';

export function LoginPage({
  onAuthenticated,
}: {
  onAuthenticated: (session: AuthenticatedSessionResponse) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function login(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const data = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      const result = await authApi.login({
        username: String(data.get('username')),
        password: String(data.get('password')),
      });
      if (result.status !== 'AUTHENTICATED') throw new Error('登录服务版本不兼容。');
      onAuthenticated(result);
    } catch (caught) {
      setError(
        caught instanceof PlatformApiError
          ? caught.code === 'AUTHENTICATION_RATE_LIMITED'
            ? '登录尝试过多，请稍后再试。'
            : caught.code === 'AUTHENTICATION_FAILED'
              ? '用户名或密码错误。'
              : `登录失败：${caught.code}`
          : '登录失败，请稍后重试。',
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
            <p className="eyebrow">ape-dts · 数据同步管理平台</p>
            <h1>登录管理平台</h1>
            <p className="summary">使用平台账号登录。</p>
          </div>
          <Tooltip title="初始管理员账号为 admin，临时密码由首次初始化日志提供。管理员忘记密码时，请联系部署人员通过容器命令重置。">
            <QuestionCircleOutlined aria-label="登录说明" />
          </Tooltip>
        </header>
        {error && (
          <div className="error-banner" role="alert">
            {error}
          </div>
        )}
        <section className="bootstrap-panel">
          <form className="form-grid compact-form" onSubmit={(event) => void login(event)}>
            <label>
              <span>用户名</span>
              <Input autoComplete="username" name="username" required size="large" />
            </label>
            <label>
              <span>密码</span>
              <Input.Password
                autoComplete="current-password"
                name="password"
                required
                size="large"
              />
            </label>
            <Button
              type="primary"
              block
              loading={busy}
              disabled={busy}
              htmlType="submit"
              size="large"
            >
              登录
            </Button>
          </form>
          <a className="text-link" href="/activate">
            使用邀请或重置码激活账号
          </a>
        </section>
      </section>
    </main>
  );
}
