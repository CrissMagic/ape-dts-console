import { useState, type FormEvent } from 'react';
import { Button, Input } from 'antd';
import { PlatformApiError, usersApi } from '../../api/platform-api';

export function ActivationPage() {
  const parameters = new URLSearchParams(window.location.search);
  const [active, setActive] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function activate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const data = new FormData(event.currentTarget);
    if (data.get('password') !== data.get('confirmation')) {
      setError('两次输入的新密码不一致。');
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const result = await usersApi.activate({
        activationCode: String(data.get('activationCode')),
        password: String(data.get('password')),
        username: String(data.get('username')),
      });
      if (result.status !== 'ACTIVE') throw new Error('激活服务版本不兼容。');
      setActive(true);
    } catch (caught) {
      setError(
        caught instanceof PlatformApiError ? `激活失败：${caught.code}` : '激活失败，请稍后重试。',
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
            <h1>激活平台账号</h1>
            <p className="summary">使用一次性激活码设置登录密码。</p>
          </div>
        </header>
        {error && (
          <div className="error-banner" role="alert">
            {error}
          </div>
        )}
        {active ? (
          <section className="bootstrap-panel ready-panel">
            <h2>账号已激活</h2>
            <a href="/">返回登录</a>
          </section>
        ) : (
          <form className="form-grid compact-form" onSubmit={(event) => void activate(event)}>
            <label>
              <span>用户名</span>
              <Input
                defaultValue={parameters.get('username') ?? ''}
                autoComplete="username"
                name="username"
                required
              />
            </label>
            <label>
              <span>一次性激活码</span>
              <Input
                defaultValue={parameters.get('code') ?? ''}
                autoComplete="off"
                name="activationCode"
                required
              />
            </label>
            <label>
              <span>新密码（至少 12 位）</span>
              <Input.Password autoComplete="new-password" minLength={12} name="password" required />
            </label>
            <label>
              <span>确认新密码</span>
              <Input.Password
                autoComplete="new-password"
                minLength={12}
                name="confirmation"
                required
              />
            </label>
            <Button type="primary" htmlType="submit" loading={busy} disabled={busy}>
              激活账号
            </Button>
          </form>
        )}
      </section>
    </main>
  );
}
