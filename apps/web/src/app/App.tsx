import { useCallback, useEffect, useState, type ReactNode } from 'react';
import type { AuthenticatedSessionResponse } from '@scs/contracts';
import { Alert, Button } from 'antd';

import { authApi, PlatformApiError } from '../api/platform-api';
import { ActivationPage } from '../features/auth/ActivationPage';
import { AuthenticatedConsole } from '../features/auth/AuthenticatedConsole';
import { LoginPage } from '../features/auth/LoginPage';
import { PasswordChangePage } from '../features/auth/PasswordChangePage';
import { BootstrapPage } from '../features/bootstrap/BootstrapPage';
import { ThemeToggle } from '../components/ThemeToggle';

function PublicPage({ children }: { children: ReactNode }) {
  return (
    <>
      <ThemeToggle floating />
      {children}
    </>
  );
}

function AuthenticatedEntry({
  onRecoveryReady,
  recoveryOnly,
}: {
  onRecoveryReady?: (() => void) | undefined;
  recoveryOnly: boolean;
}) {
  const [checking, setChecking] = useState(true);
  const [session, setSession] = useState<AuthenticatedSessionResponse | null>(null);
  const [changingPassword, setChangingPassword] = useState(false);
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const check = async () => {
      try {
        const result = await authApi.session();
        if (!active) return;
        setSession(result);
        setSessionError(null);
      } catch (cause) {
        if (!active) return;
        if (cause instanceof PlatformApiError && cause.status === 401) {
          setSession(null);
          setSessionError(null);
        } else {
          const retryable =
            cause instanceof PlatformApiError && (cause.status === 0 || cause.status >= 500);
          setSessionError(
            retryable
              ? cause.status === 0
                ? '服务连接已中断'
                : '服务暂不可用'
              : '无法核对登录状态，请重试。',
          );
          if (retryable) timer = setTimeout(() => void check(), 3000);
        }
      } finally {
        if (active) setChecking(false);
      }
    };
    void check();
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [refresh]);

  if (checking) {
    return (
      <PublicPage>
        <main className="platform-shell">
          <section className="bootstrap-card">
            <p role="status">正在核对服务端 Session…</p>
          </section>
        </main>
      </PublicPage>
    );
  }
  if (sessionError !== null) {
    return (
      <PublicPage>
        <main className="platform-shell">
          <section className="bootstrap-card auth-card">
            <h1>{sessionError}</h1>
            <Alert
              type="warning"
              showIcon
              title="连接失败不代表登录失效。请检查访问隧道或服务连接，恢复后重新核对登录状态。"
            />
            <Button onClick={() => setRefresh((value) => value + 1)}>重新连接</Button>
          </section>
        </main>
      </PublicPage>
    );
  }
  if (session === null) {
    return (
      <PublicPage>
        <LoginPage onAuthenticated={setSession} />
      </PublicPage>
    );
  }
  if (session.mustChangePassword || changingPassword) {
    return (
      <PublicPage>
        <PasswordChangePage
          required={session.mustChangePassword}
          onChanged={(next) => {
            setSession(next);
            setChangingPassword(false);
          }}
          onCancel={() => setChangingPassword(false)}
          onLoggedOut={() => {
            setSession(null);
            setChangingPassword(false);
          }}
        />
      </PublicPage>
    );
  }
  return (
    <AuthenticatedConsole
      onLoggedOut={() => setSession(null)}
      onChangePassword={() => setChangingPassword(true)}
      onRecoveryReady={onRecoveryReady}
      recoveryOnly={recoveryOnly}
      session={session}
    />
  );
}

export function App() {
  const [entry, setEntry] = useState<'BOOTSTRAP' | 'READY' | 'RECOVERY'>('BOOTSTRAP');
  const markReady = useCallback(() => setEntry('READY'), []);
  const markRecovery = useCallback(() => setEntry('RECOVERY'), []);

  if (window.location.pathname === '/activate') {
    return (
      <PublicPage>
        <ActivationPage />
      </PublicPage>
    );
  }
  if (entry !== 'BOOTSTRAP') {
    return <AuthenticatedEntry onRecoveryReady={markReady} recoveryOnly={entry === 'RECOVERY'} />;
  }
  return (
    <PublicPage>
      <BootstrapPage onReady={markReady} onRecovery={markRecovery} />
    </PublicPage>
  );
}
