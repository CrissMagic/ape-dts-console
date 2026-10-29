import { useCallback, useRef, useState } from 'react';
import { Bell, Gauge, Info, ShieldCheck, Archive, RefreshCw } from 'lucide-react';
import type { AuthenticatedSessionResponse } from '@scs/contracts';
import { runtimeCapacityApi } from '../../api/platform-api';
import { Button } from '../../components/ui/button';
import type { LeaveGuard } from '../../components/useDraftGuard';
import { RuntimeCapacityCard } from './RuntimeCapacityCard';
import { AlertSettingsCard } from './AlertSettingsCard';
import { SessionSettingsCard } from './SessionSettingsCard';
import { RuntimeRecoverySettingsCard } from './RuntimeRecoverySettingsCard';
import { ConfigTransferCard } from './ConfigTransferCard';

const groups = [
  {
    key: 'recovery',
    label: '任务恢复',
    icon: RefreshCw,
  },
  { key: 'overview', label: '平台信息', icon: Info },
  {
    key: 'capacity',
    label: '运行容量',
    icon: Gauge,
  },
  {
    key: 'sessions',
    label: '登录与会话',
    icon: ShieldCheck,
  },
  {
    key: 'alerts',
    label: '告警通知',
    icon: Bell,
  },
  {
    key: 'transfer',
    label: '配置导入导出',
    icon: Archive,
  },
] as const;

export function SystemSettingsPage({
  session,
  statusText,
  onSessionInvalidated,
  onLeaveGuardChange,
}: {
  session: AuthenticatedSessionResponse;
  statusText: string;
  onSessionInvalidated: () => void;
  onLeaveGuardChange: (guard: LeaveGuard | null) => void;
}) {
  const [section, setSection] = useState<(typeof groups)[number]['key']>('overview');
  const guard = useRef<LeaveGuard | null>(null);
  const registerGuard = useCallback(
    (next: LeaveGuard | null) => {
      guard.current = next;
      onLeaveGuardChange(next);
    },
    [onLeaveGuardChange],
  );
  const current = groups.find((group) => group.key === section)!;

  return (
    <section className="system-settings" aria-label="系统设置">
      <div className="system-settings-layout">
        <nav aria-label="设置分组" className="flex gap-1 lg:flex-col">
          {groups.map(({ key, label, icon: Icon }) => (
            <Button
              key={key}
              variant="ghost"
              aria-current={section === key ? 'page' : undefined}
              className={`justify-start ${section === key ? 'bg-muted font-medium' : 'font-normal'}`}
              onClick={() => {
                if (section === key) return;
                const proceed = () => setSection(key);
                if (guard.current) guard.current(proceed);
                else proceed();
              }}
            >
              <Icon className="size-4" />
              {label}
            </Button>
          ))}
        </nav>
        <div
          className="system-settings-panel"
          key={section}
          role="region"
          aria-label={current.label}
        >
          {section === 'transfer' ? (
            <ConfigTransferCard onSessionInvalidated={onSessionInvalidated} />
          ) : section === 'overview' ? (
            <dl className="divide-y text-sm">
              {[
                ['管理面状态', statusText],
                ['当前账号', session.user.username],
                ['当前角色', '管理员'],
              ].map(([label, value]) => (
                <div className="grid grid-cols-[9rem_1fr] gap-4 py-4 first:pt-0" key={label}>
                  <dt className="text-muted-foreground">{label}</dt>
                  <dd className="font-medium">{value}</dd>
                </div>
              ))}
            </dl>
          ) : section === 'capacity' ? (
            <RuntimeCapacityCard
              api={runtimeCapacityApi}
              onSessionInvalidated={onSessionInvalidated}
              onLeaveGuardChange={registerGuard}
            />
          ) : section === 'recovery' ? (
            <RuntimeRecoverySettingsCard
              onSessionInvalidated={onSessionInvalidated}
              onLeaveGuardChange={registerGuard}
            />
          ) : section === 'sessions' ? (
            <SessionSettingsCard
              onSessionInvalidated={onSessionInvalidated}
              onLeaveGuardChange={registerGuard}
            />
          ) : (
            <AlertSettingsCard
              onSessionInvalidated={onSessionInvalidated}
              onLeaveGuardChange={registerGuard}
            />
          )}
        </div>
      </div>
    </section>
  );
}
