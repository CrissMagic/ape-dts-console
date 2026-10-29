import { useCallback, useEffect, useRef, useState } from 'react';
import type { AuthenticatedSessionResponse, PlatformRuntimePhase } from '@scs/contracts';
import {
  Boxes,
  ChevronsUpDown,
  Database,
  FileClock,
  GitBranch,
  House,
  KeyRound,
  LogOut,
  RefreshCw,
  Settings,
  ShieldAlert,
  Users,
} from 'lucide-react';
import { authApi, healthApi } from '../../api/platform-api';
import { Button } from '../../components/ui/button';
import { ThemeToggle } from '../../components/ThemeToggle';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '../../components/ui/card';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../../components/ui/dropdown-menu';
import { TooltipProvider } from '../../components/ui/tooltip';
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
  useSidebar,
} from '../../components/ui/sidebar';
import { SystemSettingsPage } from '../settings/SystemSettingsPage';
import { ArtifactManagementPage } from '../artifacts/ArtifactManagementPage';
import { AuditPage } from '../audit/AuditPage';
import { DataSourcesPage } from '../data-sources/DataSourcesPage';
import { EmergencyPage } from '../emergency/EmergencyPage';
import { RecoveryPage } from '../recovery/RecoveryPage';
import { TaskManagementPage } from '../tasks/TaskManagementPage';
import { AdminUsersPage } from '../users/AdminUsersPage';
import { HomePage } from '../home/HomePage';
import type { LeaveGuard } from '../../components/useDraftGuard';

type ConsoleView =
  | 'HOME'
  | 'DATA_SOURCES'
  | 'TASKS'
  | 'RECOVERY'
  | 'EMERGENCY'
  | 'USERS'
  | 'SYSTEM'
  | 'ARTIFACTS'
  | 'AUDIT';
const pages = {
  HOME: { title: '首页', icon: House },
  DATA_SOURCES: {
    title: '数据源',
    icon: Database,
  },
  TASKS: {
    title: '任务管理',
    icon: GitBranch,
  },
  RECOVERY: {
    title: '容器恢复',
    icon: RefreshCw,
  },
  EMERGENCY: {
    title: '紧急停止',
    icon: ShieldAlert,
  },
  USERS: { title: '用户管理', icon: Users },
  SYSTEM: { title: '系统管理', icon: Settings },
  ARTIFACTS: { title: '运行制品', icon: Boxes },
  AUDIT: {
    title: '审计',
    icon: FileClock,
  },
};

function ConsoleNavigation({
  view,
  administrator,
  recoveryOnly,
  unavailable,
  navigate,
}: {
  view: ConsoleView;
  administrator: boolean;
  recoveryOnly: boolean;
  unavailable: boolean;
  navigate: (next: ConsoleView) => void;
}) {
  const { setOpenMobile, isMobile, state } = useSidebar();
  const groups: { title: string; views: ConsoleView[] }[] = recoveryOnly
    ? [{ title: '平台恢复', views: administrator ? ['RECOVERY'] : [] }]
    : [
        { title: '同步工作', views: ['HOME', 'TASKS', 'DATA_SOURCES'] },
        ...(administrator
          ? [
              {
                title: '系统与安全',
                views: ['SYSTEM', 'ARTIFACTS', 'USERS', 'AUDIT', 'RECOVERY'] as ConsoleView[],
              },
            ]
          : []),
        { title: '应急操作', views: ['EMERGENCY'] },
      ];
  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="h-20 justify-center px-4 group-data-[collapsible=icon]:px-1">
        <div className="flex items-center gap-3 overflow-hidden whitespace-nowrap group-data-[collapsible=icon]:justify-center">
          <div className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Database className="size-5" />
          </div>
          <div className="group-data-[collapsible=icon]:hidden">
            <span className="block text-base font-semibold tracking-tight">ape-dts</span>
            <span className="text-xs text-muted-foreground">数据同步管理平台</span>
          </div>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <nav aria-label="一级导航">
          {groups.map((group) => (
            <SidebarGroup key={group.title}>
              <SidebarGroupLabel>{group.title}</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu role="menu" aria-label={group.title}>
                  {group.views.map((key) => {
                    const Icon = pages[key].icon;
                    const disabled = unavailable && key !== 'EMERGENCY' && key !== 'RECOVERY';
                    return (
                      <SidebarMenuItem key={key}>
                        <SidebarMenuButton
                          role="menuitem"
                          aria-label={pages[key].title}
                          aria-disabled={disabled}
                          disabled={disabled}
                          isActive={view === key}
                          {...(!isMobile && state === 'collapsed'
                            ? { tooltip: pages[key].title }
                            : {})}
                          onClick={() => {
                            navigate(key);
                            setOpenMobile(false);
                          }}
                        >
                          <Icon />
                          <span>{pages[key].title}</span>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    );
                  })}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          ))}
        </nav>
      </SidebarContent>
    </Sidebar>
  );
}

export function AuthenticatedConsole({
  session,
  onLoggedOut,
  onChangePassword,
  onRecoveryReady,
  recoveryOnly = false,
}: {
  session: AuthenticatedSessionResponse;
  onLoggedOut: () => void;
  onChangePassword?: () => void;
  onRecoveryReady?: (() => void) | undefined;
  recoveryOnly?: boolean;
}) {
  const [view, setView] = useState<ConsoleView>(recoveryOnly ? 'RECOVERY' : 'HOME');
  const [initialTaskId, setInitialTaskId] = useState<string | undefined>();
  const leaveGuard = useRef<LeaveGuard | null>(null);
  const setLeaveGuard = useCallback((guard: LeaveGuard | null) => {
    leaveGuard.current = guard;
  }, []);
  const guarded = (action: () => void) => {
    if (leaveGuard.current) leaveGuard.current(action);
    else action();
  };
  const navigate = (next: ConsoleView) => {
    if (next !== view)
      guarded(() => {
        setInitialTaskId(undefined);
        setView(next);
      });
  };
  const [platformPhase, setPlatformPhase] = useState<PlatformRuntimePhase | null>(null);
  const [phaseCheckFailed, setPhaseCheckFailed] = useState(false);
  const administrator = session.user.role === 'ADMIN';
  useEffect(() => {
    let active = true;
    const refresh = () =>
      void healthApi
        .phase()
        .then((phase) => {
          if (active) {
            setPlatformPhase(phase);
            setPhaseCheckFailed(false);
          }
        })
        .catch(() => {
          if (active) setPhaseCheckFailed(true);
        });
    refresh();
    const timer = window.setInterval(refresh, 10000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);
  const managementUnavailable = recoveryOnly || phaseCheckFailed || platformPhase !== 'READY';
  const ordinaryView = !['EMERGENCY', 'RECOVERY'].includes(view);
  const statusText = phaseCheckFailed
    ? '状态不可确认'
    : platformPhase === null
      ? '正在检查'
      : platformPhase === 'READY'
        ? '管理服务正常'
        : platformPhase;
  const legacyView = ['USERS', 'RECOVERY', 'EMERGENCY', 'DATA_SOURCES', 'AUDIT'].includes(view);
  return (
    <TooltipProvider>
      <SidebarProvider className="shadcn-console">
        <ConsoleNavigation
          view={view}
          administrator={administrator}
          recoveryOnly={recoveryOnly}
          unavailable={managementUnavailable}
          navigate={navigate}
        />
        <SidebarInset className="min-w-0">
          <header className="console-header flex h-16 shrink-0 items-center justify-between gap-3 border-b px-4 md:px-6">
            <div className="flex min-w-0 items-center gap-3">
              <SidebarTrigger aria-label="切换导航" />
              <span className="h-4 w-px bg-border" />
              <nav aria-label="面包屑" className="flex items-center gap-2 text-sm">
                <span aria-current="page">{pages[view].title}</span>
              </nav>
            </div>
            <div className="flex items-center gap-3">
              <ThemeToggle />
              <span className="hidden items-center gap-2 text-xs text-muted-foreground sm:flex">
                <span
                  className={`size-2 rounded-full ${managementUnavailable ? 'bg-amber-500' : 'bg-emerald-500'}`}
                />
                {statusText}
              </span>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    className="h-auto gap-2 px-2 py-1.5"
                    aria-label="账号菜单"
                  >
                    <span className="flex size-8 items-center justify-center rounded-full bg-muted font-medium">
                      {session.user.displayName.slice(0, 1)}
                    </span>
                    <span className="hidden text-left sm:block">
                      <span className="block text-sm font-medium">{session.user.displayName}</span>
                      <span className="block text-xs font-normal text-muted-foreground">
                        {administrator ? '管理员' : '开发者'}
                      </span>
                    </span>
                    <ChevronsUpDown className="size-4 text-muted-foreground" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuLabel>
                    {session.user.displayName}
                    <span className="block text-xs font-normal text-muted-foreground">
                      {session.user.username}
                    </span>
                  </DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  {onChangePassword && (
                    <DropdownMenuItem onSelect={() => guarded(onChangePassword)}>
                      <KeyRound />
                      修改密码
                    </DropdownMenuItem>
                  )}
                  <DropdownMenuItem
                    onSelect={() => guarded(() => void authApi.logout().finally(onLoggedOut))}
                  >
                    <LogOut />
                    退出登录
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </header>
          <main
            className={`console-workspace-main mx-auto w-full max-w-[1600px] min-w-0 space-y-6 p-4 md:p-6 lg:p-8 ${['HOME', 'TASKS', 'SYSTEM', 'DATA_SOURCES'].includes(view) ? 'console-contained' : ''}`}
          >
            {managementUnavailable &&
              (phaseCheckFailed || platformPhase !== null || recoveryOnly) && (
                <div
                  role="alert"
                  className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-950 dark:border-amber-900 dark:bg-amber-950/50 dark:text-amber-200"
                >
                  <p className="flex items-center gap-2 font-medium">
                    <ShieldAlert className="size-4" />
                    {recoveryOnly
                      ? '容器恢复尚未完成'
                      : phaseCheckFailed
                        ? '平台状态不可确认'
                        : '元数据库管理面不可用：' + platformPhase}
                  </p>
                  <p className="mt-2">
                    普通管理操作已冻结；不要依据旧页面数据判断进程。需要停止实际进程时使用紧急停止。
                  </p>
                </div>
              )}
            <div className={`console-page-content ${legacyView ? 'legacy-console' : ''}`}>
              {!recoveryOnly && managementUnavailable && ordinaryView ? (
                <Card className="text-center shadow-none">
                  <CardHeader>
                    <CardTitle>
                      <h2>
                        {platformPhase === null && !phaseCheckFailed
                          ? '正在核对平台状态'
                          : '普通管理操作已冻结'}
                      </h2>
                    </CardTitle>
                    <CardDescription>确认管理面可用后才会开放业务操作。</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Button variant="outline" onClick={() => setView('EMERGENCY')}>
                      打开紧急停止
                    </Button>
                  </CardContent>
                </Card>
              ) : recoveryOnly && !administrator ? (
                <Card>
                  <CardContent className="py-8 text-center text-muted-foreground">
                    仅管理员可以继续容器恢复
                  </CardContent>
                </Card>
              ) : view === 'USERS' && administrator ? (
                <AdminUsersPage onSessionInvalidated={onLoggedOut} session={session} />
              ) : view === 'RECOVERY' && administrator ? (
                <RecoveryPage onReady={onRecoveryReady} />
              ) : view === 'EMERGENCY' ? (
                <EmergencyPage />
              ) : view === 'TASKS' ? (
                <TaskManagementPage
                  initialDetailTaskId={initialTaskId}
                  onLeaveGuardChange={setLeaveGuard}
                  onOpenDataSources={() => navigate('DATA_SOURCES')}
                  onSessionInvalidated={onLoggedOut}
                  session={session}
                />
              ) : view === 'HOME' ? (
                <HomePage
                  key={`${session.user.id}:${session.user.role}`}
                  session={session}
                  onSessionInvalidated={onLoggedOut}
                  onViewAudit={() => navigate('AUDIT')}
                  onOpenTask={(taskId) =>
                    guarded(() => {
                      setInitialTaskId(taskId);
                      setView('TASKS');
                    })
                  }
                />
              ) : view === 'DATA_SOURCES' ? (
                <DataSourcesPage
                  onLeaveGuardChange={setLeaveGuard}
                  onSessionInvalidated={onLoggedOut}
                  session={session}
                />
              ) : view === 'AUDIT' && administrator ? (
                <AuditPage onSessionInvalidated={onLoggedOut} />
              ) : view === 'ARTIFACTS' && administrator ? (
                <ArtifactManagementPage onSessionInvalidated={onLoggedOut} />
              ) : view === 'SYSTEM' && administrator ? (
                <SystemSettingsPage
                  session={session}
                  statusText={statusText}
                  onSessionInvalidated={onLoggedOut}
                  onLeaveGuardChange={setLeaveGuard}
                />
              ) : (
                <Card>
                  <CardContent className="py-8 text-center text-muted-foreground">
                    当前账号无权访问此页面
                  </CardContent>
                </Card>
              )}
            </div>
          </main>
        </SidebarInset>
      </SidebarProvider>
    </TooltipProvider>
  );
}
