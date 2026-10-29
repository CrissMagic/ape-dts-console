import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { AuthenticatedSessionResponse } from '@scs/contracts';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConfigProvider } from 'antd';

import {
  authApi,
  auditApi,
  dataSourcesApi,
  healthApi,
  recoveryApi,
  runtimeCapacityApi,
  tasksApi,
  taskGroupsApi,
  usersApi,
} from '../../api/platform-api';
import { AuthenticatedConsole } from './AuthenticatedConsole';

const session: AuthenticatedSessionResponse = {
  mustChangePassword: false,
  absoluteExpiresAt: '2026-09-08T12:00:00.000Z',
  csrfToken: 'scs-csrf-v1_test',
  idleExpiresAt: '2026-09-08T10:00:00.000Z',
  status: 'AUTHENTICATED',
  user: {
    displayName: 'Admin',
    id: '0199c617-249d-7459-bbcb-f97ad47a3120',
    role: 'ADMIN',
    status: 'ACTIVE',
    username: 'admin',
  },
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

beforeEach(() => {
  vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
  vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [] });
  vi.spyOn(taskGroupsApi, 'list').mockResolvedValue({ groups: [] });
  vi.spyOn(auditApi, 'list').mockResolvedValue({ events: [], nextCursor: null });
});

describe('AuthenticatedConsole', () => {
  it('keeps the account role with the signed-in identity instead of repeating it in the page header', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    const { container } = render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);

    await screen.findByText('管理服务正常');
    const consoleHeader = container.querySelector('.console-header');
    const pageHeader = container.querySelector('.console-page-header');

    expect(consoleHeader).toHaveTextContent('Admin');
    expect(consoleHeader).toHaveTextContent('管理员');
    expect(pageHeader).toBeNull();
    expect(screen.queryByRole('heading', { name: '任务管理' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '任务管理说明' })).not.toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: '面包屑' })).toHaveTextContent('首页');
    expect(screen.getByRole('menuitem', { name: '首页' })).toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: '驾驶舱' })).not.toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: '任务运行' })).not.toBeInTheDocument();
    expect(screen.queryByText('同步管理 · 运行与安全')).not.toBeInTheDocument();
  });

  it('places metadata outage above all management surfaces and points to emergency runtime facts', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('METADATA_DEGRADED');
    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);
    const warning = await screen.findByRole('alert');
    expect(warning).toHaveTextContent('元数据库管理面不可用：METADATA_DEGRADED');
    expect(warning).toHaveTextContent('不要依据旧页面数据判断进程');
    expect(screen.getByRole('menuitem', { name: '紧急停止' })).toBeInTheDocument();
  });

  it('fails closed when the current platform phase cannot be read', async () => {
    vi.spyOn(healthApi, 'phase').mockRejectedValue(new Error('metadata unavailable'));

    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);

    expect(await screen.findByRole('alert')).toHaveTextContent('平台状态不可确认');
    expect(screen.getByRole('heading', { name: '普通管理操作已冻结' })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: '数据源' })).toHaveAttribute(
      'aria-disabled',
      'true',
    );
    expect(screen.getByRole('menuitem', { name: '任务管理' })).toHaveAttribute(
      'aria-disabled',
      'true',
    );
    expect(screen.getByRole('menuitem', { name: '用户管理' })).toHaveAttribute(
      'aria-disabled',
      'true',
    );
    expect(screen.getByRole('button', { name: '打开紧急停止' })).toBeEnabled();
  });

  it('shows only the administrator recovery surface during bootstrap recovery', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('BOOTSTRAPPING');
    vi.spyOn(recoveryApi, 'current').mockResolvedValue({
      candidates: [],
      id: '0199c617-249d-7459-bbcb-f97ad47a3110',
      rowVersion: '2',
      state: 'PAUSED',
      steps: [],
    });

    render(<AuthenticatedConsole onLoggedOut={vi.fn()} recoveryOnly session={session} />);

    expect(await screen.findByRole('alert')).toHaveTextContent('容器恢复尚未完成');
    expect(screen.getByRole('menuitem', { name: '容器恢复' })).toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: '驾驶舱' })).not.toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: '紧急停止' })).not.toBeInTheDocument();
  });
});
describe('console navigation', () => {
  it('keeps only the navigation label for data sources', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);
    await screen.findByText('管理服务正常');
    fireEvent.click(screen.getByRole('menuitem', { name: '数据源' }));
    expect(await screen.findByRole('table', { name: '数据源列表' })).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '数据源' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '数据源目录' })).not.toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: '数据源' })).toBeInTheDocument();
  });

  it('loads and saves runtime capacity from the system management menu', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(runtimeCapacityApi, 'get').mockResolvedValue({
      maxRuntimeUnits: null,
      rowVersion: '4',
      used: 0,
    });
    const update = vi
      .spyOn(runtimeCapacityApi, 'update')
      .mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '5', used: 0 });
    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);
    await screen.findByText('管理服务正常');
    fireEvent.click(screen.getByRole('menuitem', { name: '系统管理' }));
    expect(screen.queryByRole('heading', { name: '系统管理' })).not.toBeInTheDocument();
    expect(screen.getByRole('region', { name: '平台信息' })).toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: '设置分组' })).toBeInTheDocument();
    expect(runtimeCapacityApi.get).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '运行容量' }));
    expect(await screen.findByText('未配置')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
    fireEvent.change(screen.getByRole('spinbutton', { name: '最大运行单元数' }), {
      target: { value: '3' },
    });
    fireEvent.click(screen.getByRole('button', { name: /保存容量/u }));
    await waitFor(() =>
      expect(update).toHaveBeenCalledWith({ maxRuntimeUnits: 3, expectedRowVersion: '4' }),
    );
    expect(await screen.findByText('运行容量已保存。')).toBeInTheDocument();
  });
  it('protects capacity edits when switching setting groups or leaving the settings page', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(runtimeCapacityApi, 'get').mockResolvedValue({
      maxRuntimeUnits: 3,
      rowVersion: '1',
      used: 0,
    });
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />
      </ConfigProvider>,
    );
    await screen.findByText('管理服务正常');
    fireEvent.click(screen.getByRole('menuitem', { name: '系统管理' }));
    fireEvent.click(screen.getByRole('button', { name: '运行容量' }));
    await waitFor(() =>
      expect(screen.getByRole('spinbutton', { name: '最大运行单元数' })).toHaveValue('3'),
    );
    fireEvent.change(screen.getByRole('spinbutton'), { target: { value: '4' } });
    fireEvent.click(screen.getByRole('button', { name: '平台信息' }));
    expect(
      await screen.findByRole('dialog', { name: '放弃未保存的容量配置？' }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
    expect(screen.getByRole('spinbutton')).toHaveValue('4');
    fireEvent.click(screen.getByRole('menuitem', { name: '任务管理' }));
    expect(
      await screen.findByRole('dialog', { name: '放弃未保存的容量配置？' }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '放弃修改并继续' }));
    expect(await screen.findByRole('button', { name: '新建任务' })).toBeInTheDocument();
  });
  it('guides users to sources and protects unsaved edits across navigation, password change and logout', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(tasksApi, 'list').mockResolvedValue({ tasks: [] });
    vi.spyOn(dataSourcesApi, 'list').mockResolvedValue({ dataSources: [] });
    vi.spyOn(usersApi, 'list').mockResolvedValue({ users: [] });
    const logout = vi.spyOn(authApi, 'logout').mockResolvedValue(undefined);
    const onChangePassword = vi.fn();
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <AuthenticatedConsole
          onLoggedOut={vi.fn()}
          onChangePassword={onChangePassword}
          session={session}
        />
      </ConfigProvider>,
    );
    await screen.findByText('管理服务正常');
    fireEvent.click(screen.getByRole('menuitem', { name: '数据源' }));
    fireEvent.click(await screen.findByRole('button', { name: '新建数据源' }));
    expect(screen.getByRole('button', { name: '创建数据源' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: '未保存的连接' } });
    fireEvent.click(screen.getByRole('menuitem', { name: '任务管理' }));
    const navigationPrompt = await screen.findByText('放弃未保存的数据源配置？');
    expect(navigationPrompt.closest('.ant-modal')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
    await waitFor(() =>
      expect(screen.queryByRole('button', { name: '继续编辑' })).not.toBeInTheDocument(),
    );
    expect(screen.getByLabelText('名称')).toHaveValue('未保存的连接');
    fireEvent.keyDown(screen.getByRole('button', { name: '账号菜单' }), { key: 'ArrowDown' });
    fireEvent.click(await screen.findByRole('menuitem', { name: '修改密码' }));
    expect(await screen.findByText('放弃未保存的数据源配置？')).toBeInTheDocument();
    expect(onChangePassword).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
    await waitFor(() =>
      expect(screen.queryByRole('button', { name: '继续编辑' })).not.toBeInTheDocument(),
    );
    fireEvent.keyDown(screen.getByRole('button', { name: '账号菜单' }), { key: 'ArrowDown' });
    fireEvent.click(await screen.findByRole('menuitem', { name: '退出登录' }));
    const logoutPrompt = await screen.findByText('放弃未保存的数据源配置？');
    expect(logoutPrompt.closest('.ant-modal')).toBeInTheDocument();
    expect(logout).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '继续编辑' }));
    await waitFor(() =>
      expect(screen.queryByRole('button', { name: '继续编辑' })).not.toBeInTheDocument(),
    );
    fireEvent.click(screen.getByRole('menuitem', { name: '任务管理' }));
    fireEvent.click(await screen.findByRole('button', { name: '放弃修改并继续' }));
    await screen.findByText('暂无任务');
    fireEvent.click(screen.getByRole('menuitem', { name: '数据源' }));
    fireEvent.click(await screen.findByRole('button', { name: '新建数据源' }));
    expect(await screen.findByLabelText('名称')).toHaveValue('');
  });
  it('opens system management and the administrator audit query', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(auditApi, 'list').mockResolvedValue({ events: [], nextCursor: null });
    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);
    await screen.findByText('管理服务正常');
    fireEvent.click(screen.getByRole('menuitem', { name: '系统管理' }));
    expect(await screen.findByRole('region', { name: '平台信息' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('menuitem', { name: '审计' }));
    expect(await screen.findByText('暂无符合条件的审计记录')).toBeInTheDocument();
    expect(screen.queryByText('审计查询')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: '审计' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '审计说明' })).not.toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: '面包屑' })).toHaveTextContent('审计');
  });
  it('does not expose administrator menus to developers', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    render(
      <AuthenticatedConsole
        onLoggedOut={vi.fn()}
        session={{ ...session, user: { ...session.user, role: 'DEVELOPER' } }}
      />,
    );
    await screen.findByText('管理服务正常');
    expect(screen.queryByRole('menuitem', { name: '系统管理' })).not.toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: '审计' })).not.toBeInTheDocument();
  });

  it('changes password and logs out through the account menu', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    const logout = vi.spyOn(authApi, 'logout').mockResolvedValue(undefined);
    const onLoggedOut = vi.fn();
    const onChangePassword = vi.fn();
    render(
      <AuthenticatedConsole
        onLoggedOut={onLoggedOut}
        onChangePassword={onChangePassword}
        session={session}
      />,
    );
    await screen.findByText('管理服务正常');
    fireEvent.keyDown(screen.getByRole('button', { name: '账号菜单' }), { key: 'ArrowDown' });
    fireEvent.click(await screen.findByRole('menuitem', { name: '修改密码' }));
    expect(onChangePassword).toHaveBeenCalledOnce();
    fireEvent.keyDown(screen.getByRole('button', { name: '账号菜单' }), { key: 'ArrowDown' });
    fireEvent.click(await screen.findByRole('menuitem', { name: '退出登录' }));
    await waitFor(() => expect(onLoggedOut).toHaveBeenCalledOnce());
    expect(logout).toHaveBeenCalledOnce();
  });

  it('does not allow a developer to access recovery-only actions', async () => {
    vi.spyOn(healthApi, 'phase').mockResolvedValue('BOOTSTRAPPING');
    render(
      <AuthenticatedConsole
        onLoggedOut={vi.fn()}
        recoveryOnly
        session={{ ...session, user: { ...session.user, role: 'DEVELOPER' } }}
      />,
    );
    expect(await screen.findByText('仅管理员可以继续容器恢复')).toBeInTheDocument();
    expect(screen.queryByRole('menuitem')).not.toBeInTheDocument();
  });

  it('opens mobile navigation and closes the drawer after selecting a page', async () => {
    const originalMatchMedia = window.matchMedia;
    vi.spyOn(window, 'matchMedia').mockImplementation((query) => ({
      ...originalMatchMedia(query),
      matches: query === '(max-width: 767px)',
    }));
    vi.spyOn(healthApi, 'phase').mockResolvedValue('READY');
    vi.spyOn(runtimeCapacityApi, 'get').mockResolvedValue({
      maxRuntimeUnits: 3,
      rowVersion: '4',
      used: 0,
    });
    render(<AuthenticatedConsole onLoggedOut={vi.fn()} session={session} />);
    await screen.findByText('管理服务正常');
    expect(screen.queryByRole('navigation', { name: '一级导航' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '切换导航' }));
    expect(await screen.findByRole('dialog', { name: '导航菜单' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('menuitem', { name: '系统管理' }));
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: '导航菜单' })).not.toBeInTheDocument(),
    );
    expect(await screen.findByRole('region', { name: '平台信息' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '切换导航' }));
    const drawer = await screen.findByRole('dialog', { name: '导航菜单' });
    await waitFor(() => expect(drawer).toContainElement(document.activeElement as HTMLElement));
    fireEvent.keyDown(drawer, { key: 'Escape' });
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: '导航菜单' })).not.toBeInTheDocument(),
    );
    expect(screen.getByRole('region', { name: '平台信息' })).toBeInTheDocument();
  });
});
