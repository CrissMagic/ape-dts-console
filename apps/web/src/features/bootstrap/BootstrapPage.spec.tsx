import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { bootstrapApi, BootstrapApiError } from '../../api/bootstrap-api';
import { BootstrapPage } from './BootstrapPage';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});
it('does not suggest database configuration for a phase without a reported startup issue', async () => {
  vi.spyOn(bootstrapApi, 'getStatus').mockResolvedValue({
    phase: 'SEALED',
    platformInstanceId: '0199c617-249d-7459-bbcb-f97ad47a3120',
  });
  render(<BootstrapPage />);
  await screen.findByText('等待平台启动');
  expect(screen.queryByText(/未配置数据库时/)).not.toBeInTheDocument();
  expect(screen.queryByText(/请由部署人员检查容器配置/)).not.toBeInTheDocument();
});
it('shows a disconnected connection separately and automatically routes when the tunnel returns', async () => {
  vi.useFakeTimers();
  const onReady = vi.fn();
  vi.spyOn(bootstrapApi, 'getStatus')
    .mockRejectedValueOnce(new BootstrapApiError('NETWORK_ERROR', 0))
    .mockResolvedValue({
      phase: 'READY',
      platformInstanceId: '0199c617-249d-7459-bbcb-f97ad47a3120',
    });
  render(<BootstrapPage onReady={onReady} />);
  await act(async () => {});
  expect(screen.getByText('服务连接已中断')).toBeInTheDocument();
  expect(screen.queryByText(/未配置数据库时/)).not.toBeInTheDocument();
  expect(onReady).not.toHaveBeenCalled();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(3000);
  });
  expect(onReady).toHaveBeenCalledOnce();
});
it('still displays a database configuration issue when explicitly returned by the server', async () => {
  vi.spyOn(bootstrapApi, 'getStatus').mockResolvedValue({
    phase: 'SEALED',
    platformInstanceId: '0199c617-249d-7459-bbcb-f97ad47a3120',
    startupIssue: 'DATABASE_CONFIGURATION_REQUIRED',
  });
  render(<BootstrapPage />);
  expect(
    await screen.findByText('请设置部署环境变量 SCS_PLATFORM_DATABASE_CONFIG。'),
  ).toBeInTheDocument();
});
it.each(['READY', 'RECOVERING'] as const)(
  'blocks %s routing while a startup issue exists, then resumes after it clears',
  async (phase) => {
    const onReady = vi.fn();
    const onRecovery = vi.fn();
    vi.spyOn(bootstrapApi, 'getStatus')
      .mockResolvedValueOnce({
        phase,
        platformInstanceId: '0199c617-249d-7459-bbcb-f97ad47a3120',
        startupIssue: 'LOCAL_STARTUP_FAILED',
      })
      .mockResolvedValue({ phase, platformInstanceId: '0199c617-249d-7459-bbcb-f97ad47a3120' });
    render(<BootstrapPage onReady={onReady} onRecovery={onRecovery} />);
    expect(await screen.findByText('平台启动受阻')).toBeInTheDocument();
    expect(screen.getByText('自动启动失败，请由部署人员检查容器日志。')).toBeInTheDocument();
    expect(screen.queryByText('平台已就绪')).not.toBeInTheDocument();
    expect(onReady).not.toHaveBeenCalled();
    expect(onRecovery).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '刷新状态' }));
    await screen.findByText(phase === 'READY' ? '平台已就绪' : '等待平台启动');
    expect(phase === 'READY' ? onReady : onRecovery).toHaveBeenCalledTimes(1);
    expect(phase === 'READY' ? onRecovery : onReady).not.toHaveBeenCalled();
  },
);
