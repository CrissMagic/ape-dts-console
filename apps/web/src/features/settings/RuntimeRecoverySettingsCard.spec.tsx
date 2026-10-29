import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { PlatformApiError, runtimeRecoverySettingsApi } from '../../api/platform-api';
import { RuntimeRecoverySettingsCard } from './RuntimeRecoverySettingsCard';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
function setup() {
  const settings = { enabled: true, maxAttempts: 3, retryDelaySeconds: 30, rowVersion: '7' };
  vi.spyOn(runtimeRecoverySettingsApi, 'get').mockResolvedValue(settings);
  const update = vi
    .spyOn(runtimeRecoverySettingsApi, 'update')
    .mockImplementation(async (input) => ({ ...input, rowVersion: '8' }));
  const invalidate = vi.fn();
  render(
    <RuntimeRecoverySettingsCard onSessionInvalidated={invalidate} onLeaveGuardChange={vi.fn()} />,
  );
  return { update, invalidate };
}
it('shows the saved policy and saves attempt count, delay and enabled state together', async () => {
  const f = setup();
  const attempts = await screen.findByRole('spinbutton', { name: '最大尝试次数' });
  expect(attempts).toHaveValue('3');
  expect(screen.getByRole('spinbutton', { name: '基础重试间隔（秒）' })).toHaveValue('30');
  fireEvent.change(attempts, { target: { value: '5' } });
  fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  await waitFor(() =>
    expect(f.update).toHaveBeenCalledWith({
      enabled: true,
      maxAttempts: 5,
      retryDelaySeconds: 30,
      expectedRowVersion: '7',
    }),
  );
  expect(
    await screen.findByText('任务恢复设置已保存，后续恢复尝试按新策略执行。'),
  ).toBeInTheDocument();
});
it('disables numerical editing when switched off but still permits saving that change', async () => {
  const f = setup();
  await screen.findByRole('spinbutton', { name: '最大尝试次数' });
  fireEvent.click(screen.getByRole('switch', { name: '任务异常后自动恢复' }));
  expect(screen.getByRole('spinbutton', { name: '最大尝试次数' })).toBeDisabled();
  expect(screen.queryByLabelText('重试等待时间')).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  await waitFor(() =>
    expect(f.update).toHaveBeenCalledWith(expect.objectContaining({ enabled: false })),
  );
});
it.each([
  [30, 3, '30 → 60 → 120 秒'],
  [400, 4, '400 → 800 → 900 → 900 秒'],
  [1200, 3, '1200 → 1200 → 1200 秒'],
  [5, 1, '5 秒'],
])('previews retry delays for %i seconds and %i attempts', async (delay, attempts, expected) => {
  const f = setup();
  const count = await screen.findByRole('spinbutton', { name: '最大尝试次数' });
  fireEvent.change(count, { target: { value: String(attempts) } });
  fireEvent.change(screen.getByRole('spinbutton', { name: '基础重试间隔（秒）' }), {
    target: { value: String(delay) },
  });
  expect(screen.getByLabelText('重试等待时间')).toHaveTextContent(expected);
  expect(f.update).not.toHaveBeenCalled();
  fireEvent.change(count, { target: { value: '' } });
  expect(screen.queryByLabelText('重试等待时间')).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: '保存设置' })).toBeDisabled();
});
it('keeps invalid empty edits unsaved and preserves the draft on version conflict', async () => {
  const f = setup();
  const attempts = await screen.findByRole('spinbutton', { name: '最大尝试次数' });
  fireEvent.change(attempts, { target: { value: '' } });
  expect(screen.getByRole('button', { name: '保存设置' })).toBeDisabled();
  fireEvent.change(attempts, { target: { value: '4' } });
  f.update.mockRejectedValue(new PlatformApiError('RUNTIME_RECOVERY_SETTINGS_CONFLICT', 409));
  fireEvent.click(screen.getByRole('button', { name: '保存设置' }));
  expect(
    await screen.findByText('配置已被其他管理员修改，请重新加载后再保存。'),
  ).toBeInTheDocument();
  expect(attempts).toHaveValue('4');
});
