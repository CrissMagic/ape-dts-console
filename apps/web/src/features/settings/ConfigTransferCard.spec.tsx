import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import type { ConfigBundle, ConfigImportPreview } from '@scs/contracts';
import { ConfigTransferCard } from './ConfigTransferCard';
const originalCreate = Object.getOwnPropertyDescriptor(URL, 'createObjectURL');
const originalRevoke = Object.getOwnPropertyDescriptor(URL, 'revokeObjectURL');
import { PlatformApiError } from '../../api/platform-api';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  if (originalCreate) Object.defineProperty(URL, 'createObjectURL', originalCreate);
  else Reflect.deleteProperty(URL, 'createObjectURL');
  if (originalRevoke) Object.defineProperty(URL, 'revokeObjectURL', originalRevoke);
  else Reflect.deleteProperty(URL, 'revokeObjectURL');
});
const bundle: ConfigBundle = {
  format: 'scs-platform-config',
  version: 1,
  exportedAt: '2026-10-23T03:00:00Z',
  dataSources: [],
  tasks: [],
  groups: [],
  system: null,
};
const preview: ConfigImportPreview = {
  previewHash: 'a'.repeat(64),
  dataSources: 2,
  tasks: 1,
  groups: 1,
  system: false,
  conflicts: [],
  warnings: ['密码须重新配置。'],
};
function setup(conflicts: string[] = []) {
  const api = {
    export: vi.fn(async () => bundle),
    preview: vi.fn(async () => ({ ...preview, conflicts })),
    import: vi.fn(async () => ({ dataSources: 2, tasks: 1 })),
  };
  const sessionInvalidated = vi.fn();
  const view = render(<ConfigTransferCard api={api} onSessionInvalidated={sessionInvalidated} />);
  return { api, sessionInvalidated, ...view };
}
async function upload(container: HTMLElement, content = JSON.stringify(bundle)) {
  fireEvent.click(screen.getByRole('tab', { name: '导入配置' }));
  const file = new File([content], 'config.json', { type: 'application/json' });
  Object.defineProperty(file, 'text', { value: async () => content });
  fireEvent.change(container.querySelector('input[type="file"]')!, { target: { files: [file] } });
}
it('previews without writing and imports only after a second explicit confirmation', async () => {
  const { container, api } = setup();
  await upload(container);
  fireEvent.click(await screen.findByRole('button', { name: '确认导入' }));
  expect(api.import).not.toHaveBeenCalled();
  expect(await screen.findByRole('dialog', { name: '确认导入配置' })).toBeInTheDocument();
  const execute = screen.getByRole('button', { name: '执行导入' });
  fireEvent.click(execute);
  fireEvent.click(execute);
  await waitFor(() =>
    expect(api.import).toHaveBeenCalledExactlyOnceWith(bundle, preview.previewHash),
  );
  await screen.findByText(/已导入 2 个数据源、1 个任务/u);
});
it('blocks conflicted files and does not send malformed files to the server', async () => {
  const { container, api } = setup(['数据源同名：demo']);
  await upload(container);
  expect(await screen.findByRole('button', { name: '确认导入' })).toBeDisabled();
  expect(api.import).not.toHaveBeenCalled();
  await upload(container, JSON.stringify({ ...bundle, password: 'forbidden' }));
  await screen.findByText('文件格式无效或版本不受支持。');
  expect(api.preview).toHaveBeenCalledTimes(1);
});
it('discards a stale preview after a failed confirmation and invalidates expired sessions', async () => {
  const { container, api, sessionInvalidated } = setup();
  api.import.mockRejectedValue(new PlatformApiError('CONFIG_IMPORT_RECHECK_REQUIRED', 409));
  await upload(container);
  fireEvent.click(await screen.findByRole('button', { name: '确认导入' }));
  fireEvent.click(await screen.findByRole('button', { name: '执行导入' }));
  await screen.findByText('目标配置已变化或存在冲突，请重新读取预览后确认。');
  expect(screen.queryByRole('button', { name: '确认导入' })).not.toBeInTheDocument();
  api.preview.mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
  await upload(container);
  await waitFor(() => expect(sessionInvalidated).toHaveBeenCalledOnce());
});
it('exports only the selected categories and disables empty selections', async () => {
  const { api } = setup();
  const createUrl = vi.fn(() => 'blob:config');
  Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: createUrl });
  Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: vi.fn() });
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
  fireEvent.click(screen.getByRole('checkbox', { name: '数据源' }));
  fireEvent.click(screen.getByRole('checkbox', { name: '同步任务及任务组' }));
  expect(screen.getByRole('button', { name: '下载配置文件' })).toBeDisabled();
  fireEvent.click(screen.getByRole('checkbox', { name: '系统配置' }));
  fireEvent.click(screen.getByRole('button', { name: '下载配置文件' }));
  await waitFor(() =>
    expect(api.export).toHaveBeenCalledExactlyOnceWith({
      dataSources: false,
      tasks: false,
      system: true,
    }),
  );
  expect(createUrl).toHaveBeenCalledOnce();
});
