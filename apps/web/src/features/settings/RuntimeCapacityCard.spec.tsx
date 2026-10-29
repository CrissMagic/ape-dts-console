import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { TaskPreflightCheck } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RuntimeCapacityCard } from './RuntimeCapacityCard';
import { PlatformApiError } from '../../api/platform-api';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

const ready: TaskPreflightCheck = {
  key: 'RUNTIME_ENVIRONMENT',
  code: 'RUNTIME_ENVIRONMENT_READY',
  state: 'PASSED',
  summary: '环境校验通过',
  details: {},
};

afterEach(cleanup);

describe('RuntimeCapacityCard', () => {
  it('does not trap navigation while the initial capacity request is pending', () => {
    const pending = deferred<never>();
    let guard: ((proceed: () => void) => void) | null = null;
    render(
      <RuntimeCapacityCard
        api={{ environment: () => pending.promise, get: () => pending.promise, update: vi.fn() }}
        onLeaveGuardChange={(next) => {
          guard = next;
        }}
      />,
    );
    const proceed = vi.fn();
    act(() => guard?.(proceed));
    expect(proceed).toHaveBeenCalledOnce();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
  it('keeps a newer environment failure when an older readiness request finishes later', async () => {
    const first = deferred<TaskPreflightCheck>();
    const api = {
      environment: vi
        .fn()
        .mockReturnValueOnce(first.promise)
        .mockResolvedValueOnce({
          ...ready,
          state: 'FAILED',
          summary: '运行目录不可写',
        }),
      get: vi.fn().mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '1', used: 0 }),
      update: vi.fn(),
    };
    render(<RuntimeCapacityCard api={api} />);
    await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
    expect(await screen.findByText('运行目录不可写')).toBeInTheDocument();
    await act(async () => first.resolve(ready));
    expect(screen.getByText('运行目录不可写')).toBeInTheDocument();
    expect(screen.queryByText('运行环境已就绪')).not.toBeInTheDocument();
  });

  it('ignores an obsolete environment authorization failure after refresh or unmount', async () => {
    const first = deferred<TaskPreflightCheck>();
    const last = deferred<TaskPreflightCheck>();
    const onSessionInvalidated = vi.fn();
    const api = {
      environment: vi
        .fn()
        .mockReturnValueOnce(first.promise)
        .mockResolvedValueOnce(ready)
        .mockReturnValueOnce(last.promise),
      get: vi.fn().mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '1', used: 0 }),
      update: vi.fn(),
    };
    const view = render(
      <RuntimeCapacityCard api={api} onSessionInvalidated={onSessionInvalidated} />,
    );
    await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
    expect(await screen.findByText('运行环境已就绪')).toBeInTheDocument();
    await act(async () => first.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(onSessionInvalidated).not.toHaveBeenCalled();
    expect(screen.getByText('运行环境已就绪')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
    view.unmount();
    await act(async () => last.reject(new PlatformApiError('SESSION_INVALID', 401)));
    expect(onSessionInvalidated).not.toHaveBeenCalled();
  });

  it.each(['initial', 'reload'])(
    'invalidates the session on the current %s environment request',
    async (phase) => {
      const onSessionInvalidated = vi.fn();
      const environment = vi.fn();
      if (phase === 'reload') environment.mockResolvedValueOnce(ready);
      environment.mockRejectedValue(new PlatformApiError('SESSION_INVALID', 401));
      const api = {
        environment,
        get: vi.fn().mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '1', used: 0 }),
        update: vi.fn(),
      };
      render(<RuntimeCapacityCard api={api} onSessionInvalidated={onSessionInvalidated} />);
      if (phase === 'reload') {
        expect(await screen.findByText('运行环境已就绪')).toBeInTheDocument();
        await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
        fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
      }
      await waitFor(() => expect(onSessionInvalidated).toHaveBeenCalledOnce());
      expect(screen.queryByText('运行环境已就绪')).not.toBeInTheDocument();
    },
  );

  it('does not claim readiness when the environment check fails to load', async () => {
    const api = {
      environment: vi.fn().mockRejectedValue(new Error('offline')),
      get: vi.fn().mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '1', used: 0 }),
      update: vi.fn(),
    };
    render(<RuntimeCapacityCard api={api} />);
    expect(await screen.findByText('运行环境检查未能读取，请重新加载。')).toBeInTheDocument();
    expect(screen.queryByText('运行环境已就绪')).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('spinbutton')).toHaveValue('3'));
    api.environment.mockResolvedValue({
      key: 'RUNTIME_ENVIRONMENT',
      state: 'PASSED',
      summary: '环境校验通过',
      details: {},
    });
    fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
    expect(await screen.findByText('运行环境已就绪')).toBeInTheDocument();
  });

  it('requires an explicit capacity instead of assuming a resource-derived default', async () => {
    const api = {
      environment: vi.fn().mockResolvedValue({
        key: 'RUNTIME_ENVIRONMENT',
        state: 'PASSED',
        summary: '环境校验通过',
        details: {},
      }),
      get: vi.fn().mockResolvedValue({ maxRuntimeUnits: null, rowVersion: '1', used: 0 }),
      update: vi.fn().mockResolvedValue({ maxRuntimeUnits: 3, rowVersion: '2', used: 0 }),
    };
    render(<RuntimeCapacityCard api={api} />);
    expect(await screen.findByText('未配置')).toBeInTheDocument();
    expect(screen.queryByText('运行容量', { exact: true })).not.toBeInTheDocument();
    expect(screen.getByText('任务共享容器 CPU 与内存')).toBeInTheDocument();
    fireEvent.mouseOver(screen.getByLabelText('运行容量说明'));
    expect(await screen.findByRole('tooltip')).toHaveTextContent(
      '容器整体 CPU 和内存限额由 Rancher 或 Docker 配置，包含平台自身开销',
    );
    expect(screen.getByRole('tooltip')).toHaveTextContent('不提供每任务独立硬配额');
    await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
    expect(screen.getByRole('button', { name: /保存容量/u })).toBeDisabled();
    fireEvent.change(screen.getByRole('spinbutton', { name: '最大运行单元数' }), {
      target: { value: '3' },
    });
    fireEvent.click(screen.getByRole('button', { name: /保存容量/u }));
    await waitFor(() =>
      expect(api.update).toHaveBeenCalledWith({ maxRuntimeUnits: 3, expectedRowVersion: '1' }),
    );
    expect(await screen.findByText('运行容量已保存。')).toBeInTheDocument();
  });

  it('preserves the attempted value after a concurrent update and explains reloading', async () => {
    const api = {
      environment: vi.fn().mockResolvedValue({
        key: 'RUNTIME_ENVIRONMENT',
        state: 'FAILED',
        summary: '运行目录不可写',
        details: {},
      }),
      get: vi.fn().mockResolvedValue({ maxRuntimeUnits: 5, rowVersion: '1', used: 2 }),
      update: vi.fn().mockRejectedValue(new Error('CAPACITY_VERSION_CONFLICT')),
    };
    render(<RuntimeCapacityCard api={api} />);
    await waitFor(() => expect(screen.getByRole('spinbutton')).toHaveValue('5'));
    expect(await screen.findByText('运行环境未就绪')).toBeInTheDocument();
    expect(screen.getByText('运行目录不可写')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled());
    fireEvent.change(screen.getByRole('spinbutton'), { target: { value: '3' } });
    fireEvent.click(screen.getByRole('button', { name: /保存容量/u }));
    expect(
      await screen.findByText('配置已被其他管理员修改，请重新加载后再保存。'),
    ).toBeInTheDocument();
    expect(screen.getByRole('spinbutton')).toHaveValue('3');
    expect(screen.queryByText('运行容量已保存。')).not.toBeInTheDocument();
  });
});
