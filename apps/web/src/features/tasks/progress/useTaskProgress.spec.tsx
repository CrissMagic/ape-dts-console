import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { PlatformApiError, taskProgressApi } from '../../../api/platform-api';
import { useTaskProgress } from './useTaskProgress';
import type { TaskProgress } from '@scs/contracts';

const progress: TaskProgress = {
  taskId: 'a',
  revisionId: 'revision-a',
  collectedAt: '2026-10-19T00:00:00.000Z',
  status: 'AVAILABLE',
  sourceType: 'KAFKA',
  slotStatus: null,
  unconfirmedWalBytes: null,
  retainedWalBytes: null,
  offsetLag: '42',
  timeDelayMilliseconds: null,
  timeDelayStatus: 'UNSUPPORTED',
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('task progress refresh', () => {
  it('keeps the last successful read after failure and notifies expired sessions', async () => {
    const read = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([progress]);
    const invalidated = vi.fn();
    const view = renderHook(() => useTaskProgress(['a'], invalidated));
    await act(async () => {});
    expect(view.result.current.items).toEqual([progress]);
    read.mockRejectedValueOnce(new Error('network unavailable'));
    await act(async () => view.result.current.refresh());
    expect(view.result.current.items).toEqual([progress]);
    expect(view.result.current.error).toBe(true);
    expect(invalidated).not.toHaveBeenCalled();
    read.mockRejectedValueOnce(new PlatformApiError('SESSION_INVALID', 401));
    await act(async () => view.result.current.refresh());
    expect(invalidated).toHaveBeenCalledTimes(1);
    expect(view.result.current.items).toEqual([progress]);
    await act(async () => view.result.current.refresh());
    expect(view.result.current.error).toBe(false);
  });

  it('defaults to manual refresh and retains results when the interval changes', async () => {
    vi.useFakeTimers();
    const read = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([progress]);
    const invalidated = vi.fn();
    const view = renderHook(({ interval }) => useTaskProgress(['a'], invalidated, interval), {
      initialProps: { interval: undefined as number | null | undefined },
    });
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(read).toHaveBeenCalledTimes(1);
    view.rerender({ interval: 5_000 });
    expect(view.result.current.items).toEqual([progress]);
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(read).toHaveBeenCalledTimes(2);
    view.rerender({ interval: null });
    expect(view.result.current.items).toEqual([progress]);
  });

  it('keeps manual reads single-flight and stops polling when the interval is disabled', async () => {
    vi.useFakeTimers();
    let resolvePending!: (value: []) => void;
    const read = vi.spyOn(taskProgressApi, 'list').mockResolvedValue([]);
    const invalidated = vi.fn();
    const view = renderHook(({ interval }) => useTaskProgress(['a'], invalidated, interval), {
      initialProps: { interval: 15_000 as number | null },
    });
    await act(async () => {});
    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(read).toHaveBeenCalledTimes(2);
    view.rerender({ interval: null });
    await act(async () => vi.advanceTimersByTimeAsync(60_000));
    expect(read).toHaveBeenCalledTimes(2);
    read.mockReturnValue(
      new Promise((resolve) => {
        resolvePending = resolve;
      }),
    );
    act(() => {
      void view.result.current.refresh();
      void view.result.current.refresh();
    });
    expect(read).toHaveBeenCalledTimes(3);
    expect(view.result.current.refreshing).toBe(true);
    await act(async () => resolvePending([]));
    expect(view.result.current.error).toBe(false);
    expect(view.result.current.refreshing).toBe(false);
  });

  it('ignores an old task response and a late unauthorized response after unmount', async () => {
    let rejectOld!: (cause: unknown) => void;
    let rejectLast!: (cause: unknown) => void;
    const read = vi
      .spyOn(taskProgressApi, 'list')
      .mockReturnValueOnce(
        new Promise((_resolve, reject) => {
          rejectOld = reject;
        }),
      )
      .mockResolvedValueOnce([]);
    const invalidated = vi.fn();
    const view = renderHook(({ id }) => useTaskProgress([id], invalidated, null), {
      initialProps: { id: 'a' },
    });
    view.rerender({ id: 'b' });
    await act(async () => {});
    await act(async () => rejectOld(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
    expect(view.result.current.error).toBe(false);
    read.mockReturnValue(
      new Promise((_resolve, reject) => {
        rejectLast = reject;
      }),
    );
    act(() => {
      void view.result.current.refresh();
    });
    view.unmount();
    await act(async () => rejectLast(new PlatformApiError('SESSION_INVALID', 401)));
    expect(invalidated).not.toHaveBeenCalled();
  });

  it('discards a late successful response after changing task context', async () => {
    let resolveOld!: (items: TaskProgress[]) => void;
    const nextProgress = { ...progress, taskId: 'b', offsetLag: '7' };
    vi.spyOn(taskProgressApi, 'list')
      .mockReturnValueOnce(
        new Promise((resolve) => {
          resolveOld = resolve;
        }),
      )
      .mockResolvedValueOnce([nextProgress]);
    const invalidated = vi.fn();
    const view = renderHook(({ id }) => useTaskProgress([id], invalidated), {
      initialProps: { id: 'a' },
    });
    expect(view.result.current.refreshing).toBe(true);
    view.rerender({ id: 'b' });
    await act(async () => {});
    expect(view.result.current.items).toEqual([nextProgress]);
    await act(async () => resolveOld([progress]));
    expect(view.result.current.items).toEqual([nextProgress]);
    expect(view.result.current.refreshing).toBe(false);
  });
});
