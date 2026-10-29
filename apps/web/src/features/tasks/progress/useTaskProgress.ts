import { useCallback, useEffect, useRef, useState } from 'react';
import type { TaskProgress } from '@scs/contracts';
import { PlatformApiError, taskProgressApi } from '../../../api/platform-api';

export function useTaskProgress(
  taskIds: readonly string[],
  onSessionInvalidated: () => void,
  intervalMs: number | null = null,
) {
  const key = JSON.stringify(taskIds);
  const [data, setData] = useState<{ key: string; items: readonly TaskProgress[] } | null>(null);
  const [error, setError] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const loader = useRef<(() => Promise<void>) | null>(null);
  const refresh = useCallback(() => loader.current?.() ?? Promise.resolve(), []);
  useEffect(() => {
    let active = true;
    let pending: Promise<void> | null = null;
    setData(null);
    setError(false);
    setRefreshing(false);
    if (JSON.parse(key).length === 0) return;
    const load = () => {
      if (pending !== null) return pending;
      setRefreshing(true);
      pending = (async () => {
        try {
          const items = await taskProgressApi.list();
          if (!active) return;
          setData({ key, items });
          setError(false);
        } catch (cause) {
          if (!active) return;
          setError(true);
          if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
        } finally {
          pending = null;
          if (active) setRefreshing(false);
        }
      })();
      return pending;
    };
    loader.current = load;
    void load();
    return () => {
      active = false;
      if (loader.current === load) loader.current = null;
    };
  }, [key, onSessionInvalidated]);
  useEffect(() => {
    if (intervalMs === null || JSON.parse(key).length === 0) return;
    let active = true;
    let timer = setTimeout(() => void poll(), intervalMs);
    async function poll() {
      await refresh();
      if (active) timer = setTimeout(() => void poll(), intervalMs!);
    }
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [intervalMs, key, refresh]);
  return { items: data?.key === key ? data.items : [], error, refreshing, refresh };
}
