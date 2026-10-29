import { useEffect, useState } from 'react';
import type { RuntimePerformanceSummary } from '@scs/contracts';
import { performanceApi, PlatformApiError } from '../../../api/platform-api';

export function usePerformanceSummaries(
  taskIds: readonly string[],
  onSessionInvalidated: () => void,
  polling = true,
) {
  const key = JSON.stringify(taskIds);
  const [data, setData] = useState<{ key: string; items: RuntimePerformanceSummary[] } | null>(
    null,
  );
  const [error, setError] = useState(false);
  useEffect(() => {
    if (!polling) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    setData(null);
    setError(false);
    const ids: string[] = JSON.parse(key);
    if (ids.length === 0) return;
    const load = async () => {
      try {
        const items: RuntimePerformanceSummary[] = [];
        for (let offset = 0; offset < ids.length; offset += 100) {
          const result = await performanceApi.query(ids.slice(offset, offset + 100));
          if (!active) return;
          items.push(...result.items);
        }
        if (active) {
          setData({ key, items });
          setError(false);
        }
      } catch (cause) {
        if (!active) return;
        setData(null);
        setError(true);
        if (cause instanceof PlatformApiError && cause.status === 401) onSessionInvalidated();
      } finally {
        if (active && polling) timer = setTimeout(() => void load(), 15000);
      }
    };
    void load();
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [key, onSessionInvalidated, polling]);
  return { items: data?.key === key ? data.items : [], error };
}
