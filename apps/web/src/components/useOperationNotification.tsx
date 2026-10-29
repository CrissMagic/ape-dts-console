import { useCallback, useEffect, useRef } from 'react';
import { notification } from 'antd';

const titles = { success: '操作成功', error: '操作失败', warning: '请注意', info: '操作提示' };
const config = { placement: 'topRight' as const, duration: 5, maxCount: 3 };

export function useOperationNotification() {
  const [api, holder] = notification.useNotification(config);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  const notify = useCallback(
    (type: keyof typeof titles, description: string | null) => {
      if (description === null || !active.current) return;
      api[type]({
        title: titles[type],
        description,
        className: `operation-notification operation-notification-${type}`,
      });
    },
    [api],
  );
  const setError = useCallback((text: string | null) => notify('error', text), [notify]);
  const setNotice = useCallback((text: string | null) => notify('success', text), [notify]);
  return { holder, notify, setError, setNotice };
}
