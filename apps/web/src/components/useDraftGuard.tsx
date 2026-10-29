import { useCallback, useEffect, useState } from 'react';
import { Modal } from 'antd';

export type LeaveGuard = (proceed: () => void) => void;

interface DraftGuardCopy {
  readonly description: string;
  readonly title: string;
}

const dataSourceCopy: DraftGuardCopy = {
  description:
    '离开后，尚未保存的连接信息将被清除。选择“继续编辑”可保留当前填写内容。连接密码仅留在当前页面内存中，不会保存到浏览器缓存。',
  title: '放弃未保存的数据源配置？',
};

/** Keep secrets in component memory only; ask before discarding the editor. */
export function useDraftGuard(
  dirty: boolean,
  saving: boolean,
  copy: DraftGuardCopy = dataSourceCopy,
) {
  const [leave, setLeave] = useState<(() => void) | null>(null);
  const requestLeave = useCallback<LeaveGuard>(
    (proceed) => {
      if (saving) return;
      if (dirty) setLeave(() => proceed);
      else proceed();
    },
    [dirty, saving],
  );

  useEffect(() => {
    if (!dirty && !saving) return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [dirty, saving]);

  return {
    requestLeave,
    prompt: (
      <Modal
        title={copy.title}
        open={leave !== null}
        okText="放弃修改并继续"
        cancelText="继续编辑"
        okButtonProps={{ danger: true, disabled: saving }}
        onCancel={() => setLeave(null)}
        onOk={() => {
          if (saving) return;
          setLeave(null);
          leave?.();
        }}
        mask={{ closable: false }}
      >
        <p>{copy.description}</p>
      </Modal>
    ),
  };
}
