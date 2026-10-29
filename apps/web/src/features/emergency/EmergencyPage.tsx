import { useState } from 'react';
import { Input } from 'antd';

import {
  emergencyApi,
  PlatformApiError,
  type EmergencyRuntimeTaskView,
} from '../../api/platform-api';

export function EmergencyPage() {
  const [code, setCode] = useState('');
  const [tasks, setTasks] = useState<readonly EmergencyRuntimeTaskView[]>([]);
  const [forceTask, setForceTask] = useState<EmergencyRuntimeTaskView | null>(null);
  const [reason, setReason] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const errorCode = (cause: unknown) =>
    cause instanceof PlatformApiError ? cause.code : 'UNKNOWN_ERROR';

  async function perform(action: () => Promise<unknown>, refresh = true) {
    setBusy(true);
    setError(null);
    try {
      await action();
      if (refresh) setTasks(await emergencyApi.list(code));
    } catch (cause) {
      setError(errorCode(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <div className="content-heading">
        <div>
          <p className="eyebrow emergency-eyebrow">EMERGENCY</p>
          <h2>元数据库故障紧急停止</h2>
        </div>
      </div>
      <p className="error-banner">
        该入口只读取 Supervisor 当前进程内存，不读取旧数据库记录，也不修改不可访问的 desired_state。
      </p>
      <form
        className="recovery-load"
        onSubmit={(event) => {
          event.preventDefault();
          void perform(async () => setTasks(await emergencyApi.list(code)), false);
        }}
      >
        <label>
          当前进程紧急停止码
          <Input.Password
            aria-label="当前进程紧急停止码"
            autoComplete="off"
            onChange={(event) => setCode(event.target.value)}
            required
            value={code}
          />
        </label>
        <button className="secondary-button" disabled={busy} type="submit">
          读取内存运行清单
        </button>
      </form>
      {error !== null && (
        <p className="error-banner" role="alert">
          紧急操作失败：{error}
        </p>
      )}
      <div className="emergency-list">
        {tasks.map((task) => (
          <article key={`${task.taskId}:${task.runId}`}>
            <div>
              <strong>任务 {task.taskId}</strong>
              <span>
                run {task.runId} · {task.unitCount} 个单元
              </span>
            </div>
            <div className="user-actions">
              <button
                className="secondary-button"
                disabled={busy}
                onClick={() => void perform(() => emergencyApi.stop(code, task.taskId))}
                type="button"
              >
                优雅停止
              </button>
              <button
                className="danger-button"
                disabled={busy}
                onClick={() => {
                  setForceTask(task);
                  setReason('');
                }}
                type="button"
              >
                立即强杀…
              </button>
            </div>
          </article>
        ))}
      </div>
      {forceTask !== null && (
        <div
          className="confirmation-panel"
          role="dialog"
          aria-modal="true"
          aria-labelledby="force-kill-title"
        >
          <h3 id="force-kill-title">二次确认立即强杀</h3>
          <p>将立即终止任务 {forceTask.taskId} 当前 run；不会等待优雅退出。</p>
          <label>
            强杀原因
            <textarea
              onChange={(event) => setReason(event.target.value)}
              required
              rows={3}
              value={reason}
            />
          </label>
          <div className="confirmation-actions">
            <button
              className="danger-button"
              disabled={busy || reason.trim().length === 0}
              onClick={() =>
                void perform(() =>
                  emergencyApi.forceKill(code, forceTask.taskId, reason.trim()),
                ).then(() => setForceTask(null))
              }
              type="button"
            >
              确认立即强杀
            </button>
            <button className="text-button" onClick={() => setForceTask(null)} type="button">
              取消
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
