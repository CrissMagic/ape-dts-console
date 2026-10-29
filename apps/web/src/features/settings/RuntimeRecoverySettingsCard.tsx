import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, InputNumber, Space, Spin, Switch } from 'antd';
import {
  updateRuntimeRecoverySettingsSchema,
  type RuntimeRecoverySettings,
  type RuntimeRecoveryPolicy,
} from '@scs/contracts';
import { PlatformApiError, runtimeRecoverySettingsApi } from '../../api/platform-api';
import { useDraftGuard, type LeaveGuard } from '../../components/useDraftGuard';
import { useOperationNotification } from '../../components/useOperationNotification';

export function RuntimeRecoverySettingsCard({
  onSessionInvalidated,
  onLeaveGuardChange,
}: {
  onSessionInvalidated: () => void;
  onLeaveGuardChange: (guard: LeaveGuard | null) => void;
}) {
  const [snapshot, setSnapshot] = useState<RuntimeRecoverySettings | null>(null);
  const [policy, setPolicy] = useState<{
    enabled: boolean;
    maxAttempts: number | null;
    retryDelaySeconds: number | null;
  }>({ enabled: true, maxAttempts: 3, retryDelaySeconds: 30 });
  const [busy, setBusy] = useState(false);
  const mounted = useRef(false);
  const pending = useRef(false);
  const { holder, setError, setNotice } = useOperationNotification();
  const dirty =
    snapshot !== null &&
    (Object.keys(policy) as (keyof RuntimeRecoveryPolicy)[]).some(
      (key) => snapshot[key] !== policy[key],
    );
  const { requestLeave, prompt } = useDraftGuard(dirty, busy && dirty, {
    title: '放弃未保存的恢复设置？',
    description: '离开后，尚未保存的恢复策略将被清除。',
  });
  useEffect(() => {
    onLeaveGuardChange(requestLeave);
    return () => onLeaveGuardChange(null);
  }, [onLeaveGuardChange, requestLeave]);
  const apply = (value: RuntimeRecoverySettings) => {
    setSnapshot(value);
    setPolicy({
      enabled: value.enabled,
      maxAttempts: value.maxAttempts,
      retryDelaySeconds: value.retryDelaySeconds,
    });
  };
  const report = useCallback(
    (error: unknown) => {
      if (error instanceof PlatformApiError && error.status === 401) onSessionInvalidated();
      else
        setError(
          error instanceof PlatformApiError && error.code === 'RUNTIME_RECOVERY_SETTINGS_CONFLICT'
            ? '配置已被其他管理员修改，请重新加载后再保存。'
            : '任务恢复设置操作失败，请稍后重试。',
        );
    },
    [onSessionInvalidated, setError],
  );
  useEffect(() => {
    mounted.current = true;
    let active = true;
    setBusy(true);
    void runtimeRecoverySettingsApi
      .get()
      .then((value) => {
        if (active) apply(value);
      })
      .catch((error: unknown) => {
        if (active) report(error);
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
      mounted.current = false;
    };
  }, [report]);
  const input = updateRuntimeRecoverySettingsSchema.safeParse({
    ...policy,
    expectedRowVersion: snapshot?.rowVersion,
  });
  async function execute(save: boolean) {
    if (pending.current || (save && (!input.success || !dirty))) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    try {
      const value =
        save && input.success
          ? await runtimeRecoverySettingsApi.update(input.data)
          : await runtimeRecoverySettingsApi.get();
      if (mounted.current) {
        apply(value);
        if (save) setNotice('任务恢复设置已保存，后续恢复尝试按新策略执行。');
      }
    } catch (error) {
      if (mounted.current) report(error);
    } finally {
      pending.current = false;
      if (mounted.current) setBusy(false);
    }
  }
  return (
    <div className="max-w-2xl">
      {holder}
      {prompt}
      <Spin spinning={busy}>
        {snapshot && (
          <div className="space-y-6">
            <div className="flex items-center justify-between gap-4 rounded-lg border p-4">
              <label htmlFor="recovery-enabled" className="text-sm font-medium">
                任务异常后自动恢复
              </label>
              <Switch
                id="recovery-enabled"
                checked={policy.enabled}
                disabled={busy}
                onChange={(enabled) => setPolicy((p) => ({ ...p, enabled }))}
              />
            </div>
            <div className="grid gap-6 sm:grid-cols-2">
              <div>
                <label htmlFor="recovery-attempts" className="mb-2 block text-sm font-medium">
                  最大尝试次数
                </label>
                <InputNumber
                  id="recovery-attempts"
                  className="w-full"
                  value={policy.maxAttempts}
                  min={1}
                  max={10}
                  precision={0}
                  disabled={busy || !policy.enabled}
                  onChange={(maxAttempts) => setPolicy((p) => ({ ...p, maxAttempts }))}
                />
              </div>
              <div>
                <label htmlFor="recovery-delay" className="mb-2 block text-sm font-medium">
                  基础重试间隔（秒）
                </label>
                <InputNumber
                  id="recovery-delay"
                  className="w-full"
                  value={policy.retryDelaySeconds}
                  min={5}
                  max={3600}
                  precision={0}
                  disabled={busy || !policy.enabled}
                  onChange={(retryDelaySeconds) => setPolicy((p) => ({ ...p, retryDelaySeconds }))}
                />
              </div>
            </div>
            {policy.enabled && input.success && (
              <p className="text-sm text-muted-foreground" aria-label="重试等待时间">
                重试等待：
                {Array.from({ length: input.data.maxAttempts }, (_, index) =>
                  Math.min(
                    input.data.retryDelaySeconds * 2 ** index,
                    Math.max(input.data.retryDelaySeconds, 900),
                  ),
                ).join(' → ')}
                {' 秒'}
              </p>
            )}
            <p className="text-sm text-muted-foreground">保留位点续传，不重新全量。</p>
            <details className="text-sm">
              <summary className="cursor-pointer text-muted-foreground">恢复策略说明</summary>
              <div className="mt-3 space-y-3 leading-6 text-muted-foreground">
                <p>
                  重试前执行预检；连续健康运行 30
                  分钟后重置计数。超过次数、启动失败或无法确认旧进程停止时转为人工处理，可从任务列表恢复。关闭自动恢复后，异常任务交由人工处理。
                </p>
                <p>
                  异常会写入容器日志；企业微信通知需在“告警通知”中启用。平台进程或宿主机完全停止时，需由
                  Rancher 或外部监控通知。
                </p>
              </div>
            </details>
          </div>
        )}
      </Spin>
      {dirty && !input.success && (
        <p role="alert" className="mt-4 text-sm text-destructive">
          请填写 1–10 次及 5–3600 秒范围内的整数。
        </p>
      )}
      <Space className="mt-6">
        <Button
          type="primary"
          disabled={busy || !dirty || !input.success}
          onClick={() => void execute(true)}
        >
          保存设置
        </Button>
        <Button disabled={busy} onClick={() => requestLeave(() => void execute(false))}>
          重新加载
        </Button>
      </Space>
    </div>
  );
}
