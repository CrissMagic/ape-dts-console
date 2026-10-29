import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, InputNumber, Space, Spin } from 'antd';
import { type SessionSettings, updateSessionSettingsRequestSchema } from '@scs/contracts';
import { PlatformApiError, sessionSettingsApi } from '../../api/platform-api';
import { InfoTip } from '../../components/InfoTip';
import { useDraftGuard, type LeaveGuard } from '../../components/useDraftGuard';
import { useOperationNotification } from '../../components/useOperationNotification';

export function SessionSettingsCard({
  onSessionInvalidated,
  onLeaveGuardChange,
}: {
  onSessionInvalidated: () => void;
  onLeaveGuardChange: (guard: LeaveGuard | null) => void;
}) {
  const [snapshot, setSnapshot] = useState<SessionSettings | null>(null);
  const [idleMinutes, setIdleMinutes] = useState<number | null>(null);
  const [absoluteHours, setAbsoluteHours] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const mounted = useRef(false);
  const pending = useRef(false);
  const { holder, setError, setNotice } = useOperationNotification();
  const dirty =
    snapshot !== null &&
    (idleMinutes !== snapshot.idleTimeoutMinutes ||
      absoluteHours !== snapshot.absoluteTimeoutHours);
  const { requestLeave, prompt } = useDraftGuard(dirty, busy && dirty, {
    title: '放弃未保存的会话配置？',
    description: '离开后，尚未保存的登录与会话设置将被清除。',
  });
  useEffect(() => {
    onLeaveGuardChange(requestLeave);
    return () => onLeaveGuardChange(null);
  }, [onLeaveGuardChange, requestLeave]);

  const apply = (value: SessionSettings) => {
    setSnapshot(value);
    setIdleMinutes(value.idleTimeoutMinutes);
    setAbsoluteHours(value.absoluteTimeoutHours);
  };
  const reportError = useCallback(
    (error: unknown) => {
      if (error instanceof PlatformApiError && error.status === 401) onSessionInvalidated();
      else
        setError(
          error instanceof PlatformApiError && error.code === 'SESSION_SETTINGS_VERSION_CONFLICT'
            ? '配置已被其他管理员修改，请重新加载后再保存。'
            : '登录与会话配置操作失败，请稍后重试。',
        );
    },
    [onSessionInvalidated, setError],
  );
  useEffect(() => {
    mounted.current = true;
    let active = true;
    setBusy(true);
    void sessionSettingsApi
      .get()
      .then((value) => {
        if (active) apply(value);
      })
      .catch((error: unknown) => {
        if (active) reportError(error);
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
      mounted.current = false;
    };
  }, [reportError]);

  async function reload() {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    try {
      const value = await sessionSettingsApi.get();
      if (mounted.current) apply(value);
    } catch (error) {
      if (mounted.current) reportError(error);
    } finally {
      pending.current = false;
      if (mounted.current) setBusy(false);
    }
  }
  const input = updateSessionSettingsRequestSchema.safeParse({
    idleTimeoutMinutes: idleMinutes,
    absoluteTimeoutHours: absoluteHours,
    expectedRowVersion: snapshot?.rowVersion,
  });
  async function save() {
    if (!input.success || !dirty || pending.current) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    try {
      const updated = await sessionSettingsApi.update(input.data);
      if (mounted.current) {
        apply(updated);
        setNotice('登录与会话设置已保存，将于下一次登录生效。');
      }
    } catch (error) {
      if (mounted.current) reportError(error);
    } finally {
      pending.current = false;
      if (mounted.current) setBusy(false);
    }
  }
  return (
    <div className="max-w-2xl">
      {holder}
      {prompt}
      <div className="mb-6 flex items-center gap-2 text-sm text-muted-foreground">
        <span>修改仅对之后建立的登录会话生效。</span>
        <InfoTip
          label="会话策略说明"
          text="空闲超时范围为 15 分钟至 7 天，按有效的认证接口请求续期，自动刷新也会续期；仅填写表单不算接口活动。最长登录时长范围为 1 小时至 30 天，不会续期。已有会话保持登录时的策略，修改密码、账号停用或撤销会话仍立即生效。"
        />
      </div>
      <Spin spinning={busy}>
        {snapshot !== null && (
          <div className="grid gap-6 sm:grid-cols-2">
            <div>
              <label htmlFor="session-idle-timeout" className="mb-2 block text-sm font-medium">
                空闲超时（分钟）
              </label>
              <InputNumber
                id="session-idle-timeout"
                min={15}
                max={10080}
                step={15}
                precision={0}
                value={idleMinutes}
                onChange={setIdleMinutes}
                disabled={busy}
                className="w-full"
              />
            </div>
            <div>
              <label htmlFor="session-absolute-timeout" className="mb-2 block text-sm font-medium">
                最长登录时长（小时）
              </label>
              <InputNumber
                id="session-absolute-timeout"
                min={1}
                max={720}
                precision={0}
                value={absoluteHours}
                onChange={setAbsoluteHours}
                disabled={busy}
                className="w-full"
              />
            </div>
          </div>
        )}
      </Spin>
      {dirty && !input.success && (
        <p role="alert" className="mt-4 text-sm text-destructive">
          请填写有效时长，且空闲超时不能超过最长登录时长。
        </p>
      )}
      <Space className="mt-6">
        <Button
          type="primary"
          disabled={busy || !dirty || !input.success}
          loading={busy && dirty}
          onClick={() => void save()}
        >
          保存设置
        </Button>
        <Button disabled={busy} onClick={() => requestLeave(() => void reload())}>
          重新加载
        </Button>
      </Space>
    </div>
  );
}
