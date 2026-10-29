import { useEffect, useRef, useState } from 'react';
import { useOperationNotification } from '../../components/useOperationNotification';
import { Alert, Button, Card, Descriptions, Form, InputNumber, Space, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import { PlatformApiError } from '../../api/platform-api';
import type { TaskPreflightCheck } from '@scs/contracts';
import { useDraftGuard, type LeaveGuard } from '../../components/useDraftGuard';

export interface RuntimeCapacity {
  maxRuntimeUnits: number | null;
  rowVersion: string;
  used: number;
}

export interface RuntimeCapacityApi {
  environment(): Promise<TaskPreflightCheck>;
  get(): Promise<RuntimeCapacity>;
  update(input: { maxRuntimeUnits: number; expectedRowVersion: string }): Promise<RuntimeCapacity>;
}

function errorMessage(error: unknown): string {
  const code = error instanceof Error ? error.message : '';
  if (code === 'CAPACITY_VERSION_CONFLICT') return '配置已被其他管理员修改，请重新加载后再保存。';
  if (code === 'CAPACITY_BELOW_USAGE')
    return '运行容量不能小于当前已占用的运行单元数，请重新加载查看最新占用。';
  return `容量配置操作失败${code ? `（${code}）` : ''}。`;
}

export function RuntimeCapacityCard({
  api,
  onSessionInvalidated,
  onLeaveGuardChange,
}: {
  api: RuntimeCapacityApi;
  onSessionInvalidated?: () => void;
  onLeaveGuardChange?: (guard: LeaveGuard | null) => void;
}) {
  const [snapshot, setSnapshot] = useState<RuntimeCapacity | null>(null);
  const [value, setValue] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const { holder, setError, setNotice } = useOperationNotification();
  const [environment, setEnvironment] = useState<TaskPreflightCheck | null>(null);
  const [environmentError, setEnvironmentError] = useState<string | null>(null);
  const environmentGeneration = useRef(0);
  const dirty = snapshot !== null && value !== snapshot.maxRuntimeUnits;
  const { requestLeave, prompt } = useDraftGuard(dirty, busy && dirty, {
    title: '放弃未保存的容量配置？',
    description: '离开后，尚未保存的运行容量修改将被清除。',
  });
  useEffect(() => {
    onLeaveGuardChange?.(requestLeave);
    return () => onLeaveGuardChange?.(null);
  }, [onLeaveGuardChange, requestLeave]);
  const apply = (result: RuntimeCapacity) => {
    setSnapshot(result);
    setValue(result.maxRuntimeUnits);
  };
  useEffect(() => {
    let active = true;
    setBusy(true);
    const generation = ++environmentGeneration.current;
    void api
      .environment()
      .then((result) => {
        if (active && generation === environmentGeneration.current) setEnvironment(result);
      })
      .catch((reason: unknown) => {
        if (!active || generation !== environmentGeneration.current) return;
        if (reason instanceof PlatformApiError && reason.status === 401) onSessionInvalidated?.();
        setEnvironmentError('运行环境检查未能读取，请重新加载。');
      });
    void api
      .get()
      .then((result) => {
        if (active) apply(result);
      })
      .catch((reason: unknown) => {
        if (active) {
          if (reason instanceof PlatformApiError && reason.status === 401) onSessionInvalidated?.();
          else setError(errorMessage(reason));
        }
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
      environmentGeneration.current += 1;
    };
  }, [api, onSessionInvalidated, setError]);
  const reload = async () => {
    setBusy(true);
    setError(null);
    setEnvironment(null);
    setEnvironmentError(null);
    const generation = ++environmentGeneration.current;
    void api
      .environment()
      .then((result) => {
        if (generation === environmentGeneration.current) setEnvironment(result);
      })
      .catch((reason: unknown) => {
        if (generation !== environmentGeneration.current) return;
        if (reason instanceof PlatformApiError && reason.status === 401) onSessionInvalidated?.();
        setEnvironmentError('运行环境检查未能读取，请重新加载。');
      });
    try {
      apply(await api.get());
    } catch (reason) {
      if (reason instanceof PlatformApiError && reason.status === 401) onSessionInvalidated?.();
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  };
  const save = async () => {
    if (!snapshot || value === null || !Number.isInteger(value) || value < 1) return;
    setBusy(true);
    setError(null);
    try {
      apply(await api.update({ maxRuntimeUnits: value, expectedRowVersion: snapshot.rowVersion }));
      setNotice('运行容量已保存。');
    } catch (reason) {
      if (reason instanceof PlatformApiError && reason.status === 401) onSessionInvalidated?.();
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card>
      <Space orientation="vertical" style={{ width: '100%' }} size="middle">
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm text-muted-foreground">任务共享容器 CPU 与内存</span>
          <Tooltip title="上限限制同时预留及运行的单元数，一个任务可能包含多个单元。容器整体 CPU 和内存限额由 Rancher 或 Docker 配置，包含平台自身开销。各任务共享资源，不提供每任务独立硬配额；单元预算不能换算为 CPU 或内存限额，需根据实际负载设置。">
            <InfoCircleOutlined aria-label="运行容量说明" />
          </Tooltip>
        </div>
        <Alert
          showIcon
          type={
            environmentError !== null || environment?.state === 'FAILED'
              ? 'error'
              : environment?.state === 'PASSED'
                ? 'success'
                : 'info'
          }
          title={
            environmentError ??
            (environment === null
              ? '正在检查运行环境…'
              : environment.state === 'PASSED'
                ? '运行环境已就绪'
                : '运行环境未就绪')
          }
          description={
            environment === null || environment.state === 'PASSED' ? undefined : (
              <div>
                <p>{environment.summary}</p>
                {Array.isArray(environment.details.issues) &&
                  environment.details.issues.length > 0 && (
                    <ul>
                      {environment.details.issues.map((issue: unknown, index) => {
                        if (
                          typeof issue !== 'object' ||
                          issue === null ||
                          !('code' in issue) ||
                          !('summary' in issue) ||
                          typeof issue.code !== 'string' ||
                          typeof issue.summary !== 'string'
                        )
                          return null;
                        return (
                          <li key={index}>
                            {issue.summary}（{issue.code}）
                          </li>
                        );
                      })}
                    </ul>
                  )}
              </div>
            )
          }
        />
        {holder}
        {prompt}
        <Descriptions
          size="small"
          column={2}
          items={[
            {
              key: 'limit',
              label: '当前上限',
              children: snapshot ? (snapshot.maxRuntimeUnits ?? '未配置') : '—',
            },
            { key: 'used', label: '已占用单元', children: snapshot?.used ?? '—' },
          ]}
        />
        <Form layout="vertical" onFinish={() => void save()}>
          <Form.Item label="最大运行单元数" required extra="未配置时无法启动同步任务。">
            <InputNumber
              aria-label="最大运行单元数"
              min={1}
              max={2_147_483_647}
              precision={0}
              value={value}
              disabled={busy || !snapshot}
              onChange={(next) => {
                setValue(next);
              }}
              style={{ width: 240 }}
            />
          </Form.Item>
          <Space>
            <Button
              type="primary"
              htmlType="submit"
              loading={busy}
              disabled={
                !snapshot || value === null || value < 1 || value === snapshot.maxRuntimeUnits
              }
            >
              保存容量
            </Button>
            <Button disabled={busy} onClick={() => void reload()}>
              重新加载
            </Button>
          </Space>
        </Form>
      </Space>
    </Card>
  );
}
