import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Card, Checkbox, Form, Input, Space, Tag, Timeline, Typography } from 'antd';
import { operationLabel } from '../../lib/operation-labels';

import {
  PlatformApiError,
  recoveryApi,
  type WebRecoverySession,
  type WebRecoveryStepType,
} from '../../api/platform-api';

const PHASES: readonly { type: WebRecoveryStepType; label: string; retryable: boolean }[] = [
  { type: 'LOAD_METADATA', label: '元数据库', retryable: false },
  { type: 'UNLOCK_KEYS', label: '密钥加载', retryable: false },
  { type: 'ENSURE_ARTIFACTS', label: '制品核验', retryable: true },
  { type: 'VERIFY_CONTINUITY', label: '旧进程与资源连续性', retryable: true },
  { type: 'PLAN_CAPACITY', label: '容量计划', retryable: true },
  { type: 'RESTORE_BATCHES', label: '任务批次恢复', retryable: true },
  { type: 'READY_OBSERVABILITY', label: '可观测能力', retryable: true },
];

export function RecoveryPage({ onReady }: { onReady?: (() => void) | undefined }) {
  const [session, setSession] = useState<WebRecoverySession | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedTaskIds, setSelectedTaskIds] = useState<readonly string[]>([]);
  const [oldRuntimeStopReason, setOldRuntimeStopReason] = useState('');
  const [busy, setBusy] = useState(false);
  const code = (cause: unknown) =>
    cause instanceof PlatformApiError ? cause.code : 'UNKNOWN_ERROR';

  const run = useCallback(
    async (action: () => Promise<WebRecoverySession>) => {
      setBusy(true);
      setError(null);
      try {
        const next = await action();
        setSession(next);
        setSelectedTaskIds((current) =>
          current.length === 0 ? next.candidates.map((candidate) => candidate.taskId) : current,
        );
        if (next.state === 'READY') onReady?.();
      } catch (cause) {
        setError(code(cause));
      } finally {
        setBusy(false);
      }
    },
    [onReady],
  );

  useEffect(() => {
    void run(() => recoveryApi.current());
  }, [run]);

  const oldRuntimeConfirmationRequired = session?.steps.some(
    (step) =>
      step.stepType === 'VERIFY_CONTINUITY' &&
      step.status === 'PAUSED' &&
      step.failureCode === 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED',
  );

  return (
    <Space orientation="vertical" size={16} className="w-full">
      {error !== null && <Alert showIcon type="error" title={`恢复操作失败：${error}`} />}
      <Card loading={busy && session === null}>
        {session !== null && (
          <div className="mb-6 flex flex-wrap items-center gap-2">
            <Typography.Text>会话状态</Typography.Text>
            <Tag
              color={
                session.state === 'READY'
                  ? 'success'
                  : session.state === 'FAILED'
                    ? 'error'
                    : session.state === 'PAUSED'
                      ? 'warning'
                      : 'processing'
              }
            >
              {operationLabel(session.state)}
            </Tag>
            <Typography.Text type="secondary">版本 {session.rowVersion}</Typography.Text>
          </div>
        )}
        <Timeline
          items={PHASES.map((phase) => {
            const step = session?.steps.find((item) => item.stepType === phase.type);
            const color =
              step?.status === 'SUCCEEDED'
                ? 'green'
                : step?.status === 'FAILED'
                  ? 'red'
                  : step?.status === 'PAUSED'
                    ? 'orange'
                    : step?.status === 'RUNNING'
                      ? 'blue'
                      : 'gray';
            return {
              key: phase.type,
              color,
              title: (
                <Space wrap>
                  <Typography.Text strong>{phase.label}</Typography.Text>
                  <Tag color={color}>
                    {step === undefined
                      ? '未读取'
                      : step.status === 'SUCCEEDED' &&
                          Array.isArray(step.progress.excludedTasks) &&
                          step.progress.excludedTasks.length > 0
                        ? '已完成（含待处理任务）'
                        : operationLabel(step.status)}
                  </Tag>
                </Space>
              ),
              content: (
                <Space orientation="vertical" size={8} className="w-full pb-3">
                  <Typography.Text type="secondary" code>
                    {phase.type}
                  </Typography.Text>
                  {step?.failureCode !== null && step?.failureCode !== undefined && (
                    <Alert
                      showIcon
                      type={step.status === 'FAILED' ? 'error' : 'warning'}
                      title={step.failureCode}
                      description={step.errorSummary}
                    />
                  )}
                  {Array.isArray(step?.progress.excludedTasks) &&
                    step.progress.excludedTasks.length > 0 && (
                      <ul className="list-inside list-disc text-sm text-amber-800 dark:text-amber-300">
                        {step.progress.excludedTasks.map((item, index) => {
                          if (item === null || typeof item !== 'object') return null;
                          const failure = item as Record<string, unknown>;
                          return (
                            <li key={index}>
                              任务 {String(failure.taskId)} 已转为人工处理：{String(failure.code)} —{' '}
                              {String(failure.summary)}
                            </li>
                          );
                        })}
                      </ul>
                    )}
                  {phase.retryable &&
                    (step?.status === 'FAILED' ||
                      (step?.status === 'PAUSED' &&
                        step.stepType === 'PLAN_CAPACITY' &&
                        step.failureCode === 'CAPACITY_NOT_CONFIGURED')) && (
                      <Button
                        disabled={busy}
                        onClick={() => void run(() => recoveryApi.retry(session!.id, phase.type))}
                      >
                        从此失败阶段重试
                      </Button>
                    )}
                </Space>
              ),
            };
          })}
        />
      </Card>
      {oldRuntimeConfirmationRequired && session !== null && (
        <Card title="确认旧实例已经停止">
          <Typography.Paragraph type="secondary">
            当前旧实例不是 clean shutdown。请先在部署侧确认旧容器或进程已停止；该声明会以 USER
            动作审计，随后重新执行连续性检查。
          </Typography.Paragraph>
          <Form layout="vertical">
            <Form.Item label="确认原因" htmlFor="old-runtime-stop-reason" required>
              <Input.TextArea
                id="old-runtime-stop-reason"
                onChange={(event) => setOldRuntimeStopReason(event.target.value)}
                required
                rows={3}
                value={oldRuntimeStopReason}
              />
            </Form.Item>
            <Button
              type="primary"
              disabled={busy || oldRuntimeStopReason.trim().length === 0}
              onClick={() =>
                void run(async () => {
                  const confirmed = await recoveryApi.confirmOldRuntimeStopped(
                    session,
                    oldRuntimeStopReason.trim(),
                  );
                  const retried = await recoveryApi.retry(confirmed.id, 'VERIFY_CONTINUITY');
                  setOldRuntimeStopReason('');
                  return retried;
                })
              }
            >
              确认并重新核验
            </Button>
          </Form>
        </Card>
      )}
      {session?.steps.some(
        (step) =>
          step.stepType === 'PLAN_CAPACITY' &&
          step.status === 'PAUSED' &&
          step.failureCode === 'CAPACITY_EXCEEDED',
      ) && (
        <Card title="选择本次恢复任务">
          <Typography.Paragraph type="secondary">
            未选任务会明确标记为 CAPACITY_BLOCKED，不会进入后台队列。
          </Typography.Paragraph>
          <Space orientation="vertical" size={16}>
            <Space orientation="vertical">
              {session.candidates.map((candidate) => (
                <Checkbox
                  key={candidate.taskId}
                  disabled={busy}
                  checked={selectedTaskIds.includes(candidate.taskId)}
                  onChange={(event) =>
                    setSelectedTaskIds((current) =>
                      event.target.checked
                        ? [...current, candidate.taskId]
                        : current.filter((taskId) => taskId !== candidate.taskId),
                    )
                  }
                >
                  {candidate.taskName}（{candidate.units} 单元）
                </Checkbox>
              ))}
            </Space>
            <Button
              type="primary"
              disabled={busy || selectedTaskIds.length === 0}
              onClick={() =>
                void run(() => recoveryApi.selectCapacityCandidates(session, selectedTaskIds))
              }
            >
              确认恢复选中任务
            </Button>
          </Space>
        </Card>
      )}
      {session !== null &&
        session.steps.some(
          (step) => step.stepType === 'RESTORE_BATCHES' && step.status === 'FAILED',
        ) && (
          <Card title="处理失败任务">
            <Button
              type="primary"
              disabled={busy}
              onClick={() => void run(() => recoveryApi.continueAfterFailedTask(session))}
            >
              跳过失败任务并继续
            </Button>
          </Card>
        )}
    </Space>
  );
}
