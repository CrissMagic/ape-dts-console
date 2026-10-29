import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { PlatformApiError, recoveryApi, type WebRecoverySession } from '../../api/platform-api';
import { RecoveryPage } from './RecoveryPage';

const session: WebRecoverySession = {
  candidates: [],
  id: '0199c617-249d-7459-bbcb-f97ad47a3110',
  rowVersion: '2',
  state: 'FAILED',
  steps: [
    {
      attemptCount: 1,
      errorSummary: '制品哈希不一致',
      failureCode: 'ARTIFACT_HASH_MISMATCH',
      position: 3,
      progress: {},
      status: 'FAILED',
      stepType: 'ENSURE_ARTIFACTS',
    },
  ],
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('RecoveryPage', () => {
  it('keeps recovery failures visible after a retry fails', async () => {
    vi.spyOn(recoveryApi, 'current').mockResolvedValue(session);
    vi.spyOn(recoveryApi, 'retry').mockRejectedValue(
      new PlatformApiError('RECOVERY_CONFLICT', 409),
    );
    render(<RecoveryPage />);
    fireEvent.click(await screen.findByRole('button', { name: '从此失败阶段重试' }));
    expect(screen.queryByText('恢复阶段')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '恢复会话说明' })).not.toBeInTheDocument();
    expect(await screen.findByText('恢复操作失败：RECOVERY_CONFLICT')).toBeInTheDocument();
    expect(screen.getByText('ARTIFACT_HASH_MISMATCH')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '从此失败阶段重试' })).toBeEnabled();
  });

  it('selects only checked capacity candidates and prevents an empty selection', async () => {
    const firstTaskId = '11111111-1111-4111-8111-111111111111';
    const secondTaskId = '22222222-2222-4222-8222-222222222222';
    const paused: WebRecoverySession = {
      ...session,
      candidates: [firstTaskId, secondTaskId].map((taskId, index) => ({
        revisionId: '33333333-3333-4333-8333-333333333333',
        taskId,
        taskName: `恢复任务 ${index + 1}`,
        taskType: 'PG_KAFKA',
        unitKeys: [`unit-${index}`],
        units: 1,
      })),
      state: 'PAUSED',
      steps: [
        {
          attemptCount: 1,
          errorSummary: '容量不足',
          failureCode: 'CAPACITY_EXCEEDED',
          position: 5,
          progress: {},
          status: 'PAUSED',
          stepType: 'PLAN_CAPACITY',
        },
      ],
    };
    vi.spyOn(recoveryApi, 'current').mockResolvedValue(paused);
    const select = vi.spyOn(recoveryApi, 'selectCapacityCandidates').mockResolvedValue(paused);
    render(<RecoveryPage />);
    const first = await screen.findByRole('checkbox', { name: '恢复任务 1（1 单元）' });
    const second = screen.getByRole('checkbox', { name: '恢复任务 2（1 单元）' });
    fireEvent.click(first);
    fireEvent.click(second);
    const submit = screen.getByRole('button', { name: '确认恢复选中任务' });
    expect(submit).toBeDisabled();
    fireEvent.click(first);
    fireEvent.click(submit);
    await waitFor(() => expect(select).toHaveBeenCalledExactlyOnceWith(paused, [firstTaskId]));
  });

  it('shows all fixed phases and the real failure code before offering a scoped retry', async () => {
    vi.spyOn(recoveryApi, 'current').mockResolvedValue(session);
    const retry = vi
      .spyOn(recoveryApi, 'retry')
      .mockResolvedValue({ ...session, state: 'RUNNING' });
    render(<RecoveryPage />);
    expect(await screen.findByText(/ARTIFACT_HASH_MISMATCH/)).toBeInTheDocument();
    expect(screen.getByText('READY_OBSERVABILITY')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '从此失败阶段重试' }));
    await waitFor(() => expect(retry).toHaveBeenCalledWith(session.id, 'ENSURE_ARTIFACTS'));
  });

  it('discovers the current instance session and exits restricted recovery at READY', async () => {
    const onReady = vi.fn();
    vi.spyOn(recoveryApi, 'current').mockResolvedValue({ ...session, state: 'READY' });

    render(<RecoveryPage onReady={onReady} />);

    await waitFor(() => expect(recoveryApi.current).toHaveBeenCalledOnce());
    expect(onReady).toHaveBeenCalledOnce();
    expect(screen.queryByLabelText('恢复会话 ID')).not.toBeInTheDocument();
  });

  it('confirms the old runtime with a required reason and retries continuity with the new version', async () => {
    const paused: WebRecoverySession = {
      ...session,
      rowVersion: '3',
      state: 'PAUSED',
      steps: [
        {
          attemptCount: 1,
          errorSummary: '旧平台实例不是 clean shutdown',
          failureCode: 'RECOVERY_OLD_RUNTIME_STOP_CONFIRMATION_REQUIRED',
          position: 4,
          progress: {},
          status: 'PAUSED',
          stepType: 'VERIFY_CONTINUITY',
        },
      ],
    };
    const confirmed = { ...paused, rowVersion: '4' };
    vi.spyOn(recoveryApi, 'current').mockResolvedValue(paused);
    const confirm = vi.spyOn(recoveryApi, 'confirmOldRuntimeStopped').mockResolvedValue(confirmed);
    const retry = vi
      .spyOn(recoveryApi, 'retry')
      .mockResolvedValue({ ...confirmed, state: 'RUNNING' });

    render(<RecoveryPage />);
    const submit = await screen.findByRole('button', { name: '确认并重新核验' });
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByLabelText('确认原因'), {
      target: { value: '部署侧已确认旧容器停止' },
    });
    fireEvent.click(submit);

    await waitFor(() => expect(confirm).toHaveBeenCalledWith(paused, '部署侧已确认旧容器停止'));
    expect(retry).toHaveBeenCalledWith(confirmed.id, 'VERIFY_CONTINUITY');
  });

  it('offers a manual retry when capacity was paused because the limit was not configured', async () => {
    const paused: WebRecoverySession = {
      ...session,
      state: 'PAUSED',
      steps: [
        {
          attemptCount: 1,
          errorSummary: '最大并发运行单元数尚未配置',
          failureCode: 'CAPACITY_NOT_CONFIGURED',
          position: 5,
          progress: { requested: 2, used: 0 },
          status: 'PAUSED',
          stepType: 'PLAN_CAPACITY',
        },
      ],
    };
    vi.spyOn(recoveryApi, 'current').mockResolvedValue(paused);
    const retry = vi.spyOn(recoveryApi, 'retry').mockResolvedValue({ ...paused, state: 'RUNNING' });

    render(<RecoveryPage />);
    fireEvent.click(await screen.findByRole('button', { name: '从此失败阶段重试' }));

    await waitFor(() => expect(retry).toHaveBeenCalledWith(paused.id, 'PLAN_CAPACITY'));
  });
});
