import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { TaskProgress } from '@scs/contracts';
import { TaskProgressSummary } from './TaskProgress';

const progress: TaskProgress = {
  taskId: '11111111-1111-4111-8111-111111111111',
  revisionId: '22222222-2222-4222-8222-222222222222',
  collectedAt: new Date().toISOString(),
  status: 'AVAILABLE',
  sourceType: 'POSTGRES',
  slotStatus: 'INACTIVE',
  unconfirmedWalBytes: '1024',
  retainedWalBytes: null,
  offsetLag: null,
  timeDelayMilliseconds: null,
  timeDelayStatus: 'UNSUPPORTED',
};
afterEach(cleanup);
describe('task progress presentation', () => {
  it('keeps unknown PG timing distinct from zero in compact cards and shows actual slot status', () => {
    const view = render(
      <TaskProgressSummary
        running
        variant="card"
        progress={{ ...progress, replicationLagMilliseconds: null }}
      />,
    );
    expect(screen.getByLabelText('PG 复制延迟')).toHaveTextContent('暂无时间统计');
    expect(screen.getByLabelText('未确认 WAL')).toHaveTextContent('1 KiB');
    expect(screen.getByLabelText('复制槽状态')).toHaveTextContent('未连接');
    view.rerender(
      <TaskProgressSummary
        running
        variant="card"
        progress={{ ...progress, slotStatus: 'ACTIVE', replicationLagMilliseconds: 0 }}
      />,
    );
    expect(screen.getByLabelText('PG 复制延迟')).toHaveTextContent('0 秒');
    expect(screen.getByLabelText('复制槽状态')).toHaveTextContent('活跃');
  });
  it('shows the committed MySQL Binlog position without inventing delay or Kafka offsets', () => {
    render(
      <TaskProgressSummary
        running
        progress={{
          ...progress,
          sourceType: 'MYSQL',
          currentStage: 'MONITORING_READY',
          mysqlBinlogFilename: 'mysql-bin.000003',
          mysqlBinlogPosition: '782',
          mysqlCheckpointAt: '2026-10-16T08:01:02.000Z',
        }}
      />,
    );
    expect(screen.getByText('增量同步')).toBeInTheDocument();
    expect(screen.getByText('已确认 Binlog：mysql-bin.000003:782')).toBeInTheDocument();
    expect(screen.getByText('检查点记录时间：2026年10月16日 16:01:02')).toBeInTheDocument();
    expect(screen.queryByText('同步延迟：暂不支持')).not.toBeInTheDocument();
    expect(screen.queryByText(/Offset 积压|WAL|复制槽/u)).not.toBeInTheDocument();
  });

  it('does not invent an initial MySQL checkpoint when none has been committed', () => {
    render(
      <TaskProgressSummary
        running
        progress={{
          ...progress,
          sourceType: 'MYSQL',
          currentStage: 'SNAPSHOT',
          mysqlBinlogFilename: null,
          mysqlBinlogPosition: null,
          mysqlCheckpointAt: null,
        }}
      />,
    );
    expect(screen.getByText('已确认 Binlog：—')).toBeInTheDocument();
    expect(screen.queryByText(/检查点记录时间/u)).not.toBeInTheDocument();
  });
  it('formats the PostgreSQL acknowledgement lag while keeping end-to-end delay separate', () => {
    render(
      <TaskProgressSummary
        running
        progress={{ ...progress, slotStatus: 'ACTIVE', replicationLagMilliseconds: 3678000 }}
      />,
    );
    expect(screen.getByLabelText('PG 复制延迟')).toHaveTextContent('1 小时 1 分 18 秒');
    expect(screen.queryByText(/端到端延迟/u)).not.toBeInTheDocument();
  });
  it('uses an ongoing stage band rather than a fabricated completion percentage', () => {
    render(
      <TaskProgressSummary
        running
        progress={{ ...progress, slotStatus: 'ACTIVE', currentStage: 'CDC' }}
      />,
    );
    expect(screen.queryByText('运行中')).not.toBeInTheDocument();
    expect(screen.getByText('增量同步')).toBeInTheDocument();
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
    expect(screen.queryByText('100%')).not.toBeInTheDocument();
  });
  it('does not present unavailable bindings or unknown slots as missing slots', () => {
    const { rerender } = render(
      <TaskProgressSummary running progress={{ ...progress, status: 'UNAVAILABLE' }} />,
    );
    expect(screen.getByText('指标不可用')).toBeInTheDocument();
    expect(screen.queryByText(/KiB/u)).not.toBeInTheDocument();
    rerender(<TaskProgressSummary running progress={{ ...progress, slotStatus: null }} />);
    expect(screen.getByLabelText('复制槽状态')).toHaveTextContent('未知');
    expect(screen.getByLabelText('复制槽状态')).not.toHaveTextContent('不存在');
  });
  it('separates disconnected slot, WAL bytes and unsupported time delay', () => {
    render(<TaskProgressSummary running progress={progress} />);
    expect(screen.getByLabelText('复制槽状态')).toHaveTextContent('未连接');
    expect(screen.getByLabelText('未确认 WAL')).toHaveTextContent('1 KiB');
    expect(screen.getByLabelText('保留 WAL')).toHaveTextContent('未知');
    expect(screen.queryByText(/端到端延迟/u)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '未确认 WAL 说明' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '保留 WAL 说明' })).toBeInTheDocument();
    expect(screen.getByLabelText('PG 复制延迟')).toHaveTextContent('暂无时间统计');
  });
  it('does not display old values for stopped tasks or failed collection', () => {
    const { rerender } = render(<TaskProgressSummary running={false} progress={progress} />);
    expect(screen.getByLabelText('任务未运行，无消费进度')).toHaveTextContent('—');
    rerender(<TaskProgressSummary running progress={{ ...progress, status: 'READ_FAILED' }} />);
    expect(screen.getByText('读取失败')).toBeInTheDocument();
    expect(screen.queryByText(/KiB/u)).not.toBeInTheDocument();
  });
  it('shows unavailable offsets instead of fabricated zero', () => {
    render(<TaskProgressSummary running progress={{ ...progress, sourceType: 'KAFKA' }} />);
    expect(screen.getByText('Offset 积压：未知（无有效提交位点）')).toBeInTheDocument();
  });
  it('keeps the last collected progress without a collection timestamp until refreshed', () => {
    render(
      <TaskProgressSummary
        running
        progress={{ ...progress, collectedAt: '2020-01-01T00:00:00.000Z' }}
      />,
    );
    expect(screen.queryByText('数据已过期')).not.toBeInTheDocument();
    expect(screen.getByLabelText('未确认 WAL')).toHaveTextContent('1 KiB');
    expect(screen.queryByText(/采集于/u)).not.toBeInTheDocument();
  });
});
