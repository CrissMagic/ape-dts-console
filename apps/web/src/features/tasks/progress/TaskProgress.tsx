import { Tag } from 'antd';
import type { TaskProgress } from '@scs/contracts';
import { formatDateTime } from '../../../lib/date-time';
import { InfoTip } from '../../../components/InfoTip';
import { formatWalBytes, progressStageLabel } from '../../../lib/source-progress';

function replicationDelay(milliseconds: number | null | undefined) {
  if (milliseconds == null) return '暂无时间统计';
  if (milliseconds < 1000) return `${Number((milliseconds / 1000).toFixed(3))} 秒`;
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds} 秒`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes >= 60 ? `${Math.floor(minutes / 60)} 小时 ` : ''}${minutes % 60} 分 ${seconds % 60} 秒`;
}

export function TaskProgressSummary({
  running,
  progress,
  variant = 'detail',
}: {
  running: boolean;
  progress?: TaskProgress | undefined;
  variant?: 'detail' | 'card';
}) {
  const compact = variant === 'card';
  if (!running) return <span aria-label="任务未运行，无消费进度">—</span>;
  if (!progress) return <span className="text-muted-foreground">尚未采集</span>;
  if (progress.status !== 'AVAILABLE')
    return (
      <Tag color="orange">{progress.status === 'UNAVAILABLE' ? '指标不可用' : '读取失败'}</Tag>
    );
  return (
    <div className={`min-w-0 text-left text-xs ${compact ? 'task-progress-card' : 'space-y-2.5'}`}>
      {!compact && (
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-medium">{progressStageLabel(progress)}</span>
        </div>
      )}
      {progress.sourceType === 'POSTGRES' ? (
        <div
          className={
            compact
              ? 'task-progress-pg'
              : 'rounded-lg border bg-card p-3 tabular-nums [&_.ant-btn]:size-4 [&_.ant-btn]:min-w-4'
          }
        >
          <dl className={compact ? '' : 'flex items-start justify-between gap-4'}>
            <div className="task-progress-delay min-w-0">
              <dt className="flex items-center gap-1 text-muted-foreground">
                <span>PG 复制延迟</span>
                <InfoTip
                  label="PG 复制延迟说明"
                  text="按复制槽名称找到 active_pid，再读取 pg_stat_replication.replay_lag。PostgreSQL 可能在空闲时将时间统计置空，或因未连接、统计权限不足而无法提供。暂无时间统计不表示零延迟；仍可查看下方直接按复制槽查询的 WAL 字节差。此指标不是写入 Kafka 的端到端延迟。"
                />
              </dt>
              <dd
                aria-label="PG 复制延迟"
                className={`mt-1.5 font-semibold leading-6 ${progress.replicationLagMilliseconds == null ? 'text-sm text-muted-foreground' : 'text-lg'}`}
              >
                {replicationDelay(progress.replicationLagMilliseconds)}
              </dd>
            </div>
            <div className={`task-progress-slot ${compact ? '' : 'shrink-0 text-right'}`}>
              <dt className="text-muted-foreground">复制槽状态</dt>
              <dd className="mt-1.5 [&_.ant-tag]:m-0">
                <Tag
                  color={
                    progress.slotStatus === 'ACTIVE'
                      ? 'green'
                      : progress.slotStatus === null
                        ? 'orange'
                        : 'red'
                  }
                  aria-label="复制槽状态"
                >
                  {progress.slotStatus === 'ACTIVE'
                    ? '活跃'
                    : progress.slotStatus === 'INACTIVE'
                      ? '未连接'
                      : progress.slotStatus === 'MISSING'
                        ? '不存在'
                        : '未知'}
                </Tag>
              </dd>
            </div>
          </dl>
          <dl className={compact ? '' : 'mt-2.5 grid grid-cols-2 divide-x border-t pt-2.5'}>
            <div className={`task-progress-unconfirmed ${compact ? '' : 'pr-3'}`}>
              <dt className="flex items-center gap-1 text-muted-foreground">
                <span>未确认 WAL</span>
                <InfoTip
                  label="未确认 WAL 说明"
                  text="当前 WAL 位置与复制槽 confirmed_flush_lsn 的字节差，表示尚未确认的实例级日志位置差；可能包含其他数据库的日志，不等于本任务待同步记录数，也不能换算为秒数。未知不等于零。"
                />
              </dt>
              <dd aria-label="未确认 WAL" className="mt-1 font-medium">
                {formatWalBytes(progress.unconfirmedWalBytes)}
              </dd>
            </div>
            <div className={`task-progress-retained ${compact ? '' : 'pl-3'}`}>
              <dt className="flex items-center gap-1 text-muted-foreground">
                <span>保留 WAL</span>
                <InfoTip
                  label="保留 WAL 说明"
                  text="当前 WAL 位置与复制槽 restart_lsn 的字节差，表示此复制槽要求源库保留的日志范围。即使部分日志已确认，也可能仍需保留；积压过大会占用源库磁盘，不是精确的磁盘占用值。"
                />
              </dt>
              <dd aria-label="保留 WAL" className="mt-1 font-medium">
                {formatWalBytes(progress.retainedWalBytes)}
              </dd>
            </div>
          </dl>
        </div>
      ) : progress.sourceType === 'MYSQL' ? (
        <div
          className={
            compact
              ? 'task-progress-mysql space-y-2'
              : 'space-y-2 rounded-md border bg-muted/30 p-2.5'
          }
        >
          <div className="break-all font-medium tabular-nums">
            已确认 Binlog：
            {progress.mysqlBinlogFilename && progress.mysqlBinlogPosition
              ? `${progress.mysqlBinlogFilename}:${progress.mysqlBinlogPosition}`
              : '—'}
          </div>
          {progress.mysqlCheckpointAt && (
            <div className="text-muted-foreground">
              检查点记录时间：{formatDateTime(progress.mysqlCheckpointAt)}
            </div>
          )}
        </div>
      ) : (
        <div
          className={
            compact
              ? 'task-progress-kafka'
              : 'flex flex-wrap items-center gap-x-4 gap-y-1 rounded-md border bg-muted/30 p-2.5'
          }
        >
          <div className="font-medium tabular-nums">
            Offset 积压：
            {progress.offsetLag === null
              ? '未知（无有效提交位点）'
              : BigInt(progress.offsetLag).toLocaleString('zh-CN')}
          </div>
        </div>
      )}
    </div>
  );
}
