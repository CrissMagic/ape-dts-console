import type { ReactNode } from 'react';
import type { TaskProgress, TaskSummary } from '@scs/contracts';
import { ArrowRight, Database, GripVertical, ShieldAlert } from 'lucide-react';
import { Card } from '../../components/ui/card';
import { operationName } from '../../lib/operation-labels';
import { TaskProgressSummary } from './progress/TaskProgress';
import { progressStageLabel } from '../../lib/source-progress';
import { mappingTargets, readTaskMappings } from './task-mappings';

export function TaskCard({
  task,
  typeLabel,
  sourceName,
  targetName,
  groupName,
  progress,
  status,
  pending,
  dragDisabled,
  onDetails,
  children,
}: {
  task: TaskSummary;
  typeLabel: string;
  sourceName: string;
  targetName: string;
  groupName?: string | undefined;
  progress?: TaskProgress | undefined;
  status: ReactNode;
  pending: boolean;
  dragDisabled: boolean;
  onDetails: () => void;
  children: ReactNode;
}) {
  const running = task.observedState === 'RUNNING';
  const mappings = readTaskMappings(
    task.applied?.configuration ?? task.draft?.configuration,
  ).filter((row) =>
    task.taskType === 'KAFKA_KAFKA'
      ? row.sourceTopic.trim().length > 0
      : row.sourceSchema.trim().length > 0 && row.sourceTable.trim().length > 0,
  );
  const targetCount = mappingTargets(mappings).filter(
    (target) => target.topic.trim().length > 0,
  ).length;
  return (
    <li
      role="listitem"
      aria-label={task.name}
      draggable={!dragDisabled}
      onDragStart={(event) => {
        if (
          dragDisabled ||
          (event.target as HTMLElement).closest('button, input, a, [role="menuitem"]')
        ) {
          event.preventDefault();
          return;
        }
        event.dataTransfer.setData('application/x-scs-task-id', task.id);
        event.dataTransfer.effectAllowed = 'move';
      }}
    >
      <Card
        className={`task-tile ${['FAILED', 'BLOCKED'].includes(task.observedState) ? 'is-failed' : ''}`}
      >
        <div className="task-tile-heading">
          <span className="task-tile-grip" title="拖拽到任务组" aria-label={`拖拽 ${task.name}`}>
            <GripVertical className="size-4" aria-hidden="true" />
          </span>
          <button
            type="button"
            className="task-tile-name"
            aria-label={`${task.name} 详情`}
            title={task.name}
            disabled={pending}
            onClick={onDetails}
          >
            {task.name}
          </button>
          {status}
        </div>
        <div className="task-tile-kind">
          {typeLabel}
          {running && progress?.status === 'AVAILABLE' && (
            <>
              <span className="mx-1">·</span>
              {progressStageLabel(progress)}
            </>
          )}
        </div>
        <div className="task-tile-chain">
          <Database size={14} aria-hidden="true" />
          <span title={sourceName}>{sourceName}</span>
          <ArrowRight size={14} aria-label="同步至" />
          <span title={targetName}>{targetName}</span>
        </div>
        <div className="task-tile-content">
          {running ? (
            <TaskProgressSummary running progress={progress} variant="card" />
          ) : (
            <div className="task-tile-inactive">
              {['FAILED', 'BLOCKED'].includes(task.observedState) && (
                <button
                  type="button"
                  className="task-tile-failure"
                  disabled={pending}
                  aria-label={`${task.name} 查看原因`}
                  onClick={onDetails}
                >
                  <ShieldAlert size={15} aria-hidden="true" />
                  查看失败原因
                  <ArrowRight size={14} aria-hidden="true" />
                </button>
              )}
              {task.observedState === 'BLOCKED' && (
                <p className="text-destructive">{operationName(task.recoveryDisposition)}</p>
              )}
              {mappings.length > 0 && (
                <span>
                  {mappings.length} 个来源 <ArrowRight size={12} aria-hidden="true" /> {targetCount}{' '}
                  个目标 Topic
                </span>
              )}
            </div>
          )}
        </div>
        <div className="task-tile-footer">
          <span className="task-tile-group">{groupName}</span>
          {children}
        </div>
      </Card>
    </li>
  );
}
