import type { TaskProgress } from '@scs/contracts';
import { operationName } from './operation-labels';

export function formatWalBytes(value: string | null) {
  if (value === null) return '未知';
  const size = BigInt(value);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let divisor = 1n;
  let index = 0;
  while (size >= divisor * 1024n && index < units.length - 1) {
    divisor *= 1024n;
    index++;
  }
  return `${Number((size * 100n) / divisor) / 100} ${units[index]}`;
}

export function progressStageLabel(progress: TaskProgress) {
  return progress.currentStage === 'MONITORING_READY'
    ? progress.sourceType !== 'KAFKA'
      ? '增量同步'
      : '流式同步'
    : progress.currentStage
      ? operationName(progress.currentStage)
      : '数据同步';
}
