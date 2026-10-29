import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import { RuntimePerformanceCollector } from './performance-collector.js';
import type { RuntimePerformanceBinding, RuntimePerformanceStore } from './performance-store.js';
import type { RuntimeSupervisorService } from './runtime-supervisor.service.js';
import { assertMetricsPortOwner } from './performance-port-owner.js';

/** Read candidates from durable state, but only trust processes this controller owns. */
export function createRuntimePerformanceCollector(
  database: PlatformDatabaseService,
  runtime: Pick<RuntimeSupervisorService, 'listActiveRuntimeTasks'>,
  store: RuntimePerformanceStore,
  verifyPort: (binding: RuntimePerformanceBinding) => Promise<void> = assertMetricsPortOwner,
): RuntimePerformanceCollector {
  const owns = (binding: RuntimePerformanceBinding) =>
    runtime
      .listActiveRuntimeTasks()
      .some(
        (task) =>
          task.taskId === binding.taskId &&
          task.runId === binding.runId &&
          task.units.some(
            (unit) =>
              unit.unitId === binding.unitId &&
              unit.processToken === binding.processToken &&
              unit.pid === binding.pid,
          ),
      );
  return new RuntimePerformanceCollector({
    owns,
    verifyPort,
    repository: {
      async list() {
        if (!database.hasPool()) return [];
        const owned = runtime
          .listActiveRuntimeTasks()
          .flatMap((task) => task.units.map((unit) => unit.unitId));
        if (owned.length === 0) return [];
        const rows = await database.requirePool().query<{
          task_id: string;
          run_id: string;
          unit_id: string;
          process_token: string;
          process_pid: number;
          metrics_port: number;
        }>(
          `SELECT task.id AS task_id, run.id AS run_id, unit.id AS unit_id,
                  unit.process_token, unit.process_pid, unit.metrics_port
             FROM ape_dts_platform.tasks task
             JOIN ape_dts_platform.task_runs run ON run.task_id = task.id
             JOIN ape_dts_platform.task_run_units unit ON unit.run_id = run.id AND unit.task_id = task.id
            WHERE unit.id = ANY($1::uuid[]) AND task.deleted_at IS NULL
              AND run.status = 'RUNNING' AND run.record_purged_at IS NULL
              AND unit.status = 'RUNNING' AND unit.process_token IS NOT NULL
              AND unit.process_pid IS NOT NULL AND unit.metrics_port IS NOT NULL
              AND unit.metrics_port_released_at IS NULL`,
          [owned],
        );
        return rows.rows
          .map((row) => ({
            taskId: row.task_id,
            runId: row.run_id,
            unitId: row.unit_id,
            processToken: row.process_token,
            pid: row.process_pid,
            port: row.metrics_port,
          }))
          .filter(owns);
      },
      append: (binding, sample) => store.append(binding, sample),
      recordGap: (binding, gap) => store.recordGap(binding, gap),
      maintain: async (now) => {
        if (database.hasPool()) await store.maintain(now);
      },
    },
  });
}
