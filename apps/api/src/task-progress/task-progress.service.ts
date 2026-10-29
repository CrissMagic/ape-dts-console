import { Injectable, type OnModuleInit, type OnModuleDestroy } from '@nestjs/common';
import { kafkaTaskMappings, platformTaskConfigSchema } from '@scs/config-model';
import type { TaskProgress, UserSummary } from '@scs/contracts';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import {
  DataSourcesService,
  DataSourcesServiceError,
} from '../data-sources/data-sources.service.js';

interface AppliedTask {
  id: string;
  current_applied_revision_id: string;
  configuration: unknown;
  source_version: string | null;
  run_id: string | null;
  current_stage: string | null;
}

@Injectable()
export class TaskProgressService implements OnModuleInit, OnModuleDestroy {
  private readonly samples = new Map<string, TaskProgress>();
  private readonly sourceVersions = new Map<string, string | null>();
  private readonly runIds = new Map<string, string | null>();
  private timer?: ReturnType<typeof setInterval>;
  private collecting = false;
  private stopped = false;
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly sources: DataSourcesService,
  ) {}

  onModuleInit() {
    this.timer = setInterval(() => void this.collect(), 15_000);
    this.timer.unref();
    void this.collect();
  }
  onModuleDestroy() {
    this.stopped = true;
    clearInterval(this.timer);
  }

  async collect(): Promise<void> {
    if (this.collecting || this.stopped) return;
    this.collecting = true;
    try {
      const tasks = await this.database.requirePool().query<AppliedTask>(
        `SELECT task.id, task.current_applied_revision_id, revision.configuration, running.id AS run_id,
                running.current_stage,
                preflight.result #>> '{dataSourceVersions,source}' AS source_version
           FROM ape_dts_platform.tasks task
           JOIN ape_dts_platform.task_revisions revision
             ON revision.id=task.current_applied_revision_id AND revision.task_id=task.id
           LEFT JOIN LATERAL (
             SELECT id, started_at, current_stage FROM ape_dts_platform.task_runs run
              WHERE run.task_id=task.id AND run.task_revision_id=revision.id
                AND (revision.configuration->>'kind'='PG_TO_KAFKA'
                     OR (run.status='RUNNING' AND run.ended_at IS NULL))
              ORDER BY run.started_at DESC, run.id DESC LIMIT 1
           ) running ON true
           LEFT JOIN LATERAL (
             SELECT result FROM ape_dts_platform.preflight_runs preflight
              WHERE preflight.task_revision_id=revision.id
                AND preflight.started_at<=running.started_at
                AND preflight.completed_at<=running.started_at
              ORDER BY preflight.started_at DESC, preflight.id DESC LIMIT 1
           ) preflight ON true
          WHERE task.deleted_at IS NULL AND (task.observed_state='RUNNING'
                OR revision.configuration->>'kind'='PG_TO_KAFKA')`,
      );
      const ids = new Set(tasks.rows.map((task) => task.id));
      for (const id of this.samples.keys())
        if (!ids.has(id)) {
          this.samples.delete(id);
          this.sourceVersions.delete(id);
          this.runIds.delete(id);
        }
      // Bound source connections; an unavailable source does not prevent the next task's collection.
      for (let start = 0; start < tasks.rows.length; start += 4) {
        await Promise.all(tasks.rows.slice(start, start + 4).map((task) => this.collectTask(task)));
      }
    } catch {
      // Initialization or database loss makes every cached source fact unavailable.
      for (const [id, sample] of this.samples)
        this.samples.set(id, { ...sample, status: 'UNAVAILABLE' });
    } finally {
      this.collecting = false;
    }
  }

  private async collectTask(task: AppliedTask) {
    const parsed = platformTaskConfigSchema.safeParse(task.configuration);
    if (!parsed.success) {
      this.samples.delete(task.id);
      return;
    }
    const config = parsed.data;
    const base: TaskProgress = {
      taskId: task.id,
      revisionId: task.current_applied_revision_id,
      collectedAt: new Date().toISOString(),
      status: 'READ_FAILED',
      sourceType:
        config.kind === 'PG_TO_KAFKA'
          ? 'POSTGRES'
          : config.kind === 'MYSQL_TO_KAFKA'
            ? 'MYSQL'
            : 'KAFKA',
      ...(config.kind === 'MYSQL_TO_KAFKA'
        ? { mysqlBinlogFilename: null, mysqlBinlogPosition: null, mysqlCheckpointAt: null }
        : {}),
      currentStage: task.current_stage ?? null,
      replicationLagMilliseconds: null,
      slotStatus: null,
      unconfirmedWalBytes: null,
      retainedWalBytes: null,
      offsetLag: null,
      timeDelayMilliseconds: null,
      timeDelayStatus: 'UNSUPPORTED',
    };
    if (
      task.run_id === null ||
      task.source_version === null ||
      !/^[1-9]\d*$/u.test(task.source_version)
    ) {
      this.sourceVersions.set(task.id, task.source_version);
      this.runIds.set(task.id, task.run_id);
      this.samples.set(task.id, { ...base, status: 'UNAVAILABLE' });
      return;
    }
    try {
      if (config.kind === 'MYSQL_TO_KAFKA') {
        const result = await this.database.requirePool().query<{
          binlog_filename: string;
          binlog_position: string;
          checkpoint_at: Date | null;
        }>(
          `SELECT checkpoint.binlog_filename, checkpoint.binlog_position::text,checkpoint.checkpoint_at
          FROM ape_dts_platform.mysql_task_checkpoints checkpoint
          JOIN ape_dts_platform.tasks task ON task.id=checkpoint.task_id AND task.current_applied_revision_id=checkpoint.task_revision_id
          JOIN ape_dts_platform.task_runs run ON run.id=checkpoint.active_run_id AND run.task_id=checkpoint.task_id AND run.task_revision_id=checkpoint.task_revision_id
          WHERE checkpoint.task_id=$1::uuid AND checkpoint.task_revision_id=$2::uuid
            AND checkpoint.active_run_id=$3::uuid AND checkpoint.source_row_version=$4::bigint
            AND task.deleted_at IS NULL AND task.observed_state='RUNNING'
            AND run.status='RUNNING' AND run.ended_at IS NULL`,
          [task.id, task.current_applied_revision_id, task.run_id, task.source_version],
        );
        const checkpoint = result.rows[0];
        if (!this.stopped) {
          this.sourceVersions.set(task.id, task.source_version);
          this.runIds.set(task.id, task.run_id);
          this.samples.set(task.id, {
            ...base,
            status: checkpoint === undefined ? 'UNAVAILABLE' : 'AVAILABLE',
            mysqlBinlogFilename: checkpoint?.binlog_filename ?? null,
            mysqlBinlogPosition: checkpoint?.binlog_position ?? null,
            mysqlCheckpointAt: checkpoint?.checkpoint_at?.toISOString() ?? null,
            collectedAt: new Date().toISOString(),
          });
        }
        return;
      }
      let progress: Pick<
        TaskProgress,
        | 'slotStatus'
        | 'unconfirmedWalBytes'
        | 'retainedWalBytes'
        | 'offsetLag'
        | 'replicationLagMilliseconds'
      >;
      if (config.kind === 'PG_TO_KAFKA') {
        progress = await this.sources.readProgressForSystem(
          config.source.dataSourceId,
          { slotName: config.source.slotName },
          task.source_version,
        );
      } else {
        let total: bigint | null = 0n;
        for (const mapping of kafkaTaskMappings(config)) {
          const sample = await this.sources.readProgressForSystem(
            config.source.dataSourceId,
            { topic: mapping.source, groupId: config.source.groupId },
            task.source_version,
          );
          total =
            total !== null && sample.offsetLag !== null && /^\d+$/u.test(sample.offsetLag)
              ? total + BigInt(sample.offsetLag)
              : null;
        }
        progress = {
          slotStatus: null,
          unconfirmedWalBytes: null,
          retainedWalBytes: null,
          replicationLagMilliseconds: null,
          offsetLag: total?.toString() ?? null,
        };
      }
      if (!this.stopped) {
        this.sourceVersions.set(task.id, task.source_version);
        this.runIds.set(task.id, task.run_id);
        this.samples.set(task.id, {
          ...base,
          ...progress,
          status: 'AVAILABLE',
          collectedAt: new Date().toISOString(),
        });
      }
    } catch (error) {
      if (!this.stopped) {
        this.sourceVersions.set(task.id, task.source_version);
        this.runIds.set(task.id, task.run_id);
        this.samples.set(task.id, {
          ...base,
          collectedAt: new Date().toISOString(),
          status:
            error instanceof DataSourcesServiceError &&
            error.code === 'DATA_SOURCE_VERSION_CONFLICT'
              ? 'UNAVAILABLE'
              : 'READ_FAILED',
        });
      }
    }
  }

  async listForMonitoring(): Promise<readonly TaskProgress[]> {
    const tasks = await this.database.requirePool().query<{
      id: string;
      current_applied_revision_id: string;
      source_row_version: string | null;
      run_id: string | null;
    }>(
      `SELECT task.id, task.current_applied_revision_id, source.row_version::text AS source_row_version,
                running.id AS run_id
         FROM ape_dts_platform.tasks task
         JOIN ape_dts_platform.task_revisions revision
           ON revision.id=task.current_applied_revision_id AND revision.task_id=task.id
         LEFT JOIN ape_dts_platform.data_sources source ON source.id=task.source_data_source_id
           AND source.state='ACTIVE' AND source.deleted_at IS NULL
         LEFT JOIN LATERAL (
           SELECT id FROM ape_dts_platform.task_runs run
            WHERE run.task_id=task.id AND run.task_revision_id=task.current_applied_revision_id
              AND (revision.configuration->>'kind'='PG_TO_KAFKA'
                   OR (run.status='RUNNING' AND run.ended_at IS NULL))
            ORDER BY run.started_at DESC, run.id DESC LIMIT 1
         ) running ON true
        WHERE task.deleted_at IS NULL AND (task.observed_state='RUNNING'
              OR revision.configuration->>'kind'='PG_TO_KAFKA')`,
    );
    return tasks.rows.flatMap((task) => {
      const sample = this.samples.get(task.id);
      if (sample?.revisionId !== task.current_applied_revision_id) return [];
      return [
        {
          ...sample,
          status:
            this.sourceVersions.get(task.id) !== task.source_row_version ||
            this.runIds.get(task.id) !== task.run_id ||
            this.runIds.get(task.id) == null ||
            this.sourceVersions.get(task.id) == null ||
            Date.now() - Date.parse(sample.collectedAt) > 60_000
              ? ('UNAVAILABLE' as const)
              : sample.status,
        },
      ];
    });
  }

  async list(actor: UserSummary): Promise<readonly TaskProgress[]> {
    const allowed = await this.database.requirePool().query<{
      id: string;
      current_applied_revision_id: string;
      source_row_version: string | null;
      run_id: string | null;
    }>(
      `SELECT task.id, task.current_applied_revision_id, source.row_version::text AS source_row_version,
                running.id AS run_id
         FROM ape_dts_platform.tasks task
         LEFT JOIN ape_dts_platform.data_sources source ON source.id=task.source_data_source_id
           AND source.state='ACTIVE' AND source.deleted_at IS NULL
         LEFT JOIN LATERAL (
           SELECT id FROM ape_dts_platform.task_runs run
            WHERE run.task_id=task.id AND run.task_revision_id=task.current_applied_revision_id
              AND run.status='RUNNING' AND run.ended_at IS NULL
            ORDER BY run.started_at DESC, run.id DESC LIMIT 1
         ) running ON true
        WHERE task.deleted_at IS NULL AND task.observed_state='RUNNING'
          AND ($1::text='ADMIN' OR NOT EXISTS (
            SELECT required.data_source_id
              FROM (VALUES (task.source_data_source_id), (task.target_data_source_id)) required(data_source_id)
             WHERE NOT EXISTS (
               SELECT 1 FROM ape_dts_platform.data_source_grants grant_row
                WHERE grant_row.user_id=$2::uuid AND grant_row.data_source_id=required.data_source_id
                  AND grant_row.revoked_at IS NULL
             )
          ))`,
      [actor.role, actor.id],
    );
    return allowed.rows.flatMap((task) => {
      const sample = this.samples.get(task.id);
      if (sample?.revisionId !== task.current_applied_revision_id) return [];
      return [
        {
          ...sample,
          status:
            this.sourceVersions.get(task.id) !== task.source_row_version ||
            this.runIds.get(task.id) !== task.run_id ||
            this.runIds.get(task.id) == null ||
            this.sourceVersions.get(task.id) == null
              ? ('UNAVAILABLE' as const)
              : sample.status,
        },
      ];
    });
  }
}
