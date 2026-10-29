import { createHash } from 'node:crypto';
import {
  BadRequestException,
  ConflictException,
  Injectable,
  ServiceUnavailableException,
} from '@nestjs/common';
import {
  CONFIG_BUNDLE_MAX_BYTES,
  configBundleSchema,
  configSystemSchema,
  type ConfigBundle,
  type ConfigExportOptions,
  type UserSummary,
} from '@scs/contracts';
import { platformTaskConfigSchema } from '@scs/config-model';
import type { PoolClient } from 'pg';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { DataSourcesService } from '../data-sources/data-sources.service.js';
import { TasksService, TasksServiceError } from '../tasks/tasks.service.js';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';

const normalize = (value: string) => value.normalize('NFKC').trim().toLowerCase();
type NamedRow = { id: string; name: string; rowVersion?: string };
type ArtifactRow = { id: string; version: string; sha256: string };

@Injectable()
export class ConfigTransferService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly sources: DataSourcesService,
    private readonly tasks: TasksService,
    private readonly audit: AuditLedgerService,
  ) {}

  private validate(bundle: ConfigBundle) {
    if (Buffer.byteLength(JSON.stringify(bundle, null, 2)) > CONFIG_BUNDLE_MAX_BYTES)
      throw new BadRequestException({ code: 'CONFIG_BUNDLE_TOO_LARGE' });
    const sourceIds = new Set(bundle.dataSources.map((item) => item.id));
    const groupIds = new Set(bundle.groups.map((item) => item.id));
    for (const items of [bundle.dataSources, bundle.groups, bundle.tasks]) {
      if (new Set(items.map((item) => normalize(item.name))).size !== items.length)
        throw new BadRequestException({ code: 'CONFIG_BUNDLE_DUPLICATE_NAME' });
    }
    if (sourceIds.size !== bundle.dataSources.length || groupIds.size !== bundle.groups.length)
      throw new BadRequestException({ code: 'CONFIG_BUNDLE_DUPLICATE_ID' });
    for (const task of bundle.tasks) {
      const parsed = platformTaskConfigSchema.safeParse(task.configuration);
      const expectedKind = {
        PG_KAFKA: 'PG_TO_KAFKA',
        MYSQL_KAFKA: 'MYSQL_TO_KAFKA',
        KAFKA_KAFKA: 'KAFKA_RAW_FORWARD',
      }[task.taskType];
      if (
        !parsed.success ||
        parsed.data.kind !== expectedKind ||
        parsed.data.source.dataSourceId !== task.sourceId ||
        parsed.data.target.dataSourceId !== task.targetId
      )
        throw new BadRequestException({ code: 'CONFIG_BUNDLE_TASK_INVALID' });
      if (
        !sourceIds.has(task.sourceId) ||
        !sourceIds.has(task.targetId) ||
        (task.groupId !== null && !groupIds.has(task.groupId))
      )
        throw new BadRequestException({ code: 'CONFIG_BUNDLE_DEPENDENCY_MISSING' });
      const sourceType = bundle.dataSources.find((item) => item.id === task.sourceId)!.configuration
        .type;
      const targetType = bundle.dataSources.find((item) => item.id === task.targetId)!.configuration
        .type;
      const expectedSource = { PG_KAFKA: 'POSTGRES', MYSQL_KAFKA: 'MYSQL', KAFKA_KAFKA: 'KAFKA' }[
        task.taskType
      ];
      if (sourceType !== expectedSource || targetType !== 'KAFKA')
        throw new BadRequestException({ code: 'CONFIG_BUNDLE_TASK_INVALID' });
    }
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>, readOnly = false) {
    const client = await this.database.requirePool().connect();
    try {
      await client.query(readOnly ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY' : 'BEGIN');
      const value = await work(client);
      await client.query('COMMIT');
      return value;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (
        error instanceof TasksServiceError ||
        (error instanceof Error && 'code' in error && error.code === '23505')
      ) {
        throw new ConflictException({ code: 'CONFIG_IMPORT_RECHECK_REQUIRED' });
      }
      throw error;
    } finally {
      client.release();
    }
  }

  private async auditEvent(
    client: PoolClient,
    actor: UserSummary,
    action: string,
    bundle: ConfigBundle,
  ) {
    await this.audit.append(client, {
      action,
      actorType: 'USER',
      actorUserId: actor.id,
      subjectType: 'CONFIGURATION_TRANSFER',
      subjectId: null,
      details: {
        dataSourceCount: bundle.dataSources.length,
        taskCount: bundle.tasks.length,
        includesSystem: bundle.system !== null,
      },
    });
  }

  async export(actor: UserSummary, options: ConfigExportOptions) {
    // Whitelisted columns only: never read secrets, users, sessions, checkpoints or runtime resources.
    const bundle = await this.transaction(async (client) => {
      const tasks = options.tasks
        ? (
            await client.query<ConfigBundle['tasks'][number]>(
              `SELECT t.display_name AS name, t.task_type AS "taskType", t.source_data_source_id AS "sourceId",
          t.target_data_source_id AS "targetId", g.group_id AS "groupId", r.configuration,
          CASE WHEN a.id IS NULL THEN NULL ELSE json_build_object('version',a.version_identifier,'sha256',encode(a.actual_sha256,'hex')) END AS artifact
         FROM ape_dts_platform.tasks t
         JOIN ape_dts_platform.task_revisions r ON r.id=COALESCE(t.current_draft_revision_id,t.current_applied_revision_id)
         LEFT JOIN ape_dts_platform.task_group_memberships g ON g.task_id=t.id
         LEFT JOIN ape_dts_platform.artifacts a ON a.id=r.artifact_id
         WHERE t.deleted_at IS NULL ORDER BY t.normalized_name,t.id`,
            )
          ).rows
        : [];
      const dependencies = [...new Set(tasks.flatMap((task) => [task.sourceId, task.targetId]))];
      const dataSources =
        options.dataSources || dependencies.length
          ? (
              await client.query<ConfigBundle['dataSources'][number]>(
                `SELECT id, normalized_name AS name, configuration FROM ape_dts_platform.data_sources
         WHERE deleted_at IS NULL AND state='ACTIVE' AND ($1::boolean OR id=ANY($2::uuid[])) ORDER BY normalized_name,id`,
                [options.dataSources, dependencies],
              )
            ).rows
          : [];
      const groupIds = tasks.flatMap((task) => (task.groupId === null ? [] : [task.groupId]));
      const groups = groupIds.length
        ? (
            await client.query<NamedRow>(
              'SELECT id,display_name AS name FROM ape_dts_platform.task_groups WHERE id=ANY($1::uuid[]) ORDER BY sort_order,normalized_name,id',
              [groupIds],
            )
          ).rows
        : [];
      let system: ConfigBundle['system'] = null;
      if (options.system) {
        const settings = await this.system(client);
        system = configSystemSchema.parse({
          maxRuntimeUnits: settings.maxRuntimeUnits,
          sessions: settings.sessions,
          alerts: settings.alerts,
          recovery: settings.recovery,
        });
      }
      const parsed = configBundleSchema.safeParse({
        format: 'scs-platform-config',
        version: 1,
        exportedAt: new Date().toISOString(),
        dataSources,
        groups,
        tasks,
        system,
      });
      if (!parsed.success)
        throw new BadRequestException({ code: 'CONFIG_EXPORT_INVALID_CONFIGURATION' });
      this.validate(parsed.data);
      return parsed.data;
    }, true);
    // Audit is a separate short write transaction, after a consistent read-only snapshot.
    await this.transaction((client) =>
      this.auditEvent(client, actor, 'CONFIGURATION_EXPORTED', bundle),
    );
    return bundle;
  }

  private async system(client: PoolClient, lock = false) {
    const suffix = lock ? ' FOR UPDATE' : '';
    const capacity = (
      await client.query<{ max_runtime_units: number | null; row_version: string }>(
        `SELECT max_runtime_units,row_version::text FROM ape_dts_platform.platform_settings WHERE singleton_id=1${suffix}`,
      )
    ).rows[0]!;
    const sessions = (
      await client.query<{
        idle_timeout_minutes: number;
        absolute_timeout_hours: number;
        row_version: string;
      }>(
        `SELECT idle_timeout_minutes,absolute_timeout_hours,row_version::text FROM ape_dts_platform.auth_session_settings WHERE singleton_id=1${suffix}`,
      )
    ).rows[0]!;
    const alerts = (
      await client.query<{
        configuration: NonNullable<ConfigBundle['system']>['alerts'];
        row_version: string;
      }>(
        `SELECT configuration,row_version::text FROM ape_dts_platform.alert_settings WHERE singleton_id=1${suffix}`,
      )
    ).rows[0]!;
    const recovery = (
      await client.query<{
        enabled: boolean;
        maxAttempts: number;
        retryDelaySeconds: number;
        rowVersion: string;
      }>(
        `SELECT enabled,max_attempts AS "maxAttempts",retry_delay_seconds AS "retryDelaySeconds",row_version::text AS "rowVersion" FROM ape_dts_platform.runtime_recovery_settings WHERE singleton_id=1${suffix}`,
      )
    ).rows[0];
    if (!capacity || !sessions || !alerts || !recovery)
      throw new ServiceUnavailableException({ code: 'CONFIG_SYSTEM_SETTINGS_UNAVAILABLE' });
    return {
      maxRuntimeUnits: capacity.max_runtime_units,
      sessions: {
        idleTimeoutMinutes: sessions.idle_timeout_minutes,
        absoluteTimeoutHours: sessions.absolute_timeout_hours,
      },
      alerts: alerts.configuration,
      recovery: {
        enabled: recovery.enabled,
        maxAttempts: recovery.maxAttempts,
        retryDelaySeconds: recovery.retryDelaySeconds,
      },
      versions: [
        capacity.row_version,
        sessions.row_version,
        alerts.row_version,
        recovery.rowVersion,
      ],
    };
  }

  private async inspect(
    client: PoolClient,
    actor: UserSummary,
    bundle: ConfigBundle,
    lock = false,
  ) {
    this.validate(bundle);
    const sources = (
      await client.query<NamedRow>(
        "SELECT id,normalized_name AS name FROM ape_dts_platform.data_sources WHERE state='ACTIVE' AND deleted_at IS NULL ORDER BY id",
      )
    ).rows;
    const tasks = (
      await client.query<NamedRow>(
        'SELECT id,display_name AS name FROM ape_dts_platform.tasks WHERE deleted_at IS NULL ORDER BY id',
      )
    ).rows;
    const groups = (
      await client.query<NamedRow>(
        'SELECT id,display_name AS name FROM ape_dts_platform.task_groups ORDER BY id',
      )
    ).rows;
    const artifacts = (
      await client.query<ArtifactRow>(
        `SELECT id,version_identifier AS version,encode(actual_sha256,'hex') AS sha256 FROM ape_dts_platform.artifacts WHERE deleted_at IS NULL AND state<>'QUARANTINED' ORDER BY id`,
      )
    ).rows;
    const system = bundle.system ? await this.system(client, lock) : null;
    const conflicts: string[] = [];
    for (const source of bundle.dataSources)
      if (sources.some((item) => normalize(item.name) === normalize(source.name)))
        conflicts.push(`数据源同名：${source.name}`);
    for (const task of bundle.tasks)
      if (tasks.some((item) => normalize(item.name) === normalize(task.name)))
        conflicts.push(`任务同名：${task.name}`);
    if (bundle.system) {
      const used = (
        await client.query<{ used: string }>(
          "SELECT count(*)::text AS used FROM ape_dts_platform.capacity_reservations WHERE state IN ('RESERVED','ACTIVE')",
        )
      ).rows[0]!.used;
      if (
        bundle.system.maxRuntimeUnits !== null &&
        BigInt(bundle.system.maxRuntimeUnits) < BigInt(used)
      )
        conflicts.push('导入的运行容量小于当前已占用容量。');
    }
    const warnings = [
      '不含密码和机器人 Webhook，导入后须重新配置。',
      '任务以新 ID 和停止状态导入；不继承检查点、运行历史或复制资源绑定。',
    ];
    if (bundle.tasks.length)
      warnings.push(
        '请核对复制槽、Publication 及目标 Topic 配置；配置迁移不等于位点恢复，重新全量可能产生重复数据。',
      );
    if (bundle.system)
      warnings.push(
        '将替换目标平台的运行容量、会话策略及告警规则；告警会关闭，现有会话不延长，现有 Webhook 不覆盖。',
      );
    for (const task of bundle.tasks)
      if (task.artifact && !artifacts.some((item) => item.sha256 === task.artifact!.sha256))
        warnings.push(`任务「${task.name}」的制品未找到，导入后需重新绑定。`);
    const previewHash = createHash('sha256')
      .update(
        JSON.stringify({ actorId: actor.id, bundle, sources, tasks, groups, artifacts, system }),
      )
      .digest('hex');
    return {
      preview: {
        previewHash,
        dataSources: bundle.dataSources.length,
        tasks: bundle.tasks.length,
        groups: bundle.groups.length,
        system: bundle.system !== null,
        conflicts,
        warnings,
      },
      groups,
      artifacts,
    };
  }

  preview(actor: UserSummary, bundle: ConfigBundle) {
    return this.transaction(
      async (client) => (await this.inspect(client, actor, bundle)).preview,
      true,
    );
  }

  import(actor: UserSummary, bundle: ConfigBundle, expectedPreviewHash: string) {
    return this.transaction(async (client) => {
      // Folder mutations precede audit/settings locks, including sources created in this batch.
      await this.tasks.lockGroupMutationsWithin(client);
      const inspected = await this.inspect(client, actor, bundle, true);
      if (
        inspected.preview.conflicts.length ||
        inspected.preview.previewHash !== expectedPreviewHash
      )
        throw new ConflictException({ code: 'CONFIG_IMPORT_RECHECK_REQUIRED' });
      const ids = new Map<string, string>();
      for (const source of bundle.dataSources) {
        const created = await this.sources.importConfigurationWithin(client, actor.id, source);
        ids.set(source.id, created.id);
      }
      const groupIds = new Map<string, string>();
      for (const group of bundle.groups) {
        const existing = inspected.groups.find(
          (item) => normalize(item.name) === normalize(group.name),
        );
        const created =
          existing ?? (await this.tasks.createGroup(actor, { name: group.name }, client));
        groupIds.set(group.id, created.id);
      }
      for (const task of bundle.tasks) {
        const parsed = platformTaskConfigSchema.parse(task.configuration);
        const configuration = {
          ...parsed,
          source: { ...parsed.source, dataSourceId: ids.get(task.sourceId)! },
          target: { ...parsed.target, dataSourceId: ids.get(task.targetId)! },
        };
        const artifact = task.artifact
          ? inspected.artifacts.find((item) => item.sha256 === task.artifact!.sha256)
          : undefined;
        await this.tasks.create(
          actor,
          {
            name: task.name,
            taskType: task.taskType,
            sourceDataSourceId: ids.get(task.sourceId)!,
            targetDataSourceId: ids.get(task.targetId)!,
            groupId: task.groupId === null ? null : groupIds.get(task.groupId)!,
            artifactId: artifact?.id ?? null,
            configuration,
          },
          client,
        );
      }
      if (bundle.system) {
        const settings = bundle.system;
        if (settings.recovery)
          await client.query(
            'UPDATE ape_dts_platform.runtime_recovery_settings SET enabled=$1,max_attempts=$2,retry_delay_seconds=$3,row_version=row_version+1,updated_at=statement_timestamp() WHERE singleton_id=1',
            [
              settings.recovery.enabled,
              settings.recovery.maxAttempts,
              settings.recovery.retryDelaySeconds,
            ],
          );
        await client.query(
          'UPDATE ape_dts_platform.platform_settings SET max_runtime_units=$1,row_version=row_version+1,updated_by=$2::uuid,updated_at=statement_timestamp() WHERE singleton_id=1',
          [settings.maxRuntimeUnits, actor.id],
        );
        await client.query(
          'UPDATE ape_dts_platform.auth_session_settings SET idle_timeout_minutes=$1,absolute_timeout_hours=$2,row_version=row_version+1,updated_by_user_id=$3::uuid,updated_at=statement_timestamp() WHERE singleton_id=1',
          [settings.sessions.idleTimeoutMinutes, settings.sessions.absoluteTimeoutHours, actor.id],
        );
        await client.query(
          'UPDATE ape_dts_platform.alert_settings SET enabled=false,configuration=$1::jsonb,row_version=row_version+1,updated_at=statement_timestamp() WHERE singleton_id=1',
          [JSON.stringify(settings.alerts)],
        );
      }
      await this.auditEvent(client, actor, 'CONFIGURATION_IMPORTED', bundle);
      return { dataSources: bundle.dataSources.length, tasks: bundle.tasks.length };
    });
  }
}
