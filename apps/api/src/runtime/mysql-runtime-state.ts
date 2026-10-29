import { createHash, randomInt } from 'node:crypto';
import { open, rm } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';

import { Injectable, Logger, type OnModuleDestroy } from '@nestjs/common';
import type { MysqlCdcRuntimePosition, MysqlToKafkaTaskConfig } from '@scs/config-model';
import type { PoolClient } from 'pg';

import { DataSourcesService } from '../data-sources/data-sources.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';

export type MysqlRuntimePhase = 'SNAPSHOT' | 'CDC';
export interface MysqlRuntimeBinding extends MysqlCdcRuntimePosition {
  readonly phase: MysqlRuntimePhase;
  readonly sourceIdentity: string;
  readonly sourceRowVersion: string;
}

export type MysqlRuntimeStateErrorCode =
  | 'MYSQL_CHECKPOINT_INVALID'
  | 'MYSQL_BINLOG_FAMILY_CHANGED'
  | 'MYSQL_CDC_NOT_READY'
  | 'MYSQL_CHECKPOINT_MISSING'
  | 'MYSQL_SNAPSHOT_NOT_ENABLED'
  | 'MYSQL_SOURCE_IDENTITY_CHANGED'
  | 'MYSQL_CHECKPOINT_CONFIGURATION_CHANGED'
  | 'MYSQL_REPLICATION_CLIENT_ID_CONFLICT'
  | 'MYSQL_BINLOG_POSITION_UNAVAILABLE'
  | 'MYSQL_CHECKPOINT_BINDING_CHANGED'
  | 'MYSQL_SNAPSHOT_COMPLETION_UNPROVEN'
  | 'MYSQL_CDC_START_FAILED';
export class MysqlRuntimeStateError extends Error {
  constructor(readonly code: MysqlRuntimeStateErrorCode) {
    super(code);
  }
}

function invalid(): never {
  throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_INVALID');
}

function fileSequence(filename: string): { family: string; sequence: bigint } {
  const match = /^([A-Za-z0-9._-]+)\.(\d+)$/u.exec(filename);
  if (match === null) invalid();
  return { family: match[1]!, sequence: BigInt(match[2]!) };
}

export function compareMysqlPositions(
  left: MysqlCdcRuntimePosition,
  right: MysqlCdcRuntimePosition,
): number {
  const a = fileSequence(left.binlogFilename);
  const b = fileSequence(right.binlogFilename);
  if (a.family !== b.family) throw new MysqlRuntimeStateError('MYSQL_BINLOG_FAMILY_CHANGED');
  if (a.sequence !== b.sequence) return a.sequence < b.sequence ? -1 : 1;
  return Math.sign(left.binlogPosition - right.binlogPosition);
}

export function parseMysqlCheckpointLog(
  text: string,
  serverId: number,
): MysqlCdcRuntimePosition | null {
  let latest: MysqlCdcRuntimePosition | null = null;
  // A trailing partial write is not yet a complete checkpoint.
  for (const line of text.slice(0, text.lastIndexOf('\n') + 1).split('\n')) {
    if (!line.startsWith('checkpoint_position | ')) continue;
    let value: unknown;
    try {
      value = JSON.parse(line.slice('checkpoint_position | '.length));
    } catch {
      invalid();
    }
    if (typeof value !== 'object' || value === null) invalid();
    const record = value as Record<string, unknown>;
    if (record.type !== 'MysqlCdc') continue;
    if (
      record.server_id !== String(serverId) ||
      typeof record.binlog_filename !== 'string' ||
      !/^[A-Za-z0-9._-]{1,255}$/u.test(record.binlog_filename) ||
      typeof record.next_event_position !== 'number' ||
      !Number.isSafeInteger(record.next_event_position) ||
      record.next_event_position < 4 ||
      record.next_event_position > 4_294_967_295
    )
      invalid();
    const position = {
      binlogFilename: record.binlog_filename,
      binlogPosition: record.next_event_position,
      serverId,
    };
    fileSequence(position.binlogFilename);
    if (latest === null || compareMysqlPositions(latest, position) < 0) latest = position;
  }
  return latest;
}

export function mysqlConfigurationFingerprint(config: MysqlToKafkaTaskConfig): string {
  return createHash('sha256')
    .update(
      JSON.stringify({
        source: config.source.dataSourceId,
        target: config.target.dataSourceId,
        mappings: config.mappings
          .map((mapping) => ({
            source: mapping.source,
            targets: mapping.targets.map((target) => target.topic).sort(),
          }))
          .sort((a, b) =>
            `${a.source.schema}.${a.source.name}`.localeCompare(
              `${b.source.schema}.${b.source.name}`,
            ),
          ),
      }),
    )
    .digest('hex');
}

interface CheckpointRow {
  phase: MysqlRuntimePhase;
  configuration_sha256: string;
  source_identity: string;
  source_row_version: string;
  client_server_id: string;
  binlog_filename: string;
  binlog_position: string;
}

function binding(row: CheckpointRow): MysqlRuntimeBinding {
  return {
    phase: row.phase,
    sourceIdentity: row.source_identity,
    sourceRowVersion: row.source_row_version,
    serverId: Number(row.client_server_id),
    binlogFilename: row.binlog_filename,
    binlogPosition: Number(row.binlog_position),
  };
}

async function boundedLog(file: string): Promise<string> {
  let handle;
  try {
    handle = await open(file, 'r');
    const stat = await handle.stat();
    if (!stat.isFile()) invalid();
    const start = Math.max(0, stat.size - 256 * 1024);
    const buffer = Buffer.alloc(Math.min(stat.size, 256 * 1024));
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, start);
    const text = buffer.subarray(0, bytesRead).toString('utf8');
    return start === 0 ? text : text.slice(text.indexOf('\n') + 1);
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT')
      return '';
    throw error;
  } finally {
    await handle?.close();
  }
}

export async function readMysqlRuntimeLog(directory: string, kind: 'checkpoint'): Promise<string> {
  const base = path.join(directory, `mysql-${kind}`);
  return (await boundedLog(`${base}.1.log`)) + (await boundedLog(`${base}.log`));
}

type SnapshotTables = {
  readonly mappings: readonly {
    readonly source: { readonly schema: string; readonly name: string };
  }[];
};

function snapshotEvidence(config: SnapshotTables) {
  const remaining = new Set(config.mappings.map(({ source }) => `${source.schema}.${source.name}`));
  let taskFinished = false;
  return {
    accept(line: string) {
      if (line.trim() === 'task finished') {
        taskFinished = true;
        return;
      }
      if (!line.startsWith('{')) return;
      try {
        const value: unknown = JSON.parse(line);
        if (typeof value !== 'object' || value === null) return;
        const record = value as Record<string, unknown>;
        if (
          record.type === 'RdbSnapshotFinished' &&
          record.db_type === 'mysql' &&
          typeof record.schema === 'string' &&
          typeof record.tb === 'string'
        )
          remaining.delete(`${record.schema}.${record.tb}`);
      } catch {
        /* A truncated log line cannot prove that a table finished. */
      }
    },
    completed: () => taskFinished && remaining.size === 0,
  };
}

export function mysqlSnapshotCompleted(text: string, config: SnapshotTables): boolean {
  const evidence = snapshotEvidence(config);
  for (const line of text.split('\n')) evidence.accept(line);
  return evidence.completed();
}

export async function readMysqlSnapshotCompleted(
  directory: string,
  config: SnapshotTables,
): Promise<boolean> {
  let handle;
  try {
    handle = await open(path.join(directory, 'mysql-finished.log'), 'r');
    if (!(await handle.stat()).isFile()) invalid();
    const evidence = snapshotEvidence(config);
    // One compact completion record per table; stream all evidence, not just a log tail.
    const lines = createInterface({
      input: handle.createReadStream({ autoClose: false }),
      crlfDelay: Infinity,
    });
    try {
      for await (const line of lines) evidence.accept(line);
    } finally {
      lines.close();
    }
    return evidence.completed();
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT')
      return false;
    throw error;
  } finally {
    await handle?.close();
  }
}

interface Watch {
  readonly taskId: string;
  readonly revisionId: string;
  readonly runId: string;
  readonly directory: string;
  position: MysqlRuntimeBinding;
  timer?: ReturnType<typeof setTimeout>;
  pending?: Promise<void>;
  stopped: boolean;
}

@Injectable()
export class MysqlRuntimeStateStore implements OnModuleDestroy {
  private readonly logger = new Logger('MysqlCheckpoint');
  private readonly watches = new Map<string, Watch>();
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly sources: DataSourcesService,
    private readonly audit: AuditLedgerService,
  ) {}

  async prepareWithin(
    client: PoolClient,
    input: {
      readonly taskId: string;
      readonly revisionId: string;
      readonly runId: string;
      readonly config: MysqlToKafkaTaskConfig;
      readonly recovery: boolean;
      readonly reinitialize?: boolean;
      readonly actorUserId: string | null;
    },
  ): Promise<MysqlRuntimeBinding> {
    const source = await this.sources.getForSystem(input.config.source.dataSourceId);
    const inspection = await this.sources.inspectMysqlCdcForSystem(source.id, source.rowVersion);
    if (
      inspection.checks.some((check) => check.state !== 'PASSED') ||
      inspection.identity.value === null ||
      inspection.position === null
    )
      throw new MysqlRuntimeStateError('MYSQL_CDC_NOT_READY');
    const existing = await client.query<CheckpointRow>(
      `SELECT phase, configuration_sha256, source_identity, source_row_version::text,
              client_server_id::text, binlog_filename, binlog_position::text
         FROM ape_dts_platform.mysql_task_checkpoints WHERE task_id=$1::uuid FOR UPDATE`,
      [input.taskId],
    );
    const old = existing.rows[0];
    const hash = mysqlConfigurationFingerprint(input.config);
    if (input.recovery && old === undefined)
      throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_MISSING');
    if (old === undefined && !input.reinitialize) {
      const previous = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM ape_dts_platform.task_runs
          WHERE task_id=$1::uuid AND id<>$2::uuid) AS exists`,
        [input.taskId, input.runId],
      );
      if (previous.rows[0]?.exists !== false)
        throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_MISSING');
    }
    if (
      input.reinitialize &&
      (input.recovery || input.actorUserId === null || !input.config.source.includeSnapshot)
    )
      throw new MysqlRuntimeStateError('MYSQL_SNAPSHOT_NOT_ENABLED');
    if (old !== undefined && !input.reinitialize) {
      if (old.phase === 'SNAPSHOT' && !input.config.source.includeSnapshot)
        throw new MysqlRuntimeStateError('MYSQL_SNAPSHOT_NOT_ENABLED');
      if (old.source_identity !== inspection.identity.value)
        throw new MysqlRuntimeStateError('MYSQL_SOURCE_IDENTITY_CHANGED');
      if (old.configuration_sha256 !== hash)
        throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_CONFIGURATION_CHANGED');
      const current = { ...binding(old), sourceRowVersion: source.rowVersion };
      await this.assertAvailable(input.config, current);
      await client.query(
        `UPDATE ape_dts_platform.mysql_task_checkpoints
          SET task_revision_id=$2::uuid, active_run_id=$3::uuid, source_row_version=$4::bigint, updated_at=statement_timestamp()
          WHERE task_id=$1::uuid`,
        [input.taskId, input.revisionId, input.runId, source.rowVersion],
      );
      return current;
    }
    const serverId = old === undefined ? randomInt(1, 4_294_967_296) : Number(old.client_server_id);
    if (serverId === inspection.binlog.serverId)
      throw new MysqlRuntimeStateError('MYSQL_REPLICATION_CLIENT_ID_CONFLICT');
    const current: MysqlRuntimeBinding = {
      phase: input.config.source.includeSnapshot ? 'SNAPSHOT' : 'CDC',
      sourceIdentity: inspection.identity.value,
      sourceRowVersion: source.rowVersion,
      serverId,
      binlogFilename: inspection.position.filename,
      binlogPosition: inspection.position.position,
    };
    await this.assertAvailable(input.config, current);
    await client.query(
      `INSERT INTO ape_dts_platform.mysql_task_checkpoints
        (task_id,task_revision_id,active_run_id,configuration_sha256,source_identity,source_row_version,
         client_server_id,phase,binlog_filename,binlog_position)
        VALUES ($1::uuid,$2::uuid,$3::uuid,$4,$5,$6::bigint,$7::bigint,$8,$9,$10::bigint)
        ON CONFLICT (task_id) DO UPDATE SET task_revision_id=EXCLUDED.task_revision_id,
          active_run_id=EXCLUDED.active_run_id, configuration_sha256=EXCLUDED.configuration_sha256,
          source_identity=EXCLUDED.source_identity, source_row_version=EXCLUDED.source_row_version,
          phase=EXCLUDED.phase, binlog_filename=EXCLUDED.binlog_filename, binlog_position=EXCLUDED.binlog_position,
          checkpoint_at=NULL, updated_at=statement_timestamp()`,
      [
        input.taskId,
        input.revisionId,
        input.runId,
        hash,
        current.sourceIdentity,
        current.sourceRowVersion,
        current.serverId,
        current.phase,
        current.binlogFilename,
        current.binlogPosition,
      ],
    );
    if (input.reinitialize)
      await this.audit.append(client, {
        action: 'TASK_MYSQL_CHECKPOINT_REINITIALIZED',
        actorType: 'USER',
        actorUserId: input.actorUserId,
        details: { runId: input.runId, previousCheckpoint: old ?? null },
        subjectId: input.taskId,
        subjectType: 'TASK',
      });
    return current;
  }

  async assertAvailable(
    config: MysqlToKafkaTaskConfig,
    position: MysqlRuntimeBinding,
  ): Promise<void> {
    const inspection = await this.sources.inspectMysqlCdcForSystem(
      config.source.dataSourceId,
      position.sourceRowVersion,
    );
    if (inspection.checks.some((check) => check.state !== 'PASSED'))
      throw new MysqlRuntimeStateError('MYSQL_CDC_NOT_READY');
    if (inspection.identity.value !== position.sourceIdentity)
      throw new MysqlRuntimeStateError('MYSQL_SOURCE_IDENTITY_CHANGED');
    if (inspection.binlog.serverId === position.serverId)
      throw new MysqlRuntimeStateError('MYSQL_REPLICATION_CLIENT_ID_CONFLICT');
    const files = await this.sources.readMysqlBinlogsForSystem(
      config.source.dataSourceId,
      position.sourceRowVersion,
    );
    const file = files.find((item) => item.filename === position.binlogFilename);
    fileSequence(position.binlogFilename);
    if (file === undefined || BigInt(file.sizeBytes) < BigInt(position.binlogPosition))
      throw new MysqlRuntimeStateError('MYSQL_BINLOG_POSITION_UNAVAILABLE');
  }

  async finishSnapshotWithin(client: PoolClient, taskId: string, runId: string): Promise<void> {
    const result = await client.query(
      `UPDATE ape_dts_platform.mysql_task_checkpoints SET phase='CDC', updated_at=statement_timestamp()
        WHERE task_id=$1::uuid AND active_run_id=$2::uuid AND phase='SNAPSHOT'`,
      [taskId, runId],
    );
    if (result.rowCount !== 1) throw new MysqlRuntimeStateError('MYSQL_CHECKPOINT_BINDING_CHANGED');
  }

  watch(input: Omit<Watch, 'stopped' | 'timer' | 'pending'>): void {
    this.unwatch(input.runId);
    const watch: Watch = { ...input, stopped: false };
    this.watches.set(input.runId, watch);
    const poll = async () => {
      await this.flush(input.runId);
      if (!watch.stopped) {
        watch.timer = setTimeout(() => void poll(), 1000);
        watch.timer.unref();
      }
    };
    void poll();
  }

  async flush(runId: string): Promise<void> {
    const watch = this.watches.get(runId);
    if (watch === undefined) return;
    while (watch.pending !== undefined) await watch.pending;
    watch.pending = this.collect(watch).catch((error: unknown) => {
      this.logger.warn({
        code:
          error instanceof MysqlRuntimeStateError ? error.code : 'MYSQL_CHECKPOINT_PERSIST_FAILED',
        taskId: watch.taskId,
        runId,
      });
    });
    try {
      await watch.pending;
    } finally {
      delete watch.pending;
    }
  }

  private async collect(watch: Watch): Promise<void> {
    const position = parseMysqlCheckpointLog(
      await readMysqlRuntimeLog(watch.directory, 'checkpoint'),
      watch.position.serverId,
    );
    if (position === null || compareMysqlPositions(watch.position, position) >= 0) return;
    const result = await this.database.requirePool().query(
      `UPDATE ape_dts_platform.mysql_task_checkpoints AS checkpoint
        SET binlog_filename=$4,binlog_position=$5::bigint,checkpoint_at=statement_timestamp(),updated_at=statement_timestamp()
        WHERE task_id=$1::uuid AND task_revision_id=$2::uuid AND active_run_id=$3::uuid AND phase='CDC'
          AND EXISTS (SELECT 1 FROM ape_dts_platform.task_runs run WHERE run.id=$3::uuid AND run.task_id=$1::uuid
                       AND run.task_revision_id=$2::uuid AND run.status IN ('STARTING','RUNNING','STOPPING'))`,
      [
        watch.taskId,
        watch.revisionId,
        watch.runId,
        position.binlogFilename,
        position.binlogPosition,
      ],
    );
    if (result.rowCount === 1) watch.position = { ...watch.position, ...position };
  }

  unwatch(runId: string): void {
    const watch = this.watches.get(runId);
    if (watch === undefined) return;
    watch.stopped = true;
    clearTimeout(watch.timer);
    this.watches.delete(runId);
  }

  async cleanup(directory: string): Promise<void> {
    for (const kind of ['checkpoint', 'finished'])
      for (const suffix of ['.log', '.1.log'])
        await rm(path.join(directory, `mysql-${kind}${suffix}`), { force: true });
  }

  onModuleDestroy(): void {
    // Keep bindings available for the supervisor's final flush during graceful shutdown.
    for (const watch of this.watches.values()) {
      watch.stopped = true;
      clearTimeout(watch.timer);
    }
  }
}
