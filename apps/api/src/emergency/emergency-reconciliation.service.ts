import { randomUUID } from 'node:crypto';

import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import type { PoolClient } from 'pg';

import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { BootstrapStateService } from '../bootstrap/bootstrap-state.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { MetadataAvailabilityService } from '../metadata-availability/metadata-availability.service.js';
import { RuntimeSupervisorService } from '../runtime/runtime-supervisor.service.js';
import type { EmergencyPendingEvent } from './emergency-pending-events.js';
import { EmergencyPendingEventQueue } from './emergency-pending-events.js';

@Injectable()
export class EmergencyReconciliationService implements OnModuleDestroy, OnModuleInit {
  private unregister: (() => void) | undefined;

  constructor(
    private readonly availability: MetadataAvailabilityService,
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    private readonly bootstrap: BootstrapStateService,
    private readonly pending: EmergencyPendingEventQueue,
    private readonly runtime: RuntimeSupervisorService,
  ) {}

  onModuleInit(): void {
    this.unregister = this.availability.registerReconciler(() => this.reconcile());
  }

  onModuleDestroy(): void {
    this.unregister?.();
    this.unregister = undefined;
  }

  async reconcile(): Promise<void> {
    const events = this.pending.snapshot();
    const outage = this.availability.pendingOutageFact();
    const gap = this.pending.gapFact();

    await this.transaction(async (client) => {
      for (const event of events) {
        await this.persistEmergencyEvent(client, event);
      }
    });

    await this.runtime.reconcilePersistedRuns();

    await this.transaction(async (client) => {
      if (outage !== null) {
        const existing = await client.query(
          'SELECT 1 FROM ape_dts_platform.metadata_outage_events WHERE id = $1::uuid',
          [outage.eventId],
        );
        if (existing.rows[0] === undefined) {
          const auditSequence = await this.audit.append(client, {
            action: 'METADATA_OUTAGE_RECOVERED',
            actorType: 'SYSTEM',
            actorUserId: null,
            details: { detectedAt: outage.detectedAt, detectionSource: outage.source },
            subjectId: this.platformInstanceId(),
            subjectType: 'PLATFORM_INSTANCE',
          });
          await client.query(
            `INSERT INTO ape_dts_platform.metadata_outage_events
               (id, platform_instance_id, detected_at, recovered_at, detection_source, audit_sequence)
             VALUES ($1::uuid, $2::uuid, $3::timestamptz, statement_timestamp(), $4, $5::bigint)`,
            [
              outage.eventId,
              this.platformInstanceId(),
              outage.detectedAt,
              outage.source,
              auditSequence,
            ],
          );
        }
      }
      if (gap !== null) {
        const existing = await client.query(
          'SELECT 1 FROM ape_dts_platform.audit_gap_events WHERE id = $1::uuid',
          [gap.eventId],
        );
        if (existing.rows[0] === undefined) {
          const auditSequence = await this.audit.append(client, {
            action: 'EMERGENCY_AUDIT_GAP_RECORDED',
            actorType: 'SYSTEM',
            actorUserId: null,
            details: { omittedEventCount: gap.omittedEvents },
            subjectId: this.platformInstanceId(),
            subjectType: 'PLATFORM_INSTANCE',
          });
          await client.query(
            `INSERT INTO ape_dts_platform.audit_gap_events
               (id, platform_instance_id, gap_kind, omitted_event_count, external_evidence, audit_sequence)
             VALUES ($1::uuid, $2::uuid, 'EMERGENCY_QUEUE_OVERFLOW', $3, $4, $5::bigint)`,
            [
              gap.eventId,
              this.platformInstanceId(),
              gap.omittedEvents,
              'in-memory emergency queue overflow counter',
              auditSequence,
            ],
          );
        }
      }
      await this.persistUncleanInstanceGaps(client);
    });

    if (events.at(-1) !== undefined) {
      this.pending.acknowledgeThrough(events.at(-1)!.sequence);
    }
    if (gap !== null) {
      this.pending.acknowledgeGap(gap.omittedEvents);
    }
    if (outage !== null) {
      this.availability.acknowledgeOutageFact(outage.eventId);
    }
  }

  private async persistEmergencyEvent(
    client: PoolClient,
    event: EmergencyPendingEvent,
  ): Promise<void> {
    const existing = await client.query(
      'SELECT 1 FROM ape_dts_platform.emergency_runtime_events WHERE event_id = $1::uuid',
      [event.eventId],
    );
    if (existing.rows[0] !== undefined) return;
    if (
      event.taskId === null ||
      event.processBindingHash === undefined ||
      event.operationId === undefined ||
      event.platformInstanceId === undefined ||
      event.revisionId === undefined ||
      typeof event.details?.runId !== 'string' ||
      typeof event.details.unitCount !== 'number'
    ) {
      throw new Error('EMERGENCY_RECONCILIATION_FACT_INCOMPLETE');
    }

    const confirmed = event.action.endsWith('_CONFIRMED');
    let reconciliationCode = 'REQUEST_ONLY';
    if (confirmed) {
      const updated = await client.query(
        `UPDATE ape_dts_platform.tasks AS task
            SET desired_state = 'STOPPED', observed_state = 'STOPPED',
                row_version = row_version + 1, updated_at = statement_timestamp()
          WHERE task.id = $1::uuid
            AND task.deleted_at IS NULL
            AND task.desired_state = 'RUNNING'
            AND task.current_applied_revision_id = $3::uuid
            AND EXISTS (
              SELECT 1 FROM ape_dts_platform.task_runs AS target_run
               WHERE target_run.id = $2::uuid
                 AND target_run.task_id = task.id
                 AND target_run.task_revision_id = $3::uuid
                 AND target_run.operation_id = $4::uuid
                 AND target_run.platform_instance_id = $5::uuid
                 AND target_run.process_binding_hash = $6
                 AND target_run.status IN ('STARTING', 'RUNNING', 'STOPPING')
            )
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_runs AS newer_run
               WHERE newer_run.task_id = task.id
                 AND newer_run.id <> $2::uuid
                 AND newer_run.status IN ('STARTING', 'RUNNING', 'STOPPING')
            )
            AND NOT EXISTS (
              SELECT 1 FROM ape_dts_platform.task_operations AS newer_operation
               WHERE newer_operation.task_id = task.id
                 AND newer_operation.id <> $4::uuid
                 AND newer_operation.state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING')
            )`,
        [
          event.taskId,
          event.details.runId,
          event.revisionId,
          event.operationId,
          event.platformInstanceId,
          event.processBindingHash,
        ],
      );
      if (updated.rowCount === 1) {
        reconciliationCode = 'CONFIRMED_APPLIED';
        await client.query(
          `UPDATE ape_dts_platform.task_run_units SET status = 'STOPPED', ended_at = statement_timestamp(),
                  metrics_port_released_at = statement_timestamp()
            WHERE run_id = $1::uuid AND task_id = $2::uuid
              AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
          [event.details.runId, event.taskId],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_runs SET status = 'STOPPED', current_stage = 'STOPPED', ended_at = statement_timestamp()
            WHERE id = $1::uuid AND task_id = $2::uuid AND task_revision_id = $3::uuid
              AND operation_id = $4::uuid AND platform_instance_id = $5::uuid
              AND process_binding_hash = $6 AND status IN ('STARTING', 'RUNNING', 'STOPPING')`,
          [
            event.details.runId,
            event.taskId,
            event.revisionId,
            event.operationId,
            event.platformInstanceId,
            event.processBindingHash,
          ],
        );
        await client.query(
          `UPDATE ape_dts_platform.task_operations SET state = 'SUCCEEDED', execution_stage = 'STOPPED',
                  completed_at = statement_timestamp(), updated_at = statement_timestamp()
            WHERE id = $1::uuid AND task_id = $2::uuid AND run_id = $3::uuid
              AND target_revision_id = $4::uuid AND state IN ('PREPARED', 'EXECUTING')`,
          [event.operationId, event.taskId, event.details.runId, event.revisionId],
        );
        await client.query(
          `UPDATE ape_dts_platform.capacity_reservations SET state = 'RELEASED', released_at = statement_timestamp()
            WHERE run_id = $1::uuid AND task_id = $2::uuid AND task_revision_id = $3::uuid AND state = 'ACTIVE'`,
          [event.details.runId, event.taskId, event.revisionId],
        );
      } else {
        reconciliationCode = 'STALE_OR_BINDING_MISMATCH';
      }
    }
    const actorType = confirmed ? 'SYSTEM' : 'DEPLOYMENT_ADMIN';
    const auditSequence = await this.audit.append(client, {
      action: event.action,
      actorType,
      actorUserId: null,
      details: {
        operationId: event.operationId,
        reconciliationOutcome: reconciliationCode,
        revisionId: event.revisionId,
        runId: event.details.runId,
        unitCount: event.details.unitCount,
      },
      subjectId: event.taskId,
      subjectType: 'TASK',
    });
    await client.query(
      `INSERT INTO ape_dts_platform.emergency_runtime_events
         (event_id, platform_instance_id, task_id, run_id, revision_id, operation_id, action,
          actor_type, reason, unit_count, process_binding_hash, occurred_at, reconciliation_code, audit_sequence)
       VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6::uuid, $7, $8, $9, $10, $11, $12::timestamptz, $13, $14::bigint)`,
      [
        event.eventId,
        event.platformInstanceId,
        event.taskId,
        event.details.runId,
        event.revisionId,
        event.operationId,
        event.action,
        actorType,
        event.reason,
        event.details.unitCount,
        event.processBindingHash,
        event.occurredAt,
        reconciliationCode,
        auditSequence,
      ],
    );
  }

  private async persistUncleanInstanceGaps(client: PoolClient): Promise<void> {
    const instances = await client.query<{
      clean_shutdown: boolean | null;
      ended_at: Date | null;
      id: string;
      started_at: Date;
    }>(
      `SELECT instance.id, instance.started_at, instance.ended_at, instance.clean_shutdown
         FROM ape_dts_platform.platform_instances AS instance
        WHERE instance.id <> $1::uuid
          AND (instance.ended_at IS NULL OR instance.clean_shutdown IS DISTINCT FROM TRUE)
          AND NOT EXISTS (SELECT 1 FROM ape_dts_platform.audit_gap_events AS gap
                           WHERE gap.platform_instance_id = instance.id AND gap.gap_kind = 'UNCLEAN_PLATFORM_INSTANCE')
        FOR UPDATE`,
      [this.platformInstanceId()],
    );
    for (const instance of instances.rows) {
      const auditSequence = await this.audit.append(client, {
        action: 'UNCLEAN_PLATFORM_INSTANCE_GAP_RECORDED',
        actorType: 'SYSTEM',
        actorUserId: null,
        details: {
          cleanShutdown: instance.clean_shutdown,
          knownEndedAt: instance.ended_at?.toISOString() ?? null,
          knownStartedAt: instance.started_at.toISOString(),
        },
        subjectId: instance.id,
        subjectType: 'PLATFORM_INSTANCE',
      });
      await client.query(
        `INSERT INTO ape_dts_platform.audit_gap_events
           (id, platform_instance_id, gap_kind, known_started_at, known_ended_at, external_evidence, audit_sequence)
         VALUES ($1::uuid, $2::uuid, 'UNCLEAN_PLATFORM_INSTANCE', $3, $4, $5, $6::bigint)
         ON CONFLICT (platform_instance_id, gap_kind) WHERE gap_kind = 'UNCLEAN_PLATFORM_INSTANCE' DO NOTHING`,
        [
          randomUUID(),
          instance.id,
          instance.started_at,
          instance.ended_at,
          instance.ended_at === null
            ? 'platform instance has no ended_at; termination cleanliness is unknown'
            : 'platform instance ended without clean_shutdown=true',
          auditSequence,
        ],
      );
    }
  }

  private platformInstanceId(): string {
    return this.bootstrap.getStatus().platformInstanceId;
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
