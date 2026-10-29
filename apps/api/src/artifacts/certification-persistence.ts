import { randomUUID } from 'node:crypto';
import { ApeDtsV1Adapter, type TaskScenario } from '@scs/config-model';
import type { PoolClient } from 'pg';
import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { safeCertificationFailureCode } from './certification-errors.js';

export interface CertificationIdentity {
  artifactId: string;
  sha256: string;
  platformVersion: string;
  scenarios: readonly TaskScenario[];
}
export interface CertificationEvidence {
  passed: boolean;
  exitCode: number;
  summary: Record<string, unknown>;
  failureCode?: string;
}

/** Deployment-only orchestration. No HTTP/report-import interface accepts a pass result. */
export async function certifyArtifact(
  client: PoolClient,
  audit: Pick<AuditLedgerService, 'append'>,
  identity: CertificationIdentity,
  execute: () => Promise<CertificationEvidence>,
): Promise<{ probeId: string; passed: boolean; failureCode: string | null }> {
  const probeId = randomUUID();
  const adapter = new ApeDtsV1Adapter();
  const lock = await client.query<{ acquired: boolean }>(
    'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS acquired',
    [`artifact-certification:${identity.artifactId}`],
  );
  if (lock.rows[0]?.acquired !== true) throw new Error('ARTIFACT_CERTIFICATION_BUSY');
  try {
    await client.query('BEGIN');
    const actor = await client.query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.users
       WHERE normalized_username = 'admin' AND role = 'ADMIN' AND status = 'ACTIVE' FOR SHARE`,
    );
    const actorId = actor.rows[0]?.id ?? '';
    if (!actorId) throw new Error('ACTIVE_ADMIN_REQUIRED');
    await assertArtifact(client, identity);
    await client.query(
      `INSERT INTO ape_dts_platform.artifact_probe_runs
       (id, artifact_id, probe_kind, probe_version, status, started_by_user_id, evidence_summary)
       VALUES ($1::uuid,$2::uuid,'BEHAVIOR','deployment-phase0-v1','RUNNING',$3::uuid,$4::jsonb)`,
      [probeId, identity.artifactId, actorId, JSON.stringify(identity)],
    );
    for (const scenario of identity.scenarios) {
      await client.query(
        `INSERT INTO ape_dts_platform.artifact_certifications
         (id,artifact_id,platform_version,task_scenario,adapter_id,adapter_version,state)
         VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,'PENDING')
         ON CONFLICT (artifact_id,platform_version,task_scenario,adapter_id,adapter_version)
         DO UPDATE SET state='PENDING',evidence_probe_run_id=NULL,failure_code=NULL,
           revoked_at=NULL,row_version=artifact_certifications.row_version+1,
           updated_at=statement_timestamp()`,
        [
          randomUUID(),
          identity.artifactId,
          identity.platformVersion,
          scenario,
          adapter.id,
          adapter.version,
        ],
      );
    }
    await audit.append(client, {
      action: 'ARTIFACT_CERTIFICATION_STARTED',
      actorType: 'USER',
      actorUserId: actorId,
      subjectId: identity.artifactId,
      subjectType: 'ARTIFACT',
      details: {
        probeId,
        sha256: identity.sha256,
        scenarios: identity.scenarios,
        execution: 'DEPLOYMENT_CLI',
      },
    });
    await client.query('COMMIT');

    let evidence: CertificationEvidence;
    try {
      evidence = await execute();
    } catch (error) {
      const failureCode = safeCertificationFailureCode(error);
      evidence = {
        passed: false,
        exitCode: 1,
        failureCode,
        summary: { failure: failureCode },
      };
    }
    await client.query('BEGIN');
    let bindingValid = true;
    try {
      await assertArtifact(client, identity);
    } catch {
      bindingValid = false;
    }
    const activeActor = await client.query<{ id: string }>(
      `SELECT id FROM ape_dts_platform.users WHERE id=$1::uuid AND role='ADMIN' AND status='ACTIVE' FOR SHARE`,
      [actorId],
    );
    if (activeActor.rows[0] === undefined) bindingValid = false;
    const passed = bindingValid && evidence.passed && evidence.exitCode === 0;
    const failure = passed
      ? null
      : bindingValid
        ? safeCertificationFailureCode(evidence.failureCode ?? 'BEHAVIOR_SUITE_FAILED')
        : 'ARTIFACT_BINDING_CHANGED';
    await client.query(
      `UPDATE ape_dts_platform.artifact_probe_runs SET status=$2,ended_at=statement_timestamp(),
       exit_code=$3,failure_code=$4,evidence_summary=$5::jsonb
       WHERE id=$1::uuid AND status='RUNNING'`,
      [
        probeId,
        passed ? 'PASSED' : 'FAILED',
        evidence.exitCode,
        failure,
        JSON.stringify({
          ...evidence.summary,
          ...identity,
          adapterId: adapter.id,
          adapterVersion: adapter.version,
        }),
      ],
    );
    for (const scenario of identity.scenarios) {
      await client.query(
        `UPDATE ape_dts_platform.artifact_certifications SET state=$6,evidence_probe_run_id=$7::uuid,
         failure_code=$8,row_version=row_version+1,updated_at=statement_timestamp()
         WHERE artifact_id=$1::uuid AND platform_version=$2 AND task_scenario=$3
           AND adapter_id=$4 AND adapter_version=$5 AND state='PENDING'`,
        [
          identity.artifactId,
          identity.platformVersion,
          scenario,
          adapter.id,
          adapter.version,
          passed ? 'PASSED' : 'FAILED',
          probeId,
          failure,
        ],
      );
    }
    await audit.append(client, {
      action: passed ? 'ARTIFACT_CERTIFICATION_PASSED' : 'ARTIFACT_CERTIFICATION_FAILED',
      actorType: 'USER',
      actorUserId: actorId,
      subjectId: identity.artifactId,
      subjectType: 'ARTIFACT',
      details: { probeId, sha256: identity.sha256, scenarios: identity.scenarios, passed },
    });
    await client.query('COMMIT');
    return { probeId, passed, failureCode: failure };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    await client.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [
      `artifact-certification:${identity.artifactId}`,
    ]);
  }
}

async function assertArtifact(client: PoolClient, identity: CertificationIdentity): Promise<void> {
  const result = await client.query<{ valid: boolean }>(
    `SELECT (a.state='INACTIVE' AND encode(a.declared_sha256,'hex')=$2
       AND encode(a.actual_sha256,'hex')=$2 AND encode(o.sha256,'hex')=$2
       AND o.state='AVAILABLE' AND a.size_bytes=o.size_bytes) AS valid
     FROM ape_dts_platform.artifacts a JOIN ape_dts_platform.stored_objects o ON o.id=a.stored_object_id
     WHERE a.id=$1::uuid AND a.deleted_at IS NULL FOR UPDATE OF a`,
    [identity.artifactId, identity.sha256],
  );
  if (result.rows[0]?.valid !== true) throw new Error('ARTIFACT_INACTIVE_SHA_BINDING_REQUIRED');
}
