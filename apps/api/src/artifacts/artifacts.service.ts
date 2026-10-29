import { randomUUID } from 'node:crypto';
import type { Readable } from 'node:stream';
import { ConflictException, Inject, Injectable, NotFoundException } from '@nestjs/common';
import type {
  ArtifactSummary,
  RegisterArtifactRequest,
  UserSummary,
  ArtifactFileCandidate,
  RegisterArtifactFileRequest,
} from '@scs/contracts';
import {
  ArtifactCache,
  ArtifactLoader,
  ElfArtifactInspector,
  type ObjectStorageDriver,
} from '@scs/storage';
import type { PoolClient } from 'pg';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { ARTIFACT_CACHE, LOCAL_OBJECT_STORAGE } from '../storage/storage.tokens.js';
import { ArtifactFiles } from './artifact-files.js';

interface ArtifactRow {
  id: string;
  version_identifier: string;
  declared_source: string;
  object_key: string;
  sha256: string;
  size_bytes: string;
  state: ArtifactSummary['state'];
  state_reason: string | null;
  verified: boolean;
  certifications: ArtifactSummary['certifications'];
  relative_prefix: string;
  driver: string;
}
const SELECT = `SELECT a.id, a.version_identifier, a.declared_source, o.object_key,
  encode(a.declared_sha256, 'hex') AS sha256, a.size_bytes::text, a.state, a.state_reason,
  a.actual_sha256 IS NOT NULL AS verified, p.driver,
  COALESCE(p.config ->> 'relative_prefix', '') AS relative_prefix,
  COALESCE((SELECT jsonb_agg(jsonb_build_object('scenario', c.task_scenario,
    'platformVersion', c.platform_version, 'state', c.state))
    FROM ape_dts_platform.artifact_certifications c WHERE c.artifact_id = a.id), '[]'::jsonb) AS certifications
  FROM ape_dts_platform.artifacts a
  JOIN ape_dts_platform.stored_objects o ON o.id = a.stored_object_id
  JOIN ape_dts_platform.storage_profiles p ON p.id = o.storage_profile_id`;
function summary(row: ArtifactRow): ArtifactSummary {
  return {
    id: row.id,
    versionIdentifier: row.version_identifier,
    declaredSource: row.declared_source,
    objectKey: row.object_key,
    sha256: row.sha256,
    sizeBytes: row.size_bytes,
    state: row.state,
    stateReason: row.state_reason,
    verified: row.verified,
    certifications: row.certifications,
  };
}

@Injectable()
export class ArtifactsService {
  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly audit: AuditLedgerService,
    @Inject(ARTIFACT_CACHE) private readonly cache: ArtifactCache,
    @Inject(LOCAL_OBJECT_STORAGE) private readonly storage: ObjectStorageDriver,
    @Inject(ArtifactFiles) private readonly files: ArtifactFiles = new ArtifactFiles(),
  ) {}

  private async writableProfile() {
    const profiles = await this.database
      .requirePool()
      .query<{ id: string; relative_prefix: string }>(
        `SELECT id, COALESCE(config ->> 'relative_prefix', '') AS relative_prefix
       FROM ape_dts_platform.storage_profiles WHERE driver = 'LOCAL' AND state = 'ACTIVE_WRITE'`,
      );
    if (profiles.rows.length !== 1)
      throw new ConflictException({ code: 'ARTIFACT_LOCAL_STORAGE_UNAVAILABLE' });
    return profiles.rows[0]!;
  }

  async listFiles() {
    const profile = await this.writableProfile();
    return this.files.scan(profile.relative_prefix);
  }

  async uploadFile(name: string, input: Readable, signal?: AbortSignal) {
    const profile = await this.writableProfile();
    return this.files.upload(
      profile.relative_prefix,
      name,
      input,
      async (binary) => {
        await this.inspectBinary(binary.path);
        return {
          fileName: binary.fileName,
          sha256: binary.sha256,
          sizeBytes: binary.sizeBytes,
          suggestedVersion: binary.suggestedVersion,
        };
      },
      signal,
    );
  }

  private async inspectBinary(binaryPath: string) {
    if (process.arch !== 'x64' && process.arch !== 'arm64') {
      throw new ConflictException({ code: 'ARTIFACT_HOST_ARCHITECTURE_UNSUPPORTED' });
    }
    await new ElfArtifactInspector().inspect(binaryPath, {
      expectedArchitecture: process.arch === 'arm64' ? 'aarch64' : 'x86_64',
      allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
      allowedInterpreters: { mode: 'REPORT_ONLY' },
    });
  }

  async prepareFile(file: ArtifactFileCandidate) {
    const profile = await this.writableProfile();
    return this.files.withPrepared(profile.relative_prefix, file, async (binary) => {
      await this.inspectBinary(binary.path);
      return {
        fileName: binary.fileName,
        sha256: binary.sha256,
        sizeBytes: binary.sizeBytes,
        suggestedVersion: binary.suggestedVersion,
      };
    });
  }

  async registerFile(actor: UserSummary, input: RegisterArtifactFileRequest) {
    const profile = await this.writableProfile();
    return this.files.withPrepared(profile.relative_prefix, input.file, async (binary) => {
      if (binary.sha256 !== input.expectedSha256)
        throw new ConflictException({ code: 'ARTIFACT_FILE_CHANGED' });
      await this.inspectBinary(binary.path);
      const published = await this.files.publish(profile.relative_prefix, binary);
      try {
        return await this.register(
          actor,
          {
            versionIdentifier: input.versionIdentifier,
            declaredSource: input.declaredSource,
            objectKey: published.objectKey,
            sha256: binary.sha256,
            sizeBytes: binary.sizeBytes,
          },
          profile,
        );
      } catch (error) {
        // A failed COMMIT response can have an unknown outcome. Never delete a
        // published binary unless the database confirms that it is unreferenced.
        const referenced = await this.database.requirePool().query(
          `SELECT 1 FROM ape_dts_platform.stored_objects
           WHERE storage_profile_id = $1::uuid AND object_key = $2`,
          [profile.id, published.objectKey],
        );
        if (referenced.rows.length === 0) await published.remove();
        throw error;
      }
    });
  }

  async list(actor: UserSummary): Promise<ArtifactSummary[]> {
    const result = await this.database
      .requirePool()
      .query<ArtifactRow>(
        `${SELECT} WHERE a.deleted_at IS NULL AND ($1::boolean OR a.state <> 'QUARANTINED') ORDER BY a.created_at DESC`,
        [actor.role === 'ADMIN'],
      );
    return result.rows.map(summary);
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.database.requirePool().connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async delete(actor: UserSummary, id: string): Promise<{ deleted: true }> {
    return this.transaction(async (client) => {
      const result = await client.query<{ version_identifier: string; deleted_at: Date | null }>(
        `SELECT version_identifier, deleted_at FROM ape_dts_platform.artifacts WHERE id = $1::uuid FOR UPDATE`,
        [id],
      );
      const artifact = result.rows[0];
      if (!artifact) throw new NotFoundException({ code: 'ARTIFACT_NOT_FOUND' });
      if (artifact.deleted_at !== null) return { deleted: true };
      const references = await client.query<{ id: string; displayName: string }>(
        `SELECT DISTINCT t.id, t.display_name AS "displayName" FROM ape_dts_platform.tasks t
         WHERE (t.deleted_at IS NULL AND EXISTS (
           SELECT 1 FROM ape_dts_platform.task_revisions r WHERE r.artifact_id = $1::uuid
             AND r.id IN (t.current_draft_revision_id, t.current_applied_revision_id)))
         OR EXISTS (SELECT 1 FROM ape_dts_platform.task_runs run WHERE run.task_id = t.id
           AND run.artifact_id = $1::uuid AND run.ended_at IS NULL)
         ORDER BY t.id`,
        [id],
      );
      if (references.rows.length > 0)
        throw new ConflictException({ code: 'ARTIFACT_IN_USE', tasks: references.rows });
      await client.query(
        `UPDATE ape_dts_platform.artifacts SET deleted_at = statement_timestamp(),
          deleted_by_user_id = $2::uuid, updated_at = statement_timestamp(), row_version = row_version + 1
          WHERE id = $1::uuid AND deleted_at IS NULL`,
        [id, actor.id],
      );
      await this.audit.append(client, {
        action: 'ARTIFACT_DELETED',
        actorType: 'USER',
        actorUserId: actor.id,
        subjectType: 'ARTIFACT',
        subjectId: id,
        details: { versionIdentifier: artifact.version_identifier, filesPreserved: true },
      });
      return { deleted: true };
    });
  }

  private async inspect(objectKey: string, sha256: string, size: number): Promise<void> {
    if (process.arch !== 'x64' && process.arch !== 'arm64') {
      throw new ConflictException({ code: 'ARTIFACT_HOST_ARCHITECTURE_UNSUPPORTED' });
    }
    const loader = new ArtifactLoader(this.cache, new ElfArtifactInspector());
    const loaded = await loader.load(this.storage, {
      objectKey,
      expectedSha256: sha256,
      expectedSize: size,
      signaturePolicy: { mode: 'DISABLED' },
      staticPolicy: {
        expectedArchitecture: process.arch === 'arm64' ? 'aarch64' : 'x86_64',
        allowedDynamicLibraries: { mode: 'REPORT_ONLY' },
        allowedInterpreters: { mode: 'REPORT_ONLY' },
      },
    });
    await loaded.binding.close();
  }

  async register(
    actor: UserSummary,
    input: RegisterArtifactRequest,
    expectedProfile?: { id: string; relative_prefix: string },
  ): Promise<ArtifactSummary> {
    return this.transaction(async (client) => {
      const profiles = await client.query<{ id: string; relative_prefix: string }>(
        `SELECT id, COALESCE(config ->> 'relative_prefix', '') AS relative_prefix
         FROM ape_dts_platform.storage_profiles WHERE driver = 'LOCAL' AND state = 'ACTIVE_WRITE' FOR SHARE`,
      );
      const profile = profiles.rows[0];
      if (!profile) throw new ConflictException({ code: 'ARTIFACT_LOCAL_STORAGE_UNAVAILABLE' });
      if (
        expectedProfile !== undefined &&
        (profile.id !== expectedProfile.id ||
          profile.relative_prefix !== expectedProfile.relative_prefix)
      ) {
        throw new ConflictException({ code: 'ARTIFACT_STORAGE_PROFILE_CHANGED' });
      }
      await this.inspect(
        [profile.relative_prefix, input.objectKey].filter(Boolean).join('/'),
        input.sha256,
        input.sizeBytes,
      );
      const object = await client.query<{ id: string }>(
        `INSERT INTO ape_dts_platform.stored_objects
          (id, storage_profile_id, kind, object_key, size_bytes, sha256, content_type, state,
           created_by_user_id, available_at, last_verified_at)
         VALUES ($1::uuid, $2::uuid, 'ARTIFACT', $3, $4, decode($5, 'hex'), 'application/octet-stream',
           'AVAILABLE', $6::uuid, statement_timestamp(), statement_timestamp())
         ON CONFLICT (storage_profile_id, object_key) DO NOTHING RETURNING id`,
        [randomUUID(), profile.id, input.objectKey, input.sizeBytes, input.sha256, actor.id],
      );
      let objectId = object.rows[0]?.id;
      if (!objectId) {
        const existing = await client.query<{ id: string }>(
          `SELECT id FROM ape_dts_platform.stored_objects WHERE storage_profile_id = $1::uuid
           AND object_key = $2 AND kind = 'ARTIFACT' AND state = 'AVAILABLE'
           AND sha256 = decode($3, 'hex') AND size_bytes = $4 FOR SHARE`,
          [profile.id, input.objectKey, input.sha256, input.sizeBytes],
        );
        objectId = existing.rows[0]?.id;
        if (!objectId) throw new ConflictException({ code: 'ARTIFACT_OBJECT_CONFLICT' });
      }
      const id = randomUUID();
      await client.query(
        `INSERT INTO ape_dts_platform.artifacts
        (id, version_identifier, declared_source, stored_object_id, declared_sha256, actual_sha256,
         size_bytes, state, state_reason, created_by_user_id, activated_at)
        VALUES ($1::uuid, $2, $3, $4::uuid, decode($5, 'hex'), decode($5, 'hex'), $6,
          'ACTIVE', NULL, $7::uuid, statement_timestamp())`,
        [
          id,
          input.versionIdentifier,
          input.declaredSource,
          objectId,
          input.sha256,
          input.sizeBytes,
          actor.id,
        ],
      );
      await this.recordVerification(client, actor, id);
      await this.audit.append(client, {
        action: 'ARTIFACT_REGISTERED',
        actorType: 'USER',
        actorUserId: actor.id,
        subjectType: 'ARTIFACT',
        subjectId: id,
        details: { versionIdentifier: input.versionIdentifier, sha256: input.sha256 },
      });
      return summary(
        (await client.query<ArtifactRow>(`${SELECT} WHERE a.id = $1::uuid`, [id])).rows[0]!,
      );
    });
  }

  private async recordVerification(
    client: PoolClient,
    actor: UserSummary,
    id: string,
  ): Promise<void> {
    // The local-upload inspection above uses the explicit DISABLED policy.
    // Persist that decision for the runtime verifier; missing evidence is not a pass.
    await client.query(
      `INSERT INTO ape_dts_platform.artifact_signature_checks
       (id, artifact_id, policy_version, status, checked_by_user_id)
       VALUES ($1::uuid, $2::uuid, 'local-upload-signature-disabled-v1', 'NOT_REQUIRED', $3::uuid)`,
      [randomUUID(), id, actor.id],
    );
    await client.query(
      `INSERT INTO ape_dts_platform.artifact_probe_runs
      (id, artifact_id, probe_kind, probe_version, status, started_by_user_id, ended_at, evidence_summary)
      VALUES ($1::uuid, $2::uuid, 'STATIC', 'elf-sha256-v1', 'PASSED', $3::uuid, statement_timestamp(),
      '{"checks":["sha256","size","elf","architecture"],"behaviorCertified":false}'::jsonb)`,
      [randomUUID(), id, actor.id],
    );
  }

  async verify(actor: UserSummary, id: string, activate = false): Promise<ArtifactSummary> {
    return this.transaction(async (client) => {
      const result = await client.query<ArtifactRow>(
        `${SELECT} WHERE a.id = $1::uuid AND a.deleted_at IS NULL FOR UPDATE OF a`,
        [id],
      );
      const row = result.rows[0];
      if (!row) throw new NotFoundException({ code: 'ARTIFACT_NOT_FOUND' });
      if (row.state === 'QUARANTINED')
        throw new ConflictException({ code: 'ARTIFACT_QUARANTINED' });
      if (row.driver !== 'LOCAL')
        throw new ConflictException({ code: 'ARTIFACT_LOCAL_STORAGE_REQUIRED' });
      await this.inspect(
        [row.relative_prefix, row.object_key].filter(Boolean).join('/'),
        row.sha256,
        Number(row.size_bytes),
      );
      await this.recordVerification(client, actor, id);
      await client.query(
        `UPDATE ape_dts_platform.artifacts SET actual_sha256 = declared_sha256,
        state = CASE WHEN $2 THEN 'ACTIVE' ELSE state END,
        activated_at = CASE WHEN $2 THEN statement_timestamp() ELSE activated_at END,
        state_reason = CASE WHEN $2 THEN NULL ELSE state_reason END,
        row_version = row_version + 1, updated_at = statement_timestamp() WHERE id = $1::uuid`,
        [id, activate],
      );
      await this.audit.append(client, {
        action: activate ? 'ARTIFACT_ACTIVATED' : 'ARTIFACT_VERIFIED',
        actorType: 'USER',
        actorUserId: actor.id,
        subjectType: 'ARTIFACT',
        subjectId: id,
        details: { sha256: row.sha256 },
      });
      return summary(
        (await client.query<ArtifactRow>(`${SELECT} WHERE a.id = $1::uuid`, [id])).rows[0]!,
      );
    });
  }
}
