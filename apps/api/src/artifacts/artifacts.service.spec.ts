import { ArtifactCache, ArtifactLoader, type ObjectStorageDriver } from '@scs/storage';
import type { UserSummary } from '@scs/contracts';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PlatformDatabaseService } from '../database/platform-database.service.js';
import type { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { ArtifactsService } from './artifacts.service.js';

const actor = { id: 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa', role: 'ADMIN' } as UserSummary;
const artifact = {
  id: 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb',
  version_identifier: 'candidate-1',
  declared_source: 'local build',
  object_key: 'artifacts/ape-dts',
  sha256: 'a'.repeat(64),
  size_bytes: '1024',
  state: 'ACTIVE',
  state_reason: null,
  verified: true,
  certifications: [],
  relative_prefix: '',
  driver: 'LOCAL',
};
function setup() {
  const query = vi.fn(async (sql: string) => {
    if (sql.includes('FROM ape_dts_platform.storage_profiles'))
      return { rows: [{ id: actor.id, relative_prefix: '' }] };
    if (sql.includes('INSERT INTO ape_dts_platform.stored_objects'))
      return { rows: [{ id: actor.id }] };
    if (sql.includes('FROM ape_dts_platform.artifacts a')) return { rows: [{ ...artifact }] };
    return { rows: [] };
  });
  const release = vi.fn();
  const pool = { query, connect: vi.fn(async () => ({ query, release })) };
  const audit = { append: vi.fn(async () => undefined) };
  const service = new ArtifactsService(
    { requirePool: () => pool } as unknown as PlatformDatabaseService,
    audit as unknown as AuditLedgerService,
    new ArtifactCache('unused-test-cache'),
    {} as ObjectStorageDriver,
  );
  return { service, query, audit };
}
afterEach(() => vi.restoreAllMocks());

describe('artifact registration and activation', () => {
  it('registers a checked binary for immediate use without claiming behavior certification', async () => {
    const close = vi.fn(async () => undefined);
    vi.spyOn(ArtifactLoader.prototype, 'load').mockResolvedValue({
      binding: { close, descriptor: 1, path: '/verified', sha256: artifact.sha256, size: 1024 },
      signature: { mode: 'DISABLED', signerId: null, verified: false },
      staticInspection: {
        architecture: 'x86_64',
        dynamicLibraries: [],
        elfClass: 64,
        format: 'ELF',
        interpreter: null,
      },
    });
    const { service, query } = setup();
    const result = await service.register(actor, {
      versionIdentifier: 'candidate-1',
      declaredSource: 'local build',
      objectKey: artifact.object_key,
      sha256: artifact.sha256,
      sizeBytes: 1024,
    });
    expect(result.state).toBe('ACTIVE');
    expect(result.certifications).toEqual([]);
    expect(close).toHaveBeenCalledOnce();
    expect(
      query.mock.calls.some(([sql]) =>
        sql.includes('INSERT INTO ape_dts_platform.artifact_certifications'),
      ),
    ).toBe(false);
    expect(query.mock.calls.some(([sql]) => sql === 'COMMIT')).toBe(true);
  });

  it('does not persist a registration when binary integrity validation fails', async () => {
    vi.spyOn(ArtifactLoader.prototype, 'load').mockRejectedValue(
      new Error('ARTIFACT_SHA256_MISMATCH'),
    );
    const { service, query, audit } = setup();
    await expect(
      service.register(actor, {
        versionIdentifier: 'bad',
        declaredSource: 'local build',
        objectKey: artifact.object_key,
        sha256: artifact.sha256,
        sizeBytes: 1024,
      }),
    ).rejects.toThrow('ARTIFACT_SHA256_MISMATCH');
    expect(query.mock.calls.some(([sql]) => sql.includes('INSERT'))).toBe(false);
    expect(query.mock.calls.some(([sql]) => sql === 'ROLLBACK')).toBe(true);
    expect(audit.append).not.toHaveBeenCalled();
  });

  it('still refuses use of a quarantined artifact before reading its bytes', async () => {
    const load = vi.spyOn(ArtifactLoader.prototype, 'load');
    const { service, query } = setup();
    query.mockImplementation(async (sql) =>
      sql.includes('FROM ape_dts_platform.artifacts a')
        ? { rows: [{ ...artifact, state: 'QUARANTINED' }] }
        : { rows: [] },
    );
    await expect(service.verify(actor, artifact.id, true)).rejects.toThrow();
    expect(load).not.toHaveBeenCalled();
    expect(
      query.mock.calls.some(([sql]) => sql.includes('UPDATE ape_dts_platform.artifacts')),
    ).toBe(false);
  });
});
