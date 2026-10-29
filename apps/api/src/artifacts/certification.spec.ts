import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { parseCertificationArguments } from './certification-cli.js';
import { certifyArtifact } from './certification-persistence.js';
import { validateCertificationReport } from './certification-runner.js';
import { safeCertificationFailureCode } from './certification-errors.js';

const identity = {
  artifactId: '11111111-1111-4111-8111-111111111111',
  sha256: 'a'.repeat(64),
  platformVersion: 'test-version',
  scenarios: ['PG_CDC_TO_KAFKA'] as const,
};
function fixture(valid = true) {
  const state = { valid, activeAdmin: true, acquired: true };
  const statements: { sql: string; values: unknown[] }[] = [];
  const query = vi.fn(async (sql: string, values: unknown[] = []) => {
    statements.push({ sql, values });
    if (sql.includes('pg_try_advisory_lock')) return { rows: [{ acquired: state.acquired }] };
    if (sql.includes('FROM ape_dts_platform.users'))
      return { rows: state.activeAdmin ? [{ id: '22222222-2222-4222-8222-222222222222' }] : [] };
    if (sql.includes('AS valid')) return { rows: [{ valid: state.valid }] };
    return { rows: [], rowCount: 1 };
  });
  return {
    state,
    client: { query } as unknown as PoolClient,
    statements,
    audit: { append: vi.fn(async () => '1') },
  };
}
describe('deployment behavior certification', () => {
  it('reports invalid CLI values without echoing candidate arguments', () => {
    expect(() => parseCertificationArguments([])).toThrow('INVALID_ARGUMENTS');
    expect(() => parseCertificationArguments(['--artifact-id', 'bad-secret-input'])).toThrow(
      'INVALID_ARGUMENTS',
    );
  });
  it.each([
    'CERTIFICATION_CANDIDATE_SHA_MISMATCH',
    'CERTIFICATION_IMAGE_UNAVAILABLE',
    'CERTIFICATION_CANDIDATE_UNAVAILABLE',
    'ARTIFACT_ARCHIVE_CLI_ONLY',
  ])('preserves safe runner failure %s in persisted and CLI results', async (code) => {
    const f = fixture();
    const result = await certifyArtifact(f.client, f.audit, identity, async () => {
      throw new Error(code);
    });
    expect(result).toMatchObject({ passed: false, failureCode: code });
    expect(
      f.statements.find(({ sql }) => sql.startsWith('UPDATE ape_dts_platform.artifact_probe_runs'))
        ?.values[3],
    ).toBe(code);
  });
  it('never exposes raw credential-bearing execution exceptions', async () => {
    const f = fixture();
    const raw = new Error('postgres://admin:private-password@localhost/test');
    expect(safeCertificationFailureCode(raw)).toBe('CERTIFICATION_EXECUTION_FAILED');
    const result = await certifyArtifact(f.client, f.audit, identity, async () => {
      throw raw;
    });
    expect(result.failureCode).toBe('CERTIFICATION_EXECUTION_FAILED');
    expect(JSON.stringify(f.statements)).not.toContain('private-password');
    expect(JSON.stringify(result)).not.toContain('private-password');
  });
  it('executes behavior before passing a matching inactive artifact without activating it', async () => {
    const f = fixture();
    const execute = vi.fn(async () => {
      expect(f.statements.some(({ sql }) => sql.includes("'RUNNING'"))).toBe(true);
      expect(
        f.statements.filter(({ sql }) =>
          sql.startsWith('UPDATE ape_dts_platform.artifact_probe_runs'),
        ),
      ).toHaveLength(0);
      return { passed: true, exitCode: 0, summary: { binarySha256: identity.sha256 } };
    });
    const result = await certifyArtifact(f.client, f.audit, identity, execute);
    expect(result.passed).toBe(true);
    expect(execute).toHaveBeenCalledOnce();
    expect(
      f.statements.find(({ sql }) => sql.startsWith('UPDATE ape_dts_platform.artifact_probe_runs'))
        ?.values[1],
    ).toBe('PASSED');
    expect(
      f.statements.some(({ sql }) => sql.startsWith('UPDATE ape_dts_platform.artifacts ')),
    ).toBe(false);
    expect(f.audit.append).toHaveBeenLastCalledWith(
      f.client,
      expect.objectContaining({ actorType: 'USER', action: 'ARTIFACT_CERTIFICATION_PASSED' }),
    );
  });
  it.each(['failure', 'exception', 'nonzero'] as const)(
    'records %s without certification pass',
    async (mode) => {
      const f = fixture();
      const result = await certifyArtifact(f.client, f.audit, identity, async () => {
        if (mode === 'exception') throw new Error('test process failed');
        return { passed: mode === 'nonzero', exitCode: 1, summary: {} };
      });
      expect(result.passed).toBe(false);
      expect(
        f.statements.find(({ sql }) =>
          sql.startsWith('UPDATE ape_dts_platform.artifact_certifications'),
        )?.values[5],
      ).toBe('FAILED');
    },
  );
  it('never executes or records a pass for a different SHA or active artifact', async () => {
    const f = fixture(false);
    const execute = vi.fn();
    await expect(certifyArtifact(f.client, f.audit, identity, execute)).rejects.toThrow(
      'ARTIFACT_INACTIVE_SHA_BINDING_REQUIRED',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(f.statements.some(({ sql }) => sql.startsWith('INSERT'))).toBe(false);
  });
  it('rejects passed/report/test-command flags rather than importing evidence', () => {
    const args = [
      '--artifact-id',
      identity.artifactId,
      '--sha256',
      identity.sha256,
      '--candidate',
      'engine.tar.gz',
      '--runtime-image',
      'runtime:test',
      '--suite',
      'pg',
    ];
    expect(parseCertificationArguments(args)['--suite']).toBe('pg');
    for (const flag of ['--passed', '--report', '--test-command']) {
      expect(() => parseCertificationArguments([...args, flag, 'true'])).toThrow();
    }
  });
  it('records failure when the artifact binding changes during the actual suite', async () => {
    const f = fixture();
    const result = await certifyArtifact(f.client, f.audit, identity, async () => {
      f.state.valid = false;
      return { passed: true, exitCode: 0, summary: {} };
    });
    expect(result.passed).toBe(false);
    expect(
      f.statements.find(({ sql }) => sql.startsWith('UPDATE ape_dts_platform.artifact_probe_runs'))
        ?.values[3],
    ).toBe('ARTIFACT_BINDING_CHANGED');
  });
  it('does not grant certification after the executing administrator is disabled', async () => {
    const f = fixture();
    const result = await certifyArtifact(f.client, f.audit, identity, async () => {
      f.state.activeAdmin = false;
      return { passed: true, exitCode: 0, summary: {} };
    });
    expect(result.passed).toBe(false);
  });
  it('does not run a second concurrent certification for the same artifact', async () => {
    const f = fixture();
    f.state.acquired = false;
    const execute = vi.fn();
    await expect(certifyArtifact(f.client, f.audit, identity, execute)).rejects.toThrow(
      'ARTIFACT_CERTIFICATION_BUSY',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(f.audit.append).not.toHaveBeenCalled();
  });
  it('requires all five real test results with no failed or skipped assertions', () => {
    const report = {
      success: true,
      numTotalTests: 5,
      numPassedTests: 5,
      numFailedTests: 0,
      numPendingTests: 0,
      testResults: [{ assertionResults: Array.from({ length: 5 }, () => ({ status: 'passed' })) }],
    };
    expect(validateCertificationReport(report)).toBe(true);
    expect(validateCertificationReport({ passed: true })).toBe(false);
    expect(validateCertificationReport({ ...report, numPendingTests: 1 })).toBe(false);
    expect(validateCertificationReport({ ...report, numTotalTests: 4 })).toBe(false);
    expect(validateCertificationReport({ ...report, testResults: [] })).toBe(false);
  });
});
