import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { chmod, mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import { createGzip } from 'node:zlib';
import tar from 'tar-stream';
import { ArtifactFiles } from './artifact-files.js';
import type { CertificationEvidence } from './certification-persistence.js';

const executeFile = promisify(execFile);
export const certificationSuites = {
  pg: 'tests/integration/phase0/pg-snapshot-cdc.integration.spec.ts',
  kafka: 'tests/integration/phase0/kafka-raw.integration.spec.ts',
} as const;
const basicSuite = 'tests/integration/phase0/artifact-behavior.integration.spec.ts';

async function sha256File(file: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest('hex');
}

function testDetails(value: unknown): unknown[] {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('testResults' in value) ||
    !Array.isArray(value.testResults)
  )
    return [];
  return value.testResults
    .flatMap((suite: unknown) => {
      if (
        typeof suite !== 'object' ||
        suite === null ||
        !('assertionResults' in suite) ||
        !Array.isArray(suite.assertionResults)
      )
        return [];
      return suite.assertionResults.slice(0, 5).map((item: unknown) => {
        if (typeof item !== 'object' || item === null) return { status: 'invalid' };
        const assertion = item as Record<string, unknown>;
        return {
          name: String(assertion.fullName ?? '').slice(0, 512),
          status: String(assertion.status ?? ''),
          failures: Array.isArray(assertion.failureMessages)
            ? assertion.failureMessages.slice(0, 2).map((message: unknown) =>
                String(message)
                  .replace(/:\/\/[^@\s]+@/gu, '://[REDACTED]@')
                  .slice(0, 2000),
              )
            : [],
        };
      });
    })
    .slice(0, 5);
}

export function validateCertificationReport(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) return false;
  const report = value as Record<string, unknown>;
  if (
    report.success !== true ||
    report.numTotalTests !== 5 ||
    report.numPassedTests !== 5 ||
    report.numFailedTests !== 0 ||
    report.numPendingTests !== 0 ||
    !Array.isArray(report.testResults)
  )
    return false;
  const assertions = report.testResults.flatMap((suite: unknown) => {
    if (
      typeof suite !== 'object' ||
      suite === null ||
      !('assertionResults' in suite) ||
      !Array.isArray(suite.assertionResults)
    )
      return [];
    return suite.assertionResults as unknown[];
  });
  return (
    assertions.length === 5 &&
    assertions.every(
      (assertion) =>
        typeof assertion === 'object' &&
        assertion !== null &&
        'status' in assertion &&
        assertion.status === 'passed',
    )
  );
}

/** No supplied reports: this function produces a private report by running fixed repository tests. */
export async function runCertificationSuite(options: {
  root: string;
  candidate: string;
  sha256: string;
  runtimeImage: string;
  suite: keyof typeof certificationSuites;
}): Promise<CertificationEvidence> {
  const directory = await mkdtemp(path.join(tmpdir(), 'scs-certification-'));
  try {
    await stat(options.candidate).catch(() => {
      throw new Error('CERTIFICATION_CANDIDATE_UNAVAILABLE');
    });
    const image = (
      await executeFile(
        'docker',
        ['image', 'inspect', '--format', '{{.Id}}', options.runtimeImage],
        { timeout: 30_000, maxBuffer: 1024 * 1024 },
      ).catch(() => {
        throw new Error('CERTIFICATION_IMAGE_UNAVAILABLE');
      })
    ).stdout.trim();
    if (!/^sha256:[a-f0-9]{64}$/u.test(image)) throw new Error('CERTIFICATION_IMAGE_REQUIRED');
    let evidence: CertificationEvidence | undefined;
    await new ArtifactFiles(directory).upload(
      '',
      path.basename(options.candidate),
      createReadStream(options.candidate),
      async (binary) => {
        if (binary.sha256 !== options.sha256)
          throw new Error('CERTIFICATION_CANDIDATE_SHA_MISMATCH');
        const archive = path.join(directory, 'candidate.tar.gz');
        const pack = tar.pack();
        const archiveWrite = pipeline(
          pack,
          createGzip(),
          createWriteStream(archive, { flags: 'wx', mode: 0o600 }),
        );
        await pipeline(
          createReadStream(binary.path),
          pack.entry({ name: 'ape-dts', size: binary.sizeBytes, mode: 0o555 }),
        );
        pack.finalize();
        await archiveWrite;
        // Public binary bytes only; the image's non-root user must read this bind mount.
        await chmod(archive, 0o444);
        const archiveHash = await sha256File(archive);
        const reportFile = path.join(directory, 'result.json');
        const suite = certificationSuites[options.suite];
        const testSourceHash = createHash('sha256')
          .update(await readFile(path.join(options.root, basicSuite)))
          .update(await readFile(path.join(options.root, suite)))
          .digest('hex');
        let exitCode = 0;
        const testEnvironment = { ...process.env };
        delete testEnvironment.SCS_CERTIFICATION_DATABASE_URL;
        try {
          await executeFile(
            process.execPath,
            [
              path.join(options.root, 'node_modules/vitest/vitest.mjs'),
              'run',
              '--config',
              'vitest.integration.config.ts',
              '--no-file-parallelism',
              '--reporter=json',
              `--outputFile=${reportFile}`,
              basicSuite,
              suite,
            ],
            {
              cwd: options.root,
              timeout: 45 * 60_000,
              maxBuffer: 8 * 1024 * 1024,
              env: {
                ...testEnvironment,
                SCS_PHASE0_APE_DTS_ARCHIVE: archive,
                SCS_PHASE0_RUNTIME_IMAGE: image,
              },
            },
          );
        } catch {
          exitCode = 1;
        }
        const report = await readFile(reportFile, 'utf8').catch(() => '');
        let reportValid = false;
        let details: unknown[] = [];
        try {
          const parsed: unknown = JSON.parse(report);
          reportValid = validateCertificationReport(parsed);
          details = testDetails(parsed);
        } catch {
          /* Invalid output never certifies. */
        }
        const unchanged = archiveHash === (await sha256File(archive));
        evidence = {
          passed: exitCode === 0 && reportValid && unchanged,
          exitCode,
          ...(!unchanged
            ? { failureCode: 'CERTIFICATION_ARCHIVE_CHANGED' }
            : exitCode !== 0
              ? { failureCode: 'BEHAVIOR_SUITE_FAILED' }
              : !reportValid
                ? { failureCode: 'CERTIFICATION_REPORT_INVALID' }
                : {}),
          summary: {
            suite,
            runtimeImage: image,
            binarySha256: binary.sha256,
            archiveSha256: archiveHash,
            testSourceSha256: testSourceHash,
            reportSha256: createHash('sha256').update(report).digest('hex'),
            expectedTests: 5,
            reportValid,
            archiveUnchanged: unchanged,
            tests: details,
          },
        };
      },
    );
    if (evidence === undefined) throw new Error('CERTIFICATION_EXECUTION_FAILED');
    return evidence;
  } finally {
    // Only remove the unique directory allocated by this invocation, never candidate paths.
    await rm(directory, { recursive: true, force: true });
  }
}
