import 'reflect-metadata';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Pool } from 'pg';
import { z } from 'zod';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { certifyArtifact } from './certification-persistence.js';
import { runCertificationSuite } from './certification-runner.js';

const optionsSchema = z
  .object({
    '--artifact-id': z.uuid(),
    '--sha256': z.string().regex(/^[a-f0-9]{64}$/u),
    '--candidate': z.string().min(1),
    '--runtime-image': z.string().min(1),
    '--suite': z.enum(['pg', 'kafka']),
  })
  .strict();
export function parseCertificationArguments(args: readonly string[]) {
  const options: Record<string, string> = {};
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index];
    const value = args[index + 1];
    if (key === undefined || value === undefined || Object.hasOwn(options, key))
      throw new Error('INVALID_ARGUMENTS');
    options[key] = value;
  }
  const parsed = optionsSchema.safeParse(options);
  if (!parsed.success) throw new Error('INVALID_ARGUMENTS');
  return parsed.data;
}
export async function runCertificationCli(args: readonly string[]): Promise<void> {
  if (args.length === 1 && args[0] === '--help') {
    process.stdout.write(
      'Deployment administrator only. Requires Docker, built packages and platform DB access.\n' +
        'node tools/certify-artifact.mjs --artifact-id UUID --sha256 ELF_SHA256 --candidate FILE --runtime-image IMAGE --suite pg|kafka\n' +
        'Environment: SCS_CERTIFICATION_DATABASE_URL, SCS_PLATFORM_VERSION (must match deployment).\n' +
        'Runs isolated Phase0 tests, records results as admin, never activates artifacts. No report import.\n',
    );
    return;
  }
  const options = parseCertificationArguments(args);
  const connectionString = process.env.SCS_CERTIFICATION_DATABASE_URL;
  const platformVersion = process.env.SCS_PLATFORM_VERSION?.trim();
  if (!connectionString || !platformVersion) throw new Error('CERTIFICATION_ENVIRONMENT_REQUIRED');
  const root = fileURLToPath(new URL('../../../../', import.meta.url));
  const pool = new Pool({
    connectionString,
    max: 1,
    connectionTimeoutMillis: 10_000,
    statement_timeout: 10_000,
    query_timeout: 10_000,
    application_name: 'scs-deployment-certification',
  });
  try {
    const client = await pool.connect();
    try {
      const result = await certifyArtifact(
        client,
        new AuditLedgerService(new PlatformDatabaseService()),
        {
          artifactId: options['--artifact-id'],
          sha256: options['--sha256'],
          platformVersion,
          scenarios:
            options['--suite'] === 'pg'
              ? ['PG_SNAPSHOT_TO_KAFKA', 'PG_CDC_TO_KAFKA']
              : ['KAFKA_RAW_FORWARD'],
        },
        async () =>
          await runCertificationSuite({
            root,
            candidate: path.resolve(options['--candidate']),
            sha256: options['--sha256'],
            runtimeImage: options['--runtime-image'],
            suite: options['--suite'],
          }),
      );
      process.stdout.write(`${JSON.stringify(result)}\n`);
      if (!result.passed) process.exitCode = 1;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

export function reportCertificationFailure(error: unknown): void {
  const allowed = new Set([
    'INVALID_ARGUMENTS',
    'CERTIFICATION_ENVIRONMENT_REQUIRED',
    'ARTIFACT_CERTIFICATION_BUSY',
    'ACTIVE_ADMIN_REQUIRED',
    'ARTIFACT_INACTIVE_SHA_BINDING_REQUIRED',
  ]);
  process.stderr.write(
    `${error instanceof Error && allowed.has(error.message) ? error.message : 'CERTIFICATION_FAILED'}\n`,
  );
  process.exitCode = 1;
}
if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void runCertificationCli(process.argv.slice(2)).catch(reportCertificationFailure);
}
