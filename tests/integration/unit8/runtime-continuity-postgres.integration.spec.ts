import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { PostgresDataSourceAdapter } from '../../../apps/api/src/data-sources/adapters/postgres-data-source.adapter.js';

const container = process.env.SCS_TEST_POSTGRES_CONTAINER ?? 'postgresql-12';
const suffix = `${process.pid}_${Date.now()}`;
const role = `scs_cont_${suffix}`;
const database = `scs_cont_${suffix}`;
const slot = `scs_cont_${process.pid}`;
const password = randomBytes(24).toString('base64url');
let port = 0;

function docker(arguments_: readonly string[], input?: string): string {
  return execFileSync('docker', [...arguments_], {
    encoding: 'utf8',
    input,
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 120_000,
  }).trim();
}

function administer(sql: string, targetDatabase = 'postgres'): void {
  docker(
    [
      'exec',
      '--interactive',
      container,
      'psql',
      '--username',
      'postgres',
      '--dbname',
      targetDatabase,
      '--set',
      'ON_ERROR_STOP=1',
    ],
    sql,
  );
}

describe('PostgreSQL runtime continuity producer', () => {
  beforeAll(() => {
    expect(docker(['inspect', '--format', '{{.State.Running}}', container])).toBe('true');
    const binding = docker(['port', container, '5432/tcp']);
    port = Number(binding.slice(binding.lastIndexOf(':') + 1));
    administer(
      `CREATE ROLE "${role}" LOGIN REPLICATION PASSWORD '${password}'; GRANT pg_monitor TO "${role}"; CREATE DATABASE "${database}" OWNER "${role}";`,
    );
    administer(
      `SELECT * FROM pg_create_logical_replication_slot('${slot}', 'test_decoding');`,
      database,
    );
  });

  afterAll(() => {
    try {
      administer(
        `SELECT pg_drop_replication_slot('${slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='${slot}');`,
        database,
      );
    } catch (error) {
      process.stderr.write(`failed to clean up continuity slot: ${String(error)}\n`);
    }
    try {
      administer(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${database}' AND pid <> pg_backend_pid(); DROP DATABASE IF EXISTS "${database}"; DROP ROLE IF EXISTS "${role}";`,
      );
    } catch (error) {
      process.stderr.write(`failed to clean up continuity database: ${String(error)}\n`);
    }
  });

  it('reads real system, database, slot, plugin and confirmed LSN facts', async () => {
    const fact = await new PostgresDataSourceAdapter().readPostgresContinuity(
      {
        configuration: {
          database,
          host: '127.0.0.1',
          port,
          type: 'POSTGRES',
          username: role,
          tls: { mode: 'DISABLE', riskAcknowledged: true },
        },
        password,
      },
      slot,
    );
    expect(fact).toMatchObject({
      databaseName: database,
      slotActive: false,
      slotName: slot,
      slotPlugin: 'test_decoding',
    });
    expect(fact.databaseOid).toMatch(/^\d+$/u);
    expect(fact.systemIdentifier).toMatch(/^\d+$/u);
    expect(fact.confirmedFlushLsn).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/u);
  });
});
