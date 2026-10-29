import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const repositoryRoot = fileURLToPath(new URL('../../..', import.meta.url));
const composeFile = fileURLToPath(new URL('../../../docker/compose.phase0.yml', import.meta.url));
const composeProjectName = `scs-phase0-${process.pid}-${Date.now()}`;

let environmentStarted = false;

function compose(arguments_: readonly string[], timeout = 180_000): string {
  return execFileSync(
    'docker',
    ['compose', '--project-name', composeProjectName, '--file', composeFile, ...arguments_],
    {
      cwd: repositoryRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout,
    },
  ).trim();
}

beforeAll(() => {
  compose(['up', '--detach', '--wait', '--wait-timeout', '120']);
  environmentStarted = true;
});

afterAll(() => {
  if (!environmentStarted) {
    return;
  }
  compose(['down', '--volumes', '--remove-orphans']);
  environmentStarted = false;
});

describe('Phase 0 external services', () => {
  it('starts isolated PostgreSQL 12 with logical replication and a reachable Kafka broker', () => {
    const postgresSettings = compose([
      'exec',
      '--no-TTY',
      'postgres',
      'psql',
      '--username=phase0',
      '--dbname=phase0',
      '--tuples-only',
      '--no-align',
      '--command',
      "SELECT current_setting('wal_level') || '|' || current_setting('max_replication_slots') || '|' || current_setting('max_wal_senders');",
    ]);
    expect(postgresSettings).toBe('logical|16|16');

    expect(
      compose([
        'exec',
        '--no-TTY',
        'kafka',
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server',
        'kafka:9092',
        '--list',
      ]),
    ).toBe('');
  });
});
