import { execFileSync } from 'node:child_process';
import { stat } from 'node:fs/promises';
import path from 'node:path';

import { afterEach, beforeAll, describe, expect, it } from 'vitest';

const candidateArchive = process.env.SCS_PHASE0_APE_DTS_ARCHIVE;
const runtimeImage = process.env.SCS_PHASE0_RUNTIME_IMAGE ?? 'centos:7';

if (candidateArchive === undefined || candidateArchive.trim().length === 0) {
  throw new Error('SCS_PHASE0_APE_DTS_ARCHIVE is required for Phase 0 certification');
}

let candidateContainerName: string | undefined;

function docker(arguments_: readonly string[], timeout = 60_000): string {
  return execFileSync('docker', arguments_, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

beforeAll(async () => {
  await stat(candidateArchive);
});

afterEach(() => {
  if (candidateContainerName === undefined) {
    return;
  }
  docker(['rm', '--force', candidateContainerName]);
  candidateContainerName = undefined;
});

describe('Phase 0 artifact behavior', () => {
  it('executes the supplied artifact on the deployment image with required dynamic libraries under 4C/8G', () => {
    const containerName = `scs-phase0-artifact-${process.pid}-${Date.now()}`;
    candidateContainerName = containerName;
    docker([
      'run',
      '--detach',
      '--name',
      containerName,
      '--cpus',
      '4',
      '--memory',
      '8g',
      '--mount',
      `type=bind,source=${path.resolve(candidateArchive)},target=/artifact/candidate.tar.gz,readonly`,
      '--entrypoint',
      '/bin/bash',
      runtimeImage,
      '-lc',
      'set -euo pipefail; mkdir -p /tmp/ape-dts; tar -xzf /artifact/candidate.tar.gz -C /tmp/ape-dts; chmod 0555 /tmp/ape-dts/ape-dts; /tmp/ape-dts/ape-dts --version; ldd /tmp/ape-dts/ape-dts',
    ]);

    expect(docker(['wait', containerName])).toBe('0');
    const output = docker(['logs', containerName]);
    expect(output).toMatch(/(?:dt-main|ape[-_]dts)\s+\d+\.\d+/u);
    expect(output).not.toContain('not found');

    const inspection = JSON.parse(docker(['inspect', containerName])) as Array<{
      HostConfig: { Memory: number; NanoCpus: number };
    }>;
    expect(inspection[0]?.HostConfig).toMatchObject({
      Memory: 8 * 1024 * 1024 * 1024,
      NanoCpus: 4 * 1_000_000_000,
    });
  });
});
