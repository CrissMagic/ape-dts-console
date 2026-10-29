import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const repositoryRoot = fileURLToPath(new URL('../..', import.meta.url));
const uniqueSuffix = `${process.pid}-${Date.now()}`;
const imageName = `scs-data-sync-platform:test-${uniqueSuffix}`;
const containerName = `scs-data-sync-platform-test-${uniqueSuffix}`;

let containerCreated = false;
let imageCreated = false;

function docker(arguments_: string[], timeout = 120_000): string {
  return execFileSync('docker', arguments_, {
    cwd: repositoryRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout,
  }).trim();
}

async function waitForLiveEndpoint(port: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/health/live`);
      if (response.ok) {
        return;
      }
      lastError = new Error(`liveness returned HTTP ${response.status}`);
    } catch (error) {
      lastError = error;
    }

    await new Promise((resolve) => setTimeout(resolve, 250));
  }

  throw lastError instanceof Error ? lastError : new Error('liveness endpoint did not start');
}

beforeAll(() => {
  docker(['build', '--tag', imageName, '.'], 600_000);
  imageCreated = true;
});

afterAll(() => {
  if (containerCreated) {
    try {
      docker(['rm', '--force', containerName]);
    } catch (error) {
      process.stderr.write(`failed to remove test container: ${String(error)}\n`);
    }
  }

  if (imageCreated) {
    try {
      docker(['image', 'rm', '--force', imageName]);
    } catch (error) {
      process.stderr.write(`failed to remove test image: ${String(error)}\n`);
    }
  }
});

describe('production Docker image', () => {
  it('runs as the fixed non-root user and distinguishes liveness from readiness', async () => {
    expect(docker(['image', 'inspect', '--format', '{{.Config.User}}', imageName])).toBe(
      '10001:10001',
    );
    expect(
      JSON.parse(
        docker(['image', 'inspect', '--format', '{{json .Config.Entrypoint}}', imageName]),
      ),
    ).toEqual(['/usr/bin/tini', '--', '/app/docker/entrypoint/entrypoint.sh']);
    expect(
      docker([
        'run',
        '--rm',
        '--entrypoint',
        '/usr/bin/test',
        imageName,
        '-r',
        '/app/node_modules/@scs/database/migrations/manifest.json',
      ]),
    ).toBe('');
    for (const packagedFile of [
      '/app/node_modules/@scs/storage/dist/index.js',
      '/app/node_modules/@scs/config-model/dist/index.js',
      '/app/node_modules/@scs/artifact-probe/dist/index.js',
    ]) {
      expect(
        docker(['run', '--rm', '--entrypoint', '/usr/bin/test', imageName, '-r', packagedFile]),
      ).toBe('');
    }

    docker(['run', '--detach', '--name', containerName, '--publish', '127.0.0.1::3000', imageName]);
    containerCreated = true;

    const portOutput = docker(['port', containerName, '3000/tcp']);
    const port = Number(portOutput.slice(portOutput.lastIndexOf(':') + 1));
    expect(Number.isSafeInteger(port)).toBe(true);

    await waitForLiveEndpoint(port);

    const readiness = await fetch(`http://127.0.0.1:${port}/api/health/ready`);
    expect(readiness.status).toBe(503);
    await expect(readiness.json()).resolves.toMatchObject({
      code: 'PLATFORM_NOT_READY',
      phase: 'SEALED',
      status: 'not_ready',
    });

    expect(docker(['exec', containerName, 'id', '-u'])).toBe('10001');
    docker(['stop', '--time', '15', containerName], 30_000);
    expect(docker(['inspect', '--format', '{{.State.ExitCode}}', containerName])).toBe('0');
  });
});
