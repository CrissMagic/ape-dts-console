import { copyFile, open, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { ProcessProbeRunner } from './process-probe-runner.js';

const cleanupPaths: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'scs-process-probe-'));
  cleanupPaths.push(directory);
  return directory;
}

async function executableBinding(directory: string) {
  const executablePath = path.join(directory, 'controlled-runtime.exe');
  await copyFile(process.execPath, executablePath);
  const handle = await open(executablePath, 'r');
  return {
    close: async () => {
      await handle.close();
    },
    descriptor: handle.fd,
    path: executablePath,
    sha256: '0'.repeat(64),
    size: (await handle.stat()).size,
  };
}

async function waitForProcessToExit(processId: number): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    try {
      process.kill(processId, 0);
    } catch {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('process ' + processId + ' was still running after the probe timeout');
}

function requireProcessId(value: number | undefined): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error('probe did not emit a valid process ID');
  }
  return value;
}

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map(async (directory) => {
      await rm(directory, {
        force: true,
        maxRetries: 10,
        recursive: true,
        retryDelay: 50,
      });
    }),
  );
});

describe('ProcessProbeRunner', () => {
  it('captures bounded redacted output and preserves the real non-zero exit code', async () => {
    const directory = await temporaryDirectory();
    const script = path.join(directory, 'probe.cjs');
    await writeFile(
      script,
      "process.stdout.write('token=very-secret\\n'); process.stderr.write('failure\\n' + 'x'.repeat(4096)); process.exit(7);",
    );
    const binding = await executableBinding(directory);

    try {
      const result = await new ProcessProbeRunner().run(binding, {
        allowPathFallbackForNonLinuxTests: process.platform !== 'linux',
        arguments: [script],
        environment: {},
        killGraceMilliseconds: 100,
        maxOutputBytesPerStream: 128,
        redact: (value) => value.replaceAll('very-secret', '[REDACTED]'),
        timeoutMilliseconds: 2_000,
        workingDirectory: directory,
      });

      expect(result).toMatchObject({
        exitCode: 7,
        outcome: 'EXITED',
        stderr: '[PROBE_OUTPUT_TRUNCATED]',
        stderrTruncated: true,
        stdoutTruncated: false,
      });
      expect(result.stdout).toContain('[REDACTED]');
      expect(result.stdout).not.toContain('very-secret');
    } finally {
      await binding.close();
    }
  });

  it('terminates a timed-out process tree without leaving its parent or child running', async () => {
    const directory = await temporaryDirectory();
    const script = path.join(directory, 'hang.cjs');
    await writeFile(
      script,
      [
        "const { spawn } = require('node:child_process');",
        "const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });",
        "process.stdout.write([process.pid, child.pid].join(',') + '\\n');",
        'setInterval(() => {}, 1000);',
      ].join(' '),
    );
    const binding = await executableBinding(directory);

    try {
      const result = await new ProcessProbeRunner().run(binding, {
        allowPathFallbackForNonLinuxTests: process.platform !== 'linux',
        arguments: [script],
        environment: {},
        killGraceMilliseconds: 100,
        maxOutputBytesPerStream: 1_024,
        redact: (value) => value,
        timeoutMilliseconds: 1_000,
        workingDirectory: directory,
      });
      const processIds = result.stdout.trim().split(',').map(Number);
      const parentProcessId = requireProcessId(processIds[0]);
      const childProcessId = requireProcessId(processIds[1]);

      expect(result.outcome).toBe('TIMED_OUT');
      await waitForProcessToExit(parentProcessId);
      await waitForProcessToExit(childProcessId);
    } finally {
      await binding.close();
    }
  });

  it('fails closed when evidence redaction throws', async () => {
    const directory = await temporaryDirectory();
    const script = path.join(directory, 'output.cjs');
    await writeFile(script, "process.stdout.write('sensitive-output');");
    const binding = await executableBinding(directory);

    try {
      const result = await new ProcessProbeRunner().run(binding, {
        allowPathFallbackForNonLinuxTests: process.platform !== 'linux',
        arguments: [script],
        environment: {},
        killGraceMilliseconds: 100,
        maxOutputBytesPerStream: 1_024,
        redact: () => {
          throw new Error('redactor failed');
        },
        timeoutMilliseconds: 2_000,
        workingDirectory: directory,
      });

      expect(result.redactionFailed).toBe(true);
      expect(result.stdout).toBe('[PROBE_OUTPUT_REDACTION_FAILED]');
      expect(result.stdout).not.toContain('sensitive-output');
    } finally {
      await binding.close();
    }
  });
});
