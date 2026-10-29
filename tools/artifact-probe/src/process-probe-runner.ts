import { execFile, spawn, type ChildProcess, type StdioOptions } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { link, rm } from 'node:fs/promises';
import path from 'node:path';

import type { ArtifactExecutionBinding } from '@scs/storage';

export interface ProcessProbeOptions {
  readonly allowPathFallbackForNonLinuxTests?: boolean;
  readonly arguments: readonly string[];
  readonly environment: Readonly<Record<string, string>>;
  readonly killGraceMilliseconds: number;
  readonly maxOutputBytesPerStream: number;
  readonly redact: (value: string) => string;
  readonly timeoutMilliseconds: number;
  readonly workingDirectory: string;
}

export interface ProcessProbeResult {
  readonly durationMilliseconds: number;
  readonly exitCode: number | null;
  readonly outcome: 'EXITED' | 'TIMED_OUT';
  readonly redactionFailed: boolean;
  readonly signal: NodeJS.Signals | null;
  readonly stderr: string;
  readonly stderrTruncated: boolean;
  readonly stdout: string;
  readonly stdoutTruncated: boolean;
}

class BoundedOutput {
  private readonly chunks: Buffer[] = [];
  private storedBytes = 0;
  truncated = false;

  constructor(private readonly limit: number) {}

  add(chunk: Buffer): void {
    const remaining = this.limit - this.storedBytes;
    if (remaining > 0) {
      const retained = chunk.subarray(0, remaining);
      this.chunks.push(Buffer.from(retained));
      this.storedBytes += retained.length;
    }
    if (chunk.length > remaining) {
      this.truncated = true;
    }
  }

  redact(redactor: (value: string) => string): { failed: boolean; value: string } {
    if (this.truncated) {
      return { failed: false, value: '[PROBE_OUTPUT_TRUNCATED]' };
    }
    const raw = Buffer.concat(this.chunks).toString('utf8');
    if (raw.length === 0) {
      return { failed: false, value: '' };
    }
    try {
      return { failed: false, value: redactor(raw) };
    } catch {
      return { failed: true, value: '[PROBE_OUTPUT_REDACTION_FAILED]' };
    }
  }
}

function positiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function validateOptions(options: ProcessProbeOptions): void {
  if (
    !positiveSafeInteger(options.timeoutMilliseconds) ||
    !positiveSafeInteger(options.killGraceMilliseconds) ||
    !positiveSafeInteger(options.maxOutputBytesPerStream)
  ) {
    throw new Error('PROBE_LIMIT_INVALID');
  }
  if (
    !path.isAbsolute(options.workingDirectory) ||
    options.workingDirectory.includes('\r') ||
    options.workingDirectory.includes('\n') ||
    options.workingDirectory.includes('\u0000')
  ) {
    throw new Error('PROBE_WORKING_DIRECTORY_INVALID');
  }
  if (options.arguments.some((argument) => argument.includes('\u0000'))) {
    throw new Error('PROBE_ARGUMENT_INVALID');
  }
  for (const [key, value] of Object.entries(options.environment)) {
    if (
      key.length === 0 ||
      key.includes('=') ||
      key.includes('\u0000') ||
      value.includes('\u0000')
    ) {
      throw new Error('PROBE_ENVIRONMENT_INVALID');
    }
  }
}

function taskkill(processId: number, force: boolean): Promise<boolean> {
  return new Promise((resolve) => {
    const arguments_ = ['/PID', String(processId), '/T'];
    if (force) {
      arguments_.push('/F');
    }
    execFile('taskkill.exe', arguments_, { windowsHide: true }, (error) => resolve(error === null));
  });
}

async function terminateProcessTree(
  child: ChildProcess,
  signal: Extract<NodeJS.Signals, 'SIGKILL' | 'SIGTERM'>,
): Promise<void> {
  if (child.pid === undefined) {
    return;
  }
  if (process.platform === 'win32') {
    const processTreeTerminated = await taskkill(child.pid, signal === 'SIGKILL');
    if (!processTreeTerminated) {
      child.kill(signal);
    }
    return;
  }
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) {
      child.kill(signal);
    }
  }
}

export class ProcessProbeRunner {
  async run(
    binding: ArtifactExecutionBinding,
    options: ProcessProbeOptions,
  ): Promise<ProcessProbeResult> {
    validateOptions(options);
    const linuxDescriptorBinding = process.platform === 'linux';
    if (!linuxDescriptorBinding && options.allowPathFallbackForNonLinuxTests !== true) {
      throw new Error('NON_LINUX_PATH_FALLBACK_FORBIDDEN');
    }
    let executablePath = linuxDescriptorBinding ? '/proc/self/fd/3' : binding.path;
    let windowsTestHardLink: string | undefined;
    if (process.platform === 'win32') {
      windowsTestHardLink = `${binding.path}.${randomUUID()}.probe.exe`;
      await link(binding.path, windowsTestHardLink);
      executablePath = windowsTestHardLink;
    }
    const stdio: StdioOptions = linuxDescriptorBinding
      ? ['ignore', 'pipe', 'pipe', binding.descriptor]
      : ['ignore', 'pipe', 'pipe'];
    const startedAt = Date.now();
    const child: ChildProcess = spawn(executablePath, [...options.arguments], {
      cwd: options.workingDirectory,
      detached: process.platform !== 'win32',
      env: { ...options.environment },
      stdio,
      windowsHide: true,
    });
    const stdout = new BoundedOutput(options.maxOutputBytesPerStream);
    const stderr = new BoundedOutput(options.maxOutputBytesPerStream);
    child.stdout?.on('data', (chunk: Buffer) => stdout.add(chunk));
    child.stderr?.on('data', (chunk: Buffer) => stderr.add(chunk));

    let timedOut = false;
    let childClosed = false;
    let forceKillTimer: NodeJS.Timeout | undefined;
    const timeout = setTimeout(() => {
      timedOut = true;
      void terminateProcessTree(child, 'SIGTERM').finally(() => {
        if (childClosed) {
          return;
        }
        forceKillTimer = setTimeout(() => {
          if (!childClosed) {
            void terminateProcessTree(child, 'SIGKILL');
          }
        }, options.killGraceMilliseconds);
        forceKillTimer.unref();
      });
    }, options.timeoutMilliseconds);
    timeout.unref();

    const closed = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        child.once('error', reject);
        child.once('close', (code: number | null, signal: NodeJS.Signals | null) => {
          childClosed = true;
          resolve({ code, signal });
        });
      },
    ).finally(async () => {
      childClosed = true;
      clearTimeout(timeout);
      if (forceKillTimer !== undefined) {
        clearTimeout(forceKillTimer);
      }
      if (windowsTestHardLink !== undefined) {
        await rm(windowsTestHardLink, {
          force: true,
          maxRetries: 5,
          recursive: true,
          retryDelay: 50,
        });
      }
    });
    const redactedStdout = stdout.redact(options.redact);
    const redactedStderr = stderr.redact(options.redact);
    return {
      durationMilliseconds: Date.now() - startedAt,
      exitCode: closed.code,
      outcome: timedOut ? 'TIMED_OUT' : 'EXITED',
      redactionFailed: redactedStdout.failed || redactedStderr.failed,
      signal: closed.signal,
      stderr: redactedStderr.value,
      stderrTruncated: stderr.truncated,
      stdout: redactedStdout.value,
      stdoutTruncated: stdout.truncated,
    };
  }
}
