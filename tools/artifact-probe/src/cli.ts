#!/usr/bin/env node
import { ArtifactCache } from '@scs/storage';

import { ProcessProbeRunner } from './process-probe-runner.js';

interface CliOptions {
  readonly arguments: readonly string[];
  readonly cacheRoot: string;
  readonly killGraceMilliseconds: number;
  readonly maxOutputBytes: number;
  readonly sha256: string;
  readonly size: number;
  readonly timeoutMilliseconds: number;
  readonly workingDirectory: string;
}

function requiredValue(values: Map<string, string>, key: string): string {
  const value = values.get(key);
  if (value === undefined || value.length === 0) {
    throw new Error(`missing required option: ${key}`);
  }
  return value;
}

function integer(value: string, key: string): number {
  if (!/^\d+$/u.test(value)) {
    throw new Error(`invalid integer option: ${key}`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`invalid integer option: ${key}`);
  }
  return parsed;
}

function parseArguments(arguments_: readonly string[]): CliOptions {
  const separator = arguments_.indexOf('--');
  if (separator < 0) {
    throw new Error('missing -- before artifact arguments');
  }
  const optionArguments = arguments_.slice(0, separator);
  if (optionArguments.length % 2 !== 0) {
    throw new Error('every CLI option requires a value');
  }
  const values = new Map<string, string>();
  for (let index = 0; index < optionArguments.length; index += 2) {
    const key = optionArguments[index];
    const value = optionArguments[index + 1];
    if (key === undefined || value === undefined || !key.startsWith('--') || values.has(key)) {
      throw new Error('invalid or duplicate CLI option');
    }
    values.set(key, value);
  }
  return {
    arguments: arguments_.slice(separator + 1),
    cacheRoot: requiredValue(values, '--cache-root'),
    killGraceMilliseconds: integer(requiredValue(values, '--kill-grace-ms'), '--kill-grace-ms'),
    maxOutputBytes: integer(requiredValue(values, '--max-output-bytes'), '--max-output-bytes'),
    sha256: requiredValue(values, '--sha256'),
    size: integer(requiredValue(values, '--size'), '--size'),
    timeoutMilliseconds: integer(requiredValue(values, '--timeout-ms'), '--timeout-ms'),
    workingDirectory: requiredValue(values, '--cwd'),
  };
}

async function main(): Promise<void> {
  const options = parseArguments(process.argv.slice(2));
  const binding = await new ArtifactCache(options.cacheRoot).bind(options.sha256, options.size);
  try {
    const result = await new ProcessProbeRunner().run(binding, {
      arguments: options.arguments,
      environment: {},
      killGraceMilliseconds: options.killGraceMilliseconds,
      maxOutputBytesPerStream: options.maxOutputBytes,
      redact: (value) => (value.length === 0 ? '' : '[PROBE_OUTPUT_REDACTED_BY_CLI]'),
      timeoutMilliseconds: options.timeoutMilliseconds,
      workingDirectory: options.workingDirectory,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    if (result.outcome !== 'EXITED' || result.exitCode !== 0 || result.redactionFailed) {
      process.exitCode = 1;
    }
  } finally {
    await binding.close();
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `artifact probe failed: ${error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
});
