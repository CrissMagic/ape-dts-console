import path from 'node:path';

import type { PlatformTaskConfig, MysqlCdcRuntimePosition } from '@scs/config-model';

import type { RuntimeSpawnSpec } from './process-supervisor.js';

export interface RuntimeLaunchSpecFactoryInput {
  readonly config: PlatformTaskConfig;
  readonly metricsPort: number;
  readonly revisionId: string;
  readonly runId: string;
  readonly taskId: string;
  readonly unitId: string;
  readonly unitKey: string;
  readonly mysql?: MysqlCdcRuntimePosition & { readonly phase: 'SNAPSHOT' | 'CDC' };
}

export interface RuntimeArtifactVerificationInput {
  readonly revisionId: string;
  readonly taskId: string;
}

/**
 * The supervisor receives only unit identity/port from a request.  A factory
 * owned by the platform converts that metadata into a complete OS launch
 * specification after the artifact, adapter and secrets have been verified.
 */
export abstract class RuntimeLaunchSpecFactory {
  abstract create(
    input: RuntimeLaunchSpecFactoryInput,
  ): Promise<RuntimeSpawnSpec> | RuntimeSpawnSpec;

  verifyArtifact(_input: RuntimeArtifactVerificationInput): Promise<void> {
    void _input;
    throw new RuntimeLaunchSpecError('RUNTIME_LAUNCH_SPEC_UNCONFIGURED');
  }
}

export type RuntimeLaunchSpecErrorCode = 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED';

export class RuntimeLaunchSpecError extends Error {
  constructor(readonly code: RuntimeLaunchSpecErrorCode) {
    super(code);
    this.name = 'RuntimeLaunchSpecError';
  }
}

export interface ControlledRuntimeLaunchSpecOptions {
  /** Immutable path selected by the authenticated artifact binding. */
  readonly executablePath: string;
  /** Root under which all per-run directories are created. */
  readonly runtimeRoot: string;
  /** Adapter-generated INI and console-only log configuration. */
  readonly render: (input: RuntimeLaunchSpecFactoryInput) => {
    readonly iniContent: string;
    readonly log4rsContent: string;
  };
  /** Explicit environment allow-list; caller values are never merged. */
  readonly environment?: Readonly<Record<string, string>>;
}

function assertAbsolutePath(value: string, code: string): string {
  if (
    value.trim().length === 0 ||
    !path.isAbsolute(value) ||
    value.includes('\u0000') ||
    value.includes('\r') ||
    value.includes('\n')
  ) {
    throw new Error(code);
  }
  return path.resolve(value);
}

function assertUuid(value: string, code: string): void {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)) {
    throw new Error(code);
  }
}

function assertUnitKey(unitKey: string): void {
  if (
    unitKey.trim().length === 0 ||
    unitKey.includes('\u0000') ||
    unitKey.includes('/') ||
    unitKey.includes('\\') ||
    [...unitKey].some(
      (character) => character.charCodeAt(0) <= 0x1f || character.charCodeAt(0) === 0x7f,
    )
  ) {
    throw new Error('RUNTIME_UNIT_KEY_INVALID');
  }
}

function assertChildPath(root: string, candidate: string): void {
  const relative = path.relative(root, candidate);
  if (
    relative.length === 0 ||
    relative === '..' ||
    relative.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relative)
  ) {
    throw new Error('RUNTIME_LAUNCH_PATH_ESCAPE');
  }
}

function safeEnvironment(
  environment: Readonly<Record<string, string>>,
): Readonly<Record<string, string>> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(environment)) {
    if (
      key.trim().length === 0 ||
      key.includes('=') ||
      key.includes('\u0000') ||
      value.includes('\u0000') ||
      value.includes('\r') ||
      value.includes('\n')
    ) {
      throw new Error('RUNTIME_ENVIRONMENT_INVALID');
    }
    result[key] = value;
  }
  return Object.freeze(result);
}

/**
 * A conservative platform-owned implementation.  It intentionally has no
 * request-controlled executable, arguments, cwd, environment or file paths.
 */
export class ControlledRuntimeLaunchSpecFactory extends RuntimeLaunchSpecFactory {
  private readonly executablePath: string;
  private readonly runtimeRoot: string;
  private readonly render: ControlledRuntimeLaunchSpecOptions['render'];
  private readonly environment: Readonly<Record<string, string>>;

  constructor(options: ControlledRuntimeLaunchSpecOptions) {
    super();
    this.executablePath = assertAbsolutePath(
      options.executablePath,
      'RUNTIME_EXECUTABLE_PATH_INVALID',
    );
    this.runtimeRoot = assertAbsolutePath(options.runtimeRoot, 'RUNTIME_DIRECTORY_INVALID');
    this.render = options.render;
    this.environment = safeEnvironment(options.environment ?? {});
  }

  create(input: RuntimeLaunchSpecFactoryInput): RuntimeSpawnSpec {
    assertUuid(input.taskId, 'RUNTIME_TASK_ID_INVALID');
    assertUuid(input.revisionId, 'RUNTIME_REVISION_ID_INVALID');
    assertUuid(input.runId, 'RUNTIME_RUN_ID_INVALID');
    assertUuid(input.unitId, 'RUNTIME_UNIT_ID_INVALID');
    if (
      !Number.isSafeInteger(input.metricsPort) ||
      input.metricsPort < 1 ||
      input.metricsPort > 65_535
    ) {
      throw new Error('RUNTIME_METRICS_PORT_INVALID');
    }
    assertUnitKey(input.unitKey);
    const taskDirectory = path.resolve(this.runtimeRoot, input.taskId);
    const runDirectory = path.resolve(taskDirectory, input.runId);
    const runtimeDirectory = path.resolve(runDirectory, input.unitId);
    assertChildPath(this.runtimeRoot, taskDirectory);
    assertChildPath(taskDirectory, runDirectory);
    assertChildPath(runDirectory, runtimeDirectory);
    const iniPath = path.join(runtimeDirectory, 'config.ini');
    const log4rsPath = path.join(runtimeDirectory, 'log4rs.yaml');
    assertChildPath(runtimeDirectory, iniPath);
    assertChildPath(runtimeDirectory, log4rsPath);
    const rendered = this.render(input);
    if (rendered.iniContent.includes('\u0000') || rendered.log4rsContent.includes('\u0000')) {
      throw new Error('RUNTIME_CONFIGURATION_CONTENT_INVALID');
    }
    return {
      args: ['--config', iniPath],
      cwd: runtimeDirectory,
      env: this.environment,
      executablePath: this.executablePath,
      iniContent: rendered.iniContent,
      iniPath,
      log4rsContent: rendered.log4rsContent,
      log4rsPath,
      runtimeDirectory,
      unitId: input.unitId,
    };
  }
}

/** Production remains fail-closed until an authenticated artifact adapter is installed. */
export class UnconfiguredRuntimeLaunchSpecFactory extends RuntimeLaunchSpecFactory {
  create(): never {
    throw new RuntimeLaunchSpecError('RUNTIME_LAUNCH_SPEC_UNCONFIGURED');
  }
}
