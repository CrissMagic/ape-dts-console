import os from 'node:os';
import path from 'node:path';

import { describe, expect, it } from 'vitest';

import {
  ControlledRuntimeLaunchSpecFactory,
  RuntimeLaunchSpecError,
  UnconfiguredRuntimeLaunchSpecFactory,
  type RuntimeLaunchSpecFactoryInput,
} from './runtime-launch-spec.js';

const taskId = '11111111-1111-4111-8111-111111111111';
const revisionId = '22222222-2222-4222-8222-222222222222';
const runId = '33333333-3333-4333-8333-333333333333';
const unitId = '44444444-4444-4444-8444-444444444444';

function input(
  overrides: Partial<RuntimeLaunchSpecFactoryInput> = {},
): RuntimeLaunchSpecFactoryInput {
  return {
    config: {
      kind: 'KAFKA_RAW_FORWARD',
      schemaVersion: 1,
      source: {
        dataSourceId: taskId,
        groupId: 'runtime-group',
        topic: 'runtime.source',
      },
      target: { dataSourceId: revisionId, topic: 'runtime.target' },
      tuning: {
        ackIntervalSeconds: 5,
        batchSize: 100,
        bufferSize: 16_000,
        checkpointIntervalSeconds: 15,
        requiredAcks: 'all',
      },
    },
    metricsPort: 35_001,
    revisionId,
    runId,
    taskId,
    unitId,
    unitKey: 'partition:0',
    ...overrides,
  };
}

function factory() {
  return new ControlledRuntimeLaunchSpecFactory({
    environment: { RUST_LOG: 'info' },
    executablePath: process.execPath,
    render: () => ({
      iniContent: '[runtime]\nmode=controlled\n',
      log4rsContent: 'appenders: []\n',
    }),
    runtimeRoot: path.join(os.tmpdir(), 'scs-runtime-controlled'),
  });
}

describe('ControlledRuntimeLaunchSpecFactory', () => {
  it('creates fixed platform-owned paths, arguments, and environment', () => {
    const launchInput = input();
    const spec = factory().create(launchInput);
    const expectedRoot = path.resolve(os.tmpdir(), 'scs-runtime-controlled');

    expect(spec).toMatchObject({
      args: ['--config', path.join(spec.runtimeDirectory, 'config.ini')],
      cwd: spec.runtimeDirectory,
      env: { RUST_LOG: 'info' },
      executablePath: process.execPath,
      iniContent: '[runtime]\nmode=controlled\n',
      log4rsContent: 'appenders: []\n',
      unitId,
    });
    expect(spec.runtimeDirectory).toBe(
      path.join(expectedRoot, launchInput.taskId, launchInput.runId, launchInput.unitId),
    );
    expect(spec.iniPath).toBe(path.join(spec.runtimeDirectory, 'config.ini'));
    expect(spec.log4rsPath).toBe(path.join(spec.runtimeDirectory, 'log4rs.yaml'));
    expect(Object.isFrozen(spec.env)).toBe(true);
  });

  it.each(['../escape', '..\\escape', 'partition/0', 'partition\\0', ''])(
    'rejects an unsafe unit key: %s',
    (unitKey) => {
      expect(() => factory().create(input({ unitKey }))).toThrow('RUNTIME_UNIT_KEY_INVALID');
    },
  );

  it.each([0, 65_536, 1.5, Number.NaN])('rejects an invalid metrics port: %s', (metricsPort) => {
    expect(() => factory().create(input({ metricsPort }))).toThrow('RUNTIME_METRICS_PORT_INVALID');
  });

  it('rejects invalid environment entries before any launch can be created', () => {
    expect(
      () =>
        new ControlledRuntimeLaunchSpecFactory({
          executablePath: process.execPath,
          environment: { 'INJECT=ARG': 'value' },
          render: () => ({ iniContent: '', log4rsContent: '' }),
          runtimeRoot: os.tmpdir(),
        }),
    ).toThrow('RUNTIME_ENVIRONMENT_INVALID');
  });

  it('rejects configuration content containing a NUL byte', () => {
    expect(() =>
      new ControlledRuntimeLaunchSpecFactory({
        executablePath: process.execPath,
        render: () => ({ iniContent: 'bad\u0000value', log4rsContent: '' }),
        runtimeRoot: os.tmpdir(),
      }).create(input()),
    ).toThrow('RUNTIME_CONFIGURATION_CONTENT_INVALID');
  });

  it('fails closed when no authenticated artifact adapter is configured', () => {
    expect(() => new UnconfiguredRuntimeLaunchSpecFactory().create()).toThrowError(
      expect.objectContaining({
        code: 'RUNTIME_LAUNCH_SPEC_UNCONFIGURED',
        name: RuntimeLaunchSpecError.name,
      }),
    );
  });
});
