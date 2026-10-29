import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { PassThrough } from 'node:stream';

import { afterEach, describe, expect, it } from 'vitest';

import {
  RuntimeConfigurationCleanupError,
  RuntimeProcessSupervisor,
  StreamingRuntimeRedactor,
  redactRuntimeLog,
  type RuntimeChildProcess,
  type RuntimeLogCloseResult,
  type RuntimeLogSink,
  type RuntimeProcessFactory,
} from './process-supervisor.js';

class FakeChildProcess extends EventEmitter {
  readonly stdout = new PassThrough();
  readonly stderr = new PassThrough();
  readonly signals: NodeJS.Signals[] = [];
  readonly pid = 12_345;
  private closed = false;
  private readonly closeOn: NodeJS.Signals | null;

  constructor(closeOn: NodeJS.Signals | null = null) {
    super();
    this.closeOn = closeOn;
  }

  kill(signal: NodeJS.Signals): boolean {
    this.signals.push(signal);
    if (this.closeOn === signal) {
      this.close(0, signal);
    }
    return true;
  }

  close(code: number | null, signal: NodeJS.Signals | null): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.stdout.end();
    this.stderr.end();
    this.emit('close', code, signal);
  }
}

class RecordingSink implements RuntimeLogSink {
  readonly entries: Array<{ stream: 'STDOUT' | 'STDERR'; value: string }> = [];
  readonly finalizedRuns: string[] = [];
  readonly gaps: Array<{
    lostBytes: number | null;
    reason: string;
    runId?: string;
    unitId: string;
  }> = [];
  closeCalls = 0;
  appendCalls = 0;
  gate: Promise<void> | undefined;
  closeResult: RuntimeLogCloseResult | undefined;
  closeFailure: Error | undefined;
  private releaseGate: (() => void) | undefined;

  hold(): void {
    this.gate = new Promise<void>((resolve) => {
      this.releaseGate = resolve;
    });
  }

  release(): void {
    this.releaseGate?.();
    this.releaseGate = undefined;
  }

  async append(input: { stream: 'STDOUT' | 'STDERR'; chunk: Buffer }): Promise<void> {
    this.appendCalls += 1;
    if (this.gate !== undefined) {
      await this.gate;
    }
    this.entries.push({ stream: input.stream, value: input.chunk.toString('utf8') });
  }

  async close(): Promise<RuntimeLogCloseResult | undefined> {
    this.closeCalls += 1;
    if (this.closeFailure !== undefined) {
      throw this.closeFailure;
    }
    return this.closeResult;
  }

  finalizeRun(runId: string): void {
    this.finalizedRuns.push(runId);
  }

  recordGap(input: {
    lostBytes: number | null;
    reason: string;
    runId?: string;
    unitId: string;
  }): void {
    this.gaps.push(input);
  }
}

function launchSpec(root: string) {
  return {
    args: ['--config', 'config.ini'],
    cwd: root,
    env: { PATH: process.env.PATH ?? '' },
    executablePath: process.execPath,
    iniContent: 'password=should-not-be-logged\n',
    iniPath: path.join(root, 'config.ini'),
    log4rsContent: 'appenders: []\n',
    log4rsPath: path.join(root, 'log4rs.yaml'),
    runtimeDirectory: root,
  };
}

describe('RuntimeProcessSupervisor', () => {
  const temporaryDirectories: string[] = [];

  afterEach(async () => {
    const directories = temporaryDirectories.splice(0);
    await Promise.all(
      directories.map((directory) => rm(directory, { force: true, recursive: true })),
    );
  });

  it('preserves Chinese and emoji split across byte chunks in independent stdout and stderr streams', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-utf8-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: redactRuntimeLog,
    });
    const handle = await supervisor.start({
      ...launchSpec(root),
      unitId: 'utf8',
      sensitiveValues: ['密码样例'],
    });
    const text = `同步正常😀 ${'😀'.repeat(700)} password=密码样例\n完成\n`;
    for (const stream of [child.stdout, child.stderr])
      for (const byte of Buffer.from(text)) stream.write(Buffer.from([byte]));
    child.close(0, null);
    const result = await handle.waitForExit();
    for (const stream of ['STDOUT', 'STDERR']) {
      const output = sink.entries
        .filter((entry) => entry.stream === stream)
        .map((entry) => entry.value)
        .join('');
      expect(output).toContain('同步正常😀');
      expect(output).toContain('😀'.repeat(700));
      expect(output).toContain('完成');
      expect(output).toContain('[REDACTED]');
      expect(output).not.toContain('密码样例');
      expect(output).not.toContain('\uFFFD');
    }
    expect(result.logGapBytes).toBe(0);
  });

  it('writes the PostgreSQL CA before spawn and removes it with runtime configuration on exit', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-pg-tls-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const ca = path.join(root, 'postgres-ca.pem');
    const supervisor = new RuntimeProcessSupervisor({
      factory: {
        spawn: () => {
          expect(readFileSync(ca, 'utf8')).toBe('pg-private-ca');
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
    });
    const handle = await supervisor.start({
      ...launchSpec(root),
      unitId: 'pg',
      postgresCaCertificate: 'pg-private-ca',
    });
    expect(await readFile(ca, 'utf8')).toBe('pg-private-ca');
    child.close(0, null);
    expect((await handle.waitForExit()).configurationCleanupFailed).toBe(false);
    await expect(readFile(ca)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('writes the MySQL verification CA before spawn and removes it with plaintext configuration on exit', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-mysql-tls-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: new RecordingSink(),
      redact: (value) => value,
    });
    const spec = {
      ...launchSpec(root),
      unitId: 'mysql-unit',
      caCertificate: 'test-ca-certificate',
    };
    const handle = await supervisor.start(spec);
    const ca = path.join(root, 'mysql-ca.pem');
    expect(await readFile(ca, 'utf8')).toBe('test-ca-certificate');
    child.close(0, null);
    expect((await handle.waitForExit()).configurationCleanupFailed).toBe(false);
    await expect(readFile(ca)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(readFile(spec.iniPath)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('loads durable log limits before writing plaintext runtime configuration or spawning', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    let spawned = false;
    const supervisor = new RuntimeProcessSupervisor({
      beforeStart: async (runId) => {
        expect(runId).toBe('11111111-1111-4111-8111-111111111111');
        throw new Error('LOG_SETTINGS_UNAVAILABLE');
      },
      factory: {
        spawn: () => {
          spawned = true;
          return new FakeChildProcess() as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
    });

    const spec = {
      ...launchSpec(root),
      runId: '11111111-1111-4111-8111-111111111111',
      unitId: 'unit-1',
    };
    await expect(supervisor.start(spec)).rejects.toThrow('LOG_SETTINGS_UNAVAILABLE');
    await expect(readFile(spec.iniPath)).rejects.toMatchObject({ code: 'ENOENT' });
    expect(spawned).toBe(false);
  });

  it('confirms OS exit without hanging when plaintext configuration cleanup fails', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: new RecordingSink(),
      redact: (value) => value,
      removeFile: async () => {
        throw new Error('injected cleanup failure');
      },
    });

    const processHandle = await supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    child.close(0, null);

    await expect(processHandle.waitForExit()).resolves.toMatchObject({
      configurationCleanupFailed: true,
      confirmed: true,
      terminationTimedOut: false,
    });
  });

  it('prioritizes configuration residue while preserving the original start failure', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const original = new Error('original spawn failure');
    const supervisor = new RuntimeProcessSupervisor({
      factory: {
        spawn: () => {
          throw original;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
      removeFile: async () => {
        throw new Error('injected cleanup failure');
      },
    });

    await expect(supervisor.start({ ...launchSpec(root), unitId: 'unit-1' })).rejects.toMatchObject(
      {
        cause: expect.any(Error),
        message: 'RUNTIME_CONFIGURATION_CLEANUP_FAILED',
        originalError: original,
      } satisfies Partial<RuntimeConfigurationCleanupError>,
    );
  });

  it('redacts common credential fields and URL credentials before persistence', () => {
    const redacted = redactRuntimeLog(
      'password=plain secret: value token:abc authorization: Bearer123 https://user:pass@example.test postgresql://dbuser:dbpass@db.example.test/db?password=query-secret',
    );

    expect(redacted).not.toContain('plain');
    expect(redacted).not.toContain('value');
    expect(redacted).not.toContain('abc');
    expect(redacted).not.toContain('Bearer123');
    expect(redacted).not.toContain('user:pass');
    expect(redacted).not.toContain('dbuser:dbpass');
    expect(redacted).not.toContain('query-secret');
    expect(redacted).toContain('password=[REDACTED]');
    expect(redacted).toContain('https://user:[REDACTED]@example.test');
    expect(redacted).toContain('postgresql://dbuser:[REDACTED]@db.example.test');
    expect(redacted).toContain('password=[REDACTED]');
  });

  it('does not leak a credential split across chunks', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: redactRuntimeLog,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-1',
      unitId: 'unit-1',
    });
    child.stdout.write('password=split-');
    child.stdout.write('secret\n');
    child.close(0, null);
    await processHandle.waitForExit();

    const persisted = sink.entries.map((entry) => entry.value).join('');
    expect(persisted).not.toContain('split-secret');
    expect(persisted).toContain('password=[REDACTED]');
  });

  it('does not emit an exact secret prefix when the secret crosses the safe boundary', () => {
    const secret = 'opaque-secret-value';
    const redactor = new StreamingRuntimeRedactor((value) => value, 1024, [secret]);
    const prefix = 'x'.repeat(1500);
    const first = redactor.append(`${prefix}${secret.slice(0, 8)}`);
    const second = redactor.append(`${secret.slice(8)}${'y'.repeat(1024)}`);
    const output = first + second + redactor.flush();

    expect(output).toContain('[REDACTED]');
    expect(output).not.toContain(secret);
    expect(output).not.toContain(secret.slice(0, 8));
  });

  it('redacts multiple exact secrets in arbitrary formats across chunk boundaries', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: redactRuntimeLog,
    });
    const processHandle = await supervisor.start({
      ...launchSpec(root),
      sensitiveValues: ['opaque::value/without-field-name', 'another secret!?'],
      unitId: 'unit-exact-secret',
    });

    child.stdout.write('prefix opaque::value/with');
    child.stdout.write('out-field-name middle another ');
    child.stdout.write('secret!? suffix');
    child.close(0, null);
    await processHandle.waitForExit();

    const persisted = sink.entries.map((entry) => entry.value).join('');
    expect(persisted).toContain('prefix [REDACTED] middle [REDACTED] suffix');
    expect(persisted).not.toContain('opaque::value/without-field-name');
    expect(persisted).not.toContain('another secret!?');
  });

  it('rejects an empty exact sensitive value', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => new FakeChildProcess() as unknown as RuntimeChildProcess },
      logSink: new RecordingSink(),
      redact: redactRuntimeLog,
    });

    await expect(
      supervisor.start({ ...launchSpec(root), sensitiveValues: [''], unitId: 'unit-empty-secret' }),
    ).rejects.toThrow('RUNTIME_SENSITIVE_VALUE_INVALID');
  });

  it('drops an oversized raw block before redaction and records the whole block as a gap', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      maxRawLogBlockBytes: 4,
      redact: (value) => value,
    });
    const handle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-raw',
      unitId: 'unit-raw',
    });
    child.stdout.write('12345');
    child.close(0, null);
    const result = await handle.waitForExit();

    expect(result.logGapBytes).toBe(5);
    expect(sink.entries).toEqual([]);
    expect(sink.gaps).toContainEqual({
      lostBytes: 5,
      reason: 'RAW_QUEUE_OVERFLOW',
      runId: 'run-raw',
      unitId: 'unit-raw',
    });
  });

  it('shares one burst and sustained-rate budget across stdout and stderr of a unit', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    let now = 0;
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      maxRawLogBlockBytes: 4,
      monotonicNowMilliseconds: () => now,
      rawLogBurstBytes: 6,
      rawLogRateBytesPerSecond: 2,
      redact: (value) => value,
    });
    const handle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-rate',
      unitId: 'unit-rate',
    });
    child.stdout.write('1234');
    child.stderr.write('abcd');
    now += 1000;
    child.stderr.write('ok');
    child.close(0, null);
    const result = await handle.waitForExit();

    expect(result.logGapBytes).toBe(4);
    expect(sink.entries.map((entry) => entry.value).join('')).toContain('1234');
    expect(sink.entries.map((entry) => entry.value).join('')).toContain('ok');
    expect(sink.entries.map((entry) => entry.value).join('')).not.toContain('abcd');
  });

  it('bounds the global raw queue and lets stop complete while a unit floods logs', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess('SIGINT');
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      maxQueuedRawLogBytes: 8,
      maxRawLogBlockBytes: 4,
      rawLogBurstBytes: 1024,
      rawLogRateBytesPerSecond: 1024,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });
    const handle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-flood',
      unitId: 'unit-flood',
    });
    for (let index = 0; index < 100; index += 1) child.stdout.write('1234');

    const result = await handle.stop();
    expect(result.confirmed).toBe(true);
    expect(result.logGapBytes).toBeGreaterThan(0);
    expect(child.signals).toEqual(['SIGINT']);
  });

  it('round-robins the global raw queue so one unit cannot starve another', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const first = new FakeChildProcess();
    const second = new FakeChildProcess();
    const children = [first, second];
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => children.shift() as unknown as RuntimeChildProcess },
      logSink: sink,
      maxRawLogBlockBytes: 2048,
      rawLogBurstBytes: 16 * 1024,
      rawLogRateBytesPerSecond: 16 * 1024,
      redact: (value) => value,
    });
    const firstHandle = await supervisor.start({
      ...launchSpec(root),
      iniPath: path.join(root, 'fair-first.ini'),
      log4rsPath: path.join(root, 'fair-first.yaml'),
      unitId: 'fair-first',
    });
    const secondHandle = await supervisor.start({
      ...launchSpec(root),
      iniPath: path.join(root, 'fair-second.ini'),
      log4rsPath: path.join(root, 'fair-second.yaml'),
      unitId: 'fair-second',
    });
    first.stdout.write(`A${'a'.repeat(1499)}`);
    first.stdout.write(`C${'c'.repeat(1499)}`);
    first.stdout.write(`D${'d'.repeat(1499)}`);
    second.stdout.write(`B${'b'.repeat(1499)}`);
    for (let index = 0; index < 10; index += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    first.close(0, null);
    second.close(0, null);
    await Promise.all([firstHandle.waitForExit(), secondHandle.waitForExit()]);

    expect(sink.entries[0]?.value.startsWith('A')).toBe(true);
    expect(sink.entries[1]?.value.startsWith('B')).toBe(true);
  });

  it('releases every per-unit raw ingress structure after repeated start and close', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const children = Array.from({ length: 6 }, () => new FakeChildProcess());
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => children.shift() as unknown as RuntimeChildProcess },
      logSink: new RecordingSink(),
      redact: (value) => value,
    });

    for (let index = 0; index < 6; index += 1) {
      const child = children[0]!;
      const handle = await supervisor.start({
        ...launchSpec(root),
        iniPath: path.join(root, `cleanup-${index}.ini`),
        log4rsPath: path.join(root, `cleanup-${index}.yaml`),
        unitId: `cleanup-unit-${index}`,
      });
      child.stdout.write(`entry-${index}`);
      child.close(0, null);
      await handle.waitForExit();
    }

    const ingress = (
      supervisor as unknown as {
        rawHandlers: Map<string, unknown>;
        rawIngress: {
          buckets: Map<string, unknown>;
          pendingByUnit: Map<string, unknown>;
          queues: Map<string, unknown>;
          unitOrder: string[];
          waiters: Map<string, unknown>;
        };
      }
    ).rawIngress;
    expect(ingress.queues.size).toBe(0);
    expect(ingress.unitOrder).toEqual([]);
    expect(ingress.pendingByUnit.size).toBe(0);
    expect(ingress.waiters.size).toBe(0);
    expect(ingress.buckets.size).toBe(0);
    expect((supervisor as unknown as { rawHandlers: Map<string, unknown> }).rawHandlers.size).toBe(
      0,
    );
  });

  it('writes restricted runtime files, drains both streams, and removes config after exit', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const factory: RuntimeProcessFactory = {
      spawn: () => child as unknown as RuntimeChildProcess,
    };
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory,
      logSink: sink,
      redact: (value) => value.replaceAll('secret', '[REDACTED]'),
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({
      ...launchSpec(root),
      unitId: 'unit-1',
    });
    await expect(readFile(path.join(root, 'config.ini'), 'utf8')).resolves.toBe(
      'password=should-not-be-logged\n',
    );
    child.emit('spawn');
    child.stdout.write('stdout secret\n');
    child.stderr.write('stderr secret\n');
    child.close(0, null);
    await processHandle.waitForExit();

    expect(sink.entries).toEqual([
      { stream: 'STDOUT', value: 'stdout [REDACTED]\n' },
      { stream: 'STDERR', value: 'stderr [REDACTED]\n' },
    ]);
    expect(sink.closeCalls).toBe(1);
    await expect(readFile(path.join(root, 'config.ini'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(readFile(path.join(root, 'log4rs.yaml'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('waits for the operating system spawn event when awaitSpawn is enabled', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    let resolveSpawned: (() => void) | undefined;
    const spawned = new Promise<void>((resolve) => {
      resolveSpawned = resolve;
    });
    const supervisor = new RuntimeProcessSupervisor({
      awaitSpawn: true,
      factory: {
        spawn: () => {
          resolveSpawned?.();
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    let settled = false;
    const startPromise = supervisor.start({ ...launchSpec(root), unitId: 'unit-1' }).then(
      (value) => {
        settled = true;
        return value;
      },
      (error: unknown) => {
        settled = true;
        throw error;
      },
    );
    await spawned;
    expect(settled).toBe(false);

    child.emit('spawn');
    const processHandle = await startPromise;
    child.close(0, null);
    await expect(processHandle.waitForExit()).resolves.toMatchObject({
      confirmed: true,
      code: 0,
    });
  });

  it('fails closed when the child closes before emitting spawn', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    let resolveSpawned: (() => void) | undefined;
    const spawned = new Promise<void>((resolve) => {
      resolveSpawned = resolve;
    });
    const supervisor = new RuntimeProcessSupervisor({
      awaitSpawn: true,
      factory: {
        spawn: () => {
          resolveSpawned?.();
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const startPromise = supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    await spawned;
    child.close(null, null);

    await expect(startPromise).rejects.toThrow('RUNTIME_PROCESS_SPAWN_FAILED');
    await expect(readFile(path.join(root, 'config.ini'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
    await expect(readFile(path.join(root, 'log4rs.yaml'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('stops and cleans up when a readiness probe rejects after spawn', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess('SIGINT');
    let resolveSpawned: (() => void) | undefined;
    const spawned = new Promise<void>((resolve) => {
      resolveSpawned = resolve;
    });
    const supervisor = new RuntimeProcessSupervisor({
      awaitSpawn: true,
      factory: {
        spawn: () => {
          resolveSpawned?.();
          return child as unknown as RuntimeChildProcess;
        },
      },
      logSink: new RecordingSink(),
      readinessProbe: async () => {
        throw new Error('runtime is not ready');
      },
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const startPromise = supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    await spawned;
    child.emit('spawn');

    await expect(startPromise).rejects.toThrow('runtime is not ready');
    expect(child.signals).toEqual(['SIGINT']);
    await expect(readFile(path.join(root, 'config.ini'), 'utf8')).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('drops raw data when redaction fails and emits only a safe placeholder', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: () => {
        throw new Error('redactor unavailable');
      },
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-1',
      unitId: 'unit-1',
    });
    child.emit('spawn');
    child.stdout.write('top-secret-value');
    child.close(0, null);
    const result = await processHandle.waitForExit();

    expect(result.logGapBytes).toBeGreaterThan(0);
    expect(sink.entries).toEqual([{ stream: 'STDOUT', value: '[REDACTION_FAILED]' }]);
    expect(sink.entries.some((entry) => entry.value.includes('top-secret'))).toBe(false);
    expect(sink.gaps).toContainEqual({
      lostBytes: Buffer.byteLength('top-secret-value'),
      reason: 'REDACTION_FAILURE',
      runId: 'run-1',
      unitId: 'unit-1',
    });
  });

  it('records a log stream error without inventing a lost-byte count', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-1',
      unitId: 'unit-1',
    });
    child.stdout.write('buffered output');
    child.stdout.emit('error', new Error('stdout failed'));
    child.close(0, null);

    const result = await processHandle.waitForExit();
    expect(result.logStreamError).toBe(true);
    expect(result.logGapBytes).toBe(Buffer.byteLength('buffered output'));
    expect(sink.entries).toEqual([]);
    expect(sink.gaps).toContainEqual({
      lostBytes: null,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      unitId: 'unit-1',
    });
  });

  it('propagates a durable log recovery state without changing the process exit fact', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    sink.closeResult = { recoveryState: 'DURABLE_PENDING', storageState: 'DEGRADED' };
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    child.close(0, null);

    await expect(processHandle.waitForExit()).resolves.toMatchObject({
      code: 0,
      confirmed: true,
      logRecoveryState: 'DURABLE_PENDING',
      logStorageState: 'DEGRADED',
    });
    supervisor.finalizeRun('run-1');
    expect(sink.finalizedRuns).toEqual(['run-1']);
  });

  it('reports a rejected log close as memory-pending degradation instead of swallowing it', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    sink.closeFailure = new Error('storage close failed');
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    child.close(0, null);

    await expect(processHandle.waitForExit()).resolves.toMatchObject({
      code: 0,
      confirmed: true,
      logRecoveryState: 'MEMORY_PENDING',
      logStorageState: 'DEGRADED',
    });
  });

  it('reports a timed-out log close as memory-pending degradation', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    sink.close = async () => await new Promise<never>(() => undefined);
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      logSinkTimeoutMilliseconds: 5,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const processHandle = await supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    child.close(0, null);

    await expect(processHandle.waitForExit()).resolves.toMatchObject({
      code: 0,
      confirmed: true,
      logRecoveryState: 'MEMORY_PENDING',
      logStorageState: 'DEGRADED',
    });
  });

  it('escalates stop from SIGINT to SIGTERM and then SIGKILL', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess('SIGKILL');
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
    });
    const processHandle = await supervisor.start({ ...launchSpec(root), unitId: 'unit-1' });
    child.emit('spawn');

    const result = await processHandle.stop();

    expect(child.signals).toEqual(['SIGINT', 'SIGTERM', 'SIGKILL']);
    expect(result.forcedKill).toBe(true);
    expect(result.signal).toBe('SIGKILL');
  });

  it('returns a bounded timeout result when SIGKILL still produces no close event', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 1, terminateGraceMilliseconds: 1 },
    });
    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-1',
      unitId: 'unit-1',
    });
    child.stdout.emit('error', new Error('stdout failed'));

    const result = await processHandle.stop();

    expect(result).toMatchObject({
      forcedKill: true,
      logGapBytes: 0,
      logStreamError: true,
      signal: 'SIGKILL',
      terminationTimedOut: true,
    });
    expect(sink.gaps).toContainEqual({
      lostBytes: null,
      reason: 'STREAM_ERROR',
      runId: 'run-1',
      unitId: 'unit-1',
    });
  });

  it('force kills immediately and merges concurrent force requests', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess('SIGKILL');
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: new RecordingSink(),
      redact: redactRuntimeLog,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });
    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-force',
      unitId: 'unit-force',
    });

    const first = processHandle.forceKill();
    const second = processHandle.forceKill();
    const [firstResult, secondResult] = await Promise.all([first, second]);

    expect(child.signals).toEqual(['SIGKILL']);
    expect(firstResult).toEqual(secondResult);
    expect(firstResult).toMatchObject({
      confirmed: true,
      forcedKill: true,
      signals: ['SIGKILL'],
      terminationTimedOut: false,
    });
  });

  it('bounds pending log memory and reports overflow without blocking the child', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const child = new FakeChildProcess();
    const sink = new RecordingSink();
    sink.hold();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => child as unknown as RuntimeChildProcess },
      logSink: sink,
      maxBufferedLogBytes: 8,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });
    const processHandle = await supervisor.start({
      ...launchSpec(root),
      runId: 'run-1',
      unitId: 'unit-1',
    });
    child.emit('spawn');
    child.stdout.write('12345678');
    child.stdout.write('abcdefgh');
    child.close(0, null);
    sink.release();
    const result = await processHandle.waitForExit();

    expect(result.logGapBytes).toBeGreaterThan(0);
    const persisted = sink.entries.map((entry) => entry.value).join('');
    expect(persisted).not.toContain('12345678');
    expect(persisted).toContain('abcdefgh');
    expect(Buffer.byteLength(persisted)).toBeLessThanOrEqual(8);
    expect(sink.gaps).toContainEqual({
      lostBytes: 8,
      reason: 'RAW_QUEUE_OVERFLOW',
      runId: 'run-1',
      unitId: 'unit-1',
    });
  });

  it('shares the outage buffer limit across runtime units', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-'));
    temporaryDirectories.push(root);
    const first = new FakeChildProcess();
    const second = new FakeChildProcess();
    const children = [first, second];
    const sink = new RecordingSink();
    sink.hold();
    const supervisor = new RuntimeProcessSupervisor({
      factory: { spawn: () => children.shift() as unknown as RuntimeChildProcess },
      logSink: sink,
      maxBufferedLogBytes: 8,
      redact: (value) => value,
      stopPolicy: { interruptGraceMilliseconds: 10, terminateGraceMilliseconds: 10 },
    });

    const firstHandle = await supervisor.start({
      ...launchSpec(root),
      iniPath: path.join(root, 'first.ini'),
      log4rsPath: path.join(root, 'first.yaml'),
      unitId: 'unit-1',
    });
    const secondHandle = await supervisor.start({
      ...launchSpec(root),
      iniPath: path.join(root, 'second.ini'),
      log4rsPath: path.join(root, 'second.yaml'),
      unitId: 'unit-2',
    });
    first.stdout.write('12345678');
    second.stdout.write('abcdefgh');
    first.close(0, null);
    second.close(0, null);
    for (let index = 0; index < 10; index += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    sink.release();
    const [firstExit, secondExit] = await Promise.all([
      firstHandle.waitForExit(),
      secondHandle.waitForExit(),
    ]);

    expect(firstExit.logGapBytes + secondExit.logGapBytes).toBeGreaterThan(0);
    expect(
      Buffer.byteLength(sink.entries.map((entry) => entry.value).join('')),
    ).toBeLessThanOrEqual(8);
  });
});
