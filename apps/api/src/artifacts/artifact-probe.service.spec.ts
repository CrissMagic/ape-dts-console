import { describe, expect, it, vi } from 'vitest';

import type { ProcessProbeRunner } from '@scs/artifact-probe';
import type { ArtifactCache, ArtifactExecutionBinding } from '@scs/storage';

import { ArtifactProbeService } from './artifact-probe.service.js';

const request = {
  arguments: ['--version'],
  environment: {},
  expectedSha256: 'a'.repeat(64),
  expectedSize: 1024,
  killGraceMilliseconds: 100,
  maxOutputBytesPerStream: 4096,
  redact: (value: string) => value,
  timeoutMilliseconds: 1000,
  workingDirectory: 'C:\\probe',
};

describe('ArtifactProbeService', () => {
  it('always closes the verified cache binding after a successful probe', async () => {
    const close = vi.fn(async () => undefined);
    const binding = { close } as unknown as ArtifactExecutionBinding;
    const cache = { bind: vi.fn(async () => binding) } as unknown as ArtifactCache;
    const result = {
      durationMilliseconds: 1,
      exitCode: 0,
      outcome: 'EXITED' as const,
      redactionFailed: false,
      signal: null,
      stderr: '',
      stderrTruncated: false,
      stdout: '',
      stdoutTruncated: false,
    };
    const runner = { run: vi.fn(async () => result) } as unknown as ProcessProbeRunner;

    await expect(new ArtifactProbeService(cache, runner).runCachedProbe(request)).resolves.toBe(
      result,
    );
    expect(close).toHaveBeenCalledOnce();
  });

  it('closes the binding when the behavior probe fails', async () => {
    const close = vi.fn(async () => undefined);
    const binding = { close } as unknown as ArtifactExecutionBinding;
    const cache = { bind: vi.fn(async () => binding) } as unknown as ArtifactCache;
    const runner = {
      run: vi.fn(async () => {
        throw new Error('probe failed');
      }),
    } as unknown as ProcessProbeRunner;

    await expect(new ArtifactProbeService(cache, runner).runCachedProbe(request)).rejects.toThrow(
      'probe failed',
    );
    expect(close).toHaveBeenCalledOnce();
  });
});
