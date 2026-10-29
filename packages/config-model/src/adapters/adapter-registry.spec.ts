import { describe, expect, it } from 'vitest';

import {
  AdapterBindingConflictError,
  AdapterRegistry,
  CertifiedAdapterRegistry,
  UnknownArtifactAdapterError,
  type ApeDtsConfigAdapter,
} from './adapter-registry.js';

const artifactSha256 = 'a'.repeat(64);
const adapter: ApeDtsConfigAdapter = {
  id: 'ape-dts-ini',
  render: () => ({ log4rsYaml: 'console-only', previewIni: 'preview', runtimeIni: 'runtime' }),
  version: '1',
};

describe('AdapterRegistry', () => {
  it('resolves only an exact artifact hash, platform version and task scenario binding', () => {
    const registry = new AdapterRegistry();
    registry.register({
      adapter,
      artifactSha256,
      platformVersion: '0.1.0',
      scenario: 'PG_CDC_TO_KAFKA',
    });

    expect(
      registry.resolve({ artifactSha256, platformVersion: '0.1.0', scenario: 'PG_CDC_TO_KAFKA' }),
    ).toBe(adapter);
    expect(() =>
      registry.resolve({
        artifactSha256: 'b'.repeat(64),
        platformVersion: '0.1.0',
        scenario: 'PG_CDC_TO_KAFKA',
      }),
    ).toThrow(UnknownArtifactAdapterError);
    expect(() =>
      registry.resolve({ artifactSha256, platformVersion: '0.1.1', scenario: 'PG_CDC_TO_KAFKA' }),
    ).toThrow(UnknownArtifactAdapterError);
    expect(() =>
      registry.resolve({
        artifactSha256,
        platformVersion: '0.1.0',
        scenario: 'PG_SNAPSHOT_TO_KAFKA',
      }),
    ).toThrow(UnknownArtifactAdapterError);
  });

  it('rejects an ambiguous duplicate binding instead of using registration order', () => {
    const registry = new AdapterRegistry();
    const binding = {
      adapter,
      artifactSha256,
      platformVersion: '0.1.0',
      scenario: 'KAFKA_RAW_FORWARD' as const,
    };
    registry.register(binding);

    expect(() => registry.register(binding)).toThrow(AdapterBindingConflictError);
  });
});

describe('CertifiedAdapterRegistry', () => {
  it.each([
    'PG_CDC_TO_KAFKA',
    'PG_SNAPSHOT_TO_KAFKA',
    'KAFKA_RAW_FORWARD',
    'MYSQL_CDC_TO_KAFKA',
    'MYSQL_SNAPSHOT_TO_KAFKA',
  ] as const)('uses the shipped runtime adapter for %s without a certified binding', (scenario) => {
    const registry = new CertifiedAdapterRegistry([adapter]);
    expect(registry.resolveRuntime({ artifactSha256, platformVersion: '0.1.0', scenario })).toBe(
      adapter,
    );
  });
  it('materializes only the exact adapter identity from a certified binding', () => {
    const registry = new CertifiedAdapterRegistry([adapter]);
    const binding = {
      adapterId: adapter.id,
      adapterVersion: adapter.version,
      artifactSha256,
      platformVersion: '0.1.0',
      scenario: 'KAFKA_RAW_FORWARD' as const,
    };

    expect(registry.resolveCertified(binding)).toBe(adapter);
    expect(registry.resolveCertified(binding)).toBe(adapter);
    expect(() => registry.resolveCertified({ ...binding, adapterVersion: 'unsupported' })).toThrow(
      AdapterBindingConflictError,
    );
  });

  it('does not substitute an implementation for an unknown certified identity', () => {
    const registry = new CertifiedAdapterRegistry([adapter]);

    expect(() =>
      registry.resolveCertified({
        adapterId: 'unknown',
        adapterVersion: '1',
        artifactSha256,
        platformVersion: '0.1.0',
        scenario: 'PG_CDC_TO_KAFKA',
      }),
    ).toThrow(UnknownArtifactAdapterError);
  });
});
