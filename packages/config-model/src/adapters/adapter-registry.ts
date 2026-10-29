import type { ApeDtsConfigAdapter, TaskScenario } from './types.js';

const SHA256_PATTERN = /^[a-f0-9]{64}$/u;

export interface AdapterLookupKey {
  readonly artifactSha256: string;
  readonly platformVersion: string;
  readonly scenario: TaskScenario;
}

export interface AdapterBinding extends AdapterLookupKey {
  readonly adapter: ApeDtsConfigAdapter;
}

export interface CertifiedAdapterBinding extends AdapterLookupKey {
  readonly adapterId: string;
  readonly adapterVersion: string;
}

export class UnknownArtifactAdapterError extends Error {
  readonly code = 'UNKNOWN_ARTIFACT_ADAPTER';

  constructor() {
    super('UNKNOWN_ARTIFACT_ADAPTER');
    this.name = 'UnknownArtifactAdapterError';
  }
}

export class AdapterBindingConflictError extends Error {
  readonly code = 'ADAPTER_BINDING_CONFLICT';

  constructor() {
    super('ADAPTER_BINDING_CONFLICT');
    this.name = 'AdapterBindingConflictError';
  }
}

function key(input: AdapterLookupKey): string {
  if (!SHA256_PATTERN.test(input.artifactSha256) || input.platformVersion.trim().length === 0) {
    throw new UnknownArtifactAdapterError();
  }
  return `${input.artifactSha256}\u0000${input.platformVersion}\u0000${input.scenario}`;
}

export class AdapterRegistry {
  private readonly adapters = new Map<string, ApeDtsConfigAdapter>();

  register(binding: AdapterBinding): void {
    if (binding.adapter.id.trim().length === 0 || binding.adapter.version.trim().length === 0) {
      throw new Error('ADAPTER_IDENTITY_INVALID');
    }
    const bindingKey = key(binding);
    if (this.adapters.has(bindingKey)) {
      throw new AdapterBindingConflictError();
    }
    this.adapters.set(bindingKey, binding.adapter);
  }

  resolve(input: AdapterLookupKey): ApeDtsConfigAdapter {
    const adapter = this.adapters.get(key(input));
    if (adapter === undefined) {
      throw new UnknownArtifactAdapterError();
    }
    return adapter;
  }
}

/** Resolves shipped adapters for runtime configuration and optional certification tooling. */
export class CertifiedAdapterRegistry extends AdapterRegistry {
  private readonly catalog = new Map<string, ApeDtsConfigAdapter>();

  constructor(adapters: readonly ApeDtsConfigAdapter[]) {
    super();
    for (const adapter of adapters) {
      const identity = `${adapter.id}\u0000${adapter.version}`;
      if (adapter.id.trim().length === 0 || adapter.version.trim().length === 0) {
        throw new Error('ADAPTER_IDENTITY_INVALID');
      }
      if (this.catalog.has(identity)) throw new AdapterBindingConflictError();
      this.catalog.set(identity, adapter);
    }
  }

  resolveCertified(binding: CertifiedAdapterBinding): ApeDtsConfigAdapter {
    try {
      const existing = this.resolve(binding);
      if (existing.id !== binding.adapterId || existing.version !== binding.adapterVersion) {
        throw new AdapterBindingConflictError();
      }
      return existing;
    } catch (error) {
      if (!(error instanceof UnknownArtifactAdapterError)) throw error;
    }

    const adapter = this.catalog.get(`${binding.adapterId}\u0000${binding.adapterVersion}`);
    if (adapter === undefined) throw new UnknownArtifactAdapterError();
    this.register({
      adapter,
      artifactSha256: binding.artifactSha256,
      platformVersion: binding.platformVersion,
      scenario: binding.scenario,
    });
    return adapter;
  }

  resolveRuntime(binding: AdapterLookupKey): ApeDtsConfigAdapter {
    return this.resolveCertified({ ...binding, adapterId: 'ape-dts-ini', adapterVersion: '1' });
  }
}

export type { ApeDtsConfigAdapter, TaskScenario } from './types.js';
