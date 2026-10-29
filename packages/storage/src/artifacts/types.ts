export type ArtifactArchitecture = 'aarch64' | 'x86_64';

export type NamePolicy =
  | { readonly mode: 'REPORT_ONLY' }
  | { readonly allowedNames: readonly string[]; readonly mode: 'ALLOWLIST' };

export interface StaticInspectionPolicy {
  readonly allowedDynamicLibraries: NamePolicy;
  readonly allowedInterpreters: NamePolicy;
  readonly expectedArchitecture: ArtifactArchitecture;
}

export interface StaticArtifactInspection {
  readonly architecture: ArtifactArchitecture;
  readonly dynamicLibraries: readonly string[];
  readonly elfClass: 64;
  readonly format: 'ELF';
  readonly interpreter: string | null;
}

export interface StaticArtifactInspector {
  inspect(path: string, policy: StaticInspectionPolicy): Promise<StaticArtifactInspection>;
}

export interface ArtifactExecutionBinding {
  readonly descriptor: number;
  readonly path: string;
  readonly sha256: string;
  readonly size: number;
  close(): Promise<void>;
}

export interface ArtifactSignatureVerifier {
  verifyDigest(input: {
    readonly algorithm: string;
    readonly sha256: string;
    readonly signature: Uint8Array;
    readonly signerId: string;
  }): Promise<boolean>;
}

export type ArtifactSignaturePolicy =
  | { readonly mode: 'DISABLED' }
  | {
      readonly algorithm: string;
      readonly allowedSignerIds: readonly string[];
      readonly mode: 'REQUIRED';
      readonly signature: Uint8Array;
      readonly signerId: string;
      readonly verifier: ArtifactSignatureVerifier;
    };

export interface ArtifactSignatureResult {
  readonly mode: ArtifactSignaturePolicy['mode'];
  readonly signerId: string | null;
  readonly verified: boolean;
}
