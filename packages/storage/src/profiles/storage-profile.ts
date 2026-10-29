import { LOCAL_STORAGE_ROOT } from '../drivers/local/local-storage.js';
import type { StorageDriverKind } from '../drivers/types.js';

interface StorageProfileBase {
  readonly id: string;
  readonly logicalName: string;
  readonly version: number;
}

export interface LocalStorageProfileSnapshot extends StorageProfileBase {
  readonly config: {
    readonly relativePrefix: string;
    readonly root: typeof LOCAL_STORAGE_ROOT;
  };
  readonly driver: 'LOCAL';
  readonly secretId: null;
}

export interface OssStorageProfileSnapshot extends StorageProfileBase {
  readonly config: {
    readonly bucket: string;
    readonly endpoint: string;
    readonly prefix: string;
  };
  readonly driver: 'OSS';
  readonly secretId: string;
}

export type StorageProfileSnapshot = LocalStorageProfileSnapshot | OssStorageProfileSnapshot;

export type StorageProfileInput =
  | (Omit<LocalStorageProfileSnapshot, 'config'> & {
      readonly config: Omit<LocalStorageProfileSnapshot['config'], 'root'>;
    })
  | OssStorageProfileSnapshot;

function validateBase(input: StorageProfileInput): void {
  if (input.id.trim().length === 0 || input.logicalName.trim().length === 0) {
    throw new Error('STORAGE_PROFILE_IDENTITY_INVALID');
  }
  if (!Number.isSafeInteger(input.version) || input.version <= 0) {
    throw new Error('STORAGE_PROFILE_VERSION_INVALID');
  }
}

function validateRelativePrefix(prefix: string): void {
  const segments = prefix.split('/');
  if (
    prefix.startsWith('/') ||
    prefix.endsWith('/') ||
    prefix.includes('\\') ||
    (prefix.length > 0 &&
      segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')) ||
    hasControlCharacter(prefix)
  ) {
    throw new Error('STORAGE_PROFILE_PREFIX_INVALID');
  }
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 0x1f || code === 0x7f;
  });
}

export function createStorageProfileSnapshot(input: StorageProfileInput): StorageProfileSnapshot {
  validateBase(input);
  if (input.driver === 'LOCAL') {
    validateRelativePrefix(input.config.relativePrefix);
    const snapshot: LocalStorageProfileSnapshot = {
      ...input,
      config: Object.freeze({
        relativePrefix: input.config.relativePrefix,
        root: LOCAL_STORAGE_ROOT,
      }),
      driver: 'LOCAL',
      secretId: null,
    };
    return Object.freeze(snapshot);
  }
  if (input.secretId.trim().length === 0) {
    throw new Error('STORAGE_PROFILE_SECRET_REQUIRED');
  }
  validateRelativePrefix(input.config.prefix);
  const snapshot: OssStorageProfileSnapshot = {
    ...input,
    config: Object.freeze({ ...input.config }),
    driver: 'OSS',
  };
  return Object.freeze(snapshot);
}

export function isStorageDriverKind(value: string): value is StorageDriverKind {
  return value === 'LOCAL' || value === 'OSS';
}
