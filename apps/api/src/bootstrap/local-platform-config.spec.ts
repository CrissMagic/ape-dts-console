import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';

import { afterEach, describe, expect, it } from 'vitest';
import type { WrappedKey } from '@scs/security';

import {
  makeLocalPlatformConfig,
  readLocalPlatformConfig,
  writeLocalPlatformConfig,
  syncConfigDirectoryEntries,
} from './local-platform-config.js';

const temporaryDirectories: string[] = [];
const connection = {
  host: 'localhost',
  port: 5432,
  username: 'owner',
  password: 'database-secret',
  database: 'platform',
  tls: { mode: 'DISABLE' as const },
};
const wrapper = { ciphertext: randomBytes(32).toString('base64') } as WrappedKey;
async function location() {
  const directory = await mkdtemp(join(tmpdir(), 'scs-local-config-'));
  temporaryDirectories.push(directory);
  return join(directory, 'private', 'config.json');
}
afterEach(async () => {
  for (const path of temporaryDirectories.splice(0))
    await rm(path, { recursive: true, force: true });
});

describe('local platform key persistence', () => {
  it('persists the file directory and every newly created ancestor entry before success', async () => {
    const root = join(tmpdir(), 'durability-example');
    const created = join(root, 'new');
    const directory = join(created, 'private');
    const synchronized: string[] = [];
    await syncConfigDirectoryEntries(directory, created, async (path) => {
      synchronized.push(path);
    });
    expect(synchronized).toEqual([directory, created, root]);
    synchronized.length = 0;
    await syncConfigDirectoryEntries(directory, undefined, async (path) => {
      synchronized.push(path);
    });
    expect(synchronized).toEqual([directory]);
  });

  it('does not report durability when a directory sync fails', async () => {
    const directory = join(tmpdir(), 'private');
    await expect(
      syncConfigDirectoryEntries(directory, undefined, async () => {
        throw new Error('directory fsync failed');
      }),
    ).rejects.toThrow('directory fsync failed');
  });
  it('loads the same key and database connection across restarts', async () => {
    const path = await location();
    const config = makeLocalPlatformConfig(connection, randomBytes(32), wrapper);
    expect(await readLocalPlatformConfig(path)).toBeNull();
    await writeLocalPlatformConfig(config, path);
    expect(await readLocalPlatformConfig(path)).toEqual(config);
  });

  it('never overwrites an existing key file', async () => {
    const path = await location();
    const config = makeLocalPlatformConfig(connection, randomBytes(32), wrapper);
    await writeLocalPlatformConfig(config, path);
    await expect(
      writeLocalPlatformConfig(makeLocalPlatformConfig(connection, randomBytes(32), wrapper), path),
    ).rejects.toThrow();
    expect(await readLocalPlatformConfig(path)).toEqual(config);
  });

  it('rejects a changed key and preserves the damaged file for recovery', async () => {
    const path = await location();
    const config = makeLocalPlatformConfig(connection, randomBytes(32), wrapper);
    await writeLocalPlatformConfig(config, path);
    const damaged = JSON.stringify({ ...config, dek: randomBytes(32).toString('base64') });
    await writeFile(path, damaged);
    await expect(readLocalPlatformConfig(path)).rejects.toThrow();
    expect(await readFile(path, 'utf8')).toBe(damaged);
  });
});
