import { constants } from 'node:fs';
import { chmod, lstat, mkdir, open, realpath, unlink } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

import { postgresConnectionConfigSchema } from '@scs/contracts';
import { decryptSecret, encryptSecret, type WrappedKey } from '@scs/security';
import { z } from 'zod';

const configSchema = z
  .object({
    version: z.literal(1),
    connection: postgresConnectionConfigSchema,
    dek: z.string().regex(/^[A-Za-z0-9+/]{43}=$/u),
    wrapperFingerprint: z.string().regex(/^[a-f0-9]{64}$/u),
    keyCheckId: z.uuid(),
    keyCheck: z
      .object({
        aadVersion: z.literal(1),
        algorithm: z.literal('AES_256_GCM'),
        ciphertext: z.string(),
        nonce: z.string(),
        tag: z.string(),
      })
      .strict(),
  })
  .strict();

export type LocalPlatformConfig = z.infer<typeof configSchema>;
const checkPlaintext = Buffer.from('scs-platform-local-key-v1');
const checkContext = (id: string) => ({
  aadVersion: 1 as const,
  ownerId: id,
  ownerType: 'PLATFORM',
  purpose: 'LOCAL_KEY_CHECK',
  secretId: id,
});

export function localPlatformConfigPath(): string {
  return resolve(process.env.SCS_PLATFORM_CONFIG_PATH ?? '/data/storage/.platform/config.json');
}

export function wrapperFingerprint(wrapper: WrappedKey): string {
  return createHash('sha256').update(Buffer.from(wrapper.ciphertext, 'base64')).digest('hex');
}

export function makeLocalPlatformConfig(
  connection: LocalPlatformConfig['connection'],
  dek: Buffer,
  wrapper: WrappedKey,
): LocalPlatformConfig {
  const keyCheckId = randomUUID();
  return {
    version: 1,
    connection,
    dek: dek.toString('base64'),
    keyCheckId,
    keyCheck: encryptSecret(dek, checkPlaintext, checkContext(keyCheckId)),
    wrapperFingerprint: wrapperFingerprint(wrapper),
  };
}

async function validateDirectory(path: string, create: boolean): Promise<string | undefined> {
  const directory = dirname(resolve(path));
  const firstCreated = create
    ? await mkdir(directory, { recursive: true, mode: 0o700 })
    : undefined;
  const stat = await lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || (await realpath(directory)) !== directory) {
    throw new Error('LOCAL_PLATFORM_CONFIG_UNSAFE_DIRECTORY');
  }
  if (process.platform !== 'win32' && (stat.mode & 0o077) !== 0) {
    throw new Error('LOCAL_PLATFORM_CONFIG_UNSAFE_PERMISSIONS');
  }
  return firstCreated;
}

async function syncDirectory(directory: string): Promise<void> {
  if (process.platform === 'win32') return;
  const handle = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/** Persist both the file name and every directory entry created by recursive mkdir. */
export async function syncConfigDirectoryEntries(
  directory: string,
  firstCreated: string | undefined,
  synchronize: (path: string) => Promise<void> = syncDirectory,
): Promise<void> {
  const stop = firstCreated === undefined ? directory : dirname(firstCreated);
  let current = directory;
  while (true) {
    await synchronize(current);
    if (current === stop) return;
    const parent = dirname(current);
    if (parent === current) throw new Error('LOCAL_PLATFORM_CONFIG_INVALID_DIRECTORY_BOUNDARY');
    current = parent;
  }
}

export async function readLocalPlatformConfig(
  path = localPlatformConfigPath(),
): Promise<LocalPlatformConfig | null> {
  try {
    const stat = await lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink()) {
      throw new Error('LOCAL_PLATFORM_CONFIG_UNSAFE_FILE');
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw error;
  }
  await validateDirectory(path, false);
  const handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const stat = await handle.stat();
    if (
      !stat.isFile() ||
      stat.size > 128 * 1024 ||
      (process.platform !== 'win32' && (stat.mode & 0o077) !== 0)
    ) {
      throw new Error('LOCAL_PLATFORM_CONFIG_UNSAFE_FILE');
    }
    const config = configSchema.parse(JSON.parse(await handle.readFile('utf8')));
    const dek = Buffer.from(config.dek, 'base64');
    try {
      const plain = decryptSecret(dek, config.keyCheck, checkContext(config.keyCheckId));
      if (!plain.equals(checkPlaintext)) throw new Error('LOCAL_PLATFORM_CONFIG_INVALID_KEY');
      plain.fill(0);
    } finally {
      dek.fill(0);
    }
    return config;
  } finally {
    await handle.close();
  }
}

/** Exclusive creation: an existing or damaged key file is never replaced. */
export async function writeLocalPlatformConfig(
  config: LocalPlatformConfig,
  path = localPlatformConfigPath(),
): Promise<void> {
  const firstCreated = await validateDirectory(path, true);
  const handle = await open(path, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
  try {
    await handle.writeFile(JSON.stringify(config));
    await handle.sync();
    if (process.platform !== 'win32') await chmod(path, 0o600);
    if (process.platform !== 'win32') {
      await syncConfigDirectoryEntries(dirname(resolve(path)), firstCreated);
    }
  } catch (error) {
    await handle.close();
    await unlink(path);
    throw error;
  }
  await handle.close();
}
