import 'reflect-metadata';

import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';

import { NestFactory } from '@nestjs/core';
import { postgresConnectionConfigSchema } from '@scs/contracts';
import { MigrationRunner, PgMigrationStore } from '@scs/database';
import { KeyUnwrapError, unwrapKey } from '@scs/security';

import { MigrationBundleService } from '../database/migration-bundle.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { BootstrapModule } from './bootstrap.module.js';
import { BootstrapPersistenceService } from './bootstrap-persistence.service.js';
import { waitForUpgradeControllerLock } from './upgrade-controller-lock.js';
import {
  inspectAdministratorIdentity,
  migrateAdministratorIdentity,
} from './administrator-identity-migration.js';
import {
  makeLocalPlatformConfig,
  readLocalPlatformConfig,
  wrapperFingerprint,
  writeLocalPlatformConfig,
} from './local-platform-config.js';

async function readPassphrase(): Promise<string> {
  let value = '';
  for await (const chunk of process.stdin) {
    value += String(chunk);
    if (value.length > 1024) throw new Error('INVALID_UNLOCK_PASSPHRASE');
  }
  value = value.replace(/\r?\n$/u, '');
  if (value.length === 0 || value.length > 256) throw new Error('INVALID_UNLOCK_PASSPHRASE');
  return value;
}

export async function runSecurityCli(
  args: readonly string[],
  passphraseInput: () => Promise<string> = readPassphrase,
): Promise<void> {
  if (args.length === 1 && args[0] === '--help') {
    process.stdout.write(
      'Usage: security-cli.js migrate-security|upgrade --backup-confirmed [--administrator USERNAME]\n',
    );
    return;
  }
  const command = args[0];
  if (
    !['migrate-security', 'upgrade'].includes(command ?? '') ||
    ![2, 4].includes(args.length) ||
    args[1] !== '--backup-confirmed' ||
    (args.length === 4 &&
      (args[2] !== '--administrator' || !/^[\p{L}\p{N}._-]{3,64}$/u.test(args[3] ?? '')))
  ) {
    throw new Error('INVALID_SECURITY_COMMAND_ARGUMENTS');
  }
  const local = await readLocalPlatformConfig();
  if (command === 'migrate-security' && local !== null)
    throw new Error('LOCAL_PLATFORM_CONFIG_ALREADY_EXISTS');
  if (command === 'upgrade' && local === null) throw new Error('LOCAL_PLATFORM_CONFIG_REQUIRED');
  const connection =
    local?.connection ??
    postgresConnectionConfigSchema.parse(
      JSON.parse(process.env.SCS_PLATFORM_DATABASE_CONFIG ?? '{}'),
    );
  const app = await NestFactory.createApplicationContext(BootstrapModule, { logger: false });
  let dek: Buffer | undefined;
  try {
    const database = app.get(PlatformDatabaseService);
    const persistence = app.get(BootstrapPersistenceService);
    const bundle = await app.get(MigrationBundleService).get();
    const classification = await database.inspectAndUse(connection, bundle.manifest);
    if (!['CURRENT', 'OLDER'].includes(classification.state))
      throw new Error('EXISTING_VALID_DATABASE_REQUIRED');
    if (command === 'upgrade') await waitForUpgradeControllerLock(database);
    else if (!(await database.acquireControllerLock()))
      throw new Error('STOP_PLATFORM_BEFORE_SECURITY_MIGRATION');
    const selected = await inspectAdministratorIdentity(
      { query: (sql, values) => database.requirePool().query(sql, [...(values ?? [])]) },
      args[3]?.normalize('NFKC').toLocaleLowerCase('en-US'),
    );
    const wrapper = await persistence.loadActivePassphraseWrapper(database.requirePool());
    if (local === null) {
      dek = await unwrapKey(wrapper, { kind: 'PASSPHRASE', value: await passphraseInput() });
    } else if (wrapperFingerprint(wrapper) !== local.wrapperFingerprint) {
      throw new Error('LOCAL_KEY_DATABASE_MISMATCH');
    }
    if (classification.state === 'OLDER') {
      const instanceId = randomUUID();
      await new MigrationRunner(new PgMigrationStore(database.requirePool())).migrate(
        bundle,
        instanceId,
        {
          expectedState: 'OLDER',
          afterMigrations: async (transaction) => {
            await migrateAdministratorIdentity(transaction, selected);
            await persistence.recordMigrationConfirmation(
              transaction,
              instanceId,
              classification.pendingVersions,
            );
            if (dek !== undefined)
              await writeLocalPlatformConfig(makeLocalPlatformConfig(connection, dek, wrapper));
          },
        },
      );
    } else {
      const client = await database.requirePool().connect();
      try {
        await client.query('BEGIN');
        await migrateAdministratorIdentity(
          { query: (sql, values) => client.query(sql, [...(values ?? [])]) },
          selected,
        );
        if (dek !== undefined)
          await writeLocalPlatformConfig(makeLocalPlatformConfig(connection, dek, wrapper));
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    }
    process.stdout.write('Security configuration saved; start the platform to resume.\n');
  } finally {
    dek?.fill(0);
    await app.close();
  }
}

export function securityCommandErrorCode(error: unknown): string {
  if (error instanceof KeyUnwrapError) return 'INVALID_UNLOCK_PASSPHRASE';
  const allowed = new Set([
    'INVALID_SECURITY_COMMAND_ARGUMENTS',
    'LOCAL_PLATFORM_CONFIG_ALREADY_EXISTS',
    'LOCAL_PLATFORM_CONFIG_REQUIRED',
    'EXISTING_VALID_DATABASE_REQUIRED',
    'STOP_PLATFORM_BEFORE_SECURITY_MIGRATION',
    'LOCAL_KEY_DATABASE_MISMATCH',
    'LOCAL_PLATFORM_CONFIG_UNSAFE_DIRECTORY',
    'LOCAL_PLATFORM_CONFIG_UNSAFE_PERMISSIONS',
    'LOCAL_PLATFORM_CONFIG_UNSAFE_FILE',
    'LOCAL_PLATFORM_CONFIG_INVALID_KEY',
    'PLATFORM_DATABASE_CONNECTION_FAILED',
    'INVALID_UNLOCK_PASSPHRASE',
    'ADMIN_USERNAME_CONFLICT',
    'ADMIN_IDENTITY_MIGRATION_REQUIRED',
    'SELECTED_ADMINISTRATOR_NOT_FOUND',
    'ADMIN_IDENTITY_MIGRATION_CONFLICT',
  ]);
  return error instanceof Error && allowed.has(error.message)
    ? error.message
    : 'SECURITY_COMMAND_FAILED';
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runSecurityCli(process.argv.slice(2)).catch((error: unknown) => {
    process.stderr.write(`${securityCommandErrorCode(error)}\n`);
    process.exitCode = 1;
  });
}
