import 'reflect-metadata';
import { pathToFileURL } from 'node:url';
import { NestFactory } from '@nestjs/core';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { AuditModule } from '../audit/audit.module.js';
import { readLocalPlatformConfig, wrapperFingerprint } from '../bootstrap/local-platform-config.js';
import { BootstrapPersistenceService } from '../bootstrap/bootstrap-persistence.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { MigrationBundleService } from '../database/migration-bundle.service.js';
import { readArgon2idPolicy } from '../security/security-config.js';
import { resetAdministratorPassword } from './reset-administrator-password.js';

/** Runs only with container access; there is deliberately no unauthenticated HTTP reset route. */
export async function resetAdmin(): Promise<void> {
  const config = await readLocalPlatformConfig();
  if (config === null) throw new Error('LOCAL_PLATFORM_CONFIG_REQUIRED');
  const policy = readArgon2idPolicy();
  if (policy === null) throw new Error('KDF_POLICY_NOT_CONFIGURED');
  const app = await NestFactory.createApplicationContext(AuditModule, { logger: false });
  try {
    const database = app.get(PlatformDatabaseService);
    const bundle = await app.get(MigrationBundleService).get();
    const state = await database.inspectAndUse(config.connection, bundle.manifest);
    if (state.state !== 'CURRENT') throw new Error('DATABASE_UPGRADE_REQUIRED');
    const wrapper = await app
      .get(BootstrapPersistenceService)
      .loadActivePassphraseWrapper(database.requirePool());
    if (wrapperFingerprint(wrapper) !== config.wrapperFingerprint)
      throw new Error('LOCAL_KEY_DATABASE_MISMATCH');
    const temporaryPassword = await resetAdministratorPassword(
      database.requirePool(),
      app.get(AuditLedgerService),
      policy,
    );
    process.stdout.write(
      `admin 临时密码：${temporaryPassword}\n旧会话已失效，下次登录必须修改密码。\n`,
    );
  } finally {
    await app.close();
  }
}

export async function runResetAdminCli(args: readonly string[]): Promise<void> {
  if (args.length === 1 && args[0] === '--help') {
    process.stdout.write(
      'Usage: node dist/auth/reset-admin-cli.js\nReset only admin; revoke its sessions and require a password change.\n',
    );
    return;
  }
  if (args.length !== 0) throw new Error('INVALID_ARGUMENTS');
  await resetAdmin();
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void runResetAdminCli(process.argv.slice(2)).catch((error: unknown) => {
    const allowed = new Set([
      'LOCAL_PLATFORM_CONFIG_REQUIRED',
      'KDF_POLICY_NOT_CONFIGURED',
      'DATABASE_UPGRADE_REQUIRED',
      'LOCAL_KEY_DATABASE_MISMATCH',
      'ADMIN_ACCOUNT_NOT_FOUND',
      'INVALID_ARGUMENTS',
    ]);
    // Raw database/validation exceptions may contain connection credentials.
    const message =
      error instanceof Error && allowed.has(error.message) ? error.message : 'ADMIN_RESET_FAILED';
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  });
}
