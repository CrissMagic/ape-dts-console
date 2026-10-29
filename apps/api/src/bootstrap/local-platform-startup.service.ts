import { randomBytes, randomUUID } from 'node:crypto';

import { Inject, Injectable, Logger, type OnApplicationBootstrap } from '@nestjs/common';
import { postgresConnectionConfigSchema, type PostgresConnectionConfig } from '@scs/contracts';
import { MigrationRunner, PgMigrationStore } from '@scs/database';
import { createInitialKeyMaterial, hashPassword, type Argon2idParameters } from '@scs/security';

import { MigrationBundleService } from '../database/migration-bundle.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { RecoveryCoordinatorService } from '../recovery/recovery-coordinator.service.js';
import { ARGON2ID_POLICY } from '../security/security-config.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { BootstrapPersistenceService } from './bootstrap-persistence.service.js';
import { BootstrapRecoveryCompletionService } from './bootstrap-recovery-completion.service.js';
import { BootstrapStateService } from './bootstrap-state.service.js';
import {
  makeLocalPlatformConfig,
  readLocalPlatformConfig,
  wrapperFingerprint,
  writeLocalPlatformConfig,
} from './local-platform-config.js';

@Injectable()
export class LocalPlatformStartupService implements OnApplicationBootstrap {
  constructor(
    private readonly state: BootstrapStateService,
    private readonly database: PlatformDatabaseService,
    private readonly migrations: MigrationBundleService,
    private readonly persistence: BootstrapPersistenceService,
    private readonly unlockedKey: UnlockedKeyService,
    private readonly readiness: PlatformReadinessService,
    private readonly recovery: RecoveryCoordinatorService,
    private readonly completion: BootstrapRecoveryCompletionService,
    @Inject(ARGON2ID_POLICY) private readonly policy: Argon2idParameters | null,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    try {
      await this.start();
    } catch {
      // Close recovery HTTP access before releasing its key and exclusive controller lease.
      this.state.markStartupFailed('LOCAL_STARTUP_FAILED');
      this.readiness.markSealed();
      this.unlockedKey.clear();
      await this.database.releaseControllerLock();
      // Never log raw exceptions: connection configuration may contain credentials.
      Logger.error(
        'LOCAL_STARTUP_FAILED: check database configuration and persistent key file',
        'PlatformStartup',
      );
    }
  }

  async start(): Promise<void> {
    const local = await readLocalPlatformConfig();
    const raw = process.env.SCS_PLATFORM_DATABASE_CONFIG;
    const connection =
      local?.connection ??
      (raw === undefined ? undefined : postgresConnectionConfigSchema.parse(JSON.parse(raw)));
    if (connection === undefined) {
      this.state.setStartupIssue('DATABASE_CONFIGURATION_REQUIRED');
      return;
    }
    this.state.markCodeVerified();
    this.readiness.beginBootstrap();
    const bundle = await this.migrations.get();
    const classification = await this.database.inspectAndUse(connection, bundle.manifest);
    this.state.recordSchemaInspection(classification.state);
    if (classification.state === 'ABSENT') {
      if (local !== null) throw new Error('LOCAL_KEY_DATABASE_MISSING');
      await this.initialize(connection);
      return;
    }
    if (local === null) {
      this.state.setStartupIssue('LEGACY_SECURITY_MIGRATION_REQUIRED');
      return;
    }
    if (classification.state !== 'CURRENT') {
      this.state.setStartupIssue('DATABASE_UPGRADE_REQUIRED');
      return;
    }
    const wrapper = await this.persistence.loadActivePassphraseWrapper(this.database.requirePool());
    if (wrapperFingerprint(wrapper) !== local.wrapperFingerprint)
      throw new Error('LOCAL_KEY_DATABASE_MISMATCH');
    const dek = Buffer.from(local.dek, 'base64');
    try {
      this.state.beginUnlock();
      if (!(await this.database.acquireControllerLock())) {
        this.state.markConcurrentInstanceBlocked();
        return;
      }
      this.unlockedKey.set(dek);
      await this.persistence.ensureDefaultLocalStorage(this.database.requirePool());
      this.state.beginRecovery();
      const session = await this.persistence.startExistingRecovery(
        this.database.requirePool(),
        this.state.getStatus().platformInstanceId,
      );
      if (!(await this.completion.complete(session)))
        throw new Error('PLATFORM_METADATA_NOT_READY');
      this.recovery.startForPlatform(session);
    } finally {
      dek.fill(0);
    }
  }

  private async initialize(connection: PostgresConnectionConfig): Promise<void> {
    if (this.policy === null) throw new Error('KDF_POLICY_NOT_CONFIGURED');
    this.state.beginInitialization();
    const instanceId = this.state.getStatus().platformInstanceId;
    const administratorId = randomUUID();
    const recoverySessionId = randomUUID();
    const temporaryPassword = randomBytes(24).toString('base64url');
    const material = await createInitialKeyMaterial(
      randomBytes(48).toString('base64url'),
      this.policy,
    );
    try {
      const password = await hashPassword(temporaryPassword, this.policy);
      const bundle = await this.migrations.get();
      await new MigrationRunner(new PgMigrationStore(this.database.requirePool())).migrate(
        bundle,
        instanceId,
        {
          expectedState: 'ABSENT',
          afterMigrations: async (transaction) => {
            await this.persistence.insertInitialPlatformRows(transaction, {
              administrator: {
                id: administratorId,
                displayName: '管理员',
                normalizedUsername: 'admin',
                password,
              },
              instanceId,
              passphraseWrapper: material.passphraseWrapper,
              recoveryWrapper: material.recoveryWrapper,
              recoverySessionId,
              startedAt: new Date().toISOString(),
              passwordOnly: true,
            });
            // Persist before DB commit: a failed commit leaves a recoverable key, never an unreadable DB.
            await writeLocalPlatformConfig(
              makeLocalPlatformConfig(connection, material.dek, material.passphraseWrapper),
            );
          },
        },
      );
      Logger.warn(
        JSON.stringify({
          event: 'bootstrap.administrator_created',
          username: 'admin',
          temporaryPassword,
          message: '首次登录必须修改密码；此临时密码只输出一次。',
        }),
        'PlatformStartup',
      );
      this.database.notifySchemaCurrent();
      this.state.beginRecovery();
      if (!(await this.database.acquireControllerLock())) {
        this.state.markConcurrentInstanceBlocked();
        return;
      }
      this.unlockedKey.set(material.dek);
      await this.persistence.ensureDefaultLocalStorage(this.database.requirePool());
      if (!(await this.completion.complete(recoverySessionId)))
        throw new Error('PLATFORM_METADATA_NOT_READY');
      this.recovery.startForPlatform(recoverySessionId);
    } finally {
      material.dek.fill(0);
    }
  }
}
