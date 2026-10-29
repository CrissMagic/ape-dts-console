import { randomUUID } from 'node:crypto';

import { Inject, Injectable } from '@nestjs/common';
import type {
  BootstrapStatusResponse,
  InitializePlatformRequest,
  InitializePlatformResponse,
  InspectBootstrapDatabaseResponse,
  PostgresConnectionConfig,
  UnlockPlatformResponse,
} from '@scs/contracts';
import { MigrationRunner, PgMigrationStore, type SchemaClassification } from '@scs/database';
import {
  createInitialKeyMaterial,
  createTotpUri,
  encryptSecret,
  generateRecoveryCodes,
  generateTotpSecret,
  hashRecoveryCode,
  hashPassword,
  KeyUnwrapError,
  PLATFORM_TOTP_OPTIONS,
  unwrapKey,
  verifyTotp,
  type Argon2idParameters,
} from '@scs/security';

import { AUTH_RECOVERY_CODE_COUNT } from '../auth/auth-policy.js';
import { MigrationBundleService } from '../database/migration-bundle.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { EmergencyStopCodeLifecycleService } from '../emergency/emergency-stop-code-lifecycle.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { RecoveryCoordinatorService } from '../recovery/recovery-coordinator.service.js';
import { ARGON2ID_POLICY } from '../security/security-config.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { BootstrapAccessService } from './bootstrap-access.service.js';
import { BootstrapPersistenceService } from './bootstrap-persistence.service.js';
import { BootstrapRecoveryCompletionService } from './bootstrap-recovery-completion.service.js';
import { PendingEnrollmentService } from './pending-enrollment.service.js';
import { BootstrapStateService } from './bootstrap-state.service.js';

export type BootstrapCoordinatorErrorCode =
  | 'BOOTSTRAP_SESSION_UNAUTHORIZED'
  | 'CONTROLLER_LOCK_UNAVAILABLE'
  | 'INVALID_INITIAL_TOTP'
  | 'INVALID_UNLOCK_PASSPHRASE'
  | 'KDF_POLICY_NOT_CONFIGURED'
  | 'NO_PENDING_TOTP_ENROLLMENT'
  | 'NO_SCHEMA_INSPECTION'
  | 'RECOVERY_SESSION_NOT_READY';

export class BootstrapCoordinatorError extends Error {
  constructor(
    readonly code: BootstrapCoordinatorErrorCode,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'BootstrapCoordinatorError';
  }
}

function normalizeUsername(username: string): string {
  return username.normalize('NFKC').toLocaleLowerCase('en-US');
}

@Injectable()
export class BootstrapCoordinatorService {
  private lastClassification: SchemaClassification | undefined;
  private lastInspectionResponse: InspectBootstrapDatabaseResponse | undefined;

  constructor(
    private readonly access: BootstrapAccessService,
    private readonly state: BootstrapStateService,
    private readonly database: PlatformDatabaseService,
    private readonly migrations: MigrationBundleService,
    private readonly persistence: BootstrapPersistenceService,
    private readonly pendingEnrollment: PendingEnrollmentService,
    private readonly unlockedKey: UnlockedKeyService,
    private readonly readiness: PlatformReadinessService,
    private readonly emergencyStopCode: EmergencyStopCodeLifecycleService,
    private readonly recovery: RecoveryCoordinatorService,
    private readonly recoveryCompletion: BootstrapRecoveryCompletionService,
    @Inject(ARGON2ID_POLICY)
    private readonly argon2idPolicy: Argon2idParameters | null,
  ) {}

  getStatus(): BootstrapStatusResponse {
    return this.state.getStatus();
  }

  verifyInitializationCode(initializationCode: string) {
    const session = this.access.verifyInitializationCode(initializationCode);
    this.state.markCodeVerified();
    this.readiness.beginBootstrap();
    return session;
  }

  private authorize(sessionToken: string): void {
    if (!this.access.authorizeSession(sessionToken)) {
      throw new BootstrapCoordinatorError('BOOTSTRAP_SESSION_UNAUTHORIZED');
    }
  }

  async inspectDatabase(
    sessionToken: string,
    connection: PostgresConnectionConfig,
  ): Promise<InspectBootstrapDatabaseResponse> {
    this.authorize(sessionToken);
    const bundle = await this.migrations.get();
    const classification = await this.database.inspectAndUse(connection, bundle.manifest);
    this.lastClassification = classification;
    const phase = this.state.recordSchemaInspection(classification.state);
    const pending = new Set(classification.pendingVersions);
    const response: InspectBootstrapDatabaseResponse = {
      appliedVersion: classification.appliedVersion,
      issues: [...classification.issues],
      pendingMigrations: bundle.manifest.migrations
        .filter(({ version }) => pending.has(version))
        .map(({ description, platformVersion, version }) => ({
          description,
          platformVersion,
          version,
        })),
      phase,
      state: classification.state,
      targetVersion: classification.targetVersion,
    };
    this.lastInspectionResponse = response;
    return response;
  }

  getLastInspection(sessionToken: string): InspectBootstrapDatabaseResponse {
    this.authorize(sessionToken);
    if (this.lastInspectionResponse === undefined) {
      throw new BootstrapCoordinatorError('NO_SCHEMA_INSPECTION');
    }
    return this.lastInspectionResponse;
  }

  async initializePlatform(
    sessionToken: string,
    request: InitializePlatformRequest,
  ): Promise<InitializePlatformResponse> {
    this.authorize(sessionToken);
    if (this.argon2idPolicy === null) {
      throw new BootstrapCoordinatorError('KDF_POLICY_NOT_CONFIGURED');
    }
    this.state.beginInitialization();
    const instanceId = this.state.getStatus().platformInstanceId;
    const administratorId = randomUUID();
    const recoverySessionId = randomUUID();
    const startedAt = new Date().toISOString();
    const normalizedUsername = normalizeUsername(request.administrator.username);
    let generatedDek: Buffer | undefined;
    let generatedTotpSecret: Buffer | undefined;

    try {
      const bundle = await this.migrations.get();
      const keyMaterial = await createInitialKeyMaterial(
        request.unlockPassphrase,
        this.argon2idPolicy,
      );
      generatedDek = keyMaterial.dek;
      const password = await hashPassword(request.administrator.password, this.argon2idPolicy);
      generatedTotpSecret = generateTotpSecret();
      const totpUri = createTotpUri(
        normalizedUsername,
        'SCS ape-dts Platform',
        generatedTotpSecret,
      );
      const runner = new MigrationRunner(new PgMigrationStore(this.database.requirePool()));
      await runner.migrate(bundle, instanceId, {
        afterMigrations: async (transaction) => {
          await this.persistence.insertInitialPlatformRows(transaction, {
            administrator: {
              displayName: request.administrator.displayName,
              id: administratorId,
              normalizedUsername,
              password,
            },
            instanceId,
            passphraseWrapper: keyMaterial.passphraseWrapper,
            recoverySessionId,
            recoveryWrapper: keyMaterial.recoveryWrapper,
            startedAt,
          });
        },
        expectedState: 'ABSENT',
      });
      this.database.notifySchemaCurrent();

      this.unlockedKey.set(generatedDek);
      this.pendingEnrollment.set({
        administratorId,
        recoveryKey: keyMaterial.recoveryKey,
        recoverySessionId,
        totpSecret: generatedTotpSecret,
        totpUri,
      });
      this.state.requireTotpEnrollment();
      return {
        administratorId,
        phase: 'TOTP_ENROLLMENT_REQUIRED',
        recoveryKey: keyMaterial.recoveryKey,
        totpUri,
      };
    } catch (error) {
      if (this.state.getStatus().phase === 'INITIALIZING') {
        this.state.markInitializationFailed();
      }
      this.unlockedKey.clear();
      this.pendingEnrollment.clear();
      throw error;
    } finally {
      generatedDek?.fill(0);
      generatedTotpSecret?.fill(0);
    }
  }

  async verifyInitialTotp(sessionToken: string, code: string): Promise<readonly string[]> {
    this.authorize(sessionToken);
    if (this.state.getStatus().phase === 'BLOCKED_CONCURRENT_INSTANCE') {
      this.state.resumeTotpAfterConcurrentBlock();
    }
    const pending = this.pendingEnrollment.require();
    const acceptedCounter = verifyTotp(pending.totpSecret, code, Date.now(), {
      ...PLATFORM_TOTP_OPTIONS,
      window: 1,
    });
    if (acceptedCounter === null) {
      throw new BootstrapCoordinatorError('INVALID_INITIAL_TOTP');
    }

    this.state.beginRecovery();
    if (!(await this.database.acquireControllerLock())) {
      this.state.markConcurrentInstanceBlocked();
      throw new BootstrapCoordinatorError('CONTROLLER_LOCK_UNAVAILABLE');
    }

    const secretId = randomUUID();
    const encryptedTotpSecret = encryptSecret(this.unlockedKey.require(), pending.totpSecret, {
      aadVersion: 1,
      ownerId: pending.administratorId,
      ownerType: 'USER',
      purpose: 'TOTP_SEED',
      secretId,
    });
    const recoveryCodes = generateRecoveryCodes(AUTH_RECOVERY_CODE_COUNT);
    try {
      await this.persistence.completeInitialAdministrator(this.database.requirePool(), {
        acceptedTotpCounter: acceptedCounter,
        administratorId: pending.administratorId,
        encryptedTotpSecret,
        recoveryCodeHashes: recoveryCodes.map(hashRecoveryCode),
        recoverySessionId: pending.recoverySessionId,
        secretId,
      });
      await this.database.probe();
    } catch (error) {
      await this.database.releaseControllerLock().catch(() => undefined);
      this.state.markRecoveryFailedToTotp();
      throw error;
    }

    this.emergencyStopCode.activateForReadyPlatform();
    this.state.markReady();
    this.readiness.markReady();
    this.pendingEnrollment.clear();
    this.access.invalidateSession();
    return recoveryCodes;
  }

  getPendingEnrollment(sessionToken: string): InitializePlatformResponse {
    this.authorize(sessionToken);
    if (!this.pendingEnrollment.hasPending()) {
      throw new BootstrapCoordinatorError('NO_PENDING_TOTP_ENROLLMENT');
    }
    const pending = this.pendingEnrollment.require();
    return {
      administratorId: pending.administratorId,
      phase: 'TOTP_ENROLLMENT_REQUIRED',
      recoveryKey: pending.recoveryKey,
      totpUri: pending.totpUri,
    };
  }

  async migrateSchema(sessionToken: string): Promise<void> {
    this.authorize(sessionToken);
    if (this.lastClassification === undefined) {
      throw new BootstrapCoordinatorError('NO_SCHEMA_INSPECTION');
    }
    this.state.beginMigration();
    try {
      const bundle = await this.migrations.get();
      const instanceId = this.state.getStatus().platformInstanceId;
      await new MigrationRunner(new PgMigrationStore(this.database.requirePool())).migrate(
        bundle,
        instanceId,
        {
          afterMigrations: async (transaction) => {
            await this.persistence.recordMigrationConfirmation(
              transaction,
              instanceId,
              this.lastClassification?.pendingVersions ?? [],
            );
          },
          expectedState: 'OLDER',
        },
      );
      this.database.notifySchemaCurrent();
      this.state.markMigrationSucceeded();
    } catch (error) {
      if (this.state.getStatus().phase === 'MIGRATING') {
        this.state.markMigrationFailed();
      }
      throw error;
    }
  }

  async unlockExistingPlatform(
    sessionToken: string,
    passphrase: string,
  ): Promise<UnlockPlatformResponse> {
    this.authorize(sessionToken);
    if (this.pendingEnrollment.hasPending()) {
      throw new BootstrapCoordinatorError('INVALID_UNLOCK_PASSPHRASE');
    }
    this.state.beginUnlock();
    let dek: Buffer | undefined;
    try {
      const wrapper = await this.persistence.loadActivePassphraseWrapper(
        this.database.requirePool(),
      );
      dek = await unwrapKey(wrapper, { kind: 'PASSPHRASE', value: passphrase });
    } catch (error) {
      this.state.markUnlockFailed();
      if (error instanceof KeyUnwrapError) {
        throw new BootstrapCoordinatorError('INVALID_UNLOCK_PASSPHRASE');
      }
      throw error;
    }

    if (!(await this.database.acquireControllerLock())) {
      dek.fill(0);
      this.state.markConcurrentInstanceBlocked();
      throw new BootstrapCoordinatorError('CONTROLLER_LOCK_UNAVAILABLE');
    }
    this.state.beginRecovery();
    let recoverySessionId: string;
    try {
      recoverySessionId = await this.persistence.startExistingRecovery(
        this.database.requirePool(),
        this.state.getStatus().platformInstanceId,
      );
      this.unlockedKey.set(dek);
    } catch (error) {
      await this.database.releaseControllerLock().catch(() => undefined);
      this.state.markRecoveryFailedToCurrentSchema();
      throw error;
    } finally {
      dek.fill(0);
    }

    await this.completeExistingRecovery(recoverySessionId);
    this.recovery.startForPlatform(recoverySessionId);
    return { phase: 'READY', recoverySessionId, recoveryState: 'RUNNING' };
  }

  async completeExistingRecovery(recoverySessionId: string): Promise<void> {
    if (!(await this.recoveryCompletion.complete(recoverySessionId))) {
      throw new BootstrapCoordinatorError('RECOVERY_SESSION_NOT_READY');
    }
  }
}
