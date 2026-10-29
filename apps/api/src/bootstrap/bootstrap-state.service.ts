import { randomUUID } from 'node:crypto';

import { Injectable } from '@nestjs/common';
import type { BootstrapPhase, BootstrapStatusResponse } from '@scs/contracts';
import type { SchemaState } from '@scs/database';

export class BootstrapStateError extends Error {
  readonly code = 'INVALID_BOOTSTRAP_TRANSITION';

  constructor(
    readonly current: BootstrapPhase,
    readonly requested: BootstrapPhase,
  ) {
    super(`INVALID_BOOTSTRAP_TRANSITION:${current}:${requested}`);
    this.name = 'BootstrapStateError';
  }
}

const INSPECTION_PHASE: Readonly<Record<SchemaState, BootstrapPhase>> = {
  ABSENT: 'SCHEMA_ABSENT',
  CORRUPT: 'BLOCKED_CORRUPT_SCHEMA',
  CURRENT: 'SCHEMA_CURRENT',
  NEWER: 'BLOCKED_NEWER_SCHEMA',
  OLDER: 'MIGRATION_REQUIRED',
};

@Injectable()
export class BootstrapStateService {
  private phase: BootstrapPhase = 'SEALED';
  private startupIssue: string | undefined;

  constructor(private readonly platformInstanceId: string = randomUUID()) {}

  getStatus(): BootstrapStatusResponse {
    return {
      phase: this.phase,
      platformInstanceId: this.platformInstanceId,
      ...(this.startupIssue === undefined ? {} : { startupIssue: this.startupIssue }),
    };
  }

  setStartupIssue(issue: string): void {
    this.startupIssue = issue;
  }

  markStartupFailed(issue: string): void {
    this.phase = 'SEALED';
    this.startupIssue = issue;
  }

  private transition(next: BootstrapPhase, allowed: readonly BootstrapPhase[]): void {
    if (!allowed.includes(this.phase)) {
      throw new BootstrapStateError(this.phase, next);
    }
    this.phase = next;
  }

  markCodeVerified(): void {
    this.transition('DB_CONFIGURATION_ALLOWED', ['SEALED']);
  }

  recordSchemaInspection(state: SchemaState): BootstrapPhase {
    const next = INSPECTION_PHASE[state];
    this.transition(next, [
      'DB_CONFIGURATION_ALLOWED',
      'SCHEMA_ABSENT',
      'SCHEMA_CURRENT',
      'MIGRATION_REQUIRED',
      'BLOCKED_NEWER_SCHEMA',
      'BLOCKED_CORRUPT_SCHEMA',
    ]);
    return next;
  }

  beginInitialization(): void {
    this.transition('INITIALIZING', ['SCHEMA_ABSENT']);
  }

  markInitializationFailed(): void {
    this.transition('SCHEMA_ABSENT', ['INITIALIZING']);
  }

  requireTotpEnrollment(): void {
    this.transition('TOTP_ENROLLMENT_REQUIRED', ['INITIALIZING']);
  }

  beginMigration(): void {
    this.transition('MIGRATING', ['MIGRATION_REQUIRED']);
  }

  markMigrationFailed(): void {
    this.transition('MIGRATION_REQUIRED', ['MIGRATING']);
  }

  markMigrationSucceeded(): void {
    this.transition('SCHEMA_CURRENT', ['MIGRATING']);
  }

  beginUnlock(): void {
    this.transition('UNLOCKING', ['SCHEMA_CURRENT', 'BLOCKED_CONCURRENT_INSTANCE']);
  }

  markUnlockFailed(): void {
    this.transition('SCHEMA_CURRENT', ['UNLOCKING']);
  }

  beginRecovery(): void {
    this.transition('RECOVERING', ['UNLOCKING', 'TOTP_ENROLLMENT_REQUIRED', 'INITIALIZING']);
  }

  markConcurrentInstanceBlocked(): void {
    this.transition('BLOCKED_CONCURRENT_INSTANCE', ['UNLOCKING', 'RECOVERING']);
  }

  resumeTotpAfterConcurrentBlock(): void {
    this.transition('TOTP_ENROLLMENT_REQUIRED', ['BLOCKED_CONCURRENT_INSTANCE']);
  }

  markRecoveryFailedToTotp(): void {
    this.transition('TOTP_ENROLLMENT_REQUIRED', ['RECOVERING']);
  }

  markRecoveryFailedToCurrentSchema(): void {
    this.transition('SCHEMA_CURRENT', ['RECOVERING']);
  }

  markReady(): void {
    this.transition('READY', ['RECOVERING']);
  }
}
