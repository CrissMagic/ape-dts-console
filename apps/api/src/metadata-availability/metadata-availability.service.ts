import { randomUUID } from 'node:crypto';

import { Injectable, Logger, type OnApplicationShutdown, type OnModuleInit } from '@nestjs/common';
import type { PlatformRuntimePhase } from '@scs/contracts';

import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';

const POSTGRES_OUTAGE_CODES = new Set([
  '57P01',
  '57P02',
  '57P03',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EPIPE',
  'ETIMEDOUT',
]);

export type MetadataAvailabilityErrorCode =
  | 'BLOCKED_CONCURRENT_INSTANCE'
  | 'METADATA_DATABASE_UNAVAILABLE'
  | 'METADATA_RECONCILIATION_FAILED'
  | 'METADATA_RECONCILIATION_NOT_CONFIGURED'
  | 'PLATFORM_NOT_READY';

export class MetadataAvailabilityError extends Error {
  constructor(
    readonly code: MetadataAvailabilityErrorCode,
    readonly phase: PlatformRuntimePhase,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'MetadataAvailabilityError';
  }
}

export interface MetadataOutageFact {
  readonly detectedAt: string;
  readonly eventId: string;
  readonly source: string;
}

function errorCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null || !('code' in error)) {
    return undefined;
  }
  return typeof error.code === 'string' ? error.code.toUpperCase() : undefined;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : '';
}

export function isMetadataConnectivityError(error: unknown): boolean {
  const code = errorCode(error);
  if (code !== undefined && (code.startsWith('08') || POSTGRES_OUTAGE_CODES.has(code))) {
    return true;
  }

  if (
    /\b(connection (?:closed|refused|reset|terminated)|socket (?:closed|hang up)|(?:connect|read) etimedout|timeout expired)\b/i.test(
      errorMessage(error),
    )
  ) {
    return true;
  }

  return error instanceof Error && error.cause !== undefined
    ? isMetadataConnectivityError(error.cause)
    : false;
}

@Injectable()
export class MetadataAvailabilityService implements OnApplicationShutdown, OnModuleInit {
  private readonly logger = new Logger('MetadataAvailability');
  private probeInFlight: Promise<void> | undefined;
  private outageFact: MetadataOutageFact | undefined;
  private reconciliationInFlight: Promise<void> | undefined;
  private reconcile: (() => Promise<void>) | undefined;
  private unregisterPoolErrorListener: (() => void) | undefined;

  constructor(
    private readonly database: PlatformDatabaseService,
    private readonly readiness: PlatformReadinessService,
  ) {}

  onModuleInit(): void {
    this.unregisterPoolErrorListener = this.database.registerConnectionErrorListener((error) => {
      this.reportFailure(error, 'postgres_pool');
    });
  }

  onApplicationShutdown(): void {
    this.unregisterPoolErrorListener?.();
    this.unregisterPoolErrorListener = undefined;
  }

  registerReconciler(reconcile: () => Promise<void>): () => void {
    if (this.reconcile !== undefined) {
      throw new Error('METADATA_RECONCILER_ALREADY_REGISTERED');
    }
    this.reconcile = reconcile;
    return () => {
      if (this.reconcile === reconcile) {
        this.reconcile = undefined;
      }
    };
  }

  isMetadataUnavailable(): boolean {
    return ['METADATA_DEGRADED', 'RECONCILING', 'BLOCKED_CONCURRENT_INSTANCE'].includes(
      this.readiness.getPhase(),
    );
  }

  reportFailure(error: unknown, source: string): boolean {
    if (!isMetadataConnectivityError(error)) {
      return false;
    }

    const phase = this.readiness.getPhase();
    if (phase === 'READY' || phase === 'RECONCILING') {
      this.database.abandonControllerLock();
      this.readiness.markMetadataDegraded();
      this.outageFact ??= {
        detectedAt: new Date().toISOString(),
        eventId: randomUUID(),
        source,
      };
      this.logger.error(
        JSON.stringify({
          code: 'METADATA_DATABASE_UNAVAILABLE',
          event: 'metadata.availability.degraded',
          source,
        }),
      );
    }
    return true;
  }

  pendingOutageFact(): MetadataOutageFact | null {
    return this.outageFact === undefined ? null : { ...this.outageFact };
  }

  acknowledgeOutageFact(eventId: string): void {
    if (this.outageFact?.eventId === eventId) {
      this.outageFact = undefined;
    }
  }

  async ensureManagementAvailable(): Promise<void> {
    const phase = this.readiness.getPhase();
    switch (phase) {
      case 'READY':
        try {
          await this.probeOnce();
        } catch (error) {
          if (!this.reportFailure(error, 'management_probe')) {
            throw error;
          }
          throw new MetadataAvailabilityError(
            'METADATA_DATABASE_UNAVAILABLE',
            this.readiness.getPhase(),
            { cause: error },
          );
        }
        if (this.readiness.getPhase() !== 'READY') {
          await this.ensureManagementAvailable();
        }
        return;
      case 'METADATA_DEGRADED':
        try {
          await this.probeOnce();
        } catch (error) {
          if (!this.reportFailure(error, 'recovery_probe')) {
            throw error;
          }
          throw new MetadataAvailabilityError('METADATA_DATABASE_UNAVAILABLE', phase, {
            cause: error,
          });
        }
        await this.reconcileOnce();
        return;
      case 'RECONCILING':
        if (this.reconciliationInFlight !== undefined) {
          await this.reconciliationInFlight;
          return;
        }
        throw new MetadataAvailabilityError('METADATA_RECONCILIATION_FAILED', phase);
      case 'BLOCKED_CONCURRENT_INSTANCE':
        throw new MetadataAvailabilityError('BLOCKED_CONCURRENT_INSTANCE', phase);
      case 'BOOTSTRAPPING':
      case 'SEALED':
        throw new MetadataAvailabilityError('PLATFORM_NOT_READY', phase);
    }
  }

  private probeOnce(): Promise<void> {
    if (this.probeInFlight !== undefined) {
      return this.probeInFlight;
    }
    let current: Promise<void>;
    try {
      current = this.database.probe();
    } catch (error) {
      current = Promise.reject(error);
    }
    this.probeInFlight = current;
    const clear = (): void => {
      if (this.probeInFlight === current) {
        this.probeInFlight = undefined;
      }
    };
    void current.then(clear, clear);
    return current;
  }

  private reconcileOnce(): Promise<void> {
    if (this.reconciliationInFlight !== undefined) {
      return this.reconciliationInFlight;
    }
    const current = this.performReconciliation();
    this.reconciliationInFlight = current;
    const clear = (): void => {
      if (this.reconciliationInFlight === current) {
        this.reconciliationInFlight = undefined;
      }
    };
    void current.then(clear, clear);
    return current;
  }

  private async performReconciliation(): Promise<void> {
    this.readiness.markReconciling();
    try {
      const acquired = await this.database.acquireControllerLock();
      if (!acquired) {
        this.readiness.markConcurrentInstanceBlocked();
        this.logger.error(
          JSON.stringify({
            code: 'BLOCKED_CONCURRENT_INSTANCE',
            event: 'metadata.availability.blocked',
          }),
        );
        throw new MetadataAvailabilityError(
          'BLOCKED_CONCURRENT_INSTANCE',
          'BLOCKED_CONCURRENT_INSTANCE',
        );
      }
      if (this.reconcile === undefined) {
        throw new MetadataAvailabilityError(
          'METADATA_RECONCILIATION_NOT_CONFIGURED',
          'RECONCILING',
        );
      }
      await this.reconcile();
      this.readiness.markReady();
      this.logger.log(JSON.stringify({ event: 'metadata.availability.ready' }));
    } catch (error) {
      if (error instanceof MetadataAvailabilityError) {
        throw error;
      }
      if (this.reportFailure(error, 'reconciliation')) {
        throw new MetadataAvailabilityError(
          'METADATA_DATABASE_UNAVAILABLE',
          this.readiness.getPhase(),
          { cause: error },
        );
      }
      this.logger.error(
        JSON.stringify({
          code: 'METADATA_RECONCILIATION_FAILED',
          event: 'metadata.availability.reconciliation_failed',
        }),
      );
      this.readiness.markMetadataDegraded();
      throw new MetadataAvailabilityError(
        'METADATA_RECONCILIATION_FAILED',
        this.readiness.getPhase(),
        { cause: error },
      );
    }
  }
}
