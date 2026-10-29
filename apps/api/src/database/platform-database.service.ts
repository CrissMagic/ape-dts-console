import type { ConnectionOptions } from 'node:tls';
import { hostname } from 'node:os';

import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import type { PostgresConnectionConfig } from '@scs/contracts';
import {
  ControllerLockLease,
  PgSchemaInspector,
  tryAcquireControllerLock,
  type MigrationManifest,
  type SchemaClassification,
} from '@scs/database';
import { Pool, type PoolClient, type PoolConfig } from 'pg';

const CONNECTION_TIMEOUT_MS = 10_000;
const QUERY_TIMEOUT_MS = 10_000;
const IDLE_TIMEOUT_MS = 30_000;
const BOOTSTRAP_POOL_SIZE = 4;

export class PlatformDatabaseError extends Error {
  readonly code = 'PLATFORM_DATABASE_CONNECTION_FAILED';

  constructor(options?: ErrorOptions) {
    super('PLATFORM_DATABASE_CONNECTION_FAILED', options);
    this.name = 'PlatformDatabaseError';
  }
}

function sslConfiguration(config: PostgresConnectionConfig): false | ConnectionOptions {
  switch (config.tls.mode) {
    case 'DISABLE':
      return false;
    case 'REQUIRE':
      return { rejectUnauthorized: false };
    case 'VERIFY_FULL':
      return { ca: config.tls.caCertificate, rejectUnauthorized: true };
  }
}

function poolConfiguration(config: PostgresConnectionConfig): PoolConfig {
  return {
    application_name: `scs-data-sync-platform/${hostname()
      .replace(/[^a-zA-Z0-9_.-]/gu, '_')
      .slice(0, 24)}/${process.pid}`,
    connectionTimeoutMillis: CONNECTION_TIMEOUT_MS,
    database: config.database,
    host: config.host,
    idleTimeoutMillis: IDLE_TIMEOUT_MS,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    // Bound dead server-side sessions too, including when the container can no longer send FIN.
    options:
      '-c tcp_keepalives_idle=10 -c tcp_keepalives_interval=5 -c tcp_keepalives_count=3 -c tcp_user_timeout=15000',
    max: BOOTSTRAP_POOL_SIZE,
    password: config.password,
    port: config.port,
    query_timeout: QUERY_TIMEOUT_MS,
    ssl: sslConfiguration(config),
    statement_timeout: QUERY_TIMEOUT_MS,
    user: config.username,
  };
}

@Injectable()
export class PlatformDatabaseService implements OnApplicationShutdown {
  private controllerLease: ControllerLockLease | undefined;
  private pool: Pool | undefined;
  private readonly connectionErrorListeners = new Set<(error: Error) => void>();
  private readonly schemaCurrentListeners = new Set<() => void>();

  async inspectAndUse(
    config: PostgresConnectionConfig,
    manifest: MigrationManifest,
  ): Promise<SchemaClassification> {
    if (this.controllerLease !== undefined) {
      throw new Error('DATABASE_RECONFIGURATION_FORBIDDEN_WHILE_CONTROLLER_LOCKED');
    }
    const candidate = new Pool(poolConfiguration(config));
    candidate.on('error', (error) => this.notifyConnectionError(error));
    let client: PoolClient | undefined;
    try {
      client = await candidate.connect();
      await client.query(
        "SELECT current_database(), current_user, current_setting('server_version_num')",
      );
      const classification = await new PgSchemaInspector().inspect(client, manifest);
      client.release();
      client = undefined;
      const previous = this.pool;
      this.pool = candidate;
      await previous?.end();
      if (classification.state === 'CURRENT') {
        this.notifySchemaCurrent();
      }
      return classification;
    } catch (cause) {
      client?.release(true);
      await candidate.end().catch(() => undefined);
      throw new PlatformDatabaseError({ cause });
    }
  }

  requirePool(): Pool {
    if (this.pool === undefined) {
      throw new Error('PLATFORM_DATABASE_NOT_CONFIGURED');
    }
    return this.pool;
  }

  hasPool(): boolean {
    return this.pool !== undefined;
  }

  async probe(): Promise<void> {
    try {
      if (this.controllerLease !== undefined) await this.controllerLease.probe();
      else await this.requirePool().query('SELECT 1');
    } catch (error) {
      this.notifyConnectionError(error);
      throw error;
    }
  }

  registerConnectionErrorListener(listener: (error: Error) => void): () => void {
    this.connectionErrorListeners.add(listener);
    return () => this.connectionErrorListeners.delete(listener);
  }

  private notifyConnectionError(error: unknown): void {
    const normalized = error instanceof Error ? error : new Error('UNKNOWN_DATABASE_ERROR');
    for (const listener of this.connectionErrorListeners) {
      listener(normalized);
    }
  }

  registerSchemaCurrentListener(listener: () => void): () => void {
    this.schemaCurrentListeners.add(listener);
    return () => this.schemaCurrentListeners.delete(listener);
  }

  notifySchemaCurrent(): void {
    for (const listener of this.schemaCurrentListeners) {
      listener();
    }
  }

  async acquireControllerLock(): Promise<boolean> {
    if (this.controllerLease !== undefined && !this.controllerLease.isReleased()) {
      return true;
    }
    let lease: ControllerLockLease | null;
    try {
      lease = await tryAcquireControllerLock(this.requirePool(), (cause) => {
        this.notifyConnectionError(
          Object.assign(new Error('CONTROLLER_LOCK_CONNECTION_LOST', { cause }), { code: '08006' }),
        );
      });
    } catch (error) {
      this.notifyConnectionError(error);
      throw error;
    }
    if (lease === null) {
      return false;
    }
    this.controllerLease = lease;
    return true;
  }

  async releaseControllerLock(): Promise<void> {
    await this.controllerLease?.release();
    this.controllerLease = undefined;
  }

  abandonControllerLock(): void {
    this.controllerLease?.abandon();
    this.controllerLease = undefined;
  }

  async onApplicationShutdown(): Promise<void> {
    await this.releaseControllerLock().catch(() => undefined);
    await this.pool?.end().catch(() => undefined);
    this.pool = undefined;
    this.connectionErrorListeners.clear();
    this.schemaCurrentListeners.clear();
  }
}
