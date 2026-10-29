import { createServer } from 'node:net';

import type { PoolClient, QueryResultRow } from 'pg';

const METRICS_PORT_LOCK_NAMESPACE = 1_397_046_851;
const METRICS_PORT_LOCK_KEY = 1;

export type RuntimeMetricsPortAllocatorErrorCode =
  'RUNTIME_METRICS_PORT_ALLOCATION_INVALID' | 'RUNTIME_METRICS_PORTS_UNAVAILABLE';

export class RuntimeMetricsPortAllocatorError extends Error {
  constructor(readonly code: RuntimeMetricsPortAllocatorErrorCode) {
    super(code);
    this.name = 'RuntimeMetricsPortAllocatorError';
  }
}

export interface RuntimeMetricsPortAllocatorOptions {
  readonly maxPort: number;
  readonly minPort: number;
}

interface OccupiedPortRow extends QueryResultRow {
  readonly metrics_port: number;
}

type PortProbe = (port: number) => Promise<boolean>;

async function canBindLoopback(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolve, reject) => {
    const server = createServer();
    server.unref();
    server.once('error', (error: NodeJS.ErrnoException) => {
      if (error.code === 'EADDRINUSE' || error.code === 'EACCES') {
        resolve(false);
        return;
      }
      reject(error);
    });
    server.listen({ exclusive: true, host: '127.0.0.1', port }, () => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve(true);
      });
    });
  });
}

export class RuntimeMetricsPortAllocator {
  private readonly maxPort: number;
  private readonly minPort: number;
  private readonly probe: PortProbe;

  constructor(options: RuntimeMetricsPortAllocatorOptions, probe: PortProbe = canBindLoopback) {
    if (
      !Number.isSafeInteger(options.minPort) ||
      !Number.isSafeInteger(options.maxPort) ||
      options.minPort < 1 ||
      options.maxPort > 65_535 ||
      options.minPort > options.maxPort
    ) {
      throw new RuntimeMetricsPortAllocatorError('RUNTIME_METRICS_PORT_ALLOCATION_INVALID');
    }
    this.minPort = options.minPort;
    this.maxPort = options.maxPort;
    this.probe = probe;
  }

  async allocate(client: PoolClient, count: number): Promise<readonly number[]> {
    if (!Number.isSafeInteger(count) || count < 1) {
      throw new RuntimeMetricsPortAllocatorError('RUNTIME_METRICS_PORT_ALLOCATION_INVALID');
    }

    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      METRICS_PORT_LOCK_NAMESPACE,
      METRICS_PORT_LOCK_KEY,
    ]);
    const occupied = await client.query<OccupiedPortRow>(
      `SELECT metrics_port
         FROM ape_dts_platform.task_run_units
        WHERE metrics_port BETWEEN $1::integer AND $2::integer
          AND metrics_port_released_at IS NULL
        ORDER BY metrics_port`,
      [this.minPort, this.maxPort],
    );
    const unavailable = new Set(occupied.rows.map((row) => row.metrics_port));
    const selected: number[] = [];

    for (let port = this.minPort; port <= this.maxPort && selected.length < count; port += 1) {
      if (!unavailable.has(port) && (await this.probe(port))) {
        selected.push(port);
      }
    }

    if (selected.length !== count) {
      throw new RuntimeMetricsPortAllocatorError('RUNTIME_METRICS_PORTS_UNAVAILABLE');
    }
    return selected;
  }
}
