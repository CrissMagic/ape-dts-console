import { createServer, type Server } from 'node:net';

import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import {
  RuntimeMetricsPortAllocator,
  RuntimeMetricsPortAllocatorError,
} from './runtime-metrics-port-allocator.js';

function clientWithOccupiedPorts(ports: readonly number[] = []): {
  readonly client: PoolClient;
  readonly query: ReturnType<typeof vi.fn>;
} {
  const query = vi.fn(async (sql: string, parameters?: readonly unknown[]) => {
    void parameters;
    if (sql.includes('pg_advisory_xact_lock')) return { rowCount: 1, rows: [] };
    if (sql.includes('task_run_units')) {
      return { rowCount: ports.length, rows: ports.map((metrics_port) => ({ metrics_port })) };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  return { client: { query } as unknown as PoolClient, query };
}

async function listen(port: number): Promise<Server> {
  return await new Promise<Server>((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen({ exclusive: true, host: '127.0.0.1', port }, () => resolve(server));
  });
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
}

describe('RuntimeMetricsPortAllocator', () => {
  it.each([
    { maxPort: 20_000, minPort: 0 },
    { maxPort: 65_536, minPort: 20_000 },
    { maxPort: 19_999, minPort: 20_000 },
    { maxPort: 20_000.5, minPort: 20_000 },
  ])('rejects invalid configuration: %o', (options) => {
    expect(() => new RuntimeMetricsPortAllocator(options)).toThrow(
      expect.objectContaining({ code: 'RUNTIME_METRICS_PORT_ALLOCATION_INVALID' }),
    );
  });

  it('takes the transaction lock before reading occupied ports and returns candidates in order', async () => {
    const { client, query } = clientWithOccupiedPorts([20_002]);
    const probe = vi.fn(async (port: number) => {
      void port;
      return true;
    });
    const allocator = new RuntimeMetricsPortAllocator({ maxPort: 20_004, minPort: 20_001 }, probe);

    await expect(allocator.allocate(client, 3)).resolves.toEqual([20_001, 20_003, 20_004]);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0]?.[0]).toContain('pg_advisory_xact_lock');
    expect(query.mock.calls[1]?.[0]).toContain('metrics_port_released_at IS NULL');
    expect(query.mock.calls[1]?.[0]).toContain('ORDER BY metrics_port');
    expect(query.mock.calls[1]?.[1]).toEqual([20_001, 20_004]);
    expect(probe.mock.calls.map(([port]) => port)).toEqual([20_001, 20_003, 20_004]);
  });

  it('skips a port currently occupied on loopback', async () => {
    const occupied = await listen(0);
    const address = occupied.address();
    if (address === null || typeof address === 'string') throw new Error('TCP address unavailable');
    const { client } = clientWithOccupiedPorts();
    const allocator = new RuntimeMetricsPortAllocator({
      maxPort: address.port,
      minPort: address.port,
    });

    try {
      await expect(allocator.allocate(client, 1)).rejects.toMatchObject({
        code: 'RUNTIME_METRICS_PORTS_UNAVAILABLE',
      });
    } finally {
      await close(occupied);
    }
  });

  it('throws without returning a partial allocation when too few ports are available', async () => {
    const { client } = clientWithOccupiedPorts([20_001]);
    const probe = vi.fn(async (port: number) => port !== 20_002);
    const allocator = new RuntimeMetricsPortAllocator({ maxPort: 20_003, minPort: 20_001 }, probe);

    await expect(allocator.allocate(client, 2)).rejects.toEqual(
      new RuntimeMetricsPortAllocatorError('RUNTIME_METRICS_PORTS_UNAVAILABLE'),
    );
    expect(probe.mock.calls.map(([port]) => port)).toEqual([20_002, 20_003]);
  });

  it('rejects an invalid requested count before querying PostgreSQL', async () => {
    const { client, query } = clientWithOccupiedPorts();
    const allocator = new RuntimeMetricsPortAllocator({ maxPort: 20_001, minPort: 20_001 });

    await expect(allocator.allocate(client, 0)).rejects.toMatchObject({
      code: 'RUNTIME_METRICS_PORT_ALLOCATION_INVALID',
    });
    expect(query).not.toHaveBeenCalled();
  });
});
