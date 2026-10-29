import { createServer, type Server, type RequestListener } from 'node:http';
import type { AddressInfo } from 'node:net';

import { afterEach, describe, expect, it } from 'vitest';

import { readPerformanceMetrics } from './performance-http.js';

const servers: Server[] = [];
async function serve(handler: RequestListener): Promise<number> {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as AddressInfo).port;
}
afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.closeAllConnections();
          server.close(() => resolve());
        }),
    ),
  );
});

describe('loopback runtime metrics reader', () => {
  it('does not follow redirects to another endpoint', async () => {
    const port = await serve((_request, response) => {
      response.writeHead(302, { Location: 'http://example.invalid/' });
      response.end();
    });
    await expect(readPerformanceMetrics(port)).rejects.toThrow('METRICS_HTTP_UNAVAILABLE');
  });

  it('bounds the body even when the server does not send Content-Length', async () => {
    const port = await serve((_request, response) => {
      response.write('sinker_sinked_records 1\n');
      response.end(' '.repeat(1024));
    });
    await expect(readPerformanceMetrics(port, { maxBytes: 100 })).rejects.toThrow(
      'METRICS_RESPONSE_TOO_LARGE',
    );
  });

  it('times out an unresponsive server', async () => {
    const port = await serve(() => undefined);
    await expect(readPerformanceMetrics(port, { timeoutMs: 25 })).rejects.toThrow(
      'METRICS_TIMEOUT',
    );
  });

  it('keeps its absolute deadline when the server sends a partial body but never finishes', async () => {
    const port = await serve((_request, response) => {
      response.write('sinker_sinked_records 1\n');
    });
    await expect(readPerformanceMetrics(port, { timeoutMs: 30 })).rejects.toThrow(
      'METRICS_TIMEOUT',
    );
  });

  it('rejects malformed and empty successful HTTP bodies', async () => {
    const port = await serve((_request, response) => response.end('<html>not metrics</html>'));
    await expect(readPerformanceMetrics(port)).rejects.toThrow('METRICS_INVALID_PAYLOAD');
  });

  it('reads actual /metrics HTTP responses', async () => {
    const port = await serve((request, response) => {
      expect(request.url).toBe('/metrics');
      response.end('sinker_sinked_records 42');
    });
    expect(await readPerformanceMetrics(port)).toMatchObject({ sinkRecords: 42 });
  });
});
