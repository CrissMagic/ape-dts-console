import { get } from 'node:http';

import { parsePerformanceMetrics, type PerformanceCounters } from './performance-metrics.js';

/** No caller-controlled host, path, redirects or proxy configuration. */
export function readPerformanceMetrics(
  port: number,
  limits: { timeoutMs?: number; maxBytes?: number } = {},
): Promise<PerformanceCounters> {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    return Promise.reject(new Error('METRICS_PORT_INVALID'));
  }
  return new Promise((resolve, reject) => {
    const request = get({ host: '127.0.0.1', port, path: '/metrics', agent: false }, (response) => {
      if (response.statusCode !== 200) {
        response.destroy();
        reject(new Error('METRICS_HTTP_UNAVAILABLE'));
        return;
      }
      let bytes = 0;
      const chunks: Buffer[] = [];
      response.on('data', (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > (limits.maxBytes ?? 1024 * 1024)) {
          reject(new Error('METRICS_RESPONSE_TOO_LARGE'));
          request.destroy(new Error('METRICS_RESPONSE_TOO_LARGE'));
          return;
        }
        chunks.push(chunk);
      });
      response.on('error', () => reject(new Error('METRICS_HTTP_UNAVAILABLE')));
      response.on('end', () => {
        try {
          resolve(parsePerformanceMetrics(Buffer.concat(chunks).toString('utf8')));
        } catch {
          reject(new Error('METRICS_INVALID_PAYLOAD'));
        }
      });
    });
    const deadline = setTimeout(
      () => request.destroy(new Error('METRICS_TIMEOUT')),
      limits.timeoutMs ?? 3000,
    );
    deadline.unref();
    request.on('close', () => clearTimeout(deadline));
    request.on('error', (error) => {
      reject(
        new Error(
          ['METRICS_TIMEOUT', 'METRICS_RESPONSE_TOO_LARGE'].includes(error.message)
            ? error.message
            : 'METRICS_HTTP_UNAVAILABLE',
        ),
      );
    });
  });
}
