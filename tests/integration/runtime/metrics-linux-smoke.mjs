// Run with a read-only repository mount after building apps/api.
import assert from 'node:assert/strict';
import console from 'node:console';
import process from 'node:process';
import { createServer } from 'node:http';
import { assertMetricsPortOwner } from '../../../apps/api/dist/runtime/performance-port-owner.js';
import { readPerformanceMetrics } from '../../../apps/api/dist/runtime/performance-http.js';

assert.equal(process.platform, 'linux');
const server = createServer((_request, response) =>
  response.end('sinker_sinked_records 42\nsinker_sinked_bytes 840\n'),
);
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = server.address().port;
try {
  await assertMetricsPortOwner({ pid: process.pid, port });
  assert.deepEqual(await readPerformanceMetrics(port), {
    sinkRecords: 42,
    sinkBytes: 840,
    queueRecords: null,
    queueBytes: null,
  });
  await assert.rejects(
    assertMetricsPortOwner({ pid: 999999, port }),
    /METRICS_PORT_OWNERSHIP_UNVERIFIED/u,
  );
} finally {
  await new Promise((resolve) => server.close(resolve));
}
const wildcard = createServer((_request, response) => response.end('sinker_sinked_records 1'));
await new Promise((resolve) => wildcard.listen(0, '0.0.0.0', resolve));
try {
  await assert.rejects(
    assertMetricsPortOwner({ pid: process.pid, port: wildcard.address().port }),
    /METRICS_PORT_OWNERSHIP_UNVERIFIED/u,
  );
} finally {
  await new Promise((resolve) => wildcard.close(resolve));
}
console.log(
  'Linux metrics smoke passed: process-owned loopback socket, real HTTP counters, foreign PID and wildcard listener rejection.',
);
