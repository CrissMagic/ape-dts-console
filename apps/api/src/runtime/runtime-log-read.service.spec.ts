import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import type { UserSummary } from '@scs/contracts';
import { LocalLogSegmentObjectStore, segmentSha256 } from '@scs/storage';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RuntimeLogReadError, RuntimeLogReadService } from './runtime-log-read.service.js';
import { RuntimeReadNotFoundError } from './runtime-read.service.js';

const actor: UserSummary = {
  displayName: 'Developer',
  id: '33333333-3333-4333-8333-333333333333',
  role: 'DEVELOPER',
  status: 'ACTIVE',
  username: 'developer',
};
const taskId = '11111111-1111-4111-8111-111111111111';
const runId = '22222222-2222-4222-8222-222222222222';
const unitId = '44444444-4444-4444-8444-444444444444';
const occurredAt = new Date('2026-09-08T00:00:00.000Z');
const roots: string[] = [];

function context(root: string, responses: unknown[]) {
  const query = vi.fn(async (sql: string, parameters?: readonly unknown[]) => {
    void sql;
    void parameters;
    return responses.shift();
  });
  return {
    query,
    service: new RuntimeLogReadService({ requirePool: () => ({ query }) } as never, root),
  };
}

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(roots.splice(0).map((root) => rm(root, { force: true, recursive: true })));
});

describe('RuntimeLogReadService', () => {
  it.each(['ASC', 'DESC'] as const)(
    'keeps all cross-segment ERROR continuations in %s reads and downloads',
    async (order) => {
      const root = await mkdtemp(path.join(tmpdir(), 'scs-log-continuation-'));
      roots.push(root);
      const store = new LocalLogSegmentObjectStore(root);
      const head = `ERROR ${'x'.repeat(65_530)}`;
      const makeSegment = async (
        sequence: number,
        lines: Array<{ continuation: number; continues: boolean; payload: string; second: number }>,
      ) => {
        const content = Buffer.from(
          lines
            .map(({ second, ...line }) =>
              JSON.stringify({
                ...line,
                stream: 'STDOUT',
                unitId,
                occurredAt: new Date(occurredAt.getTime() + second * 1000).toISOString(),
              }),
            )
            .join('\n') + '\n',
        );
        const key = `logs/${sequence}.log`;
        await store.put({ objectKey: key, content });
        return {
          id: String(sequence),
          sequence: String(sequence),
          started_at: occurredAt,
          ended_at: new Date(occurredAt.getTime() + lines.at(-1)!.second * 1000),
          config: { relative_prefix: '' },
          driver: 'LOCAL',
          object_key: key,
          run_unit_id: unitId,
          unit_key: 'primary',
          sha256: Buffer.from(segmentSha256(content), 'hex'),
          size_bytes: String(content.length),
          stream: 'STDOUT',
          state: 'AVAILABLE',
        };
      };
      const first = await makeSegment(1, [
        { continuation: 0, continues: true, payload: head, second: 1 },
      ]);
      const second = await makeSegment(2, [
        { continuation: 1, continues: true, payload: 'failure record id=123', second: 2 },
        { continuation: 2, continues: false, payload: 'end of error', second: 3 },
        { continuation: 0, continues: true, payload: 'INFO unrelated', second: 4 },
        { continuation: 1, continues: false, payload: 'unrelated info tail', second: 5 },
      ]);
      const query = vi.fn(async (sql: string) => {
        if (sql.includes('SELECT 1 FROM')) return { rowCount: 1, rows: [{}] };
        if (sql.includes('max_active_bytes')) return { rows: [{ max_active_bytes: '0' }] };
        if (sql.includes('FROM ape_dts_platform.log_segments segment'))
          return {
            rows: sql.includes('segment.sequence <')
              ? [first]
              : order === 'DESC' && sql.includes('ORDER BY segment.ended_at DESC')
                ? [second, first]
                : [first, second],
          };
        return { rows: [] };
      });
      const service = new RuntimeLogReadService({ requirePool: () => ({ query }) } as never, root);
      const result = await service.list(actor, taskId, runId, {
        limit: 500,
        order,
        level: 'ERROR',
      });
      expect(
        result.facts.map((fact) => (fact.kind === 'LINE' ? fact.payload : fact.reason)),
      ).toEqual([head, 'failure record id=123', 'end of error']);
      const download = await service.download(actor, taskId, runId, { level: 'ERROR' });
      let content = '';
      for await (const chunk of download.content) content += String(chunk);
      expect(content).toContain('failure record id=123');
      expect(content).toContain('end of error');
      expect(content).not.toContain('unrelated');
    },
  );

  it.each([
    { segments: 65, payload: 'INFO ready', maximumReads: 64 },
    { segments: 9, payload: `INFO ${'x'.repeat(1_000_000)}`, maximumReads: 8 },
  ])(
    'bounds scans of nonmatching live logs at $maximumReads object reads and explicitly reports partial search results',
    async ({ segments: segmentCount, payload, maximumReads }) => {
      const root = await mkdtemp(path.join(tmpdir(), 'scs-log-scan-budget-'));
      roots.push(root);
      const content = Buffer.from(
        `${JSON.stringify({ continuation: 0, continues: false, occurredAt: occurredAt.toISOString(), payload, stream: 'STDOUT', unitId })}\n`,
      );
      await new LocalLogSegmentObjectStore(root).put({ objectKey: 'logs/ready.log', content });
      const segments = Array.from({ length: segmentCount }, (_, index) => ({
        id: String(index),
        sequence: String(segmentCount - index),
        started_at: occurredAt,
        ended_at: occurredAt,
        config: { relative_prefix: '' },
        driver: 'LOCAL',
        object_key: 'logs/ready.log',
        run_unit_id: unitId,
        unit_key: 'primary',
        sha256: Buffer.from(segmentSha256(content), 'hex'),
        size_bytes: String(content.length),
        stream: 'STDOUT',
        state: 'AVAILABLE',
      }));
      const test = context(root, [
        { rowCount: 1, rows: [{}] },
        { rows: segments },
        { rows: [] },
        { rows: [] },
      ]);
      const read = vi.spyOn(LocalLogSegmentObjectStore.prototype, 'read');
      const result = await test.service.list(actor, taskId, runId, {
        limit: 500,
        order: 'DESC',
        level: 'ERROR',
      });
      expect(result).toMatchObject({ facts: [], limited: true, scanLimited: true });
      expect(read.mock.calls.length).toBe(maximumReads);
    },
  );

  it('reports missing logical level context instead of silently dropping continuation details', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-log-context-gap-'));
    roots.push(root);
    const content = Buffer.from(
      `${JSON.stringify({ continuation: 1, continues: false, occurredAt: occurredAt.toISOString(), payload: 'error tail without its original header', stream: 'STDOUT', unitId })}\n`,
    );
    await new LocalLogSegmentObjectStore(root).put({ objectKey: 'logs/tail.log', content });
    const segment = {
      id: 'tail',
      sequence: '2',
      started_at: occurredAt,
      ended_at: occurredAt,
      config: { relative_prefix: '' },
      driver: 'LOCAL',
      object_key: 'logs/tail.log',
      run_unit_id: unitId,
      unit_key: 'primary',
      sha256: Buffer.from(segmentSha256(content), 'hex'),
      size_bytes: String(content.length),
      stream: 'STDOUT',
      state: 'AVAILABLE',
    };
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows: [segment] },
      { rows: [] },
      { rows: [] },
      { rows: [] },
    ]);
    const result = await test.service.list(actor, taskId, runId, {
      limit: 500,
      order: 'DESC',
      level: 'ERROR',
    });
    expect(result.facts).toEqual([
      expect.objectContaining({ kind: 'GAP', reason: 'LOG_LEVEL_CONTEXT_UNAVAILABLE' }),
    ]);
  });

  it('tails the latest matching levels across overlapping segments and keeps gap evidence', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-tail-'));
    roots.push(root);
    const store = new LocalLogSegmentObjectStore(root);
    const row = async (name: string, lines: Array<[number, string]>) => {
      const content = Buffer.from(
        lines
          .map(([second, payload]) =>
            JSON.stringify({
              continuation: 0,
              continues: false,
              occurredAt: new Date(occurredAt.getTime() + second * 1000).toISOString(),
              payload,
              stream: 'STDOUT',
              unitId,
            }),
          )
          .join('\n') + '\n',
      );
      const key = `runs/run/logs/stdout/${name}.log`;
      await store.put({ objectKey: key, content });
      return {
        id: name,
        sequence: '1',
        started_at: occurredAt,
        ended_at: new Date(occurredAt.getTime() + lines.at(-1)![0] * 1000),
        state: 'AVAILABLE',
        stream: 'STDOUT',
        run_unit_id: unitId,
        unit_key: 'primary',
        object_key: key,
        size_bytes: String(content.length),
        sha256: Buffer.from(segmentSha256(content), 'hex'),
        driver: 'LOCAL',
        config: { relative_prefix: '' },
      };
    };
    const newest = await row('newest', [
      [1, 'ERROR oldest'],
      [5, '2026-09-08T00:00:05Z INFO not an ERROR'],
      [6, 'ERROR latest'],
    ]);
    const overlapping = await row('overlapping', [
      [2, 'WARN excluded'],
      [4, '\u001b[31mERROR\u001b[0m middle'],
    ]);
    const responses = [
      { rowCount: 1, rows: [{}] },
      { rows: [newest, overlapping] },
      {
        rows: [
          {
            id: 'gap',
            reason: 'TEST_GAP',
            lost_bytes: '1',
            occurred_at: new Date(occurredAt.getTime() + 3000),
            run_unit_id: unitId,
            unit_key: 'primary',
          },
        ],
      },
      { rows: [] },
    ];
    const test = context(root, responses);
    const result = await test.service.list(actor, taskId, runId, {
      limit: 3,
      order: 'DESC',
      level: 'ERROR',
    });
    expect(result.limited).toBe(true);
    expect(result.facts.map((fact) => (fact.kind === 'LINE' ? fact.payload : fact.reason))).toEqual(
      ['TEST_GAP', '\u001b[31mERROR\u001b[0m middle', 'ERROR latest'],
    );
    const downloadTest = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows: [newest, overlapping] },
      { rows: [{ max_active_bytes: '0' }] },
      { rows: [newest, overlapping] },
      { rows: [] },
      { rows: [] },
    ]);
    const download = await downloadTest.service.download(actor, taskId, runId, { level: 'ERROR' });
    let content = '';
    for await (const chunk of download.content) content += String(chunk);
    expect(content).toContain('ERROR latest');
    expect(content).toContain('ERROR oldest');
    expect(content).not.toContain('INFO');
    expect(content).not.toContain('WARN');
  });

  it('hides deleted and unauthorized task runs behind the dynamic two-source grant predicate', async () => {
    const test = context('unused', [{ rowCount: 0, rows: [] }]);
    const flushRun = vi.fn();
    const service = new RuntimeLogReadService(
      { requirePool: () => ({ query: test.query }) } as never,
      'unused',
      { flushRun } as never,
    );
    await expect(
      service.list(actor, taskId, runId, { limit: 500, order: 'DESC' }),
    ).rejects.toBeInstanceOf(RuntimeReadNotFoundError);
    expect(String(test.query.mock.calls[0]?.[0])).toContain('grant_row.revoked_at IS NULL');
    expect(test.query.mock.calls[0]?.[1]).toEqual([taskId, runId, actor.role, actor.id]);
    expect(flushRun).not.toHaveBeenCalled();
  });

  it('reads verified JSONL, represents legacy and capacity loss as GAPs, and never reads deleted segment bodies', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-read-'));
    roots.push(root);
    const store = new LocalLogSegmentObjectStore(root);
    const jsonl = Buffer.from(
      `${JSON.stringify({ continuation: 0, continues: false, occurredAt: occurredAt.toISOString(), payload: 'ready', stream: 'STDOUT', unitId })}\n`,
    );
    const legacy = Buffer.from('old plain log\n');
    await store.put({ content: jsonl, objectKey: 'runs/run/logs/stdout/one.log' });
    await store.put({ content: legacy, objectKey: 'runs/run/logs/stdout/two.log' });
    const row = (objectKey: string, content: Buffer) => ({
      config: { relative_prefix: '' },
      driver: 'LOCAL',
      ended_at: occurredAt,
      id: objectKey,
      object_key: objectKey,
      run_unit_id: unitId,
      sha256: Buffer.from(segmentSha256(content), 'hex'),
      size_bytes: String(content.length),
      state: 'AVAILABLE',
      stream: 'STDOUT',
      unit_key: 'partition:0',
    });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          row('runs/run/logs/stdout/one.log', jsonl),
          row('runs/run/logs/stdout/two.log', legacy),
        ],
      },
      { rows: [] },
      {
        rows: [
          {
            deleted_at: new Date('2026-09-08T00:00:01.000Z'),
            run_unit_id: unitId,
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);
    const result = await test.service.list(actor, taskId, runId, { limit: 500 });
    expect(result.facts).toEqual([
      expect.objectContaining({ kind: 'GAP', reason: 'LEGACY_FORMAT' }),
      expect.objectContaining({ kind: 'LINE', payload: 'ready', unitKey: 'partition:0' }),
      expect.objectContaining({ kind: 'GAP', reason: 'CAPACITY_ROLLOVER' }),
    ]);
    expect(String(test.query.mock.calls[1]?.[0])).toContain(
      "segment.state NOT IN ('OPEN','DELETED')",
    );
    expect(String(test.query.mock.calls[3]?.[0])).toContain("request.reason='CAPACITY_ROLLOVER'");
  });

  it('applies an inclusive from and exclusive to bound to individual log facts', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-time-filter-'));
    roots.push(root);
    const content = Buffer.from(
      [
        {
          continuation: 0,
          continues: false,
          occurredAt: '2026-09-08T00:00:00.000Z',
          payload: 'included',
          stream: 'STDOUT',
          unitId,
        },
        {
          continuation: 0,
          continues: false,
          occurredAt: '2026-09-08T00:00:02.000Z',
          payload: 'excluded-at-upper-bound',
          stream: 'STDOUT',
          unitId,
        },
      ]
        .map((fact) => JSON.stringify(fact))
        .join('\n') + '\n',
    );
    const key = 'runs/run/logs/stdout/time-filter.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: new Date('2026-09-08T00:00:03.000Z'),
            id: 'segment',
            object_key: key,
            run_unit_id: unitId,
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);

    const result = await test.service.list(actor, taskId, runId, {
      from: '2026-09-08T00:00:00.000Z',
      limit: 500,
      to: '2026-09-08T00:00:02.000Z',
    });

    expect(result.facts).toEqual([expect.objectContaining({ kind: 'LINE', payload: 'included' })]);
  });

  it('stops reading segment objects after collecting the requested limit plus one fact', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-bounded-list-'));
    roots.push(root);
    const content = Buffer.from(
      ['first', 'second']
        .map((payload, continuation) =>
          JSON.stringify({
            continuation,
            continues: continuation === 0,
            occurredAt: new Date(occurredAt.getTime() + continuation).toISOString(),
            payload,
            stream: 'STDOUT',
            unitId,
          }),
        )
        .join('\n') + '\n',
    );
    const key = 'runs/run/logs/stdout/bounded.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: new Date('2026-09-08T00:00:01.000Z'),
            id: '11111111-1111-4111-8111-111111111111',
            object_key: key,
            run_unit_id: unitId,
            sequence: '1',
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
          {
            config: {},
            driver: 'OSS',
            ended_at: new Date('2026-09-08T00:00:02.000Z'),
            id: '22222222-2222-4222-8222-222222222222',
            object_key: 'must-not-be-read',
            run_unit_id: unitId,
            sequence: '2',
            sha256: Buffer.alloc(32),
            size_bytes: '1',
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);

    await expect(test.service.list(actor, taskId, runId, { limit: 1 })).resolves.toMatchObject({
      facts: [expect.objectContaining({ kind: 'LINE', payload: 'first' })],
      limited: true,
    });
  });

  it('excludes an OPEN segment because it has no durable readable boundary yet', async () => {
    const test = context('unused', [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: null,
            driver: null,
            ended_at: null,
            id: '11111111-1111-4111-8111-111111111111',
            object_key: null,
            run_unit_id: unitId,
            sequence: '1',
            sha256: null,
            size_bytes: null,
            state: 'OPEN',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);

    await expect(test.service.list(actor, taskId, runId, { limit: 500 })).resolves.toMatchObject({
      facts: [],
      limited: false,
    });
    expect(String(test.query.mock.calls[1]?.[0])).toContain(
      "segment.state NOT IN ('OPEN','DELETED')",
    );
  });

  it('uses the same inclusive from and exclusive to bounds for downloads', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-time-download-'));
    roots.push(root);
    const content = Buffer.from(
      [
        {
          continuation: 0,
          continues: false,
          occurredAt: '2026-09-08T00:00:00.000Z',
          payload: 'included',
          stream: 'STDOUT',
          unitId,
        },
        {
          continuation: 0,
          continues: false,
          occurredAt: '2026-09-08T00:00:02.000Z',
          payload: 'excluded-at-upper-bound',
          stream: 'STDOUT',
          unitId,
        },
      ]
        .map((fact) => JSON.stringify(fact))
        .join('\n') + '\n',
    );
    const key = 'runs/run/logs/stdout/time-download.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows: [] },
      { rows: [{ max_active_bytes: '0' }] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: new Date('2026-09-08T00:00:03.000Z'),
            id: 'segment',
            object_key: key,
            run_unit_id: unitId,
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);

    const download = await test.service.download(actor, taskId, runId, {
      from: '2026-09-08T00:00:00.000Z',
      to: '2026-09-08T00:00:02.000Z',
    });
    let body = '';
    for await (const chunk of download.content) body += String(chunk);

    expect(body).toContain('included');
    expect(body).not.toContain('excluded-at-upper-bound');
  });

  it('applies inclusive from and exclusive to bounds to regular and capacity GAPs', async () => {
    const test = context('unused', [
      { rowCount: 1, rows: [{}] },
      { rows: [] },
      {
        rows: [
          {
            id: '11111111-1111-4111-8111-111111111111',
            lost_bytes: '2',
            occurred_at: new Date('2026-09-08T00:00:00.000Z'),
            reason: 'RAW_QUEUE_OVERFLOW',
            run_unit_id: unitId,
            unit_key: 'partition:0',
          },
          {
            id: '22222222-2222-4222-8222-222222222222',
            lost_bytes: '3',
            occurred_at: new Date('2026-09-08T00:00:02.000Z'),
            reason: 'RAW_QUEUE_OVERFLOW',
            run_unit_id: unitId,
            unit_key: 'partition:0',
          },
        ],
      },
      {
        rows: [
          {
            deleted_at: new Date('2026-09-08T00:00:01.000Z'),
            log_segment_id: '33333333-3333-4333-8333-333333333333',
            run_unit_id: unitId,
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
          {
            deleted_at: new Date('2026-09-08T00:00:02.000Z'),
            log_segment_id: '44444444-4444-4444-8444-444444444444',
            run_unit_id: unitId,
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);

    const result = await test.service.list(actor, taskId, runId, {
      from: '2026-09-08T00:00:00.000Z',
      limit: 500,
      to: '2026-09-08T00:00:02.000Z',
    });

    expect(result.facts).toEqual([
      expect.objectContaining({
        occurredAt: '2026-09-08T00:00:00.000Z',
        reason: 'RAW_QUEUE_OVERFLOW',
      }),
      expect.objectContaining({
        occurredAt: '2026-09-08T00:00:01.000Z',
        reason: 'CAPACITY_ROLLOVER',
      }),
    ]);
  });

  it('does not trust a JSON record whose physical stream or unit differs from its segment', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-integrity-'));
    roots.push(root);
    await mkdir(root, { recursive: true });
    const content = Buffer.from(
      `${JSON.stringify({ continuation: 0, continues: false, occurredAt: occurredAt.toISOString(), payload: 'forged', stream: 'STDERR', unitId })}\n`,
    );
    const key = 'runs/run/logs/stdout/forged.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: occurredAt,
            id: 'segment',
            object_key: key,
            run_unit_id: unitId,
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);
    await expect(test.service.list(actor, taskId, runId, { limit: 500 })).resolves.toMatchObject({
      facts: [expect.objectContaining({ kind: 'GAP', reason: 'INTEGRITY_FAILED' })],
    });
  });

  it('fails explicitly for a non-LOCAL available object instead of falling back', async () => {
    const test = context('unused', [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: {},
            driver: 'OSS',
            ended_at: occurredAt,
            id: 'segment',
            object_key: 'key',
            run_unit_id: unitId,
            sha256: Buffer.alloc(32),
            size_bytes: '1',
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);
    await expect(test.service.list(actor, taskId, runId, { limit: 500 })).rejects.toEqual(
      new RuntimeLogReadError('LOG_STORAGE_DRIVER_UNSUPPORTED'),
    );
  });

  it('rejects a LOCAL profile prefix that escapes the configured storage root', async () => {
    const test = context('configured-root', [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '../outside' },
            driver: 'LOCAL',
            ended_at: occurredAt,
            id: 'segment',
            object_key: 'segment.log',
            run_unit_id: unitId,
            sha256: Buffer.alloc(32),
            size_bytes: '1',
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);
    await expect(test.service.list(actor, taskId, runId, { limit: 500 })).rejects.toEqual(
      new RuntimeLogReadError('LOG_STORAGE_PROFILE_CONFIG_INVALID'),
    );
  });

  it('streams more than 1000 verified facts without a fixed record cutoff', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-download-'));
    roots.push(root);
    const content = Buffer.from(
      Array.from({ length: 1001 }, (_, continuation) =>
        JSON.stringify({
          continuation,
          continues: continuation < 1000,
          occurredAt: occurredAt.toISOString(),
          payload: String(continuation),
          stream: 'STDOUT',
          unitId,
        }),
      ).join('\n') + '\n',
    );
    const key = 'runs/run/logs/stdout/download.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows: [] },
      { rows: [{ max_active_bytes: '0' }] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: occurredAt,
            id: 'segment',
            object_key: key,
            run_unit_id: unitId,
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);
    const download = await test.service.download(actor, taskId, runId, {});
    let body = '';
    for await (const chunk of download.content) body += String(chunk);
    expect(body.trimEnd().split('\n')).toHaveLength(1001);
    expect(download.filename).toBe(`runtime-${runId}.log`);
  });

  it('downloads segment lines and both GAP sources in one chronological timeline', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-download-order-'));
    roots.push(root);
    const content = Buffer.from(
      `${JSON.stringify({ continuation: 0, continues: false, occurredAt: '2026-09-08T00:00:02.000Z', payload: 'middle', stream: 'STDOUT', unitId })}\n`,
    );
    const key = 'runs/run/logs/stdout/ordered.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows: [] },
      { rows: [{ max_active_bytes: '0' }] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: new Date('2026-09-08T00:00:02.500Z'),
            id: '11111111-1111-4111-8111-111111111111',
            object_key: key,
            run_unit_id: unitId,
            sequence: '1',
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      {
        rows: [
          {
            id: '22222222-2222-4222-8222-222222222222',
            lost_bytes: '3',
            occurred_at: new Date('2026-09-08T00:00:01.000Z'),
            reason: 'RAW_QUEUE_OVERFLOW',
            run_unit_id: unitId,
            unit_key: 'partition:0',
          },
        ],
      },
      {
        rows: [
          {
            deleted_at: new Date('2026-09-08T00:00:03.000Z'),
            log_segment_id: '33333333-3333-4333-8333-333333333333',
            run_unit_id: unitId,
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);

    const download = await test.service.download(actor, taskId, runId, {});
    const facts: Array<{ occurredAt: string }> = [];
    for await (const chunk of download.content) {
      facts.push(JSON.parse(String(chunk)) as { occurredAt: string });
    }

    expect(facts.map((fact) => fact.occurredAt)).toEqual([
      '2026-09-08T00:00:01.000Z',
      '2026-09-08T00:00:02.000Z',
      '2026-09-08T00:00:03.000Z',
    ]);
  });

  it('rejects an oversized available object from metadata without reading its body', async () => {
    const oversized = {
      config: { relative_prefix: '' },
      driver: 'LOCAL',
      ended_at: occurredAt,
      id: 'segment',
      object_key: 'must-not-be-read.log',
      run_unit_id: unitId,
      sha256: Buffer.alloc(32),
      size_bytes: String(1024 * 1024 + 1),
      started_at: occurredAt,
      state: 'AVAILABLE',
      stream: 'STDOUT',
      unit_key: 'partition:0',
    };
    const test = context('unused', [{ rowCount: 1, rows: [{}] }, { rows: [oversized] }]);

    await expect(test.service.download(actor, taskId, runId, {})).rejects.toEqual(
      new RuntimeLogReadError('LOG_SEGMENT_OBJECT_TOO_LARGE'),
    );
    expect(test.query).toHaveBeenCalledTimes(2);
  });

  it('rejects a missing durable end boundary during download preflight', async () => {
    const test = context('unused', [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: null,
            id: 'segment',
            object_key: 'object.log',
            run_unit_id: unitId,
            sha256: Buffer.alloc(32),
            size_bytes: '1',
            started_at: occurredAt,
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
    ]);
    await expect(test.service.download(actor, taskId, runId, {})).rejects.toEqual(
      new RuntimeLogReadError('LOG_SEGMENT_BOUNDARY_INVALID'),
    );
  });

  it('rejects an excessive overlap during download preflight before returning a stream', async () => {
    const test = context('unused', [
      { rowCount: 1, rows: [{}] },
      { rows: [] },
      { rows: [{ max_active_bytes: String(64 * 1024 * 1024 + 1) }] },
    ]);
    await expect(test.service.download(actor, taskId, runId, {})).rejects.toEqual(
      new RuntimeLogReadError('LOG_STORAGE_DECODE_BUDGET_EXCEEDED'),
    );
  });

  it('turns all lines into one integrity gap when a timestamp is outside segment bounds', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-boundary-'));
    roots.push(root);
    const content = Buffer.from(
      `${JSON.stringify({ continuation: 0, continues: false, occurredAt: '2026-09-07T23:59:59.999Z', payload: 'outside', stream: 'STDOUT', unitId })}\n`,
    );
    const key = 'runs/run/logs/stdout/outside.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      {
        rows: [
          {
            config: { relative_prefix: '' },
            driver: 'LOCAL',
            ended_at: occurredAt,
            id: 'segment',
            object_key: key,
            run_unit_id: unitId,
            sha256: Buffer.from(segmentSha256(content), 'hex'),
            size_bytes: String(content.length),
            started_at: occurredAt,
            state: 'AVAILABLE',
            stream: 'STDOUT',
            unit_key: 'partition:0',
          },
        ],
      },
      { rows: [] },
      { rows: [] },
    ]);
    await expect(test.service.list(actor, taskId, runId, { limit: 500 })).resolves.toMatchObject({
      facts: [expect.objectContaining({ kind: 'GAP', reason: 'INTEGRITY_FAILED' })],
    });
  });

  it('rejects downloads with overlapping decoded segments that exceed the total active buffer budget', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'scs-runtime-log-budget-'));
    roots.push(root);
    const prefix = JSON.stringify({
      continuation: 0,
      continues: false,
      occurredAt: '2026-09-08T01:00:00.000Z',
      payload: '',
      stream: 'STDOUT',
      unitId,
    });
    const content = Buffer.from(
      prefix.replace('"payload":""', `"payload":"${'x'.repeat(1_040_000)}"`) + '\n',
    );
    const key = 'runs/run/logs/stdout/budget.log';
    await new LocalLogSegmentObjectStore(root).put({ content, objectKey: key });
    const rows = Array.from({ length: 65 }, (_, index) => ({
      config: { relative_prefix: '' },
      driver: 'LOCAL',
      ended_at: new Date('2026-09-08T02:00:00.000Z'),
      id: `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
      object_key: key,
      run_unit_id: unitId,
      sequence: String(index),
      sha256: Buffer.from(segmentSha256(content), 'hex'),
      size_bytes: String(content.length),
      started_at: new Date(occurredAt.getTime() + index),
      state: 'AVAILABLE',
      stream: 'STDOUT',
      unit_key: 'partition:0',
    }));
    const test = context(root, [
      { rowCount: 1, rows: [{}] },
      { rows },
      { rows: [{ max_active_bytes: '0' }] },
      { rows },
    ]);

    await expect(async () => {
      const download = await test.service.download(actor, taskId, runId, {});
      for await (const chunk of download.content) void chunk;
    }).rejects.toEqual(new RuntimeLogReadError('LOG_STORAGE_DECODE_BUDGET_EXCEEDED'));
  }, 15_000);
});
