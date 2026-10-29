import { mkdtemp, mkdir, writeFile, readFile, rm, lstat, symlink, readdir } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { watch } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { spawnSync } from 'node:child_process';
import tar from 'tar-stream';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ArtifactFiles } from './artifact-files.js';

const ELF = Buffer.from([0x7f, 0x45, 0x4c, 0x46, 1, 2, 3, 4]);
async function archive(
  entries: {
    name: string;
    type?: 'file' | 'directory' | 'symlink' | 'link';
    body?: Buffer;
    linkname?: string;
  }[],
) {
  const pack = tar.pack();
  const chunks: Buffer[] = [];
  const collecting = (async () => {
    for await (const chunk of pack) chunks.push(Buffer.from(chunk as Uint8Array));
  })();
  for (const entry of entries)
    pack.entry(
      {
        name: entry.name,
        type: entry.type ?? 'file',
        ...(entry.linkname ? { linkname: entry.linkname } : {}),
      },
      entry.body ?? Buffer.alloc(0),
    );
  pack.finalize();
  await collecting;
  return Buffer.concat(chunks);
}
describe('ArtifactFiles actual file preparation', () => {
  let root: string;
  let files: ArtifactFiles;
  beforeEach(async () => {
    root = await mkdtemp(path.join(tmpdir(), 'scs-import-test-'));
    files = new ArtifactFiles(root);
  });
  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });
  async function candidate(name: string, bytes: Buffer) {
    await files.scan('tenant');
    const file = path.join(root, 'tenant', 'artifact-inbox', name);
    await writeFile(file, bytes);
    const status = await lstat(file);
    return { name, sizeBytes: status.size, modifiedAt: status.mtime.toISOString() };
  }
  it('accepts dt-main engine packages through upload and container selection', async () => {
    const bytes = gzipSync(
      await archive([
        { name: './', type: 'directory' },
        { name: './dt-main', body: ELF },
        { name: './log4rs.yaml', body: Buffer.from('config') },
      ]),
    );
    const uploaded = await files.upload(
      'tenant',
      'engine.tar.gz',
      Readable.from([bytes]),
      async (binary) => {
        expect(binary.fileName).toBe('dt-main');
        expect(await readFile(binary.path)).toEqual(ELF);
        return binary.sha256;
      },
    );
    await files.withPrepared('tenant', uploaded.file, async (binary) => {
      expect(binary.sha256).toBe(uploaded.prepared);
      const published = await files.publish('tenant', binary);
      expect(published.objectKey).toMatch(/\/ape-dts$/u);
      expect(await readFile(path.join(root, 'tenant', published.objectKey))).toEqual(ELF);
    });
  });
  it('explains CLI-only packages and cleans the failed upload', async () => {
    const bytes = gzipSync(await archive([{ name: './dtscli', body: ELF }]));
    await expect(
      files.upload('tenant', 'cli.tar.gz', Readable.from([bytes]), async () => null),
    ).rejects.toMatchObject({ code: 'ARTIFACT_ARCHIVE_CLI_ONLY' });
    expect((await files.scan('tenant')).files).toEqual([]);
  });
  it('still rejects ambiguous engines and invalid dt-main content', async () => {
    for (const [entries, code] of [
      [
        [
          { name: 'dt-main', body: ELF },
          { name: 'ape-dts', body: ELF },
        ],
        'ARTIFACT_ARCHIVE_AMBIGUOUS',
      ],
      [[{ name: 'dt-main', body: Buffer.from('not ELF') }], 'ARTIFACT_ARCHIVE_BINARY_MISSING'],
    ] as const) {
      const selected = await candidate('invalid.tar.gz', gzipSync(await archive([...entries])));
      await expect(files.withPrepared('tenant', selected, async () => null)).rejects.toMatchObject({
        code,
      });
    }
  });
  it('uploads complete files without overwriting existing files or exposing partial data', async () => {
    const result = await files.upload('tenant', 'ape-dts', Readable.from([ELF]), async (binary) => {
      expect((await files.scan('tenant')).files).toEqual([]);
      expect(await readFile(binary.path)).toEqual(ELF);
      return binary.sha256;
    });
    expect(result.file.name).toBe('ape-dts');
    expect(result.prepared).toHaveLength(64);
    await expect(
      files.upload('tenant', 'ape-dts', Readable.from([ELF]), async () => null),
    ).rejects.toMatchObject({ code: 'ARTIFACT_FILE_ALREADY_EXISTS' });
    expect(await readFile(path.join(root, 'tenant/artifact-inbox/ape-dts'))).toEqual(ELF);
    expect(await readdir(path.join(root, 'tenant/artifact-inbox/.uploads'))).toEqual([]);
  });
  it('uploads gzip and xz using the original file name for the suggested version', async () => {
    const tarBytes = await archive([{ name: 'release/ape-dts', body: ELF }]);
    const xz = spawnSync('xz', ['--compress', '--stdout'], { input: tarBytes });
    expect(xz.status).toBe(0);
    for (const [name, bytes] of [
      ['release.tar.gz', gzipSync(tarBytes)],
      ['release.tar.xz', xz.stdout],
    ] as const) {
      const result = await files.upload('', name, Readable.from([bytes]), async (binary) => {
        expect(await readFile(binary.path)).toEqual(ELF);
        return binary.suggestedVersion;
      });
      expect(result.prepared).toBe('release');
    }
  });
  it('cleans failed, empty, corrupt and cancelled uploads', async () => {
    for (const [name, bytes] of [
      ['empty', Buffer.alloc(0)],
      ['bad.tar.gz', Buffer.from('invalid')],
      ['text', Buffer.from('text')],
    ] as const) {
      await expect(
        files.upload('', name, Readable.from([bytes]), async () => null),
      ).rejects.toThrow();
    }
    await expect(
      files.upload('', '../outside', Readable.from([ELF]), async () => null),
    ).rejects.toMatchObject({ code: 'ARTIFACT_PATH_INVALID' });
    await expect(
      files.upload('', 'ape-dts', Readable.from([ELF]), async () => {
        throw new Error('inspection failed');
      }),
    ).rejects.toThrow('inspection failed');
    const cancellation = new AbortController();
    await expect(
      files.upload(
        '',
        'ape-dts',
        Readable.from([ELF]),
        async () => {
          cancellation.abort();
        },
        cancellation.signal,
      ),
    ).rejects.toThrow();
    const disconnected = Readable.from(
      (async function* () {
        yield ELF;
        throw new Error('disconnected');
      })(),
    );
    await expect(files.upload('', 'ape-dts', disconnected, async () => null)).rejects.toThrow(
      'disconnected',
    );
    expect((await files.scan('')).files).toEqual([]);
    expect(await readdir(path.join(root, 'artifact-inbox/.uploads'))).toEqual([]);
  });
  it('rejects oversized streaming uploads and concurrent preparation', async () => {
    const oversized = Readable.from(
      (async function* () {
        const chunk = Buffer.alloc(8 * 1024 * 1024);
        for (let i = 0; i < 65; i++) yield chunk;
      })(),
    );
    await expect(files.upload('', 'large', oversized, async () => null)).rejects.toMatchObject({
      code: 'ARTIFACT_SIZE_LIMIT',
    });
    await files.upload('', 'ape-dts', Readable.from([ELF]), async () => {
      await expect(
        files.upload('', 'other', Readable.from([ELF]), async () => null),
      ).rejects.toMatchObject({ code: 'ARTIFACT_FILE_BUSY' });
    });
    expect(await readdir(path.join(root, 'artifact-inbox/.uploads'))).toEqual([]);
  });
  it.each(['gz', 'xz'])(
    'cancels %s extraction immediately and releases the upload slot',
    async (format) => {
      const tarBytes = await archive([
        { name: 'ape-dts', body: Buffer.concat([ELF, Buffer.alloc(64 * 1024 * 1024)]) },
      ]);
      const bytes =
        format === 'gz'
          ? gzipSync(tarBytes)
          : spawnSync('xz', ['--compress', '--stdout'], { input: tarBytes }).stdout;
      const cancellation = new AbortController();
      let cancelledDuringExtraction = false;
      const watcher = watch(root, { recursive: true }, (_event, filename) => {
        if (filename?.includes('candidate-')) {
          cancelledDuringExtraction = true;
          cancellation.abort();
        }
      });
      try {
        await expect(
          files.upload(
            '',
            `cancel.tar.${format}`,
            Readable.from([bytes]),
            async () => null,
            cancellation.signal,
          ),
        ).rejects.toThrow();
        expect(cancelledDuringExtraction).toBe(true);
      } finally {
        watcher.close();
      }
      expect((await files.scan('')).files).toEqual([]);
      expect(await readdir(path.join(root, 'artifact-inbox/.uploads'))).toEqual([]);
      await expect(
        files.upload('', 'next', Readable.from([ELF]), async () => 'ready'),
      ).resolves.toMatchObject({ prepared: 'ready' });
    },
  );
  it('scans only direct ELF and supported archives, prepares a private snapshot, publishes relative to profile', async () => {
    const selected = await candidate('ape-dts-v1', ELF);
    await candidate('README', Buffer.from('not executable'));
    await mkdir(path.join(root, 'tenant', 'artifact-inbox', 'nested'));
    expect((await files.scan('tenant')).files.map((file) => file.name)).toEqual(['ape-dts-v1']);
    let temporary = '';
    await files.withPrepared('tenant', selected, async (binary) => {
      temporary = binary.path;
      expect(await readFile(binary.path)).toEqual(ELF);
      expect(binary.sha256).toHaveLength(64);
      const published = await files.publish('tenant', binary);
      expect(published.objectKey).toMatch(/^artifacts\/imported\/[^/]+\/ape-dts$/u);
      expect(await readFile(path.join(root, 'tenant', published.objectKey))).toEqual(ELF);
      await published.remove();
      await expect(lstat(path.join(root, 'tenant', published.objectKey))).rejects.toThrow();
    });
    await expect(lstat(temporary)).rejects.toThrow();
  });
  it('rejects files replaced since scanning and path traversal', async () => {
    const selected = await candidate('ape-dts', ELF);
    await writeFile(
      path.join(root, 'tenant', 'artifact-inbox', selected.name),
      Buffer.concat([ELF, ELF]),
    );
    await expect(
      files.withPrepared('tenant', selected, async () => undefined),
    ).rejects.toMatchObject({ code: 'ARTIFACT_FILE_CHANGED' });
    await expect(files.scan('../escape')).rejects.toMatchObject({ code: 'ARTIFACT_PATH_INVALID' });
    await expect(
      files.withPrepared('tenant', { ...selected, name: '../ape-dts' }, async () => undefined),
    ).rejects.toMatchObject({ code: 'ARTIFACT_PATH_INVALID' });
  });
  it('prepares gzip archive and ignores unrelated files without extracting them', async () => {
    const bytes = await archive([
      { name: 'release', type: 'directory' },
      { name: 'release/README', body: Buffer.from('notes') },
      { name: 'release/ape-dts', body: ELF },
    ]);
    const selected = await candidate('ape-dts-v1.tar.gz', gzipSync(bytes));
    await files.withPrepared('tenant', selected, async (binary) => {
      expect(await readFile(binary.path)).toEqual(ELF);
      expect(binary.suggestedVersion).toBe('ape-dts-v1');
      await expect(lstat(path.join(path.dirname(binary.path), 'release'))).rejects.toThrow();
    });
  });
  it.each(['../escape', '/absolute', 'C:/windows', 'dir\\escape'])(
    'rejects dangerous tar member %s',
    async (name) => {
      const selected = await candidate(
        'bad.tar.gz',
        gzipSync(await archive([{ name, body: ELF }])),
      );
      await expect(
        files.withPrepared('tenant', selected, async () => undefined),
      ).rejects.toMatchObject({ code: 'ARTIFACT_PATH_INVALID' });
    },
  );
  it.each(['symlink', 'link'] as const)('rejects archive %s entries', async (type) => {
    const selected = await candidate(
      'bad.tar.gz',
      gzipSync(await archive([{ name: 'ape-dts', type, linkname: '/outside' }])),
    );
    await expect(
      files.withPrepared('tenant', selected, async () => undefined),
    ).rejects.toMatchObject({ code: 'ARTIFACT_ARCHIVE_INVALID' });
  });
  it('rejects duplicate members, ambiguous and missing binary', async () => {
    for (const [entries, code] of [
      [
        [
          { name: 'ape-dts', body: ELF },
          { name: 'ape-dts', body: ELF },
        ],
        'ARTIFACT_ARCHIVE_INVALID',
      ],
      [
        [
          { name: 'a/ape-dts', body: ELF },
          { name: 'b/ape_dts', body: ELF },
        ],
        'ARTIFACT_ARCHIVE_AMBIGUOUS',
      ],
      [[{ name: 'README', body: ELF }], 'ARTIFACT_ARCHIVE_BINARY_MISSING'],
    ] as const) {
      const selected = await candidate('bad.tar.gz', gzipSync(await archive([...entries])));
      await expect(
        files.withPrepared('tenant', selected, async () => undefined),
      ).rejects.toMatchObject({ code });
    }
  });
  it('preserves callback errors and removes temporary snapshot', async () => {
    const selected = await candidate('ape-dts', ELF);
    let temporary = '';
    const error = new Error('database unavailable');
    await expect(
      files.withPrepared('tenant', selected, async (binary) => {
        temporary = binary.path;
        throw error;
      }),
    ).rejects.toBe(error);
    await expect(lstat(temporary)).rejects.toThrow();
  });
  it('rejects concurrent preparation and releases the guard after completion', async () => {
    const selected = await candidate('ape-dts', ELF);
    let release!: () => void;
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = files.withPrepared('tenant', selected, async () => {
      entered();
      await held;
    });
    await ready;
    await expect(
      files.withPrepared('tenant', selected, async () => undefined),
    ).rejects.toMatchObject({ code: 'ARTIFACT_FILE_BUSY' });
    release();
    await first;
    await expect(files.withPrepared('tenant', selected, async () => 'ready')).resolves.toBe(
      'ready',
    );
  });
  it('accepts conventional dot-prefixed archive members', async () => {
    const selected = await candidate(
      'ape-dts.tar.gz',
      gzipSync(
        await archive([
          { name: './', type: 'directory' },
          { name: './ape-dts', body: ELF },
        ]),
      ),
    );
    await files.withPrepared('tenant', selected, async (binary) => {
      expect(await readFile(binary.path)).toEqual(ELF);
    });
  });
  it('rejects corrupted compressed input without invoking callback', async () => {
    const selected = await candidate('corrupted.tar.gz', Buffer.from('not gzip'));
    await expect(
      files.withPrepared('tenant', selected, async () => {
        throw new Error('must not reach callback');
      }),
    ).rejects.toMatchObject({ code: 'ARTIFACT_ARCHIVE_INVALID' });
  });
  it.skipIf(process.platform === 'win32')(
    'rejects linked profile paths and does not browse symlinks',
    async () => {
      await mkdir(path.join(root, 'outside'));
      await symlink(path.join(root, 'outside'), path.join(root, 'linked'));
      await expect(files.scan('linked')).rejects.toMatchObject({ code: 'ARTIFACT_PATH_INVALID' });
      await candidate('ape-dts', ELF);
      await symlink(
        path.join(root, 'outside'),
        path.join(root, 'tenant', 'artifact-inbox', 'symlink'),
      );
      expect((await files.scan('tenant')).files).toHaveLength(1);
    },
  );
  it.skipIf(spawnSync('xz', ['--version']).status !== 0)('prepares real xz archive', async () => {
    const compressed = spawnSync('xz', ['-c'], {
      input: await archive([{ name: 'release/ape-dts', body: ELF }]),
      maxBuffer: 1024 * 1024,
    });
    expect(compressed.status).toBe(0);
    const selected = await candidate('ape-dts.tar.xz', compressed.stdout);
    await files.withPrepared('tenant', selected, async (binary) => {
      expect(await readFile(binary.path)).toEqual(ELF);
    });
  });
  it.skipIf(spawnSync('xz', ['--version']).status !== 0)(
    'rejects xz nonzero exit even when tar stream was produced',
    async () => {
      const compressed = spawnSync('xz', ['-c'], {
        input: await archive([{ name: 'ape-dts', body: ELF }]),
        maxBuffer: 1024 * 1024,
      });
      const selected = await candidate(
        'truncated.tar.xz',
        compressed.stdout.subarray(0, compressed.stdout.length - 12),
      );
      await expect(
        files.withPrepared('tenant', selected, async () => undefined),
      ).rejects.toMatchObject({ code: 'ARTIFACT_ARCHIVE_INVALID' });
    },
  );
});
