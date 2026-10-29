// Run with the built runtime image and this file mounted read-only at /tmp/check.mjs.
import assert from 'node:assert/strict';
import console from 'node:console';
import { mkdtemp, mkdir, copyFile, lstat, symlink, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { ArtifactFiles } from '/app/dist/artifacts/artifact-files.js';

const root = await mkdtemp(path.join(tmpdir(), 'scs-artifact-linux-'));
try {
  const files = new ArtifactFiles(root);
  const { directory } = await files.scan('local');
  const packaging = path.join(root, 'packaging');
  await mkdir(packaging);
  await copyFile('/bin/true', path.join(packaging, 'ape-dts'));
  const expected = createHash('sha256')
    .update(await readFile('/bin/true'))
    .digest('hex');
  const uploaded = await files.upload(
    'local',
    'uploaded-ape-dts',
    createReadStream('/bin/true'),
    async (binary) => ({ sha256: binary.sha256 }),
  );
  assert.equal(uploaded.prepared.sha256, expected);
  await assert.rejects(
    files.upload('local', 'uploaded-ape-dts', createReadStream('/bin/true'), async () => ({})),
    { code: 'ARTIFACT_FILE_ALREADY_EXISTS' },
  );
  assert.equal(
    createHash('sha256')
      .update(await readFile(path.join(directory, uploaded.file.name)))
      .digest('hex'),
    expected,
  );
  for (const [suffix, flag] of [
    ['gz', '-czf'],
    ['xz', '-cJf'],
  ]) {
    const name = `fixture.tar.${suffix}`;
    execFileSync('tar', [flag, path.join(directory, name), '-C', packaging, 'ape-dts']);
    const archiveUpload = await files.upload(
      'local',
      `uploaded-${name}`,
      createReadStream(path.join(directory, name)),
      async (binary) => ({ sha256: binary.sha256 }),
    );
    assert.equal(archiveUpload.prepared.sha256, expected);
    const candidate = (await files.scan('local')).files.find((item) => item.name === name);
    await files.withPrepared('local', candidate, async (binary) => {
      assert.equal(binary.sha256, expected);
      assert.equal(binary.fileName, 'ape-dts');
      const published = await files.publish('local', binary);
      assert.equal(
        createHash('sha256')
          .update(await readFile(path.join(root, 'local', published.objectKey)))
          .digest('hex'),
        expected,
      );
      await published.remove();
    });
  }
  await symlink('/bin/true', path.join(directory, 'unsafe-binary'));
  assert.ok(!(await files.scan('local')).files.some((item) => item.name === 'unsafe-binary'));
  const info = await lstat(path.join(directory, 'unsafe-binary'));
  await assert.rejects(
    files.withPrepared(
      'local',
      { name: 'unsafe-binary', sizeBytes: info.size, modifiedAt: info.mtime.toISOString() },
      async () => {},
    ),
    { code: 'ARTIFACT_FILE_CHANGED' },
  );
  await symlink(packaging, path.join(root, 'linked-profile'));
  await assert.rejects(files.scan('linked-profile'), { code: 'ARTIFACT_PATH_INVALID' });
  await symlink('/bin/true', path.join(packaging, 'bad-link'));
  execFileSync('tar', [
    '-czf',
    path.join(directory, 'linked.tar.gz'),
    '-C',
    packaging,
    'ape-dts',
    'bad-link',
  ]);
  const linked = (await files.scan('local')).files.find((item) => item.name === 'linked.tar.gz');
  await assert.rejects(
    files.withPrepared('local', linked, async () => {}),
    { code: 'ARTIFACT_ARCHIVE_INVALID' },
  );
  console.log(
    'Linux smoke passed: binary/gzip/xz upload, collision preserved, snapshots, published bytes, file/profile symlinks, archive symlink rejection',
  );
} finally {
  assert.equal(path.dirname(root), tmpdir());
  assert.ok(path.basename(root).startsWith('scs-artifact-linux-'));
  await rm(root, { recursive: true, force: true });
}
