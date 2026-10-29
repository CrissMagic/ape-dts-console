import assert from 'node:assert/strict';
import process from 'node:process';
import { randomBytes } from 'node:crypto';
import { chmod, mkdtemp, rm, stat, symlink } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { execFileSync } from 'node:child_process';
import {
  makeLocalPlatformConfig,
  readLocalPlatformConfig,
  writeLocalPlatformConfig,
} from '/app/dist/bootstrap/local-platform-config.js';

const root = await mkdtemp(join(tmpdir(), 'scs-key-smoke-'));
try {
  const location = join(root, 'private', 'nested', 'config.json');
  const config = makeLocalPlatformConfig(
    {
      host: 'localhost',
      port: 5432,
      username: 'test',
      password: 'test-only-not-a-credential',
      database: 'test',
      tls: { mode: 'DISABLE' },
    },
    randomBytes(32),
    { ciphertext: randomBytes(32).toString('base64') },
  );
  await writeLocalPlatformConfig(config, location);
  assert.equal((await stat(location)).mode & 0o777, 0o600);
  assert.equal((await stat(join(root, 'private', 'nested'))).mode & 0o777, 0o700);
  assert.deepEqual(await readLocalPlatformConfig(location), config);
  await assert.rejects(writeLocalPlatformConfig(config, location));
  const linked = join(root, 'private', 'nested', 'link.json');
  await symlink(location, linked);
  await assert.rejects(readLocalPlatformConfig(linked), /UNSAFE_FILE/);
  await chmod(location, 0o644);
  await assert.rejects(readLocalPlatformConfig(location), /UNSAFE_FILE/);
  const help = execFileSync(process.execPath, ['/app/dist/auth/reset-admin-cli.js', '--help'], {
    encoding: 'utf8',
  });
  assert.match(help, /Usage:/);
  const migrationHelp = execFileSync(
    process.execPath,
    ['/app/dist/bootstrap/security-cli.js', '--help'],
    { encoding: 'utf8' },
  );
  assert.match(migrationHelp, /Usage:/);
  process.stdout.write(
    'Linux key smoke passed: durable nested creation, 0700/0600 permissions, exact reload, no overwrite, symlink/public-file rejection, non-mutating CLI help.\n',
  );
} finally {
  await rm(root, { recursive: true, force: true });
}
