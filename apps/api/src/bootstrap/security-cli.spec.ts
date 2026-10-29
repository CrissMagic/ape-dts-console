import { afterEach, expect, it, vi } from 'vitest';

import { runSecurityCli, securityCommandErrorCode } from './security-cli.js';

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

it('prints help without reading configuration or requesting a passphrase', async () => {
  vi.stubEnv('SCS_PLATFORM_CONFIG_PATH', 'invalid\0path');
  vi.stubEnv('SCS_PLATFORM_DATABASE_CONFIG', 'not json');
  const input = vi.fn(async () => 'secret');
  const output = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
  await runSecurityCli(['--help'], input);
  expect(output).toHaveBeenCalledWith(expect.stringContaining('Usage:'));
  expect(input).not.toHaveBeenCalled();
});

it('rejects unknown and extra arguments before touching configuration', async () => {
  vi.stubEnv('SCS_PLATFORM_CONFIG_PATH', 'invalid\0path');
  for (const args of [
    [],
    ['upgrade'],
    ['--help', '--backup-confirmed'],
    ['upgrade', '--backup-confirmed', '--unknown'],
  ]) {
    await expect(runSecurityCli(args)).rejects.toThrow('INVALID_SECURITY_COMMAND_ARGUMENTS');
  }
});

it('reports known diagnostics but never exposes unknown exception details', () => {
  expect(securityCommandErrorCode(new Error('LOCAL_KEY_DATABASE_MISMATCH'))).toBe(
    'LOCAL_KEY_DATABASE_MISMATCH',
  );
  expect(securityCommandErrorCode(new Error('postgres://owner:secret@host'))).toBe(
    'SECURITY_COMMAND_FAILED',
  );
});
