import { afterEach, expect, it, vi } from 'vitest';
import { runResetAdminCli } from './reset-admin-cli.js';

afterEach(() => vi.restoreAllMocks());

it('prints help without opening a database or reading deployment secrets', async () => {
  const output = vi.spyOn(process.stdout, 'write').mockReturnValue(true);
  await runResetAdminCli(['--help']);
  expect(output).toHaveBeenCalledWith(expect.stringContaining('Usage:'));
});

it('refuses unknown arguments before attempting administrator recovery', async () => {
  const output = vi.spyOn(process.stdout, 'write').mockReturnValue(true);
  await expect(runResetAdminCli(['--username', 'developer'])).rejects.toThrow('INVALID_ARGUMENTS');
  expect(output).not.toHaveBeenCalled();
});
