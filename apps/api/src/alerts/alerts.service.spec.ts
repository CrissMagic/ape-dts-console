import { describe, expect, it, vi } from 'vitest';
import { AlertsService } from './alerts.service.js';
import { PlatformDatabaseService } from '../database/platform-database.service.js';
import { UnlockedKeyService } from '../security/unlocked-key.service.js';
import { AuditLedgerService } from '../audit/audit-ledger.service.js';
import { decryptSecret } from '@scs/security';

const defaults = {
  channel: 'WECOM' as const,
  notifyRecovery: true,
  durationSeconds: 30,
  repeatSeconds: 300,
  slotInactive: true,
  slotMissing: true,
  retainedWalBytes: null,
  unconfirmedWalBytes: null,
  kafkaOffsetLag: null,
  delayMilliseconds: null,
};
const webhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abcdefgh';
function fixture() {
  const keys = new UnlockedKeyService();
  keys.set(Buffer.alloc(32, 9));
  const row = {
    enabled: false,
    configuration: defaults,
    encrypted_webhook: null as ReturnType<typeof import('@scs/security').encryptSecret> | null,
    row_version: '1',
  };
  const query = vi.fn(async (sql: string, values?: unknown[]) => {
    if (sql.startsWith('SELECT enabled')) return { rows: [row] };
    if (sql.startsWith('UPDATE ape_dts_platform.alert_settings')) {
      row.enabled = values![0] as boolean;
      row.configuration = JSON.parse(values![1] as string);
      row.encrypted_webhook = JSON.parse(values![2] as string);
      row.row_version = String(Number(row.row_version) + 1);
    }
    if (sql.startsWith('INSERT INTO ape_dts_platform.alert_deliveries'))
      return { rows: [{ created_at: new Date('2026-10-15T00:00:00Z') }] };
    return { rows: [] };
  });
  const client = { query, release: vi.fn() };
  const database = {
    requirePool: () => ({ query, connect: async () => client }),
  } as unknown as PlatformDatabaseService;
  const audit = { append: vi.fn() } as unknown as AuditLedgerService;
  return { service: new AlertsService(database, keys, audit), row, keys, query, audit };
}
describe('alert configuration persistence', () => {
  it('encrypts the destination and returns only the configured flag', async () => {
    const f = fixture();
    const result = await f.service.update('actor', {
      ...defaults,
      enabled: true,
      expectedRowVersion: '1',
      webhookUrl: webhook,
    });
    expect(result.configured).toBe(true);
    expect(JSON.stringify(result)).not.toContain('abcdefgh');
    expect(JSON.stringify(f.row.encrypted_webhook)).not.toContain('abcdefgh');
    const decrypted = decryptSecret(f.keys.require(), f.row.encrypted_webhook!, {
      aadVersion: 1,
      ownerType: 'ALERT_SETTINGS',
      ownerId: '1',
      purpose: 'WECOM_WEBHOOK',
      secretId: '1',
    });
    expect(decrypted.toString()).toBe(webhook);
    decrypted.fill(0);
    expect(f.audit.append).toHaveBeenCalled();
  });
  it('retains the encrypted destination when the masked field is left empty', async () => {
    const f = fixture();
    const result = await f.service.update('actor', {
      ...defaults,
      enabled: true,
      expectedRowVersion: '1',
      webhookUrl: webhook,
    });
    const original = f.row.encrypted_webhook;
    await f.service.update('actor', {
      ...defaults,
      enabled: true,
      expectedRowVersion: result.rowVersion,
      webhookUrl: '',
    });
    expect(f.row.encrypted_webhook).toEqual(original);
  });
  it('rejects enabling a missing destination without changing stored settings', async () => {
    const f = fixture();
    await expect(
      f.service.update('actor', { ...defaults, enabled: true, expectedRowVersion: '1' }),
    ).rejects.toMatchObject({ response: { code: 'ALERT_WEBHOOK_REQUIRED' } });
    expect(f.row.enabled).toBe(false);
  });
  it('rejects stale edits and invalid URLs before persisting', async () => {
    const f = fixture();
    await expect(
      f.service.update('actor', { ...defaults, enabled: false, expectedRowVersion: '99' }),
    ).rejects.toMatchObject({ response: { code: 'ALERT_SETTINGS_VERSION_CONFLICT' } });
    await expect(
      f.service.update('actor', {
        ...defaults,
        enabled: false,
        expectedRowVersion: '1',
        webhookUrl: 'https://localhost/secret',
      }),
    ).rejects.toMatchObject({ response: { code: 'INVALID_WECOM_WEBHOOK' } });
    expect(f.row.row_version).toBe('1');
  });
  it('persists failed test delivery and does not expose transport error details', async () => {
    const f = fixture();
    await f.service.update('actor', {
      ...defaults,
      enabled: false,
      expectedRowVersion: '1',
      webhookUrl: webhook,
    });
    // Replace only the channel transport; no real external message is sent.
    vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(new Error(webhook));
    // The service captures fetch on construction, so make a new instance after stubbing it.
    const senderFixture = fixture();
    senderFixture.row.encrypted_webhook = f.row.encrypted_webhook;
    try {
      const result = await senderFixture.service.test('actor');
      expect(result.status).toBe('FAILED');
      expect(result.failureCode).toBe('CHANNEL_UNREACHABLE');
      expect(JSON.stringify(result)).not.toContain('abcdefgh');
      expect(
        senderFixture.query.mock.calls.some(([sql]) =>
          sql.startsWith('UPDATE ape_dts_platform.alert_deliveries'),
        ),
      ).toBe(true);
    } finally {
      vi.restoreAllMocks();
    }
  });
});
