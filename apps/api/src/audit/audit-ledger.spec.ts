import { randomUUID } from 'node:crypto';

import { describe, expect, it } from 'vitest';

import { calculateAuditEventHash, type AuditHashInput } from './audit-hash.js';
import { verifyAuditChainRows, type AuditChainRow } from './audit-ledger.service.js';

function row(
  sequence: number,
  previousHash: Buffer | null,
  overrides: Partial<AuditHashInput> = {},
): AuditChainRow {
  const event: AuditHashInput = {
    action: 'USER_LOGIN_SUCCEEDED',
    actorType: 'USER',
    actorUserId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3',
    details: { method: 'PASSWORD' },
    eventId: randomUUID(),
    occurredAt: `2026-09-01T00:00:0${sequence}.000Z`,
    subjectId: '018f6da0-6ca7-7dcb-8bb7-38fdb986a3f3',
    subjectType: 'USER',
    ...overrides,
  };
  return {
    ...event,
    eventHash: calculateAuditEventHash(previousHash, event),
    previousHash,
    sequence: String(sequence),
  };
}

describe('audit ledger verification', () => {
  it('verifies a complete chain from its independent canonical event values', () => {
    const first = row(1, null);
    const second = row(2, first.eventHash);

    expect(verifyAuditChainRows([first, second], null)).toEqual({
      checkedEvents: 2,
      valid: true,
    });
  });

  it('reports modified details and a truncated prefix without a checkpoint', () => {
    const first = row(1, null);
    const second = row(2, first.eventHash);
    const changedSecond = {
      ...second,
      details: { method: 'RECOVERY_CODE' },
    };

    expect(verifyAuditChainRows([first, changedSecond], null)).toEqual({
      checkedEvents: 2,
      reason: 'EVENT_HASH_MISMATCH',
      sequence: '2',
      valid: false,
    });
    expect(verifyAuditChainRows([second], null)).toEqual({
      checkedEvents: 0,
      reason: 'MISSING_CHECKPOINT',
      sequence: '2',
      valid: false,
    });
  });
});
