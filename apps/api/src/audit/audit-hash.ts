import { createHash } from 'node:crypto';

function canonicalize(value: unknown): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'number') {
    return JSON.stringify(value);
  }
  if (typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(',')}]`;
  }
  if (typeof value === 'object') {
    const input = value as Record<string, unknown>;
    return `{${Object.keys(input)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalize(input[key])}`)
      .join(',')}}`;
  }
  throw new Error('AUDIT_VALUE_NOT_CANONICALIZABLE');
}

export interface AuditHashInput {
  readonly action: string;
  readonly actorType: 'DEPLOYMENT_ADMIN' | 'SYSTEM' | 'USER';
  readonly actorUserId: string | null;
  readonly details: Readonly<Record<string, unknown>>;
  readonly eventId: string;
  readonly occurredAt: string;
  readonly subjectId: string | null;
  readonly subjectType: string;
}

export function calculateAuditEventHash(
  previousHash: Uint8Array | null,
  event: AuditHashInput,
): Buffer {
  const hash = createHash('sha256');
  if (previousHash !== null) {
    hash.update(previousHash);
  }
  hash.update(canonicalize(event), 'utf8');
  return hash.digest();
}
