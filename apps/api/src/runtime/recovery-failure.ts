/** Only emit bounded categories; never copy connection URLs or exception text. */
export function recoveryFailureReason(
  error: unknown,
  fallback = 'RUNTIME_RECOVERY_FAILED',
): string {
  if (!(error instanceof Error)) return fallback;
  const value = error as Error & {
    code?: unknown;
    details?: { reason?: unknown };
    cause?: unknown;
  };
  for (const code of [value.details?.reason, value.code, value.message]) {
    if (typeof code === 'string' && /^[A-Z0-9_]{1,100}$/u.test(code)) return code;
  }
  if (/timeout exceeded when trying to connect/iu.test(value.message))
    return 'METADATA_POOL_ACQUIRE_TIMEOUT';
  if (/query read timeout/iu.test(value.message)) return 'METADATA_QUERY_TIMEOUT';
  if (value.cause instanceof Error && value.cause !== error) {
    const cause = value.cause as Error & { code?: unknown };
    if (typeof cause.code === 'string' && /^[A-Z0-9_]{1,100}$/u.test(cause.code)) return cause.code;
  }
  return fallback;
}
