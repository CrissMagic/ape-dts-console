export interface AlertRuleState {
  firstBadAt: number | null;
  lastAttemptAt: number | null;
  alarmSent: boolean;
}

export function evaluateAlertRule(
  state: AlertRuleState,
  bad: boolean | null,
  now: number,
  durationMs: number,
  repeatMs: number,
  notifyRecovery: boolean,
): { state: AlertRuleState; event: 'ALARM' | 'RECOVERY' | null } {
  if (bad === null) return { state, event: null };
  if (!bad) {
    if (state.alarmSent && notifyRecovery) {
      const due =
        state.firstBadAt !== null ||
        state.lastAttemptAt === null ||
        now - state.lastAttemptAt >= repeatMs;
      return {
        state: { ...state, firstBadAt: null, lastAttemptAt: due ? now : state.lastAttemptAt },
        event: due ? 'RECOVERY' : null,
      };
    }
    return { state: { firstBadAt: null, lastAttemptAt: null, alarmSent: false }, event: null };
  }
  const firstBadAt = state.firstBadAt ?? now;
  const due =
    now - firstBadAt >= durationMs &&
    (state.lastAttemptAt === null || now - state.lastAttemptAt >= repeatMs);
  return {
    state: { ...state, firstBadAt, lastAttemptAt: due ? now : state.lastAttemptAt },
    event: due ? 'ALARM' : null,
  };
}
