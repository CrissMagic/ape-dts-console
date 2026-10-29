import { describe, expect, it } from 'vitest';
import { evaluateAlertRule, type AlertRuleState } from './alert-rules.js';
const empty: AlertRuleState = { firstBadAt: null, lastAttemptAt: null, alarmSent: false };
describe('alert rule business behavior', () => {
  it('requires a sustained abnormal condition and suppresses repeated observations', () => {
    const first = evaluateAlertRule(empty, true, 1000, 30000, 300000, true);
    expect(first.event).toBeNull();
    expect(evaluateAlertRule(first.state, true, 30999, 30000, 300000, true).event).toBeNull();
    const alarm = evaluateAlertRule(first.state, true, 31000, 30000, 300000, true);
    expect(alarm.event).toBe('ALARM');
    expect(evaluateAlertRule(alarm.state, true, 32000, 30000, 300000, true).event).toBeNull();
    expect(evaluateAlertRule(alarm.state, true, 331000, 30000, 300000, true).event).toBe('ALARM');
  });
  it('unknown measurements never signal recovery or erase the accumulated abnormal state', () => {
    const state = { firstBadAt: 1000, lastAttemptAt: 31000, alarmSent: true };
    expect(evaluateAlertRule(state, null, 999999, 30000, 300000, true)).toEqual({
      state,
      event: null,
    });
  });
  it('does not send recovery for an alarm that never reached the channel', () => {
    const state = { firstBadAt: 1000, lastAttemptAt: 31000, alarmSent: false };
    expect(evaluateAlertRule(state, false, 999999, 30000, 300000, true)).toEqual({
      state: empty,
      event: null,
    });
  });
  it('sends recovery for delivered alarms, and respects repeat interval after a recovery failure', () => {
    const state = { firstBadAt: 1000, lastAttemptAt: 31000, alarmSent: true };
    const recovery = evaluateAlertRule(state, false, 331000, 30000, 300000, true);
    expect(recovery.event).toBe('RECOVERY');
    expect(evaluateAlertRule(recovery.state, false, 332000, 30000, 300000, true).event).toBeNull();
  });
  it('clears acknowledged alarms when recovery notification is disabled', () => {
    expect(
      evaluateAlertRule(
        { firstBadAt: 1, lastAttemptAt: 2, alarmSent: true },
        false,
        3,
        0,
        30000,
        false,
      ),
    ).toEqual({ state: empty, event: null });
  });
});
