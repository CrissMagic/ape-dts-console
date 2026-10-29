import { describe, expect, it, vi } from 'vitest';

import { EmergencyPendingEventQueue } from './emergency-pending-events.js';
import type { EmergencyStructuredLogPort } from './emergency-ports.js';

describe('EmergencyPendingEventQueue', () => {
  it('bounds pending facts and explicitly logs an audit gap on overflow', () => {
    const log = { write: vi.fn() } satisfies EmergencyStructuredLogPort;
    const queue = new EmergencyPendingEventQueue(1, log, () => new Date('2026-09-08T00:00:00Z'));
    const first = {
      action: 'EMERGENCY_STOP_REQUESTED' as const,
      eventId: 'event-1',
      occurredAt: '2026-09-08T00:00:00.000Z',
      reason: null,
      taskId: 'task-1',
    };

    expect(queue.enqueue(first)).toBe(true);
    expect(queue.enqueue({ ...first, taskId: 'task-2' })).toBe(false);

    expect(queue.snapshot()).toHaveLength(1);
    expect(queue.gapFact()).toEqual({ eventId: expect.any(String), omittedEvents: 1 });
    expect(log.write).toHaveBeenCalledWith(
      expect.objectContaining({
        action: 'EMERGENCY_AUDIT_GAP',
        reason: 'EMERGENCY_PENDING_QUEUE_OVERFLOW',
        taskId: 'task-2',
      }),
    );

    queue.acknowledgeGap(1);
    expect(queue.gapFact()).toBeNull();
  });

  it('only acknowledges facts proven persisted by their monotonic sequence', () => {
    const queue = new EmergencyPendingEventQueue(2, { write: vi.fn() });
    const event = {
      action: 'EMERGENCY_STOP_REQUESTED' as const,
      eventId: 'event-1',
      occurredAt: '2026-09-08T00:00:00.000Z',
      reason: null,
      taskId: 'task-1',
    };
    queue.enqueue(event);
    queue.enqueue({ ...event, eventId: 'event-2', taskId: 'task-2' });

    queue.acknowledgeThrough(1);

    expect(queue.snapshot()).toEqual([expect.objectContaining({ sequence: 2, taskId: 'task-2' })]);
  });
});
