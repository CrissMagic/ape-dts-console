import { randomUUID } from 'node:crypto';

import type { EmergencyStructuredLogEvent, EmergencyStructuredLogPort } from './emergency-ports.js';

export interface EmergencyPendingEvent extends EmergencyStructuredLogEvent {
  readonly sequence: number;
}

export class EmergencyPendingEventQueue {
  private readonly pending: EmergencyPendingEvent[] = [];
  private nextSequence = 1;
  private omittedEvents = 0;
  private gapEventId: string | undefined;

  constructor(
    private readonly capacity: number,
    private readonly log: EmergencyStructuredLogPort,
    private readonly now: () => Date = () => new Date(),
  ) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error('EMERGENCY_PENDING_CAPACITY_INVALID');
    }
  }

  enqueue(event: EmergencyStructuredLogEvent): boolean {
    if (this.pending.length >= this.capacity) {
      this.gapEventId ??= randomUUID();
      this.omittedEvents += 1;
      this.log.write({
        action: 'EMERGENCY_AUDIT_GAP',
        details: { omittedEvents: this.omittedEvents, queueCapacity: this.capacity },
        eventId: randomUUID(),
        occurredAt: this.now().toISOString(),
        reason: 'EMERGENCY_PENDING_QUEUE_OVERFLOW',
        taskId: event.taskId,
      });
      return false;
    }
    this.pending.push({ ...event, sequence: this.nextSequence++ });
    return true;
  }

  snapshot(): readonly EmergencyPendingEvent[] {
    return this.pending.map((event) => ({ ...event }));
  }

  acknowledgeThrough(sequence: number): void {
    if (!Number.isSafeInteger(sequence) || sequence < 1) {
      throw new Error('EMERGENCY_PENDING_SEQUENCE_INVALID');
    }
    while (this.pending[0] !== undefined && this.pending[0].sequence <= sequence) {
      this.pending.shift();
    }
  }

  gapFact(): Readonly<{ eventId: string; omittedEvents: number }> | null {
    return this.omittedEvents === 0 || this.gapEventId === undefined
      ? null
      : { eventId: this.gapEventId, omittedEvents: this.omittedEvents };
  }

  acknowledgeGap(omittedEvents: number): void {
    if (
      !Number.isSafeInteger(omittedEvents) ||
      omittedEvents < 1 ||
      omittedEvents > this.omittedEvents
    ) {
      throw new Error('EMERGENCY_PENDING_GAP_ACKNOWLEDGEMENT_INVALID');
    }
    this.omittedEvents -= omittedEvents;
    if (this.omittedEvents === 0) {
      this.gapEventId = undefined;
    }
  }
}
