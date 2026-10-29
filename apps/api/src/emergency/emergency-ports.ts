export const EMERGENCY_RUNTIME_PORT = Symbol('EMERGENCY_RUNTIME_PORT');
export const EMERGENCY_METADATA_STATE_PORT = Symbol('EMERGENCY_METADATA_STATE_PORT');
export const EMERGENCY_STRUCTURED_LOG_PORT = Symbol('EMERGENCY_STRUCTURED_LOG_PORT');

// Kept structurally identical to packages/contracts/src/emergency.ts. The root
// integration will export that contract from @scs/contracts without making this
// isolated unit modify the shared barrel.
export interface EmergencyRuntimeTask {
  readonly runId: string;
  readonly taskId: string;
  readonly unitCount: number;
}

export interface EmergencyRuntimeBinding extends EmergencyRuntimeTask {
  /**
   * SHA-256 over the sorted `unitId:processToken` bindings.  The digest is
   * reconciliation evidence only and must never be returned by the HTTP list.
   */
  readonly processBindingHash: string;
  readonly operationId: string;
  readonly platformInstanceId: string;
  readonly revisionId: string;
}

export interface EmergencyRuntimePort {
  listActiveTasks(): readonly EmergencyRuntimeBinding[];
  sendSigint(taskId: string): Promise<void>;
  forceKill(taskId: string): Promise<void>;
}

export interface EmergencyMetadataStatePort {
  isMetadataUnavailable(): boolean;
}

export interface EmergencyStructuredLogPort {
  write(event: EmergencyStructuredLogEvent): void;
}

export interface EmergencyStructuredLogEvent {
  readonly action:
    | 'EMERGENCY_AUDIT_GAP'
    | 'EMERGENCY_FORCE_KILL_CONFIRMED'
    | 'EMERGENCY_FORCE_KILL_REQUESTED'
    | 'EMERGENCY_STOP_CONFIRMED'
    | 'EMERGENCY_STOP_REQUESTED';
  readonly eventId: string;
  readonly occurredAt: string;
  readonly reason: string | null;
  readonly taskId: string | null;
  readonly processBindingHash?: string;
  readonly operationId?: string;
  readonly platformInstanceId?: string;
  readonly revisionId?: string;
  readonly details?: Readonly<Record<string, unknown>>;
}
