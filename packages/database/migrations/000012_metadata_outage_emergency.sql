CREATE TABLE ape_dts_platform.metadata_outage_events (
  id uuid PRIMARY KEY,
  platform_instance_id uuid NOT NULL REFERENCES ape_dts_platform.platform_instances(id),
  detected_at timestamptz NOT NULL,
  recovered_at timestamptz NOT NULL,
  detection_source text NOT NULL CHECK (btrim(detection_source) <> ''),
  audit_sequence bigint UNIQUE REFERENCES ape_dts_platform.audit_events(sequence),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT metadata_outage_event_time_order CHECK (recovered_at >= detected_at)
);

ALTER TABLE ape_dts_platform.task_runs
  ADD COLUMN process_binding_hash text,
  ADD CONSTRAINT task_runs_process_binding_hash_check
    CHECK (process_binding_hash IS NULL OR process_binding_hash ~ '^[0-9a-f]{64}$');

CREATE TABLE ape_dts_platform.emergency_runtime_events (
  event_id uuid PRIMARY KEY,
  platform_instance_id uuid NOT NULL REFERENCES ape_dts_platform.platform_instances(id),
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  run_id uuid NOT NULL,
  revision_id uuid NOT NULL,
  operation_id uuid NOT NULL,
  action text NOT NULL CHECK (action IN (
    'EMERGENCY_STOP_REQUESTED',
    'EMERGENCY_STOP_CONFIRMED',
    'EMERGENCY_FORCE_KILL_REQUESTED',
    'EMERGENCY_FORCE_KILL_CONFIRMED'
  )),
  actor_type text NOT NULL CHECK (actor_type IN ('DEPLOYMENT_ADMIN', 'SYSTEM')),
  reason text,
  unit_count integer NOT NULL CHECK (unit_count > 0),
  process_binding_hash text NOT NULL CHECK (process_binding_hash ~ '^[0-9a-f]{64}$'),
  occurred_at timestamptz NOT NULL,
  reconciliation_code text,
  audit_sequence bigint UNIQUE REFERENCES ape_dts_platform.audit_events(sequence),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT emergency_runtime_event_actor CHECK (
    (action LIKE '%_REQUESTED' AND actor_type = 'DEPLOYMENT_ADMIN')
    OR (action LIKE '%_CONFIRMED' AND actor_type = 'SYSTEM')
  ),
  CONSTRAINT emergency_runtime_event_reason CHECK (
    (action LIKE 'EMERGENCY_FORCE_KILL_%' AND reason IS NOT NULL AND btrim(reason) <> '')
    OR (action LIKE 'EMERGENCY_STOP_%' AND reason IS NULL)
  ),
  CONSTRAINT emergency_runtime_event_run_fk
    FOREIGN KEY (run_id, task_id)
    REFERENCES ape_dts_platform.task_runs(id, task_id),
  CONSTRAINT emergency_runtime_event_revision_fk
    FOREIGN KEY (run_id, task_id, revision_id)
    REFERENCES ape_dts_platform.task_runs(id, task_id, task_revision_id),
  CONSTRAINT emergency_runtime_event_operation_fk
    FOREIGN KEY (operation_id, task_id)
    REFERENCES ape_dts_platform.task_operations(id, task_id)
);

CREATE INDEX emergency_runtime_events_task_time_idx
  ON ape_dts_platform.emergency_runtime_events(task_id, occurred_at DESC);

CREATE TABLE ape_dts_platform.audit_gap_events (
  id uuid PRIMARY KEY,
  platform_instance_id uuid REFERENCES ape_dts_platform.platform_instances(id),
  gap_kind text NOT NULL CHECK (gap_kind IN (
    'EMERGENCY_QUEUE_OVERFLOW',
    'UNCLEAN_PLATFORM_INSTANCE'
  )),
  known_started_at timestamptz,
  known_ended_at timestamptz,
  omitted_event_count integer CHECK (omitted_event_count IS NULL OR omitted_event_count > 0),
  external_evidence text NOT NULL CHECK (btrim(external_evidence) <> ''),
  detected_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  audit_sequence bigint UNIQUE REFERENCES ape_dts_platform.audit_events(sequence),
  CONSTRAINT audit_gap_event_time_order CHECK (
    known_started_at IS NULL OR known_ended_at IS NULL OR known_ended_at >= known_started_at
  ),
  CONSTRAINT audit_gap_event_payload CHECK (
    (gap_kind = 'EMERGENCY_QUEUE_OVERFLOW' AND omitted_event_count IS NOT NULL)
    OR (gap_kind = 'UNCLEAN_PLATFORM_INSTANCE' AND omitted_event_count IS NULL)
  )
);

CREATE UNIQUE INDEX audit_gap_events_unclean_instance_idx
  ON ape_dts_platform.audit_gap_events(platform_instance_id, gap_kind)
  WHERE gap_kind = 'UNCLEAN_PLATFORM_INSTANCE';
