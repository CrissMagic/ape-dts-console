CREATE TABLE ape_dts_platform.task_runs (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  task_revision_id uuid NOT NULL REFERENCES ape_dts_platform.task_revisions(id),
  operation_id uuid REFERENCES ape_dts_platform.task_operations(id),
  artifact_id uuid REFERENCES ape_dts_platform.artifacts(id),
  platform_instance_id uuid REFERENCES ape_dts_platform.platform_instances(id),
  status text NOT NULL CHECK (status IN (
    'STARTING', 'RUNNING', 'STOPPING', 'STOPPED', 'FAILED', 'BLOCKED'
  )),
  current_stage text NOT NULL CHECK (btrim(current_stage) <> ''),
  generated_ini_sha256 bytea CHECK (
    generated_ini_sha256 IS NULL OR octet_length(generated_ini_sha256) = 32
  ),
  started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  ended_at timestamptz,
  truncated boolean NOT NULL DEFAULT false,
  has_gap boolean NOT NULL DEFAULT false,
  failure_code text,
  failure_summary text,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT task_runs_completion_consistency CHECK (
    (status IN ('STARTING', 'RUNNING', 'STOPPING') AND ended_at IS NULL)
    OR (status IN ('STOPPED', 'FAILED', 'BLOCKED') AND ended_at IS NOT NULL)
  ),
  CONSTRAINT task_runs_failure_consistency CHECK (
    (status IN ('FAILED', 'BLOCKED') AND failure_code IS NOT NULL AND failure_summary IS NOT NULL)
    OR (status IN ('STARTING', 'RUNNING', 'STOPPING', 'STOPPED')
        AND failure_code IS NULL AND failure_summary IS NULL)
  )
);

CREATE INDEX task_runs_task_started_idx
  ON ape_dts_platform.task_runs (task_id, started_at DESC);

CREATE INDEX task_runs_active_idx
  ON ape_dts_platform.task_runs (task_id, status)
  WHERE status IN ('STARTING', 'RUNNING', 'STOPPING');

ALTER TABLE ape_dts_platform.task_operations
  ADD COLUMN run_id uuid REFERENCES ape_dts_platform.task_runs(id),
  ADD COLUMN execution_stage text,
  ADD COLUMN started_at timestamptz,
  ADD COLUMN completed_at timestamptz,
  ADD COLUMN failure_code text,
  ADD COLUMN failure_summary text;

ALTER TABLE ape_dts_platform.task_run_units
  DROP CONSTRAINT IF EXISTS task_run_units_task_id_unit_key_key,
  ADD COLUMN run_id uuid REFERENCES ape_dts_platform.task_runs(id),
  ADD COLUMN runtime_directory text,
  ADD COLUMN ini_path text,
  ADD COLUMN log4rs_path text,
  ADD COLUMN metrics_port integer CHECK (
    metrics_port IS NULL OR (metrics_port BETWEEN 1 AND 65535)
  ),
  ADD COLUMN metrics_port_released_at timestamptz,
  ADD COLUMN process_pid integer CHECK (process_pid IS NULL OR process_pid > 0),
  ADD COLUMN process_group_id text,
  ADD COLUMN process_token text,
  ADD COLUMN started_at timestamptz,
  ADD COLUMN ended_at timestamptz,
  ADD COLUMN exit_code integer,
  ADD COLUMN exit_signal text,
  ADD COLUMN failure_code text,
  ADD COLUMN failure_summary text;

CREATE UNIQUE INDEX task_run_units_run_unit_key
  ON ape_dts_platform.task_run_units (run_id, unit_key)
  WHERE run_id IS NOT NULL;

CREATE UNIQUE INDEX task_run_units_one_active_metrics_port
  ON ape_dts_platform.task_run_units (metrics_port)
  WHERE metrics_port IS NOT NULL AND metrics_port_released_at IS NULL;

CREATE INDEX task_run_units_run_status_idx
  ON ape_dts_platform.task_run_units (run_id, status);

CREATE TABLE ape_dts_platform.task_run_stages (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ape_dts_platform.task_runs(id),
  stage_order integer NOT NULL CHECK (stage_order > 0),
  stage_type text NOT NULL CHECK (btrim(stage_type) <> ''),
  status text NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PAUSED')),
  progress_completed bigint NOT NULL DEFAULT 0 CHECK (progress_completed >= 0),
  progress_total bigint CHECK (progress_total IS NULL OR progress_total >= 0),
  throughput numeric,
  heartbeat_at timestamptz,
  failure_code text,
  failure_summary text,
  started_at timestamptz,
  ended_at timestamptz,
  UNIQUE (run_id, stage_order),
  CONSTRAINT task_run_stages_progress_consistency CHECK (
    progress_total IS NULL OR progress_completed <= progress_total
  ),
  CONSTRAINT task_run_stages_completion_consistency CHECK (
    (status IN ('PENDING', 'RUNNING', 'PAUSED') AND ended_at IS NULL)
    OR (status IN ('SUCCEEDED', 'FAILED') AND ended_at IS NOT NULL)
  )
);

CREATE INDEX task_run_stages_run_order_idx
  ON ape_dts_platform.task_run_stages (run_id, stage_order);

CREATE TABLE ape_dts_platform.log_segments (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ape_dts_platform.task_runs(id),
  run_unit_id uuid REFERENCES ape_dts_platform.task_run_units(id),
  stream text NOT NULL CHECK (stream IN ('STDOUT', 'STDERR')),
  sequence bigint NOT NULL CHECK (sequence > 0),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  stored_object_id uuid REFERENCES ape_dts_platform.stored_objects(id),
  byte_size bigint NOT NULL DEFAULT 0 CHECK (byte_size >= 0),
  sha256 bytea CHECK (sha256 IS NULL OR octet_length(sha256) = 32),
  state text NOT NULL CHECK (
    state IN ('OPEN', 'SEALED', 'AVAILABLE', 'DELETED', 'MISSING', 'INTEGRITY_FAILED')
  ),
  deleted_at timestamptz,
  deleted_reason text CHECK (deleted_reason IS NULL OR deleted_reason IN ('RETENTION', 'CAPACITY_ROLLOVER')),
  unavailable_reason text CHECK (
    unavailable_reason IS NULL OR unavailable_reason IN ('STORAGE_MISSING', 'INTEGRITY_MISMATCH')
  ),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (run_id, stream, sequence),
  CONSTRAINT log_segments_end_consistency CHECK (
    (state = 'OPEN' AND ended_at IS NULL)
    OR (state <> 'OPEN' AND ended_at IS NOT NULL)
  ),
  CONSTRAINT log_segments_delete_consistency CHECK (
    (state = 'DELETED' AND deleted_at IS NOT NULL AND deleted_reason IS NOT NULL)
    OR (state <> 'DELETED' AND deleted_at IS NULL AND deleted_reason IS NULL)
  ),
  CONSTRAINT log_segments_unavailable_consistency CHECK (
    (state IN ('MISSING', 'INTEGRITY_FAILED') AND unavailable_reason IS NOT NULL)
    OR (state NOT IN ('MISSING', 'INTEGRITY_FAILED') AND unavailable_reason IS NULL)
  )
);

CREATE INDEX log_segments_run_sequence_idx
  ON ape_dts_platform.log_segments (run_id, stream, sequence);

CREATE TABLE ape_dts_platform.log_gaps (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ape_dts_platform.task_runs(id),
  run_unit_id uuid REFERENCES ape_dts_platform.task_run_units(id),
  reason text NOT NULL CHECK (reason IN (
    'REDACTION_FAILURE', 'RAW_QUEUE_OVERFLOW', 'STORAGE_BUFFER_OVERFLOW', 'CONTAINER_RESTART'
  )),
  started_at timestamptz,
  ended_at timestamptz,
  lost_bytes bigint CHECK (lost_bytes IS NULL OR lost_bytes >= 0),
  occurred_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  container_restart boolean NOT NULL DEFAULT false
);

CREATE INDEX log_gaps_run_occurred_idx
  ON ape_dts_platform.log_gaps (run_id, occurred_at);

ALTER TABLE ape_dts_platform.capacity_reservations
  DROP CONSTRAINT IF EXISTS capacity_reservations_task_id_task_revision_id_unit_key_key,
  ADD COLUMN run_id uuid REFERENCES ape_dts_platform.task_runs(id);

CREATE UNIQUE INDEX capacity_reservations_run_unit_key
  ON ape_dts_platform.capacity_reservations (run_id, unit_key)
  WHERE run_id IS NOT NULL;
