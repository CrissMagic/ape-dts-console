CREATE TABLE ape_dts_platform.tasks (
  id uuid PRIMARY KEY,
  normalized_name text NOT NULL,
  display_name text NOT NULL,
  task_type text NOT NULL CHECK (task_type IN ('PG_KAFKA', 'KAFKA_KAFKA')),
  source_data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  target_data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  desired_state text NOT NULL CHECK (desired_state IN ('STOPPED', 'RUNNING', 'DELETED')),
  observed_state text NOT NULL CHECK (observed_state IN (
    'STOPPED', 'STARTING', 'RUNNING', 'STOPPING', 'FAILED', 'BLOCKED', 'DELETED'
  )),
  observability_health text NOT NULL CHECK (
    observability_health IN ('HEALTHY', 'DEGRADED', 'UNAVAILABLE')
  ),
  recovery_disposition text NOT NULL CHECK (recovery_disposition IN (
    'AUTO_ELIGIBLE', 'MANUAL_REQUIRED', 'CAPACITY_BLOCKED',
    'PERMANENTLY_BLOCKED', 'NOT_APPLICABLE'
  )),
  current_draft_revision_id uuid,
  current_applied_revision_id uuid,
  last_auto_recovery_at timestamptz,
  last_healthy_at timestamptz,
  manual_gate_reason text,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  deleted_at timestamptz,
  CONSTRAINT tasks_name_normalized CHECK (
    normalized_name = lower(btrim(normalized_name))
    AND normalized_name <> ''
  ),
  CONSTRAINT tasks_display_name_nonempty CHECK (btrim(display_name) <> ''),
  CONSTRAINT tasks_deleted_state_consistency CHECK (
    (deleted_at IS NULL AND desired_state <> 'DELETED' AND observed_state <> 'DELETED')
    OR (deleted_at IS NOT NULL AND desired_state = 'DELETED')
  )
);

CREATE UNIQUE INDEX tasks_one_active_name
  ON ape_dts_platform.tasks (normalized_name)
  WHERE deleted_at IS NULL;

CREATE INDEX tasks_source_target_active_idx
  ON ape_dts_platform.tasks (source_data_source_id, target_data_source_id)
  WHERE deleted_at IS NULL;

CREATE TABLE ape_dts_platform.task_revisions (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  revision_number bigint NOT NULL CHECK (revision_number > 0),
  state text NOT NULL CHECK (state IN (
    'INCOMPLETE_DRAFT', 'READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED',
    'APPLIED', 'SUPERSEDED', 'ABANDONED', 'STALE'
  )),
  configuration_schema_version integer CHECK (
    configuration_schema_version IS NULL OR configuration_schema_version > 0
  ),
  configuration jsonb NOT NULL CHECK (jsonb_typeof(configuration) = 'object'),
  artifact_id uuid REFERENCES ape_dts_platform.artifacts(id),
  preview_ini_sha256 bytea CHECK (
    preview_ini_sha256 IS NULL OR octet_length(preview_ini_sha256) = 32
  ),
  semantic_sha256 bytea CHECK (semantic_sha256 IS NULL OR octet_length(semantic_sha256) = 32),
  draft_version bigint NOT NULL DEFAULT 1 CHECK (draft_version > 0),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (task_id, revision_number)
);

ALTER TABLE ape_dts_platform.tasks
  ADD CONSTRAINT tasks_current_draft_revision_fk
  FOREIGN KEY (current_draft_revision_id)
  REFERENCES ape_dts_platform.task_revisions(id)
  DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT tasks_current_applied_revision_fk
  FOREIGN KEY (current_applied_revision_id)
  REFERENCES ape_dts_platform.task_revisions(id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX task_revisions_one_active_draft
  ON ape_dts_platform.task_revisions (task_id)
  WHERE state IN ('INCOMPLETE_DRAFT', 'READY_DRAFT', 'PREFLIGHT_PASSED', 'RISK_CONFIRMED');

CREATE UNIQUE INDEX task_revisions_one_applied
  ON ape_dts_platform.task_revisions (task_id)
  WHERE state = 'APPLIED';

CREATE FUNCTION ape_dts_platform.enforce_task_revision_immutability()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.state = 'APPLIED' THEN
    IF NEW.state = 'SUPERSEDED'
       AND OLD.task_id IS NOT DISTINCT FROM NEW.task_id
       AND OLD.revision_number IS NOT DISTINCT FROM NEW.revision_number
       AND OLD.configuration_schema_version IS NOT DISTINCT FROM NEW.configuration_schema_version
       AND OLD.configuration IS NOT DISTINCT FROM NEW.configuration
       AND OLD.artifact_id IS NOT DISTINCT FROM NEW.artifact_id
       AND OLD.preview_ini_sha256 IS NOT DISTINCT FROM NEW.preview_ini_sha256
       AND OLD.semantic_sha256 IS NOT DISTINCT FROM NEW.semantic_sha256
       AND OLD.draft_version IS NOT DISTINCT FROM NEW.draft_version
       AND OLD.created_by_user_id IS NOT DISTINCT FROM NEW.created_by_user_id
       AND OLD.created_at IS NOT DISTINCT FROM NEW.created_at
    THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'applied task revisions are immutable'
      USING ERRCODE = '23514';
  END IF;

  IF OLD.state IN ('SUPERSEDED', 'ABANDONED', 'STALE') THEN
    RAISE EXCEPTION 'terminal task revisions are immutable'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER task_revisions_immutable
BEFORE UPDATE ON ape_dts_platform.task_revisions
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_task_revision_immutability();

CREATE TABLE ape_dts_platform.pg_kafka_revision_configs (
  task_revision_id uuid PRIMARY KEY REFERENCES ape_dts_platform.task_revisions(id),
  source_database text NOT NULL,
  source_schema text NOT NULL,
  source_table text NOT NULL,
  primary_key_column text,
  target_topic text NOT NULL,
  resource_mode text NOT NULL CHECK (
    resource_mode IN ('SELF_MANAGED', 'PLATFORM_MANAGED', 'APE_DTS_MANAGED')
  ),
  slot_name text NOT NULL,
  publication_name text NOT NULL,
  include_snapshot boolean NOT NULL,
  CONSTRAINT pg_kafka_source_database_safe CHECK (
    source_database <> '' AND source_database !~ '[\x00-\x1f\x7f]' 
  ),
  CONSTRAINT pg_kafka_source_schema_safe CHECK (source_schema ~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$'),
  CONSTRAINT pg_kafka_source_table_safe CHECK (source_table ~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$'),
  CONSTRAINT pg_kafka_primary_key_safe CHECK (
    primary_key_column IS NULL OR primary_key_column ~ '^[A-Za-z_][A-Za-z0-9_$-]{0,62}$'
  ),
  CONSTRAINT pg_kafka_topic_safe CHECK (target_topic ~ '^[A-Za-z0-9._-]{1,249}$'),
  CONSTRAINT pg_kafka_slot_safe CHECK (slot_name ~ '^[a-z0-9_]{1,63}$'),
  CONSTRAINT pg_kafka_publication_safe CHECK (
    publication_name ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'
  ),
  CONSTRAINT pg_kafka_resource_snapshot_consistency CHECK (
    resource_mode <> 'APE_DTS_MANAGED' OR include_snapshot = false
  )
);

CREATE TABLE ape_dts_platform.kafka_kafka_revision_configs (
  task_revision_id uuid PRIMARY KEY REFERENCES ape_dts_platform.task_revisions(id),
  source_topic text NOT NULL,
  target_topic text NOT NULL,
  group_id text NOT NULL,
  required_acks text NOT NULL CHECK (required_acks IN ('one', 'all')),
  CONSTRAINT kafka_kafka_source_topic_safe CHECK (source_topic ~ '^[A-Za-z0-9._-]{1,249}$'),
  CONSTRAINT kafka_kafka_target_topic_safe CHECK (target_topic ~ '^[A-Za-z0-9._-]{1,249}$'),
  CONSTRAINT kafka_kafka_group_safe CHECK (group_id ~ '^[A-Za-z0-9._-]{1,255}$')
);

CREATE TABLE ape_dts_platform.risk_acceptances (
  id uuid PRIMARY KEY,
  task_revision_id uuid NOT NULL UNIQUE REFERENCES ape_dts_platform.task_revisions(id),
  accepted_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  accepted_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  note text
);

CREATE TABLE ape_dts_platform.preflight_runs (
  id uuid PRIMARY KEY,
  task_revision_id uuid NOT NULL REFERENCES ape_dts_platform.task_revisions(id),
  state text NOT NULL CHECK (state IN ('RUNNING', 'PASSED', 'FAILED', 'STALE')),
  started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  completed_at timestamptz,
  result jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(result) = 'object'),
  failure_code text,
  failure_summary text,
  UNIQUE (task_revision_id, id),
  CONSTRAINT preflight_completion_consistency CHECK (
    (state = 'RUNNING' AND completed_at IS NULL)
    OR (state <> 'RUNNING' AND completed_at IS NOT NULL)
  ),
  CONSTRAINT preflight_failure_consistency CHECK (
    (state IN ('FAILED', 'STALE') AND failure_code IS NOT NULL AND failure_summary IS NOT NULL)
    OR (state IN ('RUNNING', 'PASSED') AND failure_code IS NULL AND failure_summary IS NULL)
  )
);

CREATE INDEX preflight_runs_revision_started_idx
  ON ape_dts_platform.preflight_runs (task_revision_id, started_at DESC);

CREATE TABLE ape_dts_platform.external_resources (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  resource_type text NOT NULL CHECK (resource_type IN (
    'PG_SLOT', 'PG_PUBLICATION', 'KAFKA_GROUP', 'KAFKA_SOURCE_TOPIC', 'KAFKA_TARGET_TOPIC'
  )),
  normalized_name text NOT NULL,
  database_name text,
  ownership_mode text NOT NULL CHECK (
    ownership_mode IN ('SELF_MANAGED', 'PLATFORM_MANAGED', 'APE_DTS_MANAGED')
  ),
  stable_identity text NOT NULL,
  lifecycle_state text NOT NULL CHECK (lifecycle_state IN (
    'BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
    'CLEANUP_REQUESTED', 'CLEANING', 'CLEANED', 'CLEANUP_FAILED'
  )),
  platform_created boolean NOT NULL DEFAULT false,
  last_verified_at timestamptz,
  cleanup_error_code text,
  cleanup_error_summary text,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT external_resources_name_nonempty CHECK (btrim(normalized_name) <> ''),
  CONSTRAINT external_resources_identity_nonempty CHECK (btrim(stable_identity) <> ''),
  CONSTRAINT external_resources_cleanup_consistency CHECK (
    (lifecycle_state = 'CLEANUP_FAILED' AND cleanup_error_code IS NOT NULL)
    OR lifecycle_state <> 'CLEANUP_FAILED'
  )
);

CREATE UNIQUE INDEX external_resources_one_active_identity
  ON ape_dts_platform.external_resources (
    data_source_id, resource_type, database_name, normalized_name, stable_identity
  )
  WHERE lifecycle_state IN (
    'BOUND', 'TRACKED_AFTER_STOP', 'TRACKED_AFTER_DELETE',
    'CLEANUP_REQUESTED', 'CLEANING', 'CLEANUP_FAILED'
  );

CREATE INDEX external_resources_task_lifecycle_idx
  ON ape_dts_platform.external_resources (task_id, lifecycle_state);

CREATE TABLE ape_dts_platform.task_run_units (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  unit_key text NOT NULL,
  status text NOT NULL CHECK (status IN (
    'STARTING', 'RUNNING', 'STOPPING', 'STOPPED', 'FAILED', 'BLOCKED'
  )),
  UNIQUE (task_id, unit_key)
);

CREATE INDEX task_run_units_active_task_idx
  ON ape_dts_platform.task_run_units (task_id, status)
  WHERE status IN ('STARTING', 'RUNNING', 'STOPPING');

CREATE TABLE ape_dts_platform.task_operations (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  action text NOT NULL CHECK (action IN (
    'START', 'STOP', 'RESTART', 'APPLY_RESTART', 'DELETE', 'RESTORE',
    'EMERGENCY_STOP', 'EMERGENCY_KILL'
  )),
  idempotency_key text NOT NULL,
  target_revision_id uuid REFERENCES ape_dts_platform.task_revisions(id),
  state text NOT NULL CHECK (state IN (
    'ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING',
    'SUCCEEDED', 'FAILED_RETRYABLE', 'BLOCKED_MANUAL', 'BLOCKED_PERMANENT', 'REJECTED'
  )),
  initiated_by_user_id uuid REFERENCES ape_dts_platform.users(id),
  reason text,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (task_id, idempotency_key)
);

CREATE UNIQUE INDEX task_operations_one_active_per_task
  ON ape_dts_platform.task_operations (task_id)
  WHERE state IN ('ACCEPTED', 'PREFLIGHTING', 'PREPARED', 'EXECUTING');

CREATE TABLE ape_dts_platform.resource_cleanup_requests (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  external_resource_id uuid NOT NULL REFERENCES ape_dts_platform.external_resources(id),
  requested_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  expected_resource_row_version bigint NOT NULL CHECK (expected_resource_row_version > 0),
  state text NOT NULL CHECK (state IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_error_code text,
  last_error_summary text,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (task_id, external_resource_id, state)
);

CREATE TABLE ape_dts_platform.capacity_reservations (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  task_revision_id uuid NOT NULL REFERENCES ape_dts_platform.task_revisions(id),
  unit_key text NOT NULL,
  state text NOT NULL CHECK (state IN ('RESERVED', 'ACTIVE', 'RELEASED')),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  released_at timestamptz,
  CONSTRAINT capacity_reservations_release_consistency CHECK (
    (state = 'RELEASED' AND released_at IS NOT NULL)
    OR (state <> 'RELEASED' AND released_at IS NULL)
  ),
  UNIQUE (task_id, task_revision_id, unit_key)
);

CREATE UNIQUE INDEX capacity_reservations_one_active_unit
  ON ape_dts_platform.capacity_reservations (task_id, unit_key)
  WHERE state IN ('RESERVED', 'ACTIVE');

CREATE FUNCTION ape_dts_platform.prevent_task_reactivation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.deleted_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'deleted tasks cannot be changed or reactivated'
      USING ERRCODE = '23514';
  END IF;
  IF OLD.deleted_at IS NULL AND NEW.row_version < OLD.row_version THEN
    RAISE EXCEPTION 'task row version cannot decrease'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_lifecycle
BEFORE UPDATE ON ape_dts_platform.tasks
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_task_reactivation();

CREATE FUNCTION ape_dts_platform.prevent_task_physical_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  RAISE EXCEPTION 'tasks cannot be physically deleted'
    USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER tasks_no_physical_delete
BEFORE DELETE ON ape_dts_platform.tasks
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_task_physical_delete();
