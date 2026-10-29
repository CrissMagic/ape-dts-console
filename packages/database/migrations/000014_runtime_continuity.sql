CREATE TABLE ape_dts_platform.task_continuity_baselines (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  task_revision_id uuid NOT NULL REFERENCES ape_dts_platform.task_revisions(id),
  baseline_kind text NOT NULL CHECK (baseline_kind IN ('KAFKA', 'POSTGRES')),
  source_cluster_id text,
  source_topic_id text,
  source_partition_ids integer[],
  source_partition_offsets jsonb,
  target_cluster_id text NOT NULL,
  target_topic_id text NOT NULL,
  target_partition_count integer NOT NULL CHECK (target_partition_count > 0),
  pg_system_identifier text,
  pg_database_name text,
  pg_database_oid oid,
  pg_slot_name text,
  pg_slot_plugin text,
  pg_confirmed_flush_lsn pg_lsn,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (task_id),
  UNIQUE (id, task_id),
  CONSTRAINT task_continuity_baseline_shape CHECK (
    (baseline_kind = 'KAFKA'
      AND source_cluster_id IS NOT NULL AND source_topic_id IS NOT NULL
      AND source_partition_ids IS NOT NULL AND cardinality(source_partition_ids) > 0
      AND source_partition_offsets IS NOT NULL
      AND jsonb_typeof(source_partition_offsets) = 'object'
      AND pg_system_identifier IS NULL AND pg_database_name IS NULL AND pg_database_oid IS NULL
      AND pg_slot_name IS NULL AND pg_slot_plugin IS NULL AND pg_confirmed_flush_lsn IS NULL)
    OR
    (baseline_kind = 'POSTGRES'
      AND source_cluster_id IS NULL AND source_topic_id IS NULL AND source_partition_ids IS NULL
      AND source_partition_offsets IS NULL
      AND pg_system_identifier IS NOT NULL AND pg_database_name IS NOT NULL
      AND pg_database_oid IS NOT NULL AND pg_slot_name IS NOT NULL
      AND pg_slot_plugin IS NOT NULL AND pg_confirmed_flush_lsn IS NOT NULL)
  )
);

CREATE TABLE ape_dts_platform.task_continuity_observations (
  id uuid PRIMARY KEY,
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  task_revision_id uuid NOT NULL REFERENCES ape_dts_platform.task_revisions(id),
  baseline_id uuid REFERENCES ape_dts_platform.task_continuity_baselines(id),
  observation jsonb NOT NULL CHECK (jsonb_typeof(observation) = 'object'),
  decision text NOT NULL CHECK (decision IN ('MATCHED', 'MANUAL_REQUIRED', 'PERMANENTLY_BLOCKED')),
  decision_code text NOT NULL CHECK (btrim(decision_code) <> ''),
  actor_type text NOT NULL CHECK (actor_type IN ('USER', 'SYSTEM')),
  actor_user_id uuid REFERENCES ape_dts_platform.users(id),
  observed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT task_continuity_observation_actor CHECK (
    (actor_type = 'USER' AND actor_user_id IS NOT NULL)
    OR (actor_type = 'SYSTEM' AND actor_user_id IS NULL)
  ),
  CONSTRAINT task_continuity_observation_revision_task_fk
    FOREIGN KEY (task_revision_id, task_id)
    REFERENCES ape_dts_platform.task_revisions (id, task_id),
  CONSTRAINT task_continuity_observation_baseline_fk
    FOREIGN KEY (baseline_id, task_id)
    REFERENCES ape_dts_platform.task_continuity_baselines (id, task_id)
);

CREATE TABLE ape_dts_platform.task_continuity_acceptances (
  id uuid PRIMARY KEY,
  observation_id uuid NOT NULL UNIQUE REFERENCES ape_dts_platform.task_continuity_observations(id),
  expected_baseline_row_version bigint NOT NULL CHECK (expected_baseline_row_version > 0),
  accepted_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  reason text NOT NULL CHECK (btrim(reason) <> ''),
  accepted_observation jsonb NOT NULL CHECK (jsonb_typeof(accepted_observation) = 'object'),
  accepted_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE INDEX task_continuity_observations_revision_observed_idx
  ON ape_dts_platform.task_continuity_observations (task_revision_id, observed_at DESC);

CREATE FUNCTION ape_dts_platform.reject_continuity_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  RAISE EXCEPTION 'continuity evidence is append-only' USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER task_continuity_observations_append_only
BEFORE UPDATE OR DELETE ON ape_dts_platform.task_continuity_observations
FOR EACH ROW EXECUTE FUNCTION ape_dts_platform.reject_continuity_evidence_mutation();

CREATE TRIGGER task_continuity_acceptances_append_only
BEFORE UPDATE OR DELETE ON ape_dts_platform.task_continuity_acceptances
FOR EACH ROW EXECUTE FUNCTION ape_dts_platform.reject_continuity_evidence_mutation();
