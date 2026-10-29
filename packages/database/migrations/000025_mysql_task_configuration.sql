-- Extend task support without rewriting existing task configurations or continuity evidence.
ALTER TABLE ape_dts_platform.tasks
  DROP CONSTRAINT tasks_task_type_check,
  ADD CONSTRAINT tasks_task_type_check CHECK (task_type IN ('PG_KAFKA', 'KAFKA_KAFKA', 'MYSQL_KAFKA'));

CREATE TABLE ape_dts_platform.mysql_kafka_revision_configs (
  task_revision_id uuid PRIMARY KEY REFERENCES ape_dts_platform.task_revisions(id),
  source_database text NOT NULL CHECK (btrim(source_database) <> ''),
  include_snapshot boolean NOT NULL,
  mappings jsonb NOT NULL,
  CONSTRAINT mysql_kafka_mapping_shape CHECK (
    CASE WHEN jsonb_typeof(mappings) = 'array' THEN jsonb_array_length(mappings) > 0 ELSE false END
  )
);

ALTER TABLE ape_dts_platform.task_continuity_baselines
  ADD COLUMN mysql_source_identity text,
  DROP CONSTRAINT task_continuity_baselines_baseline_kind_check,
  DROP CONSTRAINT task_continuity_baseline_shape,
  ADD CONSTRAINT task_continuity_baselines_baseline_kind_check
    CHECK (baseline_kind IN ('KAFKA', 'POSTGRES', 'MYSQL')),
  ADD CONSTRAINT task_continuity_baseline_shape CHECK (
    (baseline_kind = 'KAFKA'
      AND mysql_source_identity IS NULL
      AND source_cluster_id IS NOT NULL
      AND source_partition_ids IS NOT NULL AND cardinality(source_partition_ids) > 0
      AND source_partition_offsets IS NOT NULL
      AND jsonb_typeof(source_partition_offsets) = 'object'
      AND pg_system_identifier IS NULL AND pg_database_name IS NULL AND pg_database_oid IS NULL
      AND pg_slot_name IS NULL AND pg_slot_plugin IS NULL AND pg_confirmed_flush_lsn IS NULL)
    OR
    (baseline_kind = 'POSTGRES'
      AND mysql_source_identity IS NULL
      AND source_cluster_id IS NULL AND source_topic_id IS NULL AND source_partition_ids IS NULL
      AND source_partition_offsets IS NULL
      AND pg_system_identifier IS NOT NULL AND pg_database_name IS NOT NULL
      AND pg_database_oid IS NOT NULL AND pg_slot_name IS NOT NULL
      AND pg_slot_plugin IS NOT NULL AND pg_confirmed_flush_lsn IS NOT NULL)
    OR
    (baseline_kind = 'MYSQL'
      AND mysql_source_identity IS NOT NULL AND btrim(mysql_source_identity) <> ''
      AND source_cluster_id IS NULL AND source_topic_id IS NULL AND source_partition_ids IS NULL
      AND source_partition_offsets IS NULL
      AND pg_system_identifier IS NULL AND pg_database_name IS NULL AND pg_database_oid IS NULL
      AND pg_slot_name IS NULL AND pg_slot_plugin IS NULL AND pg_confirmed_flush_lsn IS NULL)
  );
