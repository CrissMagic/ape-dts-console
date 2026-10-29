-- Older Kafka brokers have no stable Topic UUID. Unknown facts remain NULL.
-- No existing rows are rewritten; all cluster, source partition and PG checks remain intact.
ALTER TABLE ape_dts_platform.task_continuity_baselines
  ALTER COLUMN target_topic_id DROP NOT NULL,
  ALTER COLUMN target_partition_count DROP NOT NULL,
  DROP CONSTRAINT task_continuity_baseline_shape;

ALTER TABLE ape_dts_platform.task_continuity_baselines
  ADD CONSTRAINT task_continuity_baseline_shape CHECK (
    (baseline_kind = 'KAFKA'
      AND source_cluster_id IS NOT NULL
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
  );
