-- Existing scalar revisions remain unchanged; mapped revisions never impersonate their first row.
ALTER TABLE ape_dts_platform.pg_kafka_revision_configs
  ADD COLUMN mappings jsonb,
  ALTER COLUMN source_schema DROP NOT NULL,
  ALTER COLUMN source_table DROP NOT NULL,
  ALTER COLUMN target_topic DROP NOT NULL,
  ADD CONSTRAINT pg_kafka_mapping_shape CHECK (
    (mappings IS NULL AND source_schema IS NOT NULL AND source_table IS NOT NULL AND target_topic IS NOT NULL)
    OR
    (mappings IS NOT NULL AND source_schema IS NULL AND source_table IS NULL AND target_topic IS NULL
      AND CASE WHEN jsonb_typeof(mappings) = 'array' THEN jsonb_array_length(mappings) > 0 ELSE false END)
  );

ALTER TABLE ape_dts_platform.kafka_kafka_revision_configs
  ADD COLUMN mappings jsonb,
  ALTER COLUMN source_topic DROP NOT NULL,
  ALTER COLUMN target_topic DROP NOT NULL,
  ADD CONSTRAINT kafka_kafka_mapping_shape CHECK (
    (mappings IS NULL AND source_topic IS NOT NULL AND target_topic IS NOT NULL)
    OR
    (mappings IS NOT NULL AND source_topic IS NULL AND target_topic IS NULL
      AND CASE WHEN jsonb_typeof(mappings) = 'array' THEN jsonb_array_length(mappings) > 0 ELSE false END)
  );

ALTER TABLE ape_dts_platform.task_continuity_baselines
  ADD COLUMN topic_snapshots jsonb,
  ADD CONSTRAINT task_continuity_topic_snapshots_shape CHECK (
    topic_snapshots IS NULL
    OR CASE WHEN jsonb_typeof(topic_snapshots) = 'array' THEN jsonb_array_length(topic_snapshots) > 0 ELSE false END
  );
