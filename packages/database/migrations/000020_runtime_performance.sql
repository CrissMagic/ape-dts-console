CREATE TABLE ape_dts_platform.metric_samples_high (
  task_id uuid NOT NULL,
  run_id uuid NOT NULL,
  unit_id uuid NOT NULL,
  collected_at timestamptz NOT NULL,
  records_per_second double precision,
  bytes_per_second double precision,
  sink_records double precision,
  sink_bytes double precision,
  queue_records double precision,
  queue_bytes double precision,
  gap_reason text,
  PRIMARY KEY (task_id, run_id, unit_id, collected_at)
) PARTITION BY RANGE (collected_at);
CREATE INDEX metric_samples_high_task_time_idx
  ON ape_dts_platform.metric_samples_high (task_id, collected_at DESC);
CREATE INDEX metric_samples_high_unit_time_idx
  ON ape_dts_platform.metric_samples_high (unit_id, collected_at DESC);

CREATE TABLE ape_dts_platform.metric_rollups_1m (
  task_id uuid NOT NULL,
  run_id uuid NOT NULL,
  unit_id uuid NOT NULL,
  bucket_start timestamptz NOT NULL,
  records_per_second double precision,
  bytes_per_second double precision,
  sink_records double precision,
  sink_bytes double precision,
  queue_records double precision,
  queue_bytes double precision,
  gap boolean NOT NULL,
  sample_count integer NOT NULL CHECK (sample_count > 0),
  PRIMARY KEY (task_id, run_id, unit_id, bucket_start)
) PARTITION BY RANGE (bucket_start);
CREATE INDEX metric_rollups_1m_task_time_idx
  ON ape_dts_platform.metric_rollups_1m (task_id, bucket_start DESC);

CREATE TABLE ape_dts_platform.metric_rollups_1h (
  task_id uuid NOT NULL,
  run_id uuid NOT NULL,
  unit_id uuid NOT NULL,
  bucket_start timestamptz NOT NULL,
  records_per_second double precision,
  bytes_per_second double precision,
  sink_records double precision,
  sink_bytes double precision,
  queue_records double precision,
  queue_bytes double precision,
  gap boolean NOT NULL,
  sample_count integer NOT NULL CHECK (sample_count > 0),
  PRIMARY KEY (task_id, run_id, unit_id, bucket_start)
) PARTITION BY RANGE (bucket_start);
CREATE INDEX metric_rollups_1h_task_time_idx
  ON ape_dts_platform.metric_rollups_1h (task_id, bucket_start DESC);

CREATE TABLE ape_dts_platform.metric_maintenance_state (
  id text PRIMARY KEY CHECK (id = 'global'),
  status text NOT NULL CHECK (status IN ('HEALTHY', 'FAILED')),
  last_attempt_at timestamptz NOT NULL
);

CREATE TABLE ape_dts_platform.observability_gaps (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  task_id uuid NOT NULL,
  run_id uuid NOT NULL,
  unit_id uuid NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at timestamptz NOT NULL,
  reason text NOT NULL CHECK (reason ~ '^[A-Z][A-Z0-9_]{0,127}$'),
  CHECK (ended_at >= started_at)
);
CREATE INDEX observability_gaps_task_time_idx
  ON ape_dts_platform.observability_gaps (task_id, started_at DESC);
CREATE INDEX observability_gaps_ended_idx
  ON ape_dts_platform.observability_gaps (ended_at);
