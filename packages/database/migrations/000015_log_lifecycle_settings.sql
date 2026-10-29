ALTER TABLE ape_dts_platform.task_runs
  ADD COLUMN record_purged_at timestamptz;

CREATE INDEX task_runs_retention_candidates_idx
  ON ape_dts_platform.task_runs (ended_at, id)
  WHERE ended_at IS NOT NULL AND record_purged_at IS NULL;
