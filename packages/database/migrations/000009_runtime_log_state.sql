ALTER TABLE ape_dts_platform.task_runs
  ADD COLUMN log_storage_state text,
  ADD COLUMN log_recovery_state text;

-- The previous runtime could swallow storage failures. Existing runs therefore
-- cannot be called healthy merely because no durable gap was recorded.
UPDATE ape_dts_platform.task_runs AS runs
   SET log_storage_state = 'UNKNOWN',
       log_recovery_state = CASE
         WHEN EXISTS (
           SELECT 1
             FROM ape_dts_platform.log_segments AS segments
             JOIN ape_dts_platform.stored_objects AS objects
               ON objects.id = segments.stored_object_id
            WHERE segments.run_id = runs.id
              AND segments.state = 'SEALED'
              AND objects.state = 'PENDING'
         ) OR EXISTS (
           SELECT 1
             FROM ape_dts_platform.log_segment_deletion_requests AS requests
            WHERE requests.run_id = runs.id
              AND requests.completed_at IS NULL
         ) THEN 'DURABLE_PENDING'
         ELSE 'NONE'
       END;

ALTER TABLE ape_dts_platform.task_runs
  ALTER COLUMN log_storage_state SET DEFAULT 'HEALTHY',
  ALTER COLUMN log_storage_state SET NOT NULL,
  ALTER COLUMN log_recovery_state SET DEFAULT 'NONE',
  ALTER COLUMN log_recovery_state SET NOT NULL,
  ADD CONSTRAINT task_runs_log_storage_state_check CHECK (
    log_storage_state IN ('HEALTHY', 'DEGRADED', 'UNKNOWN')
  ),
  ADD CONSTRAINT task_runs_log_recovery_state_check CHECK (
    log_recovery_state IN ('NONE', 'MEMORY_PENDING', 'DURABLE_PENDING', 'MIXED_PENDING')
  );
