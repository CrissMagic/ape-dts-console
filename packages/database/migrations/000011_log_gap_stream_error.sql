ALTER TABLE ape_dts_platform.log_gaps
  DROP CONSTRAINT log_gaps_reason_check,
  ADD CONSTRAINT log_gaps_reason_check CHECK (
    reason IN (
      'REDACTION_FAILURE',
      'RAW_QUEUE_OVERFLOW',
      'STORAGE_BUFFER_OVERFLOW',
      'CONTAINER_RESTART',
      'STREAM_ERROR'
    )
    AND (
      reason <> 'STREAM_ERROR'
      OR (
        run_unit_id IS NOT NULL
        AND started_at IS NULL
        AND ended_at IS NULL
        AND lost_bytes IS NULL
        AND container_restart = false
      )
    )
  );
