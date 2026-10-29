ALTER TABLE ape_dts_platform.log_gaps
  DROP CONSTRAINT log_gaps_reason_check,
  ADD CONSTRAINT log_gaps_reason_check CHECK (reason IN (
    'REDACTION_FAILURE', 'RAW_QUEUE_OVERFLOW', 'STORAGE_BUFFER_OVERFLOW',
    'CONTAINER_RESTART', 'STREAM_ERROR', 'LEGACY_FORMAT'
  ));
