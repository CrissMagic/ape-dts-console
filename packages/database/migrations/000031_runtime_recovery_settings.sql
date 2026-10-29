CREATE TABLE ape_dts_platform.runtime_recovery_settings (
  singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
  enabled boolean NOT NULL DEFAULT true,
  max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
  retry_delay_seconds integer NOT NULL DEFAULT 30 CHECK (retry_delay_seconds BETWEEN 5 AND 3600),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
INSERT INTO ape_dts_platform.runtime_recovery_settings(singleton_id) VALUES(1);
