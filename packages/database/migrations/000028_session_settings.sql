CREATE TABLE ape_dts_platform.auth_session_settings (
  singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
  idle_timeout_minutes integer NOT NULL DEFAULT 480 CHECK (idle_timeout_minutes BETWEEN 15 AND 10080),
  absolute_timeout_hours integer NOT NULL DEFAULT 168 CHECK (absolute_timeout_hours BETWEEN 1 AND 720),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  updated_by_user_id uuid REFERENCES ape_dts_platform.users(id),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT auth_session_settings_lifetime_check CHECK (idle_timeout_minutes <= absolute_timeout_hours * 60)
);
INSERT INTO ape_dts_platform.auth_session_settings (singleton_id) VALUES (1);

-- Retain the old 30-minute sliding window for every existing session. Existing
-- idle/absolute deadlines are deliberately not changed, including expired rows.
ALTER TABLE ape_dts_platform.auth_sessions
  ADD COLUMN idle_timeout_minutes integer NOT NULL DEFAULT 30
    CHECK (idle_timeout_minutes BETWEEN 15 AND 10080);
ALTER TABLE ape_dts_platform.auth_sessions ALTER COLUMN idle_timeout_minutes SET DEFAULT 480;
