CREATE TABLE ape_dts_platform.alert_settings (
  singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
  enabled boolean NOT NULL DEFAULT false,
  configuration jsonb NOT NULL DEFAULT '{"channel":"WECOM","notifyRecovery":true,"durationSeconds":30,"repeatSeconds":300,"slotInactive":true,"slotMissing":true,"retainedWalBytes":null,"unconfirmedWalBytes":null,"kafkaOffsetLag":null,"delayMilliseconds":null}'::jsonb,
  encrypted_webhook jsonb,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
INSERT INTO ape_dts_platform.alert_settings (singleton_id) VALUES (1);

CREATE TABLE ape_dts_platform.alert_rule_states (
  task_id uuid NOT NULL REFERENCES ape_dts_platform.tasks(id),
  rule text NOT NULL,
  first_bad_at timestamptz,
  last_attempt_at timestamptz,
  alarm_sent boolean NOT NULL DEFAULT false,
  PRIMARY KEY (task_id, rule)
);

CREATE TABLE ape_dts_platform.alert_deliveries (
  id uuid PRIMARY KEY,
  task_id uuid REFERENCES ape_dts_platform.tasks(id),
  rule text NOT NULL,
  event text NOT NULL CHECK (event IN ('ALARM','RECOVERY','TEST')),
  status text NOT NULL CHECK (status IN ('PENDING','SENT','FAILED')),
  failure_code text,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  completed_at timestamptz
);
CREATE INDEX alert_deliveries_created_idx ON ape_dts_platform.alert_deliveries (created_at DESC);
CREATE INDEX audit_events_alert_recovery_idx ON ape_dts_platform.audit_events (subject_id, occurred_at DESC)
  WHERE action = 'TASK_RUNTIME_AUTO_RECOVERY_MANUAL_REQUIRED';
