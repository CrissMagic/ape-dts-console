CREATE TABLE ape_dts_platform.mysql_task_checkpoints (
  task_id uuid PRIMARY KEY REFERENCES ape_dts_platform.tasks(id),
  task_revision_id uuid NOT NULL,
  active_run_id uuid NOT NULL,
  configuration_sha256 text NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
  source_identity text NOT NULL CHECK (btrim(source_identity) <> ''),
  source_row_version bigint NOT NULL CHECK (source_row_version > 0),
  client_server_id bigint NOT NULL CHECK (client_server_id BETWEEN 1 AND 4294967295),
  phase text NOT NULL CHECK (phase IN ('SNAPSHOT','CDC')),
  binlog_filename text NOT NULL CHECK (binlog_filename ~ '^[A-Za-z0-9._-]+\.[0-9]+$'),
  binlog_position bigint NOT NULL CHECK (binlog_position BETWEEN 4 AND 4294967295),
  checkpoint_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  FOREIGN KEY (task_revision_id, task_id) REFERENCES ape_dts_platform.task_revisions(id, task_id),
  FOREIGN KEY (active_run_id, task_id, task_revision_id) REFERENCES ape_dts_platform.task_runs(id, task_id, task_revision_id),
  UNIQUE (source_identity, client_server_id)
);
