CREATE TABLE ape_dts_platform.task_groups (
  id uuid PRIMARY KEY,
  display_name text NOT NULL CHECK (char_length(btrim(display_name)) BETWEEN 1 AND 128),
  normalized_name text NOT NULL UNIQUE CHECK (normalized_name = lower(btrim(normalized_name)) AND normalized_name <> ''),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE ape_dts_platform.task_group_memberships (
  task_id uuid PRIMARY KEY REFERENCES ape_dts_platform.tasks(id),
  group_id uuid NOT NULL REFERENCES ape_dts_platform.task_groups(id)
);

CREATE INDEX task_group_memberships_group_idx ON ape_dts_platform.task_group_memberships(group_id);
