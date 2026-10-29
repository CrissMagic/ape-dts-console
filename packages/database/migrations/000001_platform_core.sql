CREATE TABLE ape_dts_platform.schema_migrations (
  version bigint PRIMARY KEY,
  checksum text NOT NULL CHECK (checksum ~ '^[a-f0-9]{64}$'),
  platform_version text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  applied_by_instance_id uuid NOT NULL
);

CREATE TABLE ape_dts_platform.platform_instances (
  id uuid PRIMARY KEY,
  platform_version text NOT NULL,
  started_at timestamptz NOT NULL,
  last_heartbeat_at timestamptz NOT NULL,
  ended_at timestamptz,
  ended_reason text,
  clean_shutdown boolean,
  process_id integer,
  current_recovery_session_id uuid,
  CONSTRAINT platform_instances_end_consistency CHECK (
    (ended_at IS NULL AND clean_shutdown IS NULL)
    OR ended_at IS NOT NULL
  )
);

CREATE TABLE ape_dts_platform.recovery_sessions (
  id uuid PRIMARY KEY,
  platform_instance_id uuid NOT NULL REFERENCES ape_dts_platform.platform_instances(id),
  state text NOT NULL CHECK (state IN ('RUNNING', 'READY', 'FAILED', 'PAUSED')),
  started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  ended_at timestamptz,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0)
);

ALTER TABLE ape_dts_platform.platform_instances
  ADD CONSTRAINT platform_instances_current_recovery_session_fk
  FOREIGN KEY (current_recovery_session_id)
  REFERENCES ape_dts_platform.recovery_sessions(id);

CREATE TABLE ape_dts_platform.recovery_steps (
  id uuid PRIMARY KEY,
  recovery_session_id uuid NOT NULL REFERENCES ape_dts_platform.recovery_sessions(id),
  step_type text NOT NULL CHECK (step_type IN (
    'LOAD_METADATA',
    'UNLOCK_KEYS',
    'ENSURE_ARTIFACTS',
    'VERIFY_CONTINUITY',
    'PLAN_CAPACITY',
    'RESTORE_BATCHES',
    'READY_OBSERVABILITY'
  )),
  position smallint NOT NULL CHECK (position > 0),
  status text NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PAUSED')),
  progress jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),
  failure_code text,
  error_summary text,
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_manual_action jsonb,
  started_at timestamptz,
  finished_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  UNIQUE (recovery_session_id, step_type),
  UNIQUE (recovery_session_id, position)
);

CREATE TABLE ape_dts_platform.key_wrappers (
  id uuid PRIMARY KEY,
  wrapper_type text NOT NULL CHECK (wrapper_type IN ('PASSPHRASE', 'RECOVERY')),
  version integer NOT NULL CHECK (version > 0),
  kdf_algorithm text NOT NULL CHECK (kdf_algorithm IN ('ARGON2ID', 'HKDF_SHA256')),
  kdf_parameters jsonb NOT NULL CHECK (jsonb_typeof(kdf_parameters) = 'object'),
  salt bytea NOT NULL CHECK (octet_length(salt) >= 16),
  cipher_algorithm text NOT NULL CHECK (cipher_algorithm = 'AES_256_GCM'),
  nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
  ciphertext bytea NOT NULL CHECK (octet_length(ciphertext) = 32),
  tag bytea NOT NULL CHECK (octet_length(tag) = 16),
  active boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  deactivated_at timestamptz,
  UNIQUE (wrapper_type, version),
  CONSTRAINT key_wrappers_active_consistency CHECK (
    (active AND deactivated_at IS NULL)
    OR (NOT active AND deactivated_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX key_wrappers_one_active_per_type
  ON ape_dts_platform.key_wrappers (wrapper_type)
  WHERE active;

CREATE TABLE ape_dts_platform.secrets (
  id uuid PRIMARY KEY,
  owner_type text NOT NULL,
  owner_id uuid NOT NULL,
  purpose text NOT NULL,
  algorithm text NOT NULL CHECK (algorithm = 'AES_256_GCM'),
  dek_version integer NOT NULL CHECK (dek_version > 0),
  aad_version integer NOT NULL CHECK (aad_version > 0),
  nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
  ciphertext bytea NOT NULL,
  tag bytea NOT NULL CHECK (octet_length(tag) = 16),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  destroyed_at timestamptz
);

CREATE UNIQUE INDEX secrets_one_active_per_owner_purpose
  ON ape_dts_platform.secrets (owner_type, owner_id, purpose)
  WHERE destroyed_at IS NULL;

CREATE TABLE ape_dts_platform.users (
  id uuid PRIMARY KEY,
  normalized_username text NOT NULL UNIQUE,
  display_name text NOT NULL,
  role text NOT NULL CHECK (role IN ('ADMIN', 'DEVELOPER')),
  status text NOT NULL CHECK (status IN (
    'INVITED',
    'PASSWORD_REQUIRED',
    'TOTP_REQUIRED',
    'ACTIVE',
    'DISABLED'
  )),
  password_kdf_algorithm text NOT NULL CHECK (password_kdf_algorithm = 'ARGON2ID'),
  password_kdf_parameters jsonb NOT NULL CHECK (jsonb_typeof(password_kdf_parameters) = 'object'),
  password_salt bytea NOT NULL CHECK (octet_length(password_salt) >= 16),
  password_hash bytea NOT NULL CHECK (octet_length(password_hash) >= 32),
  totp_secret_id uuid REFERENCES ape_dts_platform.secrets(id),
  session_generation bigint NOT NULL DEFAULT 1 CHECK (session_generation > 0),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  disabled_at timestamptz,
  CONSTRAINT users_normalized_username CHECK (
    normalized_username = lower(btrim(normalized_username))
    AND normalized_username <> ''
  ),
  CONSTRAINT users_status_consistency CHECK (
    (status = 'DISABLED' AND disabled_at IS NOT NULL)
    OR (status <> 'DISABLED' AND disabled_at IS NULL)
  )
);

CREATE TABLE ape_dts_platform.platform_settings (
  singleton_id smallint PRIMARY KEY DEFAULT 1 CHECK (singleton_id = 1),
  max_runtime_units integer CHECK (max_runtime_units > 0),
  log_retention_days integer NOT NULL DEFAULT 30 CHECK (log_retention_days > 0),
  run_log_limit_bytes bigint NOT NULL DEFAULT 1073741824 CHECK (run_log_limit_bytes > 0),
  log_failure_buffer_bytes bigint NOT NULL DEFAULT 67108864 CHECK (log_failure_buffer_bytes > 0),
  run_retention_days integer NOT NULL DEFAULT 365 CHECK (run_retention_days > 0),
  alert_retention_days integer NOT NULL DEFAULT 365 CHECK (alert_retention_days > 0),
  audit_retention_days integer NOT NULL DEFAULT 180 CHECK (audit_retention_days >= 180),
  metrics_capacity_alert_bytes bigint CHECK (metrics_capacity_alert_bytes > 0),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  updated_by uuid REFERENCES ape_dts_platform.users(id),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE ape_dts_platform.audit_events (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id uuid NOT NULL UNIQUE,
  actor_type text NOT NULL CHECK (actor_type IN ('DEPLOYMENT_ADMIN', 'SYSTEM', 'USER')),
  actor_user_id uuid REFERENCES ape_dts_platform.users(id),
  action text NOT NULL,
  subject_type text NOT NULL,
  subject_id uuid,
  canonical_details jsonb NOT NULL CHECK (jsonb_typeof(canonical_details) = 'object'),
  previous_hash bytea,
  event_hash bytea NOT NULL CHECK (octet_length(event_hash) = 32),
  occurred_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT audit_events_actor_consistency CHECK (
    (actor_type = 'USER' AND actor_user_id IS NOT NULL)
    OR (actor_type <> 'USER' AND actor_user_id IS NULL)
  )
);

CREATE INDEX audit_events_occurred_at_idx
  ON ape_dts_platform.audit_events (occurred_at);
