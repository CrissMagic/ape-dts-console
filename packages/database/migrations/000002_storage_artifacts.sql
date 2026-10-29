CREATE TABLE ape_dts_platform.storage_profiles (
  id uuid PRIMARY KEY,
  profile_key text NOT NULL,
  display_name text NOT NULL,
  version integer NOT NULL CHECK (version > 0),
  driver text NOT NULL CHECK (driver IN ('LOCAL', 'OSS')),
  config jsonb NOT NULL CHECK (jsonb_typeof(config) = 'object'),
  secret_id uuid REFERENCES ape_dts_platform.secrets(id),
  state text NOT NULL CHECK (state IN (
    'DRAFT',
    'ACTIVE_WRITE',
    'READ_ONLY_REFERENCED',
    'RETIRED'
  )),
  last_test_status text NOT NULL DEFAULT 'NOT_TESTED' CHECK (last_test_status IN (
    'NOT_TESTED',
    'SUCCEEDED',
    'FAILED'
  )),
  last_tested_at timestamptz,
  last_test_error_code text,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  activated_at timestamptz,
  retired_at timestamptz,
  UNIQUE (profile_key, version),
  CONSTRAINT storage_profiles_key_normalized CHECK (
    profile_key = lower(btrim(profile_key))
    AND profile_key ~ '^[a-z0-9][a-z0-9_-]{0,63}$'
  ),
  CONSTRAINT storage_profiles_display_name_nonempty CHECK (btrim(display_name) <> ''),
  CONSTRAINT storage_profiles_driver_config CHECK (
    (
      driver = 'LOCAL'
      AND secret_id IS NULL
      AND jsonb_typeof(config -> 'relative_prefix') = 'string'
      AND (config - 'relative_prefix') = '{}'::jsonb
      AND (
        config ->> 'relative_prefix' = ''
        OR (
          left(config ->> 'relative_prefix', 1) <> '/'
          AND right(config ->> 'relative_prefix', 1) <> '/'
          AND strpos(config ->> 'relative_prefix', '//') = 0
          AND strpos(config ->> 'relative_prefix', E'\\') = 0
          AND (config ->> 'relative_prefix') !~ '(^|/)\.\.?($|/)'
          AND (config ->> 'relative_prefix') !~ E'[\\n\\r]'
        )
      )
    )
    OR
    (
      driver = 'OSS'
      AND secret_id IS NOT NULL
      AND jsonb_typeof(config -> 'endpoint') = 'string'
      AND jsonb_typeof(config -> 'bucket') = 'string'
      AND jsonb_typeof(config -> 'prefix') = 'string'
      AND btrim(config ->> 'endpoint') <> ''
      AND btrim(config ->> 'bucket') <> ''
      AND (config - ARRAY['endpoint', 'bucket', 'prefix']) = '{}'::jsonb
    )
  ),
  CONSTRAINT storage_profiles_test_result_consistency CHECK (
    (last_test_status = 'NOT_TESTED' AND last_tested_at IS NULL AND last_test_error_code IS NULL)
    OR (last_test_status = 'SUCCEEDED' AND last_tested_at IS NOT NULL AND last_test_error_code IS NULL)
    OR (last_test_status = 'FAILED' AND last_tested_at IS NOT NULL AND last_test_error_code IS NOT NULL)
  ),
  CONSTRAINT storage_profiles_lifecycle_consistency CHECK (
    (state = 'DRAFT' AND activated_at IS NULL AND retired_at IS NULL)
    OR (state IN ('ACTIVE_WRITE', 'READ_ONLY_REFERENCED') AND activated_at IS NOT NULL AND retired_at IS NULL)
    OR (state = 'RETIRED' AND retired_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX storage_profiles_one_active_write
  ON ape_dts_platform.storage_profiles ((1))
  WHERE state = 'ACTIVE_WRITE';

CREATE TABLE ape_dts_platform.stored_objects (
  id uuid PRIMARY KEY,
  storage_profile_id uuid NOT NULL REFERENCES ape_dts_platform.storage_profiles(id),
  kind text NOT NULL CHECK (kind IN ('ARTIFACT', 'LOG_SEGMENT')),
  object_key text NOT NULL CHECK (
    object_key <> ''
    AND object_key !~ E'[\\n\\r]'
  ),
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
  content_type text NOT NULL CHECK (btrim(content_type) <> ''),
  state text NOT NULL CHECK (state IN (
    'PENDING',
    'AVAILABLE',
    'DELETED',
    'MISSING',
    'INTEGRITY_FAILED'
  )),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  available_at timestamptz,
  deleted_at timestamptz,
  last_verified_at timestamptz,
  failure_code text,
  UNIQUE (storage_profile_id, object_key),
  CONSTRAINT stored_objects_deleted_consistency CHECK (
    (state = 'DELETED' AND deleted_at IS NOT NULL)
    OR (state <> 'DELETED' AND deleted_at IS NULL)
  ),
  CONSTRAINT stored_objects_available_consistency CHECK (
    state <> 'AVAILABLE' OR available_at IS NOT NULL
  ),
  CONSTRAINT stored_objects_failure_consistency CHECK (
    (state IN ('MISSING', 'INTEGRITY_FAILED') AND failure_code IS NOT NULL)
    OR (state NOT IN ('MISSING', 'INTEGRITY_FAILED'))
  )
);

CREATE INDEX stored_objects_profile_state_idx
  ON ape_dts_platform.stored_objects (storage_profile_id, state);

CREATE FUNCTION ape_dts_platform.enforce_storage_profile_immutability()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.state <> 'DRAFT' AND NEW.state = 'DRAFT' THEN
    RAISE EXCEPTION 'storage profile cannot return to draft'
      USING ERRCODE = '23514';
  END IF;

  IF OLD.state = 'RETIRED' AND NEW.state <> 'RETIRED' THEN
    RAISE EXCEPTION 'retired storage profile cannot be reactivated'
      USING ERRCODE = '23514';
  END IF;

  IF (
    OLD.profile_key IS DISTINCT FROM NEW.profile_key
    OR OLD.version IS DISTINCT FROM NEW.version
    OR OLD.driver IS DISTINCT FROM NEW.driver
    OR OLD.config IS DISTINCT FROM NEW.config
    OR OLD.secret_id IS DISTINCT FROM NEW.secret_id
  ) AND (OLD.state <> 'DRAFT' OR NEW.state <> 'DRAFT') THEN
    RAISE EXCEPTION 'storage profile definition is immutable after draft'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.state = 'RETIRED' AND EXISTS (
    SELECT 1
    FROM ape_dts_platform.stored_objects
    WHERE storage_profile_id = OLD.id
      AND state <> 'DELETED'
  ) THEN
    RAISE EXCEPTION 'referenced storage profile cannot be retired'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER storage_profiles_immutable_definition
BEFORE UPDATE ON ape_dts_platform.storage_profiles
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_storage_profile_immutability();

CREATE TABLE ape_dts_platform.artifacts (
  id uuid PRIMARY KEY,
  version_identifier text NOT NULL UNIQUE CHECK (btrim(version_identifier) <> ''),
  declared_source text NOT NULL CHECK (btrim(declared_source) <> ''),
  stored_object_id uuid NOT NULL REFERENCES ape_dts_platform.stored_objects(id),
  declared_sha256 bytea NOT NULL CHECK (octet_length(declared_sha256) = 32),
  actual_sha256 bytea CHECK (actual_sha256 IS NULL OR octet_length(actual_sha256) = 32),
  size_bytes bigint NOT NULL CHECK (size_bytes > 0),
  declared_platform_compatibility text,
  state text NOT NULL DEFAULT 'INACTIVE' CHECK (state IN (
    'ACTIVE',
    'INACTIVE',
    'QUARANTINED'
  )),
  state_reason text,
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  activated_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT artifacts_state_consistency CHECK (
    (state = 'ACTIVE' AND actual_sha256 IS NOT NULL AND activated_at IS NOT NULL)
    OR (state = 'INACTIVE' AND activated_at IS NULL)
    OR (state = 'QUARANTINED' AND state_reason IS NOT NULL)
  )
);

CREATE INDEX artifacts_stored_object_idx
  ON ape_dts_platform.artifacts (stored_object_id);

CREATE TABLE ape_dts_platform.artifact_probe_runs (
  id uuid PRIMARY KEY,
  artifact_id uuid NOT NULL REFERENCES ape_dts_platform.artifacts(id),
  probe_kind text NOT NULL CHECK (probe_kind IN ('STATIC', 'BEHAVIOR')),
  probe_version text NOT NULL CHECK (btrim(probe_version) <> ''),
  status text NOT NULL CHECK (status IN ('RUNNING', 'PASSED', 'FAILED', 'TIMED_OUT')),
  started_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  ended_at timestamptz,
  exit_code integer,
  failure_code text,
  evidence_object_id uuid REFERENCES ape_dts_platform.stored_objects(id),
  evidence_summary jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (
    jsonb_typeof(evidence_summary) = 'object'
  ),
  CONSTRAINT artifact_probe_runs_lifecycle_consistency CHECK (
    (status = 'RUNNING' AND ended_at IS NULL AND failure_code IS NULL)
    OR (status = 'PASSED' AND ended_at IS NOT NULL AND failure_code IS NULL)
    OR (status IN ('FAILED', 'TIMED_OUT') AND ended_at IS NOT NULL AND failure_code IS NOT NULL)
  )
);

CREATE INDEX artifact_probe_runs_artifact_started_idx
  ON ape_dts_platform.artifact_probe_runs (artifact_id, started_at DESC);

CREATE TABLE ape_dts_platform.artifact_capabilities (
  artifact_id uuid NOT NULL REFERENCES ape_dts_platform.artifacts(id),
  capability_name text NOT NULL CHECK (btrim(capability_name) <> ''),
  support_state text NOT NULL CHECK (support_state IN ('SUPPORTED', 'UNSUPPORTED', 'UNKNOWN')),
  evidence_probe_run_id uuid REFERENCES ape_dts_platform.artifact_probe_runs(id),
  evidence_version text NOT NULL CHECK (btrim(evidence_version) <> ''),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  PRIMARY KEY (artifact_id, capability_name)
);

CREATE TABLE ape_dts_platform.artifact_certifications (
  id uuid PRIMARY KEY,
  artifact_id uuid NOT NULL REFERENCES ape_dts_platform.artifacts(id),
  platform_version text NOT NULL CHECK (btrim(platform_version) <> ''),
  task_scenario text NOT NULL CHECK (task_scenario IN (
    'PG_SNAPSHOT_TO_KAFKA',
    'PG_CDC_TO_KAFKA',
    'KAFKA_RAW_FORWARD'
  )),
  adapter_id text NOT NULL CHECK (btrim(adapter_id) <> ''),
  adapter_version text NOT NULL CHECK (btrim(adapter_version) <> ''),
  state text NOT NULL CHECK (state IN ('PENDING', 'PASSED', 'FAILED', 'REVOKED')),
  evidence_probe_run_id uuid REFERENCES ape_dts_platform.artifact_probe_runs(id),
  failure_code text,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  revoked_at timestamptz,
  UNIQUE (artifact_id, platform_version, task_scenario, adapter_id, adapter_version),
  CONSTRAINT artifact_certifications_state_consistency CHECK (
    (state = 'PENDING' AND evidence_probe_run_id IS NULL AND failure_code IS NULL AND revoked_at IS NULL)
    OR (state = 'PASSED' AND evidence_probe_run_id IS NOT NULL AND failure_code IS NULL AND revoked_at IS NULL)
    OR (state = 'FAILED' AND failure_code IS NOT NULL AND revoked_at IS NULL)
    OR (state = 'REVOKED' AND revoked_at IS NOT NULL)
  )
);

CREATE INDEX artifact_certifications_lookup_idx
  ON ape_dts_platform.artifact_certifications (
    artifact_id,
    platform_version,
    task_scenario,
    state
  );

CREATE TABLE ape_dts_platform.artifact_signature_checks (
  id uuid PRIMARY KEY,
  artifact_id uuid NOT NULL REFERENCES ape_dts_platform.artifacts(id),
  policy_version text NOT NULL CHECK (btrim(policy_version) <> ''),
  status text NOT NULL CHECK (status IN ('NOT_REQUIRED', 'PASSED', 'FAILED')),
  signer_id text,
  signature_algorithm text,
  signature_sha256 bytea CHECK (
    signature_sha256 IS NULL OR octet_length(signature_sha256) = 32
  ),
  failure_code text,
  checked_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  checked_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  CONSTRAINT artifact_signature_checks_result_consistency CHECK (
    (status = 'NOT_REQUIRED' AND signer_id IS NULL AND signature_algorithm IS NULL AND signature_sha256 IS NULL AND failure_code IS NULL)
    OR (status = 'PASSED' AND signer_id IS NOT NULL AND signature_algorithm IS NOT NULL AND signature_sha256 IS NOT NULL AND failure_code IS NULL)
    OR (status = 'FAILED' AND failure_code IS NOT NULL)
  )
);

CREATE INDEX artifact_signature_checks_artifact_checked_idx
  ON ape_dts_platform.artifact_signature_checks (artifact_id, checked_at DESC);

CREATE FUNCTION ape_dts_platform.enforce_artifact_activation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.state = 'QUARANTINED' AND NEW.state = 'ACTIVE' THEN
    RAISE EXCEPTION 'quarantined artifact cannot be activated directly'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.state = 'ACTIVE' AND NOT EXISTS (
    SELECT 1
    FROM ape_dts_platform.artifact_certifications
    WHERE artifact_id = NEW.id
      AND state = 'PASSED'
  ) THEN
    RAISE EXCEPTION 'artifact requires a passed certification before activation'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_activation_guard
BEFORE UPDATE OF state ON ape_dts_platform.artifacts
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_artifact_activation();

CREATE FUNCTION ape_dts_platform.revoke_artifact_certifications_on_quarantine()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  UPDATE ape_dts_platform.artifact_certifications
  SET state = 'REVOKED',
      revoked_at = statement_timestamp(),
      updated_at = statement_timestamp(),
      row_version = row_version + 1
  WHERE artifact_id = NEW.id
    AND state <> 'REVOKED';
  RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_quarantine_revokes_certifications
AFTER UPDATE OF state ON ape_dts_platform.artifacts
FOR EACH ROW
WHEN (NEW.state = 'QUARANTINED' AND OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION ape_dts_platform.revoke_artifact_certifications_on_quarantine();
