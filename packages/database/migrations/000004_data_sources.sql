CREATE TABLE ape_dts_platform.data_sources (
  id uuid PRIMARY KEY,
  normalized_name text NOT NULL,
  source_type text NOT NULL CHECK (source_type IN ('POSTGRES', 'KAFKA', 'MYSQL', 'DORIS')),
  configuration jsonb NOT NULL CHECK (
    jsonb_typeof(configuration) = 'object'
    AND configuration ? 'type'
    AND configuration ->> 'type' = source_type
    AND NOT configuration ? 'password'
  ),
  connection_secret_id uuid REFERENCES ape_dts_platform.secrets(id),
  state text NOT NULL CHECK (state IN ('ACTIVE', 'DELETED')),
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  updated_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  deleted_at timestamptz,
  CONSTRAINT data_sources_name_normalized CHECK (
    normalized_name = lower(btrim(normalized_name))
    AND normalized_name <> ''
  ),
  CONSTRAINT data_sources_secret_consistency CHECK (
    (source_type = 'KAFKA' AND connection_secret_id IS NULL)
    OR (source_type <> 'KAFKA' AND connection_secret_id IS NOT NULL)
  ),
  CONSTRAINT data_sources_state_consistency CHECK (
    (state = 'ACTIVE' AND deleted_at IS NULL)
    OR (state = 'DELETED' AND deleted_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX data_sources_one_active_name
  ON ape_dts_platform.data_sources (normalized_name)
  WHERE deleted_at IS NULL;

CREATE TABLE ape_dts_platform.data_source_grants (
  id uuid PRIMARY KEY,
  data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  granted_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  granted_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  revoked_at timestamptz,
  revoked_by_user_id uuid REFERENCES ape_dts_platform.users(id),
  CONSTRAINT data_source_grants_revocation_consistency CHECK (
    (revoked_at IS NULL AND revoked_by_user_id IS NULL)
    OR (revoked_at IS NOT NULL AND revoked_by_user_id IS NOT NULL)
  )
);

CREATE UNIQUE INDEX data_source_grants_one_active_per_user
  ON ape_dts_platform.data_source_grants (data_source_id, user_id)
  WHERE revoked_at IS NULL;

CREATE INDEX data_source_grants_user_active_idx
  ON ape_dts_platform.data_source_grants (user_id, data_source_id)
  WHERE revoked_at IS NULL;

CREATE TABLE ape_dts_platform.connection_test_runs (
  id uuid PRIMARY KEY,
  data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  initiated_by_user_id uuid REFERENCES ape_dts_platform.users(id),
  purpose text NOT NULL CHECK (purpose IN ('MANUAL', 'PREFLIGHT')),
  state text NOT NULL CHECK (state IN ('SUCCEEDED', 'FAILED')),
  started_at timestamptz NOT NULL,
  completed_at timestamptz NOT NULL,
  duration_ms integer NOT NULL CHECK (duration_ms >= 0),
  error_code text,
  error_summary text,
  CONSTRAINT connection_test_runs_completion CHECK (
    completed_at >= started_at
    AND (
      (state = 'SUCCEEDED' AND error_code IS NULL AND error_summary IS NULL)
      OR (
        state = 'FAILED'
        AND error_code IS NOT NULL
        AND error_summary IS NOT NULL
        AND btrim(error_summary) <> ''
      )
    )
  )
);

CREATE INDEX connection_test_runs_data_source_completed_idx
  ON ape_dts_platform.connection_test_runs (data_source_id, completed_at DESC);

CREATE TABLE ape_dts_platform.data_source_identity_observations (
  id uuid PRIMARY KEY,
  data_source_id uuid NOT NULL REFERENCES ape_dts_platform.data_sources(id),
  connection_test_run_id uuid NOT NULL UNIQUE REFERENCES ape_dts_platform.connection_test_runs(id),
  identity_kind text NOT NULL CHECK (identity_kind IN (
    'POSTGRES_DATABASE', 'KAFKA_CLUSTER', 'UNAVAILABLE'
  )),
  identity_value text,
  server_version text,
  observed_at timestamptz NOT NULL,
  CONSTRAINT data_source_identity_observations_identity_consistency CHECK (
    (identity_kind = 'UNAVAILABLE' AND identity_value IS NULL)
    OR (identity_kind <> 'UNAVAILABLE' AND identity_value IS NOT NULL)
  )
);

CREATE INDEX data_source_identity_observations_source_observed_idx
  ON ape_dts_platform.data_source_identity_observations (data_source_id, observed_at DESC);

CREATE FUNCTION ape_dts_platform.enforce_data_source_grant_target()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
DECLARE
  target_role text;
  target_status text;
  source_state text;
BEGIN
  IF NEW.revoked_at IS NOT NULL THEN
    RETURN NEW;
  END IF;

  SELECT role, status
  INTO target_role, target_status
  FROM ape_dts_platform.users
  WHERE id = NEW.user_id;
  IF target_role IS DISTINCT FROM 'DEVELOPER' OR target_status IS DISTINCT FROM 'ACTIVE' THEN
    RAISE EXCEPTION 'data source USE grants require an active developer'
      USING ERRCODE = '23514';
  END IF;

  SELECT state
  INTO source_state
  FROM ape_dts_platform.data_sources
  WHERE id = NEW.data_source_id;
  IF source_state IS DISTINCT FROM 'ACTIVE' THEN
    RAISE EXCEPTION 'data source USE grants require an active data source'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER data_source_grants_target
BEFORE INSERT OR UPDATE OF data_source_id, user_id, revoked_at
ON ape_dts_platform.data_source_grants
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_data_source_grant_target();

CREATE FUNCTION ape_dts_platform.prevent_data_source_physical_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  RAISE EXCEPTION 'data sources cannot be physically deleted'
    USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER data_sources_no_physical_delete
BEFORE DELETE ON ape_dts_platform.data_sources
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_data_source_physical_delete();

CREATE FUNCTION ape_dts_platform.prevent_data_source_reactivation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.deleted_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'deleted data sources cannot be changed or reactivated'
      USING ERRCODE = '23514';
  END IF;
  IF OLD.deleted_at IS NULL AND NEW.row_version < OLD.row_version THEN
    RAISE EXCEPTION 'data source row version cannot decrease'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER data_sources_lifecycle
BEFORE UPDATE ON ape_dts_platform.data_sources
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_data_source_reactivation();
