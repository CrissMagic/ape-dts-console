ALTER TABLE ape_dts_platform.users
  ALTER COLUMN password_kdf_algorithm DROP NOT NULL,
  ALTER COLUMN password_kdf_parameters DROP NOT NULL,
  ALTER COLUMN password_salt DROP NOT NULL,
  ALTER COLUMN password_hash DROP NOT NULL,
  ADD COLUMN last_accepted_totp_counter bigint CHECK (
    last_accepted_totp_counter IS NULL OR last_accepted_totp_counter >= 0
  ),
  ADD COLUMN credentials_updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  ADD CONSTRAINT users_password_presence_consistency CHECK (
    (
      status = 'INVITED'
      AND password_kdf_algorithm IS NULL
      AND password_kdf_parameters IS NULL
      AND password_salt IS NULL
      AND password_hash IS NULL
    )
    OR (
      status <> 'INVITED'
      AND password_kdf_algorithm IS NOT NULL
      AND password_kdf_parameters IS NOT NULL
      AND password_salt IS NOT NULL
      AND password_hash IS NOT NULL
    )
  );

CREATE FUNCTION ape_dts_platform.enforce_user_security_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
DECLARE
  remaining_active_administrators integer;
BEGIN
  IF NEW.session_generation < OLD.session_generation THEN
    RAISE EXCEPTION 'session generation cannot decrease'
      USING ERRCODE = '23514';
  END IF;

  IF (
    OLD.role IS DISTINCT FROM NEW.role
    OR OLD.status IS DISTINCT FROM NEW.status
    OR OLD.password_kdf_algorithm IS DISTINCT FROM NEW.password_kdf_algorithm
    OR OLD.password_kdf_parameters IS DISTINCT FROM NEW.password_kdf_parameters
    OR OLD.password_salt IS DISTINCT FROM NEW.password_salt
    OR OLD.password_hash IS DISTINCT FROM NEW.password_hash
    OR OLD.totp_secret_id IS DISTINCT FROM NEW.totp_secret_id
  ) THEN
    IF NEW.session_generation <= OLD.session_generation THEN
      NEW.session_generation := OLD.session_generation + 1;
    END IF;
    NEW.credentials_updated_at := statement_timestamp();
  END IF;

  IF (
    OLD.role = 'ADMIN'
    AND OLD.status = 'ACTIVE'
    AND (NEW.role <> 'ADMIN' OR NEW.status <> 'ACTIVE')
  ) THEN
    PERFORM pg_advisory_xact_lock(1096048964, 1229342030);
    SELECT count(*)::integer
    INTO remaining_active_administrators
    FROM ape_dts_platform.users
    WHERE id <> OLD.id
      AND role = 'ADMIN'
      AND status = 'ACTIVE';
    IF remaining_active_administrators = 0 THEN
      RAISE EXCEPTION 'the final active administrator cannot be disabled or demoted'
        USING ERRCODE = '23514';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER users_security_lifecycle
BEFORE UPDATE ON ape_dts_platform.users
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_user_security_lifecycle();

CREATE FUNCTION ape_dts_platform.prevent_user_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  RAISE EXCEPTION 'users cannot be physically deleted'
    USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER users_no_physical_delete
BEFORE DELETE ON ape_dts_platform.users
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_user_delete();

CREATE TABLE ape_dts_platform.user_activation_tokens (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  purpose text NOT NULL CHECK (purpose IN (
    'INVITATION',
    'CREDENTIAL_RESET',
    'ROLE_ELEVATION'
  )),
  token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
  created_by_user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  CONSTRAINT user_activation_tokens_lifecycle CHECK (
    expires_at > created_at
    AND (consumed_at IS NULL OR consumed_at >= created_at)
  )
);

CREATE UNIQUE INDEX user_activation_tokens_one_active_per_user
  ON ape_dts_platform.user_activation_tokens (user_id)
  WHERE consumed_at IS NULL;

CREATE TABLE ape_dts_platform.user_recovery_code_batches (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  state text NOT NULL CHECK (state IN ('ACTIVE', 'REVOKED')),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  revoked_at timestamptz,
  CONSTRAINT user_recovery_code_batches_state_consistency CHECK (
    (state = 'ACTIVE' AND revoked_at IS NULL)
    OR (state = 'REVOKED' AND revoked_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX user_recovery_code_batches_one_active
  ON ape_dts_platform.user_recovery_code_batches (user_id)
  WHERE state = 'ACTIVE';

CREATE TABLE ape_dts_platform.user_recovery_codes (
  id uuid PRIMARY KEY,
  batch_id uuid NOT NULL REFERENCES ape_dts_platform.user_recovery_code_batches(id),
  ordinal smallint NOT NULL CHECK (ordinal > 0),
  code_salt bytea NOT NULL CHECK (octet_length(code_salt) >= 16),
  code_hash bytea NOT NULL CHECK (octet_length(code_hash) = 32),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  used_at timestamptz,
  UNIQUE (batch_id, ordinal)
);

CREATE INDEX user_recovery_codes_available_idx
  ON ape_dts_platform.user_recovery_codes (batch_id, ordinal)
  WHERE used_at IS NULL;

CREATE TABLE ape_dts_platform.authentication_challenges (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  challenge_type text NOT NULL CHECK (challenge_type IN (
    'LOGIN_TOTP',
    'TOTP_ENROLLMENT'
  )),
  token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
  source_fingerprint bytea NOT NULL CHECK (octet_length(source_fingerprint) = 32),
  session_generation bigint NOT NULL CHECK (session_generation > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  CONSTRAINT authentication_challenges_lifecycle CHECK (
    expires_at > created_at
    AND (consumed_at IS NULL OR consumed_at >= created_at)
  )
);

CREATE INDEX authentication_challenges_user_type_idx
  ON ape_dts_platform.authentication_challenges (user_id, challenge_type, created_at DESC);

CREATE TABLE ape_dts_platform.auth_sessions (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES ape_dts_platform.users(id),
  token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
  csrf_token_hash bytea NOT NULL CHECK (octet_length(csrf_token_hash) = 32),
  source_fingerprint bytea NOT NULL CHECK (octet_length(source_fingerprint) = 32),
  session_generation bigint NOT NULL CHECK (session_generation > 0),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  last_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  idle_expires_at timestamptz NOT NULL,
  absolute_expires_at timestamptz NOT NULL,
  reauthenticated_at timestamptz NOT NULL,
  revoked_at timestamptz,
  revocation_reason text,
  CONSTRAINT auth_sessions_expiry_consistency CHECK (
    absolute_expires_at > created_at
    AND idle_expires_at > created_at
    AND idle_expires_at <= absolute_expires_at
    AND last_seen_at >= created_at
  ),
  CONSTRAINT auth_sessions_revocation_consistency CHECK (
    (revoked_at IS NULL AND revocation_reason IS NULL)
    OR (
      revoked_at IS NOT NULL
      AND revocation_reason IS NOT NULL
      AND btrim(revocation_reason) <> ''
    )
  )
);

CREATE INDEX auth_sessions_user_active_idx
  ON ape_dts_platform.auth_sessions (user_id, absolute_expires_at)
  WHERE revoked_at IS NULL;

CREATE TABLE ape_dts_platform.authentication_rate_limits (
  account_fingerprint bytea NOT NULL CHECK (octet_length(account_fingerprint) = 32),
  source_fingerprint bytea NOT NULL CHECK (octet_length(source_fingerprint) = 32),
  factor text NOT NULL CHECK (factor IN (
    'PASSWORD',
    'TOTP',
    'RECOVERY_CODE',
    'REAUTHENTICATION'
  )),
  failure_count integer NOT NULL CHECK (failure_count > 0),
  window_started_at timestamptz NOT NULL,
  last_failed_at timestamptz NOT NULL,
  blocked_until timestamptz,
  PRIMARY KEY (account_fingerprint, source_fingerprint, factor),
  CONSTRAINT authentication_rate_limits_time_consistency CHECK (
    last_failed_at >= window_started_at
    AND (blocked_until IS NULL OR blocked_until > last_failed_at)
  )
);

CREATE TABLE ape_dts_platform.audit_chain_checkpoints (
  first_retained_sequence bigint PRIMARY KEY CHECK (first_retained_sequence > 1),
  last_deleted_sequence bigint NOT NULL UNIQUE CHECK (last_deleted_sequence > 0),
  last_deleted_event_hash bytea NOT NULL CHECK (octet_length(last_deleted_event_hash) = 32),
  created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  created_by_user_id uuid REFERENCES ape_dts_platform.users(id),
  CONSTRAINT audit_chain_checkpoints_boundary_consistency CHECK (
    first_retained_sequence = last_deleted_sequence + 1
  )
);

CREATE FUNCTION ape_dts_platform.enforce_audit_chain_tail()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
DECLARE
  expected_previous_hash bytea;
  has_previous_event boolean;
BEGIN
  PERFORM pg_advisory_xact_lock(1096048964, 1096104660);
  SELECT event_hash
  INTO expected_previous_hash
  FROM ape_dts_platform.audit_events
  ORDER BY sequence DESC
  LIMIT 1;
  has_previous_event := FOUND;

  IF has_previous_event AND NEW.previous_hash IS DISTINCT FROM expected_previous_hash THEN
    RAISE EXCEPTION 'audit previous hash does not match the current tail'
      USING ERRCODE = '23514';
  END IF;
  IF NOT has_previous_event AND NEW.previous_hash IS NOT NULL THEN
    RAISE EXCEPTION 'the first audit event cannot declare a previous hash'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER audit_events_chain_tail
BEFORE INSERT ON ape_dts_platform.audit_events
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_audit_chain_tail();

CREATE FUNCTION ape_dts_platform.prevent_audit_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  RAISE EXCEPTION 'audit events are append-only'
    USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER audit_events_no_update_or_delete
BEFORE UPDATE OR DELETE ON ape_dts_platform.audit_events
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.prevent_audit_event_mutation();
