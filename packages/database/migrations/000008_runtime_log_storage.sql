ALTER TABLE ape_dts_platform.stored_objects
  ADD COLUMN created_by_actor_type text NOT NULL DEFAULT 'USER';

ALTER TABLE ape_dts_platform.stored_objects
  ALTER COLUMN created_by_user_id DROP NOT NULL,
  ADD CONSTRAINT stored_objects_creator_consistency CHECK (
    (created_by_actor_type = 'USER' AND created_by_user_id IS NOT NULL)
    OR (created_by_actor_type = 'SYSTEM' AND created_by_user_id IS NULL)
  );

ALTER TABLE ape_dts_platform.task_runs
  ADD COLUMN log_storage_profile_id uuid,
  ADD CONSTRAINT task_runs_log_storage_profile_fk
    FOREIGN KEY (log_storage_profile_id)
    REFERENCES ape_dts_platform.storage_profiles (id);

-- Existing logs determine an existing run's immutable profile binding. A
-- migration must not guess when historical objects already span profiles.
DO $$
DECLARE
  conflicting_run_id uuid;
BEGIN
  SELECT segments.run_id
    INTO conflicting_run_id
    FROM ape_dts_platform.log_segments AS segments
    JOIN ape_dts_platform.stored_objects AS objects
      ON objects.id = segments.stored_object_id
   GROUP BY segments.run_id
  HAVING count(DISTINCT objects.storage_profile_id) > 1
   ORDER BY segments.run_id
   LIMIT 1;

  IF conflicting_run_id IS NOT NULL THEN
    RAISE EXCEPTION 'runtime log storage migration found a run with objects in multiple profiles: %', conflicting_run_id
      USING ERRCODE = '23514';
  END IF;
END;
$$;

UPDATE ape_dts_platform.task_runs AS runs
   SET log_storage_profile_id = historical.storage_profile_id
  FROM (
    SELECT segments.run_id,
           min(objects.storage_profile_id::text)::uuid AS storage_profile_id
      FROM ape_dts_platform.log_segments AS segments
      JOIN ape_dts_platform.stored_objects AS objects
        ON objects.id = segments.stored_object_id
     GROUP BY segments.run_id
    HAVING count(DISTINCT objects.storage_profile_id) = 1
  ) AS historical
 WHERE runs.id = historical.run_id
   AND runs.log_storage_profile_id IS NULL;

CREATE FUNCTION ape_dts_platform.enforce_run_log_storage_profile_immutability()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
DECLARE
  bound_profile_state text;
BEGIN
  IF TG_OP = 'UPDATE'
     AND OLD.log_storage_profile_id IS NOT NULL
     AND NEW.log_storage_profile_id IS DISTINCT FROM OLD.log_storage_profile_id THEN
    RAISE EXCEPTION 'run log storage profile cannot change after binding'
      USING ERRCODE = '23514';
  END IF;

  IF NEW.log_storage_profile_id IS NOT NULL THEN
    SELECT profiles.state
      INTO bound_profile_state
      FROM ape_dts_platform.storage_profiles AS profiles
     WHERE profiles.id = NEW.log_storage_profile_id
     FOR SHARE;

    IF NEW.status IN ('STARTING', 'RUNNING', 'STOPPING')
       AND bound_profile_state = 'RETIRED' THEN
      RAISE EXCEPTION 'active run cannot bind a retired log storage profile'
        USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
      SELECT 1
        FROM ape_dts_platform.log_segments AS segments
        JOIN ape_dts_platform.stored_objects AS objects
          ON objects.id = segments.stored_object_id
       WHERE segments.run_id = NEW.id
         AND objects.storage_profile_id IS DISTINCT FROM NEW.log_storage_profile_id
    ) THEN
      RAISE EXCEPTION 'run log storage profile conflicts with an existing log object'
        USING ERRCODE = '23514';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER task_runs_log_storage_profile_immutable
BEFORE INSERT OR UPDATE OF log_storage_profile_id, status ON ape_dts_platform.task_runs
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_run_log_storage_profile_immutability();

CREATE FUNCTION ape_dts_platform.enforce_storage_profile_active_run_retirement()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF OLD.state <> 'RETIRED'
     AND NEW.state = 'RETIRED'
     AND EXISTS (
       SELECT 1
         FROM ape_dts_platform.task_runs AS runs
        WHERE runs.log_storage_profile_id = OLD.id
          AND runs.status IN ('STARTING', 'RUNNING', 'STOPPING')
     ) THEN
    RAISE EXCEPTION 'storage profile bound to an active run cannot be retired'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER storage_profiles_active_run_retirement_guard
BEFORE UPDATE OF state ON ape_dts_platform.storage_profiles
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_storage_profile_active_run_retirement();

CREATE FUNCTION ape_dts_platform.enforce_log_segment_storage_profile_consistency()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
DECLARE
  object_profile_id uuid;
  run_profile_id uuid;
BEGIN
  IF NEW.stored_object_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT objects.storage_profile_id
    INTO object_profile_id
    FROM ape_dts_platform.stored_objects AS objects
   WHERE objects.id = NEW.stored_object_id
   FOR SHARE;

  SELECT runs.log_storage_profile_id
    INTO run_profile_id
    FROM ape_dts_platform.task_runs AS runs
   WHERE runs.id = NEW.run_id
   FOR SHARE;

  IF object_profile_id IS NOT NULL
     AND (run_profile_id IS NULL OR object_profile_id IS DISTINCT FROM run_profile_id) THEN
    RAISE EXCEPTION 'log segment object storage profile conflicts with its run binding'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER log_segments_storage_profile_consistency
BEFORE INSERT OR UPDATE OF run_id, stored_object_id ON ape_dts_platform.log_segments
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_log_segment_storage_profile_consistency();

CREATE FUNCTION ape_dts_platform.enforce_stored_object_log_profile_consistency()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, ape_dts_platform
AS $$
BEGIN
  IF NEW.storage_profile_id IS DISTINCT FROM OLD.storage_profile_id
     AND EXISTS (
       SELECT 1
         FROM ape_dts_platform.log_segments AS segments
         JOIN ape_dts_platform.task_runs AS runs
           ON runs.id = segments.run_id
        WHERE segments.stored_object_id = OLD.id
          AND (
            runs.log_storage_profile_id IS NULL
            OR runs.log_storage_profile_id IS DISTINCT FROM NEW.storage_profile_id
          )
     ) THEN
    RAISE EXCEPTION 'stored log object storage profile conflicts with its run binding'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER stored_objects_log_profile_consistency
BEFORE UPDATE OF storage_profile_id ON ape_dts_platform.stored_objects
FOR EACH ROW
EXECUTE FUNCTION ape_dts_platform.enforce_stored_object_log_profile_consistency();

ALTER TABLE ape_dts_platform.log_segments
  DROP CONSTRAINT log_segments_run_id_stream_sequence_key,
  ADD CONSTRAINT log_segments_run_unit_stream_sequence_key
    UNIQUE (run_id, run_unit_id, stream, sequence),
  ADD CONSTRAINT log_segments_deletion_identity_key
    UNIQUE (id, run_id, stored_object_id);

CREATE TABLE ape_dts_platform.log_segment_deletion_requests (
  log_segment_id uuid PRIMARY KEY,
  run_id uuid NOT NULL,
  stored_object_id uuid NOT NULL UNIQUE,
  reason text NOT NULL CHECK (reason IN ('RETENTION', 'CAPACITY_ROLLOVER')),
  requested_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  deleted_at timestamptz NOT NULL,
  completed_at timestamptz,
  last_error_code text,
  CONSTRAINT log_segment_deletion_requests_segment_fk
    FOREIGN KEY (log_segment_id, run_id, stored_object_id)
    REFERENCES ape_dts_platform.log_segments (id, run_id, stored_object_id),
  CONSTRAINT log_segment_deletion_requests_completion CHECK (
    completed_at IS NULL OR last_error_code IS NULL
  )
);

CREATE INDEX log_segment_deletion_requests_pending_idx
  ON ape_dts_platform.log_segment_deletion_requests (requested_at, log_segment_id)
  WHERE completed_at IS NULL;
