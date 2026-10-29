ALTER TABLE ape_dts_platform.artifacts
  ADD COLUMN deleted_at timestamptz,
  ADD COLUMN deleted_by_user_id uuid REFERENCES ape_dts_platform.users(id);

CREATE FUNCTION ape_dts_platform.require_live_artifact(artifact_id uuid)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, ape_dts_platform AS $$
BEGIN
  IF artifact_id IS NULL THEN RETURN; END IF;
  PERFORM 1 FROM ape_dts_platform.artifacts a
    WHERE a.id = artifact_id AND a.deleted_at IS NULL FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'artifact unavailable' USING ERRCODE = '23514';
  END IF;
END;
$$;

CREATE FUNCTION ape_dts_platform.guard_artifact_binding()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, ape_dts_platform AS $$
DECLARE bound_id uuid;
BEGIN
  IF TG_TABLE_NAME = 'tasks' THEN
    IF NEW.deleted_at IS NULL THEN
      FOR bound_id IN SELECT artifact_id FROM ape_dts_platform.task_revisions
        WHERE id IN (NEW.current_draft_revision_id, NEW.current_applied_revision_id)
      LOOP PERFORM ape_dts_platform.require_live_artifact(bound_id); END LOOP;
    END IF;
  ELSE
    PERFORM ape_dts_platform.require_live_artifact(NEW.artifact_id);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER task_revision_live_artifact BEFORE INSERT OR UPDATE OF artifact_id
  ON ape_dts_platform.task_revisions FOR EACH ROW
  EXECUTE FUNCTION ape_dts_platform.guard_artifact_binding();
CREATE TRIGGER task_live_artifact BEFORE INSERT OR UPDATE OF current_draft_revision_id, current_applied_revision_id
  ON ape_dts_platform.tasks FOR EACH ROW
  EXECUTE FUNCTION ape_dts_platform.guard_artifact_binding();
CREATE TRIGGER task_run_live_artifact BEFORE INSERT OR UPDATE OF artifact_id
  ON ape_dts_platform.task_runs FOR EACH ROW
  EXECUTE FUNCTION ape_dts_platform.guard_artifact_binding();
