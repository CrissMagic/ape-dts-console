-- Runtime rows introduced before run_id was available cannot be associated with
-- a logical run safely. Do not guess an association during an upgrade.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM ape_dts_platform.task_run_units
     WHERE run_id IS NULL
       AND status IN ('STARTING', 'RUNNING', 'STOPPING')
  ) THEN
    RAISE EXCEPTION 'runtime integrity migration requires manual remediation for active task_run_units without run_id'
      USING ERRCODE = '23514';
  END IF;

  IF EXISTS (
    SELECT 1
      FROM ape_dts_platform.capacity_reservations
     WHERE run_id IS NULL
       AND state IN ('RESERVED', 'ACTIVE')
  ) THEN
    RAISE EXCEPTION 'runtime integrity migration requires manual remediation for active capacity reservations without run_id'
      USING ERRCODE = '23514';
  END IF;
END;
$$;

-- The single-column primary keys are not sufficient targets for foreign keys
-- that also need to prove task/revision/run ownership.
ALTER TABLE ape_dts_platform.task_revisions
  ADD CONSTRAINT task_revisions_id_task_key UNIQUE (id, task_id);

ALTER TABLE ape_dts_platform.task_runs
  ADD CONSTRAINT task_runs_id_task_key UNIQUE (id, task_id),
  ADD CONSTRAINT task_runs_id_task_revision_key UNIQUE (id, task_id, task_revision_id);

ALTER TABLE ape_dts_platform.task_operations
  ADD CONSTRAINT task_operations_id_task_key UNIQUE (id, task_id);

ALTER TABLE ape_dts_platform.task_run_units
  ADD CONSTRAINT task_run_units_id_run_key UNIQUE (id, run_id),
  ADD CONSTRAINT task_run_units_run_required CHECK (
    run_id IS NOT NULL OR status IN ('STOPPED', 'FAILED', 'BLOCKED')
  );

ALTER TABLE ape_dts_platform.capacity_reservations
  ADD CONSTRAINT capacity_reservations_run_required CHECK (
    run_id IS NOT NULL OR state = 'RELEASED'
  );

ALTER TABLE ape_dts_platform.task_runs
  ADD CONSTRAINT task_runs_revision_task_fk
    FOREIGN KEY (task_revision_id, task_id)
    REFERENCES ape_dts_platform.task_revisions (id, task_id),
  ADD CONSTRAINT task_runs_operation_task_fk
    FOREIGN KEY (operation_id, task_id)
    REFERENCES ape_dts_platform.task_operations (id, task_id);

ALTER TABLE ape_dts_platform.task_operations
  ADD CONSTRAINT task_operations_target_revision_task_fk
    FOREIGN KEY (target_revision_id, task_id)
    REFERENCES ape_dts_platform.task_revisions (id, task_id),
  ADD CONSTRAINT task_operations_run_task_fk
    FOREIGN KEY (run_id, task_id)
    REFERENCES ape_dts_platform.task_runs (id, task_id);

ALTER TABLE ape_dts_platform.task_run_units
  ADD CONSTRAINT task_run_units_run_task_fk
    FOREIGN KEY (run_id, task_id)
    REFERENCES ape_dts_platform.task_runs (id, task_id);

ALTER TABLE ape_dts_platform.log_segments
  ADD CONSTRAINT log_segments_unit_run_fk
    FOREIGN KEY (run_unit_id, run_id)
    REFERENCES ape_dts_platform.task_run_units (id, run_id);

ALTER TABLE ape_dts_platform.log_gaps
  ADD CONSTRAINT log_gaps_unit_run_fk
    FOREIGN KEY (run_unit_id, run_id)
    REFERENCES ape_dts_platform.task_run_units (id, run_id);

ALTER TABLE ape_dts_platform.capacity_reservations
  ADD CONSTRAINT capacity_reservations_revision_task_fk
    FOREIGN KEY (task_revision_id, task_id)
    REFERENCES ape_dts_platform.task_revisions (id, task_id),
  ADD CONSTRAINT capacity_reservations_run_task_revision_fk
    FOREIGN KEY (run_id, task_id, task_revision_id)
    REFERENCES ape_dts_platform.task_runs (id, task_id, task_revision_id);
