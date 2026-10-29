ALTER TABLE ape_dts_platform.task_groups
  ADD COLUMN sort_order bigint NOT NULL DEFAULT 0;

WITH ordered AS (
  SELECT id, row_number() OVER (ORDER BY normalized_name, id) AS position
  FROM ape_dts_platform.task_groups
)
UPDATE ape_dts_platform.task_groups AS groups
SET sort_order = ordered.position
FROM ordered
WHERE groups.id = ordered.id;
