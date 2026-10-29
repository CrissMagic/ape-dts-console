LOCK TABLE ape_dts_platform.log_segments IN ACCESS EXCLUSIVE MODE;

DROP INDEX ape_dts_platform.log_segments_run_sequence_idx;

-- Version 8 allowed each runtime unit to restart a stream sequence at 1.  The
-- runtime now orders segments across all units in one logical run, so rewrite
-- historical rows deterministically before enforcing that invariant.
ALTER TABLE ape_dts_platform.log_segments
  DROP CONSTRAINT log_segments_run_unit_stream_sequence_key;

WITH ranked_segments AS (
  SELECT id,
         row_number() OVER (
           PARTITION BY run_id, stream
           ORDER BY created_at, id
         ) AS sequence
    FROM ape_dts_platform.log_segments
)
UPDATE ape_dts_platform.log_segments AS segments
   SET sequence = ranked.sequence
  FROM ranked_segments AS ranked
 WHERE segments.id = ranked.id
   AND segments.sequence IS DISTINCT FROM ranked.sequence;

ALTER TABLE ape_dts_platform.log_segments
  ADD CONSTRAINT log_segments_run_unit_stream_sequence_key
    UNIQUE (run_id, run_unit_id, stream, sequence),
  ADD CONSTRAINT log_segments_run_stream_sequence_key
    UNIQUE (run_id, stream, sequence);
