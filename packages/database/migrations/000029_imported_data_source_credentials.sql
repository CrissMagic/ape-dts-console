ALTER TABLE ape_dts_platform.data_sources
  ADD COLUMN credentials_required boolean NOT NULL DEFAULT false;

ALTER TABLE ape_dts_platform.data_sources
  DROP CONSTRAINT data_sources_secret_consistency,
  ADD CONSTRAINT data_sources_secret_consistency CHECK (
    (source_type = 'KAFKA' AND connection_secret_id IS NULL AND NOT credentials_required)
    OR (source_type <> 'KAFKA' AND (
      (connection_secret_id IS NOT NULL AND NOT credentials_required)
      OR (connection_secret_id IS NULL AND credentials_required)
    ))
  );
