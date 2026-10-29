-- Widen only the identity kind constraint; historical observations and tasks are unchanged.
ALTER TABLE ape_dts_platform.data_source_identity_observations
  DROP CONSTRAINT data_source_identity_observations_identity_kind_check,
  ADD CONSTRAINT data_source_identity_observations_identity_kind_check CHECK (
    identity_kind IN ('POSTGRES_DATABASE', 'KAFKA_CLUSTER', 'MYSQL_DATABASE', 'UNAVAILABLE')
  );
