ALTER TABLE ape_dts_platform.storage_profiles
  ADD COLUMN created_by_actor_type text NOT NULL DEFAULT 'USER';

ALTER TABLE ape_dts_platform.storage_profiles
  ALTER COLUMN created_by_user_id DROP NOT NULL,
  ADD CONSTRAINT storage_profiles_creator_consistency CHECK (
    (created_by_actor_type = 'USER' AND created_by_user_id IS NOT NULL)
    OR (created_by_actor_type = 'SYSTEM' AND created_by_user_id IS NULL)
  );
