ALTER TABLE ape_dts_platform.users
  ADD COLUMN must_change_password boolean NOT NULL DEFAULT false;

-- Retire the pending second-factor step only for accounts with an established password.
-- Invitations, password-reset-pending and disabled accounts remain untouched.
UPDATE ape_dts_platform.users
SET status = 'ACTIVE', row_version = row_version + 1, updated_at = statement_timestamp()
WHERE status = 'TOTP_REQUIRED' AND password_hash IS NOT NULL;

-- Previously issued second-factor challenges must not be usable after migration.
UPDATE ape_dts_platform.authentication_challenges
SET consumed_at = statement_timestamp()
WHERE consumed_at IS NULL;
