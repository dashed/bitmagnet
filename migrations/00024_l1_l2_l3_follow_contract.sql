-- +goose NO TRANSACTION
--
-- Source-owned L1/L2/L3 follow contract.
--
-- L2 filesearch and L3 pathsearch do not receive direct writes from the Go
-- crawler. They follow the canonical L1 stream:
--
--   * changed torrents: torrents.updated_at in a half-open window;
--   * hard deletes: deleted_torrents audit rows from an AFTER DELETE trigger.
--
-- Homelab originally bootstrapped this DDL operationally. Keep this migration
-- adoption-safe and idempotent so an already-provisioned database is a no-op,
-- while new installs carry the schema contract with the source tree.

-- +goose Up

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrents_updated_at_info_hash_idx
  ON torrents (updated_at, info_hash);

CREATE TABLE IF NOT EXISTS deleted_torrents (
  info_hash  bytea PRIMARY KEY,
  deleted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS deleted_torrents_deleted_at_idx
  ON deleted_torrents (deleted_at);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION record_torrent_deletion() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  INSERT INTO deleted_torrents (info_hash, deleted_at)
  VALUES (OLD.info_hash, now())
  ON CONFLICT (info_hash) DO UPDATE SET deleted_at = now();
  RETURN OLD;
END
$fn$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS torrents_deletion_audit ON torrents;
CREATE TRIGGER torrents_deletion_audit
  AFTER DELETE ON torrents
  FOR EACH ROW EXECUTE FUNCTION record_torrent_deletion();

-- +goose Down

DROP TRIGGER IF EXISTS torrents_deletion_audit ON torrents;
DROP FUNCTION IF EXISTS record_torrent_deletion();
DROP INDEX CONCURRENTLY IF EXISTS deleted_torrents_deleted_at_idx;
DROP TABLE IF EXISTS deleted_torrents;
DROP INDEX CONCURRENTLY IF EXISTS torrents_updated_at_info_hash_idx;
