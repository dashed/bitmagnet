-- Least-privilege writer for the Rust torrent.reprocess differential. Run as
-- the disposable database's owner after the Go oracle has migrated it. The
-- grants must equal admit_torrent_reprocess_writer_authority's exact set.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bitmagnet_graphql_torrent_reprocess_writer_ci') THEN
    DROP OWNED BY bitmagnet_graphql_torrent_reprocess_writer_ci;
    DROP ROLE bitmagnet_graphql_torrent_reprocess_writer_ci;
  END IF;
END
$$;
CREATE ROLE bitmagnet_graphql_torrent_reprocess_writer_ci
  LOGIN PASSWORD 'graphql-torrent-reprocess-writer-ci-only'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 4;
REVOKE ALL PRIVILEGES ON DATABASE bitmagnet_graphql_torrent_reprocess_test FROM PUBLIC;
GRANT CONNECT ON DATABASE bitmagnet_graphql_torrent_reprocess_test
  TO bitmagnet_graphql_torrent_reprocess_writer_ci;
GRANT USAGE ON SCHEMA public TO bitmagnet_graphql_torrent_reprocess_writer_ci;
GRANT SELECT ON goose_db_version, torrents, torrents_torrent_sources, torrent_hints,
  content, content_attributes, content_collections, content_collections_content
  TO bitmagnet_graphql_torrent_reprocess_writer_ci;
GRANT SELECT, INSERT, UPDATE, DELETE ON torrent_contents
  TO bitmagnet_graphql_torrent_reprocess_writer_ci;
GRANT INSERT ON torrent_tags, queue_jobs TO bitmagnet_graphql_torrent_reprocess_writer_ci;
GRANT SELECT (id, fingerprint, status) ON queue_jobs TO bitmagnet_graphql_torrent_reprocess_writer_ci;
