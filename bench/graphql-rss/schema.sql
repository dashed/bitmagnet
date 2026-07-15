-- Minimal production-compatible L1 schema for the isolated GraphQL/sqlx RSS gate.
-- This is deliberately not a replacement for Goose migrations. It contains the
-- exact columns touched by the Phase-2 candidate/refine/hydration path and keeps
-- the harness independent of non-transactional production backfills.

CREATE TABLE goose_db_version (
  id bigserial PRIMARY KEY,
  version_id bigint NOT NULL,
  is_applied boolean NOT NULL,
  tstamp timestamptz NOT NULL DEFAULT now()
);
INSERT INTO goose_db_version (version_id, is_applied) VALUES (25, true);

CREATE TABLE torrent_sources (
  key text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE metadata_sources (
  key text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE torrents (
  info_hash bytea PRIMARY KEY,
  info_hash_v1 bytea,
  info_hash_v2 bytea,
  meta_version smallint,
  name text NOT NULL,
  size bigint NOT NULL,
  private boolean NOT NULL,
  files_status text NOT NULL,
  extension text,
  files_count integer,
  files_data bytea,
  file_extensions jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE torrents_torrent_sources (
  source text NOT NULL REFERENCES torrent_sources(key),
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  import_id text,
  seeders integer,
  leechers integer,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  seen_count integer NOT NULL DEFAULT 1,
  PRIMARY KEY (source, info_hash)
);

CREATE TABLE torrent_tags (
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  name text NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (info_hash, name)
);

CREATE TABLE content (
  type text NOT NULL,
  source text NOT NULL,
  id text NOT NULL,
  title text NOT NULL,
  release_year integer,
  original_language text,
  original_title text,
  overview text,
  runtime integer,
  popularity real,
  vote_average real,
  vote_count bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (type, source, id)
);

CREATE TABLE content_attributes (
  content_type text NOT NULL,
  content_source text NOT NULL,
  content_id text NOT NULL,
  source text NOT NULL,
  key text NOT NULL,
  value text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (content_type, content_source, content_id, source, key)
);

CREATE TABLE torrent_contents (
  id text PRIMARY KEY,
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  content_type text,
  content_source text,
  content_id text,
  languages jsonb,
  episodes jsonb,
  video_resolution text,
  video_source text,
  video_codec text,
  video_3d text,
  video_modifier text,
  release_group text,
  tsv tsvector,
  seeders integer,
  leechers integer,
  published_at timestamptz NOT NULL,
  size bigint NOT NULL,
  files_count integer,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  UNIQUE (info_hash, content_type, content_source, content_id)
);
CREATE INDEX torrent_contents_info_hash_idx ON torrent_contents(info_hash);

CREATE TABLE torrent_file_summary (
  info_hash bytea PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
  file_count integer NOT NULL,
  total_size bigint NOT NULL,
  largest_file_size bigint NOT NULL,
  extensions jsonb NOT NULL,
  has_video boolean NOT NULL,
  has_subtitle boolean NOT NULL,
  has_audio boolean NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
