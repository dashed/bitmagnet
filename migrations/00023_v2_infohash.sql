-- +goose Up
-- +goose StatementBegin

-- BitTorrent v2 / hybrid (BEP 52) support — foundation (gap G1a).
-- The canonical `info_hash` primary key stays 20 bytes: the v1 SHA-1 for v1-only
-- and hybrid torrents, and the truncated (first 20 bytes) SHA-256 for pure-v2
-- torrents (the form BEP 52 mandates wherever 20 bytes are required, e.g. the DHT).
-- These columns record the full dual identity alongside it.
alter table torrents add column info_hash_v1 bytea;     -- 20-byte SHA-1 (v1 + hybrid)
alter table torrents add column info_hash_v2 bytea;     -- 32-byte SHA-256 (hybrid + pure-v2)
alter table torrents add column meta_version smallint;  -- 1 = v1-only, 2 = v2 / hybrid

-- +goose StatementEnd
-- +goose StatementBegin

create index on torrents (info_hash_v1);
-- Plain (NOT unique) index. A hybrid torrent is announced on the DHT under BOTH
-- its v1 and its truncated-v2 infohash, so it can be discovered twice with two
-- different 20-byte primary keys but the SAME full info_hash_v2. The batched
-- persist upsert only arbitrates ON CONFLICT (info_hash); a UNIQUE violation on
-- info_hash_v2 would abort the whole batch and roll back the transaction (data
-- loss). Exact v2-identity dedup / hybrid-row merge belongs to a follow-on slice
-- (G1b: look up info_hash_v2 before insert). For the foundation a plain index
-- supports those lookups without the footgun.
create index on torrents (info_hash_v2);

-- +goose StatementEnd
-- +goose StatementBegin

-- Backfill existing rows: all torrents indexed before this migration are v1.
update torrents set info_hash_v1 = info_hash, meta_version = 1;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

alter table torrents drop column if exists meta_version;
alter table torrents drop column if exists info_hash_v2;
alter table torrents drop column if exists info_hash_v1;

-- +goose StatementEnd
