-- +goose NO TRANSACTION
--
-- This migration runs OUTSIDE a wrapping transaction (goose NO TRANSACTION) so it
-- can apply ONLINE against the live ~48.2M-row `torrents` table with no outage:
--   * CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
--   * The backfill is batched with a per-batch COMMIT (see below), which also
--     requires statement-level autocommit (a transaction block forbids COMMIT).
-- Each statement below is therefore its own implicit transaction. Reads and writes
-- on `torrents` continue throughout (MVCC); no statement holds ACCESS EXCLUSIVE for
-- more than the instant it takes to update catalog metadata.

-- +goose Up

-- BitTorrent v2 / hybrid (BEP 52) support — foundation (gap G1a).
-- The canonical `info_hash` primary key stays 20 bytes: the v1 SHA-1 for v1-only
-- and hybrid torrents, and the truncated (first 20 bytes) SHA-256 for pure-v2
-- torrents (the form BEP 52 mandates wherever 20 bytes are required, e.g. the DHT).
-- These columns record the full dual identity alongside it.
--
-- ADD COLUMN of a nullable column with NO default is metadata-only in PostgreSQL
-- (11+): it takes a brief ACCESS EXCLUSIVE lock to update the catalog and returns
-- instantly without rewriting the table. IF NOT EXISTS makes a re-run a no-op.
alter table torrents add column if not exists info_hash_v1 bytea;     -- 20-byte SHA-1 (v1 + hybrid)
alter table torrents add column if not exists info_hash_v2 bytea;     -- 32-byte SHA-256 (hybrid + pure-v2)
alter table torrents add column if not exists meta_version smallint;  -- 1 = v1-only, 2 = v2 / hybrid

-- Indexes, built CONCURRENTLY so the build never takes a table lock (mirrors the
-- proven pattern in 00022_blob_indexes.sql). The names match what plain
-- `CREATE INDEX ON torrents (col)` auto-generates (torrents_<col>_idx), so the
-- end-state schema is byte-for-byte identical to the original migration.
--
-- GOTCHA: a CONCURRENT build that fails partway leaves an INVALID index behind.
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS` would then SEE that invalid index and
-- skip the rebuild, leaving it permanently broken. So we DROP any leftover first
-- (CONCURRENTLY, so even the cleanup never locks the table) — a no-op on the happy
-- path, and the thing that makes a retry self-healing.
--
-- Plain (NOT unique) indexes. A hybrid torrent is announced on the DHT under BOTH
-- its v1 and its truncated-v2 infohash, so it can be discovered twice with two
-- different 20-byte primary keys but the SAME full info_hash_v2. The batched
-- persist upsert only arbitrates ON CONFLICT (info_hash); a UNIQUE violation on
-- info_hash_v2 would abort the whole batch and roll back the transaction (data
-- loss). Exact v2-identity dedup / hybrid-row merge belongs to a follow-on slice
-- (G1b: look up info_hash_v2 before insert). For the foundation a plain index
-- supports those lookups without the footgun.
drop index concurrently if exists torrents_info_hash_v1_idx;
create index concurrently if not exists torrents_info_hash_v1_idx on torrents (info_hash_v1);

drop index concurrently if exists torrents_info_hash_v2_idx;
create index concurrently if not exists torrents_info_hash_v2_idx on torrents (info_hash_v2);

-- Backfill existing rows: all torrents indexed before this migration are v1.
--
-- Temporary PARTIAL index over only the not-yet-backfilled rows. Without it, each
-- batch's `WHERE meta_version IS NULL ... LIMIT n` is a sequential scan that must
-- skip every already-backfilled row at the front of the heap → O(n) per batch →
-- O(n^2) overall on 48.2M rows (catastrophic). With it, each batch fetches its
-- next n rows in ~O(n), and the index shrinks as rows leave the predicate. Built
-- CONCURRENTLY and dropped once the backfill finishes.
drop index concurrently if exists torrents_v2_backfill_idx;
create index concurrently if not exists torrents_v2_backfill_idx on torrents (info_hash) where meta_version is null;

-- The backfill itself is BATCHED with a COMMIT per batch rather than one giant
-- UPDATE. A single UPDATE would already be outage-free here (NO TRANSACTION ⇒ row
-- locks only, reads continue via MVCC), but on 48.2M rows it would: create 48.2M
-- dead tuples at once (heap ~doubles on disk until autovacuum catches up); emit
-- one multi-GB WAL burst (checkpoint/replication pressure); and hold a single
-- long-running transaction whose xmin pins the CLUSTER-WIDE vacuum horizon for the
-- full 15-60 min, blocking dead-tuple reclamation everywhere. Per-batch COMMIT
-- bounds dead tuples and WAL per transaction, lets the xmin horizon advance so
-- autovacuum keeps up, and — because the loop selects on `meta_version IS NULL` —
-- makes the backfill idempotent and resumable: a re-run continues from wherever a
-- previous attempt stopped. A plain DO block cannot do this (it is a single
-- transaction and cannot COMMIT); a procedure called in autocommit can.
-- +goose StatementBegin
create or replace procedure _bm_backfill_v2_identity()
language plpgsql
as $$
declare
  v_rows integer;
begin
  loop
    update torrents
    set info_hash_v1 = info_hash, meta_version = 1
    where info_hash in (
      select info_hash from torrents where meta_version is null limit 20000
    );
    get diagnostics v_rows = row_count;
    commit;
    exit when v_rows = 0;
  end loop;
end;
$$;
-- +goose StatementEnd

call _bm_backfill_v2_identity();

-- Tidy up the throwaway helpers; the end-state schema is exactly the three columns
-- plus the two info_hash_v1/v2 indexes.
drop procedure _bm_backfill_v2_identity();
drop index concurrently if exists torrents_v2_backfill_idx;

-- +goose Down
-- +goose StatementBegin

-- DROP COLUMN is metadata-only (brief ACCESS EXCLUSIVE). Dropping each column also
-- drops the single-column index that depends on it (torrents_info_hash_v1_idx /
-- _v2_idx), so the indexes need no explicit DROP here.
alter table torrents drop column if exists meta_version;
alter table torrents drop column if exists info_hash_v2;
alter table torrents drop column if exists info_hash_v1;

-- +goose StatementEnd
