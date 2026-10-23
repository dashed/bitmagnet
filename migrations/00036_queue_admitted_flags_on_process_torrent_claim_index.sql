-- +goose NO TRANSACTION
--
-- Build the index online, as 00027 and 00035 do: the production queue is
-- actively written by the Go processor, so a transactional CREATE INDEX would
-- block those writes for the duration of the build. A failed concurrent build
-- can leave an invalid index behind; dropping the known name first makes a
-- goose retry self-healing.

-- +goose Up

-- Serves the persisting Rust process_torrent worker's claim when it runs with
-- --admit-flags-on (ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL in
-- bitmagnet-rs/crates/bitmagnet-queue/src/process_torrent_pg.rs). That claim
-- takes 00035's explicit flags-off payloads and also explicit flags-on ones
-- (all three attach flags JSON true), in one claim order, so it needs one
-- index holding both. The predicate repeats the claim's admission predicate
-- exactly, which is what lets the planner prove that the claim implies it; the
-- two must change together. The key allow-list stays nested in the CASE: AND-ed
-- beside the containment it knocks the claim off its index. Organic payloads
-- carry no explicit flags, so they never match, the index holds only admitted
-- jobs, and the Go worker never uses it. 00035's index stays for the worker
-- running without the switch.
DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_admitted_flags_on_process_torrent_claim_idx;
CREATE INDEX CONCURRENTLY queue_jobs_admitted_flags_on_process_torrent_claim_idx
  ON queue_jobs ((status = 'retry'), priority, run_after)
  WHERE queue = 'process_torrent'
    AND status IN ('pending', 'retry')
    AND CASE WHEN payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false}}'::jsonb
        OR payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":true,"apis_enabled":true,"tmdb_enabled":true}}'::jsonb
      THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags'
           - 'InfoHashes' = '{}'::jsonb
      ELSE false END;

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_admitted_flags_on_process_torrent_claim_idx;
