-- +goose NO TRANSACTION
--
-- Build the index online, as 00027 does: the production queue is actively
-- written by the Go processor, so a transactional CREATE INDEX would block
-- those writes for the duration of the build. A failed concurrent build can
-- leave an invalid index behind; dropping the known name first makes a goose
-- retry self-healing.

-- +goose Up

-- Serves the persisting Rust process_torrent worker's admitted claim
-- (bitmagnet-rs/crates/bitmagnet-queue/src/process_torrent_pg.rs). That claim
-- only takes payloads that pin the classifier subset the Rust worker supports.
-- On queue_jobs_claim_idx it has to step over every organic pending row ahead
-- of the first admitted one, so with no admitted row it reads the whole
-- process_torrent backlog on every poll. The predicate repeats the claim's
-- admission predicate exactly, which is what lets the planner prove that the
-- claim implies it; the two must change together. Organic payloads never
-- match, so the index holds only admitted jobs and the Go worker never uses it.
DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_admitted_process_torrent_claim_idx;
CREATE INDEX CONCURRENTLY queue_jobs_admitted_process_torrent_claim_idx
  ON queue_jobs ((status = 'retry'), priority, run_after)
  WHERE queue = 'process_torrent'
    AND status IN ('pending', 'retry')
    AND CASE WHEN payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false}}'::jsonb
      THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags'
           - 'InfoHashes' = '{}'::jsonb
      ELSE false END;

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_admitted_process_torrent_claim_idx;
