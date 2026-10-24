-- +goose NO TRANSACTION
--
-- Build the queue indexes online. The production queue is actively written by
-- the Go processor, so a transactional CREATE INDEX would take a lock that
-- blocks those writes for the duration of the build. A failed concurrent build
-- can leave an invalid index behind; dropping the known name first makes a
-- goose retry self-healing.

-- +goose Up

-- Serves the poll-only queue claim order used by both Go and Rust. The partial
-- predicate keeps terminal archive rows out of the hot index.
DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_claim_idx;
CREATE INDEX CONCURRENTLY queue_jobs_claim_idx
  ON queue_jobs (queue, (status = 'retry'), priority, run_after)
  WHERE status IN ('pending', 'retry');

-- Serves the processed-row shadow mirror's stable (ran_at, id) cursor.
DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_processed_mirror_idx;
CREATE INDEX CONCURRENTLY queue_jobs_processed_mirror_idx
  ON queue_jobs (ran_at, id)
  WHERE queue = 'process_torrent'
    AND status = 'processed'
    AND ran_at IS NOT NULL;

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_processed_mirror_idx;
DROP INDEX CONCURRENTLY IF EXISTS queue_jobs_claim_idx;
