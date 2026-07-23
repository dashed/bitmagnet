-- +goose Up
-- +goose StatementBegin

-- Serves the poll-only queue claim order used by both Go and Rust. The partial
-- predicate keeps terminal archive rows out of the hot index.
CREATE INDEX queue_jobs_claim_idx
  ON queue_jobs (queue, (status = 'retry'), priority, run_after)
  WHERE status IN ('pending', 'retry');

-- Serves the processed-row shadow mirror's stable (ran_at, id) cursor.
CREATE INDEX queue_jobs_processed_mirror_idx
  ON queue_jobs (ran_at, id)
  WHERE queue = 'process_torrent'
    AND status = 'processed'
    AND ran_at IS NOT NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS queue_jobs_processed_mirror_idx;
DROP INDEX IF EXISTS queue_jobs_claim_idx;

-- +goose StatementEnd
