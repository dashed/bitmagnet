-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS torrents_file_extensions_idx ON torrents USING GIN (file_extensions jsonb_path_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_file_summary_extensions_idx ON torrent_file_summary USING GIN (extensions jsonb_path_ops);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS torrent_file_summary_extensions_idx;
DROP INDEX CONCURRENTLY IF EXISTS torrents_file_extensions_idx;
