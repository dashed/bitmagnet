-- +goose Up
-- +goose StatementBegin

ALTER TABLE torrents ADD COLUMN IF NOT EXISTS files_data BYTEA;
ALTER TABLE torrents ADD COLUMN IF NOT EXISTS file_extensions JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS torrent_file_summary (
    info_hash BYTEA PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
    file_count INT NOT NULL DEFAULT 0,
    total_size BIGINT NOT NULL DEFAULT 0,
    largest_file_size BIGINT NOT NULL DEFAULT 0,
    extensions JSONB NOT NULL DEFAULT '[]'::jsonb,
    has_video BOOLEAN NOT NULL DEFAULT FALSE,
    has_subtitle BOOLEAN NOT NULL DEFAULT FALSE,
    has_audio BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE IF EXISTS torrent_file_summary;
ALTER TABLE torrents DROP COLUMN IF EXISTS file_extensions;
ALTER TABLE torrents DROP COLUMN IF EXISTS files_data;

-- +goose StatementEnd
