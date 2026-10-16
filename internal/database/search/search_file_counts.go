package search

import (
	"context"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// TorrentFileCountsSearch exposes a cheap, blob-free per-torrent file-count
// lookup. The L3 pathsearch composer calls it BEFORE hydrating any files_data
// blob so it can bound (and fail-loud cap) its per-request blob-decode memory
// from the authoritative file counts rather than from the blobs themselves
// (gate7-4 byte-bound).
type TorrentFileCountsSearch interface {
	FileCounts(ctx context.Context, ids []protocol.ID) (map[protocol.ID]int, error)
}

// FileCounts returns, for each requested info_hash, its file count. It reads the
// PK-indexed torrent_file_summary.file_count (a point lookup per id — sub-10ms
// for the ≤MaxCandidates candidate ids), falling back to torrents.files_count
// for any id with no summary row. An id for which NEITHER source has a value is
// ABSENT from the returned map; the composer treats an absent id conservatively
// (as the per-torrent cap) so a missing count can never UNDER-size the decode
// budget. The query never touches torrent_files or files_data, so it adds zero
// blob decode — that is the whole point of reading it before the hydrator.
func (s search) FileCounts(ctx context.Context, ids []protocol.ID) (map[protocol.ID]int, error) {
	out := make(map[protocol.ID]int, len(ids))
	if len(ids) == 0 {
		return out, nil
	}

	db := s.q.Torrent.WithContext(ctx).ReadDB().UnderlyingDB()

	type countRow struct {
		InfoHash  protocol.ID `gorm:"column:info_hash"`
		FileCount int         `gorm:"column:file_count"`
	}

	// Primary: torrent_file_summary (PK on info_hash) — the pre-aggregated,
	// authoritative per-torrent file count.
	var summaries []countRow
	if err := db.
		Table(model.TableNameTorrentFileSummary).
		Select("info_hash, file_count").
		Where("info_hash IN ?", idValues(ids)).
		Scan(&summaries).Error; err != nil {
		return nil, err
	}

	for _, r := range summaries {
		out[r.InfoHash] = r.FileCount
	}

	// Fallback: torrents.files_count for any id missing a summary row.
	missing := make([]protocol.ID, 0, len(ids))

	for _, id := range ids {
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		var fallback []countRow
		if err := db.
			Table(model.TableNameTorrent).
			Select("info_hash, files_count AS file_count").
			Where("info_hash IN ? AND files_count IS NOT NULL", idValues(missing)).
			Scan(&fallback).Error; err != nil {
			return nil, err
		}

		for _, r := range fallback {
			out[r.InfoHash] = r.FileCount
		}
	}

	return out, nil
}

// idValues renders the info-hash set as raw bytea values for a parameterized
// `info_hash IN ?` clause (info_hash is a 20-byte bytea column).
func idValues(ids []protocol.ID) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Bytes()
	}

	return out
}
