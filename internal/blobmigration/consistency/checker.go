package consistency

import (
	"context"
	"fmt"
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

type CheckResult struct {
	InfoHash   protocol.ID
	Match      bool
	BlobFiles  int
	RowFiles   int
	Mismatches []FieldMismatch
}

type FieldMismatch struct {
	FileIndex int
	Field     string
	Expected  string
	Got       string
}

type Summary struct {
	TotalChecked    int
	Matches         int
	Mismatches      int
	Errors          int
	MismatchDetails []CheckResult
}

func CompareFiles(blobFiles, rowFiles []model.TorrentFile) CheckResult {
	result := CheckResult{
		BlobFiles: len(blobFiles),
		RowFiles:  len(rowFiles),
	}

	if len(blobFiles) != len(rowFiles) {
		result.Mismatches = append(result.Mismatches, FieldMismatch{
			FileIndex: -1,
			Field:     "count",
			Expected:  fmt.Sprintf("%d", len(rowFiles)),
			Got:       fmt.Sprintf("%d", len(blobFiles)),
		})
		result.Match = false

		return result
	}

	sortedBlob := make([]model.TorrentFile, len(blobFiles))
	copy(sortedBlob, blobFiles)
	sort.Slice(sortedBlob, func(i, j int) bool { return sortedBlob[i].Index < sortedBlob[j].Index })

	sortedRows := make([]model.TorrentFile, len(rowFiles))
	copy(sortedRows, rowFiles)
	sort.Slice(sortedRows, func(i, j int) bool { return sortedRows[i].Index < sortedRows[j].Index })

	for i := range sortedBlob {
		b := sortedBlob[i]
		r := sortedRows[i]

		if b.Index != r.Index {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: i,
				Field:     "index",
				Expected:  fmt.Sprintf("%d", r.Index),
				Got:       fmt.Sprintf("%d", b.Index),
			})
		}

		if b.Path != r.Path {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: i,
				Field:     "path",
				Expected:  r.Path,
				Got:       b.Path,
			})
		}

		if b.Size != r.Size {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: i,
				Field:     "size",
				Expected:  fmt.Sprintf("%d", r.Size),
				Got:       fmt.Sprintf("%d", b.Size),
			})
		}
	}

	result.Match = len(result.Mismatches) == 0

	return result
}

type blobRow struct {
	InfoHash  protocol.ID
	FilesData []byte
}

func CheckTorrent(ctx context.Context, q *dao.Query, infoHash protocol.ID) (CheckResult, error) {
	var row blobRow

	err := q.Torrent.UnderlyingDB().WithContext(ctx).
		Table("torrents").
		Select("info_hash, files_data").
		Where("info_hash = ?", infoHash).
		Scan(&row).Error
	if err != nil {
		return CheckResult{InfoHash: infoHash}, fmt.Errorf("reading blob: %w", err)
	}

	if row.FilesData == nil {
		return CheckResult{InfoHash: infoHash}, fmt.Errorf("no files_data for torrent")
	}

	blobFiles, err := blobmigration.DeserializeFiles(row.FilesData)
	if err != nil {
		return CheckResult{InfoHash: infoHash}, fmt.Errorf("deserializing blob: %w", err)
	}

	rowFilesPtrs, err := q.TorrentFile.WithContext(ctx).
		Where(q.TorrentFile.InfoHash.Eq(infoHash)).
		Order(q.TorrentFile.Index.Asc()).
		Find()
	if err != nil {
		return CheckResult{InfoHash: infoHash}, fmt.Errorf("reading rows: %w", err)
	}

	rowFiles := make([]model.TorrentFile, len(rowFilesPtrs))
	for i, f := range rowFilesPtrs {
		rowFiles[i] = *f
	}

	result := CompareFiles(blobFiles, rowFiles)
	result.InfoHash = infoHash

	return result, nil
}

func CheckBatch(ctx context.Context, q *dao.Query, infoHashes []protocol.ID) (Summary, error) {
	var summary Summary
	for _, h := range infoHashes {
		summary.TotalChecked++

		result, err := CheckTorrent(ctx, q, h)
		if err != nil {
			summary.Errors++
			continue
		}

		if result.Match {
			summary.Matches++
		} else {
			summary.Mismatches++
			summary.MismatchDetails = append(summary.MismatchDetails, result)
		}
	}

	return summary, nil
}

func CheckRandom(ctx context.Context, q *dao.Query, sampleSize int) (Summary, error) {
	var hashes []protocol.ID

	err := q.Torrent.UnderlyingDB().WithContext(ctx).
		Table("torrents").
		Select("t.info_hash").
		Joins("AS t INNER JOIN torrent_files tf ON t.info_hash = tf.info_hash").
		Where("t.files_data IS NOT NULL").
		Group("t.info_hash").
		Order("RANDOM()").
		Limit(sampleSize).
		Pluck("t.info_hash", &hashes).Error
	if err != nil {
		return Summary{}, fmt.Errorf("sampling torrents: %w", err)
	}

	return CheckBatch(ctx, q, hashes)
}
