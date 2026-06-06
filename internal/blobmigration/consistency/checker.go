package consistency

import (
	"context"
	"fmt"
	"sort"
	"sync"

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

// verifyRange is a disjoint info_hash range processed by one CheckAll worker.
type verifyRange struct {
	lower    protocol.ID
	hasLower bool
	upper    protocol.ID
	hasUpper bool
}

// verifyRanges partitions the 20-byte info_hash space into k disjoint, gap-free (lower, upper] ranges
// by the leading byte (mirrors the backfill's computeRanges).
func verifyRanges(k int) []verifyRange {
	if k < 1 {
		k = 1
	}

	step := 256 / k
	if step < 1 {
		step = 1
	}

	out := make([]verifyRange, 0, k)

	for i := range k {
		var r verifyRange

		if i > 0 {
			r.lower[0] = byte(i * step)
			r.hasLower = true
		}

		if i < k-1 {
			r.upper[0] = byte((i + 1) * step)
			r.hasUpper = true
		}

		out = append(out, r)
	}

	return out
}

// CheckAll verifies migrated torrents (files_data IS NOT NULL) against torrent_files using `parallelism`
// keyset-streaming workers over disjoint info_hash ranges. Unlike CheckRandom it does NO ORDER BY
// RANDOM, NO join, and NO per-torrent point queries: each worker streams torrents (the blob comes with
// the row) and reads each chunk's torrent_files in one bounded grouped query, comparing in memory.
// sampleSize<=0 verifies everything; sampleSize>0 caps the total checked (~sampleSize/k contiguous per
// range, ~uniform since info_hash is random).
func CheckAll(ctx context.Context, q *dao.Query, parallelism, chunkSize, sampleSize int) (Summary, error) {
	if parallelism < 1 {
		parallelism = 1
	}

	if chunkSize < 1 {
		chunkSize = 2000
	}

	perRangeLimit := 0
	if sampleSize > 0 {
		perRangeLimit = sampleSize/parallelism + 1
	}

	var (
		mu       sync.Mutex
		total    Summary
		wg       sync.WaitGroup
		firstErr error
	)

	for _, rg := range verifyRanges(parallelism) {
		wg.Add(1)

		go func(rg verifyRange) {
			defer wg.Done()

			local, err := checkRange(ctx, q, rg, chunkSize, perRangeLimit)

			mu.Lock()
			defer mu.Unlock()

			total.TotalChecked += local.TotalChecked
			total.Matches += local.Matches
			total.Mismatches += local.Mismatches
			total.Errors += local.Errors
			total.MismatchDetails = append(total.MismatchDetails, local.MismatchDetails...)

			if err != nil && firstErr == nil {
				firstErr = err
			}
		}(rg)
	}

	wg.Wait()

	return total, firstErr
}

func checkRange(ctx context.Context, q *dao.Query, rg verifyRange, chunkSize, limit int) (Summary, error) {
	var s Summary

	db := q.Torrent.UnderlyingDB().WithContext(ctx)

	cursor := rg.lower
	hasCursor := rg.hasLower

	for {
		if ctx.Err() != nil {
			return s, ctx.Err()
		}

		// Chunk of (info_hash, files_data) blobs — keyset, no random sort; the blob comes with the row.
		var blobs []blobRow

		tq := db.Table("torrents").
			Select("info_hash, files_data").
			Where("files_data IS NOT NULL").
			Order("info_hash").
			Limit(chunkSize)

		if hasCursor {
			tq = tq.Where("info_hash > ?", cursor)
		}

		if rg.hasUpper {
			tq = tq.Where("info_hash <= ?", rg.upper)
		}

		if err := tq.Scan(&blobs).Error; err != nil {
			return s, fmt.Errorf("reading blob chunk: %w", err)
		}

		if len(blobs) == 0 {
			break
		}

		maxHash := blobs[len(blobs)-1].InfoHash

		filesByHash, err := groupedFiles(ctx, q, cursor, hasCursor, maxHash)
		if err != nil {
			return s, err
		}

		for _, b := range blobs {
			s.TotalChecked++

			blobFiles, derr := blobmigration.DeserializeFiles(b.FilesData)
			if derr != nil {
				s.Errors++
				continue
			}

			res := CompareFiles(blobFiles, filesByHash[b.InfoHash])
			res.InfoHash = b.InfoHash

			if res.Match {
				s.Matches++
			} else {
				s.Mismatches++
				// Keep a bounded number of mismatch details for reporting.
				if len(s.MismatchDetails) < 100 {
					s.MismatchDetails = append(s.MismatchDetails, res)
				}
			}
		}

		cursor = maxHash
		hasCursor = true

		if limit > 0 && s.TotalChecked >= limit {
			break
		}

		if len(blobs) < chunkSize {
			break
		}
	}

	return s, nil
}

// groupedFiles reads torrent_files for info_hash in (lower, maxHash] in one ordered scan, grouped by
// info_hash (the chunk's torrents) — the same bounded read the backfill uses.
func groupedFiles(
	ctx context.Context,
	q *dao.Query,
	lower protocol.ID, hasLower bool,
	maxHash protocol.ID,
) (map[protocol.ID][]model.TorrentFile, error) {
	qq := q.TorrentFile.UnderlyingDB().WithContext(ctx).
		Table("torrent_files").
		Select(`info_hash, "index", path, size, extension`).
		Where("info_hash <= ?", maxHash).
		Order(`info_hash, "index"`)

	if hasLower {
		qq = qq.Where("info_hash > ?", lower)
	}

	rows, err := qq.Rows()
	if err != nil {
		return nil, fmt.Errorf("reading torrent_files chunk: %w", err)
	}

	defer func() { _ = rows.Close() }()

	out := make(map[protocol.ID][]model.TorrentFile)

	for rows.Next() {
		var f model.TorrentFile
		if err := q.TorrentFile.UnderlyingDB().ScanRows(rows, &f); err != nil {
			return nil, err
		}

		out[f.InfoHash] = append(out[f.InfoHash], f)
	}

	return out, rows.Err()
}
