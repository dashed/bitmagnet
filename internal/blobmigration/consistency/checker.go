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
	InfoHash                 protocol.ID
	Match                    bool
	BlobFiles                int
	RowFiles                 int
	LegacyDuplicatePathFiles int
	Mismatches               []FieldMismatch
}

type FieldMismatch struct {
	FileIndex int
	Field     string
	Expected  string
	Got       string
}

type Summary struct {
	TotalChecked                int
	Matches                     int
	Mismatches                  int
	Errors                      int
	LegacyDuplicatePathTorrents int
	LegacyDuplicatePathFiles    int
	MismatchDetails             []CheckResult
}

func boundLabel(h protocol.ID, ok bool, fallback string) string {
	if !ok {
		return fallback
	}

	return h.String()
}

func rangeLabel(rg verifyRange) string {
	return fmt.Sprintf(
		"(%s,%s]",
		boundLabel(rg.lower, rg.hasLower, "-inf"),
		boundLabel(rg.upper, rg.hasUpper, "+inf"),
	)
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

		// Extension is checked as the PATH-DERIVED value on both sides, never the
		// raw stored field (FB-A0/G1): crawl-path blobs legitimately carry an
		// empty `e`, and the torrent_files.extension column is itself
		// path-derived (a generated column). Comparing b.Extension to r.Extension
		// would therefore flag every crawl-path torrent as a false mismatch. The
		// contract every consumer must honour is "extension := FileExtensionFromPath(path)",
		// so that is exactly what we verify here. Because paths are compared above,
		// this also catches a path that parses to a different extension after the
		// blob round-trip.
		bExt := model.FileExtensionFromPath(b.Path)
		rExt := model.FileExtensionFromPath(r.Path)

		if bExt.String != rExt.String {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: i,
				Field:     "extension",
				Expected:  rExt.String,
				Got:       bExt.String,
			})
		}
	}

	result.Match = len(result.Mismatches) == 0

	return result
}

// CompareFileIndexSet checks the file-index set while allowing only the legacy
// torrent_files duplicate-path collapse. Legacy torrent_files is keyed by
// (info_hash, path), unique on (info_hash, index), and inserted with
// ON CONFLICT DO NOTHING. A malformed torrent with duplicate paths can therefore
// have more blob indexes than torrent_files rows, but that is not a DROP safety
// failure when each missing blob index has the same path as a retained legacy
// row. Arbitrary row-only/blob-only indexes and row/blob path or size
// disagreement still fail the gate.
func CompareFileIndexSet(blobFiles, rowFiles []model.TorrentFile) CheckResult {
	result := CheckResult{
		BlobFiles: len(blobFiles),
		RowFiles:  len(rowFiles),
	}

	blobIndexes := make(map[uint]int, len(blobFiles))
	blobByIndex := make(map[uint]model.TorrentFile, len(blobFiles))
	blobPathCounts := make(map[string]int, len(blobFiles))
	for _, f := range blobFiles {
		blobIndexes[f.Index]++
		blobByIndex[f.Index] = f
		blobPathCounts[f.Path]++
	}

	rowIndexes := make(map[uint]int, len(rowFiles))
	rowByIndex := make(map[uint]model.TorrentFile, len(rowFiles))
	rowPaths := make(map[string]struct{}, len(rowFiles))
	for _, f := range rowFiles {
		rowIndexes[f.Index]++
		rowByIndex[f.Index] = f
		rowPaths[f.Path] = struct{}{}
	}

	for index, count := range blobIndexes {
		if count > 1 {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: int(index),
				Field:     "duplicate_blob_index",
				Expected:  "1",
				Got:       fmt.Sprintf("%d", count),
			})
		}
	}

	for index, count := range rowIndexes {
		if count > 1 {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: int(index),
				Field:     "duplicate_row_index",
				Expected:  "1",
				Got:       fmt.Sprintf("%d", count),
			})
		}
	}

	var sortedRows []int
	for index := range rowIndexes {
		sortedRows = append(sortedRows, int(index))
	}
	sort.Ints(sortedRows)

	for _, index := range sortedRows {
		row := rowByIndex[uint(index)]
		blob, ok := blobByIndex[uint(index)]
		if !ok {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: index,
				Field:     "missing_in_blob",
				Expected:  fmt.Sprintf("%d", index),
				Got:       "",
			})
			continue
		}

		if blob.Path != row.Path {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: index,
				Field:     "path",
				Expected:  row.Path,
				Got:       blob.Path,
			})
		}

		if blob.Size != row.Size {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: index,
				Field:     "size",
				Expected:  fmt.Sprintf("%d", row.Size),
				Got:       fmt.Sprintf("%d", blob.Size),
			})
		}
	}

	var sortedBlobs []int
	for index := range blobIndexes {
		sortedBlobs = append(sortedBlobs, int(index))
	}
	sort.Ints(sortedBlobs)

	for _, index := range sortedBlobs {
		blob := blobByIndex[uint(index)]
		if _, ok := rowIndexes[uint(index)]; ok {
			continue
		}

		if _, ok := rowPaths[blob.Path]; ok && blobPathCounts[blob.Path] > 1 {
			result.LegacyDuplicatePathFiles++
			continue
		}

		result.Mismatches = append(result.Mismatches, FieldMismatch{
			FileIndex: index,
			Field:     "missing_in_torrent_files",
			Expected:  "",
			Got:       fmt.Sprintf("%d", index),
		})
	}

	result.Match = len(result.Mismatches) == 0

	return result
}

// compareStrictE asserts the G1 canonicalization invariant on a single blob: the
// RAW stored `e` of every file must equal model.FileExtensionFromPath(path). It
// needs only the blob (no torrent_files), so it remains valid after the DROP. A
// crawl-path blob written before the G1 serializer fix (empty `e` on a file whose
// path yields a non-empty extension) is flagged here; a fully-canonical blob passes.
func compareStrictE(blobFiles []model.TorrentFile) CheckResult {
	result := CheckResult{
		BlobFiles: len(blobFiles),
		RowFiles:  len(blobFiles),
	}

	for i := range blobFiles {
		b := blobFiles[i]

		want := model.FileExtensionFromPath(b.Path)
		if b.Extension.String != want.String {
			result.Mismatches = append(result.Mismatches, FieldMismatch{
				FileIndex: int(b.Index),
				Field:     "extension_raw",
				Expected:  want.String,
				Got:       b.Extension.String,
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

		addLegacyDuplicatePathCounts(&summary, result)
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
// sampleSize<=0 verifies everything with duplicate-path-aware DROP-gate semantics; sampleSize>0 caps
// the total checked (~sampleSize/k contiguous per range, ~uniform since info_hash is random) and keeps
// the historical strict comparator because sampled checks are diagnostics, not the D1 gate.
func CheckAll(ctx context.Context, q *dao.Query, parallelism, chunkSize, sampleSize int) (Summary, error) {
	return checkAllImpl(ctx, q, parallelism, chunkSize, sampleSize, false, sampleSize <= 0)
}

// CheckAllStrictE is the G1 strict-`e` gate. It asserts the RAW stored blob `e`
// equals the path-derived extension (model.FileExtensionFromPath(path)) for every
// file in every blob — i.e. that the backfill canonicalized `e` and the crawler is
// no longer writing empty `e`. Unlike CheckAll it reads ONLY files_data (no
// torrent_files read/join), so it is torrent_files-INDEPENDENT and stays valid
// after the DROP. 0 mismatches + 0 errors == the G1 gate is green.
func CheckAllStrictE(ctx context.Context, q *dao.Query, parallelism, chunkSize, sampleSize int) (Summary, error) {
	return checkAllImpl(ctx, q, parallelism, chunkSize, sampleSize, true, false)
}

// CheckAllFileIndexSet is the read-only crawl-path parity gate for retained
// torrent_files rows against decoded L1 blobs, with only the legacy duplicate
// path collapse allowed. It reads blobs and torrent_files in disjoint info_hash
// ranges, uses keyset pagination, and never writes verification metadata.
func CheckAllFileIndexSet(ctx context.Context, q *dao.Query, parallelism, chunkSize int) (Summary, error) {
	if parallelism < 1 {
		parallelism = 1
	}

	if chunkSize < 1 {
		chunkSize = 2000
	}

	var (
		mu       sync.Mutex
		total    Summary
		wg       sync.WaitGroup
		firstErr error
	)

	for rangeIndex, rg := range verifyRanges(parallelism) {
		wg.Add(1)

		go func(rangeIndex int, rg verifyRange) {
			defer wg.Done()

			local, err := checkRangeFileIndexSet(ctx, q, rangeIndex, rg, chunkSize)

			mu.Lock()
			defer mu.Unlock()

			total.TotalChecked += local.TotalChecked
			total.Matches += local.Matches
			total.Mismatches += local.Mismatches
			total.Errors += local.Errors
			total.LegacyDuplicatePathTorrents += local.LegacyDuplicatePathTorrents
			total.LegacyDuplicatePathFiles += local.LegacyDuplicatePathFiles
			total.MismatchDetails = append(total.MismatchDetails, local.MismatchDetails...)

			if err != nil && firstErr == nil {
				firstErr = err
			}
		}(rangeIndex, rg)
	}

	wg.Wait()

	return total, firstErr
}

func checkAllImpl(
	ctx context.Context,
	q *dao.Query,
	parallelism, chunkSize, sampleSize int,
	strictE bool,
	duplicatePathAware bool,
) (Summary, error) {
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

	for rangeIndex, rg := range verifyRanges(parallelism) {
		wg.Add(1)

		go func(rangeIndex int, rg verifyRange) {
			defer wg.Done()

			local, err := checkRange(ctx, q, rangeIndex, rg, chunkSize, perRangeLimit, strictE, duplicatePathAware)

			mu.Lock()
			defer mu.Unlock()

			total.TotalChecked += local.TotalChecked
			total.Matches += local.Matches
			total.Mismatches += local.Mismatches
			total.Errors += local.Errors
			total.LegacyDuplicatePathTorrents += local.LegacyDuplicatePathTorrents
			total.LegacyDuplicatePathFiles += local.LegacyDuplicatePathFiles
			total.MismatchDetails = append(total.MismatchDetails, local.MismatchDetails...)

			if err != nil && firstErr == nil {
				firstErr = err
			}
		}(rangeIndex, rg)
	}

	wg.Wait()

	return total, firstErr
}

func checkRange(
	ctx context.Context,
	q *dao.Query,
	rangeIndex int,
	rg verifyRange,
	chunkSize, limit int,
	strictE bool,
	duplicatePathAware bool,
) (Summary, error) {
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
			return s, fmt.Errorf(
				"reading blob chunk range_index=%d range=%s cursor=%s chunk_size=%d: %w",
				rangeIndex,
				rangeLabel(rg),
				boundLabel(cursor, hasCursor, "-inf"),
				chunkSize,
				err,
			)
		}

		if len(blobs) == 0 {
			break
		}

		maxHash := blobs[len(blobs)-1].InfoHash

		// strict-e is blob-only (asserts raw `e` == path-derived); it does NOT read
		// torrent_files, so it stays valid after the DROP. The default check still
		// cross-references torrent_files for path/size/count.
		var filesByHash map[protocol.ID][]model.TorrentFile

		if !strictE {
			var err error

			filesByHash, err = groupedFiles(ctx, q, cursor, hasCursor, maxHash)
			if err != nil {
				return s, fmt.Errorf(
					"reading grouped torrent_files range_index=%d range=%s cursor=%s max_hash=%s checked=%d: %w",
					rangeIndex,
					rangeLabel(rg),
					boundLabel(cursor, hasCursor, "-inf"),
					maxHash.String(),
					s.TotalChecked,
					err,
				)
			}
		}

		for _, b := range blobs {
			s.TotalChecked++

			blobFiles, derr := blobmigration.DeserializeFiles(b.FilesData)
			if derr != nil {
				s.Errors++
				return s, fmt.Errorf(
					"deserializing files_data range_index=%d range=%s info_hash=%s cursor=%s max_hash=%s checked=%d: %w",
					rangeIndex,
					rangeLabel(rg),
					b.InfoHash.String(),
					boundLabel(cursor, hasCursor, "-inf"),
					maxHash.String(),
					s.TotalChecked,
					derr,
				)
			}

			var res CheckResult
			if strictE {
				res = compareStrictE(blobFiles)
			} else if duplicatePathAware {
				res = CompareFileIndexSet(blobFiles, filesByHash[b.InfoHash])
			} else {
				res = CompareFiles(blobFiles, filesByHash[b.InfoHash])
			}

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

			addLegacyDuplicatePathCounts(&s, res)
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

func checkRangeFileIndexSet(ctx context.Context, q *dao.Query, rangeIndex int, rg verifyRange, chunkSize int) (Summary, error) {
	var s Summary

	db := q.Torrent.UnderlyingDB().WithContext(ctx)

	cursor := rg.lower
	hasCursor := rg.hasLower

	for {
		if ctx.Err() != nil {
			return s, ctx.Err()
		}

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
			return s, fmt.Errorf(
				"reading file-index blob chunk range_index=%d range=%s cursor=%s chunk_size=%d: %w",
				rangeIndex,
				rangeLabel(rg),
				boundLabel(cursor, hasCursor, "-inf"),
				chunkSize,
				err,
			)
		}

		if len(blobs) == 0 {
			break
		}

		maxHash := blobs[len(blobs)-1].InfoHash

		filesByHash, err := groupedFileIndexes(ctx, q, cursor, hasCursor, maxHash, true)
		if err != nil {
			return s, fmt.Errorf(
				"reading grouped file indexes range_index=%d range=%s cursor=%s max_hash=%s checked=%d: %w",
				rangeIndex,
				rangeLabel(rg),
				boundLabel(cursor, hasCursor, "-inf"),
				maxHash.String(),
				s.TotalChecked,
				err,
			)
		}

		if err := compareFileIndexChunk(&s, blobs, filesByHash, rangeIndex, rg, cursor, hasCursor, maxHash); err != nil {
			return s, err
		}

		cursor = maxHash
		hasCursor = true

		if len(blobs) < chunkSize {
			break
		}
	}

	tailRows, err := groupedFileIndexes(ctx, q, cursor, hasCursor, rg.upper, rg.hasUpper)
	if err != nil {
		return s, fmt.Errorf(
			"reading grouped file-index tail range_index=%d range=%s cursor=%s upper=%s checked=%d: %w",
			rangeIndex,
			rangeLabel(rg),
			boundLabel(cursor, hasCursor, "-inf"),
			boundLabel(rg.upper, rg.hasUpper, "+inf"),
			s.TotalChecked,
			err,
		)
	}

	addRowOnlyIndexMismatches(&s, tailRows)

	return s, nil
}

func compareFileIndexChunk(
	s *Summary,
	blobs []blobRow,
	filesByHash map[protocol.ID][]model.TorrentFile,
	rangeIndex int,
	rg verifyRange,
	cursor protocol.ID,
	hasCursor bool,
	maxHash protocol.ID,
) error {
	for _, b := range blobs {
		s.TotalChecked++

		blobFiles, derr := blobmigration.DeserializeFiles(b.FilesData)
		if derr != nil {
			s.Errors++
			return fmt.Errorf(
				"deserializing file-index files_data range_index=%d range=%s info_hash=%s cursor=%s max_hash=%s checked=%d: %w",
				rangeIndex,
				rangeLabel(rg),
				b.InfoHash.String(),
				boundLabel(cursor, hasCursor, "-inf"),
				maxHash.String(),
				s.TotalChecked,
				derr,
			)
		}

		res := CompareFileIndexSet(blobFiles, filesByHash[b.InfoHash])
		res.InfoHash = b.InfoHash
		delete(filesByHash, b.InfoHash)

		if res.Match {
			s.Matches++
		} else {
			s.Mismatches++
			if len(s.MismatchDetails) < 100 {
				s.MismatchDetails = append(s.MismatchDetails, res)
			}
		}

		addLegacyDuplicatePathCounts(s, res)
	}

	addRowOnlyIndexMismatches(s, filesByHash)

	return nil
}

func addLegacyDuplicatePathCounts(s *Summary, res CheckResult) {
	if res.LegacyDuplicatePathFiles == 0 {
		return
	}

	s.LegacyDuplicatePathTorrents++
	s.LegacyDuplicatePathFiles += res.LegacyDuplicatePathFiles
}

func addRowOnlyIndexMismatches(s *Summary, filesByHash map[protocol.ID][]model.TorrentFile) {
	if len(filesByHash) == 0 {
		return
	}

	hashes := make([]protocol.ID, 0, len(filesByHash))
	for h := range filesByHash {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return string(hashes[i][:]) < string(hashes[j][:])
	})

	for _, h := range hashes {
		rows := filesByHash[h]
		s.TotalChecked++
		s.Mismatches++

		if len(s.MismatchDetails) < 100 {
			res := CompareFileIndexSet(nil, rows)
			res.InfoHash = h
			s.MismatchDetails = append(s.MismatchDetails, res)
		}
	}
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
		return nil, fmt.Errorf(
			"opening torrent_files chunk lower=%s max_hash=%s: %w",
			boundLabel(lower, hasLower, "-inf"),
			maxHash.String(),
			err,
		)
	}

	defer func() { _ = rows.Close() }()

	out := make(map[protocol.ID][]model.TorrentFile)
	rowsRead := 0
	var lastHash protocol.ID
	hasLastHash := false

	for rows.Next() {
		var f model.TorrentFile
		if err := q.TorrentFile.UnderlyingDB().ScanRows(rows, &f); err != nil {
			return nil, fmt.Errorf(
				"scanning torrent_files row lower=%s max_hash=%s rows_read=%d last_info_hash=%s: %w",
				boundLabel(lower, hasLower, "-inf"),
				maxHash.String(),
				rowsRead,
				boundLabel(lastHash, hasLastHash, "(none)"),
				err,
			)
		}

		out[f.InfoHash] = append(out[f.InfoHash], f)
		lastHash = f.InfoHash
		hasLastHash = true
		rowsRead++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterating torrent_files rows lower=%s max_hash=%s rows_read=%d last_info_hash=%s: %w",
			boundLabel(lower, hasLower, "-inf"),
			maxHash.String(),
			rowsRead,
			boundLabel(lastHash, hasLastHash, "(none)"),
			err,
		)
	}

	return out, nil
}

func groupedFileIndexes(
	ctx context.Context,
	q *dao.Query,
	lower protocol.ID, hasLower bool,
	upper protocol.ID, hasUpper bool,
) (map[protocol.ID][]model.TorrentFile, error) {
	qq := q.TorrentFile.UnderlyingDB().WithContext(ctx).
		Table("torrent_files").
		Select(`info_hash, "index", path, size`).
		Order(`info_hash, "index"`)

	if hasLower {
		qq = qq.Where("info_hash > ?", lower)
	}

	if hasUpper {
		qq = qq.Where("info_hash <= ?", upper)
	}

	rows, err := qq.Rows()
	if err != nil {
		return nil, fmt.Errorf(
			"opening torrent_files indexes lower=%s upper=%s: %w",
			boundLabel(lower, hasLower, "-inf"),
			boundLabel(upper, hasUpper, "+inf"),
			err,
		)
	}

	defer func() { _ = rows.Close() }()

	out := make(map[protocol.ID][]model.TorrentFile)
	rowsRead := 0
	var lastHash protocol.ID
	hasLastHash := false

	for rows.Next() {
		var f model.TorrentFile
		if err := q.TorrentFile.UnderlyingDB().ScanRows(rows, &f); err != nil {
			return nil, fmt.Errorf(
				"scanning torrent_files index row lower=%s upper=%s rows_read=%d last_info_hash=%s: %w",
				boundLabel(lower, hasLower, "-inf"),
				boundLabel(upper, hasUpper, "+inf"),
				rowsRead,
				boundLabel(lastHash, hasLastHash, "(none)"),
				err,
			)
		}

		out[f.InfoHash] = append(out[f.InfoHash], f)
		lastHash = f.InfoHash
		hasLastHash = true
		rowsRead++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterating torrent_files index rows lower=%s upper=%s rows_read=%d last_info_hash=%s: %w",
			boundLabel(lower, hasLower, "-inf"),
			boundLabel(upper, hasUpper, "+inf"),
			rowsRead,
			boundLabel(lastHash, hasLastHash, "(none)"),
			err,
		)
	}

	return out, nil
}
