// Package extfix implements the G1 e-only backfill: it re-canonicalizes the
// extension (`e`) stored inside each torrent's compressed files_data blob so that
// it equals model.FileExtensionFromPath(path) for every file.
//
// Crawl-path blobs were written with an empty `e` (the crawler dual-write never set
// TorrentFile.Extension); once torrent_files is DROPped the blob is the source of
// truth, so `e` must be correct/complete. This backfill reads and rewrites ONLY the
// files_data column (~16GB) — it never touches torrent_files, so it is
// DROP-order-independent. It is idempotent: a blob that is already canonical is left
// untouched (no UPDATE), and re-running over a fixed corpus rewrites nothing, which
// also makes it safely resumable after an interruption.
package extfix

import (
	"context"
	"fmt"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/gorm"
)

// Report counts the outcome of a backfill run (or dry-run).
type Report struct {
	Scanned int64 // blobs read + decoded
	Fixed   int64 // blobs rewritten (in --dry-run: blobs that WOULD be rewritten)
	Skipped int64 // blobs already canonical (no write)
	Errors  int64 // blobs that failed to decode or re-encode (left untouched)
}

// fixBlob applies the G1 canonicalization to one files_data blob. It returns the
// re-encoded blob and needsFix=true ONLY when at least one file's stored `e` diverges
// from model.FileExtensionFromPath(path) — i.e. an empty-but-fixable `e` (the
// crawl-path case) or a stale/wrong `e`. A blob that is already canonical (every `e`
// equals the path-derived value, including a legitimately empty `e` for an
// extension-less file) returns needsFix=false and is NOT rewritten.
//
// The re-encode uses blobmigration.SerializeFiles, which (post-G1 fix) derives `e`
// from the path. The operation is therefore idempotent: fixBlob over an
// already-fixed blob reports needsFix=false.
func fixBlob(data []byte) (newBlob []byte, needsFix bool, err error) {
	files, err := blobmigration.DeserializeFiles(data)
	if err != nil {
		return nil, false, err
	}

	for i := range files {
		if files[i].Extension.String != model.FileExtensionFromPath(files[i].Path).String {
			needsFix = true
			break
		}
	}

	if !needsFix {
		return nil, false, nil
	}

	out, err := blobmigration.SerializeFiles(files)
	if err != nil {
		return nil, false, err
	}

	return out, true, nil
}

type blobRow struct {
	InfoHash  protocol.ID
	FilesData []byte
}

// idRange is a disjoint (lower, upper] info_hash range processed by one worker.
type idRange struct {
	lower    protocol.ID
	hasLower bool
	upper    protocol.ID
	hasUpper bool
}

// computeRanges partitions the 20-byte info_hash space into k disjoint, gap-free
// (lower, upper] ranges by the leading byte (mirrors the backfill/verify partition).
func computeRanges(k int) []idRange {
	if k < 1 {
		k = 1
	}

	step := 256 / k
	if step < 1 {
		step = 1
	}

	out := make([]idRange, 0, k)

	for i := range k {
		var r idRange

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

// BackfillExtensions re-canonicalizes `e` in every files_data blob using `parallelism`
// keyset-streaming workers over disjoint info_hash ranges. Each chunk is rewritten in
// ONE transaction (per-batch COMMIT) and only the genuinely-non-canonical blobs are
// UPDATEd (content-based skip). When dryRun is true nothing is written and Report.Fixed
// counts what WOULD be rewritten (use it to size the load before the real run).
//
// limit > 0 caps the TOTAL blobs scanned (~limit/parallelism per range, ~uniform since
// info_hash is random) — use it for a bounded write smoke before the full run; limit <= 0
// processes everything.
//
// progress, if non-nil, is invoked after every chunk with a cumulative snapshot of the
// Report (serialized — safe to read/print).
func BackfillExtensions(
	ctx context.Context,
	q *dao.Query,
	parallelism, chunkSize, limit int,
	dryRun bool,
	progress func(Report),
) (Report, error) {
	if parallelism < 1 {
		parallelism = 1
	}

	if chunkSize < 1 {
		chunkSize = 2000
	}

	perRangeLimit := 0
	if limit > 0 {
		perRangeLimit = limit/parallelism + 1
	}

	var (
		mu       sync.Mutex
		rep      Report
		wg       sync.WaitGroup
		firstErr error
	)

	bump := func(d Report) {
		mu.Lock()
		defer mu.Unlock()

		rep.Scanned += d.Scanned
		rep.Fixed += d.Fixed
		rep.Skipped += d.Skipped
		rep.Errors += d.Errors

		if progress != nil {
			progress(rep)
		}
	}

	for _, rg := range computeRanges(parallelism) {
		wg.Add(1)

		go func(rg idRange) {
			defer wg.Done()

			if err := backfillRange(ctx, q, rg, chunkSize, perRangeLimit, dryRun, bump); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(rg)
	}

	wg.Wait()

	return rep, firstErr
}

func backfillRange(
	ctx context.Context,
	q *dao.Query,
	rg idRange,
	chunkSize, limit int,
	dryRun bool,
	bump func(Report),
) error {
	db := q.Torrent.UnderlyingDB().WithContext(ctx)

	cursor := rg.lower
	hasCursor := rg.hasLower

	var scanned int

	for {
		if ctx.Err() != nil {
			return ctx.Err()
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
			return fmt.Errorf("reading blob chunk: %w", err)
		}

		if len(blobs) == 0 {
			break
		}

		var (
			delta   Report
			pending []blobRow
		)

		for _, b := range blobs {
			delta.Scanned++

			newBlob, needsFix, err := fixBlob(b.FilesData)
			if err != nil {
				delta.Errors++
				continue
			}

			if !needsFix {
				delta.Skipped++
				continue
			}

			delta.Fixed++

			if !dryRun {
				pending = append(pending, blobRow{InfoHash: b.InfoHash, FilesData: newBlob})
			}
		}

		// One transaction per chunk (per-batch COMMIT) — bounds WAL growth.
		if len(pending) > 0 {
			if err := db.Transaction(func(tx *gorm.DB) error {
				for _, p := range pending {
					// Rewrite files_data but deliberately do NOT bump updated_at.
					// The L2 delta carve (bitmagnet-rs stream.rs: WHERE updated_at >
					// to_timestamp($1)) and the L3 pathsearch follow loop key on
					// updated_at; bumping it on every fixed blob would make the next
					// L2 tick re-carve the entire fixed set and the L3 loop re-index
					// it — a catastrophic, pointless mass re-index. It stays pointless
					// for the extension itself: the e-canonicalization is INVISIBLE to
					// those consumers because L2 (decode.rs) and L3 derive the
					// extension from the PATH, never the blob `e`.
					if err := tx.Exec(
						"UPDATE torrents SET files_data = ? WHERE info_hash = ?",
						p.FilesData, p.InfoHash,
					).Error; err != nil {
						return err
					}

					// The blob's BYTE LENGTH, however, IS now an L3 consumer via the
					// torrent_file_summary.compressed_bytes denorm (summary-first
					// refine_metadata). A re-encode can change the length, so keep the
					// denorm in sync in the same tx (WHERE matches nothing => no-op
					// when the torrent has no summary row). updated_at stays untouched:
					// nothing keys on the summary's timestamp, so this remains a silent
					// canonicalization, not a content update.
					if err := tx.Exec(
						"UPDATE torrent_file_summary SET compressed_bytes = ? WHERE info_hash = ?",
						len(p.FilesData), p.InfoHash,
					).Error; err != nil {
						return err
					}
				}

				return nil
			}); err != nil {
				return fmt.Errorf("updating chunk: %w", err)
			}
		}

		bump(delta)

		cursor = blobs[len(blobs)-1].InfoHash
		hasCursor = true

		scanned += len(blobs)
		if limit > 0 && scanned >= limit {
			break
		}

		if len(blobs) < chunkSize {
			break
		}
	}

	return nil
}
