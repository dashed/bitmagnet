// Package bytesfill implements the one-shot backfill for
// torrent_file_summary.compressed_bytes (migration 00026). It set-based UPDATEs
// each summary row's compressed_bytes to octet_length(torrents.files_data) so the
// L3 refine path can read the compressed blob size straight from the summary,
// without probing the torrents heap.
//
// The write path (dhtcrawler + blob-migration flushChunk) already stamps
// compressed_bytes on every summary it writes; this backfill only fills the rows
// that predate migration 00026. It is INERT unless explicitly invoked via the
// `blob-migration backfill-bytes` CLI subcommand.
//
// It mirrors the extfix backfill: `parallelism` keyset-streaming workers over
// disjoint info_hash ranges, one transaction per chunk (bounding WAL growth). It
// is idempotent and resumable — the scan and the UPDATE both filter
// `compressed_bytes IS NULL`, and each chunk advances a positional cursor, so a
// re-run rewrites nothing already filled. octet_length reads the TOAST header
// only; the blob is never detoasted. Torrents with a NULL files_data (blob-less)
// keep a NULL compressed_bytes — the Rust read path treats those as misses.
package bytesfill

import (
	"context"
	"fmt"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/gorm"
)

// DefaultChunkSize is the number of summary rows filled (and committed) per chunk.
// 50k keeps each transaction's WAL bounded while amortising the per-chunk keyset
// scan over a large batch.
const DefaultChunkSize = 50_000

// Report counts the outcome of a backfill run (or dry-run).
type Report struct {
	Scanned int64 // summary rows examined (compressed_bytes was NULL)
	Updated int64 // rows written by the set-based UPDATE (RowsAffected); == Scanned in --dry-run
}

// idRange is a disjoint (lower, upper] info_hash range processed by one worker.
type idRange struct {
	lower    protocol.ID
	hasLower bool
	upper    protocol.ID
	hasUpper bool
}

// computeRanges partitions the 20-byte info_hash space into k disjoint, gap-free
// (lower, upper] ranges by the leading byte (mirrors the extfix/blob-migration
// partition so the workers cover the whole keyspace without overlap).
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

// BackfillCompressedBytes fills torrent_file_summary.compressed_bytes over the
// whole keyspace using `parallelism` keyset-streaming workers on disjoint
// info_hash ranges. Each chunk is committed in ONE transaction.
//
// limit > 0 caps the TOTAL summary rows scanned (~limit/parallelism per range,
// ~uniform since info_hash is random) — use it for a bounded smoke before the full
// run; limit <= 0 processes everything.
//
// progress, if non-nil, is invoked after every chunk with a cumulative snapshot of
// the Report (serialized — safe to read/print).
func BackfillCompressedBytes(
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
		chunkSize = DefaultChunkSize
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
		rep.Updated += d.Updated

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

		// Next chunk of still-NULL summary rows, ordered by the PK. The cursor sits
		// at the fill frontier so the rows just ahead are all NULL (stays an index
		// scan with the LIMIT pushed down). Advancing by the returned max hash means
		// blob-less rows (whose compressed_bytes stays NULL) are still skipped past
		// on the next iteration, guaranteeing forward progress.
		var hashes []protocol.ID

		sq := db.Table("torrent_file_summary").
			Select("info_hash").
			Where("compressed_bytes IS NULL").
			Order("info_hash").
			Limit(chunkSize)

		if hasCursor {
			sq = sq.Where("info_hash > ?", cursor)
		}

		if rg.hasUpper {
			sq = sq.Where("info_hash <= ?", rg.upper)
		}

		if err := sq.Pluck("info_hash", &hashes).Error; err != nil {
			return fmt.Errorf("scanning summary chunk: %w", err)
		}

		if len(hashes) == 0 {
			break
		}

		maxHash := hashes[len(hashes)-1]

		delta := Report{Scanned: int64(len(hashes))}

		if dryRun {
			// Every scanned NULL summary row has a matching torrent (FK), so the
			// UPDATE would touch all of them; report that as the would-write count.
			delta.Updated = int64(len(hashes))
		} else {
			affected, err := fillChunk(ctx, db, cursor, hasCursor, maxHash)
			if err != nil {
				return err
			}

			delta.Updated = affected
		}

		bump(delta)

		cursor = maxHash
		hasCursor = true

		scanned += len(hashes)
		if limit > 0 && scanned >= limit {
			break
		}

		if len(hashes) < chunkSize {
			break
		}
	}

	return nil
}

// fillChunk runs the set-based UPDATE for the (lower, maxHash] window in ONE
// transaction and returns the rows affected. The WHERE keeps `compressed_bytes IS
// NULL` so a re-run (or a concurrent writer that already stamped a row) is a no-op.
func fillChunk(
	ctx context.Context,
	db *gorm.DB,
	lower protocol.ID,
	hasLower bool,
	maxHash protocol.ID,
) (int64, error) {
	var affected int64

	err := db.Transaction(func(tx *gorm.DB) error {
		var b []any

		sql := "UPDATE torrent_file_summary s SET compressed_bytes = octet_length(t.files_data) " +
			"FROM torrents t WHERE s.info_hash = t.info_hash AND s.compressed_bytes IS NULL " +
			"AND s.info_hash <= ?"
		b = append(b, maxHash)

		if hasLower {
			sql += " AND s.info_hash > ?"
			b = append(b, lower)
		}

		res := tx.Exec(sql, b...)
		if res.Error != nil {
			return res.Error
		}

		affected = res.RowsAffected

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("filling summary chunk: %w", err)
	}

	return affected, nil
}
