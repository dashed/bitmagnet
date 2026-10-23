package main

// The canonical queue_jobs fields of flagson-dump (see flagson_dump.go for
// what is canonicalized and why).

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
)

const (
	// processTorrentQueue is processor.MessageName.
	processTorrentQueue = processor.MessageName
	// jobErrorPrefix is how queue/handler.Exec wraps a handler's error.
	jobErrorPrefix = "job failed to process: "
)

var (
	missingHashesLine = regexp.MustCompile(`^missing \d+ info hashes$`)
	ephemeralPort     = regexp.MustCompile(`\b(read|write) tcp 127\.0\.0\.1:\d+->`)
)

type canonStats struct {
	rows, reordered, unverified, errorsReordered, ports int
}

func (s canonStats) String() string {
	return fmt.Sprintf("%d rows: %d payloads reordered (fingerprint recomputed), %d unverified, "+
		"%d errors reordered, %d ports masked", s.rows, s.reordered, s.unverified, s.errorsReordered, s.ports)
}

// canonicalHashes keeps the leading hashes that are missing (absent from
// torrents and deleted_torrents) and sorts the rest.
func canonicalHashes(hashes []protocol.ID, missing func(protocol.ID) bool) []protocol.ID {
	head := 0
	for head < len(hashes) && missing(hashes[head]) {
		head++
	}

	out := append([]protocol.ID(nil), hashes...)
	slices.SortFunc(out[head:], func(a, b protocol.ID) int { return strings.Compare(a.String(), b.String()) })

	return out
}

// canonicalJobError sorts the joined error lines after a leading "missing N
// info hashes" line, under the handler's prefix, and masks the client port of
// a reset.
func canonicalJobError(text string) (canonical string, reordered, masked bool) {
	body, prefixed := strings.CutPrefix(text, jobErrorPrefix)
	lines := strings.Split(body, "\n")

	head := 0
	if missingHashesLine.MatchString(lines[0]) {
		head = 1
	}

	slices.Sort(lines[head:])

	out := strings.Join(lines, "\n")
	if prefixed {
		out = jobErrorPrefix + out
	}

	canonical = ephemeralPort.ReplaceAllString(out, "$1 tcp 127.0.0.1:<port>->")

	return canonical, out != text, canonical != out
}

// canonicalPayload returns the canonical payload and fingerprint of a job the
// processor queued, or the stored fingerprint marked unverified when it is not
// processor.NewQueueJob's for the stored payload.
func canonicalPayload(
	fingerprint, payload string,
	missing func(protocol.ID) bool,
) (canonicalFingerprint, canonicalText string, verified bool, err error) {
	var params processor.MessageParams
	if err := json.Unmarshal([]byte(payload), &params); err != nil {
		return "", "", false, err
	}

	stored, err := processor.NewQueueJob(params)
	if err != nil {
		return "", "", false, err
	}

	if stored.Fingerprint != fingerprint {
		return "unverified:" + fingerprint, payload, false, nil
	}

	params.InfoHashes = canonicalHashes(params.InfoHashes, missing)

	canonical, err := processor.NewQueueJob(params)
	if err != nil {
		return "", "", false, err
	}

	return canonical.Fingerprint, canonical.Payload, true, nil
}

// canonicalQueueJobs creates canonicalQueueJobsView: queue_jobs with
// fingerprint, payload and error in canonical form. Jobs whose fingerprint is
// in seeded are the seed's own and keep theirs.
func canonicalQueueJobs(ctx context.Context, conn *pgx.Conn, seeded map[string]bool) (canonStats, error) {
	var stats canonStats

	present := map[protocol.ID]bool{}

	rows, err := conn.Query(ctx, `SELECT info_hash FROM torrents UNION SELECT info_hash FROM deleted_torrents`)
	if err != nil {
		return stats, err
	}

	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return stats, err
		}

		present[protocol.ID(raw)] = true
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return stats, err
	}

	missing := func(h protocol.ID) bool { return !present[h] }

	type canonRow struct {
		id, fingerprint, payload string
		jobError                 *string
	}

	var canon []canonRow

	rows, err = conn.Query(ctx, `SELECT id::text, fingerprint, queue, payload::text, error FROM queue_jobs`)
	if err != nil {
		return stats, err
	}

	for rows.Next() {
		var (
			r     canonRow
			queue string
		)

		if err := rows.Scan(&r.id, &r.fingerprint, &queue, &r.payload, &r.jobError); err != nil {
			rows.Close()
			return stats, err
		}

		canon = append(canon, r)

		if queue != processTorrentQueue || seeded[r.fingerprint] {
			continue
		}

		fp, payload, verified, err := canonicalPayload(r.fingerprint, r.payload, missing)
		if err != nil {
			rows.Close()
			return stats, fmt.Errorf("queue job %s: %w", r.id, err)
		}

		switch {
		case !verified:
			stats.unverified++
		case fp != r.fingerprint:
			stats.reordered++
		}

		canon[len(canon)-1].fingerprint, canon[len(canon)-1].payload = fp, payload
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return stats, err
	}

	for i := range canon {
		if canon[i].jobError == nil {
			continue
		}

		text, reordered, masked := canonicalJobError(*canon[i].jobError)
		canon[i].jobError = &text

		if reordered {
			stats.errorsReordered++
		}

		if masked {
			stats.ports++
		}
	}

	stats.rows = len(canon)

	if _, err := conn.Exec(ctx, `CREATE TEMPORARY TABLE flagson_canonical_queue_jobs
		(id text PRIMARY KEY, fingerprint text NOT NULL, payload text NOT NULL, error text)`); err != nil {
		return stats, err
	}

	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"flagson_canonical_queue_jobs"},
		[]string{"id", "fingerprint", "payload", "error"},
		pgx.CopyFromSlice(len(canon), func(i int) ([]any, error) {
			return []any{canon[i].id, canon[i].fingerprint, canon[i].payload, canon[i].jobError}, nil
		})); err != nil {
		return stats, err
	}

	columns, err := tableColumns(ctx, conn, "queue_jobs")
	if err != nil {
		return stats, err
	}

	exprs := make([]string, len(columns))

	for i, c := range columns {
		col := pgx.Identifier{c}.Sanitize()

		switch c {
		case "fingerprint", "error":
			exprs[i] = "c." + col
		case "payload":
			exprs[i] = "c.payload::jsonb AS payload"
		default:
			exprs[i] = "q." + col
		}
	}

	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TEMPORARY VIEW %s AS SELECT %s
		FROM public.queue_jobs q JOIN flagson_canonical_queue_jobs c ON c.id = q.id::text`,
		pgx.Identifier{canonicalQueueJobsView}.Sanitize(), strings.Join(exprs, ", ")))

	return stats, err
}
