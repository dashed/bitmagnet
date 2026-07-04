package pathsearch

import (
	"context"
	"sort"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

const (
	FileRowSortSize      = "size"
	FileRowSortPath      = "path"
	FileRowSortExtension = "extension"
	FileRowSortIndex     = "index"
	FileRowSortInfoHash  = "info_hash"
)

// FileRowSort is a file-level ordering requested by a fileSearch caller. The
// first recognized field wins; an empty list defaults to size DESC.
type FileRowSort struct {
	Field      string
	Descending bool
}

// FileRow is one exact-refined matching file row.
type FileRow struct {
	InfoHash  protocol.ID
	Index     uint
	Path      string
	Extension string
	Size      uint64
}

// FileRowsResult is the fileSearch-shaped result produced from the pathsearch
// route. TotalCount is the L3 sidecar candidate_total upper-bound estimate, not
// an exact file count.
type FileRowsResult struct {
	Rows                 []FileRow
	TotalCount           uint
	TotalCountIsEstimate bool
	HasNextPage          bool
}

type matchingFileVisitor func(search.TorrentContentResultItem, model.TorrentFile) bool

type typeaheadBucket struct {
	text      string
	firstSeen int
	torrents  map[protocol.ID]struct{}
}

func pageCandidateLimit(limit uint) uint {
	if limit == ^uint(0) {
		return limit
	}

	return limit + 1
}

// visitMatchingFiles runs the shared L3 candidates + chunked L1 exact-refine
// pipeline and invokes visit for every matching file. It is the adapter core for
// file rows and path typeahead; existing torrent collapse/search behavior stays
// on its current methods.
func (c *Composer) visitMatchingFiles(
	ctx context.Context,
	f Filters,
	opts QueryOptions,
	limit, offset uint,
	sorts []*pb.SortBy,
	visit matchingFileVisitor,
) (candidateTotal uint, served bool, err error) {
	pred := f.predicate()
	if pred.substr == "" || !c.Eligible(f.Query) {
		c.metrics.IncRoute(RouteIneligible)

		return 0, false, nil
	}

	ctx, cancel := context.WithTimeout(ctx, c.routeTimeout)
	defer cancel()

	ids, candidateTotal, err := c.candidates(ctx, f, limit, offset, sorts)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return 0, false, err
	}

	if len(ids) == 0 {
		if c.trustEmpty() {
			c.metrics.IncRoute(RouteServed)

			return candidateTotal, true, nil
		}

		c.metrics.IncRoute(RouteFallback)

		if c.logger != nil {
			c.logger.Warnw("pathsearch: zero L3 file-row candidates while L3 unhealthy; falling back",
				"query", f.Query)
		}

		return 0, false, nil
	}

	counts, err := c.pg.FileCounts(ctx, ids)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return 0, false, err
	}

	keptIDs := c.declineOversized(ids, counts)
	if len(keptIDs) == 0 {
		c.metrics.IncRoute(RouteServed)

		return candidateTotal, true, nil
	}

	chunks := c.chunkByFileBudget(keptIDs, counts)
	if len(chunks) > 1 {
		if !c.acquireRefineSlot(ctx) {
			c.metrics.IncRefineShed()

			if c.logger != nil {
				c.logger.Warnw(
					"pathsearch: file-row refine concurrency slot unavailable; shedding (serving empty estimate)",
					"query",
					f.Query,
					"slot_wait",
					c.slotWait,
				)
			}

			c.metrics.IncRoute(RouteServed)

			return candidateTotal, true, nil
		}
		defer c.sem.Release(1)
	}

	capRsn := capNone

refine:
	for _, chunk := range chunks {
		if isRouteDeadline(ctx) {
			capRsn = capDeadline

			break
		}

		items, _, qErr := c.chunkRows(ctx, opts.refineOptions(), chunk)
		if qErr != nil {
			if isRouteDeadline(ctx) {
				capRsn = capDeadline

				break
			}

			c.metrics.IncRoute(RouteError)

			return 0, false, qErr
		}

		for i := range items {
			if isRouteDeadline(ctx) {
				capRsn = capDeadline

				break refine
			}

			item := items[i]

			files, fok := filesForRefine(item.Torrent)
			if !fok {
				c.metrics.IncRoute(RouteFallback)

				if c.logger != nil {
					c.logger.Warnw("pathsearch: file-row candidate files unobtainable; falling back",
						"query", f.Query)
				}

				return 0, false, nil
			}

			if len(files) > c.maxRefineFiles {
				c.metrics.IncRefineDeclinedOversized()

				if c.logger != nil {
					c.logger.Warnw("pathsearch: declining file-row candidate after decode; actual file count exceeds cap",
						"info_hash", item.Torrent.InfoHash.String(), "files", len(files), "cap", c.maxRefineFiles)
				}

				continue
			}

			for _, file := range files {
				if !matchFile(file, pred) {
					continue
				}

				if visit(item, file) {
					capRsn = capRetained

					break refine
				}
			}
		}
	}

	switch capRsn {
	case capRetained:
		c.metrics.IncRefineRetainedCapped()

		if c.logger != nil {
			c.logger.Warnw(
				"pathsearch: file-row retained-file budget reached; serving memory-capped estimate",
				"query",
				f.Query,
				"budget",
				c.retainedFileBudget,
			)
		}
	case capDeadline:
		c.metrics.IncRefineDeadlineCapped()

		if c.logger != nil {
			c.logger.Warnw(
				"pathsearch: file-row route deadline reached; serving deadline-capped estimate",
				"query",
				f.Query,
				"route_timeout",
				c.routeTimeout,
			)
		}
	}

	c.metrics.IncRoute(RouteServed)

	return candidateTotal, true, nil
}

// SearchFileRows adapts GraphQL fileSearch text queries onto the L3 pathsearch
// candidate route and exact-refines matching file rows from the L1 blob. It
// returns candidate_total as an estimated count; exact global file counts are not
// available on this route.
func (c *Composer) SearchFileRows(
	ctx context.Context,
	f Filters,
	opts QueryOptions,
	limit, offset uint,
	sortBy []FileRowSort,
) (FileRowsResult, bool, error) {
	rows := make([]FileRow, 0)
	retained := 0

	candidateTotal, served, err := c.visitMatchingFiles(
		ctx,
		f,
		opts,
		pageCandidateLimit(limit),
		offset,
		nil,
		func(item search.TorrentContentResultItem, file model.TorrentFile) bool {
			rows = append(rows, FileRow{
				InfoHash:  item.InfoHash,
				Index:     file.Index,
				Path:      file.Path,
				Extension: fileExtension(file),
				Size:      uint64(file.Size),
			})

			retained++

			return retained >= c.retainedFileBudget
		},
	)
	if err != nil || !served {
		return FileRowsResult{}, served, err
	}

	sortFileRows(rows, sortBy)

	page, hasNext := pageFileRows(rows, offset, limit)

	return FileRowsResult{
		Rows:                 page,
		TotalCount:           candidateTotal,
		TotalCountIsEstimate: true,
		HasNextPage:          hasNext,
	}, true, nil
}

// PathTypeahead derives child path-segment suggestions from the same L3
// candidate + L1 exact-refine machinery used by collapse:path. Suggestions are
// deduped case-insensitively and ordered by distinct torrent count descending.
func (c *Composer) PathTypeahead(
	ctx context.Context,
	prefix string,
	opts QueryOptions,
	limit uint,
) ([]string, bool, error) {
	buckets := make(map[string]*typeaheadBucket)
	nextSeen := 0

	_, served, err := c.visitMatchingFiles(
		ctx,
		Filters{Query: prefix},
		opts,
		limit,
		0,
		nil,
		func(item search.TorrentContentResultItem, file model.TorrentFile) bool {
			segment, ok := nextPathSegment(prefix, file.Path)
			if !ok {
				return false
			}

			key := strings.ToLower(segment)

			b, ok := buckets[key]
			if !ok {
				b = &typeaheadBucket{
					text:      segment,
					firstSeen: nextSeen,
					torrents:  map[protocol.ID]struct{}{},
				}
				buckets[key] = b
				nextSeen++
			}

			b.torrents[item.InfoHash] = struct{}{}

			return false
		},
	)
	if err != nil || !served {
		return nil, served, err
	}

	ordered := make([]*typeaheadBucket, 0, len(buckets))
	for _, b := range buckets {
		ordered = append(ordered, b)
	}

	sortSuggestionBuckets(ordered)

	if limit > 0 && uint(len(ordered)) > limit {
		ordered = ordered[:limit]
	}

	suggestions := make([]string, 0, len(ordered))
	for _, b := range ordered {
		suggestions = append(suggestions, b.text)
	}

	return suggestions, true, nil
}

func pageFileRows(rows []FileRow, offset, limit uint) ([]FileRow, bool) {
	if offset >= uint(len(rows)) {
		return nil, false
	}

	rows = rows[offset:]
	if limit == 0 {
		return rows, false
	}

	hasNext := uint(len(rows)) > limit
	if hasNext {
		rows = rows[:limit]
	}

	return rows, hasNext
}

func sortFileRows(rows []FileRow, sortBy []FileRowSort) {
	if len(sortBy) == 0 {
		sortBy = []FileRowSort{{Field: FileRowSortSize, Descending: true}}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]

		for _, s := range sortBy {
			if cmp, ok := compareFileRow(a, b, s); ok && cmp != 0 {
				if s.Descending {
					return -cmp < 0
				}

				return cmp < 0
			}
		}

		return compareFileRowTie(a, b) < 0
	})
}

func compareFileRow(a, b FileRow, s FileRowSort) (int, bool) {
	switch strings.ToLower(s.Field) {
	case FileRowSortSize:
		return cmpUint64(a.Size, b.Size), true
	case FileRowSortPath:
		return strings.Compare(a.Path, b.Path), true
	case FileRowSortExtension:
		return strings.Compare(a.Extension, b.Extension), true
	case FileRowSortIndex:
		return cmpUint(a.Index, b.Index), true
	case FileRowSortInfoHash, "infohash":
		return strings.Compare(a.InfoHash.String(), b.InfoHash.String()), true
	default:
		return 0, false
	}
}

func compareFileRowTie(a, b FileRow) int {
	if cmp := strings.Compare(a.Path, b.Path); cmp != 0 {
		return cmp
	}

	if cmp := strings.Compare(a.InfoHash.String(), b.InfoHash.String()); cmp != 0 {
		return cmp
	}

	return cmpUint(a.Index, b.Index)
}

func cmpUint(a, b uint) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func sortSuggestionBuckets(buckets []*typeaheadBucket) {
	sort.SliceStable(buckets, func(i, j int) bool {
		a, b := buckets[i], buckets[j]

		if len(a.torrents) != len(b.torrents) {
			return len(a.torrents) > len(b.torrents)
		}

		if a.firstSeen != b.firstSeen {
			return a.firstSeen < b.firstSeen
		}

		return a.text < b.text
	})
}

func nextPathSegment(prefix, path string) (string, bool) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || path == "" {
		return "", false
	}

	lowerPath := strings.ToLower(path)
	lowerPrefix := strings.ToLower(prefix)
	start := strings.Index(lowerPath, lowerPrefix)

	if start < 0 {
		return "", false
	}

	segmentStart := start
	if slash := strings.LastIndex(prefix, "/"); slash >= 0 {
		segmentStart = start + slash + 1
	} else if slash := strings.LastIndex(path[:start], "/"); slash >= 0 {
		segmentStart = slash + 1
	}

	afterPrefix := start + len(prefix)
	if strings.HasSuffix(prefix, "/") {
		segmentStart = afterPrefix
	}

	if segmentStart > len(path) || afterPrefix > len(path) {
		return "", false
	}

	segmentEnd := len(path)
	if slash := strings.Index(path[afterPrefix:], "/"); slash >= 0 {
		segmentEnd = afterPrefix + slash
	}

	if segmentEnd < segmentStart {
		return "", false
	}

	segment := path[segmentStart:segmentEnd]
	if segment == "" {
		return "", false
	}

	return segment, true
}
