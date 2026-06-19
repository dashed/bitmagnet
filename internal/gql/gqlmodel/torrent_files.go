package gqlmodel

import (
	"context"
	"database/sql/driver"
	"errors"
	"sort"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

type TorrentFilesQueryInput struct {
	InfoHashes  []protocol.ID
	Limit       model.NullUint
	Page        model.NullUint
	Offset      model.NullUint
	TotalCount  model.NullBool
	HasNextPage model.NullBool
	Cached      model.NullBool
	OrderBy     []gen.TorrentFilesOrderByInput
}

func (t TorrentQuery) Files(ctx context.Context, query TorrentFilesQueryInput) (search.TorrentFilesResult, error) {
	// G2: serve the per-torrent file browser from the AfterFind-hydrated blob
	// (torrents.files_data) so it survives the torrent_files DROP. Flag-gated
	// (default OFF) — until flipped the original `SELECT FROM torrent_files`
	// path below runs unchanged.
	if search.FeatureFlagsValue().FileBrowserFromBlob {
		return t.filesFromBlob(ctx, query)
	}

	limit := uint(10)
	if query.Limit.Valid {
		limit = query.Limit.Uint
	}

	options := []q.Option{
		q.SearchParams{
			Limit:             model.NullUint{Valid: true, Uint: limit},
			Page:              query.Page,
			Offset:            query.Offset,
			TotalCount:        query.TotalCount,
			HasNextPage:       query.HasNextPage,
			AggregationBudget: model.NullFloat64{Valid: true, Float64: 0},
		}.Option(),
	}

	var criteria []q.Criteria
	if query.InfoHashes != nil {
		criteria = append(criteria, search.TorrentFileInfoHashCriteria(query.InfoHashes...))
	}

	options = append(options, q.Where(criteria...))
	fullOrderBy := maps.NewInsertMap[search.TorrentFilesOrderBy, search.OrderDirection]()

	for _, ob := range query.OrderBy {
		direction := search.OrderDirectionAscending
		if desc, ok := ob.Descending.ValueOK(); ok && *desc {
			direction = search.OrderDirectionDescending
		}

		field, err := search.ParseTorrentFilesOrderBy(ob.Field.String())
		if err != nil {
			return search.TorrentFilesResult{}, err
		}

		fullOrderBy.Set(field, direction)
	}

	options = append(options, search.TorrentFilesFullOrderBy(fullOrderBy).Option())

	return t.Search.TorrentFiles(ctx, options...)
}

// filesFromBlob is the G2 blob-backed implementation of the per-torrent file
// browser. It loads the requested torrents (whose AfterFind hook hydrates Files
// from the files_data blob), derives each file's extension from its PATH
// (FB-A0/G1 — the blob's stored `e` is empty for crawl-path torrents and must
// never be trusted), then orders and paginates in memory. A per-torrent browse
// is bounded (a single info_hash, tens–hundreds of files for the common case),
// so in-memory sort/paginate is appropriate; over-threshold torrents cap their
// stored fileset the same way the blob does.
func (t TorrentQuery) filesFromBlob(
	ctx context.Context,
	in TorrentFilesQueryInput,
) (search.TorrentFilesResult, error) {
	if t.Dao == nil {
		// Defensive: a mis-wired resolver must degrade to an error, never panic
		// (the field resolver constructs TorrentQuery itself; see
		// query.resolvers.go Files).
		return search.TorrentFilesResult{}, errors.New("filesFromBlob: Dao not wired")
	}

	limit := uint(10)
	if in.Limit.Valid {
		limit = in.Limit.Uint
	}

	dao := t.Dao.Torrent.WithContext(ctx)

	if len(in.InfoHashes) > 0 {
		valuers := make([]driver.Valuer, len(in.InfoHashes))
		for i, h := range in.InfoHashes {
			valuers[i] = h
		}

		dao = dao.Where(t.Dao.Torrent.InfoHash.In(valuers...))
	}

	torrents, err := dao.Find()
	if err != nil {
		return search.TorrentFilesResult{}, err
	}

	var files []model.TorrentFile

	for _, tor := range torrents {
		for _, f := range tor.Files {
			f.InfoHash = tor.InfoHash
			// G1: extension is always path-derived, never the blob's stored `e`.
			f.Extension = model.FileExtensionFromPath(f.Path)
			files = append(files, f)
		}
	}

	sortTorrentFiles(files, in.OrderBy)

	total := uint(len(files))

	offset := uint(0)
	if in.Page.Valid && in.Page.Uint > 0 {
		offset += (in.Page.Uint - 1) * limit
	}

	if in.Offset.Valid {
		offset += in.Offset.Uint
	}

	start := offset
	if start > total {
		start = total
	}

	end := start + limit
	if end > total {
		end = total
	}

	page := files[start:end]

	result := search.TorrentFilesResult{
		Items: page,
	}
	if in.TotalCount.Valid && in.TotalCount.Bool {
		result.TotalCount = total
	}

	if in.HasNextPage.Valid && in.HasNextPage.Bool {
		result.HasNextPage = end < total
	}

	return result, nil
}

// sortTorrentFiles applies the requested order(s) to an in-memory file slice,
// mirroring the column semantics of TorrentFilesOrderBy (index, path,
// extension, size). With no order specified it falls back to path ascending,
// matching the model's AfterFind default so the blob path is order-stable.
func sortTorrentFiles(files []model.TorrentFile, orderBy []gen.TorrentFilesOrderByInput) {
	if len(orderBy) == 0 {
		sort.SliceStable(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		return
	}

	sort.SliceStable(files, func(i, j int) bool {
		a, b := files[i], files[j]

		for _, ob := range orderBy {
			desc := false
			if d, ok := ob.Descending.ValueOK(); ok && d != nil {
				desc = *d
			}

			cmp := compareTorrentFileField(a, b, ob.Field.String())
			if cmp == 0 {
				continue
			}

			if desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
}

// compareTorrentFileField returns -1/0/1 comparing two files on the named field.
func compareTorrentFileField(a, b model.TorrentFile, field string) int {
	switch field {
	case string(search.TorrentFilesOrderByIndex):
		return cmpUint(a.Index, b.Index)
	case string(search.TorrentFilesOrderBySize):
		return cmpUint(a.Size, b.Size)
	case string(search.TorrentFilesOrderByExtension):
		return cmpStr(a.Extension.String, b.Extension.String)
	case string(search.TorrentFilesOrderByPath):
		return cmpStr(a.Path, b.Path)
	default:
		return cmpStr(a.Path, b.Path)
	}
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

func cmpStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
