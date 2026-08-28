package query

import (
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

// ResolvedOptions is the resolved pagination / order / facet / flags state of
// an Option set, exposed so callers and tests can assert how a query would be
// shaped WITHOUT executing any SQL.
//
// The option layer was otherwise un-introspectable: the only consumer is the
// search execution path, whose fakes in unit tests ignore the options entirely.
// That blind spot let pagination bugs survive — notably an unintended Limit cap
// pushed onto a candidate IN(...) query (see the L3 pathsearch route). This makes
// the resolved page window assertable.
type ResolvedOptions struct {
	// Limit is the resolved row limit. Limit.Valid == false means NO limit was set
	// (the query returns all rows); Limit.Valid && Limit.Uint == 0 means an explicit
	// zero limit.
	Limit model.NullUint
	// Offset is the resolved row offset.
	Offset uint
	// OrderBy is the resolved ordering, in applied order.
	OrderBy []OrderByColumn
	// TotalCount reports whether a total-count was requested.
	TotalCount bool
	// HasTSQuery reports whether a free-text tsquery was applied.
	HasTSQuery bool
	// Facets is the resolved facet configuration. It exposes filters, logic, and
	// aggregation state so routed-query tests can prove that removing per-chunk
	// aggregation did not also remove a user's facet predicates.
	Facets []FacetConfig
}

// ResolveOptions applies opts to a fresh builder bound to daoQuery and returns the
// resolved pagination / order / flags, without executing any SQL. The option
// callbacks (hydration etc.) are registered but never run, so no query is issued.
//
// daoQuery is needed only by options that build joins/preloads (they reference
// dao field expressions); pass a dao.Query from a sqlmock-backed *gorm.DB. It may
// be nil for pagination-only option sets.
func ResolveOptions(daoQuery *dao.Query, opts ...Option) (ResolvedOptions, error) {
	b := newQueryContext(dbContext{q: daoQuery})

	for _, o := range opts {
		next, err := o(b)
		if err != nil {
			return ResolvedOptions{}, err
		}

		b = next
	}

	ob, ok := b.(optionBuilder)
	if !ok {
		return ResolvedOptions{}, nil
	}

	facets := make([]FacetConfig, len(ob.facets))
	for i, facet := range ob.facets {
		facets[i] = facet
	}

	return ResolvedOptions{
		Limit:      ob.limit,
		Offset:     ob.offset,
		OrderBy:    ob.orderBy,
		TotalCount: ob.totalCount,
		HasTSQuery: ob.tsquery != "",
		Facets:     facets,
	}, nil
}
