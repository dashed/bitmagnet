package router

import (
	"context"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"gorm.io/gen/field"
	"gorm.io/gorm/clause"
)

// requestBuilder derives a Tantivy pb.SearchRequest from the opaque query.Option
// slice the search call site passes. It is an interface so the router's mode /
// shadow orchestration can be unit-tested with a stub, independent of the
// extraction mechanism.
type requestBuilder interface {
	// build returns the derived request and its routing metadata. canCompare is
	// false when the query carried filter options the recorder could not map
	// (see recorder): such a request would match a superset of the PostgreSQL
	// result (Tantivy-unfiltered vs PG-filtered), so the router skips the shadow
	// comparison rather than pollute the similarity metrics with that artifact.
	build(options []query.Option) (req *pb.SearchRequest, meta buildResult)
}

// buildResult carries the request signals shared by the serving and shadow
// eligibility gates.
type buildResult struct {
	// canCompare is false only when an unmapped filter option was dropped.
	canCompare bool
	// hasFacets is true when facet or aggregation work was requested.
	hasFacets bool
}

// optionRequestBuilder extracts the request by replaying the options against a
// recording OptionBuilder. See recorder for why this is the chosen approach and
// what it deliberately drops (structured filters).
type optionRequestBuilder struct{}

func (optionRequestBuilder) build(options []query.Option) (*pb.SearchRequest, buildResult) {
	rec := newRecorder()
	for _, opt := range options {
		applyOption(rec, opt)
	}

	return rec.toSearchRequest(), buildResult{
		canCompare: !rec.skippedFilter,
		hasFacets:  rec.hasFacets,
	}
}

// applyOption applies a single option to the recorder, swallowing any panic and
// recording that it happened. Options that build SQL joins, preloads or filter
// criteria call OptionBuilder methods that need a live *dao.Query (in practice
// only query.Where -> criteria.Raw -> Query().Torrent.UnderlyingDB(); joins and
// preloads build field expressions off the zero-value *dao.Query without
// touching its nil db, so they don't panic). We intentionally skip the
// panicking ones — their data, crucially the structured filters, is NOT part of
// the Phase-4 SearchRequest — but flag that a filter was dropped so the router
// can skip a comparison it can't make apples-to-apples.
func applyOption(rec *recorder, opt query.Option) {
	defer func() {
		if recover() != nil {
			rec.skippedFilter = true
		}
	}()

	_, _ = opt(rec)
}

// recorder is a write-only query.OptionBuilder that captures just the structured
// pieces of a search needed to mirror it on Tantivy: the (raw, pre-tsquery)
// query string, the limit/offset, and the orderings. It must embed the
// query.OptionBuilder interface — that interface has unexported methods, so a
// type outside the query package can only satisfy it by promoting them from an
// embedded value — and then overrides the exported methods exercised while
// options are applied.
//
// Why a recorder and not the real builder: OptionBuilder.QueryString eagerly
// converts the raw string to a Postgres tsquery (fts.AppQueryToTsquery) and the
// real builder's state is unexported, so the original app-query string — which
// is what the Tantivy sidecar's own parser expects — cannot be read back. The
// recorder captures the raw argument instead.
//
// Deliberately NOT captured: structured filters. query.Where compiles its
// Criteria straight to opaque SQL (a Scope closure); there is no structured
// representation to map onto pb.SearchFilters without recognising each Criteria
// type at the call site. Until that bridge exists, filtered queries are rejected
// before either serving or shadow comparison so the engines are never compared
// with different predicates.
type recorder struct {
	query.OptionBuilder // nil; supplies the unexported interface methods (never called during option application)

	daoQuery *dao.Query

	queryString string
	limit       uint
	limitSet    bool
	offset      uint
	sorts       []*pb.SortBy
	// hasFacets records aggregation work that Tantivy serving cannot yet
	// reproduce. Shadow comparison remains eligible because it compares items.
	hasFacets bool
	// skippedFilter is set when applyOption recovered a panic, which (for the
	// current option set) means a filter option was dropped — see applyOption.
	skippedFilter bool
}

func newRecorder() *recorder {
	// A non-nil (zero-value) *dao.Query keeps join/preload option closures —
	// which call fn(b.Query()) to build field expressions — from dereferencing
	// nil. Filter criteria still panic at UnderlyingDB() and are skipped by
	// applyOption, which is the intent.
	return &recorder{daoQuery: &dao.Query{}}
}

func (r *recorder) toSearchRequest() *pb.SearchRequest {
	req := &pb.SearchRequest{Query: r.queryString}

	if r.limitSet || r.offset > 0 {
		req.Pagination = &pb.Pagination{
			Limit:  uint32(r.limit),
			Offset: uint32(r.offset),
		}
	}

	req.Sort = r.sorts

	return req
}

// --- DBContext -------------------------------------------------------------

func (r *recorder) Query() *dao.Query { return r.daoQuery }

// --- Captured options ------------------------------------------------------

func (r *recorder) QueryString(s string) query.OptionBuilder {
	r.queryString = s

	return r
}

func (r *recorder) Limit(n uint) query.OptionBuilder {
	r.limit = n
	r.limitSet = true

	return r
}

func (r *recorder) Offset(n uint) query.OptionBuilder {
	r.offset = n

	return r
}

func (r *recorder) OrderBy(columns ...query.OrderByColumn) query.OptionBuilder {
	for _, c := range columns {
		if f, ok := sortField(c.Column.Name); ok {
			r.sorts = append(r.sorts, &pb.SortBy{Field: f, Descending: c.Desc})
		}
	}

	return r
}

// --- No-op options (return r so chaining inside a composite option keeps the
// state captured by earlier sub-options) ------------------------------------

func (r *recorder) Table(string) query.OptionBuilder            { return r }
func (r *recorder) Join(...query.TableJoin) query.OptionBuilder { return r }
func (r *recorder) RequireJoin(...string) query.OptionBuilder   { return r }
func (r *recorder) Select(...clause.Expr) query.OptionBuilder   { return r }
func (r *recorder) Group(...clause.Column) query.OptionBuilder  { return r }
func (r *recorder) Facet(facets ...query.Facet) query.OptionBuilder {
	if len(facets) > 0 {
		r.hasFacets = true
	}

	return r
}

func (r *recorder) Preload(...field.RelationField) query.OptionBuilder { return r }
func (r *recorder) Scope(...query.Scope) query.OptionBuilder           { return r }
func (r *recorder) Callback(...query.Callback) query.OptionBuilder     { return r }
func (r *recorder) WithTotalCount(bool) query.OptionBuilder            { return r }
func (r *recorder) WithHasNextPage(bool) query.OptionBuilder           { return r }
func (r *recorder) WithAggregationBudget(budget float64) query.OptionBuilder {
	if budget > 0 {
		r.hasFacets = true
	}

	return r
}

func (r *recorder) Context(func(context.Context) context.Context) query.OptionBuilder {
	return r
}

// sortableFields maps the PostgreSQL ordering column names the search layer uses
// to the Tantivy fast field of the same name. Relevance ordering
// ("query_string_rank") is absent on purpose: it maps to Tantivy's default score
// order, i.e. no explicit sort.
var sortableFields = map[string]string{
	"published_at": "published_at",
	"size":         "size",
	"seeders":      "seeders",
	"leechers":     "leechers",
	"files_count":  "files_count",
	"release_year": "release_year",
}

func sortField(name string) (string, bool) {
	f, ok := sortableFields[name]

	return f, ok
}
