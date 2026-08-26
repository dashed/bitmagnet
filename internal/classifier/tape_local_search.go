package classifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
)

// Observation kinds for the local content search.
const (
	tapeKindLocalContentBySearch = "local.content_by_search"
	tapeKindLocalContentByID     = "local.content_by_id"
)

// Error kinds for a failed local search. The classifier treats every local
// search error other than classification.ErrUnmatched as fatal, but a
// cancelled or timed-out context is worth reconstructing faithfully because
// callers do compare against those sentinels.
const (
	tapeErrorKindLocalSearch     = "local_search"
	tapeErrorKindContextCanceled = "context_canceled"
	tapeErrorKindContextDeadline = "context_deadline_exceeded"
)

// TapeScopeLimits states what a green replay against a classifier tape does not
// prove. It is written verbatim into every tape's PROVENANCE.md, because an
// oracle that does not carry its own limits invites a pass to be read as
// broader evidence than it is.
const TapeScopeLimits = `**The desync guarantee stops at the ` + "`searchString`" + ` boundary.**

` + "`local.content_by_search`" + ` records the search string the classifier hands to the
query builder, not the tsquery that string is compiled into. The tsquery is
built inside the query builder and is not exposed at this seam. So an
implementation that derives a *different* tsquery from the same base title does
**not** desync: replay matches on the search string, hands back Go's recorded
candidates, and the divergence is invisible. This is the one class of bug the
request half of the tape was built to catch and cannot.

That is not hypothetical. Rust's word-character predicate (` + "`char::is_alphanumeric`" + `)
disagrees with Go's ` + "`unicode.IsLetter || unicode.IsDigit`" + ` at 12,322 code points --
see ` + "`bitmagnet-rs/crates/bitmagnet-fts/src/lib.rs`" + ` and
` + "`bitmagnet-rs/crates/bitmagnet-search/src/query.rs`" + `, both of which document the
gap as harmless. In the search path it silently narrows the query, turning an
` + "`&`" + ` into an adjacency ` + "`<->`" + `, so Rust returns a strict subset of Go's rows with
no error at all.

Tsquery construction is therefore **out of scope for this tape** and has to be
proven separately, by an all-scalar test over the two word-character predicates.
A green replay here must not be read as covering it.

Extending the seam down to the tsquery would be a query-builder change and a
separate decision; it has deliberately not been made.`

// localContentBySearchRequest is the question ContentBySearch asks.
//
// SearchString is the string actually handed to the query builder, not just the
// base title it was derived from: a port that quotes or normalises differently
// is asking a different question even when its answer happens to coincide, and
// recording only the base title would hide that.
//
// It is NOT the tsquery. Two implementations that agree on this string and
// disagree on the tsquery they compile it into will replay identically and
// never desync; see [TapeScopeLimits].
//
// ReleaseDateRange records the filter the year is expanded into, again so a
// port that expands it differently desyncs rather than passing by luck. Year
// and ReleaseDateRange are null together when the classifier searched without a
// year constraint.
type localContentBySearchRequest struct {
	ContentType      string         `json:"contentType"`
	BaseTitle        string         `json:"baseTitle"`
	SearchString     string         `json:"searchString"`
	Year             *uint16        `json:"year"`
	ReleaseDateRange *tapeDateRange `json:"releaseDateRange"`
	OrderBy          string         `json:"orderBy"`
	Limit            int            `json:"limit"`
}

type tapeDateRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// localContentByIDRequest is the question ContentByID asks. Identifier
// distinguishes the two branches of contentByIDOptions, which differ in both
// the criteria applied and the ordering imposed on the LIMIT 1 pick.
type localContentByIDRequest struct {
	ContentType string `json:"contentType"`
	Source      string `json:"source"`
	ID          string `json:"id"`
	Identifier  string `json:"identifier"`
	OrderBy     string `json:"orderBy"`
	Limit       int    `json:"limit"`
}

// localContentResponse is the ordered candidate list as returned by the query,
// before any selection is applied to it.
//
// Items is the whole point of the tape. ts_rank_cd ties for the phrase queries
// the classifier issues, so the contents and the order of this window are
// decided by the query plan; re-running the query against a snapshot would
// produce a different window. Only what was observed is replayable.
type localContentResponse struct {
	Items []localContentItem `json:"items"`
}

// localContentItem pairs a candidate with the relevance rank it was ordered by.
//
// QueryStringRank is a decimal string rather than a JSON number so the bytes
// are unambiguous across languages: Go and Rust do not agree on the shortest
// round-trip rendering of every float64. It is recorded as evidence of the tie,
// not as an input to selection.
type localContentItem struct {
	QueryStringRank string        `json:"queryStringRank"`
	Content         model.Content `json:"content"`
}

// contentSearch is the slice of search.Search the classifier's local search
// actually uses. Narrowing it keeps the production wiring unchanged while
// letting a replay stand in for the database.
type contentSearch interface {
	Content(ctx context.Context, options ...query.Option) (search.ContentResult, error)
}

// observeContent runs a content query through the tape seam.
//
// With no session on the context -- every normally configured process -- this
// is the query call and nothing else. With a recording session it records the
// query's result in the order the query returned it, before any selection has
// been applied. With a replay session it serves the recorded result and never
// touches the database.
func (l localSearch) observeContent(
	ctx context.Context,
	kind string,
	request any,
	options []query.Option,
) (search.ContentResult, error) {
	session := tape.SessionFrom(ctx)
	if session == nil {
		return l.Content(ctx, options...)
	}

	if session.Replaying() {
		return replayContent(session, kind, request)
	}

	result, err := l.Content(ctx, options...)
	if err != nil {
		session.ObserveError(kind, request, localSearchErrorKind(err), err.Error())

		return result, err
	}

	session.Observe(kind, request, newLocalContentResponse(result))

	return result, nil
}

func localSearchErrorKind(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return tapeErrorKindContextCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return tapeErrorKindContextDeadline
	default:
		return tapeErrorKindLocalSearch
	}
}

func replayContent(session *tape.Session, kind string, request any) (search.ContentResult, error) {
	responseJSON, observationErr, err := session.Next(kind, request)
	if err != nil {
		return search.ContentResult{}, err
	}

	if observationErr != nil {
		return search.ContentResult{}, rebuildLocalSearchError(observationErr)
	}

	var response localContentResponse
	// model.Content is generated and fully JSON-tagged, but musttag does not
	// follow all of its nested custom scalar types through this tape wrapper.
	//nolint:musttag
	if err := json.Unmarshal(responseJSON, &response); err != nil {
		return search.ContentResult{}, fmt.Errorf("decode taped %s response: %w", kind, err)
	}

	// A recorded empty list is an answer: the query ran and matched nothing.
	// It reaches the caller as an empty Items slice, exactly as the database
	// would have returned it. A tape with no observation at this position never
	// gets here -- session.Next reports a miss.
	items := make([]search.ContentResultItem, 0, len(response.Items))

	for i, item := range response.Items {
		rank, err := strconv.ParseFloat(item.QueryStringRank, 64)
		if err != nil {
			return search.ContentResult{}, fmt.Errorf(
				"decode taped %s response item %d rank %q: %w", kind, i, item.QueryStringRank, err,
			)
		}

		items = append(items, search.ContentResultItem{
			ResultItem: query.ResultItem{QueryStringRank: rank},
			Content:    item.Content,
		})
	}

	return search.ContentResult{Items: items}, nil
}

func rebuildLocalSearchError(observationErr *tape.ObservationError) error {
	switch observationErr.Kind {
	case tapeErrorKindContextCanceled:
		return context.Canceled
	case tapeErrorKindContextDeadline:
		return context.DeadlineExceeded
	default:
		return errors.New(observationErr.Message)
	}
}

func newLocalContentResponse(result search.ContentResult) localContentResponse {
	// Always a non-nil slice: an empty candidate list is an observation, and it
	// has to survive to disk as [] rather than null so a replay can tell it
	// apart from a gap in the tape.
	items := make([]localContentItem, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, localContentItem{
			QueryStringRank: strconv.FormatFloat(item.QueryStringRank, 'g', -1, 64),
			Content:         item.Content,
		})
	}

	return localContentResponse{Items: items}
}

func newLocalContentBySearchRequest(
	ct model.ContentType,
	baseTitle string,
	searchString string,
	year model.Year,
) localContentBySearchRequest {
	request := localContentBySearchRequest{
		ContentType:  ct.String(),
		BaseTitle:    baseTitle,
		SearchString: searchString,
		OrderBy:      contentBySearchOrderBy,
		Limit:        contentBySearchLimit,
	}
	if !year.IsNil() {
		value := uint16(year)
		dateRange := model.NewDateRangeFromYear(year)
		request.Year = &value
		request.ReleaseDateRange = &tapeDateRange{
			Start: dateRange.StartTime().UTC().Format("2006-01-02"),
			End:   dateRange.EndTime().UTC().Format("2006-01-02"),
		}
	}

	return request
}

func newLocalContentByIDRequest(ref model.ContentRef) localContentByIDRequest {
	request := localContentByIDRequest{
		ContentType: ref.Type.String(),
		Source:      ref.Source,
		ID:          ref.ID,
		Identifier:  identifierAlternative,
		OrderBy:     contentByIDAlternativeOrderBy,
		Limit:       contentByIDLimit,
	}
	if ref.Source == canonicalIdentifierSource {
		request.Identifier = identifierCanonical
		request.OrderBy = ""
	}

	return request
}
