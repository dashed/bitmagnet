package graphqlshadow

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestClassifyOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		query         string
		operationName string
		want          OperationType
		wantErr       bool
	}{
		{
			name:  "anonymous query",
			query: `{ torrentContent { search { totalCount } } }`,
			want:  OperationQuery,
		},
		{
			name:  "named query",
			query: `query Search { torrentContent { search { totalCount } } }`,
			want:  OperationQuery,
		},
		{
			name:  "explicit mutation",
			query: `mutation Del { torrent { delete(infoHashes: ["aa"]) } }`,
			want:  OperationMutation,
		},
		{
			name:  "subscription",
			query: `subscription S { workers { listAll { key } } }`,
			want:  OperationSubscription,
		},
		{
			name:          "multi-op selects the query by name",
			query:         `query Q { version } mutation M { torrent { delete(infoHashes: []) } }`,
			operationName: "Q",
			want:          OperationQuery,
		},
		{
			name:          "multi-op selects the mutation by name",
			query:         `query Q { version } mutation M { torrent { delete(infoHashes: []) } }`,
			operationName: "M",
			want:          OperationMutation,
		},
		{
			name:    "multi-op without operationName is ambiguous (error)",
			query:   `query Q { version } mutation M { torrent { delete(infoHashes: []) } }`,
			wantErr: true,
		},
		{
			name:          "operationName not found is an error",
			query:         `query Q { version }`,
			operationName: "Nope",
			wantErr:       true,
		},
		{
			name:    "parse failure is an error",
			query:   `query { this is not valid graphql `,
			wantErr: true,
		},
		{
			name:    "empty document is an error",
			query:   ``,
			wantErr: true,
		},
		{
			name:    "only fragments, no operation, is an error",
			query:   `fragment F on Query { version }`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ClassifyOperation(tt.query, tt.operationName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf(
						"ClassifyOperation(%q, %q) = %v, want error",
						tt.query,
						tt.operationName,
						got,
					)
				}

				return
			}

			if err != nil {
				t.Fatalf(
					"ClassifyOperation(%q, %q) unexpected error: %v",
					tt.query,
					tt.operationName,
					err,
				)
			}

			if got != tt.want {
				t.Errorf(
					"ClassifyOperation(%q, %q) = %v, want %v",
					tt.query,
					tt.operationName,
					got,
					tt.want,
				)
			}
		})
	}
}

// TestIsShadowEligibleFailsClosed is the gate's decision-point test: ONLY a
// read-only query is eligible; every mutation, subscription, parse failure, and
// ambiguous/missing operation must be ineligible (fail closed).
func TestIsShadowEligibleFailsClosed(t *testing.T) {
	t.Parallel()

	eligible := []struct {
		query, op string
	}{
		{`{ version }`, ""},
		{`query Q { version }`, "Q"},
		{`query Q { version } mutation M { torrent { delete(infoHashes: []) } }`, "Q"},
	}
	for _, e := range eligible {
		if !IsShadowEligible(e.query, e.op) {
			t.Errorf("IsShadowEligible(%q, %q) = false, want true", e.query, e.op)
		}
	}

	ineligible := []struct {
		query, op string
	}{
		{`mutation M { torrent { delete(infoHashes: ["aa"]) } }`, ""},
		{`mutation { torrent { delete(infoHashes: ["aa"]) } }`, ""},
		{`subscription S { version }`, ""},
		{`query Q { version } mutation M { torrent { delete(infoHashes: []) } }`, "M"},
		{`query Q { version } mutation M { torrent { delete(infoHashes: []) } }`, ""}, // ambiguous
		{`not valid graphql`, ""},
		{``, ""},
		{`query Q { version }`, "Missing"},
	}
	for _, e := range ineligible {
		if IsShadowEligible(e.query, e.op) {
			t.Errorf("IsShadowEligible(%q, %q) = true, want false (must fail closed)", e.query, e.op)
		}
	}
}

func TestIsComparableSearchOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		query         string
		operationName string
		variables     map[string]any
		want          bool
	}{
		{
			name: "direct complete search",
			query: `query Search { torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate
			  items { infoHash contentType contentSource contentId }
			  aggregations { __typename }
			} } }`,
			operationName: "Search",
			want:          true,
		},
		{
			name: "canonical id and complete partial facet",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id }
			  aggregations { releaseYear { value label count isEstimate } }
			} } }`,
			want: true,
		},
		{
			name: "fragment-expanded search",
			query: `query Search { ...Root }
			  fragment Root on Query { torrentContent { search(input: {totalCount: true}) { ...Result } } }
			  fragment Result on TorrentContentSearchResult {
			    totalCount totalCountIsEstimate items { ...Identity } aggregations { __typename }
			  }
			  fragment Identity on TorrentContent { infoHash contentType contentSource contentId }`,
			operationName: "Search",
			want:          true,
		},
		{
			name: "resolved totalCount variable true",
			query: `query Search($count: Boolean) { torrentContent { search(input: {totalCount: $count}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } }`,
			operationName: "Search",
			variables:     map[string]any{"count": true},
			want:          true,
		},
		{
			name: "resolved totalCount variable false",
			query: `query Search($count: Boolean) { torrentContent { search(input: {totalCount: $count}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } }`,
			operationName: "Search",
			variables:     map[string]any{"count": false},
		},
		{
			name: "search with typename-only siblings",
			query: `{ __typename torrentContent { __typename search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } }`,
			want: true,
		},
		{
			name: "search plus out-of-scope queue sibling",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } queue { jobs } }`,
		},
		{
			name: "search plus out-of-scope torrent files sibling",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } torrent { files } }`,
		},
		{
			name:  "unrelated query",
			query: `{ version }`,
		},
		{
			name: "missing totalCount input",
			query: `{ torrentContent { search(input: {}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } }`,
		},
		{
			name: "missing required result field",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount items { id } aggregations { __typename }
			} } }`,
		},
		{
			name: "missing identity field",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate
			  items { infoHash contentType contentSource }
			  aggregations { __typename }
			} } }`,
		},
		{
			name: "aliased response path",
			query: `{ tc: torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id } aggregations { __typename }
			} } }`,
		},
		{
			name: "aliased selected facet",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id }
			  aggregations { years: releaseYear { value label count isEstimate } }
			} } }`,
		},
		{
			name: "incomplete selected facet",
			query: `{ torrentContent { search(input: {totalCount: true}) {
			  totalCount totalCountIsEstimate items { id }
			  aggregations { releaseYear { value count isEstimate } }
			} } }`,
		},
		{
			name:  "mutation",
			query: `mutation { torrent { delete(infoHashes: []) } }`,
		},
		{
			name:  "parse failure",
			query: `not graphql`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := IsComparableSearchOperation(test.query, test.operationName, test.variables)
			if got != test.want {
				t.Errorf("IsComparableSearchOperation() = %v, want %v", got, test.want)
			}
		})
	}
}

// TestProductionReactSearchOperationIsComparable binds the admission gate to
// the real query document shipped by the embedded React UI. A frontend edit
// that makes production searches ineligible must fail here instead of silently
// driving the comparison population to zero during a soak.
func TestProductionReactSearchOperationIsComparable(t *testing.T) {
	t.Parallel()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "../../.."))
	operationPath := filepath.Join(
		repoRoot,
		"webui-react/src/graphql/operations/TorrentContentSearch.graphql",
	)

	query, err := os.ReadFile(operationPath)
	if err != nil {
		t.Fatalf("read production React search operation: %v", err)
	}

	if !IsComparableSearchOperation(
		string(query),
		"TorrentContentSearch",
		map[string]any{"totalCount": true},
	) {
		t.Fatal("production React TorrentContentSearch operation is not shadow-comparable")
	}

	if IsComparableSearchOperation(
		string(query),
		"TorrentContentSearch",
		map[string]any{"totalCount": false},
	) {
		t.Fatal("production React search with totalCount=false must remain ineligible")
	}
}

func TestExactCountControlOperationIsComparable(t *testing.T) {
	t.Parallel()

	query := `query GraphQLShadowExactControl(
	  $aggregationBudget: Float!
	  $infoHashes: [Hash20!]!
	  $limit: Int!
	  $totalCount: Boolean!
	) {
	  torrentContent {
	    search(input: {
	      aggregationBudget: $aggregationBudget
	      infoHashes: $infoHashes
	      limit: $limit
	      totalCount: $totalCount
	    }) {
	      totalCount
	      totalCountIsEstimate
	      items { id }
	      aggregations { __typename }
	    }
	  }
	}`
	variables := map[string]any{
		"aggregationBudget": 0,
		"infoHashes":        []any{},
		"limit":             0,
		"totalCount":        true,
	}

	if !IsComparableSearchOperation(query, "GraphQLShadowExactControl", variables) {
		t.Fatal("bounded exact-count control must remain shadow-comparable")
	}
}
