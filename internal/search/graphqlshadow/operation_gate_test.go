package graphqlshadow

import (
	"testing"
)

func TestClassifyOperation(t *testing.T) {
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
			got, err := ClassifyOperation(tt.query, tt.operationName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ClassifyOperation(%q, %q) = %v, want error", tt.query, tt.operationName, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("ClassifyOperation(%q, %q) unexpected error: %v", tt.query, tt.operationName, err)
			}

			if got != tt.want {
				t.Errorf("ClassifyOperation(%q, %q) = %v, want %v", tt.query, tt.operationName, got, tt.want)
			}
		})
	}
}

// TestIsShadowEligibleFailsClosed is the gate's decision-point test: ONLY a
// read-only query is eligible; every mutation, subscription, parse failure, and
// ambiguous/missing operation must be ineligible (fail closed).
func TestIsShadowEligibleFailsClosed(t *testing.T) {
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
