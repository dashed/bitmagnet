package gqlmodel

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
)

//nolint:paralleltest // mutates global search feature flags
func TestFileSearchFacetsFeatureGatesAndDelegation(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })

	wantResult := filesearch.FacetsResult{Facets: []filesearch.Facet{{
		Field: "extension",
		Buckets: []filesearch.FacetBucket{{
			Value:     "mkv",
			Count:     3,
			TotalSize: 900,
		}},
	}}}

	tests := []struct {
		name      string
		flags     search.FeatureFlags
		wantErr   error
		wantCalls int
	}{
		{
			name:      "default off",
			wantErr:   filesearch.ErrDisabled,
			wantCalls: 0,
		},
		{
			name:      "master on facets off",
			flags:     search.FeatureFlags{FileSearchEnabled: true},
			wantErr:   filesearch.ErrDisabled,
			wantCalls: 0,
		},
		{
			name: "both on",
			flags: search.FeatureFlags{
				FileSearchEnabled:       true,
				FileSearchFacetsEnabled: true,
			},
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			search.SetFeatureFlags(tt.flags)
			client := &recordingFileSearchClient{facetsResult: wantResult}

			got, err := (FileSearchQuery{Client: client}).FileSearchFacets(
				context.Background(),
				filesearch.FacetsParams{
					Query:      "  50%_files  ",
					Extensions: []string{".MKV", "mkv"},
					Fields:     []string{"unknown", " Extension ", "extension"},
				},
			)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("FileSearchFacets error = %v, want %v", err, tt.wantErr)
			}

			if client.facetsCalls != tt.wantCalls {
				t.Fatalf("Facets calls = %d, want %d", client.facetsCalls, tt.wantCalls)
			}

			if tt.wantCalls == 0 {
				return
			}

			wantInput := filesearch.FacetsInput{
				Query:            "50%_files",
				QueryLikePattern: `50\%\_files`,
				Extensions:       []string{"mkv"},
				Fields:           []string{"extension"},
			}
			if !reflect.DeepEqual(client.facetsInput, wantInput) {
				t.Fatalf("validated input = %+v, want %+v", client.facetsInput, wantInput)
			}

			if !reflect.DeepEqual(got, wantResult) {
				t.Fatalf("result = %+v, want %+v", got, wantResult)
			}
		})
	}
}

func TestNewFileSearchFacetsResult(t *testing.T) {
	t.Parallel()

	got := NewFileSearchFacetsResult(filesearch.FacetsResult{Facets: []filesearch.Facet{
		{
			Field: "extension",
			Buckets: []filesearch.FacetBucket{
				{Value: "mkv", Count: 3, TotalSize: 900},
				{Value: "huge", Count: ^uint64(0), TotalSize: ^uint64(0)},
			},
		},
		{
			// A newer sidecar may emit facet fields this schema version doesn't
			// know; they must be skipped, never cast into the non-null enum.
			Field:   "bitrate",
			Buckets: []filesearch.FacetBucket{{Value: "high", Count: 1, TotalSize: 1}},
		},
	}})

	if len(got.Facets) != 1 || got.Facets[0].Field.String() != "extension" {
		t.Fatalf("facets = %+v, want one extension facet", got.Facets)
	}

	if len(got.Facets[0].Buckets) != 2 {
		t.Fatalf("buckets = %+v, want two", got.Facets[0].Buckets)
	}

	first := got.Facets[0].Buckets[0]
	if first.Value != "mkv" || first.Count != 3 || first.TotalSize != 900 || first.IsEstimate {
		t.Fatalf("first bucket = %+v, want exact mkv aggregation", first)
	}

	maxInt := int(^uint(0) >> 1)
	huge := got.Facets[0].Buckets[1]
	if huge.Count != maxInt || huge.TotalSize != maxInt || huge.IsEstimate {
		t.Fatalf("huge bucket = %+v, want bounded exact values %d", huge, maxInt)
	}
}
