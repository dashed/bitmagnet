package graphqlshadow

import (
	"encoding/json"
	"testing"
)

func TestExtractSearchResultInferID(t *testing.T) {
	t.Parallel()

	// InferID recipe: hex:contentType:contentSource:contentId, with "?" for an
	// absent type, and source+id bound together ("?" for both when source absent).
	search := `{
      "totalCount": 3,
      "totalCountIsEstimate": false,
      "items": [
        {"infoHash": "aabb", "contentType": "movie", "contentSource": "imdb", "contentId": "tt1"},
        {"infoHash": "ccdd", "contentType": "tv_show", "contentSource": null, "contentId": null},
        {"infoHash": "eeff", "contentType": null, "contentSource": null, "contentId": null}
      ],
      "aggregations": {}
    }`

	got, err := ExtractSearchResult(json.RawMessage(search))
	if err != nil {
		t.Fatalf("ExtractSearchResult error: %v", err)
	}

	want := []string{
		"aabb:movie:imdb:tt1",
		"ccdd:tv_show:?:?",
		"eeff:?:?:?",
	}
	if len(got.IDs) != len(want) {
		t.Fatalf("got %d IDs, want %d: %v", len(got.IDs), len(want), got.IDs)
	}

	for i := range want {
		if got.IDs[i] != want[i] {
			t.Errorf("ID[%d] = %q, want %q", i, got.IDs[i], want[i])
		}
	}

	if got.TotalCount != 3 || got.TotalCountIsEstimate {
		t.Errorf("total = %d estimate=%v, want 3/false", got.TotalCount, got.TotalCountIsEstimate)
	}
}

func TestExtractSearchResultUsesCanonicalID(t *testing.T) {
	t.Parallel()

	search := `{"totalCount":1,"totalCountIsEstimate":false,"items":[{"id":"canonical:infer:id"}],"aggregations":{}}`

	got, err := ExtractSearchResult(json.RawMessage(search))
	if err != nil {
		t.Fatalf("ExtractSearchResult error: %v", err)
	}

	if len(got.IDs) != 1 || got.IDs[0] != "canonical:infer:id" {
		t.Errorf("IDs = %v, want canonical id", got.IDs)
	}
}

func TestExtractSearchResultSourcePresentIDNull(t *testing.T) {
	t.Parallel()

	// Source present, id null: id becomes "" (source is the binding field).
	search := `{"totalCount":1,"totalCountIsEstimate":false,"items":[
      {"infoHash":"aa","contentType":"movie","contentSource":"imdb","contentId":null}
    ],"aggregations":{}}`

	got, err := ExtractSearchResult(json.RawMessage(search))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if got.IDs[0] != "aa:movie:imdb:" {
		t.Errorf("ID = %q, want %q", got.IDs[0], "aa:movie:imdb:")
	}
}

func TestExtractSearchResultFacets(t *testing.T) {
	t.Parallel()

	search := `{
      "totalCount": 10,
	  "totalCountIsEstimate": false,
      "items": [{"infoHash":"aa","contentType":"movie","contentSource":null,"contentId":null}],
      "aggregations": {
        "contentType": [
          {"value":"movie","label":"Movie","count":7,"isEstimate":false},
          {"value":"tv_show","label":"TV","count":3,"isEstimate":false}
        ],
        "releaseYear": [
          {"value":2020,"label":"2020","count":4,"isEstimate":false},
          {"value":null,"label":"Unknown","count":1,"isEstimate":false}
        ],
        "torrentSource": null
      }
    }`

	got, err := ExtractSearchResult(json.RawMessage(search))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	ct := got.Facets["content_type"]
	if ct["movie"] != 7 || ct["tv_show"] != 3 {
		t.Errorf("content_type facet = %v, want movie:7 tv_show:3", ct)
	}

	ry := got.Facets["release_year"]
	if ry["2020"] != 4 || ry[nullValueKey] != 1 {
		t.Errorf("release_year facet = %v, want 2020:4 <null>:1", ry)
	}

	if _, present := got.Facets["torrent_source"]; present {
		t.Errorf("null torrentSource aggregation should be omitted, got %v", got.Facets["torrent_source"])
	}
}

func TestSearchResultFromDataEnvelope(t *testing.T) {
	t.Parallel()

	body := `{"data":{"torrentContent":{"search":{
      "totalCount": 1,
	  "totalCountIsEstimate": false,
      "items": [{"infoHash":"aa","contentType":"movie","contentSource":"imdb","contentId":"tt1"}],
      "aggregations": {"contentType":[{"value":"movie","label":"Movie","count":1,"isEstimate":false}]}
    }}}}`

	got, err := SearchResultFromData(json.RawMessage(body))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(got.IDs) != 1 || got.IDs[0] != "aa:movie:imdb:tt1" {
		t.Errorf("IDs = %v, want [aa:movie:imdb:tt1]", got.IDs)
	}

	if got.Facets["content_type"]["movie"] != 1 {
		t.Errorf("content_type facet = %v", got.Facets["content_type"])
	}
}

func TestSearchResultFromAlreadyComputedResponseData(t *testing.T) {
	t.Parallel()

	data := `{"torrentContent":{"search":{
      "totalCount":1,
	  "totalCountIsEstimate":false,
      "items":[{"infoHash":"aa","contentType":"movie","contentSource":"imdb","contentId":"tt1"}],
      "aggregations":{}
    }}}`

	got, err := SearchResultFromResponseData(json.RawMessage(data))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(got.IDs) != 1 || got.IDs[0] != testInferID {
		t.Errorf("IDs = %v", got.IDs)
	}
}

func TestExtractSearchResultMissingInfoHashErrors(t *testing.T) {
	t.Parallel()

	search := `{"totalCount":1,"totalCountIsEstimate":false,"items":[{"contentType":"movie"}],"aggregations":{}}`

	if _, err := ExtractSearchResult(json.RawMessage(search)); err == nil {
		t.Fatal("expected an error for an item missing infoHash")
	}
}

func TestExtractSearchResultRequiresTopLevelPresence(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"totalCountIsEstimate":false,"items":[],"aggregations":{}}`,
		`{"totalCount":0,"items":[],"aggregations":{}}`,
		`{"totalCount":0,"totalCountIsEstimate":false,"aggregations":{}}`,
		`{"totalCount":0,"totalCountIsEstimate":false,"items":[]}`,
		`{"totalCount":0,"totalCountIsEstimate":false,"items":null,"aggregations":{}}`,
		`{"totalCount":0,"totalCountIsEstimate":false,"items":[],"aggregations":null}`,
	}

	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			t.Parallel()

			if _, err := ExtractSearchResult(json.RawMessage(body)); err == nil {
				t.Fatal("ExtractSearchResult returned nil error")
			}
		})
	}
}

func TestExtractSearchResultTracksObservedFacets(t *testing.T) {
	t.Parallel()

	search := `{
	  "totalCount":0,
	  "totalCountIsEstimate":false,
	  "items":[],
	  "aggregations":{
	    "contentType":[],
	    "releaseYear":null
	  }
	}`

	result, err := ExtractSearchResult(json.RawMessage(search))
	if err != nil {
		t.Fatalf("ExtractSearchResult error: %v", err)
	}

	if !result.ObservedFacets["content_type"] {
		t.Error("observed empty contentType facet was not tracked")
	}

	if result.ObservedFacets["release_year"] {
		t.Error("null releaseYear facet must remain unobserved")
	}
}

// TestExtractRoundTripsThroughComparison feeds two extracted responses into the
// comparator to confirm the full extract→compare path holds together.
func TestExtractRoundTripsThroughComparison(t *testing.T) {
	t.Parallel()

	body := `{"data":{"torrentContent":{"search":{
      "totalCount": 2,
	  "totalCountIsEstimate": false,
      "items": [
        {"infoHash":"aa","contentType":"movie","contentSource":"imdb","contentId":"tt1"},
        {"infoHash":"bb","contentType":"movie","contentSource":"imdb","contentId":"tt2"}
      ],
      "aggregations": {"contentType":[{"value":"movie","label":"Movie","count":2,"isEstimate":false}]}
    }}}}`

	r, err := SearchResultFromData(json.RawMessage(body))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	c := CompareGraphQL(r, r, 0, 0)
	if c.FacetsMatched != c.FacetsObserved || !c.TotalCountMatch || !c.Top1Match {
		t.Errorf("self-comparison should be a clean match: %+v", c)
	}
}
