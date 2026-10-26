package graphqlshadow

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// nullValueKey is the facet-count key used for an aggregation item whose value
// is null (the null bucket). It is applied identically to both sides, so the
// exact sentinel does not affect the diff — it only needs to be stable and
// distinct from any real value string.
const (
	jsonNull     = "null"
	nullValueKey = "<null>"
)

// aggFieldToFacetKey maps each GraphQL TorrentContentAggregations field name (as
// it appears in the JSON response) to the canonical facet key the comparator
// diffs on.
var aggFieldToFacetKey = map[string]string{
	"contentType":     "content_type",
	"torrentSource":   "torrent_source",
	"torrentTag":      "torrent_tag",
	"torrentFileType": "file_type",
	"language":        "language",
	"genre":           "content_genre",
	"releaseYear":     "release_year",
	"videoResolution": "video_resolution",
	"videoSource":     "video_source",
}

// searchResponse is the subset of a torrentContent.search result the shadow
// diffs. Field names match the GraphQL response JSON (the resolver's camelCase
// output). A shadow query MUST select the item identity fields (infoHash,
// contentType, contentSource, contentId) and any facets it wants compared.
type searchResponse struct {
	TotalCount           *int            `json:"totalCount"`
	TotalCountIsEstimate *bool           `json:"totalCountIsEstimate"`
	Items                json.RawMessage `json:"items"`
	Aggregations         json.RawMessage `json:"aggregations"`
}

type searchItem struct {
	ID            string  `json:"id"`
	InfoHash      string  `json:"infoHash"`
	ContentType   *string `json:"contentType"`
	ContentSource *string `json:"contentSource"`
	ContentID     *string `json:"contentId"`
}

type aggItem struct {
	// Value is decoded permissively as raw JSON: enum facets emit a string,
	// releaseYear emits a number, and the null bucket emits null. aggValueKey
	// normalises each to a stable string key.
	Value      json.RawMessage `json:"value"`
	Label      *string         `json:"label"`
	Count      *int            `json:"count"`
	IsEstimate *bool           `json:"isEstimate"`
}

type responseEnvelope struct {
	Data json.RawMessage `json:"data"`
}

type responseData struct {
	TorrentContent torrentContentResponseData `json:"torrentContent"`
}

type torrentContentResponseData struct {
	Search json.RawMessage `json:"search"`
}

// ExtractSearchResult builds the engine-agnostic GraphQLResult from the JSON of
// a single torrentContent.search result object (the value at
// data.torrentContent.search — navigate to it first with SearchResultFromData,
// or pass a directly-obtained result). It prefers the public id field, which is
// already model.TorrentContent.InferID. For callers that select the four raw
// identity fields instead, it reconstructs the same stable key with the exact
// model recipe.
func ExtractSearchResult(searchObj json.RawMessage) (GraphQLResult, error) {
	var resp searchResponse
	if err := json.Unmarshal(searchObj, &resp); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode search result: %w", err)
	}

	if resp.TotalCount == nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: search result missing totalCount")
	}

	if resp.TotalCountIsEstimate == nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: search result missing totalCountIsEstimate")
	}

	if len(resp.Items) == 0 || string(resp.Items) == jsonNull {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: search result missing items")
	}

	if len(resp.Aggregations) == 0 || string(resp.Aggregations) == jsonNull {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: search result missing aggregations")
	}

	var items []searchItem
	if err := json.Unmarshal(resp.Items, &items); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode search items: %w", err)
	}

	ids := make([]string, 0, len(items))

	for i, item := range items {
		// The public GraphQL id is already TorrentContent.InferID and is selected by
		// the real webui fragment. Prefer it so the embedded hook can compare the
		// original request without injecting hidden identity fields.
		if item.ID != "" {
			ids = append(ids, item.ID)

			continue
		}

		if item.InfoHash == "" {
			return GraphQLResult{}, fmt.Errorf(
				"graphqlshadow: item %d missing id and infoHash "+
					"(query must select id or the identity fields)", i)
		}

		ids = append(ids, inferID(item))
	}

	facets, observedFacets, err := extractFacets(resp.Aggregations)
	if err != nil {
		return GraphQLResult{}, err
	}

	return GraphQLResult{
		IDs:                  ids,
		TotalCount:           *resp.TotalCount,
		TotalCountIsEstimate: *resp.TotalCountIsEstimate,
		Facets:               facets,
		ObservedFacets:       observedFacets,
	}, nil
}

// SearchResultFromData navigates a full GraphQL response body ({"data": {...}})
// to the torrentContent.search result object and extracts it. It is a
// convenience for the common webui/Hermes query shape; queries that alias the
// path must extract the search object themselves and call ExtractSearchResult.
func SearchResultFromData(responseBody json.RawMessage) (GraphQLResult, error) {
	var envelope responseEnvelope

	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode response envelope: %w", err)
	}

	return SearchResultFromResponseData(envelope.Data)
}

// SearchResultFromResponseData navigates gqlgen's already-computed Response.Data
// object ({"torrentContent": {"search": ...}}) to the search result. Unlike
// SearchResultFromData it does not expect the outer {"data": ...} HTTP envelope;
// this is the projection used by the embedded response hook, so the live Go
// operation is never re-issued just to obtain a reference value.
func SearchResultFromResponseData(data json.RawMessage) (GraphQLResult, error) {
	var responseData responseData

	if err := json.Unmarshal(data, &responseData); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode response data: %w", err)
	}

	if len(responseData.TorrentContent.Search) == 0 {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: response has no data.torrentContent.search object")
	}

	return ExtractSearchResult(responseData.TorrentContent.Search)
}

// inferID reconstructs the torrentContent stable key when the response did not
// select the canonical id field. It mirrors
// model.TorrentContent.InferID exactly: content source and id are bound
// together, so an absent source yields "?" for BOTH source and id.
func inferID(item searchItem) string {
	contentType := "?"
	if item.ContentType != nil {
		contentType = *item.ContentType
	}

	source, id := "?", "?"
	if item.ContentSource != nil {
		source = *item.ContentSource

		id = ""
		if item.ContentID != nil {
			id = *item.ContentID
		}
	}

	return item.InfoHash + ":" + contentType + ":" + source + ":" + id
}

// extractFacets decodes the aggregations object into per-facet value→count maps
// keyed by the canonical facet key. An absent aggregation field means the query
// did not request that facet and is unobserved. A present empty list is observed
// evidence and remains distinct from an absent or null field.
func extractFacets(
	aggregations json.RawMessage,
) (map[string]FacetCounts, map[string]bool, error) {
	if len(aggregations) == 0 {
		return nil, nil, fmt.Errorf("graphqlshadow: aggregations are missing")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(aggregations, &raw); err != nil {
		return nil, nil, fmt.Errorf("graphqlshadow: decode aggregations: %w", err)
	}

	facets := map[string]FacetCounts{}
	observed := map[string]bool{}

	for field, facetKey := range aggFieldToFacetKey {
		rawItems, ok := raw[field]
		if !ok || len(rawItems) == 0 || string(rawItems) == jsonNull {
			continue
		}

		observed[facetKey] = true

		var items []aggItem
		if err := json.Unmarshal(rawItems, &items); err != nil {
			return nil, nil, fmt.Errorf("graphqlshadow: decode %s aggregation: %w", field, err)
		}

		counts := make(FacetCounts, len(items))

		for i, it := range items {
			if len(it.Value) == 0 || it.Label == nil || it.Count == nil || it.IsEstimate == nil {
				return nil, nil, fmt.Errorf(
					"graphqlshadow: %s aggregation item %d is incomplete", field, i)
			}

			counts[aggValueKey(it.Value)] = *it.Count
		}

		facets[facetKey] = counts
	}

	return facets, observed, nil
}

func aggValueKey(v json.RawMessage) string {
	if len(v) == 0 || string(v) == jsonNull {
		return nullValueKey
	}

	// A JSON string value (enum facets): unquote it.
	if v[0] == '"' {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			if s == "" {
				return nullValueKey
			}

			return s
		}
	}

	// Otherwise a numeric literal (releaseYear): normalise so 2020 and 2020.0
	// map to one key.
	if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
		return strconv.FormatInt(n, 10)
	}

	return string(v)
}
