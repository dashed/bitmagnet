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
const nullValueKey = "<null>"

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
	TotalCount           int             `json:"totalCount"`
	TotalCountIsEstimate bool            `json:"totalCountIsEstimate"`
	Items                []searchItem    `json:"items"`
	Aggregations         json.RawMessage `json:"aggregations"`
}

type searchItem struct {
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
	Count      int             `json:"count"`
	IsEstimate bool            `json:"isEstimate"`
}

// ExtractSearchResult builds the engine-agnostic GraphQLResult from the JSON of
// a single torrentContent.search result object (the value at
// data.torrentContent.search — navigate to it first with SearchResultFromData,
// or pass a directly-obtained result). It reconstructs each item's InferID with
// the exact recipe model.TorrentContent.InferID uses
// (hex(info_hash):content_type:content_source:content_id, with "?" fillers for
// an absent content type / source / id, and content source + id bound together).
func ExtractSearchResult(searchObj json.RawMessage) (GraphQLResult, error) {
	var resp searchResponse
	if err := json.Unmarshal(searchObj, &resp); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode search result: %w", err)
	}

	ids := make([]string, 0, len(resp.Items))

	for i, item := range resp.Items {
		if item.InfoHash == "" {
			return GraphQLResult{}, fmt.Errorf(
				"graphqlshadow: item %d missing infoHash (shadow query must select the identity fields)", i)
		}

		ids = append(ids, inferID(item))
	}

	facets, err := extractFacets(resp.Aggregations)
	if err != nil {
		return GraphQLResult{}, err
	}

	return GraphQLResult{
		IDs:                  ids,
		TotalCount:           resp.TotalCount,
		TotalCountIsEstimate: resp.TotalCountIsEstimate,
		Facets:               facets,
	}, nil
}

// SearchResultFromData navigates a full GraphQL response body ({"data": {...}})
// to the torrentContent.search result object and extracts it. It is a
// convenience for the common webui/Hermes query shape; queries that alias the
// path must extract the search object themselves and call ExtractSearchResult.
func SearchResultFromData(responseBody json.RawMessage) (GraphQLResult, error) {
	var envelope struct {
		Data struct {
			TorrentContent struct {
				Search json.RawMessage `json:"search"`
			} `json:"torrentContent"`
		} `json:"data"`
	}

	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: decode response envelope: %w", err)
	}

	if len(envelope.Data.TorrentContent.Search) == 0 {
		return GraphQLResult{}, fmt.Errorf("graphqlshadow: response has no data.torrentContent.search object")
	}

	return ExtractSearchResult(envelope.Data.TorrentContent.Search)
}

// inferID reconstructs the torrentContent stable key. It mirrors
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
// did not request that facet; it is simply omitted (an empty facet on this side).
func extractFacets(aggregations json.RawMessage) (map[string]FacetCounts, error) {
	if len(aggregations) == 0 {
		return nil, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(aggregations, &raw); err != nil {
		return nil, fmt.Errorf("graphqlshadow: decode aggregations: %w", err)
	}

	facets := map[string]FacetCounts{}

	for field, facetKey := range aggFieldToFacetKey {
		rawItems, ok := raw[field]
		if !ok || len(rawItems) == 0 || string(rawItems) == "null" {
			continue
		}

		var items []aggItem
		if err := json.Unmarshal(rawItems, &items); err != nil {
			return nil, fmt.Errorf("graphqlshadow: decode %s aggregation: %w", field, err)
		}

		counts := make(FacetCounts, len(items))

		for _, it := range items {
			counts[aggValueKey(it.Value)] = it.Count
		}

		facets[facetKey] = counts
	}

	return facets, nil
}

func aggValueKey(v json.RawMessage) string {
	if len(v) == 0 || string(v) == "null" {
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
