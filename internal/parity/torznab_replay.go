package parity

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// QueryDiff records result-set parity for one Torznab query.
type QueryDiff struct {
	ID          string
	GoCount     int
	RustCount   int
	SetMatch    float64
	CountParity float64
	OrderMatch  bool
	GoOnly      []string
	RustOnly    []string
}

// ReplayReport aggregates all search-query diffs from a shadow replay.
type ReplayReport struct {
	Queries         []QueryDiff
	MeanSetMatch    float64
	MeanCountParity float64
}

// ReplayThresholds contains the aggregate Phase-1 replay gates.
type ReplayThresholds struct {
	SetMatch float64
	Count    float64
}

// DefaultReplayThresholds are the Phase-1 Lane G set/count gates.
var DefaultReplayThresholds = ReplayThresholds{SetMatch: 0.99, Count: 0.98}

// DiffInfohashLists compares ordered lists and their unique sets.
func DiffInfohashLists(id string, goList, rustList []string) QueryDiff {
	goSet := stringSet(goList)
	rustSet := stringSet(rustList)
	intersection := 0
	for hash := range goSet {
		if _, ok := rustSet[hash]; ok {
			intersection++
		}
	}

	union := len(goSet) + len(rustSet) - intersection
	setMatch := 1.0
	if union != 0 {
		setMatch = float64(intersection) / float64(union)
	}

	maxCount := max(len(goList), len(rustList))
	countParity := 1.0
	if maxCount != 0 {
		countParity = float64(min(len(goList), len(rustList))) / float64(maxCount)
	}

	diff := QueryDiff{
		ID:          id,
		GoCount:     len(goList),
		RustCount:   len(rustList),
		SetMatch:    setMatch,
		CountParity: countParity,
		OrderMatch:  slices.Equal(goList, rustList),
	}
	for hash := range goSet {
		if _, ok := rustSet[hash]; !ok {
			diff.GoOnly = append(diff.GoOnly, hash)
		}
	}
	for hash := range rustSet {
		if _, ok := goSet[hash]; !ok {
			diff.RustOnly = append(diff.RustOnly, hash)
		}
	}
	sort.Strings(diff.GoOnly)
	sort.Strings(diff.RustOnly)

	return diff
}

// DiffXMLPair extracts and compares the infohash lists in two Torznab XML documents.
func DiffXMLPair(id string, goXML, rustXML []byte) (QueryDiff, error) {
	goList, err := ExtractInfohashes(goXML)
	if err != nil {
		return QueryDiff{}, fmt.Errorf("extract Go infohashes for %q: %w", id, err)
	}
	rustList, err := ExtractInfohashes(rustXML)
	if err != nil {
		return QueryDiff{}, fmt.Errorf("extract Rust infohashes for %q: %w", id, err)
	}

	return DiffInfohashLists(id, goList, rustList), nil
}

// Gates evaluates the aggregate set/count gates.
func (report ReplayReport) Gates(setGate, countGate float64) (bool, []string) {
	var failures []string
	if report.MeanSetMatch < setGate {
		failures = append(failures, fmt.Sprintf(
			"mean set match %.6f is below gate %.6f",
			report.MeanSetMatch,
			setGate,
		))
	}
	if report.MeanCountParity < countGate {
		failures = append(failures, fmt.Sprintf(
			"mean count parity %.6f is below gate %.6f",
			report.MeanCountParity,
			countGate,
		))
	}

	return len(failures) == 0, failures
}

// String returns a deterministic one-line replay summary.
func (report ReplayReport) String() string {
	orderMatches := 0
	for _, query := range report.Queries {
		if query.OrderMatch {
			orderMatches++
		}
	}

	return fmt.Sprintf(
		"queries=%d mean_set_match=%.6f mean_count_parity=%.6f order_match=%d/%d",
		len(report.Queries),
		report.MeanSetMatch,
		report.MeanCountParity,
		orderMatches,
		len(report.Queries),
	)
}

// RunReplay fires every search query at the Go and Rust endpoints and aggregates the diffs.
func RunReplay(
	ctx context.Context,
	httpClient *http.Client,
	goBase string,
	rustBase string,
	corpus []CorpusQuery,
) (ReplayReport, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	report := ReplayReport{}
	for _, query := range corpus {
		if query.Kind != torznabQueryKindSearch {
			continue
		}

		goXML, err := replayGET(ctx, httpClient, goBase+query.Path)
		if err != nil {
			return ReplayReport{}, fmt.Errorf("query %q Go endpoint: %w", query.ID, err)
		}
		rustXML, err := replayGET(ctx, httpClient, rustBase+query.Path)
		if err != nil {
			return ReplayReport{}, fmt.Errorf("query %q Rust endpoint: %w", query.ID, err)
		}

		diff, err := DiffXMLPair(query.ID, goXML, rustXML)
		if err != nil {
			return ReplayReport{}, err
		}
		report.Queries = append(report.Queries, diff)
	}

	return aggregateReplayReport(report.Queries), nil
}

func replayGET(ctx context.Context, httpClient *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", url, err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("GET %s returned %s: %s", url, response.Status, strings.TrimSpace(string(body)))
	}

	return body, nil
}

func aggregateReplayReport(queries []QueryDiff) ReplayReport {
	report := ReplayReport{Queries: append([]QueryDiff(nil), queries...)}
	if len(queries) == 0 {
		report.MeanSetMatch = 1
		report.MeanCountParity = 1
		return report
	}

	for _, query := range queries {
		report.MeanSetMatch += query.SetMatch
		report.MeanCountParity += query.CountParity
	}
	report.MeanSetMatch /= float64(len(queries))
	report.MeanCountParity /= float64(len(queries))

	return report
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}

	return set
}
