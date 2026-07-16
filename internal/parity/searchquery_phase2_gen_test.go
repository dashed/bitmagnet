//go:build integration

package parity

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/fts"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const phase2SearchQuerySubsystem = "searchquery_phase2"

type phase2SearchFixtureInput struct {
	Options phase2SearchOptions     `json:"options"`
	Config  phase2SearchBuildConfig `json:"config"`
}

type phase2SearchOptions struct {
	Query             *string         `json:"query,omitempty"`
	Filter            json.RawMessage `json:"filter,omitempty"`
	Order             []phase2Order   `json:"order"`
	Facets            []phase2Facet   `json:"facets"`
	Limit             *uint32         `json:"limit"`
	Offset            uint32          `json:"offset"`
	TotalCount        bool            `json:"total_count"`
	HasNextPage       bool            `json:"has_next_page"`
	AggregationBudget float64         `json:"aggregation_budget"`
}

type phase2SearchBuildConfig struct {
	FileExtensionsJSONB   bool `json:"file_extensions_jsonb"`
	PopularitySortDefault bool `json:"popularity_sort_default"`
}

type phase2Order struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

type phase2Facet struct {
	Facet     string   `json:"facet"`
	Aggregate bool     `json:"aggregate"`
	Logic     *string  `json:"logic,omitempty"`
	Filter    []string `json:"filter"`
}

type phase2Scenario struct {
	ID       string
	Input    phase2SearchFixtureInput
	GoFilter query.Criteria
}

type phase2SearchResult struct {
	TotalCount           uint                                    `json:"total_count"`
	TotalCountIsEstimate bool                                    `json:"total_count_is_estimate"`
	HasNextPage          bool                                    `json:"has_next_page"`
	InferIDs             []string                                `json:"infer_ids"`
	Items                []phase2ResultItem                      `json:"items"`
	Aggregations         map[string]map[string]phase2BucketCount `json:"aggregations"`
}

type phase2BucketCount struct {
	Count      uint `json:"count"`
	IsEstimate bool `json:"is_estimate"`
}

// phase2ResultItem is the normalized shared portion of the fully hydrated Go
// TorrentContentResultItem and Rust SearchResultItem. Timestamps intentionally
// carry both the public second precision and the raw database microseconds: a
// .750123 fixture catches EXTRACT(EPOCH)::bigint rounding regressions while the
// public contract continues to normalize to Unix seconds.
type phase2ResultItem struct {
	ID                string                `json:"id"`
	InferID           string                `json:"infer_id"`
	InfoHash          string                `json:"info_hash"`
	Name              string                `json:"name"`
	Title             string                `json:"title"`
	Size              uint                  `json:"size"`
	ContentType       *string               `json:"content_type"`
	ContentSource     *string               `json:"content_source"`
	ContentID         *string               `json:"content_id"`
	Languages         []string              `json:"languages"`
	VideoResolution   *string               `json:"video_resolution"`
	VideoSource       *string               `json:"video_source"`
	VideoCodec        *string               `json:"video_codec"`
	Video3D           *string               `json:"video_3d"`
	VideoModifier     *string               `json:"video_modifier"`
	ReleaseGroup      *string               `json:"release_group"`
	Episodes          map[int][]int         `json:"episodes"`
	ReleaseYear       *uint16               `json:"release_year"`
	IMDBID            *string               `json:"imdb_id"`
	TMDBID            *string               `json:"tmdb_id"`
	Seeders           *uint                 `json:"seeders"`
	Leechers          *uint                 `json:"leechers"`
	FilesCount        *uint                 `json:"files_count"`
	InfoHashV1        *string               `json:"info_hash_v1"`
	InfoHashV2        *string               `json:"info_hash_v2"`
	QueryStringRank   float64               `json:"query_string_rank"`
	PublishedAt       int64                 `json:"published_at"`
	PublishedAtMicros int64                 `json:"published_at_micros"`
	CreatedAt         int64                 `json:"created_at"`
	UpdatedAt         int64                 `json:"updated_at"`
	TorrentContent    phase2TorrentContent  `json:"torrent_content"`
	Torrent           phase2Torrent         `json:"torrent"`
	Sources           []phase2TorrentSource `json:"sources"`
	Tags              []string              `json:"tags"`
	Content           *phase2Content        `json:"content"`
	DHTSeenCount      int                   `json:"dht_seen_count"`
	DHTFirstSeenAt    *int64                `json:"dht_first_seen_at"`
	DHTLastSeenAt     *int64                `json:"dht_last_seen_at"`
}

type phase2TorrentContent struct {
	ID              string        `json:"id"`
	InfoHash        string        `json:"info_hash"`
	ContentType     *string       `json:"content_type"`
	ContentSource   *string       `json:"content_source"`
	ContentID       *string       `json:"content_id"`
	Languages       []string      `json:"languages"`
	Episodes        map[int][]int `json:"episodes"`
	VideoResolution *string       `json:"video_resolution"`
	VideoSource     *string       `json:"video_source"`
	VideoCodec      *string       `json:"video_codec"`
	Video3D         *string       `json:"video_3d"`
	VideoModifier   *string       `json:"video_modifier"`
	ReleaseGroup    *string       `json:"release_group"`
	Seeders         *uint         `json:"seeders"`
	Leechers        *uint         `json:"leechers"`
	PublishedAt     int64         `json:"published_at"`
	Size            uint          `json:"size"`
	FilesCount      *uint         `json:"files_count"`
	CreatedAt       int64         `json:"created_at"`
	UpdatedAt       int64         `json:"updated_at"`
}

type phase2Torrent struct {
	Name           string   `json:"name"`
	Size           uint     `json:"size"`
	Private        bool     `json:"private"`
	CreatedAt      int64    `json:"created_at"`
	UpdatedAt      int64    `json:"updated_at"`
	FilesStatus    string   `json:"files_status"`
	Extension      *string  `json:"extension"`
	FilesCount     *uint    `json:"files_count"`
	FileExtensions []string `json:"file_extensions"`
	InfoHashV1     *string  `json:"info_hash_v1"`
	InfoHashV2     *string  `json:"info_hash_v2"`
	MetaVersion    *uint16  `json:"meta_version"`
}

type phase2TorrentSource struct {
	Key         string  `json:"key"`
	Name        string  `json:"name"`
	ImportID    *string `json:"import_id"`
	Seeders     *uint   `json:"seeders"`
	Leechers    *uint   `json:"leechers"`
	PublishedAt *int64  `json:"published_at"`
	SeenCount   uint    `json:"seen_count"`
	FirstSeenAt int64   `json:"first_seen_at"`
	LastSeenAt  int64   `json:"last_seen_at"`
}

type phase2Content struct {
	Type             string   `json:"type"`
	Source           string   `json:"source"`
	ID               string   `json:"id"`
	Title            string   `json:"title"`
	ReleaseYear      *uint16  `json:"release_year"`
	OriginalLanguage *string  `json:"original_language"`
	OriginalTitle    *string  `json:"original_title"`
	Overview         *string  `json:"overview"`
	Runtime          *uint16  `json:"runtime"`
	Popularity       *float32 `json:"popularity"`
	VoteAverage      *float32 `json:"vote_average"`
	VoteCount        *uint    `json:"vote_count"`
}

func TestGeneratePhase2SearchQueryParityFixtures(t *testing.T) {
	ctx := context.Background()
	db := setupSearchQueryParityDB(t, ctx)
	seedSearchQueryParityCorpus(t, ctx, db)
	seedPhase2SearchQueryCorpus(t, ctx, db)

	daoQuery := dao.Use(db)
	resource := search.New(search.Params{Query: lazy.New(func() (*dao.Query, error) {
		return daoQuery, nil
	})})
	searcher, err := resource.Search.Get()
	if err != nil {
		t.Fatalf("construct real Go search builder: %v", err)
	}

	scenarios := phase2SearchScenarios(t)
	fixtures := make([]Fixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		first := runPhase2GoOracle(t, ctx, db, searcher, scenario)
		second := runPhase2GoOracle(t, ctx, db, searcher, scenario)
		if !jsonEqual(t, first, second) {
			t.Fatalf("phase-2 scenario %q is nondeterministic", scenario.ID)
		}

		input, err := json.Marshal(scenario.Input)
		if err != nil {
			t.Fatalf("marshal phase-2 input %q: %v", scenario.ID, err)
		}
		expected, err := json.Marshal(first)
		if err != nil {
			t.Fatalf("marshal phase-2 expected %q: %v", scenario.ID, err)
		}
		fixtures = append(fixtures, Fixture{
			ID:        scenario.ID,
			Subsystem: phase2SearchQuerySubsystem,
			Input:     input,
			Expected:  expected,
		})
	}

	path := phase2SearchQueryFixturePath(t)
	writeSearchQueryFixtures(t, path, fixtures)
	t.Logf("wrote %d phase-2 search-query fixtures to %s", len(fixtures), path)
}

func seedPhase2SearchQueryCorpus(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	baseTime := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

	statements := []struct {
		SQL  string
		Args []any
	}{
		{
			SQL: "UPDATE torrent_contents SET languages = '[\"en\",\"ja\"]'::jsonb, " +
				"video_source = 'BluRay', video_codec = 'x264', video_modifier = 'REMUX', " +
				"release_group = 'PARITY', seeders = 101, leechers = 11, files_count = 2 " +
				"WHERE info_hash = ?",
			Args: []any{fixedInfoHash(1)},
		},
		{
			SQL: "UPDATE torrent_contents SET languages = '[\"fr\"]'::jsonb, " +
				"video_source = 'WEBDL', video_codec = 'x265', seeders = 55, leechers = 5, " +
				"files_count = 3 WHERE info_hash = ?",
			Args: []any{fixedInfoHash(2)},
		},
		{
			SQL: "UPDATE torrent_contents SET published_at = '2025-01-01 02:00:00.750123+00', " +
				"seeders = 34, leechers = 3, files_count = 4 WHERE info_hash = ?",
			Args: []any{fixedInfoHash(3)},
		},
		{
			SQL:  "UPDATE torrents SET file_extensions = '[\"mkv\",\"srt\"]'::jsonb, files_count = 2 WHERE info_hash = ?",
			Args: []any{fixedInfoHash(1)},
		},
		{
			SQL:  "UPDATE torrents SET file_extensions = '[\"mp4\"]'::jsonb, files_count = 3 WHERE info_hash = ?",
			Args: []any{fixedInfoHash(2)},
		},
		{
			SQL: "UPDATE content SET original_language = 'en', original_title = 'Matrix', " +
				"overview = 'Parity fixture', runtime = 136, popularity = 42.5, " +
				"vote_average = 8.2, vote_count = 24000 WHERE type = 'movie' AND source = 'tmdb' AND id = '603'",
		},
	}
	for _, statement := range statements {
		if err := db.WithContext(ctx).Exec(statement.SQL, statement.Args...).Error; err != nil {
			t.Fatalf("augment phase-2 parity seed: %v", err)
		}
	}

	collections := []model.ContentCollection{{
		Type:      "genre",
		Source:    model.SourceTmdb,
		ID:        "878",
		Name:      "Science Fiction",
		CreatedAt: baseTime,
		UpdatedAt: baseTime,
	}}
	if err := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Omit("MetadataSource").
		Create(&collections).Error; err != nil {
		t.Fatalf("insert phase-2 content collection: %v", err)
	}
	links := []model.ContentCollectionContent{{
		ContentType:             model.ContentTypeMovie,
		ContentSource:           model.SourceTmdb,
		ContentID:               "603",
		ContentCollectionType:   "genre",
		ContentCollectionSource: model.SourceTmdb,
		ContentCollectionID:     "878",
	}}
	if err := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Content", "Collection").
		Create(&links).Error; err != nil {
		t.Fatalf("insert phase-2 content collection link: %v", err)
	}

	extraSources := []model.TorrentsTorrentSource{{
		Source:      "dht",
		InfoHash:    fixedInfoHash(2),
		ImportID:    model.NewNullString("phase2"),
		Seeders:     model.NewNullUint(55),
		Leechers:    model.NewNullUint(5),
		PublishedAt: sql.NullTime{Time: baseTime.Add(2 * time.Hour), Valid: true},
		SeenCount:   9,
		CreatedAt:   baseTime.Add(time.Minute),
		UpdatedAt:   baseTime.Add(2 * time.Minute),
	}}
	if err := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Omit("TorrentSource").
		Create(&extraSources).Error; err != nil {
		t.Fatalf("insert phase-2 torrent source: %v", err)
	}

	extraTags := []model.TorrentTag{
		{InfoHash: fixedInfoHash(1), Name: "parity-2", CreatedAt: baseTime, UpdatedAt: baseTime},
		{InfoHash: fixedInfoHash(1), Name: "parity-10", CreatedAt: baseTime, UpdatedAt: baseTime},
	}
	if err := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&extraTags).Error; err != nil {
		t.Fatalf("insert phase-2 torrent tags: %v", err)
	}
}

func phase2SearchScenarios(t *testing.T) []phase2Scenario {
	t.Helper()
	limit20 := uint32(20)
	exact := func() phase2SearchFixtureInput {
		return phase2SearchFixtureInput{Options: phase2SearchOptions{
			Order:             []phase2Order{},
			Facets:            []phase2Facet{},
			Limit:             &limit20,
			TotalCount:        true,
			AggregationBudget: 0,
		}}
	}

	var scenarios []phase2Scenario
	orderFields := []string{
		"relevance", "published_at", "updated_at", "size", "files_count",
		"seeders", "leechers", "name", "info_hash",
	}
	for _, field := range orderFields {
		input := exact()
		if field == "relevance" {
			input.Options.Query = stringPointer("matrix")
		}
		input.Options.Order = []phase2Order{{Field: field, Direction: "descending"}}
		scenarios = append(scenarios, phase2Scenario{ID: "order_" + field, Input: input})
	}

	allFacets := []phase2Facet{
		{Facet: "content_type", Aggregate: true, Filter: []string{}},
		{Facet: "torrent_source", Aggregate: true, Filter: []string{}},
		{Facet: "torrent_tag", Aggregate: true, Filter: []string{}},
		{Facet: "file_type", Aggregate: true, Filter: []string{}},
		{Facet: "language", Aggregate: true, Filter: []string{}},
		{Facet: "content_genre", Aggregate: true, Filter: []string{}},
		{Facet: "release_year", Aggregate: true, Filter: []string{}},
		{Facet: "video_resolution", Aggregate: true, Filter: []string{}},
		{Facet: "video_source", Aggregate: true, Filter: []string{}},
	}
	facetsInput := exact()
	facetsInput.Options.Facets = allFacets
	facetsInput.Config.FileExtensionsJSONB = true
	scenarios = append(scenarios, phase2Scenario{ID: "facets_all_exact", Input: facetsInput})

	paging := exact()
	limit2 := uint32(2)
	paging.Options.Limit = &limit2
	paging.Options.Offset = 1
	paging.Options.HasNextPage = true
	scenarios = append(scenarios, phase2Scenario{ID: "paging_limit_plus_one", Input: paging})

	estimate := exact()
	estimate.Options.AggregationBudget = 0.000001
	scenarios = append(scenarios, phase2Scenario{ID: "total_count_estimate", Input: estimate})

	find2Off := exact()
	find2Off.Options.Query = stringPointer("matrix")
	find2Off.Options.Order = []phase2Order{{Field: "relevance", Direction: "descending"}}
	scenarios = append(scenarios, phase2Scenario{ID: "find2_off", Input: find2Off})
	find2On := find2Off
	// Go's production query layer races a CTE strategy for a query-string
	// search whose order is not relevance. A freshly migrated fixture schema
	// exposes the known losing-race error ("relation cte does not exist"). An
	// unbounded window exercises the exact FIND-2 rewrite and deterministic
	// order while deliberately bypassing that result-equivalent optimization.
	find2On.Options.Limit = nil
	find2On.Config.PopularitySortDefault = true
	scenarios = append(scenarios, phase2Scenario{ID: "find2_on", Input: find2On})

	multiFilter := andCriteria(
		orCriteria(
			contentTypeCriteria(contentTypeMovie),
			contentTypeCriteria(contentTypeTVShow),
		),
		notCriteria(videoResolutionCriteria(videoResolutionV480p)),
	)
	multiRaw, err := json.Marshal(multiFilter)
	if err != nil {
		t.Fatalf("marshal multi-criteria fixture: %v", err)
	}
	multi := exact()
	multi.Options.Filter = multiRaw
	multiGo, err := criteriaToGo(multiFilter)
	if err != nil {
		t.Fatalf("translate multi-criteria fixture: %v", err)
	}
	scenarios = append(scenarios, phase2Scenario{
		ID: "criteria_and_or", Input: multi, GoFilter: multiGo,
	})

	extension := exact()
	extension.Options.Filter = json.RawMessage(`{"file_extension_in":["mkv"]}`)
	extension.Config.FileExtensionsJSONB = true
	scenarios = append(scenarios, phase2Scenario{
		ID:       "file_extension_jsonb",
		Input:    extension,
		GoFilter: search.TorrentFileExtensionCriteria("mkv"),
	})

	microseconds := exact()
	microseconds.Options.Order = []phase2Order{{Field: "published_at", Direction: "ascending"}}
	scenarios = append(scenarios, phase2Scenario{ID: "published_at_microseconds", Input: microseconds})

	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID < scenarios[j].ID })
	return scenarios
}

func runPhase2GoOracle(
	t *testing.T,
	ctx context.Context,
	db *gorm.DB,
	searcher search.Search,
	scenario phase2Scenario,
) phase2SearchResult {
	t.Helper()
	search.SetFeatureFlags(search.FeatureFlags{
		GateFileExtensionsJSONB: scenario.Input.Config.FileExtensionsJSONB,
		PopularitySortDefault:   scenario.Input.Config.PopularitySortDefault,
	})
	defer search.SetFeatureFlags(search.FeatureFlags{})

	options, err := phase2OptionsToGo(scenario)
	if err != nil {
		t.Fatalf("translate phase-2 options %q: %v", scenario.ID, err)
	}
	result, err := searcher.TorrentContent(ctx, options...)
	if err != nil {
		t.Fatalf("execute real Go phase-2 query %q: %v", scenario.ID, err)
	}

	output := phase2SearchResult{
		TotalCount:           result.TotalCount,
		TotalCountIsEstimate: result.TotalCountIsEstimate,
		HasNextPage:          result.HasNextPage,
		InferIDs:             make([]string, 0, len(result.Items)),
		Items:                make([]phase2ResultItem, 0, len(result.Items)),
		Aggregations:         make(map[string]map[string]phase2BucketCount, len(result.Aggregations)),
	}
	for _, item := range result.Items {
		output.InferIDs = append(output.InferIDs, item.InferID())
		projected := phase2ProjectGoItem(item)
		if scenario.Input.Options.Query != nil && *scenario.Input.Options.Query != "" {
			projected.QueryStringRank = phase2SemanticQueryRank(
				t, ctx, db, item.ID, *scenario.Input.Options.Query,
			)
		}
		output.Items = append(output.Items, projected)
	}
	for facetKey, group := range result.Aggregations {
		items := make(map[string]phase2BucketCount, len(group.Items))
		for value, item := range group.Items {
			items[value] = phase2BucketCount{Count: item.Count, IsEstimate: item.IsEstimate}
		}
		output.Aggregations[facetKey] = items
	}
	return output
}

func phase2SemanticQueryRank(
	t *testing.T,
	ctx context.Context,
	db *gorm.DB,
	id string,
	appQuery string,
) float64 {
	t.Helper()
	// Go orders by `ts_rank_cd(...) AS _order_i`; GORM deliberately does not
	// scan that private order alias into ResultItem.QueryStringRank. The Phase-2
	// public item contract carries the semantic rank, so read the exact same
	// expression for the already-selected row as a normalization diagnostic.
	var rank float64
	if err := db.WithContext(ctx).Raw(
		"SELECT ts_rank_cd(tsv, ?::tsquery)::double precision FROM torrent_contents WHERE id = ?",
		fts.AppQueryToTsquery(appQuery), id,
	).Scan(&rank).Error; err != nil {
		t.Fatalf("read semantic query rank for %q: %v", id, err)
	}
	return rank
}

func phase2OptionsToGo(scenario phase2Scenario) ([]query.Option, error) {
	input := scenario.Input.Options
	options := []query.Option{
		query.WithAggregationBudget(input.AggregationBudget),
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
		query.WithTotalCount(input.TotalCount),
		query.WithHasNextPage(input.HasNextPage),
	}
	if input.Query != nil && *input.Query != "" {
		options = append(options, query.SearchString(*input.Query))
	}
	if scenario.GoFilter != nil {
		options = append(options, query.Where(scenario.GoFilter))
	}
	for _, facet := range input.Facets {
		goFacet, err := phase2FacetToGo(facet)
		if err != nil {
			return nil, err
		}
		options = append(options, query.WithFacet(goFacet))
	}
	if input.Limit != nil {
		options = append(options, query.Limit(uint(*input.Limit)))
	}
	if input.Offset > 0 {
		options = append(options, query.Offset(uint(input.Offset)))
	}

	orders, err := phase2EffectiveOrders(input, scenario.Input.Config)
	if err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		options = append(options, query.OrderBy(query.OrderByColumn{OrderByColumn: clause.OrderByColumn{
			Column: clause.Column{Table: model.TableNameTorrentContent, Name: "published_at"},
			Desc:   true,
		}}))
	} else {
		var clauses []query.OrderByColumn
		for _, order := range orders {
			clauses = append(clauses, order.Field.Clauses(order.Direction)...)
		}
		options = append(options, query.OrderBy(clauses...))
	}
	return options, nil
}

type phase2GoOrder struct {
	Field     search.TorrentContentOrderBy
	Direction search.OrderDirection
}

func phase2EffectiveOrders(
	input phase2SearchOptions,
	config phase2SearchBuildConfig,
) ([]phase2GoOrder, error) {
	hasQuery := input.Query != nil && *input.Query != ""
	orders := make([]phase2GoOrder, 0, len(input.Order))
	for _, raw := range input.Order {
		field, err := search.ParseTorrentContentOrderBy(raw.Field)
		if err != nil {
			return nil, err
		}
		if field == search.TorrentContentOrderByRelevance && !hasQuery {
			continue
		}
		direction := search.OrderDirectionAscending
		if raw.Direction == "descending" {
			direction = search.OrderDirectionDescending
		} else if raw.Direction != "ascending" {
			return nil, fmt.Errorf("invalid order direction %q", raw.Direction)
		}
		found := false
		for i := range orders {
			if orders[i].Field == field {
				orders[i].Direction = direction
				found = true
				break
			}
		}
		if !found {
			orders = append(orders, phase2GoOrder{Field: field, Direction: direction})
		}
	}
	if config.PopularitySortDefault && hasQuery && len(orders) == 1 &&
		orders[0].Field == search.TorrentContentOrderByRelevance {
		return []phase2GoOrder{{
			Field: search.TorrentContentOrderBySeeders, Direction: search.OrderDirectionDescending,
		}}, nil
	}
	return orders, nil
}

func phase2FacetToGo(input phase2Facet) (query.Facet, error) {
	filter := make(query.FacetFilter, len(input.Filter))
	for _, value := range input.Filter {
		filter[value] = struct{}{}
	}
	options := []query.FacetOption{query.FacetHasFilter(filter)}
	if input.Aggregate {
		options = append(options, query.FacetIsAggregated())
	}
	if input.Logic != nil {
		switch *input.Logic {
		case "and":
			options = append(options, query.FacetUsesAndLogic())
		case "or":
			options = append(options, query.FacetUsesOrLogic())
		default:
			return nil, fmt.Errorf("invalid facet logic %q", *input.Logic)
		}
	}
	switch input.Facet {
	case "content_type":
		return search.TorrentContentTypeFacet(options...), nil
	case "torrent_source":
		return search.TorrentSourceFacet(options...), nil
	case "torrent_tag":
		return search.TorrentTagsFacet(options...), nil
	case "file_type":
		return search.TorrentFileTypeFacet(options...), nil
	case "language":
		return search.TorrentContentLanguageFacet(options...), nil
	case "content_genre":
		return search.TorrentContentGenreFacet(options...), nil
	case "release_year":
		return search.ReleaseYearFacet(options...), nil
	case "video_resolution":
		return search.VideoResolutionFacet(options...), nil
	case "video_source":
		return search.VideoSourceFacet(options...), nil
	default:
		return nil, fmt.Errorf("unsupported phase-2 facet %q", input.Facet)
	}
}

func phase2ProjectGoItem(item search.TorrentContentResultItem) phase2ResultItem {
	contentType := nullContentTypeString(item.ContentType)
	sources := make([]phase2TorrentSource, 0, len(item.Torrent.Sources))
	for _, source := range item.Torrent.Sources {
		sources = append(sources, phase2TorrentSource{
			Key:         source.Source,
			Name:        source.TorrentSource.Name,
			ImportID:    nullStringPointer(source.ImportID),
			Seeders:     nullUintPointer(source.Seeders),
			Leechers:    nullUintPointer(source.Leechers),
			PublishedAt: nullTimeUnixPointer(source.PublishedAt),
			SeenCount:   source.SeenCount,
			FirstSeenAt: source.CreatedAt.Unix(),
			LastSeenAt:  source.UpdatedAt.Unix(),
		})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Key < sources[j].Key })
	tags := make([]string, 0, len(item.Torrent.Tags))
	for _, tag := range item.Torrent.Tags {
		tags = append(tags, tag.Name)
	}
	sort.Strings(tags)

	firstSeenAt, lastSeenAt, seenCount := dhtSeenStats(item.Torrent.Sources)
	episodes := normalizedEpisodes(item.Episodes)
	var content *phase2Content
	if item.Content.ID != "" {
		content = &phase2Content{
			Type:             item.Content.Type.String(),
			Source:           item.Content.Source,
			ID:               item.Content.ID,
			Title:            item.Content.Title,
			ReleaseYear:      yearPointer(item.Content.ReleaseYear),
			OriginalLanguage: nullLanguagePointer(item.Content.OriginalLanguage),
			OriginalTitle:    nullStringPointer(item.Content.OriginalTitle),
			Overview:         nullStringPointer(item.Content.Overview),
			Runtime:          nullUint16Pointer(item.Content.Runtime),
			Popularity:       nullFloat32Pointer(item.Content.Popularity),
			VoteAverage:      nullFloat32Pointer(item.Content.VoteAverage),
			VoteCount:        nullUintPointer(item.Content.VoteCount),
		}
	}

	return phase2ResultItem{
		ID:                item.ID,
		InferID:           item.InferID(),
		InfoHash:          strings.ToLower(item.InfoHash.String()),
		Name:              item.Torrent.Name,
		Title:             item.Title(),
		Size:              item.Size,
		ContentType:       contentType,
		ContentSource:     nullStringPointer(item.ContentSource),
		ContentID:         nullStringPointer(item.ContentID),
		Languages:         languageIDs(item.Languages),
		VideoResolution:   nullVideoResolutionPointer(item.VideoResolution),
		VideoSource:       nullVideoSourcePointer(item.VideoSource),
		VideoCodec:        nullVideoCodecPointer(item.VideoCodec),
		Video3D:           nullVideo3DPointer(item.Video3D),
		VideoModifier:     nullVideoModifierPointer(item.VideoModifier),
		ReleaseGroup:      nullStringPointer(item.ReleaseGroup),
		Episodes:          episodes,
		ReleaseYear:       yearPointer(item.Content.ReleaseYear),
		IMDBID:            contentIdentifierPointer(item.Content, model.SourceImdb),
		TMDBID:            contentIdentifierPointer(item.Content, model.SourceTmdb),
		Seeders:           nullUintPointer(item.Torrent.Seeders()),
		Leechers:          nullUintPointer(item.Torrent.Leechers()),
		FilesCount:        nullUintPointer(item.FilesCount),
		InfoHashV1:        protocolIDPointer(item.Torrent.InfoHashV1),
		InfoHashV2:        protocolV2Pointer(item.Torrent.InfoHashV2),
		QueryStringRank:   item.QueryStringRank,
		PublishedAt:       item.PublishedAt.Unix(),
		PublishedAtMicros: item.PublishedAt.UnixMicro(),
		CreatedAt:         item.CreatedAt.Unix(),
		UpdatedAt:         item.UpdatedAt.Unix(),
		TorrentContent: phase2TorrentContent{
			ID:              item.ID,
			InfoHash:        strings.ToLower(item.InfoHash.String()),
			ContentType:     contentType,
			ContentSource:   nullStringPointer(item.ContentSource),
			ContentID:       nullStringPointer(item.ContentID),
			Languages:       languageIDs(item.Languages),
			Episodes:        episodes,
			VideoResolution: nullVideoResolutionPointer(item.VideoResolution),
			VideoSource:     nullVideoSourcePointer(item.VideoSource),
			VideoCodec:      nullVideoCodecPointer(item.VideoCodec),
			Video3D:         nullVideo3DPointer(item.Video3D),
			VideoModifier:   nullVideoModifierPointer(item.VideoModifier),
			ReleaseGroup:    nullStringPointer(item.ReleaseGroup),
			Seeders:         nullUintPointer(item.Seeders),
			Leechers:        nullUintPointer(item.Leechers),
			PublishedAt:     item.PublishedAt.Unix(),
			Size:            item.Size,
			FilesCount:      nullUintPointer(item.FilesCount),
			CreatedAt:       item.CreatedAt.Unix(),
			UpdatedAt:       item.UpdatedAt.Unix(),
		},
		Torrent: phase2Torrent{
			Name:        item.Torrent.Name,
			Size:        item.Torrent.Size,
			Private:     item.Torrent.Private,
			CreatedAt:   item.Torrent.CreatedAt.Unix(),
			UpdatedAt:   item.Torrent.UpdatedAt.Unix(),
			FilesStatus: item.Torrent.FilesStatus.String(),
			Extension:   nullStringPointer(item.Torrent.Extension),
			FilesCount:  nullUintPointer(item.Torrent.FilesCount),
			// GORM preserves a SQL NULL JSONB column as a nil slice while the
			// Rust hydrator deliberately coalesces it to an empty collection.
			// Normalize the shared collection contract to [] on both sides.
			FileExtensions: append([]string{}, item.Torrent.FileExts...),
			InfoHashV1:     protocolIDPointer(item.Torrent.InfoHashV1),
			InfoHashV2:     protocolV2Pointer(item.Torrent.InfoHashV2),
			MetaVersion:    nullUint16Pointer(item.Torrent.MetaVersion),
		},
		Sources:        sources,
		Tags:           tags,
		Content:        content,
		DHTSeenCount:   seenCount,
		DHTFirstSeenAt: firstSeenAt,
		DHTLastSeenAt:  lastSeenAt,
	}
}

func dhtSeenStats(sources []model.TorrentsTorrentSource) (*int64, *int64, int) {
	for _, source := range sources {
		if source.Source == "dht" {
			first, last := source.CreatedAt.Unix(), source.UpdatedAt.Unix()
			return &first, &last, int(source.SeenCount)
		}
	}
	return nil, nil, 0
}

func languageIDs(languages model.Languages) []string {
	values := languages.Slice()
	ids := make([]string, 0, len(values))
	for _, language := range values {
		ids = append(ids, language.ID())
	}
	return ids
}

func normalizedEpisodes(episodes model.Episodes) map[int][]int {
	result := make(map[int][]int, len(episodes))
	for season, values := range episodes {
		items := make([]int, 0, len(values))
		for episode := range values {
			items = append(items, episode)
		}
		sort.Ints(items)
		result[season] = items
	}
	return result
}

func contentIdentifierPointer(content model.Content, source string) *string {
	if content.ID == "" {
		return nil
	}
	value, ok := content.Identifier(source)
	if !ok {
		return nil
	}
	return &value
}

func nullContentTypeString(value model.NullContentType) *string {
	if !value.Valid {
		return nil
	}
	result := value.ContentType.String()
	return &result
}

func nullStringPointer(value model.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func nullUintPointer(value model.NullUint) *uint {
	if !value.Valid {
		return nil
	}
	result := value.Uint
	return &result
}

func nullUint16Pointer(value model.NullUint16) *uint16 {
	if !value.Valid {
		return nil
	}
	result := value.Uint16
	return &result
}

func nullFloat32Pointer(value model.NullFloat32) *float32 {
	if !value.Valid {
		return nil
	}
	result := value.Float32
	return &result
}

func nullTimeUnixPointer(value sql.NullTime) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Time.Unix()
	return &result
}

func yearPointer(value model.Year) *uint16 {
	if value.IsNil() {
		return nil
	}
	result := uint16(value)
	return &result
}

func nullLanguagePointer(value model.NullLanguage) *string {
	if !value.Valid {
		return nil
	}
	result := value.Language.ID()
	return &result
}

func nullVideoResolutionPointer(value model.NullVideoResolution) *string {
	if !value.Valid {
		return nil
	}
	result := value.VideoResolution.String()
	return &result
}

func nullVideoSourcePointer(value model.NullVideoSource) *string {
	if !value.Valid {
		return nil
	}
	result := value.VideoSource.String()
	return &result
}

func nullVideoCodecPointer(value model.NullVideoCodec) *string {
	if !value.Valid {
		return nil
	}
	result := value.VideoCodec.String()
	return &result
}

func nullVideo3DPointer(value model.NullVideo3D) *string {
	if !value.Valid {
		return nil
	}
	result := value.Video3D.String()
	return &result
}

func nullVideoModifierPointer(value model.NullVideoModifier) *string {
	if !value.Valid {
		return nil
	}
	result := value.VideoModifier.String()
	return &result
}

func protocolIDPointer(value *protocol.ID) *string {
	if value == nil {
		return nil
	}
	result := value.String()
	return &result
}

func protocolV2Pointer(value *protocol.InfoHashV2) *string {
	if value == nil {
		return nil
	}
	result := value.String()
	return &result
}

func jsonEqual(t *testing.T, left, right any) bool {
	t.Helper()
	leftJSON, err := json.Marshal(left)
	if err != nil {
		t.Fatalf("marshal first deterministic result: %v", err)
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		t.Fatalf("marshal second deterministic result: %v", err)
	}
	return string(leftJSON) == string(rightJSON)
}

func phase2SearchQueryFixturePath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve phase-2 search-query generator source path")
	}
	return filepath.Clean(filepath.Join(
		filepath.Dir(filename), "..", "..", "testdata", "parity", "searchquery", "graphql_search.jsonl",
	))
}
