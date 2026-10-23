//go:build integration

package parity

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// updateTorrentHintDecodeParity regenerates the torrent hint decode golden
// instead of verifying against it.
var updateTorrentHintDecodeParity = flag.Bool(
	"update-torrent-hint-decode-parity",
	false,
	"regenerate the torrent hint decode parity golden",
)

const torrentHintDecodeSubsystem = "torrent_hint_decode"

// torrentHintDecodeRow is one `torrent_hints` row exactly as it is stored. The
// jsonb columns hold PostgreSQL's own text rendering, which the generator
// checks, so a consumer that round-trips them through jsonb sees these bytes.
type torrentHintDecodeRow struct {
	ContentType     string  `json:"contentType"`
	ContentSource   *string `json:"contentSource"`
	ContentID       *string `json:"contentId"`
	Title           *string `json:"title"`
	ReleaseYear     *int64  `json:"releaseYear"`
	Languages       *string `json:"languages"`
	Episodes        *string `json:"episodes"`
	VideoResolution *string `json:"videoResolution"`
	VideoSource     *string `json:"videoSource"`
	VideoCodec      *string `json:"videoCodec"`
	Video3D         *string `json:"video3d"`
	VideoModifier   *string `json:"videoModifier"`
	ReleaseGroup    *string `json:"releaseGroup"`
}

type torrentHintDecodeInput struct {
	Row torrentHintDecodeRow `json:"row"`
	// Twin names the case this one differs from only in title and
	// release_year. The generator fails unless both decode identically, which
	// is the proof that Go does not read those columns.
	Twin string `json:"twin,omitempty"`
}

// torrentHintDecodeExpected is Go's view of the row: whether the processor's
// GORM load fails, and otherwise the hint as loaded and the classification
// `ApplyHint` makes of it.
type torrentHintDecodeExpected struct {
	LoadError          string                       `json:"loadError,omitempty"`
	ContentSource      *string                      `json:"contentSource,omitempty"`
	ContentID          *string                      `json:"contentId,omitempty"`
	HasHintedContentID bool                         `json:"hasHintedContentId"`
	Applied            *classifier.NormalizedResult `json:"applied,omitempty"`
	// AppliedEpisodesJSON is how GORM would persist the applied episodes into
	// `torrent_contents.episodes`: `null` when nothing was applied.
	AppliedEpisodesJSON string `json:"appliedEpisodesJson,omitempty"`
}

type torrentHintDecodeCase struct {
	id   string
	twin string
	row  torrentHintDecodeRow
}

func hintString(value string) *string { return &value }

func hintYear(value int64) *int64 { return &value }

func torrentHintDecodeCorpus() []torrentHintDecodeCase {
	bare := func(contentType string) torrentHintDecodeRow {
		return torrentHintDecodeRow{ContentType: contentType}
	}
	movie := func(edit func(*torrentHintDecodeRow)) torrentHintDecodeRow {
		row := bare("movie")
		edit(&row)
		return row
	}
	sourced := func(source, id string) func(*torrentHintDecodeRow) {
		return func(row *torrentHintDecodeRow) {
			row.ContentSource = hintString(source)
			row.ContentID = hintString(id)
		}
	}
	cases := []torrentHintDecodeCase{
		{id: "type_empty", row: bare("")},
		{id: "type_unknown", row: bare("book")},
		{id: "type_upper", row: bare("Movie")},
		{id: "type_trailing_space", row: bare("movie ")},
	}
	for _, contentType := range model.ContentTypeValues() {
		cases = append(cases, torrentHintDecodeCase{
			id:  "type_" + contentType.String(),
			row: bare(contentType.String()),
		})
	}
	cases = append(cases,
		torrentHintDecodeCase{id: "sourced_imdb", row: movie(sourced("imdb", "tt0133093"))},
		torrentHintDecodeCase{id: "sourced_tmdb", row: movie(sourced("tmdb", "603"))},
		torrentHintDecodeCase{id: "sourced_tvdb_tv", row: torrentHintDecodeRow{
			ContentType: "tv_show", ContentSource: hintString("tvdb"), ContentID: hintString("81189"),
		}},
		torrentHintDecodeCase{id: "sourced_xxx_imdb", row: torrentHintDecodeRow{
			ContentType: "xxx", ContentSource: hintString("imdb"), ContentID: hintString("tt0099044"),
		}},
		torrentHintDecodeCase{id: "sourced_empty_source", row: movie(sourced("", "tt0133093"))},
		torrentHintDecodeCase{id: "sourced_empty_id", row: movie(sourced("imdb", ""))},
		torrentHintDecodeCase{id: "sourced_id_space", row: movie(sourced("imdb", "tt 0133093"))},
		torrentHintDecodeCase{id: "sourced_id_slash", row: movie(sourced("imdb", "tt0133093/x"))},
		torrentHintDecodeCase{id: "sourced_id_dotdot", row: movie(sourced("imdb", ".."))},
		torrentHintDecodeCase{id: "sourced_id_dot_lead", row: movie(sourced("imdb", ".tt1"))},
		torrentHintDecodeCase{id: "sourced_id_charset", row: movie(sourced("imdb", "tt-01._x"))},
		torrentHintDecodeCase{id: "nullsrc_with_id", row: movie(func(row *torrentHintDecodeRow) {
			row.ContentID = hintString("tt0133093")
		})},
		torrentHintDecodeCase{id: "nullsrc_with_empty_id", row: movie(func(row *torrentHintDecodeRow) {
			row.ContentID = hintString("")
		})},

		torrentHintDecodeCase{id: "title_year_bare", twin: "type_movie", row: movie(func(row *torrentHintDecodeRow) {
			row.Title = hintString("The Matrix")
			row.ReleaseYear = hintYear(1999)
		})},
		torrentHintDecodeCase{id: "title_only_empty", twin: "type_movie", row: movie(func(row *torrentHintDecodeRow) {
			row.Title = hintString("")
		})},
		torrentHintDecodeCase{id: "year_zero", twin: "type_movie", row: movie(func(row *torrentHintDecodeRow) {
			row.ReleaseYear = hintYear(0)
		})},
		torrentHintDecodeCase{id: "year_negative", twin: "type_movie", row: movie(func(row *torrentHintDecodeRow) {
			row.ReleaseYear = hintYear(-5)
		})},
		torrentHintDecodeCase{id: "year_over_uint16", twin: "type_movie", row: movie(func(row *torrentHintDecodeRow) {
			row.ReleaseYear = hintYear(70000)
		})},
		torrentHintDecodeCase{id: "title_year_sourced", twin: "sourced_imdb", row: movie(func(row *torrentHintDecodeRow) {
			sourced("imdb", "tt0133093")(row)
			row.Title = hintString("Matrix, The")
			row.ReleaseYear = hintYear(1901)
		})},
		torrentHintDecodeCase{id: "title_year_enriched", twin: "prod_enriched_video", row: movie(func(row *torrentHintDecodeRow) {
			row.Languages = hintString("[]")
			row.VideoResolution = hintString("V720p")
			row.VideoCodec = hintString("x264")
			row.Title = hintString("Another Title")
			row.ReleaseYear = hintYear(2042)
		})},

		torrentHintDecodeCase{id: "prod_sourced_video", row: movie(func(row *torrentHintDecodeRow) {
			sourced("imdb", "tt0099044")(row)
			row.Languages = hintString("[]")
			row.VideoResolution = hintString("V1080p")
			row.VideoCodec = hintString("XviD")
		})},
		torrentHintDecodeCase{id: "prod_enriched_video", row: movie(func(row *torrentHintDecodeRow) {
			row.Languages = hintString("[]")
			row.VideoResolution = hintString("V720p")
			row.VideoCodec = hintString("x264")
		})},
	)

	for _, languages := range []struct{ id, value string }{
		{"empty", `[]`},
		{"json_null", `null`},
		{"en", `["en"]`},
		{"two_out_of_order", `["fr", "en"]`},
		{"duplicate", `["en", "en"]`},
		{"upper", `["EN"]`},
		{"alpha3", `["eng"]`},
		{"name", `["english"]`},
		{"invalid", `["xx"]`},
		{"empty_string", `[""]`},
		{"number", `[1]`},
		{"null_element", `[null]`},
		{"object", `{}`},
		{"string", `"en"`},
	} {
		cases = append(cases, torrentHintDecodeCase{
			id: "languages_" + languages.id,
			row: movie(func(row *torrentHintDecodeRow) {
				row.Languages = hintString(languages.value)
			}),
		})
	}
	cases = append(cases, torrentHintDecodeCase{id: "languages_sql_null", row: bare("movie")})

	for _, episodes := range []struct{ id, value string }{
		{"json_null", `null`},
		{"empty", `{}`},
		{"season", `{"1": {}}`},
		{"episode", `{"1": {"2": {}}}`},
		{"multi_season", `{"1": {}, "2": {}, "3": {}}`},
		{"gapped_seasons", `{"1": {}, "3": {}}`},
		{"mixed", `{"1": {"2": {}, "3": {}}, "2": {}}`},
		{"episode_range", `{"2": {"1": {}, "2": {}, "3": {}, "5": {}}}`},
		{"episode_100", `{"1": {"100": {}}}`},
		{"season_100", `{"100": {}}`},
		{"season_zero", `{"0": {}}`},
		{"season_i64_max", `{"9223372036854775807": {}}`},
		{"null_season", `{"1": null}`},
		{"null_episode", `{"1": {"2": null}}`},
		{"leading_zero", `{"01": {}}`},
		{"negative", `{"-1": {}}`},
		{"plus", `{"+1": {}}`},
		{"episode_fields", `{"1": {"2": {"x": 1}}}`},
		{"non_numeric_key", `{"a": {}}`},
		{"array", `[]`},
		{"season_number", `{"1": 1}`},
		{"episode_number", `{"1": {"2": 1}}`},
		{"overflow", `{"99999999999999999999": {}}`},
	} {
		cases = append(cases, torrentHintDecodeCase{
			id: "episodes_" + episodes.id,
			row: torrentHintDecodeRow{
				ContentType: "tv_show",
				Episodes:    hintString(episodes.value),
			},
		})
	}
	// Applying `{}` is observable only on a content type that never parses
	// video attributes, where the parser cannot overwrite it.
	cases = append(cases, torrentHintDecodeCase{
		id:  "episodes_empty_music",
		row: torrentHintDecodeRow{ContentType: "music", Episodes: hintString(`{}`)},
	})

	video := func(field string, set func(*torrentHintDecodeRow, string), values ...string) {
		for _, value := range values {
			cases = append(cases, torrentHintDecodeCase{
				id: fmt.Sprintf("%s_%+q", field, value),
				row: movie(func(row *torrentHintDecodeRow) {
					set(row, value)
				}),
			})
		}
	}
	videoEdgeCases := func(lower, upper string) []string {
		return []string{lower, upper, "", " " + upper, "unknown"}
	}
	resolutions := model.VideoResolutionNames()
	video("video_resolution", func(row *torrentHintDecodeRow, value string) { row.VideoResolution = &value },
		append(resolutions, videoEdgeCases("v1080p", "V1080P")...)...)
	video("video_source", func(row *torrentHintDecodeRow, value string) { row.VideoSource = &value },
		append(model.VideoSourceNames(), append(videoEdgeCases("bluray", "BLURAY"),
			"webRIP", "WORKPRINT", "WEBRİP")...)...)
	video("video_codec", func(row *torrentHintDecodeRow, value string) { row.VideoCodec = &value },
		append(model.VideoCodecNames(), append(videoEdgeCases("xvid", "X264"), "DIVX")...)...)
	video("video_3d", func(row *torrentHintDecodeRow, value string) { row.Video3D = &value },
		append(model.Video3DNames(), videoEdgeCases("v3dsbs", "v3DoU")...)...)
	video("video_modifier", func(row *torrentHintDecodeRow, value string) { row.VideoModifier = &value },
		append(model.VideoModifierNames(), videoEdgeCases("remux", "ReMuX")...)...)

	for _, group := range []struct{ id, value string }{
		{"set", "GROUP"},
		{"empty", ""},
		{"space", " "},
	} {
		cases = append(cases, torrentHintDecodeCase{
			id: "release_group_" + group.id,
			row: movie(func(row *torrentHintDecodeRow) {
				row.ReleaseGroup = hintString(group.value)
			}),
		})
	}
	// The empty group must reach a non-video type intact too, where no parser
	// runs after the hint.
	cases = append(cases, torrentHintDecodeCase{
		id:  "release_group_empty_music",
		row: torrentHintDecodeRow{ContentType: "music", ReleaseGroup: hintString("")},
	})

	cases = append(cases, torrentHintDecodeCase{id: "everything", row: torrentHintDecodeRow{
		ContentType:     "tv_show",
		ContentSource:   hintString("tvdb"),
		ContentID:       hintString("81189"),
		Title:           hintString("Breaking Bad"),
		ReleaseYear:     hintYear(2008),
		Languages:       hintString(`["es", "en"]`),
		Episodes:        hintString(`{"1": {"1": {}, "2": {}}, "2": {}}`),
		VideoResolution: hintString("V2160p"),
		VideoSource:     hintString("WEBDL"),
		VideoCodec:      hintString("x265"),
		Video3D:         hintString("V3D"),
		VideoModifier:   hintString("REMUX"),
		ReleaseGroup:    hintString("NTb"),
	}})

	return cases
}

func torrentHintDecodeHash(index int) protocol.ID {
	return protocol.MustParseID(fmt.Sprintf("%040x", index+1))
}

func TestGenerateTorrentHintDecodePgFixture(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping torrent hint decode fixture generation")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open fixture PostgreSQL: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("open fixture sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	ctx := context.Background()
	const schema = "m3b_torrent_hint_decode"
	if err := db.WithContext(ctx).Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE; CREATE SCHEMA " + schema + "; SET search_path TO " + schema,
	).Error; err != nil {
		t.Fatalf("reset parity schema: %v", err)
	}
	// Migration 00007's columns and check, without the foreign keys: the
	// fixture needs rows (an empty source) the production keys would refuse.
	if err := db.WithContext(ctx).Exec(`CREATE TABLE torrent_hints (
		info_hash bytea NOT NULL PRIMARY KEY,
		content_type text NOT NULL,
		content_source text NULL,
		content_id text NULL,
		title text NULL,
		release_year integer NULL,
		languages jsonb NULL,
		episodes jsonb NULL,
		video_resolution text NULL,
		video_source text NULL,
		video_codec text NULL,
		video_3d text NULL,
		video_modifier text NULL,
		release_group text NULL,
		created_at timestamp with time zone NOT NULL,
		updated_at timestamp with time zone NOT NULL,
		CHECK ((content_source IS NULL) OR (content_id IS NOT NULL))
	)`).Error; err != nil {
		t.Fatalf("apply torrent_hints DDL: %v", err)
	}

	stamp := time.Date(2026, time.October, 23, 0, 0, 0, 0, time.UTC)
	query := dao.Use(db)
	cases := torrentHintDecodeCorpus()
	fixtures := make([]Fixture, 0, len(cases))
	byID := make(map[string][]byte, len(cases))
	for index, scenario := range cases {
		if _, duplicate := byID[scenario.id]; duplicate {
			t.Fatalf("duplicate case id %s", scenario.id)
		}
		row := scenario.row
		for _, jsonb := range []*string{row.Languages, row.Episodes} {
			if jsonb == nil {
				continue
			}
			var normalized string
			if err := db.WithContext(ctx).Raw(`SELECT ?::jsonb::text`, *jsonb).Scan(&normalized).Error; err != nil {
				t.Fatalf("case %s: normalize %q: %v", scenario.id, *jsonb, err)
			}
			if normalized != *jsonb {
				t.Fatalf("case %s: jsonb input %q is not in PostgreSQL's rendering %q", scenario.id, *jsonb, normalized)
			}
		}
		hash := torrentHintDecodeHash(index)
		if err := db.WithContext(ctx).Exec(
			`INSERT INTO torrent_hints (info_hash, content_type, content_source, content_id, title,
				release_year, languages, episodes, video_resolution, video_source, video_codec,
				video_3d, video_modifier, release_group, created_at, updated_at)
			VALUES (decode(?, 'hex'), ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?)`,
			hash.String(), row.ContentType, row.ContentSource, row.ContentID, row.Title,
			row.ReleaseYear, row.Languages, row.Episodes, row.VideoResolution, row.VideoSource,
			row.VideoCodec, row.Video3D, row.VideoModifier, row.ReleaseGroup, stamp, stamp,
		).Error; err != nil {
			t.Fatalf("case %s: seed hint: %v", scenario.id, err)
		}

		expected := decodeTorrentHint(ctx, query, hash)

		inputJSON, err := json.Marshal(torrentHintDecodeInput{Row: row, Twin: scenario.twin})
		if err != nil {
			t.Fatal(err)
		}
		expectedJSON, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}
		byID[scenario.id] = expectedJSON
		fixtures = append(fixtures, Fixture{
			ID:        scenario.id,
			Subsystem: torrentHintDecodeSubsystem,
			Input:     inputJSON,
			Expected:  expectedJSON,
		})
	}

	for _, scenario := range cases {
		if scenario.twin == "" {
			continue
		}
		want, ok := byID[scenario.twin]
		if !ok {
			t.Fatalf("case %s: twin %s does not exist", scenario.id, scenario.twin)
		}
		if got := byID[scenario.id]; !bytes.Equal(got, want) {
			t.Fatalf("case %s decodes unlike its twin %s, so Go reads title or release_year:\n got: %s\nwant: %s",
				scenario.id, scenario.twin, got, want)
		}
	}

	reconcileTorrentHintDecodeFixtures(t, fixtures)
}

// decodeTorrentHint loads the row through GORM's `model.TorrentHint` scan, the
// one the processor's `Torrent.Hint` preload uses, and applies it the way
// `classifier.runner.Run` does before any workflow action.
func decodeTorrentHint(ctx context.Context, query *dao.Query, hash protocol.ID) torrentHintDecodeExpected {
	hint, err := query.TorrentHint.WithContext(ctx).Where(query.TorrentHint.InfoHash.Eq(hash)).First()
	if err != nil {
		return torrentHintDecodeExpected{LoadError: err.Error()}
	}

	expected := torrentHintDecodeExpected{
		HasHintedContentID: !hint.IsNil() && hint.ContentSource.Valid,
	}
	if hint.ContentSource.Valid {
		expected.ContentSource = &hint.ContentSource.String
	}
	if hint.ContentID.Valid {
		expected.ContentID = &hint.ContentID.String
	}

	result := classification.Result{}
	if !hint.IsNil() {
		result.ApplyHint(*hint)
	}
	normalized := classifier.NormalizeResult(result, nil)
	expected.Applied = &normalized

	episodesJSON, err := json.Marshal(result.Episodes)
	if err != nil {
		return torrentHintDecodeExpected{LoadError: fmt.Sprintf("marshal applied episodes: %v", err)}
	}
	expected.AppliedEpisodesJSON = string(episodesJSON)

	return expected
}

func reconcileTorrentHintDecodeFixtures(t *testing.T, fixtures []Fixture) {
	t.Helper()

	var buf bytes.Buffer
	for _, fixture := range fixtures {
		line, err := json.Marshal(fixture)
		if err != nil {
			t.Fatalf("marshal fixture %q: %v", fixture.ID, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	actual := buf.Bytes()

	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve torrent hint decode generator source path")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "..", "..", "testdata", "parity", "torrent-hint-decode", "torrent_hint_decode.jsonl",
	))
	if *updateTorrentHintDecodeParity {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create torrent hint decode fixture dir: %v", err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatalf("write torrent hint decode golden: %v", err)
		}
		t.Logf("wrote %d fixtures to %s", len(fixtures), path)
		return
	}

	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read torrent hint decode golden (run with -update-torrent-hint-decode-parity to create it): %v", err)
	}
	if bytes.Equal(expected, actual) {
		return
	}

	line, want, got := firstJSONLDifference(expected, actual)
	t.Fatalf(
		"torrent hint decode golden differs at line %d (run with -update-torrent-hint-decode-parity to regenerate)\nwant: %s\n got: %s",
		line, want, got,
	)
}
