package model

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/fts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postgresTsvectorLimit is PostgreSQL's hard cap: a tsvector may not exceed
// 1048575 bytes. A row over it aborts its whole CreateInBatches transaction,
// taking up to 99 innocent batch-mates down with it (F7).
const postgresTsvectorLimit = 1048575

// F7: a torrent with a pathological file list must not build a tsvector that
// PostgreSQL will reject, so it can never poison its persist batch. The bag of
// weight-D file-path lexemes is truncated to a safe budget; the weight-A
// name/infohash lexemes added before it are always preserved.
func TestUpdateTsv_BoundsOversizedFilePathBag(t *testing.T) {
	const fileCount = 4000
	longSeg := strings.Repeat("q", 260)
	files := make([]TorrentFile, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		// Lead with i so consecutive paths diverge at position 0 (defeats the
		// prefix dedup) and embed i inside a long word-char run so each file
		// contributes its own long, unique lexeme (a constant segment would be
		// deduplicated by the tsvector map and not grow it).
		files = append(files, TorrentFile{
			Index: uint(i),
			Path:  fmt.Sprintf("%08d_%s%08d_zzz.mkv", i, longSeg, i),
		})
	}
	torrent := Torrent{Name: "Contested Show Name", Files: files}

	// Sanity: the unbounded (pre-fix) tsvector really does exceed the limit, so
	// the guard is what keeps the capped one under it.
	uncapped := fts.Tsvector{}
	for _, s := range torrent.fileSearchStrings() {
		uncapped.AddText(s, fts.TsvectorWeightD)
	}
	require.Greater(t, len(uncapped.String()), postgresTsvectorLimit,
		"fixture must exceed Postgres' tsvector limit without the guard")

	tc := TorrentContent{Torrent: torrent}
	tc.UpdateTsv()
	got := tc.Tsv.String()

	assert.Less(t, len(got), postgresTsvectorLimit,
		"UpdateTsv must keep the tsvector under Postgres' hard limit")
	assert.LessOrEqual(t, len(got), fts.MaxTsvectorBytes,
		"UpdateTsv must respect the configured budget")

	// Weight-A name lexemes are added before the bounded bag and must survive.
	for _, lexeme := range []string{"contested", "show", "name"} {
		assert.Contains(t, got, "'"+lexeme+"'",
			"name lexeme %q (weight A) must be preserved through truncation", lexeme)
	}
}

// F7 follow-up: a single unbroken word-char run longer than PostgreSQL's 2046-
// byte per-word limit — in the torrent NAME (weight A) or a file path (weight D)
// — must be dropped so the row's tsvector casts cleanly and can't abort its
// persist batch, regardless of total tsv size.
func TestUpdateTsv_DropsOverlongLexemes(t *testing.T) {
	overlong := strings.Repeat("z", 3000)

	tc := TorrentContent{
		Torrent: Torrent{
			Name: "Real Title " + overlong + " Edition",
			Files: []TorrentFile{
				{Index: 0, Path: "dir/" + overlong + "/episode.mkv"},
			},
		},
	}
	tc.UpdateTsv()

	// Every lexeme must be within Postgres' per-word limit, so `::tsvector`
	// accepts the value (Postgres rejects any word > 2046 bytes).
	for lexeme := range tc.Tsv {
		assert.LessOrEqualf(t, len(lexeme), fts.MaxLexemeBytes,
			"lexeme of %d bytes exceeds Postgres' per-word limit", len(lexeme))
	}

	got := tc.Tsv.String()
	assert.NotContains(t, got, overlong, "the overlong run must be dropped, not indexed")
	// The surrounding real words survive.
	for _, lexeme := range []string{"real", "title", "edition", "episode"} {
		assert.Contains(t, got, "'"+lexeme+"'", "normal lexeme %q must still be indexed", lexeme)
	}
}

// A file list that fits comfortably under budget is added in full — the guard
// only truncates when a row would otherwise overflow.
func TestUpdateTsv_KeepsSmallFilePathBag(t *testing.T) {
	torrent := Torrent{
		Name: "Small Show",
		Files: []TorrentFile{
			{Index: 0, Path: "Small.Show/episode.one.mkv"},
			{Index: 1, Path: "Small.Show/episode.two.mkv"},
		},
	}

	tc := TorrentContent{Torrent: torrent}
	tc.UpdateTsv()
	got := tc.Tsv.String()

	for _, lexeme := range []string{"episode", "one", "two"} {
		assert.Contains(t, got, "'"+lexeme+"'",
			"small file lists must be indexed in full (missing %q)", lexeme)
	}
}
