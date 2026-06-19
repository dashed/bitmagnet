package consistency

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

// TestCompareStrictE_FlagsEmptyE is the G1 gate's core assertion: a crawl-path blob
// carrying an empty `e` on a file whose path yields a non-empty extension is a
// mismatch under strict-e (unlike the default check, which derives from path on both
// sides and would pass it).
func TestCompareStrictE_FlagsEmptyE(t *testing.T) {
	blob := []model.TorrentFile{tf(0, "Season 1/Episode 1.mkv", "", 100)} // empty e, fixable

	res := compareStrictE(blob)
	if res.Match {
		t.Fatal("expected strict-e mismatch for empty e on an .mkv path")
	}

	var saw bool

	for _, m := range res.Mismatches {
		if m.Field == "extension_raw" {
			saw = true

			if m.Expected != "mkv" || m.Got != "" {
				t.Errorf("extension_raw mismatch = (want %q got %q), expected (mkv, \"\")", m.Expected, m.Got)
			}
		}
	}

	if !saw {
		t.Errorf("expected an extension_raw mismatch, got %+v", res.Mismatches)
	}
}

// TestCompareStrictE_PassesCanonical: a fully-canonical blob (every `e` == path-derived,
// including a legitimately empty `e` on an extension-less file) passes strict-e.
func TestCompareStrictE_PassesCanonical(t *testing.T) {
	blob := []model.TorrentFile{
		tf(0, "movie.mkv", "mkv", 100),
		tf(1, "README", "", 10), // extension-less: empty e is correct
	}

	res := compareStrictE(blob)
	if !res.Match {
		t.Errorf("expected strict-e match, got mismatches: %+v", res.Mismatches)
	}
}

// TestCompareStrictE_FlagsStaleE: a wrong (stale) non-empty `e` is flagged.
func TestCompareStrictE_FlagsStaleE(t *testing.T) {
	blob := []model.TorrentFile{tf(0, "clip.mkv", "mp4", 100)}

	res := compareStrictE(blob)
	if res.Match {
		t.Fatal("expected strict-e mismatch for stale e (mp4 on an .mkv path)")
	}
}
