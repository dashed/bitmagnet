package consistency

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

func tf(index uint, path, blobE string, size uint) model.TorrentFile {
	return model.TorrentFile{
		Index:     index,
		Path:      path,
		Extension: model.NullString{String: blobE, Valid: blobE != ""},
		Size:      size,
	}
}

// TestCompareFiles_CrawlPathEmptyExtensionNotFalseMismatch is the FB-A0 guard:
// a crawl-path blob carries an empty `e`, while the torrent_files row has a
// path-derived extension. Because the checker derives extension from PATH on
// both sides, the files still MATCH.
func TestCompareFiles_CrawlPathEmptyExtensionNotFalseMismatch(t *testing.T) {
	blob := []model.TorrentFile{tf(0, "Season 1/Episode 1.mkv", "", 100)} // crawl-path: empty e
	rows := []model.TorrentFile{tf(0, "Season 1/Episode 1.mkv", "mkv", 100)}

	res := CompareFiles(blob, rows)
	if !res.Match {
		t.Errorf("expected match, got mismatches: %+v", res.Mismatches)
	}
}

// TestCompareFiles_ExtensionDivergesWithPath ensures the extension check still
// fires when the PATHS parse to different extensions (caught alongside the path
// mismatch).
func TestCompareFiles_ExtensionDivergesWithPath(t *testing.T) {
	blob := []model.TorrentFile{tf(0, "clip.mkv", "", 100)}
	rows := []model.TorrentFile{tf(0, "clip.mp4", "mp4", 100)}

	res := CompareFiles(blob, rows)
	if res.Match {
		t.Fatal("expected mismatch")
	}

	var sawExtension bool

	for _, m := range res.Mismatches {
		if m.Field == "extension" {
			sawExtension = true

			if m.Expected != "mp4" || m.Got != "mkv" {
				t.Errorf("extension mismatch = (want %q got %q), expected (mp4, mkv)", m.Expected, m.Got)
			}
		}
	}

	if !sawExtension {
		t.Errorf("expected an extension mismatch, got %+v", res.Mismatches)
	}
}

// TestCompareFiles_StaleBlobEIgnored proves a WRONG non-empty blob `e` does not
// cause a mismatch as long as the paths agree (the path is authoritative).
func TestCompareFiles_StaleBlobEIgnored(t *testing.T) {
	blob := []model.TorrentFile{tf(0, "movie.mp4", "avi", 100)} // stale/wrong e
	rows := []model.TorrentFile{tf(0, "movie.mp4", "mp4", 100)}

	res := CompareFiles(blob, rows)
	if !res.Match {
		t.Errorf("stale blob e must be ignored; got mismatches: %+v", res.Mismatches)
	}
}
