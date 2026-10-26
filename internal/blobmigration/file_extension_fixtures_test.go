package blobmigration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

// fileExtensionFixtures mirrors testdata/file-extension-fixtures.json. The same
// JSON is the source of truth for the Rust counterpart (see
// bitmagnet-rs .. file_extension_from_path), so both languages prove they agree
// on the FB-A0/G1 contract: the extension is always derived from the PATH, never
// from the blob's stored `e` (which is empty for crawl-path torrents).
type fileExtensionFixtures struct {
	Cases []struct {
		Name              string `json:"name"`
		Path              string `json:"path"`
		BlobE             string `json:"blob_e"`
		ExpectedExtension string `json:"expected_extension"`
		Note              string `json:"note"`
	} `json:"cases"`
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	dir := filepath.Dir(thisFile)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (go.mod)")
		}

		dir = parent
	}
}

func loadFileExtensionFixtures(t *testing.T) fileExtensionFixtures {
	t.Helper()

	path := filepath.Join(repoRoot(t), "testdata", "file-extension-fixtures.json")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading fixtures %s: %v", path, err)
	}

	var fx fileExtensionFixtures
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("unmarshalling fixtures: %v", err)
	}

	if len(fx.Cases) == 0 {
		t.Fatal("no fixture cases loaded")
	}

	return fx
}

// TestFileExtensionFromPath_SharedFixtures asserts model.FileExtensionFromPath
// produces the path-derived extension for every shared case.
func TestFileExtensionFromPath_SharedFixtures(t *testing.T) {
	for _, c := range loadFileExtensionFixtures(t).Cases {
		t.Run(c.Name, func(t *testing.T) {
			got := model.FileExtensionFromPath(c.Path)

			if got.String != c.ExpectedExtension {
				t.Errorf(
					"path %q: extension = %q, want %q (%s)",
					c.Path,
					got.String,
					c.ExpectedExtension,
					c.Note,
				)
			}

			wantValid := c.ExpectedExtension != ""
			if got.Valid != wantValid {
				t.Errorf("path %q: Valid = %v, want %v", c.Path, got.Valid, wantValid)
			}
		})
	}
}

// TestExtractUniqueExtensions_IgnoresBlobE proves the blob consumer derives
// extensions from the PATH and never trusts the stored `e` field — even when `e`
// is empty (crawl-path) or wrong (stale).
func TestExtractUniqueExtensions_IgnoresBlobE(t *testing.T) {
	for _, c := range loadFileExtensionFixtures(t).Cases {
		t.Run(c.Name, func(t *testing.T) {
			// Simulate a deserialized blob file: Path set, Extension carrying the
			// (possibly empty / possibly wrong) stored blob `e`.
			files := []model.TorrentFile{{
				Index:     0,
				Path:      c.Path,
				Extension: model.NullString{String: c.BlobE, Valid: c.BlobE != ""},
				Size:      1,
			}}

			got := blobmigration.ExtractUniqueExtensions(files)

			if c.ExpectedExtension == "" {
				if len(got) != 0 {
					t.Errorf("path %q: expected no extension, got %v", c.Path, got)
				}

				return
			}

			if len(got) != 1 || got[0] != c.ExpectedExtension {
				t.Errorf(
					"path %q (blob_e=%q): ExtractUniqueExtensions = %v, want [%q]",
					c.Path,
					c.BlobE,
					got,
					c.ExpectedExtension,
				)
			}
		})
	}
}
