package search

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/iancoleman/strcase"
)

func TestFeatureFlagsDefaultAllOff(t *testing.T) {
	// A fresh process (no SetFeatureFlags call) must read every flag as OFF.
	got := FeatureFlagsValue()
	if (got != FeatureFlags{}) {
		t.Errorf("default flags = %+v, want all-zero/off", got)
	}

	if got.FileSearchFacetsEnabled || got.FileSearchTypeaheadRPCEnabled {
		t.Errorf("new file-search flags must default off: %+v", got)
	}
}

func TestSetAndReadFeatureFlags(t *testing.T) {
	t.Cleanup(func() { SetFeatureFlags(FeatureFlags{}) })

	SetFeatureFlags(FeatureFlags{GateFileExtensionsJSONB: true})

	if !FeatureFlagsValue().GateFileExtensionsJSONB {
		t.Error("GateFileExtensionsJSONB not published")
	}

	if FeatureFlagsValue().PopularitySortDefault {
		t.Error("PopularitySortDefault should still be off")
	}
}

func TestApplyFeatureFlags(t *testing.T) {
	t.Cleanup(func() { SetFeatureFlags(FeatureFlags{}) })

	ApplyFeatureFlags(FeatureFlagsConfig{
		DropCompatibleReads:           true,
		GateFileExtensionsJSONB:       true,
		PopularitySortDefault:         true,
		FileBrowserFromBlob:           true,
		FileSearchEnabled:             true,
		FileSearchFacetsEnabled:       true,
		FileSearchTypeaheadRPCEnabled: true,
	})

	got := FeatureFlagsValue()
	want := FeatureFlags{
		DropCompatibleReads:           true,
		GateFileExtensionsJSONB:       true,
		PopularitySortDefault:         true,
		FileBrowserFromBlob:           true,
		FileSearchEnabled:             true,
		FileSearchFacetsEnabled:       true,
		FileSearchTypeaheadRPCEnabled: true,
	}

	if got != want {
		t.Errorf("flags = %+v, want %+v", got, want)
	}
}

func TestFeatureFlagEnvVarNames(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"DropCompatibleReads":           "SEARCH_FEATURES_DROP_COMPATIBLE_READS",
		"GateFileExtensionsJSONB":       "SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB",
		"PopularitySortDefault":         "SEARCH_FEATURES_POPULARITY_SORT_DEFAULT",
		"FileBrowserFromBlob":           "SEARCH_FEATURES_FILE_BROWSER_FROM_BLOB",
		"FileSearchEnabled":             "SEARCH_FEATURES_FILE_SEARCH_ENABLED",
		"FileSearchFacetsEnabled":       "SEARCH_FEATURES_FILE_SEARCH_FACETS_ENABLED",
		"FileSearchTypeaheadRPCEnabled": "SEARCH_FEATURES_FILE_SEARCH_TYPEAHEAD_RPC_ENABLED",
	}

	ct := reflect.TypeOf(FeatureFlagsConfig{})
	for field, wantEnv := range want {
		if _, ok := ct.FieldByName(field); !ok {
			t.Fatalf("FeatureFlagsConfig must have field %s", field)
		}

		gotEnv := "SEARCH_FEATURES_" + strings.ToUpper(strcase.ToSnake(field))
		if gotEnv != wantEnv {
			t.Errorf("field %s resolves to %s, want %s", field, gotEnv, wantEnv)
		}
	}
}

func TestDropCompatibleReadsForcesNoLegacyReadGates(t *testing.T) {
	t.Cleanup(func() { SetFeatureFlags(FeatureFlags{}) })

	SetFeatureFlags(FeatureFlags{DropCompatibleReads: true})

	got := FeatureFlagsValue()
	if !got.UseFileExtensionsJSONB() {
		t.Error("DropCompatibleReads should force JSONB extension filtering")
	}

	if !got.UseFileBrowserFromBlob() {
		t.Error("DropCompatibleReads should force blob-backed file browsing")
	}

	if got.AllowTorrentFilesRepair() {
		t.Error("DropCompatibleReads should disable torrent_files-backed repair")
	}

	if err := got.CheckLegacyTorrentFilesReadAllowed("unit-test"); !errors.Is(
		err,
		ErrLegacyTorrentFilesReadDisabled,
	) {
		t.Errorf(
			"DropCompatibleReads legacy-read check error = %v, want ErrLegacyTorrentFilesReadDisabled",
			err,
		)
	}
}

func TestExplicitLegacyReadGatesRemainSupported(t *testing.T) {
	flags := FeatureFlags{
		GateFileExtensionsJSONB: true,
		FileBrowserFromBlob:     true,
	}

	if !flags.UseFileExtensionsJSONB() {
		t.Error("GateFileExtensionsJSONB should still enable JSONB extension filtering")
	}

	if !flags.UseFileBrowserFromBlob() {
		t.Error("FileBrowserFromBlob should still enable blob-backed file browsing")
	}

	if !flags.AllowTorrentFilesRepair() {
		t.Error("legacy repair should remain allowed outside drop-compatible mode")
	}

	if err := flags.CheckLegacyTorrentFilesReadAllowed("unit-test"); err != nil {
		t.Errorf("legacy read should remain allowed outside drop-compatible mode: %v", err)
	}
}

func TestSearchTorrentFilesFailsClosedInDropCompatibleMode(t *testing.T) {
	t.Cleanup(func() { SetFeatureFlags(FeatureFlags{}) })

	SetFeatureFlags(FeatureFlags{DropCompatibleReads: true})

	_, err := (search{}).TorrentFiles(context.Background())
	if !errors.Is(err, ErrLegacyTorrentFilesReadDisabled) {
		t.Fatalf("TorrentFiles error = %v, want ErrLegacyTorrentFilesReadDisabled", err)
	}
}

func TestHydrateTorrentContentTorrentWithFilesFailsClosedInDropCompatibleMode(t *testing.T) {
	t.Cleanup(func() { SetFeatureFlags(FeatureFlags{}) })

	SetFeatureFlags(FeatureFlags{DropCompatibleReads: true})

	h := torrentContentTorrentHydrator{
		torrentContentTorrentHydratorConfig: torrentContentTorrentHydratorConfig{files: true},
	}

	_, err := h.GetSubs(context.Background(), nil, nil)
	if !errors.Is(err, ErrLegacyTorrentFilesReadDisabled) {
		t.Fatalf(
			"HydrateTorrentContentTorrentWithFiles error = %v, want ErrLegacyTorrentFilesReadDisabled",
			err,
		)
	}
}

func TestFileExtensionsJSONBContains(t *testing.T) {
	tbl := model.TableNameTorrent

	t.Run("empty", func(t *testing.T) {
		sql, args := fileExtensionsJSONBContains(nil)
		if sql != "FALSE" || args != nil {
			t.Errorf("empty = (%q, %v), want (FALSE, nil)", sql, args)
		}
	})

	t.Run("single", func(t *testing.T) {
		sql, args := fileExtensionsJSONBContains([]string{"mkv"})

		wantSQL := "(" + tbl + ".file_extensions @> ?::jsonb)"
		if sql != wantSQL {
			t.Errorf("sql = %q, want %q", sql, wantSQL)
		}

		if len(args) != 1 || args[0] != `["mkv"]` {
			t.Errorf("args = %v, want [`[\"mkv\"]`]", args)
		}
	})

	t.Run("multi_is_or", func(t *testing.T) {
		sql, args := fileExtensionsJSONBContains([]string{"mkv", "mp4"})

		wantSQL := "(" + tbl + ".file_extensions @> ?::jsonb OR " + tbl + ".file_extensions @> ?::jsonb)"
		if sql != wantSQL {
			t.Errorf("sql = %q, want %q", sql, wantSQL)
		}

		if len(args) != 2 || args[0] != `["mkv"]` || args[1] != `["mp4"]` {
			t.Errorf("args = %v, want single-element JSON arrays", args)
		}
	})

	t.Run("escapes_json", func(t *testing.T) {
		// A quote in the extension must be JSON-escaped, never break out.
		_, args := fileExtensionsJSONBContains([]string{`a"b`})
		if len(args) != 1 || args[0] != `["a\"b"]` {
			t.Errorf("args = %v, want JSON-escaped", args)
		}
	})
}
