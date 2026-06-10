package search

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

func TestFeatureFlagsDefaultAllOff(t *testing.T) {
	// A fresh process (no SetFeatureFlags call) must read every flag as OFF.
	got := FeatureFlagsValue()
	if (got != FeatureFlags{}) {
		t.Errorf("default flags = %+v, want all-zero/off", got)
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
		GateFileExtensionsJSONB: true,
		PopularitySortDefault:   true,
		FileBrowserFromBlob:     true,
		FileSearchEnabled:       true,
	})

	got := FeatureFlagsValue()
	want := FeatureFlags{
		GateFileExtensionsJSONB: true,
		PopularitySortDefault:   true,
		FileBrowserFromBlob:     true,
		FileSearchEnabled:       true,
	}

	if got != want {
		t.Errorf("flags = %+v, want %+v", got, want)
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
