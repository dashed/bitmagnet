package gqlmodel

import (
	"context"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
)

//nolint:paralleltest // mutates package-wide search feature flags.
func TestTorrentFilesDropCompatibleReadsUseBlobPath(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	search.SetFeatureFlags(search.FeatureFlags{DropCompatibleReads: true})

	_, err := (TorrentQuery{}).Files(context.Background(), TorrentFilesQueryInput{})
	if err == nil || err.Error() != "filesFromBlob: Dao not wired" {
		t.Fatalf("Files error = %v, want blob-path Dao wiring error", err)
	}
}
