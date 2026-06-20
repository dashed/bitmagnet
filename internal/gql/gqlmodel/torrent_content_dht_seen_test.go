package gqlmodel

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

func TestNewTorrentContentFromResultItemDHTSeenStats(t *testing.T) {
	firstSeenAt := time.Date(2026, 6, 19, 1, 2, 3, 0, time.UTC)
	lastSeenAt := time.Date(2026, 6, 20, 4, 5, 6, 0, time.UTC)

	got := NewTorrentContentFromResultItem(search.TorrentContentResultItem{
		TorrentContent: model.TorrentContent{
			Torrent: model.Torrent{
				Sources: []model.TorrentsTorrentSource{
					{
						Source:    "import",
						CreatedAt: firstSeenAt.Add(-time.Hour),
						UpdatedAt: lastSeenAt.Add(-time.Hour),
						SeenCount: 99,
					},
					{
						Source:    "dht",
						CreatedAt: firstSeenAt,
						UpdatedAt: lastSeenAt,
						SeenCount: 7,
					},
				},
			},
		},
	})

	if got.DHTSeenCount != 7 {
		t.Fatalf("DHTSeenCount = %d, want 7", got.DHTSeenCount)
	}
	if got.DHTFirstSeenAt == nil || !got.DHTFirstSeenAt.Equal(firstSeenAt) {
		t.Fatalf("DHTFirstSeenAt = %v, want %v", got.DHTFirstSeenAt, firstSeenAt)
	}
	if got.DHTLastSeenAt == nil || !got.DHTLastSeenAt.Equal(lastSeenAt) {
		t.Fatalf("DHTLastSeenAt = %v, want %v", got.DHTLastSeenAt, lastSeenAt)
	}
}
