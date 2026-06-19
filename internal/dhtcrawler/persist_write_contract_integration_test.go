//go:build integration

package dhtcrawler

import (
	"context"
	"testing"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRunPersistTorrentsWriteContract(t *testing.T) {
	db := setupV2TestDB(t)

	var infoHash protocol.ID
	copy(infoHash[:], []byte("write-contract-hash"))

	parsed := pmetainfo.ParsedInfo{
		MetaVersion: 1,
		Info: metainfo.Info{
			Name:        "write-contract",
			PieceLength: 256 * 1024,
			Files: []metainfo.FileInfo{
				{Length: 1_000, Path: []string{"show", "episode.mkv"}},
				{Length: 100, Path: []string{"show", "episode.srt"}},
			},
		},
	}

	c := &crawler{
		persistTorrents:    concurrency.NewBatchingChannel[infoHashWithMetaInfo](1, 1, time.Hour),
		scrape:             concurrency.NewBufferedConcurrentChannel[nodeHasPeersForHash](1, 1),
		saveFilesThreshold: 1000,
		dao:                dao.Use(db),
		persistedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "persisted_total",
			Help:      "A counter of persisted database entities.",
		}, []string{"entity"}),
		torrentsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "torrents_dropped_total",
			Help:      "A counter of torrents dropped before persistence, by reason.",
		}, []string{"reason"}),
		logger: zap.NewNop().Sugar(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.runPersistTorrents(ctx)

	c.persistTorrents.In() <- infoHashWithMetaInfo{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: infoHash},
		metaInfo:            parsed,
	}

	require.Eventually(t, func() bool {
		var count int64
		require.NoError(t, db.Table(model.TableNameTorrent).
			Where("info_hash = ?", infoHash[:]).
			Count(&count).Error)

		return count == 1
	}, 5*time.Second, 50*time.Millisecond)

	var torrent model.Torrent
	require.NoError(t, db.Where("info_hash = ?", infoHash[:]).First(&torrent).Error)
	assert.Equal(t, model.FilesStatusMulti, torrent.FilesStatus)
	require.True(t, torrent.FilesCount.Valid)
	assert.Equal(t, uint(2), torrent.FilesCount.Uint)
	assert.NotEmpty(t, torrent.FilesData)
	assert.ElementsMatch(t, []string{"mkv", "srt"}, torrent.FileExts)
	assert.False(t, torrent.CreatedAt.IsZero())
	assert.False(t, torrent.UpdatedAt.IsZero())

	decodedFiles, err := blobmigration.DeserializeFiles(torrent.FilesData)
	require.NoError(t, err)
	require.Len(t, decodedFiles, 2)
	assert.Equal(t, "show/episode.mkv", decodedFiles[0].Path)
	assert.Equal(t, "show/episode.srt", decodedFiles[1].Path)

	var fileRows []model.TorrentFile
	require.NoError(t, db.Where("info_hash = ?", infoHash[:]).
		Order(`"index"`).
		Find(&fileRows).Error)
	require.Len(t, fileRows, 2)
	assert.Equal(t, "show/episode.mkv", fileRows[0].Path)
	assert.Equal(t, "show/episode.srt", fileRows[1].Path)

	var summary model.TorrentFileSummary
	require.NoError(t, db.Where("info_hash = ?", infoHash[:]).First(&summary).Error)
	assert.Equal(t, 2, summary.FileCount)
	assert.Equal(t, int64(1_100), summary.TotalSize)
	assert.Equal(t, int64(1_000), summary.LargestFileSize)
	assert.ElementsMatch(t, []string{"mkv", "srt"}, summary.Extensions)
	assert.True(t, summary.HasVideo)
	assert.True(t, summary.HasSubtitle)
	assert.False(t, summary.HasAudio)
	assert.False(t, summary.CreatedAt.IsZero())
	assert.False(t, summary.UpdatedAt.IsZero())
}
