package dhtcrawler

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestTorrentFileSummaryPersistQueryTargetsSummaryTable(t *testing.T) {
	t.Parallel()

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = mockDB.Close()
	})

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: mockDB,
	}), &gorm.Config{})
	require.NoError(t, err)

	var infoHash protocol.ID
	copy(infoHash[:], []byte("01234567890123456789"))

	now := time.Unix(1_700_000_000, 0).UTC()
	summaries := []*model.TorrentFileSummary{
		{
			InfoHash:        infoHash,
			FileCount:       2,
			TotalSize:       300,
			LargestFileSize: 200,
			Extensions:      []string{"mkv", "srt"},
			HasVideo:        true,
			HasSubtitle:     true,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
	}

	sql := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return torrentFileSummaryPersistQuery(context.Background(), dao.Use(tx)).Create(summaries[0])
	})
	assert.Contains(t, sql, `INSERT INTO "torrent_file_summary"`)
	assert.NotContains(t, sql, `INSERT INTO "torrents"`)
}
