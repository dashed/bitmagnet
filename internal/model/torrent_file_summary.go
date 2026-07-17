package model

import (
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const TableNameTorrentFileSummary = "torrent_file_summary"

type TorrentFileSummary struct {
	InfoHash        protocol.ID `gorm:"column:info_hash;primaryKey"                 json:"infoHash"`
	FileCount       int         `gorm:"column:file_count;not null;default:0"        json:"fileCount"`
	TotalSize       int64       `gorm:"column:total_size;not null;default:0"        json:"totalSize"`
	LargestFileSize int64       `gorm:"column:largest_file_size;not null;default:0" json:"largestFileSize"`
	Extensions      []string    `gorm:"column:extensions;serializer:json"           json:"extensions"`
	HasVideo        bool        `gorm:"column:has_video;not null;default:false"     json:"hasVideo"`
	HasSubtitle     bool        `gorm:"column:has_subtitle;not null;default:false"  json:"hasSubtitle"`
	HasAudio        bool        `gorm:"column:has_audio;not null;default:false"     json:"hasAudio"`
	// CompressedBytes is the octet length of the torrent's compressed files_data
	// blob (denormalized so the L3 refine path reads it without touching the
	// torrents heap). Nullable: NULL means "not yet backfilled" (or the torrent
	// has no blob), which the Rust read path treats as a miss and fills from
	// torrents.files_data.
	CompressedBytes NullInt   `gorm:"column:compressed_bytes"    json:"compressedBytes"`
	CreatedAt       time.Time `gorm:"column:created_at;not null" json:"createdAt"`
	UpdatedAt       time.Time `gorm:"column:updated_at;not null" json:"updatedAt"`
}

func (*TorrentFileSummary) TableName() string {
	return TableNameTorrentFileSummary
}
