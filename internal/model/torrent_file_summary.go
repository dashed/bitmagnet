package model

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const TableNameTorrentFileSummary = "torrent_file_summary"

type TorrentFileSummary struct {
	InfoHash        protocol.ID `gorm:"column:info_hash;primaryKey" json:"infoHash"`
	FileCount       int         `gorm:"column:file_count;not null;default:0" json:"fileCount"`
	TotalSize       int64       `gorm:"column:total_size;not null;default:0" json:"totalSize"`
	LargestFileSize int64       `gorm:"column:largest_file_size;not null;default:0" json:"largestFileSize"`
	Extensions      StringArray `gorm:"column:extensions;type:text[];default:'{}'" json:"extensions"`
	HasVideo        bool        `gorm:"column:has_video;not null;default:false" json:"hasVideo"`
	HasSubtitle     bool        `gorm:"column:has_subtitle;not null;default:false" json:"hasSubtitle"`
	HasAudio        bool        `gorm:"column:has_audio;not null;default:false" json:"hasAudio"`
	CreatedAt       time.Time   `gorm:"column:created_at;not null" json:"createdAt"`
	UpdatedAt       time.Time   `gorm:"column:updated_at;not null" json:"updatedAt"`
}

func (*TorrentFileSummary) TableName() string {
	return TableNameTorrentFileSummary
}

// StringArray implements sql Scanner/Valuer for PostgreSQL text[] columns.
type StringArray []string

func (a *StringArray) Scan(value interface{}) error {
	if value == nil {
		*a = StringArray{}
		return nil
	}

	s, ok := value.(string)
	if !ok {
		b, ok2 := value.([]byte)
		if !ok2 {
			return fmt.Errorf("StringArray.Scan: unexpected type %T", value)
		}
		s = string(b)
	}

	s = strings.TrimSpace(s)
	if s == "{}" || s == "" {
		*a = StringArray{}
		return nil
	}

	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")

	parts := strings.Split(s, ",")
	result := make(StringArray, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "\"")
		if p != "" {
			result = append(result, p)
		}
	}

	*a = result
	return nil
}

func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return "{}", nil
	}

	parts := make([]string, len(a))
	for i, s := range a {
		parts[i] = `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
	}

	return "{" + strings.Join(parts, ",") + "}", nil
}
