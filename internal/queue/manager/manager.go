package manager

import (
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"gorm.io/gorm"
)

type manager struct {
	dao *dao.Query
	db  *gorm.DB
	now func() time.Time
}

func (m manager) currentTime() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}
