package queue

import (
	"context"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/gen"
	"gorm.io/gen/field"
)

// PostgresSelector is the production Go database adapter for one
// process_torrent_batch keyset page.
type PostgresSelector struct {
	query *dao.Query
}

func NewPostgresSelector(query *dao.Query) PostgresSelector {
	return PostgresSelector{query: query}
}

func (s PostgresSelector) Select(
	ctx context.Context,
	selection batch.Selection,
) ([]protocol.ID, error) {
	var scopes []func(gen.Dao) gen.Dao
	if len(selection.ContentTypes) > 0 {
		scopes = append(scopes, contentTypesScope(s.query, selection.ContentTypes))
	}
	if selection.Orphans {
		scopes = append(scopes, func(tx gen.Dao) gen.Dao {
			return tx.Not(
				gen.Exists(
					s.query.TorrentContent.Where(
						s.query.TorrentContent.InfoHash.EqCol(
							s.query.Torrent.InfoHash,
						),
					),
				),
			)
		})
	}
	torrents, err := s.query.Torrent.WithContext(ctx).
		Scopes(scopes...).
		Where(
			s.query.Torrent.InfoHash.Gt(selection.AfterExclusive),
			s.query.Torrent.UpdatedAt.Lt(selection.UpdatedBefore),
		).
		Select(s.query.Torrent.InfoHash).
		Order(s.query.Torrent.InfoHash).
		Limit(int(selection.Limit)).
		Find()
	if err != nil {
		return nil, err
	}
	infoHashes := make([]protocol.ID, 0, len(torrents))
	for _, torrent := range torrents {
		infoHashes = append(infoHashes, torrent.InfoHash)
	}
	return infoHashes, nil
}

func contentTypesScope(
	d *dao.Query,
	contentTypeFilters []model.NullContentType,
) func(gen.Dao) gen.Dao {
	var contentTypes []string
	var unknownContentType bool
	for _, contentType := range contentTypeFilters {
		if !contentType.Valid {
			unknownContentType = true
		} else {
			contentTypes = append(contentTypes, contentType.ContentType.String())
		}
	}
	return func(tx gen.Dao) gen.Dao {
		var contentTypeCondition field.Expr
		switch {
		case len(contentTypes) > 0 && unknownContentType:
			contentTypeCondition = field.Or(
				d.TorrentContent.ContentType.In(contentTypes...),
				d.TorrentContent.ContentType.IsNull(),
			)
		case len(contentTypes) > 0:
			contentTypeCondition = d.TorrentContent.ContentType.In(contentTypes...)
		default:
			contentTypeCondition = d.TorrentContent.ContentType.IsNull()
		}
		sq := d.TorrentContent.Where(
			d.TorrentContent.InfoHash.EqCol(d.Torrent.InfoHash),
			contentTypeCondition,
		)
		return tx.Where(gen.Exists(sq))
	}
}
