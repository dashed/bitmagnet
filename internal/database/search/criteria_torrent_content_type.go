package search

import (
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"gorm.io/gen/field"
)

func TorrentContentTypeCriteria(types ...model.ContentType) query.Criteria {
	strTypes := make([]string, 0, len(types))
	for _, contentType := range types {
		strTypes = append(strTypes, contentType.String())
	}

	return query.DaoCriteria{
		Conditions: func(ctx query.DBContext) ([]field.Expr, error) {
			q := ctx.Query()
			return []field.Expr{
				q.TorrentContent.ContentType.In(strTypes...),
			}, nil
		},
		Joins: maps.NewInsertMap(
			maps.MapEntry[string, struct{}]{Key: model.TableNameTorrentContent},
		),
	}
}

// TorrentContentTypeOrNullCriteria matches rows whose content_type is one of the
// given types OR is NULL (unclassified). Roughly 64% of the corpus has a NULL
// content_type, and a strict `content_type IN (...)` hides all of it — so a
// Torznab typed/category search (Sonarr/Radarr) returns nothing for an exact
// title match against an unclassified release. Torznab uses this widened
// criterion; the general search / GraphQL API keeps the strict
// TorrentContentTypeCriteria so its content-type facet stays exact.
func TorrentContentTypeOrNullCriteria(types ...model.ContentType) query.Criteria {
	strTypes := make([]string, 0, len(types))
	for _, contentType := range types {
		strTypes = append(strTypes, contentType.String())
	}

	return query.DaoCriteria{
		Conditions: func(ctx query.DBContext) ([]field.Expr, error) {
			contentType := ctx.Query().TorrentContent.ContentType
			return []field.Expr{
				field.Or(contentType.In(strTypes...), contentType.IsNull()),
			}, nil
		},
		Joins: maps.NewInsertMap(
			maps.MapEntry[string, struct{}]{Key: model.TableNameTorrentContent},
		),
	}
}
