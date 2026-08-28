package search

import (
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"gorm.io/gorm/clause"
)

// contentIdentityColumns are the columns of the content table's primary key,
// in primary key order.
var contentIdentityColumns = []string{"type", "source", "id"}

// ContentIdentityOrderByColumns orders by the content table's canonical identity
// (type, source, id). That is content_pkey, all three columns NOT NULL text, so it's
// a total order over the content table: appending it to any ordering makes both the
// order AND the set of rows a LIMIT admits uniquely determined.
//
// It exists as a tiebreak: content searches order by ts_rank_cd, which is highly
// degenerate for the quoted single-phrase queries the classifier issues - it
// commonly returns an identical rank for every candidate. Without a total order the
// rows a LIMIT admits, and their order within it, are decided by whichever plan the
// planner happened to pick.
//
// Caveat: these are text columns, so the order is the database's collation order
// (en_US.utf8 in production), NOT byte order - "10" sorts before "9", and a
// collation or ICU version change would shift which tied row wins. Anything that
// reimplements this ordering outside postgres (the Rust rewrite) must reproduce the
// collation, not sort bytewise, or it will pick a different content than the Go
// classifier for the same tie.
func ContentIdentityOrderByColumns() []query.OrderByColumn {
	columns := make([]query.OrderByColumn, 0, len(contentIdentityColumns))
	for _, name := range contentIdentityColumns {
		columns = append(columns, query.OrderByColumn{
			OrderByColumn: clause.OrderByColumn{
				Column: clause.Column{
					Table: model.TableNameContent,
					Name:  name,
				},
			},
		})
	}

	return columns
}

// ContentOrderByIdentity orders content results by their canonical identity.
func ContentOrderByIdentity() query.Option {
	return query.OrderBy(ContentIdentityOrderByColumns()...)
}

// ContentOrderByQueryStringRankThenIdentity orders content results by full-text
// relevance, breaking ties on the canonical identity. Relevance remains the primary
// ordering; the identity columns only order rows of equal rank.
func ContentOrderByQueryStringRankThenIdentity() query.Option {
	return query.OrderBy(append(
		[]query.OrderByColumn{query.StringRankOrderByColumn()},
		ContentIdentityOrderByColumns()...,
	)...)
}
