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

// contentAttributeOrderColumns order one content's attributes. With the owning
// content's identity they are the content_attributes primary key (content_type,
// content_source, content_id, source, key), so within a content the order is total.
var contentAttributeOrderColumns = []string{"source", "key"}

// contentCollectionLinkOrderColumns order one content's collection links by the
// linked collection's identity (type, source, id). With the owning content's
// identity they are the content_collections_content primary key, so within a
// content the order is total.
var contentCollectionLinkOrderColumns = []string{
	"content_collection_type",
	"content_collection_source",
	"content_collection_id",
}

// ContentAttributeOrderBy orders a content's attributes by (source, key).
//
// It exists because the order is load-bearing: Content.UpdateTsv appends every
// `id` attribute's value to the tsvector in slice order, so the attribute order
// decides the lexeme positions persisted into torrent_contents.tsv. Without an
// ORDER BY the preload returns rows in whatever order the plan produced them.
//
// The same collation caveat as ContentIdentityOrderByColumns applies: these are
// text columns, ordered by the database collation, not bytewise. The Rust worker's
// writer_load.rs orders the same columns the same way, and any reimplementation
// must too.
func ContentAttributeOrderBy() clause.OrderBy {
	return orderByColumns(model.TableNameContentAttribute, contentAttributeOrderColumns)
}

// ContentCollectionLinkOrderBy orders a content's collection links by the
// linked collection's (type, source, id).
//
// Content.UpdateTsv appends every genre collection's name in slice order, so like
// ContentAttributeOrderBy this decides tsvector positions, and like it the order
// is the database collation's.
func ContentCollectionLinkOrderBy() clause.OrderBy {
	return orderByColumns(model.TableNameContentCollectionContent, contentCollectionLinkOrderColumns)
}

func orderByColumns(table string, names []string) clause.OrderBy {
	columns := make([]clause.OrderByColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, clause.OrderByColumn{
			Column: clause.Column{Table: table, Name: name},
		})
	}

	return clause.OrderBy{Columns: columns}
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
