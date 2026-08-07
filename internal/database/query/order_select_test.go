package query

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// newOfflineDB returns a sqlmock-backed *gorm.DB in DryRun mode, so a statement can
// be rendered to SQL without a database.
func newOfflineDB(t *testing.T) *gorm.DB {
	t.Helper()

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		DryRun: true,
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	return db
}

func contentColumn(name string) OrderByColumn {
	return OrderByColumn{
		OrderByColumn: clause.OrderByColumn{
			Column: clause.Column{Table: model.TableNameContent, Name: name},
		},
	}
}

// applySelect assigns each ordering column an `_order_i` select alias, and applyPost
// orders by those aliases. The relevance column used to terminate that loop early,
// so any column ordered AFTER relevance got no alias while still being ordered by -
// the statement then referenced an undefined `_order_1`. Ordering by relevance with
// a tiebreak has to render.
func TestApplySelectAliasesColumnsFollowingQueryStringRank(t *testing.T) {
	t.Parallel()

	// Table() returns an instanced *gorm.DB, as the query execution path passes to
	// applySelect / applyPost, so clauses accumulate on one statement.
	db := newOfflineDB(t).Table(model.TableNameContent)
	builder, ok := newQueryContext(dbContext{tableName: model.TableNameContent}).
		Table(model.TableNameContent).
		QueryString("cinderella").
		OrderBy(
			QueryStringRankOrderByColumn(),
			contentColumn("type"),
			contentColumn("source"),
			contentColumn("id"),
		).(optionBuilder)
	require.True(t, ok)

	require.NoError(t, builder.applySelect(db, true))
	require.NoError(t, builder.applyPost(db))

	db.Find(&[]model.Content{})
	sql := db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)

	require.Contains(t, sql, `ts_rank_cd(content.tsv, 'cinderella'::tsquery) AS _order_0`)
	require.Contains(t, sql, `"content"."type" AS _order_1`)
	require.Contains(t, sql, `"content"."source" AS _order_2`)
	require.Contains(t, sql, `"content"."id" AS _order_3`)
	require.Contains(t, sql, `ORDER BY "_order_0" DESC,"_order_1","_order_2","_order_3"`)
}

// The CTE strategy is only worth racing when the ordering is NOT primarily by
// relevance. A tiebreak appended after the relevance column doesn't change the shape
// of the scan, so it must not switch strategy.
//
// The condition is one-directional: every ordering that avoided the CTE strategy
// before still avoids it, and no ordering that used it has been moved off it - only
// the new "relevance first, then tiebreak" shape is affected. The cases below pin
// that.
//
// (The CTE strategy itself is fine in production. Test harnesses that open gorm
// directly hit `relation "cte" does not exist` because they don't register
// db.Use(exclause.New()) the way the app does at internal/database/gorm.go:51 - the
// WITH clause is then never rendered. That's a harness gap, not a defect in the
// strategy, and it isn't the reason for this rule.)
func TestShouldTryCteStrategyIgnoresTrailingTiebreakColumns(t *testing.T) {
	t.Parallel()

	rank := QueryStringRankOrderByColumn()
	rankAsc := rank
	rankAsc.Desc = false

	tests := []struct {
		name    string
		orderBy []OrderByColumn
		want    bool
	}{
		{name: "relevance only", orderBy: []OrderByColumn{rank}, want: false},
		{
			name:    "relevance then identity tiebreak",
			orderBy: []OrderByColumn{rank, contentColumn("type"), contentColumn("source"), contentColumn("id")},
			want:    false,
		},
		{name: "ascending relevance", orderBy: []OrderByColumn{rankAsc}, want: true},
		{
			name:    "structured ordering",
			orderBy: []OrderByColumn{contentColumn("type"), rank},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builder, ok := newQueryContext(dbContext{tableName: model.TableNameContent}).
				QueryString("cinderella").
				OrderBy(tt.orderBy...).
				Limit(10).(optionBuilder)
			require.True(t, ok)
			require.Equal(t, tt.want, builder.shouldTryCteStrategy())
		})
	}
}
