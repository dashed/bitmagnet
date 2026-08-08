package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/exclause"
	"github.com/bitmagnet-io/bitmagnet/internal/database/fts"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ResultItem struct {
	QueryStringRank float64
}

type GenericResult[T interface{}] struct {
	TotalCount           uint
	TotalCountIsEstimate bool
	HasNextPage          bool
	Items                []T
	Aggregations         Aggregations
}

type SubQueryFactory = func(context.Context, *dao.Query) SubQuery

// GenericQuery executes queries for any type of search and returns a GenericResult
func GenericQuery[T interface{}](
	_ctx context.Context,
	daoQ *dao.Query,
	option Option,
	tableName string,
	factory SubQueryFactory,
) (GenericResult[T], error) {
	gq := genericQuery[T]{
		daoQ:    daoQ,
		factory: factory,
	}
	builder, optionErr := option(newQueryContext(dbContext{
		q:         daoQ,
		tableName: tableName,
		factory:   factory,
	}))

	if optionErr != nil {
		return gq.result, optionErr
	}

	gq.ctx = builder.createContext(_ctx)
	gq.builder = builder
	wg := sync.WaitGroup{}
	wg.Add(3)

	//nolint:contextcheck
	go func() {
		defer wg.Done()
		gq.doItems()
	}()
	go func() {
		defer wg.Done()
		gq.doCount()
	}()
	go func() {
		defer wg.Done()

		if aggs, aggErr := gq.builder.calculateAggregations(gq.ctx); aggErr != nil {
			gq.addError(aggErr)
		} else {
			gq.result.Aggregations = aggs
		}
	}()
	wg.Wait()

	return gq.result, errors.Join(gq.errs...)
}

type genericQuery[T interface{}] struct {
	ctx     context.Context
	daoQ    *dao.Query
	factory SubQueryFactory
	builder OptionBuilder
	mtx     sync.Mutex
	errs    []error
	result  GenericResult[T]
}

func (gq *genericQuery[_]) newSubQuery(ctx context.Context, withOrder bool) (SubQuery, error) {
	return gq.newSubQueryFrom(ctx, gq.daoQ, withOrder)
}

// newSubQueryFrom builds the subquery against an explicit dao.Query, so a caller
// can rebind it to a transaction (see the seq-scan guard in doItems).
func (gq *genericQuery[_]) newSubQueryFrom(
	ctx context.Context,
	daoQ *dao.Query,
	withOrder bool,
) (SubQuery, error) {
	sq := gq.factory(ctx, daoQ)
	if selectErr := gq.builder.applySelect(sq.UnderlyingDB(), withOrder); selectErr != nil {
		return sq, selectErr
	}

	if preErr := gq.builder.applyPre(sq, withOrder); preErr != nil {
		return sq, preErr
	}

	return sq, nil
}

func (gq *genericQuery[_]) addError(err error) {
	gq.mtx.Lock()
	defer gq.mtx.Unlock()
	gq.errs = append(gq.errs, err)
}

func (gq *genericQuery[_]) checkExists(ctx context.Context) (bool, error) {
	sq, sqErr := gq.newSubQuery(ctx, false)
	if sqErr != nil {
		return false, sqErr
	}

	sql := dao.ToSQL(sq.UnderlyingDB().Select("*"))
	row := sq.UnderlyingDB().Raw("SELECT EXISTS(" + sql + ")")

	var exists bool

	if existsErr := row.Scan(&exists).Error; existsErr != nil {
		return false, existsErr
	}

	return exists, nil
}

func (gq *genericQuery[_]) doCount() {
	if gq.builder.withTotalCount() {
		sq, sqErr := gq.newSubQuery(gq.ctx, false)
		if sqErr != nil {
			gq.addError(sqErr)
			return
		}

		if countResult, countErr := dao.BudgetedCount(
			sq.UnderlyingDB(), gq.builder.AggregationBudget(),
		); countErr != nil {
			gq.addError(countErr)
		} else {
			gq.result.TotalCount = uint(countResult.Count)
			gq.result.TotalCountIsEstimate = countResult.BudgetExceeded
		}
	}
}

// doItems gets the items from the database and sets it to the result
//
// For querying the items, we have 2 possible strategies to try:
// - the default strategy is always tried, and is usually the most performant
// - for certain searches where items are filtered to a small number of results, and ordered with a limit,
// the default strategy can be very slow, so we try a CTE strategy, with order and limit on a materialized view of
// the complete results, and we put it in a race with the default strategy.
// The CTE strategy uses a stopping point, and will only return items where there are fewer than the stopping point.
func (gq *genericQuery[T]) doItems() {
	if !gq.builder.hasZeroLimit() || gq.builder.needsNextPage() {
		var finalItems []T

		doneChan := make(chan error)

		raceCtx, raceCancel := context.WithCancel(gq.ctx)
		defer raceCancel()

		mtx := sync.Mutex{}
		done := func(items []T, err error) {
			mtx.Lock()
			defer mtx.Unlock()

			if finalItems != nil || raceCtx.Err() != nil {
				return
			}

			if err == nil {
				// copy items slice to avoid modifying cached results
				finalItems = append([]T{}, items...)
			}
			doneChan <- err
		}
		// start the default strategy
		go func() {
			runDefaultStrategy := func(daoQ *dao.Query) ([]T, error) {
				sq, sqErr := gq.newSubQueryFrom(raceCtx, daoQ, true)
				if sqErr != nil {
					return nil, sqErr
				}

				if postErr := gq.builder.applyPost(sq.UnderlyingDB()); postErr != nil {
					return nil, postErr
				}

				var items []T
				if txErr := sq.UnderlyingDB().Find(&items).Error; txErr != nil {
					return nil, txErr
				}

				return items, nil
			}

			// Free-text searches take this strategy as `WHERE tsv @@ ? LIMIT n` with
			// no ORDER BY. Postgres estimates a CONSTANT ~0.21% selectivity for any
			// tsquery (identical for a common word and a typo — it is not a per-lexeme
			// statistic), so against a small LIMIT a seq scan looks like it will exit
			// after ~0.02% of the table. For a common term that holds and it is fast;
			// for a rare or misspelled term there are almost no matches, so it scans
			// all 49M rows — measured at 30.1s vs 4.7ms on the existing GIN index, a
			// ~6,400x difference. Latency ends up INVERSE to result count.
			//
			// Discourage the seq scan for exactly this shape. enable_seqscan=off is a
			// cost penalty, not a prohibition, so a query with no usable index still
			// gets a seq scan. SET LOCAL confines it to this transaction, so no pooled
			// connection is left mutated. Fixing the estimate is not an option: the
			// selectivity is a constant, and a rare typo can never enter the MCELEM
			// list. random_page_cost and a tsv-only GIN index were both measured and
			// do NOT change the plan.
			if gq.builder.hasTSQuery() {
				var items []T

				txErr := gq.daoQ.Transaction(func(tx *dao.Query) error {
					txDB := gq.factory(raceCtx, tx).UnderlyingDB().Session(&gorm.Session{NewDB: true})
					if setErr := txDB.Exec("SET LOCAL enable_seqscan = off").Error; setErr != nil {
						return setErr
					}

					var runErr error
					items, runErr = runDefaultStrategy(tx)

					return runErr
				})
				done(items, txErr)

				return
			}

			items, err := runDefaultStrategy(gq.daoQ)
			done(items, err)
		}()

		if gq.builder.shouldTryCteStrategy() {
			// start the CTE strategy
			go func() {
				stoppingPoint := 50_000

				sqCte, sqCteErr := gq.newSubQuery(raceCtx, true)
				if sqCteErr != nil {
					done(nil, sqCteErr)
					return
				}

				sql := dao.ToSQL(sqCte.UnderlyingDB()) + " LIMIT " + strconv.Itoa(stoppingPoint)

				cte := gq.factory(raceCtx, gq.daoQ).UnderlyingDB().Clauses(
					exclause.NewWith("cte", sql, true),
					exclause.NewWith("cte_count", "SELECT COUNT(*) AS total_count FROM cte", true),
				).Table("cte").Where("(SELECT MAX(total_count) FROM cte_count) < " + strconv.Itoa(stoppingPoint))
				if postErr := gq.builder.applyPost(cte); postErr != nil {
					done(nil, postErr)
					return
				}

				var items []T
				if scanErr := cte.Scan(&items).Error; scanErr != nil {
					done(nil, scanErr)
					return
				}

				if len(items) == 0 {
					// if no items are returned, we need a further check
					// to distinguish between stopping point reached and no matching items
					exists, existsErr := gq.checkExists(raceCtx)
					if existsErr != nil {
						done(nil, existsErr)
						return
					}

					if exists {
						// the stopping point was reached, so return without calling `done`
						return
					}
				}

				done(items, nil)
			}()
		}
		select {
		case doneErr := <-doneChan:
			raceCancel()

			if doneErr != nil {
				gq.addError(doneErr)
				return
			}
		case <-raceCtx.Done():
			gq.addError(raceCtx.Err())
			return
		}

		if gq.builder.hasNextPage(len(finalItems)) {
			gq.result.HasNextPage = true
			finalItems = finalItems[:len(finalItems)-1]
		}

		if len(finalItems) > 0 {
			if cbErr := gq.builder.applyCallbacks(gq.ctx, finalItems); cbErr != nil {
				gq.addError(cbErr)
				return
			}
		}

		gq.result.Items = finalItems
	}
}

type BaseSubQuery interface {
	Count() (int64, error)
	TableName() string
	UnderlyingDB() *gorm.DB
}

type SubQuery interface {
	BaseSubQuery
	Scopes(...GormScope) SubQuery
}

type GenericSubQuery[T BaseSubQuery] struct {
	SubQuery BaseSubQuery
}

func (sq GenericSubQuery[T]) Count() (int64, error) {
	return sq.SubQuery.Count()
}

func (sq GenericSubQuery[T]) TableName() string {
	return sq.SubQuery.TableName()
}

func (sq GenericSubQuery[T]) UnderlyingDB() *gorm.DB {
	return sq.SubQuery.UnderlyingDB()
}

type scoper[T BaseSubQuery] interface {
	Scopes(funcs ...GormScope) T
}

func (sq GenericSubQuery[T]) Scopes(fns ...func(gen.Dao) gen.Dao) SubQuery {
	sq.SubQuery = sq.SubQuery.(scoper[T]).Scopes(fns...)
	return sq
}

type Scope = func(*gorm.DB) error

type GormScope = func(gen.Dao) gen.Dao

type DBContext interface {
	Query() *dao.Query
	TableName() string
	NewSubQuery(context.Context) SubQuery
}

type dbContext struct {
	q         *dao.Query
	tableName string
	factory   SubQueryFactory
}

func (db dbContext) Query() *dao.Query {
	return db.q
}

func (db dbContext) TableName() string {
	return db.tableName
}

func (db dbContext) NewSubQuery(ctx context.Context) SubQuery {
	return db.factory(ctx, db.q)
}

type CallbackContext interface {
	DBContext
	Lock()
	Unlock()
}

type callbackContext struct {
	dbContext
	*sync.Mutex
}

type Callback func(ctx context.Context, cbCtx CallbackContext, results any) error

type OrderByColumn struct {
	clause.OrderByColumn
	RequiredJoins []string
}

type OptionBuilder interface {
	DBContext
	Table(string) OptionBuilder
	Join(...TableJoin) OptionBuilder
	RequireJoin(...string) OptionBuilder
	QueryString(string) OptionBuilder
	Scope(...Scope) OptionBuilder
	Select(...clause.Expr) OptionBuilder
	OrderBy(...OrderByColumn) OptionBuilder
	Limit(uint) OptionBuilder
	Offset(uint) OptionBuilder
	Group(...clause.Column) OptionBuilder
	Facet(...Facet) OptionBuilder
	Preload(...field.RelationField) OptionBuilder
	Callback(...Callback) OptionBuilder
	Context(func(ctx context.Context) context.Context) OptionBuilder
	applySelect(db *gorm.DB, withOrderSelect bool) error
	applyPre(sq SubQuery, withOrderJoins bool) error
	applyPost(*gorm.DB) error
	createFacetsFilterCriteria() (Criteria, error)
	calculateAggregations(context.Context) (Aggregations, error)
	WithTotalCount(bool) OptionBuilder
	WithHasNextPage(bool) OptionBuilder
	WithAggregationBudget(float64) OptionBuilder
	AggregationBudget() float64
	withTotalCount() bool
	applyCallbacks(context.Context, any) error
	hasZeroLimit() bool
	needsNextPage() bool
	hasNextPage(nItems int) bool
	withCurrentFacet(string) OptionBuilder
	shouldTryCteStrategy() bool
	hasTSQuery() bool
	createContext(context.Context) context.Context
}

type optionBuilder struct {
	dbContext
	joins map[string]TableJoin
	//revive:disable-next-line:nested-structs
	requiredJoins     maps.InsertMap[string, struct{}]
	tsquery           string
	scopes            []Scope
	selections        []clause.Expr
	groupBy           []clause.Column
	orderBy           []OrderByColumn
	limit             model.NullUint
	nextPage          bool
	offset            uint
	facets            []Facet
	currentFacet      string
	preloads          []field.RelationField
	totalCount        bool
	aggregationBudget float64
	callbacks         []Callback
	contextFn         func(context.Context) context.Context
}

type RawJoin struct {
	Query string
	Args  []interface{}
}

func newQueryContext(dbCtx dbContext) OptionBuilder {
	return optionBuilder{
		dbContext:     dbCtx,
		joins:         make(map[string]TableJoin),
		requiredJoins: maps.NewInsertMap[string, struct{}](),
	}
}

func (b optionBuilder) Table(name string) OptionBuilder {
	b.tableName = name

	return b.Scope(func(db *gorm.DB) error {
		db.Table(name)
		return nil
	})
}

func (b optionBuilder) Join(joins ...TableJoin) OptionBuilder {
	bJoins := make(map[string]TableJoin, len(b.joins))
	for _, j := range b.joins {
		bJoins[j.Table.TableName()] = j
	}

	bRequiredJoins := b.requiredJoins.Copy()

	for _, j := range joins {
		bJoins[j.Table.TableName()] = j

		if j.Required {
			bRequiredJoins.SetKey(j.Table.TableName())
		}
	}

	b.joins = bJoins
	b.requiredJoins = bRequiredJoins

	return b
}

func (b optionBuilder) RequireJoin(names ...string) OptionBuilder {
	bRequiredJoins := b.requiredJoins.Copy()
	for _, name := range names {
		bRequiredJoins.SetKey(name)
	}

	b.requiredJoins = bRequiredJoins

	return b
}

func (b optionBuilder) QueryString(str string) OptionBuilder {
	b.tsquery = fts.AppQueryToTsquery(str)
	return b
}

func (b optionBuilder) Scope(scopes ...Scope) OptionBuilder {
	b.scopes = append(b.scopes, scopes...)
	return b
}

func (b optionBuilder) Select(selections ...clause.Expr) OptionBuilder {
	b.selections = append(b.selections, selections...)
	return b
}

func (b optionBuilder) Group(columns ...clause.Column) OptionBuilder {
	b.groupBy = append(b.groupBy, columns...)
	return b
}

func (b optionBuilder) OrderBy(columns ...OrderByColumn) OptionBuilder {
	b.orderBy = columns
	return b
}

func (b optionBuilder) Limit(limit uint) OptionBuilder {
	b.limit = model.NewNullUint(limit)
	return b
}

func (b optionBuilder) Offset(offset uint) OptionBuilder {
	b.offset = offset
	return b
}

func (b optionBuilder) Facet(facets ...Facet) OptionBuilder {
	b.facets = append(b.facets, facets...)
	return b
}

func (b optionBuilder) Preload(relations ...field.RelationField) OptionBuilder {
	b.preloads = append(b.preloads, relations...)
	return b
}

func (b optionBuilder) Callback(callbacks ...Callback) OptionBuilder {
	b.callbacks = append(b.callbacks, callbacks...)
	return b
}

func (b optionBuilder) Context(fn func(context.Context) context.Context) OptionBuilder {
	prevFn := b.contextFn
	b.contextFn = func(ctx context.Context) context.Context {
		if prevFn != nil {
			ctx = prevFn(ctx)
		}

		return fn(ctx)
	}

	return b
}

func (b optionBuilder) WithTotalCount(bl bool) OptionBuilder {
	b.totalCount = bl
	return b
}

func (b optionBuilder) WithHasNextPage(bl bool) OptionBuilder {
	b.nextPage = bl
	return b
}

func (b optionBuilder) withTotalCount() bool {
	return b.totalCount
}

func (b optionBuilder) hasTSQuery() bool {
	return b.tsquery != ""
}

func (b optionBuilder) hasZeroLimit() bool {
	return b.limit.Valid && b.limit.Uint == 0
}

func (b optionBuilder) needsNextPage() bool {
	return b.limit.Valid && b.nextPage
}

func (b optionBuilder) hasNextPage(nItems int) bool {
	if !b.nextPage {
		return false
	}

	if !b.limit.Valid {
		return false
	}

	return nItems > int(b.limit.Uint)
}

func (b optionBuilder) WithAggregationBudget(budget float64) OptionBuilder {
	b.aggregationBudget = budget
	return b
}

func (b optionBuilder) AggregationBudget() float64 {
	return b.aggregationBudget
}

func (b optionBuilder) withCurrentFacet(facet string) OptionBuilder {
	b.currentFacet = facet
	return b
}

func (b optionBuilder) createContext(ctx context.Context) context.Context {
	if b.contextFn != nil {
		return b.contextFn(ctx)
	}

	return ctx
}

func (b optionBuilder) applySelect(db *gorm.DB, withOrderSelect bool) error {
	var selectQueryParts []string

	selectQueryArgs := make([]interface{}, 0)

	if len(b.selections) == 0 {
		selectQueryParts = append(selectQueryParts, "*")
	} else {
		for _, s := range b.selections {
			selectQueryParts = append(selectQueryParts, s.SQL)
			selectQueryArgs = append(selectQueryArgs, s.Vars...)
		}
	}

	if withOrderSelect {
		for i, orderBy := range b.orderBy {
			alias := "_order_" + strconv.Itoa(i)

			if orderBy.Column.Name == QueryStringRankField {
				rankFragment := "0"
				args := make([]interface{}, 0)

				if b.tsquery != "" {
					rankFragment = "ts_rank_cd(" + b.tableName + ".tsv, ?::tsquery)"
					args = append(args, b.tsquery)
				}

				selectQueryParts = append(selectQueryParts, rankFragment+" AS "+alias)
				selectQueryArgs = append(selectQueryArgs, args...)

				// don't break: applyPost orders by every `_order_i` alias, so any
				// columns following the rank column (a tiebreak, say) need selecting too.
				continue
			} else if orderBy.Column.Alias == "" {
				writer := bytes.NewBuffer(nil)
				db.Statement.QuoteTo(writer, orderBy.Column)
				selectQueryParts = append(selectQueryParts, writer.String()+" AS "+alias)
			}
		}
	}

	db.Select(strings.Join(selectQueryParts, ", "), selectQueryArgs...)

	return nil
}

func (b optionBuilder) applyPre(sq SubQuery, withOrderJoins bool) error {
	for _, s := range b.scopes {
		if err := s(sq.UnderlyingDB()); err != nil {
			return err
		}
	}

	if b.tsquery != "" {
		sq.UnderlyingDB().Where(b.tableName+".tsv @@ ?::tsquery", b.tsquery)
	}

	requiredJoins := b.requiredJoins.Copy()

	aggC, aggCErr := b.createFacetsFilterCriteria()
	if aggCErr != nil {
		return aggCErr
	}

	rawAggC, rawAggCErr := aggC.Raw(b)
	if rawAggCErr != nil {
		return rawAggCErr
	}

	requiredJoins.SetEntries(rawAggC.Joins.Entries()...)

	if withOrderJoins {
		for _, ob := range b.orderBy {
			for _, j := range ob.RequiredJoins {
				requiredJoins.Set(j, struct{}{})
			}
		}
	}

	joins, joinsErr := extractRequiredJoins(b.tableName, b.joins, requiredJoins)
	if joinsErr != nil {
		return joinsErr
	}

	applyJoins(sq, joins...)
	sq.UnderlyingDB().Where(rawAggC.Query, rawAggC.Args...)

	if len(b.groupBy) > 0 {
		sq.UnderlyingDB().Clauses(clause.GroupBy{
			Columns: b.groupBy,
		})
	}

	return nil
}

func extractRequiredJoins(
	tableName string,
	joins map[string]TableJoin,
	requiredJoins maps.InsertMap[string, struct{}],
) ([]TableJoin, error) {
	resolvedJoins := maps.NewInsertMap[string, TableJoin]()

	var addJoin func(name string) error

	addJoin = func(name string) error {
		if name == tableName {
			return nil
		}

		j, ok := joins[name]
		if !ok {
			return fmt.Errorf("required join not found: %s", name)
		}

		for _, depName := range j.Dependencies.Keys() {
			if err := addJoin(depName); err != nil {
				return err
			}
		}

		resolvedJoins.Set(j.Table.TableName(), j)

		return nil
	}
	for _, joinName := range requiredJoins.Keys() {
		if err := addJoin(joinName); err != nil {
			return nil, err
		}
	}

	return resolvedJoins.Values(), nil
}

func applyJoins(sq SubQuery, joins ...TableJoin) {
	for _, j := range joins {
		join := j

		sq.Scopes(func(dao gen.Dao) gen.Dao {
			switch join.Type {
			case TableJoinTypeInner:
				return dao.Join(join.Table, join.On...)
			case TableJoinTypeLeft:
				return dao.LeftJoin(join.Table, join.On...)
			case TableJoinTypeRight:
				return dao.RightJoin(join.Table, join.On...)
			}

			panic("invalid join type")
		})
	}
}

func (b optionBuilder) applyPost(db *gorm.DB) error {
	if len(b.orderBy) > 0 {
		cols := make([]clause.OrderByColumn, 0, len(b.orderBy))

		for i, orderBy := range b.orderBy {
			alias := orderBy.Column.Alias
			if alias == "" {
				alias = "_order_" + strconv.Itoa(i)
			}

			cols = append(cols, clause.OrderByColumn{
				Column: clause.Column{Name: alias},
				Desc:   orderBy.Desc,
			})
		}

		db.Statement.AddClause(clause.OrderBy{
			Columns: cols,
		})
	}

	if b.limit.Valid {
		limit := int(b.limit.Uint)
		if b.nextPage {
			limit++
		}

		db.Limit(limit)
	}

	db.Offset(int(b.offset))

	for _, p := range b.preloads {
		db.Preload(p.Name(), p)
	}

	return nil
}

func (b optionBuilder) applyCallbacks(ctx context.Context, results any) error {
	cbCtx := callbackContext{
		dbContext: b.dbContext,
		Mutex:     &sync.Mutex{},
	}

	var errs []error

	wg := sync.WaitGroup{}
	wg.Add(len(b.callbacks))

	for _, cb := range b.callbacks {
		go (func(cb Callback) {
			defer wg.Done()
			if err := cb(ctx, cbCtx, results); err != nil {
				cbCtx.Lock()
				defer cbCtx.Unlock()
				errs = append(errs, err)
			}
		})(cb)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (b optionBuilder) shouldTryCteStrategy() bool {
	if !b.limit.Valid || len(b.orderBy) == 0 {
		return false
	}

	for _, f := range b.facets {
		if f.TriggersCte() && len(f.Filter()) > 0 {
			return true
		}
	}

	// A text search ordered primarily by relevance is served well by the default
	// strategy. Only the leading column decides this: trailing columns are tiebreaks
	// within equal ranks and don't change the shape of the scan.
	//
	// Widening the old "exactly one ordering column" test to "the leading column" is
	// one-directional - it can only return false for orderings that already returned
	// false, since a single relevance-desc column is still the leading column. No
	// query that previously took the CTE strategy has been moved off it.
	return b.tsquery != "" && (b.orderBy[0].Column.Name != QueryStringRankField ||
		!b.orderBy[0].Desc)
}
