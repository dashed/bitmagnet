//! Phase-2 GraphQL search orchestration.
//!
//! This path is deliberately separate from the frozen Torznab builder. It
//! lowers [`SearchOptions`](crate::SearchOptions) to a lean membership query,
//! hydrates the selected identities in a second query, and then executes the
//! independently budgeted total-count and facet queries.

use crate::facets::{build_base_query, fetch_aggregations, fetch_total_count, BASE_SELECT};
use crate::order::{OrderDirection, TorrentContentOrder, TorrentContentOrderField};
use crate::query::{Bind, BuildState, CriteriaCtx, HydrateOptions, Result, SearchQuery};
use crate::result::SearchResult;
use crate::{SearchBuildConfig, SearchOptions};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderClause {
    expression: String,
    direction: OrderDirection,
}

/// Build the GraphQL torrent-content membership query without executing it.
///
/// The select is intentionally lean: torrent-content identity and the scalar
/// order aliases only. Hydration joins/subqueries and the files blob remain in
/// the id-keyed follow-up query run by [`SearchQuery::fetch_with`]. Every page
/// limit and offset is rendered as a validated integer literal, matching GORM
/// and avoiding PostgreSQL's parameterized-LIMIT plan instability.
pub fn build_search_query(
    options: &SearchOptions,
    config: &SearchBuildConfig,
) -> Result<SearchQuery> {
    build_search_query_at(options, config, Utc::now())
}

/// Execute the complete Phase-2 PostgreSQL search contract.
///
/// Relative-time criteria share one `now` across membership, total-count, and
/// facet queries. `has_next_page` uses Go's `limit + 1` over-fetch and removes
/// the sentinel row before returning the hydrated page.
pub async fn search(
    pool: &PgPool,
    options: &SearchOptions,
    config: &SearchBuildConfig,
    hydrate: HydrateOptions,
) -> Result<SearchResult> {
    let now = Utc::now();
    let membership = build_search_query_at(options, config, now)?;

    // Go runs items, total count, and aggregations concurrently. Keep that
    // execution shape here: each branch owns an independent pooled connection,
    // while facet database fan-out has its own lower request-local cap.
    let items = async {
        // Go skips doItems for an explicit zero limit unless it needs the
        // one-row next-page probe. Count and aggregation queries still run.
        if options.limit == Some(0) && !options.has_next_page {
            Ok(Vec::new())
        } else {
            membership.fetch_with(pool, hydrate).await
        }
    };

    let total_count = async {
        if options.total_count {
            fetch_total_count(pool, options, config, now).await
        } else {
            Ok((0, false))
        }
    };

    let (mut items, (total_count, total_count_is_estimate), aggregations) = futures::try_join!(
        items,
        total_count,
        fetch_aggregations(pool, options, config, now),
    )?;

    let has_next_page = options.limit.is_some_and(|limit| {
        options.has_next_page && items.len() > usize::try_from(limit).unwrap_or(usize::MAX)
    });
    if has_next_page {
        // A true next-page result proves `limit` fits in `usize`: `items.len()`
        // already exceeded it on this platform.
        items.truncate(options.limit.unwrap_or_default() as usize);
    }

    Ok(SearchResult {
        total_count,
        total_count_is_estimate,
        has_next_page,
        items,
        aggregations,
    })
}

fn build_search_query_at(
    options: &SearchOptions,
    config: &SearchBuildConfig,
    now: DateTime<Utc>,
) -> Result<SearchQuery> {
    let orders = effective_orders(options, config);
    let require_torrents_for_order = orders
        .iter()
        .any(|order| order.field == TorrentContentOrderField::Name);
    let ctx = CriteriaCtx::new(config, now);
    let mut state = BuildState::default();
    let base = build_base_query(
        options,
        &ctx,
        None,
        None,
        require_torrents_for_order,
        &mut state,
    )?;
    let tsquery_placeholder = state
        .binds()
        .iter()
        .position(|bind| matches!(bind, Bind::Tsquery(_)))
        .map(|index| format!("${}", index + 1));

    let clauses = if orders.is_empty() {
        // Go's GraphQL browse default is deliberately a single column. This is
        // distinct from an explicit `published_at`, which adds the info-hash
        // tie-break below.
        vec![OrderClause {
            expression: "torrent_contents.published_at".to_owned(),
            direction: OrderDirection::Descending,
        }]
    } else {
        orders
            .iter()
            .flat_map(|order| order_clauses(*order, tsquery_placeholder.as_deref()))
            .collect()
    };

    let mut select = "SELECT torrent_contents.info_hash".to_owned();
    for (index, clause) in clauses.iter().enumerate() {
        select.push_str(&format!(
            ",\n       {} AS _order_{index}",
            clause.expression
        ));
    }
    let mut sql = base.replacen(BASE_SELECT, &format!("{select}\nFROM torrent_contents"), 1);

    sql.push_str("\nORDER BY ");
    sql.push_str(
        &clauses
            .iter()
            .enumerate()
            .map(|(index, clause)| format!("_order_{index} {}", direction_sql(clause.direction)))
            .collect::<Vec<_>>()
            .join(", "),
    );

    if let Some(limit) = options.limit {
        let over_fetch = u64::from(options.has_next_page);
        sql.push_str(&format!("\nLIMIT {}", u64::from(limit) + over_fetch));
    }
    if options.offset > 0 {
        sql.push_str(&format!("\nOFFSET {}", options.offset));
    }

    Ok(SearchQuery::new(sql, state.binds().to_vec()))
}

/// Reproduce `maps.InsertMap`: first insertion fixes position and a duplicate
/// field replaces only its direction. gqlmodel also drops relevance when no
/// query string exists before populating that map.
fn effective_orders(
    options: &SearchOptions,
    config: &SearchBuildConfig,
) -> Vec<TorrentContentOrder> {
    let has_query = options
        .query
        .as_deref()
        .is_some_and(|query| !query.is_empty());
    let mut orders: Vec<TorrentContentOrder> = Vec::with_capacity(options.order.len());

    for order in options.order.iter().copied() {
        if order.field == TorrentContentOrderField::Relevance && !has_query {
            continue;
        }
        if let Some(existing) = orders
            .iter_mut()
            .find(|existing| existing.field == order.field)
        {
            existing.direction = order.direction;
        } else {
            orders.push(order);
        }
    }

    if config.popularity_sort_default
        && has_query
        && orders.len() == 1
        && orders[0].field == TorrentContentOrderField::Relevance
    {
        return vec![TorrentContentOrder {
            field: TorrentContentOrderField::Seeders,
            direction: OrderDirection::Descending,
        }];
    }

    orders
}

fn order_clauses(order: TorrentContentOrder, tsquery: Option<&str>) -> Vec<OrderClause> {
    let direction = order.direction;
    let primary = match order.field {
        TorrentContentOrderField::Relevance => tsquery.map_or_else(
            || "0::bigint".to_owned(),
            |placeholder| format!("ts_rank_cd(torrent_contents.tsv, {placeholder}::tsquery)"),
        ),
        TorrentContentOrderField::PublishedAt => "torrent_contents.published_at".to_owned(),
        TorrentContentOrderField::UpdatedAt => "torrent_contents.updated_at".to_owned(),
        TorrentContentOrderField::Size => "torrent_contents.size".to_owned(),
        TorrentContentOrderField::FilesCount => {
            "COALESCE(torrent_contents.files_count, 0)".to_owned()
        }
        TorrentContentOrderField::Seeders => "coalesce(torrent_contents.seeders, -1)".to_owned(),
        TorrentContentOrderField::Leechers => "coalesce(torrent_contents.leechers, -1)".to_owned(),
        TorrentContentOrderField::Name => "torrents.name".to_owned(),
        TorrentContentOrderField::InfoHash => "torrent_contents.info_hash".to_owned(),
    };

    let mut clauses = vec![OrderClause {
        expression: primary,
        direction,
    }];
    if matches!(
        order.field,
        TorrentContentOrderField::PublishedAt
            | TorrentContentOrderField::UpdatedAt
            | TorrentContentOrderField::Size
            | TorrentContentOrderField::FilesCount
            | TorrentContentOrderField::Seeders
            | TorrentContentOrderField::Leechers
    ) {
        clauses.push(OrderClause {
            expression: "torrent_contents.info_hash".to_owned(),
            direction,
        });
    }
    clauses
}

const fn direction_sql(direction: OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Ascending => "ASC",
        OrderDirection::Descending => "DESC",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::Criteria;

    fn order(field: TorrentContentOrderField, direction: OrderDirection) -> TorrentContentOrder {
        TorrentContentOrder { field, direction }
    }

    #[test]
    fn all_order_fields_expand_with_go_tie_breaks() {
        let options = SearchOptions::new()
            .with_query("matrix")
            .with_order([
                order(
                    TorrentContentOrderField::UpdatedAt,
                    OrderDirection::Ascending,
                ),
                order(
                    TorrentContentOrderField::FilesCount,
                    OrderDirection::Descending,
                ),
                order(TorrentContentOrderField::Name, OrderDirection::Ascending),
                order(
                    TorrentContentOrderField::InfoHash,
                    OrderDirection::Descending,
                ),
            ])
            .with_limit(None)
            .with_offset(7);
        let query = build_search_query(&options, &SearchBuildConfig::default()).unwrap();

        assert!(query.sql().starts_with(
            "SELECT torrent_contents.info_hash,\n       torrent_contents.updated_at AS _order_0,\n       torrent_contents.info_hash AS _order_1,\n       COALESCE(torrent_contents.files_count, 0) AS _order_2,\n       torrent_contents.info_hash AS _order_3,\n       torrents.name AS _order_4,\n       torrent_contents.info_hash AS _order_5\nFROM torrent_contents\nINNER JOIN torrents"
        ));
        assert!(query.sql().contains(
            "ORDER BY _order_0 ASC, _order_1 ASC, _order_2 DESC, _order_3 DESC, _order_4 ASC, _order_5 DESC"
        ));
        assert!(!query.sql().contains("\nLIMIT "));
        assert!(query.sql().ends_with("\nOFFSET 7"));
    }

    #[test]
    fn find2_rewrites_only_lone_relevance_with_query() {
        let relevance = order(
            TorrentContentOrderField::Relevance,
            OrderDirection::Ascending,
        );
        let options = SearchOptions::new()
            .with_query("matrix")
            .with_order([relevance]);
        let enabled = SearchBuildConfig {
            popularity_sort_default: true,
            ..SearchBuildConfig::default()
        };
        let query = build_search_query(&options, &enabled).unwrap();
        assert!(query
            .sql()
            .contains("coalesce(torrent_contents.seeders, -1) AS _order_0"));
        assert!(query
            .sql()
            .contains("torrent_contents.info_hash AS _order_1"));
        assert!(query
            .sql()
            .contains("ORDER BY _order_0 DESC, _order_1 DESC"));
        assert!(!query.sql().contains("ts_rank_cd("));

        let disabled = build_search_query(&options, &SearchBuildConfig::default()).unwrap();
        assert!(disabled
            .sql()
            .contains("ts_rank_cd(torrent_contents.tsv, $1::tsquery) AS _order_0"));
        assert!(disabled.sql().ends_with("ORDER BY _order_0 ASC\nLIMIT 10"));

        let multi = options.clone().with_order([
            relevance,
            order(TorrentContentOrderField::Size, OrderDirection::Descending),
        ]);
        assert!(build_search_query(&multi, &enabled)
            .unwrap()
            .sql()
            .contains("ts_rank_cd("));
    }

    #[test]
    fn paging_is_literal_and_overfetches_exactly_one() {
        let options = SearchOptions::new()
            .with_limit(Some(u32::MAX))
            .with_offset(9)
            .with_has_next_page(true);
        let query = build_search_query(&options, &SearchBuildConfig::default()).unwrap();
        assert!(query.sql().contains("\nLIMIT 4294967296\nOFFSET 9"));
        assert!(query.binds().is_empty());

        let no_limit = options.with_limit(None);
        let query = build_search_query(&no_limit, &SearchBuildConfig::default()).unwrap();
        assert!(!query.sql().contains("\nLIMIT "));
    }

    #[test]
    fn order_dedup_matches_insert_map_and_relevance_without_query_is_dropped() {
        let options = SearchOptions::new().with_order([
            order(TorrentContentOrderField::Size, OrderDirection::Ascending),
            order(
                TorrentContentOrderField::Relevance,
                OrderDirection::Descending,
            ),
            order(TorrentContentOrderField::Size, OrderDirection::Descending),
        ]);
        let query = build_search_query(&options, &SearchBuildConfig::default()).unwrap();
        assert!(query.sql().contains("torrent_contents.size AS _order_0"));
        assert!(query
            .sql()
            .contains("torrent_contents.info_hash AS _order_1"));
        assert!(query
            .sql()
            .contains("ORDER BY _order_0 DESC, _order_1 DESC"));
        assert!(!query.sql().contains("ts_rank_cd("));
    }

    #[test]
    fn membership_keeps_filter_joins_but_not_hydration_projection() {
        let options = SearchOptions::new()
            .with_filter(Criteria::TorrentTag(vec!["trusted".to_owned()]))
            .with_order([order(
                TorrentContentOrderField::Seeders,
                OrderDirection::Descending,
            )]);
        let query = build_search_query(&options, &SearchBuildConfig::default()).unwrap();
        assert!(query.sql().contains("INNER JOIN torrents"));
        assert!(query.sql().contains("EXISTS (SELECT 1 FROM torrent_tags"));
        assert!(!query.sql().contains("files_data"));
        assert!(!query.sql().contains("LEFT JOIN content"));
        assert!(query.sql().contains("\nLIMIT 10"));
    }
}
