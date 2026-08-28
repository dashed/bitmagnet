//! Bounded, read-only implementation of `torrent.suggestTags`.

use std::sync::Arc;

use async_graphql::{Error, Result};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use sqlx::FromRow;
use thiserror::Error;

use super::inputs::SuggestTagsQueryInput;
use super::objects::{SuggestedTag, TorrentSuggestTagsResult};

const MAX_PREFIX_CHARS: usize = 256;
const MAX_EXCLUSIONS: usize = 256;
const MAX_EXCLUSION_CHARS: usize = 256;
const SUGGESTION_LIMIT: i64 = 10;

/// Normalized request passed to a tag-suggestion runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuggestTagsRequest {
    /// Raw SQL LIKE prefix, without implicit escaping, matching Go.
    pub prefix: String,
    /// Exact tag names excluded from the aggregation.
    pub exclusions: Vec<String>,
}

/// One aggregated tag suggestion.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct SuggestedTagRecord {
    /// Tag name.
    pub name: String,
    /// Go-visible count. The current Go scan leaves this at zero.
    pub count: i64,
    /// Aggregate count used by Go's SQL ordering before its zero-value scan.
    pub rank_count: i64,
}

/// Typed failures from the tag-suggestion adapter.
#[derive(Debug, Error)]
pub enum TorrentTagsError {
    /// No database runtime was attached to this schema.
    #[error("torrent.suggestTags is unavailable without a PostgreSQL runtime")]
    Disabled,
    /// The read-only aggregation failed.
    #[error("torrent.suggestTags PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Runtime seam used by the tag-suggestion resolver.
#[async_trait]
pub trait TorrentTagsRuntime: Send + Sync {
    /// Aggregates at most ten suggestions by ascending count then name.
    async fn suggest_tags(
        &self,
        request: SuggestTagsRequest,
    ) -> std::result::Result<Vec<SuggestedTagRecord>, TorrentTagsError>;
}

struct DisabledTorrentTagsRuntime;

#[async_trait]
impl TorrentTagsRuntime for DisabledTorrentTagsRuntime {
    async fn suggest_tags(
        &self,
        _request: SuggestTagsRequest,
    ) -> std::result::Result<Vec<SuggestedTagRecord>, TorrentTagsError> {
        Err(TorrentTagsError::Disabled)
    }
}

/// PostgreSQL implementation of the tag-suggestion seam.
pub struct PgTorrentTagsRuntime {
    pool: PgPool,
}

impl PgTorrentTagsRuntime {
    /// Constructs a lazy adapter over a caller-owned pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TorrentTagsRuntime for PgTorrentTagsRuntime {
    async fn suggest_tags(
        &self,
        request: SuggestTagsRequest,
    ) -> std::result::Result<Vec<SuggestedTagRecord>, TorrentTagsError> {
        Ok(sqlx::query_as::<_, SuggestedTagRecord>(
            "SELECT name, 0::bigint AS count, count(*)::bigint AS rank_count \
             FROM torrent_tags \
             WHERE ($1::text = '' OR name LIKE ($1 || '%')) \
               AND (cardinality($2::text[]) = 0 OR NOT (name = ANY($2::text[]))) \
             GROUP BY name \
             ORDER BY rank_count ASC, name ASC \
             LIMIT $3",
        )
        .bind(request.prefix)
        .bind(request.exclusions)
        .bind(SUGGESTION_LIMIT)
        .fetch_all(&self.pool)
        .await?)
    }
}

/// GraphQL context wrapper for a torrent-tags runtime.
#[derive(Clone)]
pub struct TorrentTagsRuntimeData(Arc<dyn TorrentTagsRuntime>);

impl TorrentTagsRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentTagsRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the fail-loud context used by non-runtime schema builders.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentTagsRuntime))
    }

    /// Constructs the production PostgreSQL runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgTorrentTagsRuntime::new(pool)))
    }
}

pub(super) async fn resolve(
    runtime: &TorrentTagsRuntimeData,
    input: Option<SuggestTagsQueryInput>,
) -> Result<TorrentSuggestTagsResult> {
    let request = normalize_input(input)?;
    let mut suggestions = runtime
        .0
        .suggest_tags(request)
        .await
        .map_err(|error| Error::new(error.to_string()))?;
    suggestions.sort_by(|left, right| {
        left.rank_count
            .cmp(&right.rank_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    suggestions.truncate(usize::try_from(SUGGESTION_LIMIT).unwrap_or(10));
    Ok(TorrentSuggestTagsResult {
        suggestions: suggestions
            .into_iter()
            .map(|suggestion| SuggestedTag {
                name: suggestion.name,
                count: i32::try_from(suggestion.count).unwrap_or(i32::MAX),
            })
            .collect(),
    })
}

fn normalize_input(input: Option<SuggestTagsQueryInput>) -> Result<SuggestTagsRequest> {
    let input = input.unwrap_or(SuggestTagsQueryInput {
        exclusions: None,
        prefix: async_graphql::MaybeUndefined::Undefined,
    });
    let prefix = input.prefix.value().cloned().unwrap_or_default();
    if prefix.chars().count() > MAX_PREFIX_CHARS {
        return Err(Error::new(format!(
            "torrent.suggestTags prefix exceeds {MAX_PREFIX_CHARS} characters"
        )));
    }
    let exclusions = input.exclusions.unwrap_or_default();
    if exclusions.len() > MAX_EXCLUSIONS {
        return Err(Error::new(format!(
            "torrent.suggestTags exclusions has more than {MAX_EXCLUSIONS} entries"
        )));
    }
    if exclusions
        .iter()
        .any(|value| value.chars().count() > MAX_EXCLUSION_CHARS)
    {
        return Err(Error::new(format!(
            "torrent.suggestTags exclusion exceeds {MAX_EXCLUSION_CHARS} characters"
        )));
    }
    Ok(SuggestTagsRequest { prefix, exclusions })
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime;

    #[async_trait]
    impl TorrentTagsRuntime for FakeRuntime {
        async fn suggest_tags(
            &self,
            request: SuggestTagsRequest,
        ) -> std::result::Result<Vec<SuggestedTagRecord>, TorrentTagsError> {
            assert_eq!(
                request,
                SuggestTagsRequest {
                    prefix: "mov%".to_owned(),
                    exclusions: vec!["movie-old".to_owned()],
                }
            );
            Ok(vec![
                SuggestedTagRecord {
                    name: "movie-new".to_owned(),
                    count: 0,
                    rank_count: 4,
                },
                SuggestedTagRecord {
                    name: "movie".to_owned(),
                    count: 0,
                    rank_count: 2,
                },
            ])
        }
    }

    #[tokio::test]
    async fn schema_preserves_go_like_input_rank_order_and_zero_counts() {
        let runtime: Arc<dyn TorrentTagsRuntime> = Arc::new(FakeRuntime);
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentTagsRuntimeData::new(runtime))
            .finish();
        let response = schema
            .execute(
                "{ torrent { suggestTags(input: { prefix: \"mov%\", \
                 exclusions: [\"movie-old\"] }) { suggestions { name count } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({
                "torrent": { "suggestTags": { "suggestions": [
                    { "name": "movie", "count": 0 },
                    { "name": "movie-new", "count": 0 },
                ] } }
            })
        );
    }

    #[test]
    fn input_bounds_fail_closed() {
        let too_long = "x".repeat(MAX_PREFIX_CHARS + 1);
        let error = normalize_input(Some(SuggestTagsQueryInput {
            exclusions: None,
            prefix: async_graphql::MaybeUndefined::Value(too_long),
        }))
        .expect_err("oversized prefix is rejected");
        assert!(error.message.contains("prefix exceeds"));
    }
}
