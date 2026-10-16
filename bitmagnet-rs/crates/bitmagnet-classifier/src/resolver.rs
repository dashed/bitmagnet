//! The classifier's **dependency seam** (B′-0): the one trait through which the
//! four `attach_*` actions reach the outside world.
//!
//! Go injects two collaborators into the classifier — `LocalSearch`
//! (`internal/classifier/search.go`, a PostgreSQL query) and `tmdb.Client`
//! (`internal/tmdb`, an HTTP client) — via `classifier.dependencies`
//! (`dependencies.go`). [`ContentResolver`] is the Rust analog, collapsed into a
//! single object-safe trait so a `Classifier` can be built over *either* the
//! live backends *or* a recorded tape without a generic parameter leaking into
//! every call site.
//!
//! # Why this shape supports record/replay
//!
//! Every method is a **pure function of owned, serialisable arguments** onto a
//! **fully-materialised, serialisable response**. Nothing is streamed, nothing
//! is lazily hydrated, and no method returns a handle that would need the live
//! backend to stay reachable. That makes each call a `(request, response)` pair
//! a tape can key on, and makes replay an exact substitution.
//!
//! The load-bearing consequence is [`ContentResolver::content_by_search`]:
//!
//! 🚨 It returns the **ordered, pre-Levenshtein candidate list**, not a single
//! winner. Go's `localSearch.ContentBySearch` (`search.go:80-87`) fetches up to
//! 10 candidates ordered by `query_string_rank`, then runs
//! `levenshteinFindBestMatch` **first-wins** over them. That ordering is a
//! PostgreSQL observation — ties in `ts_rank` are broken nondeterministically by
//! the plan (parallel `Gather Merge` order is not stable), so it can only ever
//! be *captured*, never *recomputed*. Splitting the seam here — the ordering is
//! taped, the first-wins Levenshtein tie-break runs in Rust
//! (`bitmagnet-textmatch`) — is what makes the parity oracle possible. A
//! resolver that returned `Option<Content>` would bake Go's tie-break into the
//! unobservable side of the boundary and there would be nothing left to compare.
//!
//! The same reasoning applies to the TMDB methods: they return the **raw**
//! decoded API responses, with `SearchMovie`/`SearchTv` handing back the full
//! ordered `results` array. The `Details → model.Content` transform (Go
//! `tmdb.MovieDetailsToMovieModel` / `TvShowDetailsToTvShowModel`) is
//! deterministic Rust-side logic and is a later lane's job, so it stays *out* of
//! the tape.
//!
//! # Nullability convention
//!
//! `Ok(None)` / `Ok(vec![])` means "the backend answered, and the answer is
//! nothing" — Go's `classification.ErrUnmatched` and `tmdb.ErrNotFound`. A
//! genuine failure (connection refused, 500, decode error) is
//! [`ResolveError`], which the classifier propagates as an `error` outcome
//! rather than an `unmatched` one, exactly as Go does.

pub mod tape;
pub mod tmdb;

use async_trait::async_trait;
use bitmagnet_model::{Content, ContentType};

/// A backend failure — *not* a miss. See the module's nullability convention.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// The local content search (PostgreSQL) failed.
    #[error("local content search failed: {0}")]
    LocalSearch(String),
    /// A TMDB API request failed.
    #[error("TMDB request failed: {0}")]
    Tmdb(String),
    /// A replaying resolver was asked for a call the tape does not hold. Kept
    /// distinct so a tape gap can never be silently misread as a miss.
    #[error("no recorded response for {0}")]
    TapeMiss(String),
    /// A resolver cannot serve this call at all — as distinct from the backend
    /// answering "nothing" or failing. Kept separate so an unimplemented path
    /// can never be mistaken for a miss (a real gap in a recording) or for
    /// `ErrUnmatched` (a real negative answer).
    #[error("unsupported resolver call: {0}")]
    Unsupported(String),
}

/// One ordered candidate from [`ContentResolver::content_by_search`] — Go
/// `search.ContentResultItem` (`search_content.go:13`), which embeds
/// `query.ResultItem` (the rank) and `model.Content`.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentResultItem {
    /// The candidate row.
    pub content: Content,
    /// The `ts_rank` the row was ordered by (`query.ResultItem.QueryStringRank`).
    ///
    /// Recorded for diagnosis only: the rank is *not* an input to the
    /// Levenshtein tie-break, and equal ranks are precisely the case where the
    /// list order is nondeterministic. Consumers must trust the **order**, not
    /// this number.
    pub query_string_rank: f64,
}

/// The classifier's outside world: local content lookup + the TMDB API.
///
/// Implementations must be cheap to share (`&self`, no interior mutation
/// visible to callers) and safe to call concurrently — Go runs the classifier
/// under a semaphore-bounded worker pool over the same dependencies.
#[async_trait]
pub trait ContentResolver: Send + Sync {
    /// Look a content row up by its `(type, source, id)` primary key — Go
    /// `LocalSearch.ContentByID`.
    ///
    /// Go dispatches on the source (`search.go:22-46`): `"tmdb"` matches the
    /// *canonical* identifier — the `content` primary key — while any other
    /// source matches an *alternative* identifier, i.e. an `EXISTS` over a
    /// `content_attributes` row joined on the content key whose `source` is the
    /// ref's source and whose `value` is the ref's id (note: the attribute `key`
    /// is deliberately **not** constrained). That dispatch is backend detail and
    /// stays behind this method.
    ///
    /// `Ok(None)` is Go's `ErrUnmatched`.
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError>;

    /// Full-text search the local content table for `base_title` — Go
    /// `LocalSearch.ContentBySearch`, **stopping before its tie-break**.
    ///
    /// 🚨 Returns the ordered candidate list (Go: `query.Limit(10)` +
    /// `query.OrderByQueryStringRank()`), NOT a single winner. The caller runs
    /// first-wins Levenshtein over it. See the module docs for why this split is
    /// non-negotiable.
    ///
    /// `year` is Go's `model.Year`; `Some(y)` adds Go's release-date range
    /// filter for that year, `None` omits it.
    ///
    /// An empty vector is Go's `ErrUnmatched`.
    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError>;

    /// `GET /find/{external_id}` — Go `tmdb.Client.FindByID`.
    async fn tmdb_find_by_external_id(
        &self,
        request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError>;

    /// `GET /movie/{id}` — Go `tmdb.Client.MovieDetails`. `Ok(None)` is
    /// `tmdb.ErrNotFound`, which Go maps to `ErrUnmatched`.
    async fn tmdb_movie_details(
        &self,
        request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError>;

    /// `GET /tv/{series_id}` — Go `tmdb.Client.TvDetails`. `Ok(None)` is
    /// `tmdb.ErrNotFound`.
    async fn tmdb_tv_details(
        &self,
        request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError>;

    /// `GET /search/movie` — Go `tmdb.Client.SearchMovie`. Returns the raw
    /// ordered `results` array; the Levenshtein pick happens caller-side.
    async fn tmdb_search_movie(
        &self,
        request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError>;

    /// `GET /search/tv` — Go `tmdb.Client.SearchTv`. Returns the raw ordered
    /// `results` array.
    async fn tmdb_search_tv(
        &self,
        request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError>;
}

/// The flags-off resolver: every lookup misses, nothing is ever attached.
///
/// This is the default injected by [`crate::Classifier::from_core`], and it is
/// what makes the B′-0 seam a **pure refactor**: with it in place the four
/// `attach_*` actions still resolve to `unmatched` for every input, so the 330
/// flags-off goldens and the 119,991-name replay corpus are bit-identical to
/// the pre-seam classifier.
///
/// It is also the honest model of production's flags-off configuration, where
/// `local_search_enabled` / `apis_enabled` / `tmdb_enabled` are all false and
/// the dependencies are never consulted.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullContentResolver;

#[async_trait]
impl ContentResolver for NullContentResolver {
    async fn content_by_id(
        &self,
        _content_type: ContentType,
        _source: &str,
        _id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        Ok(None)
    }

    async fn content_by_search(
        &self,
        _content_type: ContentType,
        _base_title: &str,
        _year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        Ok(Vec::new())
    }

    async fn tmdb_find_by_external_id(
        &self,
        _request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        Ok(tmdb::FindByIdResponse::default())
    }

    async fn tmdb_movie_details(
        &self,
        _request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        Ok(None)
    }

    async fn tmdb_tv_details(
        &self,
        _request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        Ok(None)
    }

    async fn tmdb_search_movie(
        &self,
        _request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        Ok(tmdb::SearchMovieResponse::default())
    }

    async fn tmdb_search_tv(
        &self,
        _request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        Ok(tmdb::SearchTvResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The seam must be usable as a trait object — the whole point of boxing the
    /// futures rather than using native RPITIT.
    #[test]
    fn null_resolver_is_object_safe_and_misses_everything() {
        let resolver: Box<dyn ContentResolver> = Box::new(NullContentResolver);
        futures::executor::block_on(async {
            assert_eq!(
                resolver
                    .content_by_id(ContentType::Movie, "tmdb", "603")
                    .await
                    .unwrap(),
                None
            );
            assert!(resolver
                .content_by_search(ContentType::Movie, "The Matrix", Some(1999))
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                resolver
                    .tmdb_movie_details(&tmdb::MovieDetailsRequest {
                        id: 603,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
                None
            );
        });
    }
}
