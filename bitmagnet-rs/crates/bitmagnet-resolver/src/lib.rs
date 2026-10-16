//! The **live** [`ContentResolver`] — Go's `classifier.dependencies`
//! (`internal/classifier/dependencies.go`), which injects a `LocalSearch` and a
//! `tmdb.Client` into the classifier.
//!
//! The classifier reaches its outside world through one object-safe trait, and
//! that trait spans two unrelated backends: PostgreSQL for the local content
//! table, HTTP for TMDB. Neither backend crate can implement it alone, and
//! neither should depend on the other — so the composition lives here, in a leaf
//! crate that depends on both and that nothing depends back on.
//!
//! # This file should stay boring
//!
//! Every method is a one-line delegation, and that is the design rather than an
//! accident. Both backends were built against the *same* seam contract and
//! already return [`ResolveError`] and the same `Option`/`Vec` shapes the trait
//! asks for, so there is no translation layer to get wrong here. If logic ever
//! starts accumulating in this file, it belongs in one of the backends instead:
//! anything decided here is invisible to the tape, and the tape is the only
//! reason the port can be shown to match Go at all.
//!
//! # What the tape does and does not cover
//!
//! [`bitmagnet_classifier::resolver::tape::TapeContentResolver`] is the recorded
//! counterpart of this type — the same trait, answered from a Go recording
//! instead of from live backends. A replay proves the classifier asks the right
//! questions; it cannot prove these backends *answer* them the way Go's do.
//!
//! Two gaps are worth naming because they need separate evidence:
//!
//! * 🚨 The tape records the **search string** handed to the query builder, not
//!   the tsquery it compiles into, so a replay is blind to
//!   `app_query_to_tsquery`. That compilation is covered by `bitmagnet-fts`'s
//!   own tests, not by any parity gate.
//! * 🚨 The **ordered candidate window** `content_by_search` returns is a
//!   database observation. `ts_rank_cd` is degenerate for these single-phrase
//!   queries — real corpora show whole result sets tied at exactly 1.0 — so the
//!   order is fixed by the identity tiebreak rather than by relevance, and a
//!   replay compares against whatever order Go happened to observe.

use std::sync::Arc;

use async_trait::async_trait;
use bitmagnet_classifier::resolver::tmdb;
use bitmagnet_classifier::{ContentResolver, ContentResultItem, ResolveError};
use bitmagnet_content_search::PgContentSearch;
use bitmagnet_model::{Content, ContentType};
use bitmagnet_tmdb::TmdbClient;

/// The live resolver: PostgreSQL for the local seams, TMDB over HTTP for the
/// rest.
///
/// Both halves are shared rather than owned exclusively, because Go builds one
/// of each per process and hands them to every classification: the local search
/// carries a capacity-1 permit that serialises queries, and the TMDB client
/// carries the rate limiter, the concurrency semaphore and the process-lifetime
/// unauthorized latch. Cloning the backends instead of sharing them would
/// silently give each caller its own limiter and its own latch, which is the
/// same bug twice.
pub struct LiveContentResolver {
    local: Arc<PgContentSearch>,
    tmdb: Arc<TmdbClient>,
}

impl LiveContentResolver {
    /// Compose a resolver over an existing local search and TMDB client.
    #[must_use]
    pub fn new(local: Arc<PgContentSearch>, tmdb: Arc<TmdbClient>) -> Self {
        Self { local, tmdb }
    }

    /// The local half, for callers that need it directly.
    #[must_use]
    pub fn local(&self) -> &Arc<PgContentSearch> {
        &self.local
    }

    /// The TMDB half, for callers that need it directly — validating the API key
    /// at startup, say, which Go does outside the classifier.
    #[must_use]
    pub fn tmdb(&self) -> &Arc<TmdbClient> {
        &self.tmdb
    }
}

#[async_trait]
impl ContentResolver for LiveContentResolver {
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        self.local.content_by_id(content_type, source, id).await
    }

    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        self.local
            .content_by_search(content_type, base_title, year)
            .await
    }

    async fn tmdb_find_by_external_id(
        &self,
        request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        self.tmdb.find_by_external_id(request).await
    }

    async fn tmdb_movie_details(
        &self,
        request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        self.tmdb.movie_details(request).await
    }

    async fn tmdb_tv_details(
        &self,
        request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        self.tmdb.tv_details(request).await
    }

    async fn tmdb_search_movie(
        &self,
        request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        self.tmdb.search_movie(request).await
    }

    async fn tmdb_search_tv(
        &self,
        request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        self.tmdb.search_tv(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classifier stores its resolver as `Arc<dyn ContentResolver>`, so the
    /// live one has to be usable that way. This is a compile-time assertion
    /// wearing a test's clothing: it needs no database and no API key, and it
    /// fails the build rather than a run if the trait or a signature drifts.
    #[test]
    fn the_live_resolver_is_object_safe() {
        fn assert_object_safe<T: ContentResolver + 'static>() {}
        assert_object_safe::<LiveContentResolver>();

        // And that the concrete type actually coerces, which is what
        // `Classifier::from_core_with` requires.
        fn accepts(_: Arc<dyn ContentResolver>) {}
        let _ = accepts;
    }
}
