//! Resolver-callable search-serving contract and its fail-closed implementation.

use bitmagnet_proto::v1::SortBy;

use crate::filters::{FileRowSort, FileRowsResult, Filters, PathGroup};
use crate::pg::{empty_result, QueryOptions, SearchResult};

/// Search-serving surface consumed by Lane G's GraphQL resolvers.
///
/// A method returning `served == false` instructs the resolver to fall back to
/// the plain PostgreSQL search path.
#[async_trait::async_trait]
pub trait SearchServe: Send + Sync {
    /// Routes a path search through L3 candidates to an exact refined page.
    async fn torrent_content(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
    ) -> crate::Result<(SearchResult, bool)>;

    /// Collapses exact-refined L3 candidate matches by path.
    async fn collapse_paths(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
    ) -> crate::Result<(Vec<PathGroup>, bool)>;

    /// Routes file-search text to exact-refined matching file rows.
    async fn search_file_rows(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sort_by: Vec<FileRowSort>,
    ) -> crate::Result<(FileRowsResult, bool)>;

    /// Derives child path-segment typeahead results from candidate refine.
    async fn path_typeahead(
        &self,
        prefix: String,
        options: QueryOptions,
        limit: u32,
    ) -> crate::Result<(Vec<String>, bool)>;

    /// Calls L3's prefix-index Suggest RPC, distinct from path typeahead.
    async fn suggest(&self, prefix: String, limit: u32) -> crate::Result<(Vec<String>, bool)>;

    /// Reports whether a query passes the server broad-gram length guard.
    fn eligible(&self, query: &str) -> bool;

    /// Reports whether the cached L3 health gate allows attempting the route.
    fn healthy(&self) -> bool;

    /// Reports whether the UI path-typeahead route is enabled.
    fn typeahead_enabled(&self) -> bool;

    /// Reports whether file-search text should route through L3 and L1 refine.
    fn file_search_route_text_enabled(&self) -> bool;

    /// Reports whether `collapse:path` should route through L3 and L1 refine.
    fn collapse_enabled(&self) -> bool;
}

/// Disabled, PostgreSQL-only state wired when pathsearch is turned off.
///
/// This is the Rust equivalent of Go's nil `*Composer`: every operation declines
/// to serve and every resolver-level tier-selection gate is false.
#[derive(Debug, Clone, Copy, Default)]
pub struct Disabled;

#[async_trait::async_trait]
impl SearchServe for Disabled {
    async fn torrent_content(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sorts: Vec<SortBy>,
    ) -> crate::Result<(SearchResult, bool)> {
        Ok((empty_result(), false))
    }

    async fn collapse_paths(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sorts: Vec<SortBy>,
    ) -> crate::Result<(Vec<PathGroup>, bool)> {
        Ok((Vec::new(), false))
    }

    async fn search_file_rows(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sort_by: Vec<FileRowSort>,
    ) -> crate::Result<(FileRowsResult, bool)> {
        Ok((FileRowsResult::default(), false))
    }

    async fn path_typeahead(
        &self,
        _prefix: String,
        _options: QueryOptions,
        _limit: u32,
    ) -> crate::Result<(Vec<String>, bool)> {
        Ok((Vec::new(), false))
    }

    async fn suggest(&self, _prefix: String, _limit: u32) -> crate::Result<(Vec<String>, bool)> {
        Ok((Vec::new(), false))
    }

    fn eligible(&self, _query: &str) -> bool {
        false
    }

    fn healthy(&self) -> bool {
        false
    }

    fn typeahead_enabled(&self) -> bool {
        false
    }

    fn file_search_route_text_enabled(&self) -> bool {
        false
    }

    fn collapse_enabled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_declines_every_route_and_gate() {
        let disabled = Disabled;

        let (torrent_result, torrent_served) = disabled
            .torrent_content(
                Filters::default(),
                QueryOptions::default(),
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(!torrent_served);
        assert!(torrent_result.items.is_empty());

        let (paths, collapse_served) = disabled
            .collapse_paths(
                Filters::default(),
                QueryOptions::default(),
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(!collapse_served);
        assert!(paths.is_empty());

        let (file_rows, file_rows_served) = disabled
            .search_file_rows(
                Filters::default(),
                QueryOptions::default(),
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(!file_rows_served);
        assert!(file_rows.rows.is_empty());

        let (typeahead, typeahead_served) = disabled
            .path_typeahead(String::from("abc"), QueryOptions::default(), 10)
            .await
            .unwrap();
        assert!(!typeahead_served);
        assert!(typeahead.is_empty());

        let (suggestions, suggest_served) =
            disabled.suggest(String::from("abc"), 10).await.unwrap();
        assert!(!suggest_served);
        assert!(suggestions.is_empty());

        assert!(!disabled.eligible("long enough"));
        assert!(!disabled.healthy());
        assert!(!disabled.typeahead_enabled());
        assert!(!disabled.file_search_route_text_enabled());
        assert!(!disabled.collapse_enabled());
    }
}
