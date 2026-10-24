//! Frozen dependency contract for the future L3 pathsearch gRPC client.

/// L3 path-bag candidate and suggestion source used by the future composer.
///
/// C2 supplies the real gRPC client; C1 exposes only the testable dependency
/// boundary ported from Go's `pathsearch.candidateSource`.
#[async_trait::async_trait]
pub trait CandidateSource: Send + Sync {
    /// Returns torrent-grained recall candidates for a path query.
    async fn path_candidates(
        &self,
        request: bitmagnet_proto::v1::PathCandidatesRequest,
    ) -> crate::Result<bitmagnet_proto::v1::PathCandidatesResponse>;

    /// Returns path-segment completions from the L3 prefix index.
    async fn suggest(
        &self,
        request: bitmagnet_proto::v1::SuggestRequest,
    ) -> crate::Result<bitmagnet_proto::v1::SuggestResponse>;

    /// Probes the L3 service's serving state and index metadata.
    async fn health_check(&self) -> crate::Result<bitmagnet_proto::v1::PathSearchHealth>;
}

/// Cached, lock-free L3 health signal read on the hot path (Go `HealthGate`).
///
/// A `None` gate means "no health signal wired" and trusts an empty L3 result
/// as authoritative.
pub type HealthGate = std::sync::Arc<dyn Fn() -> bool + Send + Sync>;
