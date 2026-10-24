//! Frozen configuration contract for the path composer and Tantivy serve router.

use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

/// Default per-torrent file-count sanity cap used by the L1 refine path.
pub const DEFAULT_MAX_REFINE_FILES: u32 = 300_000;
/// Default cumulative file-count budget for one refine chunk.
pub const DEFAULT_REFINE_FILE_BUDGET: u32 = 300_000;
/// Default maximum number of torrents in one refine chunk.
pub const DEFAULT_MAX_CHUNK_TORRENTS: u32 = 1024;
/// Default cumulative retained decoded-file budget for one request.
pub const DEFAULT_RETAINED_FILE_BUDGET: u32 = 1_000_000;
/// Default compressed-input and decompressed-output ceiling per blob (64 MiB).
pub const DEFAULT_MAX_REFINE_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
/// Default cumulative decode-allocation budget for one refine chunk (128 MiB).
/// Charges decompressed MessagePack plus owned path/extension string bytes.
pub const DEFAULT_REFINE_DECODED_BYTE_BUDGET: u64 = 128 * 1024 * 1024;
/// Default cumulative retained path/extension byte budget per request (64 MiB).
pub const DEFAULT_RETAINED_BYTE_BUDGET: u64 = 64 * 1024 * 1024;
/// Default hard memory cap on candidate torrents fetched and decoded.
pub const DEFAULT_MAX_CANDIDATES: u32 = 2000;
/// Default per-request latency cap on candidate torrents decoded.
pub const DEFAULT_MAX_DECODE_CANDIDATES: u32 = 200;
/// Default timeout for the complete L3 candidate and L1 refine route.
pub const DEFAULT_ROUTE_TIMEOUT: Duration = Duration::from_secs(8);
/// Default server-side broad-gram query-length guard.
pub const DEFAULT_MIN_QUERY_LENGTH: u32 = 3;
/// Default multiplier used to oversample the requested page window.
pub const DEFAULT_OVERSAMPLE_FACTOR: u32 = 4;

/// Bounds and gates the L3 candidate plus L1 exact-refine composer.
///
/// This is the Rust contract for Go's `pathsearch.ComposerConfig` plus the
/// corresponding defaults in `searchfx.Config`. Raw fields remain exactly as
/// authored; use [`Self::normalized`] for the zero-value fallbacks applied by
/// Go's `NewComposer`.
#[derive(Debug, Clone)]
pub struct ComposerConfig {
    /// Queries shorter than this never take the L3 route; zero disables the guard.
    pub min_query_length: u32,
    /// Multiplier applied to the page window when sizing the candidate budget.
    pub oversample_factor: u32,
    /// Hard memory cap on candidates fetched and decoded; zero uses the default.
    pub max_candidates: u32,
    /// Per-request latency cap on candidates decoded; zero uses the default.
    pub max_decode_candidates: u32,
    /// Whether the UI path-typeahead route is enabled.
    pub typeahead_enabled: bool,
    /// Whether file-search text is routed through L3 candidates and L1 refine.
    pub file_search_route_text: bool,
    /// Whether `collapse:path` is routed through L3 candidates and L1 refine.
    pub collapse_enabled: bool,
    /// Per-torrent file-count sanity cap; zero uses the default.
    pub max_refine_files: u32,
    /// Cumulative file-count budget for one refine chunk; zero uses the default.
    pub refine_file_budget: u32,
    /// Maximum torrents in one refine chunk; zero uses the default.
    pub max_chunk_torrents: u32,
    /// Cumulative retained decoded-file budget; zero uses the default.
    pub retained_file_budget: u32,
    /// Per-torrent compressed input and decompressed MessagePack ceiling; zero uses the default.
    pub max_refine_decompressed_bytes: u64,
    /// Cumulative raw MessagePack plus owned string bytes per chunk; zero uses the default.
    pub refine_decoded_byte_budget: u64,
    /// Cumulative retained path/extension bytes per request; zero uses the default.
    pub retained_byte_budget: u64,
    /// Timeout for the whole candidate and refine route; zero uses the default.
    pub route_timeout: Duration,
    /// Maximum concurrent blob-decode refines; zero means one per CPU.
    pub max_concurrent_refines: usize,
    /// Time a blob-decode refine waits for a slot; zero queues to the route deadline.
    pub slot_wait: Duration,
}

impl Default for ComposerConfig {
    fn default() -> Self {
        Self {
            min_query_length: DEFAULT_MIN_QUERY_LENGTH,
            oversample_factor: DEFAULT_OVERSAMPLE_FACTOR,
            max_candidates: DEFAULT_MAX_CANDIDATES,
            max_decode_candidates: DEFAULT_MAX_DECODE_CANDIDATES,
            typeahead_enabled: false,
            file_search_route_text: true,
            collapse_enabled: false,
            max_refine_files: DEFAULT_MAX_REFINE_FILES,
            refine_file_budget: DEFAULT_REFINE_FILE_BUDGET,
            max_chunk_torrents: DEFAULT_MAX_CHUNK_TORRENTS,
            retained_file_budget: DEFAULT_RETAINED_FILE_BUDGET,
            max_refine_decompressed_bytes: DEFAULT_MAX_REFINE_DECOMPRESSED_BYTES,
            refine_decoded_byte_budget: DEFAULT_REFINE_DECODED_BYTE_BUDGET,
            retained_byte_budget: DEFAULT_RETAINED_BYTE_BUDGET,
            route_timeout: DEFAULT_ROUTE_TIMEOUT,
            max_concurrent_refines: 0,
            slot_wait: Duration::ZERO,
        }
    }
}

impl ComposerConfig {
    /// Resolves the blob-decode refine limit, using available CPU parallelism for zero.
    #[must_use]
    pub fn resolved_max_concurrent_refines(&self) -> usize {
        if self.max_concurrent_refines == 0 {
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1)
        } else {
            self.max_concurrent_refines
        }
    }

    /// Returns a copy with Go `NewComposer`'s safe zero-value fallbacks applied.
    ///
    /// The authored value is not mutated. Concurrency is intentionally resolved
    /// separately by [`Self::resolved_max_concurrent_refines`].
    #[must_use]
    pub fn normalized(&self) -> Self {
        let mut config = self.clone();

        config.oversample_factor = config.oversample_factor.max(1);
        if config.max_candidates == 0 {
            config.max_candidates = DEFAULT_MAX_CANDIDATES;
        }
        if config.max_decode_candidates == 0 {
            config.max_decode_candidates = DEFAULT_MAX_DECODE_CANDIDATES;
        }
        if config.max_refine_files == 0 {
            config.max_refine_files = DEFAULT_MAX_REFINE_FILES;
        }
        if config.refine_file_budget == 0 {
            config.refine_file_budget = DEFAULT_REFINE_FILE_BUDGET;
        }
        if config.max_chunk_torrents == 0 {
            config.max_chunk_torrents = DEFAULT_MAX_CHUNK_TORRENTS;
        }
        if config.retained_file_budget == 0 {
            config.retained_file_budget = DEFAULT_RETAINED_FILE_BUDGET;
        }
        if config.max_refine_decompressed_bytes == 0 {
            config.max_refine_decompressed_bytes = DEFAULT_MAX_REFINE_DECOMPRESSED_BYTES;
        }
        if config.refine_decoded_byte_budget == 0 {
            config.refine_decoded_byte_budget = DEFAULT_REFINE_DECODED_BYTE_BUDGET;
        }
        if config.retained_byte_budget == 0 {
            config.retained_byte_budget = DEFAULT_RETAINED_BYTE_BUDGET;
        }
        if config.route_timeout.is_zero() {
            config.route_timeout = DEFAULT_ROUTE_TIMEOUT;
        }

        config
    }
}

/// Selects how the engine-level router uses the Tantivy search sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServeMode {
    /// Serve only from PostgreSQL and never call the Tantivy sidecar.
    #[default]
    Postgres,
    /// Serve PostgreSQL while sampling background Tantivy comparisons.
    Shadow,
    /// Serve a configured canary share from healthy Tantivy, failing closed to PG.
    Canary,
    /// Serve the eligible query class from healthy Tantivy, failing closed to PG.
    Tantivy,
}

impl ServeMode {
    /// Reports whether this mode may serve a response from Tantivy.
    #[must_use]
    pub fn serving(&self) -> bool {
        matches!(self, Self::Canary | Self::Tantivy)
    }

    /// Reports whether this mode may call Tantivy for serving or shadowing.
    #[must_use]
    pub fn tantivy_backed(&self) -> bool {
        matches!(self, Self::Shadow | Self::Canary | Self::Tantivy)
    }
}

impl FromStr for ServeMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "postgres" => Ok(Self::Postgres),
            "shadow" => Ok(Self::Shadow),
            "canary" => Ok(Self::Canary),
            "tantivy" => Ok(Self::Tantivy),
            _ => Err(format!("invalid serve mode: {value}")),
        }
    }
}

impl fmt::Display for ServeMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Postgres => "postgres",
            Self::Shadow => "shadow",
            Self::Canary => "canary",
            Self::Tantivy => "tantivy",
        })
    }
}

/// Configuration contract for the Phase-6 Tantivy serving router decorator.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Routing strategy; PostgreSQL is the fail-closed default.
    pub mode: ServeMode,
    /// Fraction of queries sampled for background shadow comparison.
    pub sample_rate: f64,
    /// Percentage of eligible requests served by the canary route.
    pub canary_percent: u32,
    /// Timeout for the Tantivy Search RPC on the serve hot path.
    pub serve_timeout: Duration,
    /// Maximum acceptable Tantivy watermark lag for serving.
    pub max_staleness: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            mode: ServeMode::Postgres,
            sample_rate: 0.0,
            canary_percent: 0,
            serve_timeout: Duration::from_millis(800),
            max_staleness: Duration::from_secs(120),
        }
    }
}

/// C5 Tantivy-serve is GATED (Risk P2-2: blocked on P4 shadow soak verdict
/// ≥2026-07-16); this is the fail-closed placeholder — never serves.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledServeRouter;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composer_defaults_match_the_go_contract() {
        let config = ComposerConfig::default();

        assert_eq!(config.min_query_length, DEFAULT_MIN_QUERY_LENGTH);
        assert_eq!(config.oversample_factor, DEFAULT_OVERSAMPLE_FACTOR);
        assert_eq!(config.max_candidates, DEFAULT_MAX_CANDIDATES);
        assert_eq!(config.max_decode_candidates, DEFAULT_MAX_DECODE_CANDIDATES);
        assert!(!config.typeahead_enabled);
        assert!(config.file_search_route_text);
        assert!(!config.collapse_enabled);
        assert_eq!(config.max_refine_files, DEFAULT_MAX_REFINE_FILES);
        assert_eq!(config.refine_file_budget, DEFAULT_REFINE_FILE_BUDGET);
        assert_eq!(config.max_chunk_torrents, DEFAULT_MAX_CHUNK_TORRENTS);
        assert_eq!(config.retained_file_budget, DEFAULT_RETAINED_FILE_BUDGET);
        assert_eq!(
            config.max_refine_decompressed_bytes,
            DEFAULT_MAX_REFINE_DECOMPRESSED_BYTES
        );
        assert_eq!(
            config.refine_decoded_byte_budget,
            DEFAULT_REFINE_DECODED_BYTE_BUDGET
        );
        assert_eq!(config.retained_byte_budget, DEFAULT_RETAINED_BYTE_BUDGET);
        assert_eq!(config.route_timeout, DEFAULT_ROUTE_TIMEOUT);
        assert_eq!(config.max_concurrent_refines, 0);
        assert_eq!(config.slot_wait, Duration::ZERO);
        assert!(config.resolved_max_concurrent_refines() >= 1);
    }

    #[test]
    fn normalized_applies_safe_zero_fallbacks_without_mutating_raw_values() {
        let raw = ComposerConfig {
            oversample_factor: 0,
            max_candidates: 0,
            max_decode_candidates: 0,
            max_refine_files: 0,
            refine_file_budget: 0,
            max_chunk_torrents: 0,
            retained_file_budget: 0,
            max_refine_decompressed_bytes: 0,
            refine_decoded_byte_budget: 0,
            retained_byte_budget: 0,
            route_timeout: Duration::ZERO,
            max_concurrent_refines: 7,
            ..ComposerConfig::default()
        };

        let normalized = raw.normalized();

        assert_eq!(raw.oversample_factor, 0);
        assert_eq!(raw.max_candidates, 0);
        assert_eq!(raw.route_timeout, Duration::ZERO);
        assert_eq!(normalized.oversample_factor, 1);
        assert_eq!(normalized.max_candidates, DEFAULT_MAX_CANDIDATES);
        assert_eq!(
            normalized.max_decode_candidates,
            DEFAULT_MAX_DECODE_CANDIDATES
        );
        assert_eq!(normalized.max_refine_files, DEFAULT_MAX_REFINE_FILES);
        assert_eq!(normalized.refine_file_budget, DEFAULT_REFINE_FILE_BUDGET);
        assert_eq!(normalized.max_chunk_torrents, DEFAULT_MAX_CHUNK_TORRENTS);
        assert_eq!(
            normalized.retained_file_budget,
            DEFAULT_RETAINED_FILE_BUDGET
        );
        assert_eq!(
            normalized.max_refine_decompressed_bytes,
            DEFAULT_MAX_REFINE_DECOMPRESSED_BYTES
        );
        assert_eq!(
            normalized.refine_decoded_byte_budget,
            DEFAULT_REFINE_DECODED_BYTE_BUDGET
        );
        assert_eq!(
            normalized.retained_byte_budget,
            DEFAULT_RETAINED_BYTE_BUDGET
        );
        assert_eq!(normalized.route_timeout, DEFAULT_ROUTE_TIMEOUT);
        assert_eq!(normalized.max_concurrent_refines, 7);
        assert_eq!(normalized.resolved_max_concurrent_refines(), 7);
    }

    #[test]
    fn serve_defaults_match_the_router_contract() {
        let config = ServeConfig::default();

        assert_eq!(config.mode, ServeMode::Postgres);
        assert_eq!(config.sample_rate, 0.0);
        assert_eq!(config.canary_percent, 0);
        assert_eq!(config.serve_timeout, Duration::from_millis(800));
        assert_eq!(config.max_staleness, Duration::from_secs(120));
    }

    #[test]
    fn serve_modes_round_trip_and_report_capabilities() {
        let cases = [
            (ServeMode::Postgres, "postgres", false, false),
            (ServeMode::Shadow, "shadow", false, true),
            (ServeMode::Canary, "canary", true, true),
            (ServeMode::Tantivy, "tantivy", true, true),
        ];

        for (mode, text, serving, tantivy_backed) in cases {
            assert_eq!(text.parse::<ServeMode>().unwrap(), mode);
            assert_eq!(mode.to_string(), text);
            assert_eq!(mode.serving(), serving);
            assert_eq!(mode.tantivy_backed(), tantivy_backed);
        }

        assert!("invalid".parse::<ServeMode>().is_err());
    }
}
