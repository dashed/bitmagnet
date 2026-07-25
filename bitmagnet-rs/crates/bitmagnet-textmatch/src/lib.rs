//! Fuzzy title matching for the B′ enrichment-parity lanes.
//!
//! Placeholder registered by the B′-0 classifier-dependency-seam lane so the
//! follow-on lane can land `levenshteinFindBestMatch` (Go
//! `internal/classifier/util.go`) without editing the workspace manifest.
//!
//! 🔑 Levenshtein selection runs on the **Rust** side, over the ordered
//! candidate list returned by
//! [`bitmagnet_classifier::ContentResolver::content_by_search`]. Go's candidate
//! *ordering* is a nondeterministic PostgreSQL observation and is therefore
//! recorded by the parity tape; the first-wins tie-break over that ordering is
//! deterministic logic and belongs here.
