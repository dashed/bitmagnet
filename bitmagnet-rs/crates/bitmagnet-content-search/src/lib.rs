//! Local content lookup for the B′ enrichment-parity lanes (Go
//! `internal/classifier/search.go` `localSearch`).
//!
//! Placeholder registered by the B′-0 classifier-dependency-seam lane so the
//! follow-on lane can land the PostgreSQL implementation of
//! [`bitmagnet_classifier::ContentResolver`]'s `content_by_id` /
//! `content_by_search` without editing the workspace manifest.
//!
//! 🚨 `content_by_search` must return the **ordered, pre-Levenshtein** candidate
//! list (Go's `query.Limit(10)` + `OrderByQueryStringRank`), never a single
//! winner — the tie-break belongs to `bitmagnet-textmatch`.
