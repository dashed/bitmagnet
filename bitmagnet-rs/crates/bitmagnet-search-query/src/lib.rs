//! Phase-1 `search-query`: the subset of the Go PG search query builder that
//! Torznab exercises, ported against sqlx (roadmap 05 §Phase 1; reused by
//! Phase 2). Semantics parity with `internal/database/search` is gated by the
//! Phase-0 differential harness — every predicate added here needs a fixture
//! pair.
//!
//! Lane contract (phase1-tasks.md): this crate owns query construction ONLY —
//! no HTTP, no XML, no Torznab category logic (that is bitmagnet-torznab).

pub mod builder {
    //! Query builder entry points (Lane Q implements).
}
