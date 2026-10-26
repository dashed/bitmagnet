//! Phase-2 count-per-value facet aggregation results.
//!
//! The shape mirrors `AggregationItem`, `AggregationGroup`, and `Aggregations`
//! in Go `internal/database/query/facets.go`. Lane S S3 fills these maps from
//! budgeted count SQL; Lane G owns GraphQL label ordering and transformation.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// How a facet's selected values combine.
///
/// Mirrors Go `model.FacetLogic` from
/// `internal/model/facet_logic_enum.go`. Lane S S3 uses `And` to combine
/// per-value criteria with SQL `AND`, and `Or` to combine them with SQL `OR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FacetLogic {
    /// Combine selected facet-value predicates with SQL `AND`.
    And,
    /// Combine selected facet-value predicates with SQL `OR`.
    Or,
}

/// One value bucket returned by a facet aggregation.
///
/// Mirrors Go `query.AggregationItem` in
/// `internal/database/query/facets.go`; Lane S S3 fills it from one
/// `BudgetedCount` SQL result for the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregationItem {
    /// Human-readable value label supplied by the Go-equivalent facet builder.
    pub label: String,
    /// Rows matching this facet value's count SQL.
    pub count: u64,
    /// Whether `BudgetedCount` exceeded the configured budget and estimated
    /// the count.
    pub is_estimate: bool,
}

/// The count-per-value buckets for one requested facet.
///
/// Mirrors Go `query.AggregationGroup` in
/// `internal/database/query/facets.go`. Lane S S3 returns deterministic
/// value-key order; Lane G applies `gqlmodel/facet.go` natural label sorting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregationGroup {
    /// Human-readable facet label.
    pub label: String,
    /// Logic used by the facet's selected-value SQL predicates.
    pub logic: FacetLogic,
    /// Value key to aggregation item. Go uses an unordered map; the Rust
    /// contract uses a deterministic map and leaves label natsort to Lane G.
    pub items: BTreeMap<String, AggregationItem>,
}

/// Facet key to aggregation group, mirroring Go
/// `maps.StringMap[AggregationGroup]` in `internal/database/query/facets.go`.
/// Keys are [`crate::TorrentContentFacet::key`] strings; Lane S S3 populates
/// the map from count-per-value SQL.
pub type Aggregations = BTreeMap<String, AggregationGroup>;
