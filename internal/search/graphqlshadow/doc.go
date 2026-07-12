// Package graphqlshadow implements shadow-mode comparison for the GraphQL read
// API rewrite (Rust async-graphql vs the legacy Go /graphql), the Phase-2 Lane P
// deliverable.
//
// Unlike the Tantivy search shadow (internal/search/shadow, PG-vs-Tantivy on a
// synthetic corpus), the GraphQL surface carries real, continuous production
// traffic (the Angular /webui, the React /app, and Hermes). The shadow population
// is therefore sampled from that live traffic. This package supplies the two
// mechanism-agnostic pieces Lane P owns:
//
//   - The comparator (comparison.go): extends the engine-agnostic
//     shadow.Compare math (Jaccard@20/@50, RBO p=0.9, Top1, result-count delta)
//     with the two GraphQL-specific diffs — the per-facet-count diff over the 9
//     torrentContent facets and the total-count diff. It operates purely on the
//     already-extracted GraphQLResult projection, so it is identical whichever
//     shadow mechanism ships (the Traefik-mirror self-shadow inside the dark Rust
//     service, or the documented Go-embedded fallback).
//
//   - The self-shadow driver (driver.go) and its operation gate
//     (operation_gate.go): enforce the load-bearing safety property of the whole
//     shadow mechanism — a mutation must NEVER be double-executed. Live /graphql
//     traffic includes mutations; the self-shadow re-issues the mirrored request
//     to the live Go /graphql as its reference, so a mutation reaching that
//     reference call would double-apply a prod side effect (a second delete, a
//     second reprocess enqueue). The driver classifies the operation type FIRST
//     and hard-drops anything that is not a read-only query BEFORE any Rust
//     execution or Go reference call.
//
// The result IDs the comparator ranks are torrentContent InferIDs
// (hex(info_hash):content_type:content_source:content_id), the same stable key
// the Tantivy shadow uses (see internal/search/shadow/comparator.go and
// model.TorrentContent.InferID); response.go reconstructs them from a GraphQL
// search response.
package graphqlshadow
