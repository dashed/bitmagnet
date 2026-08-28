// Package graphqlshadow implements shadow-mode comparison for the GraphQL read
// API rewrite (Rust async-graphql vs the legacy Go /graphql), the Phase-2 Lane P
// deliverable.
//
// Unlike the Tantivy search shadow (internal/search/shadow, PG-vs-Tantivy on a
// synthetic corpus), the GraphQL surface carries real, continuous production
// traffic (the Angular /webui, the React /app, and Hermes). The shadow population
// is therefore sampled from that live traffic. This package supplies the two
// embedded runtime pieces Lane P owns:
//
//   - The comparator (comparison.go): extends the engine-agnostic
//     shadow.Compare math (Jaccard@20/@50, RBO p=0.9, Top1, result-count delta)
//     with the two GraphQL-specific diffs — the per-facet-count diff over the 9
//     torrentContent facets and the total-count diff. It operates purely on the
//     already-extracted GraphQLResult projection.
//
//   - The gqlgen response hook (hook.go), dark HTTP executor, and operation gate:
//     capture the already-computed primary Go response and its request-entry to
//     pre-write response-generation duration, classify the selected operation
//     before any Rust call, sample and admit work non-blockingly, then call Rust
//     in a detached timeout-bounded background task. Mutations, subscriptions,
//     parse failures, and ambiguous operations make zero Rust calls. The design
//     contains no Go reference client, so a second Go execution is structurally
//     impossible.
//
// The result IDs the comparator ranks are torrentContent InferIDs
// (hex(info_hash):content_type:content_source:content_id), the same stable key
// the Tantivy shadow uses (see internal/search/shadow/comparator.go and
// model.TorrentContent.InferID); response.go reconstructs them from a GraphQL
// search response.
package graphqlshadow
