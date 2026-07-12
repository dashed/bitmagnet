//! GraphQL read API for the bitmagnet Rust rewrite (Phase-2).
//!
//! Lane G: async-graphql code-first schema reproducing the Go gqlgen SDL
//! (0-diff gate via [`normalize`]) plus the read resolvers.

pub mod normalize;
pub mod schema;

pub use schema::{schema, Schema};
