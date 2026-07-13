//! GraphQL read API for the bitmagnet Rust rewrite (Phase-2).
//!
//! Lane G: async-graphql code-first schema reproducing the Go gqlgen SDL
//! (0-diff gate via [`normalize`]) plus the read resolvers.

mod health;
pub mod normalize;
pub mod schema;

pub use health::RuntimeConfig;
pub use schema::{build_runtime_schema, build_schema, schema, Schema};
