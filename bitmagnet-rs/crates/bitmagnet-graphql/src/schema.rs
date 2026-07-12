mod enums;
mod inputs;
mod objects;
mod roots;
mod scalars;

use async_graphql::EmptySubscription;

pub use roots::{Mutation, Query};

/// The complete bitmagnet GraphQL schema.
pub type Schema = async_graphql::Schema<Query, Mutation, EmptySubscription>;

/// Build the code-first GraphQL schema.
#[must_use]
pub fn schema() -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription).finish()
}
