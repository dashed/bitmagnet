mod enums;
mod inputs;
mod objects;
mod roots;
mod scalars;

use async_graphql::EmptySubscription;

pub use roots::{Mutation, Query};

/// Runtime version data available to GraphQL resolvers.
pub struct Version(pub String);

/// The complete bitmagnet GraphQL schema.
pub type Schema = async_graphql::Schema<Query, Mutation, EmptySubscription>;

/// Build the code-first GraphQL schema.
#[must_use]
pub fn schema() -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription).finish()
}

/// Build the GraphQL schema with runtime version context attached.
#[must_use]
pub fn build_schema(version: String) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .finish()
}

#[cfg(test)]
mod tests {
    use async_graphql::value;

    use super::build_schema;

    #[tokio::test]
    async fn build_schema_injects_version_into_query_resolver() {
        let response = build_schema("t1.2.3".into()).execute("{ version }").await;

        assert!(
            response.errors.is_empty(),
            "version query returned errors: {:?}",
            response.errors
        );
        assert_eq!(response.data, value!({ "version": "t1.2.3" }));
    }
}
