pub(crate) mod enums;
mod inputs;
pub(crate) mod objects;
mod roots;
pub(crate) mod scalars;

use async_graphql::EmptySubscription;

use crate::health::{HealthRuntime, RuntimeConfig};

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

/// Build the runtime GraphQL schema with database health and optional peer
/// federation attached.
#[must_use]
pub fn build_runtime_schema(
    version: String,
    pool: bitmagnet_db::PgPool,
    config: RuntimeConfig,
) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(HealthRuntime::new(pool, config))
        .finish()
}

#[cfg(test)]
mod tests {
    use async_graphql::value;

    use super::{build_runtime_schema, build_schema};
    use crate::health::RuntimeConfig;

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

    #[tokio::test]
    async fn runtime_schema_reports_the_started_http_worker_without_peers() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let response = build_runtime_schema("test".into(), pool, RuntimeConfig::default())
            .execute("{ workers { listAll { workers { key started } } } }")
            .await;

        assert!(
            response.errors.is_empty(),
            "workers query returned errors: {:?}",
            response.errors
        );
        assert_eq!(
            response.data,
            value!({
                "workers": {
                    "listAll": {
                        "workers": [{ "key": "http_server", "started": true }]
                    }
                }
            })
        );
    }
}
