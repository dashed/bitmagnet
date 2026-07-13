pub(crate) mod enums;
pub mod file_search_client;
mod inputs;
pub mod lane_s;
pub(crate) mod objects;
mod roots;
pub mod runtime;
pub(crate) mod scalars;
pub mod search;
mod search_resolvers;

use async_graphql::EmptySubscription;

use crate::health::{HealthRuntime, RuntimeConfig};

pub use roots::{Mutation, Query};
pub use search::{SearchRuntime, SearchRuntimeData};

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
        .data(SearchRuntimeData::disabled())
        .finish()
}

/// Build the GraphQL schema with a search runtime attached.
#[must_use]
pub fn build_search_schema(version: String, search: std::sync::Arc<dyn SearchRuntime>) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(SearchRuntimeData::new(search))
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
        .data(SearchRuntimeData::disabled())
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
    async fn mutations_remain_declared_but_are_explicitly_unserved() {
        let response = build_schema("test".into())
            .execute(
                r#"mutation {
                    torrent {
                        delete(infoHashes: ["0123456789abcdef0123456789abcdef01234567"])
                    }
                }"#,
            )
            .await;

        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0]
            .message
            .contains("declared for SDL parity but is not served"));
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
