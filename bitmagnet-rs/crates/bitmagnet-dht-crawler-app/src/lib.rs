//! Bounded process configuration and ownership for the writer-capable Rust DHT
//! crawler.
//!
//! PostgreSQL connection material is deliberately absent from this crate's Clap
//! configuration. The eventual executable loads the existing redacted
//! `bitmagnet_db::DbConfig` through a separate environment-only boundary.

mod app_config;
mod http;

pub use app_config::{
    DhtCrawlerWriterAppConfig, DhtCrawlerWriterAppConfigError, DhtCrawlerWriterAppProjection,
    DhtCrawlerWriterProcessTimeout, DhtCrawlerWriterProcessTimeoutError,
    DHT_CRAWLER_WRITER_DEFAULT_PROCESS_TIMEOUT_SECONDS,
    DHT_CRAWLER_WRITER_MAX_PROCESS_TIMEOUT_SECONDS,
};
pub use http::{
    register_writer_metrics, writer_http_router, DhtCrawlerWriterDatabaseStatus,
    DhtCrawlerWriterRuntimeStatus, DhtCrawlerWriterStatusResponse, DhtCrawlerWriterWriteStatus,
};
