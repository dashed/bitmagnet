//! Bounded process configuration and ownership for the writer-capable Rust DHT
//! crawler.
//!
//! PostgreSQL connection material is deliberately absent from this crate's Clap
//! configuration. The executable loads the existing redacted
//! `bitmagnet_db::DbConfig` through a separate environment-only boundary. This
//! source composition has not yet passed live PostgreSQL, image, deployment, or
//! restore/rollback admission gates.
//!
//! One signal future is polled throughout startup and steady state, including a
//! biased activation gate before the supervisor can start workers. Startup
//! cleanup and forced process joins are deadline-raced; missing terminal task
//! evidence is retained as `None` rather than extending the hard deadline or
//! claiming quiescence. The executable's runtime guard also uses a zero-wait
//! shutdown on normal return or unwind so blocking OS resolution cannot extend
//! process exit without bound. Unknown PostgreSQL DSN query keys fail before
//! SQLx can log their values, and remaining DSN parse errors are redacted.

mod app_config;
mod http;
mod process;

pub use app_config::{
    DhtCrawlerWriterAppConfig, DhtCrawlerWriterAppConfigError, DhtCrawlerWriterAppProjection,
    DhtCrawlerWriterProcessTimeout, DhtCrawlerWriterProcessTimeoutError,
    DHT_CRAWLER_WRITER_DEFAULT_PROCESS_TIMEOUT_SECONDS,
    DHT_CRAWLER_WRITER_MAX_PROCESS_TIMEOUT_SECONDS, DHT_CRAWLER_WRITER_MIN_PROCESS_TIMEOUT_SECONDS,
};
pub use http::{
    register_writer_metrics, writer_http_router, DhtCrawlerWriterDatabaseStatus,
    DhtCrawlerWriterRuntimeStatus, DhtCrawlerWriterStatusResponse, DhtCrawlerWriterWriteStatus,
};
pub use process::{
    supervise_writer_process, DhtCrawlerWriterFinalizerDisposition,
    DhtCrawlerWriterForcedStopReason, DhtCrawlerWriterPoolCloseDisposition,
    DhtCrawlerWriterProcessExit, DhtCrawlerWriterProcessSignal, DhtCrawlerWriterProcessTrigger,
    DhtCrawlerWriterSignalReceiver, DhtCrawlerWriterTaskExit,
};
