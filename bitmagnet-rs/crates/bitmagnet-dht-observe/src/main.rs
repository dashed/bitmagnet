use anyhow::{bail, Context as _};
use bitmagnet_dht_crawler::{DhtCrawlerObserveOnlyAppConfig, DhtCrawlerObserveOnlySupervisor};
use bitmagnet_dht_observe::{
    register_observe_only_metrics, supervise_observe_only_process, DhtObserveShutdownTimeout,
    DhtObserveSignalReceiver,
};
use clap::Parser;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-dht-observe",
    about = "PostgreSQL-nonmutating Rust DHT network observer",
    disable_help_subcommand = true,
    after_long_help = "The optional common metrics listener is configured separately through \
BITMAGNET_METRICS_ADDR; an unset or empty value disables it."
)]
struct Args {
    #[command(flatten)]
    observe: DhtCrawlerObserveOnlyAppConfig,

    /// Whole-process grace period before unfinished owned tasks are forced down.
    #[arg(
        long = "graceful-shutdown-timeout-seconds",
        env = "BITMAGNET_DHT_OBSERVE_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS",
        default_value_t = DhtObserveShutdownTimeout::DEFAULT
    )]
    graceful_shutdown_timeout: DhtObserveShutdownTimeout,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    let projection = args.observe.projection()?;
    let signals = DhtObserveSignalReceiver::install()
        .context("could not install observe-only process signal handlers")?;
    let (http_addr, graph) = projection.into_parts();

    // TCP and the optional metrics listener bind before the DHT UDP runtime,
    // so address conflicts cannot leave a partially started graph behind.
    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("could not bind observe-only HTTP listener {http_addr}"))?;
    let bound_http_addr = listener.local_addr()?;
    let metrics = bitmagnet_common::metrics::maybe_spawn_metrics_server()
        .await
        .context("could not bind optional observe-only metrics listener")?;
    let (supervisor, observability) = match DhtCrawlerObserveOnlySupervisor::start(graph).await {
        Ok(started) => started,
        Err(error) => {
            if let Some((handle, _)) = metrics {
                handle.abort();
                let _ = handle.await;
            }
            return Err(error.into());
        }
    };
    let dht_addr = supervisor.local_addr();
    register_observe_only_metrics(observability.clone());
    let metrics_handle = match metrics {
        Some((handle, address)) => {
            info!(%address, "observe-only metrics listener started");
            Some(handle)
        }
        None => {
            info!(
                "observe-only metrics listener disabled; BITMAGNET_METRICS_ADDR is unset or empty"
            );
            None
        }
    };

    info!(http_addr = %bound_http_addr, %dht_addr, "bitmagnet-dht-observe started");
    let exit = supervise_observe_only_process(
        listener,
        supervisor,
        observability,
        metrics_handle,
        signals.recv(),
        args.graceful_shutdown_timeout,
    )
    .await;
    if !exit.is_success() {
        error!(?exit, "bitmagnet-dht-observe stopped abnormally");
        bail!("observe-only process did not complete a clean signal-triggered shutdown");
    }
    info!(?exit, "bitmagnet-dht-observe stopped cleanly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use clap::{CommandFactory, FromArgMatches};

    use super::*;

    fn try_parse_without_ambient_env<const N: usize>(
        args: [&'static str; N],
    ) -> Result<Args, clap::Error> {
        let command = Args::command().mut_args(|argument| argument.env(None::<&str>));
        let matches = command.try_get_matches_from(args)?;
        Args::from_arg_matches(&matches)
    }

    #[test]
    fn app_cli_flattens_exactly_the_closed_graph_and_bounded_process_grace() {
        let args = try_parse_without_ambient_env([
            "bitmagnet-dht-observe",
            "--http-server-local-address",
            "127.0.0.1:0",
            "--dht-server-port",
            "0",
            "--dht-server-query-timeout",
            "1500ms",
            "--dht-crawler-scaling-factor",
            "2",
            "--dht-crawler-bootstrap-nodes",
            "one.invalid:1,two.invalid:2",
            "--dht-crawler-reseed-bootstrap-nodes-interval",
            "1m30s",
            "--graceful-shutdown-timeout-seconds",
            "7",
        ])
        .expect("observe-only app CLI parses");
        assert_eq!(args.graceful_shutdown_timeout.seconds(), 7);
        let projection = args.observe.projection().unwrap();
        assert_eq!(projection.http_listen_addr, "127.0.0.1:0".parse().unwrap());
        assert_eq!(projection.graph.runtime.bind_addr.port(), 0);
        assert_eq!(projection.graph.runtime.query_timeout.as_millis(), 1_500);
        assert_eq!(projection.graph.runtime.discovery_capacity.get(), 200);
        assert_eq!(projection.graph.maintenance.ping_capacity.get(), 2);
        assert_eq!(
            projection.graph.maintenance.bootstrap_ping.bootstrap_nodes,
            ["one.invalid:1", "two.invalid:2"]
        );
        assert_eq!(
            projection
                .graph
                .maintenance
                .bootstrap_ping
                .reseed_interval
                .as_secs(),
            90
        );

        assert!(try_parse_without_ambient_env([
            "bitmagnet-dht-observe",
            "--graceful-shutdown-timeout-seconds",
            "0",
        ])
        .is_err());
        assert!(try_parse_without_ambient_env([
            "bitmagnet-dht-observe",
            "--graceful-shutdown-timeout-seconds",
            "301",
        ])
        .is_err());
        assert!(try_parse_without_ambient_env([
            "bitmagnet-dht-observe",
            "--expected-goose-version",
            "29",
        ])
        .is_err());
    }

    #[test]
    fn args_have_exactly_seven_environment_assignments() {
        let actual = Args::command()
            .get_arguments()
            .filter_map(|argument| {
                argument.get_env().map(|value| {
                    (
                        argument.get_id().as_str().to_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        let expected = [
            ("bootstrap_nodes", "DHT_CRAWLER_BOOTSTRAP_NODES"),
            (
                "graceful_shutdown_timeout",
                "BITMAGNET_DHT_OBSERVE_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS",
            ),
            ("dht_server_port", "DHT_SERVER_PORT"),
            ("dht_server_query_timeout", "DHT_SERVER_QUERY_TIMEOUT"),
            ("http_server_local_address", "HTTP_SERVER_LOCAL_ADDRESS"),
            ("scaling_factor", "DHT_CRAWLER_SCALING_FACTOR"),
            (
                "reseed_bootstrap_nodes_interval",
                "DHT_CRAWLER_RESEED_BOOTSTRAP_NODES_INTERVAL",
            ),
        ]
        .into_iter()
        .map(|(argument, environment)| (argument.to_owned(), environment.to_owned()))
        .collect();

        assert_eq!(actual, expected);
        assert!(!actual
            .values()
            .any(|value| value == "BITMAGNET_METRICS_ADDR"));
    }
}
