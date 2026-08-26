//! Strict source-only consumer for the frozen Go crawler-composition oracle.
//!
//! Normalized Go AST digests are pinned as oracle evidence. Rust deliberately
//! does not reproduce the Go parser/formatter normalization algorithm.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::{Digest, Sha256};

const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_composition.jsonl"
));
const FIXTURE_SHA256: &str = "fc1dfd4e28f0cd32aeef424af5f4b8aa65f18c0a663589e31daa174010bb0474";
const FIXTURE_BYTE_LENGTH: usize = 14_725;
const FIXTURE_ID: &str = "production_construction_and_lifecycle_source_contract";

const SOURCE_FUNCTIONS: [&str; 16] = [
    "config.NewDefaultConfig",
    "factory.New",
    "crawler.start",
    "discovered.NewDiscoveredNodes",
    "batching.NewBatchingChannel",
    "buffered.NewBufferedConcurrentChannel",
    "buffered.Run",
    "metainforequester.New",
    "requestLimiter.Request",
    "protocol.RandomPeerID",
    "dhtfx.New",
    "protocol.RandomNodeIDWithClientSuffix",
    "blocking.New",
    "persist.runPersistTorrents",
    "persist.runPersistSources",
    "persist.persistScrapedTorrentSources",
];

const LAUNCH_FUNCTIONS: [&str; 15] = [
    "rotateSoughtNodeID",
    "runDiscoveredNodes",
    "runPing",
    "runFindNode",
    "getNodesForFindNode",
    "runSampleInfoHashes",
    "getNodesForSampleInfoHashes",
    "runInfoHashTriage",
    "runGetPeers",
    "runRequestMetaInfo",
    "runScrape",
    "reseedBootstrapNodes",
    "runPersistTorrents",
    "runPersistSources",
    "getOldNodes",
];

#[derive(Clone, Copy)]
struct ExpectedRoute {
    name: &'static str,
    capacity: u64,
    batch_size: u64,
    batch_interval_milliseconds: i64,
    concurrency: u64,
    implementation: &'static str,
}

const ROUTES: [ExpectedRoute; 10] = [
    ExpectedRoute {
        name: "discovered_nodes",
        capacity: 1_000,
        batch_size: 10,
        batch_interval_milliseconds: 10,
        concurrency: 0,
        implementation: "BatchingChannel",
    },
    ExpectedRoute {
        name: "nodes_for_ping",
        capacity: 10,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 10,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "nodes_for_find_node",
        capacity: 100,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 100,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "nodes_for_sample_infohashes",
        capacity: 100,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 100,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "info_hash_triage",
        capacity: 100,
        batch_size: 1_000,
        batch_interval_milliseconds: 20_000,
        concurrency: 0,
        implementation: "BatchingChannel",
    },
    ExpectedRoute {
        name: "get_peers",
        capacity: 100,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 200,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "scrape",
        capacity: 100,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 200,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "request_meta_info",
        capacity: 100,
        batch_size: 0,
        batch_interval_milliseconds: 0,
        concurrency: 400,
        implementation: "BufferedConcurrentChannel",
    },
    ExpectedRoute {
        name: "persist_torrents",
        capacity: 1_000,
        batch_size: 1_000,
        batch_interval_milliseconds: 60_000,
        concurrency: 0,
        implementation: "BatchingChannel",
    },
    ExpectedRoute {
        name: "persist_sources",
        capacity: 1_000,
        batch_size: 1_000,
        batch_interval_milliseconds: 60_000,
        concurrency: 0,
        implementation: "BatchingChannel",
    },
];

const SCALING_FORMULAS: [(&str, &str, &str); 8] = [
    ("discovered_nodes", "int(100*ScalingFactor)", ""),
    ("nodes_for_ping", "int(ScalingFactor)", "int(ScalingFactor)"),
    (
        "nodes_for_find_node",
        "10*int(ScalingFactor)",
        "10*int(ScalingFactor)",
    ),
    (
        "nodes_for_sample_infohashes",
        "10*int(ScalingFactor)",
        "10*int(ScalingFactor)",
    ),
    ("info_hash_triage", "10*int(ScalingFactor)", ""),
    (
        "get_peers",
        "10*int(ScalingFactor)",
        "20*int(ScalingFactor)",
    ),
    ("scrape", "10*int(ScalingFactor)", "20*int(ScalingFactor)"),
    (
        "request_meta_info",
        "10*int(ScalingFactor)",
        "40*int(ScalingFactor)",
    ),
];

const GO_NONCLAIMS: [&str; 6] = [
    "no_production_function_channel_goroutine_Fx_hook_DNS_network_database_limiter_clock_logger_or_metric_execution",
    "no_runtime_goroutine_start_completion_or_map_iteration_order",
    "no_Go_shutdown_join_queue_drain_final_batch_flush_or_completed_side_effect_guarantee",
    "no_relative_order_claim_between_worker_registry_stop_and_Fx_app_hooks",
    "no_database_transaction_rows_affected_rollback_commit_durability_or_partial_chunk_runtime_evidence",
    "no_Rust_supervisor_application_deployment_or_production_readiness_claim",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 1] = [(
    FIXTURE_ID,
    "GO_SOURCE_INSPECTION_ONLY_NO_RUST_COMPOSITION_RUNTIME_REPLAY",
)];

const RUST_OWNED_HARDENINGS: [&str; 5] = [
    "Rust_injected_stable_peer_ID_is_not_Go_random_factory_parity",
    "Rust_request_limiter_uses_lazy_expiry_without_the_Go_cleanup_task_and_omits_logger_and_Prometheus_wrappers",
    "Rust_joined_staged_drain_and_blocking_finalization_are_hardening_not_Go_OnStop_parity",
    "Rust_source_writer_whole_transaction_contract_differs_from_Go_possible_prior_chunk_commits",
    "Rust_rejects_zero_overflowing_and_Tokio_out_of_range_scaling_before_graph_construction",
];

const RUST_NONCLAIMS: [&str; 6] = [
    "no_Go_unchecked_zero_or_native_width_overflow_acceptance_claim",
    "no_Go_map_or_goroutine_runtime_order_claim",
    "no_whole_crawler_lossless_shutdown_claim",
    "no_live_DNS_network_database_limiter_logger_metrics_or_clock_execution",
    "no_Rust_application_factory_deployment_readiness_or_restart_policy_claim",
    "no_normalized_Go_AST_reproduction_in_Rust",
];

const NORMALIZED_AST_SHA256: [(&str, &str); 16] = [
    (
        "batching.NewBatchingChannel",
        "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
    ),
    (
        "blocking.New",
        "b0b08a55e7683980c3140ff4d8a9d41b0a70926e999fb22d83f5c5103e27362f",
    ),
    (
        "buffered.NewBufferedConcurrentChannel",
        "562428750b1aaf7a4811758daa63468461d995ac00f36e4d7b620fedfb4633ec",
    ),
    (
        "buffered.Run",
        "0a8f90020ab24fb50cad498fcf376777cde3b5f6aa6424da3e66b15b54e3292f",
    ),
    (
        "config.NewDefaultConfig",
        "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
    ),
    (
        "crawler.start",
        "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
    ),
    (
        "dhtfx.New",
        "d44ba2acd0373b50e1192229539d2337acfa4590d8668c3cf286d1a6dbae95c5",
    ),
    (
        "discovered.NewDiscoveredNodes",
        "8fcfcd3864cc5e815edbc40e3dd96393bddeb97ccf7c8eaa7fb30c7ad6382a17",
    ),
    (
        "factory.New",
        "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
    ),
    (
        "metainforequester.New",
        "4ef3e1d45eadf204d63dd85f83e06f7a7bd869a68ba6b5f0d451352f5f9c17f5",
    ),
    (
        "persist.persistScrapedTorrentSources",
        "e3b5338f2bd11789760caa263f1880535165ce58649bce0ef364941c04454097",
    ),
    (
        "persist.runPersistSources",
        "07ad92a09673d00523cc463c4c6b3cf6f31881c3ed279e0d77e3ce2c0659dc6a",
    ),
    (
        "persist.runPersistTorrents",
        "fb761e3ec7c805218cc826f978352c8ebf831ab35b329ef5d868b9d8d12be199",
    ),
    (
        "protocol.RandomNodeIDWithClientSuffix",
        "e764bf47d979f53df9e4222427d87323853f691ec4cf0f6f44a8e2f374446709",
    ),
    (
        "protocol.RandomPeerID",
        "5f222ba96ce39c6b6ba2173080815de49ab7044eaa52d980e3d2dd7fe92e25a0",
    ),
    (
        "requestLimiter.Request",
        "37beb86d12ef9cf110268f0efd99921a637befafbc32305a6a65e84e44ff54ae",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 17] = [
    (
        "internal/blocking/factory.go",
        include_bytes!("../../../../internal/blocking/factory.go"),
        "4761db2241277fdec317a15e53fb6f3d74ba79306fd9a1c04f3ef77d9fcbf8d9",
    ),
    (
        "internal/concurrency/batching_channel.go",
        include_bytes!("../../../../internal/concurrency/batching_channel.go"),
        "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
    ),
    (
        "internal/concurrency/buffered_concurrent_channel.go",
        include_bytes!("../../../../internal/concurrency/buffered_concurrent_channel.go"),
        "4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c",
    ),
    (
        "internal/dhtcrawler/config.go",
        include_bytes!("../../../../internal/dhtcrawler/config.go"),
        "b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef",
    ),
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
        "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
    ),
    (
        "internal/dhtcrawler/discovered_nodes.go",
        include_bytes!("../../../../internal/dhtcrawler/discovered_nodes.go"),
        "22806cabf39173df71010a54d874a4319458f1715308834be828dbdb99767027",
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/infohash_triage.go",
        include_bytes!("../../../../internal/dhtcrawler/infohash_triage.go"),
        "7950da30f12ec9d54ba830c7465a749d4625ad0fd7e0aa2bebbdc4cef2027f02",
    ),
    (
        "internal/dhtcrawler/persist.go",
        include_bytes!("../../../../internal/dhtcrawler/persist.go"),
        "8a76c32f1aeaad074ce22c53193a012ae447a34a3c07e9c2123e9e7e27ac2022",
    ),
    (
        "internal/dhtcrawler/request_meta_info.go",
        include_bytes!("../../../../internal/dhtcrawler/request_meta_info.go"),
        "d20fc943aee947055dd5521235a55fd19c5fdd41a4203b8c09523b443c6ea0a6",
    ),
    (
        "internal/protocol/dht/dhtfx/module.go",
        include_bytes!("../../../../internal/protocol/dht/dhtfx/module.go"),
        "4928ce635bd44b855e83d3dc9ff2c23d66d2d8d45f64d5d8faf09215216d86f7",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
    (
        "internal/protocol/metainfo/metainforequester/config.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainforequester/config.go"),
        "5b8367dafd3953e88496be9c40a3d4b534e2d472a69fa1fbe56b2aff5bc11454",
    ),
    (
        "internal/protocol/metainfo/metainforequester/factory.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainforequester/factory.go"),
        "c16eac60c6c84b21b70602613641d0a6b08354a1a6412c1e3409831db613571a",
    ),
    (
        "internal/protocol/metainfo/metainforequester/limiter.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainforequester/limiter.go"),
        "d8ff732554e8a3a3260b1e3c8765871bce8361513b9f5ac314dca72c55379f1b",
    ),
    (
        "internal/protocol/metainfo/metainforequester/logger.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainforequester/logger.go"),
        "73b8822b87c932b10fbb4c68f26a93b9e4901a4e088e97c61a5b341e058b4cba",
    ),
    (
        "internal/protocol/metainfo/metainforequester/prometheus_collector.go",
        include_bytes!(
            "../../../../internal/protocol/metainfo/metainforequester/prometheus_collector.go"
        ),
        "a6e34d9e7f1a1ad45ca32575ea513f98c56db596000a91436e5ca65980204e23",
    ),
];

const PREREQUISITES: [(&str, &[u8], &str); 9] = [
    (
        "testdata/parity/dht/dht_crawler_get_peers.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_get_peers.jsonl"),
        "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844",
    ),
    (
        "testdata/parity/dht/dht_crawler_info_hash_triage.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_info_hash_triage.jsonl"),
        "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
    ),
    (
        "testdata/parity/dht/dht_crawler_persist_sources.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_persist_sources.jsonl"),
        "01acacdc5ccc425bda88e87643328101499af3873f3a52c7eef2f46a92697bd9",
    ),
    (
        "testdata/parity/dht/dht_crawler_persist_torrents.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_persist_torrents.jsonl"),
        "40adced4a96a860354d8ba74c412566e2a72979261bd674994c4ef18d6680bc5",
    ),
    (
        "testdata/parity/dht/dht_crawler_request_meta_info.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_request_meta_info.jsonl"),
        "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5",
    ),
    (
        "testdata/parity/dht/dht_crawler_scrape.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_scrape.jsonl"),
        "d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b",
    ),
    (
        "testdata/parity/dht/dht_info_hash_block_filter.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_info_hash_block_filter.jsonl"),
        "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4",
    ),
    (
        "testdata/parity/dht/keyed_limiter.jsonl",
        include_bytes!("../../../../testdata/parity/dht/keyed_limiter.jsonl"),
        "53787bb82f1b4c51519a4e412848ead5d9e03a316bc8403a928004f2446bfac8",
    ),
    (
        "testdata/parity/dht/metainfo_requester.jsonl",
        include_bytes!("../../../../testdata/parity/dht/metainfo_requester.jsonl"),
        "990f4d503065ed08689df37881817386874f12cda2fdaeaeb56c05e12bbcc80e",
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrictStringMap(BTreeMap<String, String>);

impl StrictStringMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'de> Deserialize<'de> for StrictStringMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictStringMapVisitor;

        impl<'de> Visitor<'de> for StrictStringMapVisitor {
            type Value = StrictStringMap;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string map without duplicate keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, String>()? {
                    if values.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate map key {key:?}"
                        )));
                    }
                }
                Ok(StrictStringMap(values))
            }
        }

        deserializer.deserialize_map(StrictStringMapVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    execution: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    actual_functions_executed: Vec<String>,
    source_pinned_functions: Vec<String>,
    production_functions_executed: bool,
    network_executed: bool,
    database_executed: bool,
    goroutines_started: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    config: Config,
    scaling: Scaling,
    routes: Vec<Route>,
    crawler_launches: Vec<Launch>,
    lifecycle: Lifecycle,
    requester: Requester,
    blocking: Blocking,
    persistence: Persistence,
    normalized_ast_sha256: StrictStringMap,
    source_sha256: StrictStringMap,
    prerequisite_fixture_sha256: StrictStringMap,
    nonclaims: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    scaling_factor: u64,
    default_bootstrap_reseed_seconds: i64,
    factory_bootstrap_reseed_seconds: i64,
    factory_uses_configured_bootstrap_reseed: bool,
    oldest_node_scan_seconds: i64,
    old_peer_threshold_seconds: i64,
    save_files_threshold: u64,
    save_pieces: bool,
    rescrape_threshold_seconds: i64,
    sought_node_rotation_seconds: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Scaling {
    arithmetic: String,
    formulas: Vec<ScalingFormula>,
    vectors: Vec<ScalingVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScalingFormula {
    name: String,
    capacity_expression: String,
    concurrency_expression: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScalingVector {
    scaling_factor: u64,
    discovery_capacity: i64,
    ping_capacity: i64,
    ping_concurrency: i64,
    find_node_capacity: i64,
    find_node_concurrency: i64,
    sample_infohashes_capacity: i64,
    sample_infohashes_concurrency: i64,
    info_hash_triage_capacity: i64,
    get_peers_capacity: i64,
    get_peers_concurrency: i64,
    scrape_capacity: i64,
    scrape_concurrency: i64,
    request_meta_info_capacity: i64,
    request_meta_info_concurrency: i64,
}

const SCALING_VECTORS: [ScalingVector; 6] = [
    ScalingVector {
        scaling_factor: 0,
        discovery_capacity: 0,
        ping_capacity: 0,
        ping_concurrency: 0,
        find_node_capacity: 0,
        find_node_concurrency: 0,
        sample_infohashes_capacity: 0,
        sample_infohashes_concurrency: 0,
        info_hash_triage_capacity: 0,
        get_peers_capacity: 0,
        get_peers_concurrency: 0,
        scrape_capacity: 0,
        scrape_concurrency: 0,
        request_meta_info_capacity: 0,
        request_meta_info_concurrency: 0,
    },
    ScalingVector {
        scaling_factor: 2,
        discovery_capacity: 200,
        ping_capacity: 2,
        ping_concurrency: 2,
        find_node_capacity: 20,
        find_node_concurrency: 20,
        sample_infohashes_capacity: 20,
        sample_infohashes_concurrency: 20,
        info_hash_triage_capacity: 20,
        get_peers_capacity: 20,
        get_peers_concurrency: 40,
        scrape_capacity: 20,
        scrape_concurrency: 40,
        request_meta_info_capacity: 20,
        request_meta_info_concurrency: 80,
    },
    ScalingVector {
        scaling_factor: 10,
        discovery_capacity: 1_000,
        ping_capacity: 10,
        ping_concurrency: 10,
        find_node_capacity: 100,
        find_node_concurrency: 100,
        sample_infohashes_capacity: 100,
        sample_infohashes_concurrency: 100,
        info_hash_triage_capacity: 100,
        get_peers_capacity: 100,
        get_peers_concurrency: 200,
        scrape_capacity: 100,
        scrape_concurrency: 200,
        request_meta_info_capacity: 100,
        request_meta_info_concurrency: 400,
    },
    ScalingVector {
        scaling_factor: 92_233_720_368_547_758,
        discovery_capacity: 9_223_372_036_854_775_800,
        ping_capacity: 92_233_720_368_547_758,
        ping_concurrency: 92_233_720_368_547_758,
        find_node_capacity: 922_337_203_685_477_580,
        find_node_concurrency: 922_337_203_685_477_580,
        sample_infohashes_capacity: 922_337_203_685_477_580,
        sample_infohashes_concurrency: 922_337_203_685_477_580,
        info_hash_triage_capacity: 922_337_203_685_477_580,
        get_peers_capacity: 922_337_203_685_477_580,
        get_peers_concurrency: 1_844_674_407_370_955_160,
        scrape_capacity: 922_337_203_685_477_580,
        scrape_concurrency: 1_844_674_407_370_955_160,
        request_meta_info_capacity: 922_337_203_685_477_580,
        request_meta_info_concurrency: 3_689_348_814_741_910_320,
    },
    ScalingVector {
        scaling_factor: 92_233_720_368_547_759,
        discovery_capacity: -9_223_372_036_854_775_716,
        ping_capacity: 92_233_720_368_547_759,
        ping_concurrency: 92_233_720_368_547_759,
        find_node_capacity: 922_337_203_685_477_590,
        find_node_concurrency: 922_337_203_685_477_590,
        sample_infohashes_capacity: 922_337_203_685_477_590,
        sample_infohashes_concurrency: 922_337_203_685_477_590,
        info_hash_triage_capacity: 922_337_203_685_477_590,
        get_peers_capacity: 922_337_203_685_477_590,
        get_peers_concurrency: 1_844_674_407_370_955_180,
        scrape_capacity: 922_337_203_685_477_590,
        scrape_concurrency: 1_844_674_407_370_955_180,
        request_meta_info_capacity: 922_337_203_685_477_590,
        request_meta_info_concurrency: 3_689_348_814_741_910_360,
    },
    ScalingVector {
        scaling_factor: u64::MAX,
        discovery_capacity: -100,
        ping_capacity: -1,
        ping_concurrency: -1,
        find_node_capacity: -10,
        find_node_concurrency: -10,
        sample_infohashes_capacity: -10,
        sample_infohashes_concurrency: -10,
        info_hash_triage_capacity: -10,
        get_peers_capacity: -10,
        get_peers_concurrency: -20,
        scrape_capacity: -10,
        scrape_concurrency: -20,
        request_meta_info_capacity: -10,
        request_meta_info_concurrency: -40,
    },
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Route {
    name: String,
    capacity: u64,
    batch_size: u64,
    batch_interval_milliseconds: i64,
    concurrency: u64,
    implementation: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Launch {
    source_order: u64,
    function: String,
    detached: bool,
    joined: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Lifecycle {
    start_uses_background_context: bool,
    cancel_deferred_until_start_returns: bool,
    start_waits_only_for_stopped: bool,
    on_stop_sets_active_false: bool,
    on_stop_closes_stopped: bool,
    on_stop_joins_crawler_children: bool,
    on_stop_closes_pipeline_inputs: bool,
    batchers_start_detached_at_construction: bool,
    batcher_output_capacity: u64,
    concurrent_callbacks_start_detached: bool,
    concurrent_callbacks_joined: bool,
    launch_order_evidence: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Requester {
    peer_id_generated_once_per_factory: bool,
    peer_id_generator: String,
    peer_id_client_prefix: String,
    peer_id_separate_from_dht_node_id: bool,
    request_timeout_seconds: i64,
    connect_timeout_seconds: i64,
    wrapper_call_order: Vec<String>,
    limiter_key: String,
    limiter_token_interval_milliseconds: i64,
    limiter_burst: u64,
    limiter_key_capacity: u64,
    limiter_ttl_seconds: i64,
    configured_key_mutex_size_used_by_factory: bool,
    logger_sample_tick_seconds: i64,
    logger_sample_initial: u64,
    logger_sample_thereafter: u64,
    limiter_failures_reach_logger_or_metrics: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Blocking {
    shared_by_triage_and_request_meta_info: bool,
    triage_operation: String,
    banned_operation: String,
    banned_flush_argument: bool,
    crawler_on_stop_calls_flush: bool,
    factory_provides_flush_hook: bool,
    hook_calls_if_initialized: bool,
    hook_returns_manager_flush_result: bool,
    pool_wait_released_after_flush: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Persistence {
    torrent_transaction_stages: Vec<String>,
    torrent_chunk_sizes: Vec<u64>,
    torrent_single_transaction: bool,
    torrent_scrape_fanout_after_success: bool,
    torrent_scrape_fanout_after_error: bool,
    torrent_scrape_fanout_order: String,
    source_chunk_size: u64,
    source_whole_batch_transaction: bool,
    source_prior_chunk_commit_possible: bool,
    source_retry: bool,
    persisted_metric_after_success_only: bool,
}

fn strings_match(actual: &[String], expected: &[&str]) -> bool {
    actual
        .iter()
        .map(String::as_str)
        .eq(expected.iter().copied())
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_digest_entries<'a>(
    label: &str,
    actual: &StrictStringMap,
    expected: impl IntoIterator<Item = (&'a str, &'a str)>,
    expected_len: usize,
) -> Result<(), String> {
    if actual.len() != expected_len {
        return Err(format!(
            "{label} entry count = {}, want {expected_len}",
            actual.len()
        ));
    }
    if actual.keys().any(str::is_empty) {
        return Err(format!("{label} contains an empty key"));
    }
    for (key, expected_digest) in expected {
        if !valid_sha256(expected_digest) {
            return Err(format!("pinned {label}[{key:?}] is not a SHA-256"));
        }
        let actual_digest = actual
            .get(key)
            .ok_or_else(|| format!("{label} is missing {key:?}"))?;
        if !valid_sha256(actual_digest) {
            return Err(format!("{label}[{key:?}] is not a lowercase SHA-256"));
        }
        if actual_digest != expected_digest {
            return Err(format!("{label}[{key:?}] changed"));
        }
    }
    Ok(())
}

macro_rules! require_contract {
    ($condition:expr, $label:literal) => {
        if !$condition {
            return Err(concat!("frozen composition contract changed: ", $label).to_owned());
        }
    };
}

#[allow(clippy::too_many_lines)]
fn validate_frozen_row(row: &Fixture) -> Result<(), String> {
    require_contract!(row.id == FIXTURE_ID, "id");
    require_contract!(row.subsystem == "dht_crawler_composition", "subsystem");
    require_contract!(row.classification == "SOURCE_ONLY", "classification");
    require_contract!(row.execution == "SOURCE_INSPECTION", "execution");
    require_contract!(
        row.oracle.composition
            == "production_cross_stage_construction_and_lifecycle_source_contract",
        "oracle.composition"
    );
    require_contract!(
        row.oracle.determinism
            == "typed_semantic_inventory_normalized_AST_and_exact_source_and_prerequisite_SHA256",
        "oracle.determinism"
    );
    require_contract!(
        row.oracle.actual_functions_executed.is_empty(),
        "oracle.actualFunctionsExecuted"
    );
    require_contract!(
        strings_match(&row.oracle.source_pinned_functions, &SOURCE_FUNCTIONS),
        "oracle.sourcePinnedFunctions"
    );
    require_contract!(
        !row.oracle.production_functions_executed
            && !row.oracle.network_executed
            && !row.oracle.database_executed
            && !row.oracle.goroutines_started,
        "oracle execution flags"
    );
    require_contract!(row.input.kind == "source_contract", "input.kind");

    let config = &row.expected.config;
    require_contract!(
        (
            config.scaling_factor,
            config.default_bootstrap_reseed_seconds,
            config.factory_bootstrap_reseed_seconds,
            config.factory_uses_configured_bootstrap_reseed,
            config.oldest_node_scan_seconds,
            config.old_peer_threshold_seconds,
            config.save_files_threshold,
            config.save_pieces,
            config.rescrape_threshold_seconds,
            config.sought_node_rotation_seconds,
        ) == (10, 60, 600, false, 10, 900, 100, false, 2_592_000, 10),
        "config"
    );

    let scaling = &row.expected.scaling;
    require_contract!(
        scaling.arithmetic == "64_bit_source_expression_evaluation_without_channel_allocation",
        "scaling.arithmetic"
    );
    require_contract!(
        scaling.formulas.len() == SCALING_FORMULAS.len(),
        "scaling formula count"
    );
    for (formula, expected) in scaling.formulas.iter().zip(SCALING_FORMULAS) {
        require_contract!(
            formula.name == expected.0
                && formula.capacity_expression == expected.1
                && formula.concurrency_expression == expected.2,
            "ordered scaling formula"
        );
    }
    require_contract!(
        scaling.vectors == SCALING_VECTORS,
        "ordered scaling vectors"
    );

    require_contract!(row.expected.routes.len() == ROUTES.len(), "route count");
    for (route, expected) in row.expected.routes.iter().zip(ROUTES) {
        require_contract!(
            route.name == expected.name
                && route.capacity == expected.capacity
                && route.batch_size == expected.batch_size
                && route.batch_interval_milliseconds == expected.batch_interval_milliseconds
                && route.concurrency == expected.concurrency
                && route.implementation == expected.implementation,
            "ordered route"
        );
    }

    require_contract!(
        row.expected.crawler_launches.len() == LAUNCH_FUNCTIONS.len(),
        "launch count"
    );
    for (index, (launch, function)) in row
        .expected
        .crawler_launches
        .iter()
        .zip(LAUNCH_FUNCTIONS)
        .enumerate()
    {
        require_contract!(
            launch.source_order == u64::try_from(index).expect("15 launches fit u64")
                && launch.function == function
                && launch.detached
                && !launch.joined,
            "ordered crawler launch"
        );
    }

    let lifecycle = &row.expected.lifecycle;
    require_contract!(
        lifecycle.start_uses_background_context
            && lifecycle.cancel_deferred_until_start_returns
            && lifecycle.start_waits_only_for_stopped
            && lifecycle.on_stop_sets_active_false
            && lifecycle.on_stop_closes_stopped
            && !lifecycle.on_stop_joins_crawler_children
            && !lifecycle.on_stop_closes_pipeline_inputs
            && lifecycle.batchers_start_detached_at_construction
            && lifecycle.batcher_output_capacity == 1
            && lifecycle.concurrent_callbacks_start_detached
            && !lifecycle.concurrent_callbacks_joined
            && lifecycle.launch_order_evidence
                == "source_lexical_go_statement_order_only_not_runtime_goroutine_scheduling_order",
        "lifecycle"
    );

    let requester = &row.expected.requester;
    require_contract!(
        requester.peer_id_generated_once_per_factory
            && requester.peer_id_generator == "protocol.RandomPeerID"
            && requester.peer_id_client_prefix == "-BM0001-"
            && requester.peer_id_separate_from_dht_node_id
            && requester.request_timeout_seconds == 6
            && requester.connect_timeout_seconds == 3
            && strings_match(
                &requester.wrapper_call_order,
                &[
                    "requestLimiter",
                    "requestLogger",
                    "prometheusCollector",
                    "requester",
                ],
            )
            && requester.limiter_key == "remote_IP_without_port"
            && requester.limiter_token_interval_milliseconds == 500
            && requester.limiter_burst == 4
            && requester.limiter_key_capacity == 1_000
            && requester.limiter_ttl_seconds == 20
            && !requester.configured_key_mutex_size_used_by_factory
            && requester.logger_sample_tick_seconds == 60
            && requester.logger_sample_initial == 10
            && requester.logger_sample_thereafter == 0
            && !requester.limiter_failures_reach_logger_or_metrics,
        "requester"
    );

    let blocking = &row.expected.blocking;
    require_contract!(
        blocking.shared_by_triage_and_request_meta_info
            && blocking.triage_operation == "Filter"
            && blocking.banned_operation == "Block"
            && !blocking.banned_flush_argument
            && !blocking.crawler_on_stop_calls_flush
            && blocking.factory_provides_flush_hook
            && blocking.hook_calls_if_initialized
            && blocking.hook_returns_manager_flush_result
            && blocking.pool_wait_released_after_flush,
        "blocking"
    );

    let persistence = &row.expected.persistence;
    require_contract!(
        strings_match(
            &persistence.torrent_transaction_stages,
            &[
                "torrents",
                "torrent_files",
                "torrent_file_summary",
                "torrents_torrent_sources",
                "torrent_pieces_if_enabled",
                "queue_jobs",
            ],
        ) && persistence.torrent_chunk_sizes == [100, 100, 100, 100, 10, 10]
            && persistence.torrent_single_transaction
            && persistence.torrent_scrape_fanout_after_success
            && !persistence.torrent_scrape_fanout_after_error
            && persistence.torrent_scrape_fanout_order == "Go_map_iteration_unspecified"
            && persistence.source_chunk_size == 100
            && !persistence.source_whole_batch_transaction
            && persistence.source_prior_chunk_commit_possible
            && !persistence.source_retry
            && persistence.persisted_metric_after_success_only,
        "persistence"
    );

    require_contract!(
        strings_match(&row.expected.nonclaims, &GO_NONCLAIMS),
        "nonclaims"
    );
    validate_digest_entries(
        "normalizedAstSha256",
        &row.expected.normalized_ast_sha256,
        NORMALIZED_AST_SHA256.iter().copied(),
        NORMALIZED_AST_SHA256.len(),
    )?;
    validate_digest_entries(
        "sourceSha256",
        &row.expected.source_sha256,
        GO_SOURCES.iter().map(|(path, _, digest)| (*path, *digest)),
        GO_SOURCES.len(),
    )?;
    validate_digest_entries(
        "prerequisiteFixtureSha256",
        &row.expected.prerequisite_fixture_sha256,
        PREREQUISITES
            .iter()
            .map(|(path, _, digest)| (*path, *digest)),
        PREREQUISITES.len(),
    )?;
    Ok(())
}

fn decode_strict_jsonl(bytes: &[u8]) -> Result<Fixture, String> {
    if bytes.is_empty() || bytes.last() != Some(&b'\n') {
        return Err("composition fixture must be nonempty and end with LF".to_owned());
    }
    if bytes.contains(&b'\r') {
        return Err("composition fixture must be LF-only".to_owned());
    }
    let body = &bytes[..bytes.len() - 1];
    if body.is_empty() || body.contains(&b'\n') {
        return Err("composition fixture must contain exactly one nonempty row".to_owned());
    }
    let line = std::str::from_utf8(body).map_err(|error| error.to_string())?;

    // Typed-first parsing makes duplicate known struct fields and duplicate
    // digest-map keys errors before a generic JSON value can collapse them.
    let row: Fixture = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let raw: Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    if raw.as_object().map(serde_json::Map::len) != Some(7) {
        return Err("composition fixture top-level object shape changed".to_owned());
    }
    validate_frozen_row(&row)?;
    Ok(row)
}

fn fixture() -> Fixture {
    assert_eq!(FIXTURE_BYTES.len(), FIXTURE_BYTE_LENGTH);
    assert_eq!(
        format!("{:x}", Sha256::digest(FIXTURE_BYTES)),
        FIXTURE_SHA256
    );
    decode_strict_jsonl(FIXTURE_BYTES).expect("frozen composition fixture must decode")
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, FromArgMatches};
    use serde_json::{Map, Number};

    use super::*;

    fn fixture_value() -> Value {
        serde_json::from_slice(&FIXTURE_BYTES[..FIXTURE_BYTES.len() - 1]).unwrap()
    }

    fn encode_line(value: &Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn object_mut<'a>(value: &'a mut Value, pointer: &str) -> &'a mut Map<String, Value> {
        value
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing mutation pointer {pointer}"))
            .as_object_mut()
            .unwrap_or_else(|| panic!("mutation pointer {pointer} is not an object"))
    }

    fn assert_mutation_rejected(mutate: impl FnOnce(&mut Value)) {
        let mut value = fixture_value();
        mutate(&mut value);
        assert!(decode_strict_jsonl(&encode_line(&value)).is_err());
    }

    fn replace_once(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let offset = input
            .windows(from.len())
            .position(|window| window == from)
            .expect("mutation source exists");
        let mut output = Vec::with_capacity(input.len() - from.len() + to.len());
        output.extend_from_slice(&input[..offset]);
        output.extend_from_slice(to);
        output.extend_from_slice(&input[offset + from.len()..]);
        output
    }

    fn app_config(scaling_factor: u64) -> crate::DhtCrawlerAppConfig {
        let command = crate::DhtCrawlerAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None));
        let matches = command
            .try_get_matches_from([
                "bitmagnet-dht-crawler".to_owned(),
                "--expected-goose-version".to_owned(),
                "29".to_owned(),
                "--classifier-queue".to_owned(),
                "shadow".to_owned(),
                "--dht-crawler-scaling-factor".to_owned(),
                scaling_factor.to_string(),
            ])
            .expect("parse scaling fixture input without ambient environment");
        crate::DhtCrawlerAppConfig::from_arg_matches(&matches)
            .expect("build typed scaling fixture input")
    }

    #[test]
    fn frozen_envelope_execution_partition_and_every_semantic_fact_are_exact() {
        let row = fixture();

        assert_eq!(
            FIXTURE_BYTES.iter().filter(|byte| **byte == b'\n').count(),
            1
        );
        assert_eq!(row.id, FIXTURE_ID);
        assert_eq!(row.oracle.source_pinned_functions.len(), 16);
        assert_eq!(row.expected.scaling.formulas.len(), 8);
        assert_eq!(row.expected.scaling.vectors.len(), 6);
        assert_eq!(row.expected.routes.len(), 10);
        assert_eq!(row.expected.crawler_launches.len(), 15);
        assert!(row.oracle.actual_functions_executed.is_empty());
        assert!(!row.oracle.production_functions_executed);
        assert!(!row.oracle.network_executed);
        assert!(!row.oracle.database_executed);
        assert!(!row.oracle.goroutines_started);
        assert_eq!(
            RUST_EXECUTION_PARTITION,
            [(
                FIXTURE_ID,
                "GO_SOURCE_INSPECTION_ONLY_NO_RUST_COMPOSITION_RUNTIME_REPLAY",
            )]
        );
        assert_eq!(
            row.expected.lifecycle.launch_order_evidence,
            "source_lexical_go_statement_order_only_not_runtime_goroutine_scheduling_order"
        );
        assert_eq!(row.expected.nonclaims.len(), 6);
        assert!(row.expected.nonclaims.iter().any(|claim| claim
            == "no_Rust_supervisor_application_deployment_or_production_readiness_claim"));
    }

    #[test]
    fn source_and_prerequisite_bytes_recompute_exact_pinned_digests() {
        let row = fixture();
        assert_eq!(row.expected.normalized_ast_sha256.len(), 16);
        assert_eq!(row.expected.source_sha256.len(), 17);
        assert_eq!(row.expected.prerequisite_fixture_sha256.len(), 9);

        for (path, bytes, expected) in GO_SOURCES {
            let actual = format!("{:x}", Sha256::digest(bytes));
            assert_eq!(actual, expected, "source digest changed for {path}");
            assert_eq!(row.expected.source_sha256.get(path), Some(expected));
        }
        for (path, bytes, expected) in PREREQUISITES {
            let actual = format!("{:x}", Sha256::digest(bytes));
            assert_eq!(actual, expected, "prerequisite digest changed for {path}");
            assert_eq!(
                row.expected.prerequisite_fixture_sha256.get(path),
                Some(expected)
            );
        }
        for (key, expected) in NORMALIZED_AST_SHA256 {
            assert_eq!(row.expected.normalized_ast_sha256.get(key), Some(expected));
        }
    }

    #[test]
    fn safe_go_scaling_vectors_replay_on_the_atomic_rust_projection() {
        let row = fixture();
        for vector in row
            .expected
            .scaling
            .vectors
            .iter()
            .filter(|vector| matches!(vector.scaling_factor, 2 | 10))
        {
            let projection = app_config(vector.scaling_factor)
                .projection()
                .expect("safe Go vector projects into the Rust graph");
            assert_eq!(
                i64::try_from(projection.runtime.discovery_capacity.get()).unwrap(),
                vector.discovery_capacity
            );
            assert_eq!(
                i64::try_from(projection.maintenance.ping_capacity.get()).unwrap(),
                vector.ping_capacity
            );
            assert_eq!(vector.ping_capacity, vector.ping_concurrency);
            assert_eq!(
                i64::try_from(projection.maintenance.find_node_capacity.get()).unwrap(),
                vector.find_node_capacity
            );
            assert_eq!(vector.find_node_capacity, vector.find_node_concurrency);
            assert_eq!(
                i64::try_from(projection.maintenance.sample_infohashes_capacity.get()).unwrap(),
                vector.sample_infohashes_capacity
            );
            assert_eq!(
                vector.sample_infohashes_capacity,
                vector.sample_infohashes_concurrency
            );
            assert_eq!(
                i64::try_from(projection.downstream.root_triage_capacity.get()).unwrap(),
                vector.info_hash_triage_capacity
            );
            assert_eq!(
                i64::try_from(projection.downstream.get_peers_lane.route_capacity.get()).unwrap(),
                vector.get_peers_capacity
            );
            assert_eq!(
                i64::try_from(
                    projection
                        .downstream
                        .get_peers_lane
                        .worker_max_inflight
                        .get()
                )
                .unwrap(),
                vector.get_peers_concurrency
            );
            assert_eq!(
                i64::try_from(projection.downstream.scrape_lane.route_capacity.get()).unwrap(),
                vector.scrape_capacity
            );
            assert_eq!(
                i64::try_from(projection.downstream.scrape_lane.worker_max_inflight.get()).unwrap(),
                vector.scrape_concurrency
            );
            assert_eq!(
                i64::try_from(
                    projection
                        .downstream
                        .request_meta_info_lane
                        .route_capacity
                        .get()
                )
                .unwrap(),
                vector.request_meta_info_capacity
            );
            assert_eq!(
                i64::try_from(
                    projection
                        .downstream
                        .request_meta_info_lane
                        .worker_max_inflight
                        .get()
                )
                .unwrap(),
                vector.request_meta_info_concurrency
            );
            assert_eq!(
                projection.downstream.persist_torrent,
                crate::DhtPersistTorrentWorkerConfig {
                    classifier_queue: crate::DhtCrawlerClassifierQueue::Shadow,
                    ..crate::DhtPersistTorrentWorkerConfig::default()
                },
                "ScalingFactor must not change persistence policy"
            );
        }

        let zero = row
            .expected
            .scaling
            .vectors
            .first()
            .expect("Go zero vector is present");
        assert_eq!(zero.scaling_factor, 0);
        assert_eq!(zero.discovery_capacity, 0);
        assert_eq!(
            app_config(0).projection().unwrap_err().into_kind(),
            crate::DhtCrawlerAppConfigErrorKind::ScalingFactorZero
        );
    }

    #[test]
    fn rust_owned_hardenings_and_nonclaims_do_not_become_go_runtime_parity() {
        fn assert_send<T: Send>() {}
        assert_send::<crate::DhtCrawlerPipelineSupervisor>();

        assert_eq!(RUST_OWNED_HARDENINGS, [
            "Rust_injected_stable_peer_ID_is_not_Go_random_factory_parity",
            "Rust_request_limiter_uses_lazy_expiry_without_the_Go_cleanup_task_and_omits_logger_and_Prometheus_wrappers",
            "Rust_joined_staged_drain_and_blocking_finalization_are_hardening_not_Go_OnStop_parity",
            "Rust_source_writer_whole_transaction_contract_differs_from_Go_possible_prior_chunk_commits",
            "Rust_rejects_zero_overflowing_and_Tokio_out_of_range_scaling_before_graph_construction",
        ]);
        assert_eq!(
            RUST_NONCLAIMS,
            [
                "no_Go_unchecked_zero_or_native_width_overflow_acceptance_claim",
                "no_Go_map_or_goroutine_runtime_order_claim",
                "no_whole_crawler_lossless_shutdown_claim",
                "no_live_DNS_network_database_limiter_logger_metrics_or_clock_execution",
                "no_Rust_application_factory_deployment_readiness_or_restart_policy_claim",
                "no_normalized_Go_AST_reproduction_in_Rust",
            ]
        );
    }

    #[test]
    fn recursive_schema_and_frozen_semantics_reject_adversarial_mutations() {
        for pointer in [
            "",
            "/oracle",
            "/input",
            "/expected",
            "/expected/config",
            "/expected/scaling",
            "/expected/scaling/formulas/0",
            "/expected/scaling/vectors/0",
            "/expected/routes/0",
            "/expected/crawlerLaunches/0",
            "/expected/lifecycle",
            "/expected/requester",
            "/expected/blocking",
            "/expected/persistence",
        ] {
            assert_mutation_rejected(|value| {
                object_mut(value, pointer).insert("unknown".to_owned(), Value::Bool(true));
            });
        }

        assert_mutation_rejected(|value| {
            object_mut(value, "").remove("id");
        });
        assert_mutation_rejected(|value| {
            object_mut(value, "/expected/config").remove("scalingFactor");
        });
        assert_mutation_rejected(|value| {
            object_mut(value, "/expected/scaling").remove("vectors");
        });
        assert_mutation_rejected(|value| value["oracle"] = Value::Null);
        assert_mutation_rejected(|value| value["expected"]["routes"][0]["name"] = Value::Null);
        assert_mutation_rejected(|value| {
            value["expected"]["routes"][0]["capacity"] = Value::Number(Number::from(-1));
        });
        assert_mutation_rejected(|value| {
            value["expected"]["routes"][0]["capacity"] =
                Value::Number(Number::from_f64(1.5).unwrap());
        });
        assert_mutation_rejected(|value| {
            value["expected"]["routes"]
                .as_array_mut()
                .unwrap()
                .swap(0, 1);
        });
        assert_mutation_rejected(|value| {
            value["expected"]["routes"].as_array_mut().unwrap().pop();
        });
        assert_mutation_rejected(|value| {
            value["expected"]["crawlerLaunches"]
                .as_array_mut()
                .unwrap()
                .swap(0, 1);
        });
        assert_mutation_rejected(|value| {
            value["expected"]["crawlerLaunches"]
                .as_array_mut()
                .unwrap()
                .pop();
        });
        assert_mutation_rejected(|value| {
            object_mut(value, "/expected/sourceSha256").remove("internal/blocking/factory.go");
        });
        assert_mutation_rejected(|value| {
            object_mut(value, "/expected/sourceSha256")
                .insert("foreign.go".to_owned(), Value::String("0".repeat(64)));
        });
        assert_mutation_rejected(|value| {
            value["expected"]["normalizedAstSha256"]["factory.New"] =
                Value::String("ABC".to_owned());
        });
        assert_mutation_rejected(|value| {
            object_mut(value, "/expected/prerequisiteFixtureSha256")
                .remove("testdata/parity/dht/metainfo_requester.jsonl");
        });

        let duplicate_top = [b"{\"id\":\"duplicate\",".as_slice(), &FIXTURE_BYTES[1..]].concat();
        assert!(decode_strict_jsonl(&duplicate_top).is_err());
        let duplicate_struct = replace_once(
            FIXTURE_BYTES,
            b"\"config\":{",
            b"\"config\":{},\"config\":{",
        );
        assert!(decode_strict_jsonl(&duplicate_struct).is_err());
        let digest = NORMALIZED_AST_SHA256[0].1;
        let original = format!("\"batching.NewBatchingChannel\":\"{digest}\"");
        let duplicate = format!("{original},{original}");
        let duplicate_map = replace_once(FIXTURE_BYTES, original.as_bytes(), duplicate.as_bytes());
        assert!(decode_strict_jsonl(&duplicate_map).is_err());

        let mut extra_row = FIXTURE_BYTES.to_vec();
        extra_row.extend_from_slice(FIXTURE_BYTES);
        assert!(decode_strict_jsonl(&extra_row).is_err());
        let mut trailing_json = FIXTURE_BYTES[..FIXTURE_BYTES.len() - 1].to_vec();
        trailing_json.extend_from_slice(b" {}\n");
        assert!(decode_strict_jsonl(&trailing_json).is_err());
        assert!(decode_strict_jsonl(&FIXTURE_BYTES[..FIXTURE_BYTES.len() - 1]).is_err());
        let crlf = FIXTURE_BYTES
            .iter()
            .flat_map(|byte| {
                if *byte == b'\n' {
                    b"\r\n".as_slice()
                } else {
                    std::slice::from_ref(byte)
                }
            })
            .copied()
            .collect::<Vec<_>>();
        assert!(decode_strict_jsonl(&crlf).is_err());

        let out_of_range = replace_once(
            FIXTURE_BYTES,
            b"\"defaultBootstrapReseedSeconds\":60",
            b"\"defaultBootstrapReseedSeconds\":9223372036854775808",
        );
        assert!(decode_strict_jsonl(&out_of_range).is_err());
    }
}
