use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use bitmagnet_dht::DhtRuntimeConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    schema_version: u32,
    subsystem: String,
    evidence: Evidence,
    defaults: Defaults,
    identity: Identity,
    lifecycle: Lifecycle,
    limitations: Limitations,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Evidence {
    generator: String,
    mode: String,
    production_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Defaults {
    config_namespace: String,
    bind_ip: String,
    bind_port: u16,
    bind_addr_port: String,
    query_timeout_nanos: u64,
    responder_timeout_nanos: u64,
    sample_info_hashes_interval_seconds: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Identity {
    provider: String,
    total_bytes: usize,
    random_prefix_bytes: usize,
    suffix_offset_bytes: usize,
    suffix_ascii: String,
    suffix_hex: String,
    samples_checked: usize,
    all_samples_match_suffix_shape: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Lifecycle {
    construction_is_lazy: bool,
    start_trigger: String,
    stop_before_initialization_is_no_op: bool,
    socket_open_before_goroutines: bool,
    stop_mechanism: String,
    stop_is_idempotent: bool,
    second_stop_panics: bool,
    shutdown_worker_detached: bool,
    read_loop_detached: bool,
    query_handlers_detached: bool,
    response_handlers_detached: bool,
    stop_waits_for_read_loop: bool,
    stop_waits_for_handlers: bool,
    socket_close_error_ignored: bool,
    active_receive_error_policy: String,
    pending_queries: PendingQueries,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PendingQueries {
    registry_initially_empty: bool,
    response_channel_capacity: usize,
    cleanup_only_when_query_returns: bool,
    stop_touches_registry: bool,
    stop_closes_response_channels: bool,
    query_select_inputs: Vec<String>,
    stop_signal_selected_by_query: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Limitations {
    socket_opened: bool,
    network_used: bool,
    goroutines_started: bool,
    timing_observed: bool,
    lifecycle_evidence_class: String,
    detached_completion_order: String,
    pending_at_stop_count: String,
}

#[test]
fn real_go_lifecycle_fixture_locks_defaults_identity_and_known_deltas() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../../../fixtures/dht_runtime_lifecycle.json"))
            .expect("checked Go DHT runtime lifecycle fixture");

    assert_eq!(fixture.schema_version, 1);
    assert_eq!(fixture.subsystem, "dht_runtime_lifecycle");
    assert_eq!(
        fixture.evidence.generator,
        "tools/parity/dht_runtime_lifecycle"
    );
    assert_eq!(
        fixture.evidence.mode,
        "production public calls plus Go AST structural validation"
    );
    assert_eq!(
        fixture.evidence.production_files,
        [
            "internal/lazy/lazy.go",
            "internal/protocol/dht/dhtfx/module.go",
            "internal/protocol/dht/responder/factory.go",
            "internal/protocol/dht/server/config.go",
            "internal/protocol/dht/server/factory.go",
            "internal/protocol/dht/server/server.go",
            "internal/protocol/id.go",
        ]
    );

    let config = DhtRuntimeConfig::default();
    assert_eq!(fixture.defaults.config_namespace, "dht_server");
    assert_eq!(fixture.defaults.bind_ip, Ipv4Addr::UNSPECIFIED.to_string());
    assert_eq!(fixture.defaults.bind_port, config.bind_addr.port());
    assert_eq!(
        fixture.defaults.bind_addr_port,
        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 3334).to_string()
    );
    assert_eq!(
        config.bind_addr,
        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 3334)
    );
    assert_eq!(
        Duration::from_nanos(fixture.defaults.query_timeout_nanos),
        config.query_timeout
    );
    assert_eq!(config.query_timeout, Duration::from_secs(4));
    assert_eq!(fixture.defaults.responder_timeout_nanos, 5_000_000_000);
    assert_eq!(
        fixture.defaults.sample_info_hashes_interval_seconds,
        config.sample_infohashes_interval
    );

    assert_eq!(
        fixture.identity.provider,
        "protocol.RandomNodeIDWithClientSuffix"
    );
    assert_eq!(fixture.identity.total_bytes, 20);
    assert_eq!(fixture.identity.random_prefix_bytes, 12);
    assert_eq!(fixture.identity.suffix_offset_bytes, 12);
    assert_eq!(fixture.identity.suffix_ascii, "-BM0001-");
    assert_eq!(fixture.identity.suffix_hex, "2d424d303030312d");
    assert_eq!(fixture.identity.samples_checked, 64);
    assert!(fixture.identity.all_samples_match_suffix_shape);

    let lifecycle = fixture.lifecycle;
    assert!(lifecycle.construction_is_lazy);
    assert_eq!(
        lifecycle.start_trigger,
        "first lazy.Get attempt; initialization result is cached"
    );
    assert!(lifecycle.stop_before_initialization_is_no_op);
    assert!(lifecycle.socket_open_before_goroutines);
    assert_eq!(lifecycle.stop_mechanism, "unguarded close(stopped)");
    assert!(!lifecycle.stop_is_idempotent);
    assert!(lifecycle.second_stop_panics);
    assert!(lifecycle.shutdown_worker_detached);
    assert!(lifecycle.read_loop_detached);
    assert!(lifecycle.query_handlers_detached);
    assert!(lifecycle.response_handlers_detached);
    assert!(!lifecycle.stop_waits_for_read_loop);
    assert!(!lifecycle.stop_waits_for_handlers);
    assert!(lifecycle.socket_close_error_ignored);
    assert_eq!(
        lifecycle.active_receive_error_policy,
        "panic in detached read goroutine while context is active"
    );

    let pending = lifecycle.pending_queries;
    assert!(pending.registry_initially_empty);
    assert_eq!(pending.response_channel_capacity, 1);
    assert!(pending.cleanup_only_when_query_returns);
    assert!(!pending.stop_touches_registry);
    assert!(!pending.stop_closes_response_channels);
    assert_eq!(
        pending.query_select_inputs,
        ["query_context", "response_channel"]
    );
    assert!(!pending.stop_signal_selected_by_query);

    let limitations = fixture.limitations;
    assert!(!limitations.socket_opened);
    assert!(!limitations.network_used);
    assert!(!limitations.goroutines_started);
    assert!(!limitations.timing_observed);
    assert_eq!(
        limitations.lifecycle_evidence_class,
        "source-derived AST invariants; not behaviorally executed"
    );
    assert_eq!(limitations.detached_completion_order, "not asserted");
    assert_eq!(
        limitations.pending_at_stop_count,
        "not observed; only absence of stop-side registry draining is asserted"
    );
}
