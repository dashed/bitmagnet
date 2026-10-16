use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::*;

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_sought_node_id.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_sought_node_id.jsonl");
const FIXTURE_SHA256: &str = "683162fe0da0c9fe8f39b80fffaaa3aae4f98683a0c1579b521eeb69f9aa1ea4";
const FIXTURE_IDS: [&str; 5] = [
    "production_shared_target_source_contract",
    "zero_value_get_returns_zero_id",
    "set_then_get_returns_exact_id",
    "aliases_observe_replacement_a_to_b",
    "controlled_cross_goroutine_whole_value_handoff",
];

const GO_SOURCES: [(&str, &[u8], &str); 6] = [
    (
        "internal/concurrency/atomic.go",
        include_bytes!("../../../../internal/concurrency/atomic.go"),
        "09cc4842dbdf516f8574f26b411130daba526f69dbf217e1f2867e829f781a4f",
    ),
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
        "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/find_node.go",
        include_bytes!("../../../../internal/dhtcrawler/find_node.go"),
        "cd5fab8aa078ad40ed82331dbbfd141a38badc018287dd13211d221b230087bb",
    ),
    (
        "internal/dhtcrawler/sample_infohashes.go",
        include_bytes!("../../../../internal/dhtcrawler/sample_infohashes.go"),
        "483b9037673dce82f9026f2aec9448812f804c13484fd0bd2f55fcfc70a52983",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
];

const DELIBERATE_RUST_HARDENING_DELTAS: [&str; 7] = [
    "explicit_initial_value_constructor_replaces_implicit_zero_value_storage",
    "complete_twenty_byte_entropy_is_required",
    "typed_entropy_failure_discards_partial_bytes_constructor_publishes_nothing_and_rotation_preserves_the_last_published_target",
    "shutdown_is_biased_ahead_of_a_simultaneously_ready_timer",
    "the_rotator_future_is_owned_taskless_and_starts_no_detached_work",
    "poisoned_target_locks_are_recovered",
    "replacement_is_module_private_and_owned_by_one_non_clone_rotator",
];

const RUST_HARDENING_EVIDENCE: [(&str, &str); 7] = [
    (
        DELIBERATE_RUST_HARDENING_DELTAS[0],
        "DhtCrawlerTarget::new; zero_is_an_accepted_explicit_target; injected_zero_initial_id_is_accepted_by_the_pair_constructor",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[1],
        "TARGET_BYTES; injected_raw_initial_id_is_published_before_run",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[2],
        "DhtCrawlerTargetError::Entropy; constructor_entropy_failure_returns_no_pair; rotation_entropy_failure_preserves_last_published_target; later_rotation_entropy_failure_preserves_the_last_successful_replacement",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[3],
        "ready_shutdown_wins_without_generation; tied_shutdown_wins_without_generation",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[4],
        "DhtCrawlerTargetRotator::run; dropping_a_polled_run_drops_its_delay_without_detaching_work",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[5],
        "poisoned_writer_does_not_prevent_reads_or_replacement",
    ),
    (
        DELIBERATE_RUST_HARDENING_DELTAS[6],
        "private DhtCrawlerTarget::set; non-Clone compile-fail doctest; rotator_is_send",
    ),
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    storage: String,
    consumers: String,
    clock: String,
    random: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    kind: String,
    actors: Vec<String>,
    writes: Vec<Write>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Write {
    actor: String,
    target: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Expected {
    reads: Vec<Read>,
    events: Vec<String>,
    final_target: String,
    get_count: usize,
    source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Read {
    actor: String,
    after: String,
    target: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    atomic_zero_value_is_zero_id: bool,
    atomic_get_uses_read_lock: bool,
    atomic_get_returns_whole_value_copy: bool,
    atomic_set_uses_exclusive_lock: bool,
    target_storage_is_shared_pointer: bool,
    target_initialized_before_crawler_start: bool,
    initial_target_nonzero_guaranteed: bool,
    find_reads_target_at_each_client_call: bool,
    sample_reads_same_target_at_each_client_call: bool,
    rotation_started_as_detached_goroutine: bool,
    rotation_joined_before_start_returns: bool,
    rotation_context_cancelled_after_stop: bool,
    rotation_delay_seconds: u64,
    rotation_uses_fresh_time_after_each_loop: bool,
    rotation_has_no_immediate_replacement: bool,
    rotation_next_delay_starts_after_set: bool,
    rotation_timer_backlog_possible: bool,
    rotation_cancel_timer_tie_outcome: String,
    rotation_random_and_set_cancellation_aware: bool,
    random_byte_length: usize,
    random_source: String,
    random_read_result_checked: bool,
    random_applies_client_suffix: bool,
    random_failure_preserves_previous_target: bool,
    random_failure_outcome: String,
    atomic_runtime_observed: bool,
    clock_runtime_observed: bool,
    random_runtime_observed: bool,
    clock_and_random_evidence_scope: String,
    source_sha256: BTreeMap<String, String>,
    evidence: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    reads: Vec<Read>,
    events: Vec<String>,
    final_target: String,
    get_count: usize,
}

fn fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("fixture line {} is invalid: {error}", index + 1))
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn fixture_id(value: &str) -> Id20 {
    Id20::from_hex(value).unwrap_or_else(|error| panic!("invalid fixture target {value}: {error}"))
}

#[test]
fn fixture_schema_identity_sources_and_hardening_deltas_are_frozen() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    for (index, fixture) in fixtures.iter().enumerate() {
        assert_eq!(fixture.subsystem, "dht_crawler_sought_node_id");
        assert_oracle(fixture, index == 0);
    }
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.expected.get_count)
            .collect::<Vec<_>>(),
        [0, 1, 1, 3, 3]
    );
    assert_source_fixture(&fixtures[0]);
    for fixture in &fixtures[1..] {
        assert!(fixture.expected.source.is_none());
    }

    assert_eq!(
        DELIBERATE_RUST_HARDENING_DELTAS,
        [
            "explicit_initial_value_constructor_replaces_implicit_zero_value_storage",
            "complete_twenty_byte_entropy_is_required",
            "typed_entropy_failure_discards_partial_bytes_constructor_publishes_nothing_and_rotation_preserves_the_last_published_target",
            "shutdown_is_biased_ahead_of_a_simultaneously_ready_timer",
            "the_rotator_future_is_owned_taskless_and_starts_no_detached_work",
            "poisoned_target_locks_are_recovered",
            "replacement_is_module_private_and_owned_by_one_non_clone_rotator",
        ]
    );
    assert_eq!(
        RUST_HARDENING_EVIDENCE,
        [
            (
                DELIBERATE_RUST_HARDENING_DELTAS[0],
                "DhtCrawlerTarget::new; zero_is_an_accepted_explicit_target; injected_zero_initial_id_is_accepted_by_the_pair_constructor",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[1],
                "TARGET_BYTES; injected_raw_initial_id_is_published_before_run",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[2],
                "DhtCrawlerTargetError::Entropy; constructor_entropy_failure_returns_no_pair; rotation_entropy_failure_preserves_last_published_target; later_rotation_entropy_failure_preserves_the_last_successful_replacement",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[3],
                "ready_shutdown_wins_without_generation; tied_shutdown_wins_without_generation",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[4],
                "DhtCrawlerTargetRotator::run; dropping_a_polled_run_drops_its_delay_without_detaching_work",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[5],
                "poisoned_writer_does_not_prevent_reads_or_replacement",
            ),
            (
                DELIBERATE_RUST_HARDENING_DELTAS[6],
                "private DhtCrawlerTarget::set; non-Clone compile-fail doctest; rotator_is_send",
            ),
        ]
    );

    assert_eq!(TARGET_BYTES, 20);
    assert_eq!(ROTATION_DELAY, Duration::from_secs(10));
    assert_eq!(std::mem::size_of::<Id20>(), TARGET_BYTES);
    let _explicit_constructor: fn(Id20) -> DhtCrawlerTarget = DhtCrawlerTarget::new;
    let _pair_constructor: fn() -> Result<
        (DhtCrawlerTarget, DhtCrawlerTargetRotator),
        DhtCrawlerTargetError,
    > = DhtCrawlerTargetRotator::new;
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send::<DhtCrawlerTargetRotator>();
    assert_send_sync::<DhtCrawlerTarget>();
    fn classify_error(error: &DhtCrawlerTargetError) -> &'static str {
        match error {
            DhtCrawlerTargetError::Entropy(_) => "entropy",
        }
    }
    let _typed_error_classifier: fn(&DhtCrawlerTargetError) -> &'static str = classify_error;
}

fn assert_oracle(fixture: &Fixture, source: bool) {
    let oracle = &fixture.oracle;
    if source {
        assert_eq!(
            oracle.composition,
            "production_source_and_atomic_runtime_freshness_gate"
        );
        assert_eq!(
            oracle.determinism,
            "exact_go_ast_and_source_sha256_clock_and_random_source_only"
        );
        assert_eq!(
            oracle.storage,
            "production_concurrency_AtomicValue_protocol_ID"
        );
        assert_eq!(
            oracle.consumers,
            "production_find_node_and_sample_infohashes_call_sites"
        );
        assert_eq!(
            oracle.clock,
            "source_only_time_After_no_wall_clock_execution"
        );
        assert_eq!(
            oracle.random,
            "source_only_crypto_rand_no_entropy_execution"
        );
    } else {
        assert_eq!(
            oracle.composition,
            "actual_concurrency_AtomicValue_protocol_ID"
        );
        assert_eq!(
            oracle.determinism,
            "synchronous_or_channel_gated_operations"
        );
        assert_eq!(
            oracle.storage,
            "production_concurrency_AtomicValue_protocol_ID"
        );
        assert_eq!(oracle.consumers, "controlled_test_actors");
        assert_eq!(oracle.clock, "not_invoked");
        assert_eq!(oracle.random, "not_invoked");
    }
}

fn assert_source_fixture(fixture: &Fixture) {
    assert_eq!(fixture.input.kind, "source_contract");
    assert!(fixture.input.actors.is_empty());
    assert!(fixture.input.writes.is_empty());
    assert!(fixture.expected.reads.is_empty());
    assert!(fixture.expected.events.is_empty());
    assert!(fixture.expected.final_target.is_empty());
    assert_eq!(fixture.expected.get_count, 0);
    let source = fixture.expected.source.as_ref().unwrap();

    assert!(source.atomic_zero_value_is_zero_id);
    assert!(source.atomic_get_uses_read_lock);
    assert!(source.atomic_get_returns_whole_value_copy);
    assert!(source.atomic_set_uses_exclusive_lock);
    assert!(source.target_storage_is_shared_pointer);
    assert!(source.target_initialized_before_crawler_start);
    assert!(!source.initial_target_nonzero_guaranteed);
    assert!(source.find_reads_target_at_each_client_call);
    assert!(source.sample_reads_same_target_at_each_client_call);
    assert!(source.rotation_started_as_detached_goroutine);
    assert!(!source.rotation_joined_before_start_returns);
    assert!(source.rotation_context_cancelled_after_stop);
    assert_eq!(source.rotation_delay_seconds, 10);
    assert!(source.rotation_uses_fresh_time_after_each_loop);
    assert!(source.rotation_has_no_immediate_replacement);
    assert!(source.rotation_next_delay_starts_after_set);
    assert!(!source.rotation_timer_backlog_possible);
    assert_eq!(
        source.rotation_cancel_timer_tie_outcome,
        "go_select_unspecified_ready_case_selection"
    );
    assert!(!source.rotation_random_and_set_cancellation_aware);
    assert_eq!(source.random_byte_length, 20);
    assert_eq!(source.random_source, "crypto/rand.Read");
    assert!(!source.random_read_result_checked);
    assert!(!source.random_applies_client_suffix);
    assert!(!source.random_failure_preserves_previous_target);
    assert_eq!(
        source.random_failure_outcome,
        "ignored_error_installs_new_id_with_any_written_prefix_and_zero_initialized_remainder"
    );
    assert!(source.atomic_runtime_observed);
    assert!(!source.clock_runtime_observed);
    assert!(!source.random_runtime_observed);
    assert_eq!(
        source.clock_and_random_evidence_scope,
        "exact_ast_and_source_digest_only_not_runtime_executed"
    );
    assert_eq!(
        source.evidence,
        "actual_AtomicValue_rows_plus_exact_Go_AST_and_source_freshness"
    );

    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, expected_digest) in GO_SOURCES {
        assert_eq!(
            sha256(bytes),
            expected_digest,
            "Go source drifted for {path}"
        );
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(expected_digest),
            "fixture source digest drifted for {path}"
        );
    }
    assert_eq!(source.random_byte_length, TARGET_BYTES);
    assert_eq!(
        Duration::from_secs(source.rotation_delay_seconds),
        ROTATION_DELAY
    );
}

#[test]
fn real_go_atomic_rows_replay_on_rust_target() {
    let fixtures = fixtures();
    assert_eq!(
        observe_zero(&fixtures[1]),
        expected_observation(&fixtures[1])
    );
    assert_eq!(
        observe_set_get(&fixtures[2]),
        expected_observation(&fixtures[2])
    );
    assert_eq!(
        observe_aliases(&fixtures[3]),
        expected_observation(&fixtures[3])
    );
    assert_eq!(
        observe_cross_thread(&fixtures[4]),
        expected_observation(&fixtures[4])
    );
}

fn expected_observation(fixture: &Fixture) -> Observation {
    assert!(fixture.expected.source.is_none());
    Observation {
        reads: fixture.expected.reads.clone(),
        events: fixture.expected.events.clone(),
        final_target: fixture.expected.final_target.clone(),
        get_count: fixture.expected.get_count,
    }
}

fn current_counted(target: &DhtCrawlerTarget, get_count: &AtomicUsize) -> Id20 {
    let current = target.current();
    get_count.fetch_add(1, Ordering::SeqCst);
    current
}

fn observe_zero(fixture: &Fixture) -> Observation {
    assert_eq!(fixture.input.kind, "zero_get");
    assert_eq!(fixture.input.actors, ["main"]);
    assert!(fixture.input.writes.is_empty());
    let target = DhtCrawlerTarget::new(Id20::ZERO);
    let get_count = AtomicUsize::new(0);
    let current = current_counted(&target, &get_count).to_string();
    Observation {
        reads: vec![Read {
            actor: "main".into(),
            after: "zero_value".into(),
            target: current.clone(),
        }],
        events: vec![format!("main_get:{current}")],
        final_target: current,
        get_count: get_count.load(Ordering::SeqCst),
    }
}

fn observe_set_get(fixture: &Fixture) -> Observation {
    assert_eq!(fixture.input.kind, "set_get");
    assert_eq!(fixture.input.actors, ["main"]);
    assert_eq!(fixture.input.writes.len(), 1);
    let write = &fixture.input.writes[0];
    assert_eq!(write.actor, "main");
    let next = fixture_id(&write.target);
    let target = DhtCrawlerTarget::new(Id20::ZERO);
    let get_count = AtomicUsize::new(0);
    target.set(next);
    let current = current_counted(&target, &get_count).to_string();
    Observation {
        reads: vec![Read {
            actor: "main".into(),
            after: "main_set_a".into(),
            target: current.clone(),
        }],
        events: vec![
            format!("main_set:{}", write.target),
            format!("main_get:{current}"),
        ],
        final_target: current,
        get_count: get_count.load(Ordering::SeqCst),
    }
}

fn observe_aliases(fixture: &Fixture) -> Observation {
    assert_eq!(fixture.input.kind, "shared_aliases");
    assert_eq!(fixture.input.actors, ["primary", "alias_one", "alias_two"]);
    assert_eq!(fixture.input.writes.len(), 2);
    let first = &fixture.input.writes[0];
    let second = &fixture.input.writes[1];
    assert_eq!(first.actor, "primary");
    assert_eq!(second.actor, "alias_two");

    let primary = DhtCrawlerTarget::new(Id20::ZERO);
    let alias_one = primary.clone();
    let alias_two = primary.clone();
    let get_count = AtomicUsize::new(0);
    primary.set(fixture_id(&first.target));
    let after_a = current_counted(&alias_one, &get_count).to_string();
    alias_two.set(fixture_id(&second.target));
    let primary_after_b = current_counted(&primary, &get_count).to_string();
    let alias_after_b = current_counted(&alias_one, &get_count).to_string();

    Observation {
        reads: vec![
            Read {
                actor: "alias_one".into(),
                after: "primary_set_a".into(),
                target: after_a.clone(),
            },
            Read {
                actor: "primary".into(),
                after: "alias_two_set_b".into(),
                target: primary_after_b.clone(),
            },
            Read {
                actor: "alias_one".into(),
                after: "alias_two_set_b".into(),
                target: alias_after_b.clone(),
            },
        ],
        events: vec![
            format!("primary_set:{}", first.target),
            format!("alias_one_get:{after_a}"),
            format!("alias_two_set:{}", second.target),
            format!("primary_get:{primary_after_b}"),
            format!("alias_one_get:{alias_after_b}"),
        ],
        final_target: primary_after_b,
        get_count: get_count.load(Ordering::SeqCst),
    }
}

fn observe_cross_thread(fixture: &Fixture) -> Observation {
    assert_eq!(fixture.input.kind, "controlled_cross_goroutine_handoff");
    assert_eq!(fixture.input.actors, ["writer", "reader"]);
    assert_eq!(fixture.input.writes.len(), 2);
    assert!(fixture
        .input
        .writes
        .iter()
        .all(|write| write.actor == "writer"));
    let first = fixture_id(&fixture.input.writes[0].target);
    let second = fixture_id(&fixture.input.writes[1].target);

    let target = DhtCrawlerTarget::new(Id20::ZERO);
    let writer_target = target.clone();
    let reader_target = target.clone();
    let get_count = Arc::new(AtomicUsize::new(0));
    let reader_get_count = Arc::clone(&get_count);
    let (write_tx, write_rx) = mpsc::sync_channel::<Id20>(0);
    let (written_tx, written_rx) = mpsc::sync_channel::<()>(0);
    let (read_tx, read_rx) = mpsc::sync_channel::<()>(0);
    let (read_value_tx, read_value_rx) = mpsc::sync_channel::<Id20>(0);
    let writer = thread::spawn(move || {
        while let Ok(value) = write_rx.recv() {
            writer_target.set(value);
            written_tx.send(()).unwrap();
        }
    });
    let reader = thread::spawn(move || {
        while read_rx.recv().is_ok() {
            read_value_tx
                .send(current_counted(&reader_target, &reader_get_count))
                .unwrap();
        }
    });

    let mut reads = Vec::with_capacity(2);
    let mut events = Vec::with_capacity(4);
    write_tx.send(first).unwrap();
    written_rx.recv().unwrap();
    events.push(format!("writer_set:{}", first));
    read_tx.send(()).unwrap();
    let first_read = read_value_rx.recv().unwrap().to_string();
    events.push(format!("reader_get:{first_read}"));
    reads.push(Read {
        actor: "reader".into(),
        after: "writer_set_a".into(),
        target: first_read,
    });

    write_tx.send(second).unwrap();
    written_rx.recv().unwrap();
    events.push(format!("writer_set:{}", second));
    read_tx.send(()).unwrap();
    let second_read = read_value_rx.recv().unwrap().to_string();
    events.push(format!("reader_get:{second_read}"));
    reads.push(Read {
        actor: "reader".into(),
        after: "writer_set_b".into(),
        target: second_read,
    });

    drop(write_tx);
    drop(read_tx);
    writer.join().unwrap();
    reader.join().unwrap();
    let final_target = current_counted(&target, &get_count).to_string();
    Observation {
        reads,
        events,
        final_target,
        get_count: get_count.load(Ordering::SeqCst),
    }
}
