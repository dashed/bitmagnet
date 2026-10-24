use std::path::Path;
use std::time::Duration;

use bitmagnet_dht::{DhtRuntimeHealthFailure, DhtRuntimeHealthSnapshot, DhtRuntimeHealthStatus};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DhtHealthFixture {
    id: String,
    subsystem: String,
    oracle: DhtHealthOracle,
    input: DhtHealthInput,
    expected: DhtHealthExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DhtHealthOracle {
    implementation: String,
    clock: String,
    last_response_is_evaluated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DhtHealthInput {
    active: bool,
    start_age_nanos: Option<u64>,
    last_response_age_nanos: Option<u64>,
    last_success_age_nanos: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DhtHealthExpected {
    classification: String,
    error_message: String,
}

#[test]
fn real_go_fixture_locks_exact_dht_health_boundaries() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/dht/health.jsonl");
    let contents = std::fs::read_to_string(&path).expect("read checked Go DHT health fixture");
    let fixtures = contents
        .lines()
        .enumerate()
        .map(|(index, line)| {
            assert!(!line.is_empty(), "blank fixture row at {}", index + 1);
            serde_json::from_str::<DhtHealthFixture>(line).unwrap_or_else(|error| {
                panic!("decode {} row {}: {error}", path.display(), index + 1)
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(fixtures.len(), 7);
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        [
            "inactive_ignores_stale_success",
            "zero_start_is_up",
            "initial_just_before_30_seconds",
            "initial_exactly_30_seconds",
            "successful_exactly_60_seconds",
            "successful_after_60_seconds",
            "last_response_ignored_without_success",
        ]
    );

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_health");
        assert_eq!(
            fixture.oracle.implementation,
            "production NewCheck activity and checkLastResponsesAt threshold policy"
        );
        assert_eq!(
            fixture.oracle.clock,
            "fixed UTC instant with exact age vectors; no sleeps"
        );
        assert!(!fixture.oracle.last_response_is_evaluated);

        let snapshot = DhtRuntimeHealthSnapshot {
            active: fixture.input.active,
            running_for: fixture.input.start_age_nanos.map(Duration::from_nanos),
            last_response_ago: fixture
                .input
                .last_response_age_nanos
                .map(Duration::from_nanos),
            last_success_ago: fixture
                .input
                .last_success_age_nanos
                .map(Duration::from_nanos),
        };
        let status = snapshot.status();
        let (classification, error_message) = match status {
            DhtRuntimeHealthStatus::Inactive => ("inactive", String::new()),
            DhtRuntimeHealthStatus::Up => ("up", String::new()),
            DhtRuntimeHealthStatus::Down(failure) => ("down", failure.to_string()),
        };
        assert_eq!(
            classification, fixture.expected.classification,
            "{}",
            fixture.id
        );
        assert_eq!(
            error_message, fixture.expected.error_message,
            "{}",
            fixture.id
        );
    }
}

#[test]
fn failure_messages_remain_go_compatible() {
    assert_eq!(
        DhtRuntimeHealthFailure::NoResponseWithinInitialGrace.to_string(),
        "no response within 30 seconds"
    );
    assert_eq!(
        DhtRuntimeHealthFailure::NoSuccessfulResponseWithinFreshness.to_string(),
        "no successful responses within last minute"
    );
}
