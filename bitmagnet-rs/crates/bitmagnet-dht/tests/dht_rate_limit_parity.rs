use std::error::Error;
use std::future::{poll_fn, ready, Future};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Barrier};
use std::task::Poll;
use std::thread;
use std::time::Duration;

use bitmagnet_dht::{
    DhtInboundRateLimitDenial, DhtInboundRateLimiter, DhtOutboundRateLimiter, DhtRateLimitWaitError,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};

const ADDR_A: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6_881));
const ADDR_A_OTHER_PORT: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6_882));
const ADDR_B: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 2), 6_881));
const ADDR_C: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 3), 6_881));
const ADDR_D: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 4), 6_881));

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RateLimiterFixture {
    id: String,
    subsystem: String,
    input: RateLimiterInput,
    expected: RateLimiterExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RateLimiterInput {
    limit_per_second: f64,
    burst: i64,
    tick_nanos: i64,
    anchor_unix_nano: i64,
    steps: Vec<RateLimiterStep>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RateLimiterStep {
    operation: String,
    at_tick: i64,
    count: i64,
    reservation_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RateLimiterExpected {
    events: Vec<RateLimiterEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RateLimiterEvent {
    operation: String,
    at_tick: i64,
    count: i64,
    reservation_id: String,
    allowed: bool,
    reservation_ok: bool,
    reservation_delay_nanos: i64,
    tokens_before_milli: i64,
    tokens_after_milli: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyedLimiterFixture {
    id: String,
    subsystem: String,
    input: KeyedLimiterInput,
    expected: KeyedLimiterExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyedLimiterInput {
    limit_per_second: f64,
    burst: i64,
    capacity: i64,
    ttl_nanos: i64,
    steps: Vec<KeyedLimiterStep>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyedLimiterStep {
    operation: String,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyedLimiterExpected {
    events: Vec<KeyedLimiterEvent>,
    ttl_clock_injection_available: bool,
    positive_ttl_boundary_fixture: bool,
    implementation_limit: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyedLimiterEvent {
    operation: String,
    key: String,
    allowed: bool,
    same_instance_as_previous_key: bool,
    keys_oldest_to_newest: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterFixture {
    id: String,
    subsystem: String,
    runtime: ResponderLimiterRuntime,
    production_defaults: ResponderLimiterDefaults,
    input: ResponderLimiterInput,
    expected: ResponderLimiterExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterRuntime {
    implementation: String,
    clock: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterDefaults {
    overall_every_nanos: i64,
    overall_burst: i64,
    per_ip_every_nanos: i64,
    per_ip_burst: i64,
    per_ip_capacity: i64,
    per_ip_ttl_nanos: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterInput {
    layer: String,
    addresses: Vec<String>,
    scripted_per_ip_allows: Vec<bool>,
    global_burst: i64,
    scripted_outer_allows: Vec<bool>,
    delegate_outcomes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterExpected {
    events: Vec<ResponderLimiterEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResponderLimiterEvent {
    call: i64,
    address: String,
    allowed: bool,
    per_ip_keys: Vec<String>,
    global_tokens_before: i64,
    global_tokens_after: i64,
    delegate_calls: i64,
    return_id_hex: String,
    error_code: i64,
    error_message: String,
    error_is_too_many_requests: bool,
    error_is_delegate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterFixture {
    id: String,
    subsystem: String,
    runtime: QueryLimiterRuntime,
    production_defaults: QueryLimiterDefaults,
    input: QueryLimiterInput,
    expected: QueryLimiterExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterRuntime {
    implementation: String,
    clock: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterDefaults {
    per_ip_every_nanos: i64,
    per_ip_burst: i64,
    per_ip_capacity: i64,
    per_ip_ttl_nanos: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterInput {
    limiter_kind: String,
    context_kind: String,
    addresses: Vec<String>,
    scripted_waits: Vec<String>,
    delegate: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterExpected {
    events: Vec<QueryLimiterEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryLimiterEvent {
    call: i64,
    address: String,
    wait_keys: Vec<String>,
    sequence: Vec<String>,
    delegate_calls: i64,
    delegate_before_wait_ended: i64,
    return_id_hex: String,
    error_message: String,
    error_is_wait_sentinel: bool,
    error_is_delegate_sentinel: bool,
    error_is_canceled: bool,
    error_is_deadline_exceeded: bool,
}

fn read_fixture<T: DeserializeOwned>(filename: &str) -> Vec<T> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht")
        .join(filename);
    let contents = std::fs::read_to_string(&path).expect("read checked Go fixture");
    contents
        .lines()
        .enumerate()
        .map(|(index, line)| {
            assert!(!line.is_empty(), "blank fixture row at {}", index + 1);
            serde_json::from_str(line).unwrap_or_else(|error| {
                panic!("decode {} row {}: {error}", path.display(), index + 1)
            })
        })
        .collect()
}

fn bools(values: &[bool]) -> String {
    values
        .iter()
        .map(bool::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn rate_step_projection(step: RateLimiterStep) -> String {
    let RateLimiterStep {
        operation,
        at_tick,
        count,
        reservation_id,
    } = step;
    format!("{operation}|{at_tick}|{count}|{reservation_id}")
}

fn rate_event_projection(event: RateLimiterEvent) -> String {
    let RateLimiterEvent {
        operation,
        at_tick,
        count,
        reservation_id,
        allowed,
        reservation_ok,
        reservation_delay_nanos,
        tokens_before_milli,
        tokens_after_milli,
    } = event;
    format!(
        "{operation}|{at_tick}|{count}|{reservation_id}|{allowed}|{reservation_ok}|\
         {reservation_delay_nanos}|{tokens_before_milli}|{tokens_after_milli}"
    )
}

#[test]
fn real_go_token_bucket_fixture_is_strict_exhaustive_primitive_evidence() {
    let fixtures = read_fixture::<RateLimiterFixture>("rate_limiter.jsonl");
    assert_eq!(fixtures.len(), 4);
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        [
            "allow_refill_exact",
            "reservation_cancel_before_action",
            "reservation_cancel_after_action_noop",
            "invalid_reservation_over_burst",
        ]
    );

    for fixture in fixtures {
        let RateLimiterFixture {
            id,
            subsystem,
            input,
            expected,
        } = fixture;
        assert_eq!(subsystem, "dht_rate_limiter");
        let RateLimiterInput {
            limit_per_second,
            burst,
            tick_nanos,
            anchor_unix_nano,
            steps,
        } = input;
        assert_eq!(limit_per_second, 10.0);
        assert_eq!(burst, 2);
        assert_eq!(tick_nanos, 100_000_000);
        assert_eq!(anchor_unix_nano, 1_700_000_000_123_000_000);
        let RateLimiterExpected { events } = expected;
        let actual_steps = steps
            .into_iter()
            .map(rate_step_projection)
            .collect::<Vec<_>>();
        let actual_events = events
            .into_iter()
            .map(rate_event_projection)
            .collect::<Vec<_>>();
        let (expected_steps, expected_events): (&[&str], &[&str]) = match id.as_str() {
            "allow_refill_exact" => (
                &[
                    "tokens|0|0|",
                    "allow|0|2|",
                    "allow|0|1|",
                    "tokens|1|0|",
                    "allow|1|1|",
                    "tokens|5|0|",
                    "allow|5|2|",
                ],
                &[
                    "tokens|0|0||false|false|0|2000|2000",
                    "allow|0|2||true|false|0|2000|0",
                    "allow|0|1||false|false|0|0|0",
                    "tokens|1|0||false|false|0|1000|1000",
                    "allow|1|1||true|false|0|1000|0",
                    "tokens|5|0||false|false|0|2000|2000",
                    "allow|5|2||true|false|0|2000|0",
                ],
            ),
            "reservation_cancel_before_action" => (
                &[
                    "reserve|0|2|immediate",
                    "reserve|0|2|future",
                    "cancel|1|0|future",
                    "reserve|1|2|after_cancel",
                ],
                &[
                    "reserve|0|2|immediate|false|true|0|2000|0",
                    "reserve|0|2|future|false|true|200000000|0|-2000",
                    "cancel|1|0|future|false|false|0|-1000|1000",
                    "reserve|1|2|after_cancel|false|true|100000000|1000|-1000",
                ],
            ),
            "reservation_cancel_after_action_noop" => (
                &[
                    "reserve|0|2|immediate",
                    "reserve|0|2|future",
                    "cancel|3|0|future",
                    "reserve|3|2|after_late_cancel",
                ],
                &[
                    "reserve|0|2|immediate|false|true|0|2000|0",
                    "reserve|0|2|future|false|true|200000000|0|-2000",
                    "cancel|3|0|future|false|false|0|1000|1000",
                    "reserve|3|2|after_late_cancel|false|true|100000000|1000|-1000",
                ],
            ),
            "invalid_reservation_over_burst" => (
                &["reserve|0|3|invalid", "cancel|0|0|invalid", "tokens|0|0|"],
                &[
                    "reserve|0|3|invalid|false|false|9223372036854775807|2000|2000",
                    "cancel|0|0|invalid|false|false|0|2000|2000",
                    "tokens|0|0||false|false|0|2000|2000",
                ],
            ),
            _ => panic!("unclassified token-bucket row {id}"),
        };
        assert_eq!(actual_steps, expected_steps, "{id} input steps");
        assert_eq!(actual_events, expected_events, "{id} expected events");
    }
}

fn keyed_step_projection(step: KeyedLimiterStep) -> String {
    let KeyedLimiterStep { operation, key } = step;
    format!("{operation}|{key}")
}

fn keyed_event_projection(event: KeyedLimiterEvent) -> String {
    let KeyedLimiterEvent {
        operation,
        key,
        allowed,
        same_instance_as_previous_key,
        keys_oldest_to_newest,
    } = event;
    format!(
        "{operation}|{key}|{allowed}|{same_instance_as_previous_key}|{}",
        keys_oldest_to_newest.join(",")
    )
}

#[test]
fn real_go_keyed_limiter_fixture_is_strict_exhaustive_primitive_evidence() {
    let fixtures = read_fixture::<KeyedLimiterFixture>("keyed_limiter.jsonl");
    assert_eq!(fixtures.len(), 5);
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        [
            "initial_burst_is_independent_per_key",
            "exact_string_keys_remain_distinct",
            "get_refreshes_lru_recency_and_capacity_evicts_oldest",
            "zero_ttl_disables_expiry",
            "positive_ttl_wall_clock_limit",
        ]
    );

    for fixture in fixtures {
        let KeyedLimiterFixture {
            id,
            subsystem,
            input,
            expected,
        } = fixture;
        assert_eq!(subsystem, "dht_keyed_limiter");
        let KeyedLimiterInput {
            limit_per_second,
            burst,
            capacity,
            ttl_nanos,
            steps,
        } = input;
        assert_eq!(limit_per_second, 0.0);
        let KeyedLimiterExpected {
            events,
            ttl_clock_injection_available,
            positive_ttl_boundary_fixture,
            implementation_limit,
        } = expected;
        assert!(!ttl_clock_injection_available);
        assert!(!positive_ttl_boundary_fixture);
        let actual_steps = steps
            .into_iter()
            .map(keyed_step_projection)
            .collect::<Vec<_>>();
        let actual_events = events
            .into_iter()
            .map(keyed_event_projection)
            .collect::<Vec<_>>();

        let (expected_shape, expected_steps, expected_events, expected_limit):
            ((i64, i64, i64), &[&str], &[&str], &str) = match id.as_str() {
                "initial_burst_is_independent_per_key" => (
                    (2, 4, 0),
                    &[
                        "allow|alpha",
                        "allow|alpha",
                        "allow|alpha",
                        "allow|beta",
                        "allow|beta",
                        "allow|beta",
                    ],
                    &[
                        "allow|alpha|true|false|alpha",
                        "allow|alpha|true|false|alpha",
                        "allow|alpha|false|false|alpha",
                        "allow|beta|true|false|alpha,beta",
                        "allow|beta|true|false|alpha,beta",
                        "allow|beta|false|false|alpha,beta",
                    ],
                    "",
                ),
                "exact_string_keys_remain_distinct" => (
                    (1, 8, 0),
                    &[
                        "allow|192.0.2.1",
                        "allow|::ffff:192.0.2.1",
                        "allow|fe80::1%7",
                        "allow|fe80::1%8",
                        "allow|192.0.2.1",
                        "allow|::ffff:192.0.2.1",
                    ],
                    &[
                        "allow|192.0.2.1|true|false|192.0.2.1",
                        "allow|::ffff:192.0.2.1|true|false|192.0.2.1,::ffff:192.0.2.1",
                        "allow|fe80::1%7|true|false|192.0.2.1,::ffff:192.0.2.1,fe80::1%7",
                        "allow|fe80::1%8|true|false|192.0.2.1,::ffff:192.0.2.1,fe80::1%7,fe80::1%8",
                        "allow|192.0.2.1|false|false|::ffff:192.0.2.1,fe80::1%7,fe80::1%8,192.0.2.1",
                        "allow|::ffff:192.0.2.1|false|false|fe80::1%7,fe80::1%8,192.0.2.1,::ffff:192.0.2.1",
                    ],
                    "",
                ),
                "get_refreshes_lru_recency_and_capacity_evicts_oldest" => (
                    (1, 2, 0),
                    &["get|alpha", "get|beta", "get|alpha", "get|gamma", "get|beta"],
                    &[
                        "get|alpha|false|false|alpha",
                        "get|beta|false|false|alpha,beta",
                        "get|alpha|false|true|beta,alpha",
                        "get|gamma|false|false|alpha,gamma",
                        "get|beta|false|false|gamma,beta",
                    ],
                    "",
                ),
                "zero_ttl_disables_expiry" => (
                    (1, 2, 0),
                    &["get|stable", "get|stable"],
                    &[
                        "get|stable|false|false|stable",
                        "get|stable|false|true|stable",
                    ],
                    "",
                ),
                "positive_ttl_wall_clock_limit" => (
                    (1, 2, 20_000_000_000),
                    &["get|wall-clock", "get|wall-clock"],
                    &[
                        "get|wall-clock|false|false|wall-clock",
                        "get|wall-clock|false|true|wall-clock",
                    ],
                    "positive TTL expiry and reset boundaries use time.Now plus a non-injectable \
                     background reaper; only pre-expiry identity is deterministic without sleeps \
                     or production changes",
                ),
                _ => panic!("unclassified keyed-limiter row {id}"),
            };
        assert_eq!((burst, capacity, ttl_nanos), expected_shape, "{id} policy");
        assert_eq!(actual_steps, expected_steps, "{id} input steps");
        assert_eq!(actual_events, expected_events, "{id} expected events");
        assert_eq!(implementation_limit, expected_limit, "{id} limitation");
    }
}

fn responder_event_projection(event: ResponderLimiterEvent) -> String {
    let ResponderLimiterEvent {
        call,
        address,
        allowed,
        per_ip_keys,
        global_tokens_before,
        global_tokens_after,
        delegate_calls,
        return_id_hex,
        error_code,
        error_message,
        error_is_too_many_requests,
        error_is_delegate,
    } = event;
    format!(
        "{call}|{address}|{allowed}|{}|{global_tokens_before}|{global_tokens_after}|\
         {delegate_calls}|{return_id_hex}|{error_code}|{error_message}|\
         {error_is_too_many_requests}|{error_is_delegate}",
        per_ip_keys.join(",")
    )
}

#[test]
fn real_go_responder_limiter_fixture_locks_wrappers_ordering_keys_and_defaults() {
    let fixtures = read_fixture::<ResponderLimiterFixture>("responder_limiter.jsonl");
    assert_eq!(fixtures.len(), 3);
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        [
            "inner_per_ip_denial_precedes_global",
            "inner_exact_ip_string_keys",
            "outer_denial_and_delegate_effects",
        ]
    );

    for fixture in fixtures {
        let ResponderLimiterFixture {
            id,
            subsystem,
            runtime,
            production_defaults,
            input,
            expected,
        } = fixture;
        assert_eq!(subsystem, "dht_responder_limiter");
        let ResponderLimiterRuntime {
            implementation,
            clock,
        } = runtime;
        assert_eq!(implementation, "production responderLimiter and limiter");
        assert_eq!(
            clock,
            "rate.Limit(0) makes global token observations independent of wall time"
        );
        let ResponderLimiterDefaults {
            overall_every_nanos,
            overall_burst,
            per_ip_every_nanos,
            per_ip_burst,
            per_ip_capacity,
            per_ip_ttl_nanos,
        } = production_defaults;
        assert_eq!(
            (
                overall_every_nanos,
                overall_burst,
                per_ip_every_nanos,
                per_ip_burst,
                per_ip_capacity,
                per_ip_ttl_nanos,
            ),
            (20_000_000, 20, 1_000_000_000, 10, 1_000, 20_000_000_000)
        );
        let ResponderLimiterInput {
            layer,
            addresses,
            scripted_per_ip_allows,
            global_burst,
            scripted_outer_allows,
            delegate_outcomes,
        } = input;
        let input_projection = format!(
            "{layer}|{}|{}|{global_burst}|{}|{}",
            addresses.join(","),
            bools(&scripted_per_ip_allows),
            bools(&scripted_outer_allows),
            delegate_outcomes.join(",")
        );
        let ResponderLimiterExpected { events } = expected;
        let actual_events = events
            .into_iter()
            .map(responder_event_projection)
            .collect::<Vec<_>>();
        let (expected_input, expected_events): (&str, &[&str]) = match id.as_str() {
            "inner_per_ip_denial_precedes_global" => (
                "inner|192.0.2.1,192.0.2.1,192.0.2.1|false,true,true|1||",
                &[
                    "1|192.0.2.1|false|192.0.2.1|1|1|0||0||false|false",
                    "2|192.0.2.1|true|192.0.2.1,192.0.2.1|1|0|0||0||false|false",
                    "3|192.0.2.1|false|192.0.2.1,192.0.2.1,192.0.2.1|0|0|0||0||false|false",
                ],
            ),
            "inner_exact_ip_string_keys" => (
                "inner|192.0.2.1,::ffff:192.0.2.1,fe80::1%7,fe80::1%8|\
                 true,true,true,true|4||",
                &[
                    "1|192.0.2.1|true|192.0.2.1|4|3|0||0||false|false",
                    "2|::ffff:192.0.2.1|true|192.0.2.1,::ffff:192.0.2.1|3|2|0||0||false|false",
                    "3|fe80::1%7|true|192.0.2.1,::ffff:192.0.2.1,fe80::1%7|2|1|0||0||false|false",
                    "4|fe80::1%8|true|192.0.2.1,::ffff:192.0.2.1,fe80::1%7,fe80::1%8|1|0|0||0||false|false",
                ],
            ),
            "outer_denial_and_delegate_effects" => (
                "outer|192.0.2.9,192.0.2.9,192.0.2.9||0|false,true,true|success,error",
                &[
                    "1|192.0.2.9|false||0|0|0|0000000000000000000000000000000000000000|201|too many requests|true|false",
                    "2|192.0.2.9|true||0|0|1|aabbcc0000000000000000000000000000000000|0||false|false",
                    "3|192.0.2.9|true||0|0|2|0000000000000000000000000000000000000000|0|fixed delegate error|false|true",
                ],
            ),
            _ => panic!("unclassified responder-limiter row {id}"),
        };
        assert_eq!(input_projection, expected_input, "{id} input");
        assert_eq!(actual_events, expected_events, "{id} expected events");
    }
}

fn query_event_projection(event: QueryLimiterEvent) -> String {
    let QueryLimiterEvent {
        call,
        address,
        wait_keys,
        sequence,
        delegate_calls,
        delegate_before_wait_ended,
        return_id_hex,
        error_message,
        error_is_wait_sentinel,
        error_is_delegate_sentinel,
        error_is_canceled,
        error_is_deadline_exceeded,
    } = event;
    format!(
        "{call}|{address}|{}|{}|{delegate_calls}|{delegate_before_wait_ended}|\
         {return_id_hex}|{error_message}|{error_is_wait_sentinel}|\
         {error_is_delegate_sentinel}|{error_is_canceled}|{error_is_deadline_exceeded}",
        wait_keys.join(","),
        sequence.join(",")
    )
}

#[test]
fn real_go_query_limiter_fixture_locks_wait_boundary_errors_keys_and_defaults() {
    let fixtures = read_fixture::<QueryLimiterFixture>("query_limiter.jsonl");
    assert_eq!(fixtures.len(), 7);
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        [
            "wait_barrier_precedes_delegate",
            "wait_error_short_circuits_exact",
            "delegate_error_identity_after_wait",
            "pre_canceled_actual_keyed_limiter",
            "expired_deadline_actual_keyed_limiter",
            "future_deadline_rejected_before_delegate",
            "exact_ip_string_keys",
        ]
    );

    for fixture in fixtures {
        let QueryLimiterFixture {
            id,
            subsystem,
            runtime,
            production_defaults,
            input,
            expected,
        } = fixture;
        assert_eq!(subsystem, "dht_query_limiter");
        let QueryLimiterRuntime {
            implementation,
            clock,
        } = runtime;
        assert_eq!(
            implementation,
            "production queryLimiter with actual keyed limiter where identified"
        );
        assert_eq!(
            clock,
            "channel barriers or already-decided contexts/reservations; no sleeps"
        );
        let QueryLimiterDefaults {
            per_ip_every_nanos,
            per_ip_burst,
            per_ip_capacity,
            per_ip_ttl_nanos,
        } = production_defaults;
        assert_eq!(
            (
                per_ip_every_nanos,
                per_ip_burst,
                per_ip_capacity,
                per_ip_ttl_nanos,
            ),
            (1_000_000_000, 4, 1_000, 20_000_000_000)
        );
        let QueryLimiterInput {
            limiter_kind,
            context_kind,
            addresses,
            scripted_waits,
            delegate,
        } = input;
        let input_projection = format!(
            "{limiter_kind}|{context_kind}|{}|{}|{delegate}",
            addresses.join(","),
            scripted_waits.join(",")
        );
        let QueryLimiterExpected { events } = expected;
        let actual_events = events
            .into_iter()
            .map(query_event_projection)
            .collect::<Vec<_>>();
        let (expected_input, expected_events): (&str, &[&str]) = match id.as_str() {
            "wait_barrier_precedes_delegate" => (
                "scripted_barrier|background|192.0.2.11|barrier_then_success|success",
                &["1|192.0.2.11|192.0.2.11|wait,delegate|1|0|1122000000000000000000000000000000000000||false|false|false|false"],
            ),
            "wait_error_short_circuits_exact" => (
                "scripted|background|192.0.2.12|fixed wait error|must_not_run",
                &["1|192.0.2.12|192.0.2.12|wait|0|0||fixed wait error|true|false|false|false"],
            ),
            "delegate_error_identity_after_wait" => (
                "scripted|background|192.0.2.13|success|fixed delegate error",
                &["1|192.0.2.13|192.0.2.13|wait,delegate|1|0|0000000000000000000000000000000000000000|fixed delegate error|false|true|false|false"],
            ),
            "pre_canceled_actual_keyed_limiter" => (
                "actual_keyed_rate_1_per_hour_burst_1_ttl_0|pre_canceled_then_background|192.0.2.14,192.0.2.14||success",
                &[
                    "1|192.0.2.14|||0|0||context canceled|false|false|true|false",
                    "2|192.0.2.14|||1|0|1400000000000000000000000000000000000000||false|false|false|false",
                ],
            ),
            "expired_deadline_actual_keyed_limiter" => (
                "actual_keyed_rate_1_per_hour_burst_1_ttl_0|expired_deadline|192.0.2.15||must_not_run",
                &["1|192.0.2.15|||0|0||context deadline exceeded|false|false|false|true"],
            ),
            "future_deadline_rejected_before_delegate" => (
                "actual_keyed_rate_1_per_hour_burst_1_ttl_0|background_then_1_second_deadline|192.0.2.16,192.0.2.16||first_only",
                &[
                    "1|192.0.2.16|||1|0|1600000000000000000000000000000000000000||false|false|false|false",
                    "2|192.0.2.16|||1|0||rate: Wait(n=1) would exceed context deadline|false|false|false|false",
                ],
            ),
            "exact_ip_string_keys" => (
                "scripted|background|192.0.2.1,::ffff:192.0.2.1,fe80::1%7,fe80::1%8|success,success,success,success|success",
                &[
                    "1|192.0.2.1|192.0.2.1||1|0|0000000000000000000000000000000000000000||false|false|false|false",
                    "2|::ffff:192.0.2.1|192.0.2.1,::ffff:192.0.2.1||2|0|0000000000000000000000000000000000000000||false|false|false|false",
                    "3|fe80::1%7|192.0.2.1,::ffff:192.0.2.1,fe80::1%7||3|0|0000000000000000000000000000000000000000||false|false|false|false",
                    "4|fe80::1%8|192.0.2.1,::ffff:192.0.2.1,fe80::1%7,fe80::1%8||4|0|0000000000000000000000000000000000000000||false|false|false|false",
                ],
            ),
            _ => panic!("unclassified query-limiter row {id}"),
        };
        assert_eq!(input_projection, expected_input, "{id} input");
        assert_eq!(actual_events, expected_events, "{id} expected events");
    }
}

fn generated_addr(index: u32) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::from(0x0a00_0001_u32 + index),
        6_881,
    ))
}

fn allow_exactly(limiter: &DhtInboundRateLimiter, addr: SocketAddr, count: usize) {
    for index in 0..count {
        assert!(
            limiter.allow(addr),
            "admission {index} of {count} unexpectedly failed for {addr}"
        );
    }
}

async fn wait_exactly(limiter: &DhtOutboundRateLimiter, addr: SocketAddr, count: usize) {
    for _ in 0..count {
        limiter.wait(addr).await;
    }
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[tokio::test(start_paused = true)]
async fn inbound_defaults_lock_bursts_refill_and_per_ip_before_global_consumption() {
    let limiter = DhtInboundRateLimiter::new();

    allow_exactly(&limiter, ADDR_A, 10);
    assert_eq!(limiter.admit(ADDR_A), Err(DhtInboundRateLimitDenial::PerIp));
    allow_exactly(&limiter, ADDR_B, 10);
    assert_eq!(limiter.admit(ADDR_B), Err(DhtInboundRateLimitDenial::PerIp));

    // The global burst is now empty. Go's short-circuit order still consumes
    // one fresh C token before rejecting on the global bucket.
    assert_eq!(
        limiter.admit(ADDR_C),
        Err(DhtInboundRateLimitDenial::Global)
    );
    tokio::time::advance(Duration::from_millis(200)).await;

    // Ten global tokens refilled, but C has only nine whole tokens because its
    // rejected request above was consumed. Its per-IP rejection leaves the
    // tenth global token available for D.
    allow_exactly(&limiter, ADDR_C, 9);
    assert!(!limiter.allow(ADDR_C));
    assert!(limiter.allow(ADDR_D));

    let defaulted = DhtInboundRateLimiter::default();
    allow_exactly(&defaulted, ADDR_A, 10);
    assert!(!defaulted.allow(ADDR_A));
}

#[tokio::test(start_paused = true)]
async fn exact_ip_string_identities_keep_ipv4_mapped_and_native_buckets_distinct() {
    let mapped = SocketAddr::V6(SocketAddrV6::new(
        Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(),
        6_881,
        0,
        0,
    ));
    let native = SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
        6_881,
        0,
        0,
    ));
    let scoped_7 = SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
        6_881,
        0,
        7,
    ));
    let scoped_8 = SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
        6_881,
        0,
        8,
    ));
    assert_ne!(ADDR_A.to_string(), mapped.to_string());
    assert_ne!(mapped.to_string(), native.to_string());
    assert_ne!(scoped_7.to_string(), scoped_8.to_string());

    let inbound = DhtInboundRateLimiter::new();
    allow_exactly(&inbound, ADDR_A, 10);
    allow_exactly(&inbound, mapped, 10);
    assert!(!inbound.allow(ADDR_A));
    assert!(!inbound.allow(mapped));

    let scoped_inbound = DhtInboundRateLimiter::new();
    allow_exactly(&scoped_inbound, scoped_7, 10);
    allow_exactly(&scoped_inbound, scoped_8, 10);
    assert!(!scoped_inbound.allow(scoped_7));
    assert!(!scoped_inbound.allow(scoped_8));

    let outbound = DhtOutboundRateLimiter::new();
    wait_exactly(&outbound, ADDR_A, 4).await;
    wait_exactly(&outbound, mapped, 4).await;
    wait_exactly(&outbound, native, 4).await;
    wait_exactly(&outbound, scoped_7, 4).await;
    wait_exactly(&outbound, scoped_8, 4).await;

    for addr in [ADDR_A, mapped, native, scoped_7, scoped_8] {
        let mut pending = Box::pin(outbound.wait(addr));
        assert!(poll_once(pending.as_mut()).await.is_pending());
    }

    // Ports are not part of the Go/Rust policy key.
    let same_ip = DhtOutboundRateLimiter::new();
    wait_exactly(&same_ip, ADDR_A, 4).await;
    let mut other_port = Box::pin(same_ip.wait(ADDR_A_OTHER_PORT));
    assert!(poll_once(other_port.as_mut()).await.is_pending());

    let scoped_same_key = DhtOutboundRateLimiter::new();
    wait_exactly(&scoped_same_key, scoped_7, 4).await;
    let same_scope_other_flow_and_port = SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
        7_777,
        99,
        7,
    ));
    let mut ignored_fields = Box::pin(scoped_same_key.wait(same_scope_other_flow_and_port));
    assert!(poll_once(ignored_fields.as_mut()).await.is_pending());
}

#[tokio::test(start_paused = true)]
async fn outbound_defaults_admit_four_immediately_then_wait_exact_seconds() {
    let limiter = DhtOutboundRateLimiter::default();
    let started = tokio::time::Instant::now();
    wait_exactly(&limiter, ADDR_A, 4).await;
    assert_eq!(tokio::time::Instant::now(), started);

    let mut fifth = Box::pin(limiter.wait(ADDR_A));
    assert!(poll_once(fifth.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_millis(999)).await;
    assert!(poll_once(fifth.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(poll_once(fifth.as_mut()).await.is_ready());
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(1)
    );

    // The policy is keyed only by IP, so a different address retains its full
    // immediate burst.
    wait_exactly(&limiter, ADDR_B, 4).await;
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(1)
    );
}

#[tokio::test(start_paused = true)]
async fn concurrent_outbound_waiters_complete_in_reservation_order() {
    let limiter = DhtOutboundRateLimiter::new();
    wait_exactly(&limiter, ADDR_A, 4).await;

    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let mut handles = Vec::new();
    for id in 0_u8..6 {
        let limiter = limiter.clone();
        let done_tx = done_tx.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            start_rx.await.expect("start owner remains alive");
            entered_tx.send(()).expect("test awaits entry");
            limiter.wait(ADDR_A).await;
            done_tx.send(id).expect("test retains completion receiver");
        });
        start_tx.send(()).expect("worker retains start receiver");
        entered_rx.await.expect("worker reaches the wait call");
        tokio::task::yield_now().await;
        assert!(!handle.is_finished());
        handles.push(handle);
    }
    drop(done_tx);

    for id in 0_u8..6 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(done_rx.try_recv().expect("one waiter became ready"), id);
        assert!(matches!(
            done_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));
    }
    for handle in handles {
        handle.await.expect("ordered waiter does not panic");
    }
}

#[tokio::test(start_paused = true)]
async fn dropped_and_aborted_latest_waits_restore_only_eligible_reservations() {
    let limiter = DhtOutboundRateLimiter::new();
    wait_exactly(&limiter, ADDR_A, 4).await;

    let mut dropped = Box::pin(limiter.wait(ADDR_A));
    assert!(poll_once(dropped.as_mut()).await.is_pending());
    drop(dropped);
    let mut replacement = Box::pin(limiter.wait(ADDR_A));
    assert!(poll_once(replacement.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll_once(replacement.as_mut()).await.is_ready());

    let abort_limiter = DhtOutboundRateLimiter::new();
    wait_exactly(&abort_limiter, ADDR_A, 4).await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let aborted = tokio::spawn({
        let limiter = abort_limiter.clone();
        async move {
            entered_tx.send(()).expect("test awaits entry");
            limiter.wait(ADDR_A).await;
        }
    });
    entered_rx.await.expect("task reaches the wait call");
    tokio::task::yield_now().await;
    assert!(!aborted.is_finished());
    aborted.abort();
    assert!(aborted.await.expect_err("task was aborted").is_cancelled());
    let mut after_abort = Box::pin(abort_limiter.wait(ADDR_A));
    assert!(poll_once(after_abort.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll_once(after_abort.as_mut()).await.is_ready());

    // An older reservation cannot be blindly restored across a later one.
    let ordered = DhtOutboundRateLimiter::new();
    wait_exactly(&ordered, ADDR_A, 4).await;
    let mut older = Box::pin(ordered.wait(ADDR_A));
    let mut later = Box::pin(ordered.wait(ADDR_A));
    assert!(poll_once(older.as_mut()).await.is_pending());
    assert!(poll_once(later.as_mut()).await.is_pending());
    drop(older);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll_once(later.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll_once(later.as_mut()).await.is_ready());

    // Once an admission has completed it is committed, so dropping its
    // completed future cannot manufacture another token at the same instant.
    let mut committed_successor = Box::pin(ordered.wait(ADDR_A));
    assert!(poll_once(committed_successor.as_mut()).await.is_pending());
}

fn wait_error_class(error: DhtRateLimitWaitError) -> &'static str {
    match error {
        DhtRateLimitWaitError::Cancelled => "cancelled",
        DhtRateLimitWaitError::WouldExceedDeadline => "would_exceed_deadline",
    }
}

#[tokio::test(start_paused = true)]
async fn typed_cancellation_and_deadlines_fail_before_or_rollback_exact_reservations() {
    let pre_cancelled = DhtOutboundRateLimiter::new();
    let now = tokio::time::Instant::now();
    let expired = now
        .checked_sub(Duration::from_nanos(1))
        .expect("paused Tokio instant has a predecessor");
    let error = pre_cancelled
        .wait_with(ADDR_A, Some(expired), ready(()))
        .await
        .expect_err("biased pre-cancellation wins before an expired deadline");
    assert_eq!(error, DhtRateLimitWaitError::Cancelled);
    assert_eq!(wait_error_class(error), "cancelled");
    assert_eq!(error.to_string(), "DHT rate-limit wait cancelled");
    assert!(error.source().is_none());
    wait_exactly(&pre_cancelled, ADDR_A, 4).await;

    let expired_deadline = DhtOutboundRateLimiter::new();
    let error = expired_deadline
        .wait_until(ADDR_A, expired)
        .await
        .expect_err("expired deadline is rejected");
    assert_eq!(error, DhtRateLimitWaitError::WouldExceedDeadline);
    assert_eq!(wait_error_class(error), "would_exceed_deadline");
    assert_eq!(
        error.to_string(),
        "DHT rate-limit reservation would exceed deadline"
    );
    assert!(error.source().is_none());
    wait_exactly(&expired_deadline, ADDR_A, 4).await;

    let insufficient = DhtOutboundRateLimiter::new();
    wait_exactly(&insufficient, ADDR_A, 4).await;
    let scheduled_from = tokio::time::Instant::now();
    assert_eq!(
        insufficient
            .wait_until(ADDR_A, scheduled_from + Duration::from_millis(999))
            .await,
        Err(DhtRateLimitWaitError::WouldExceedDeadline)
    );
    let exact = tokio::spawn({
        let limiter = insufficient.clone();
        async move {
            limiter
                .wait_until(ADDR_A, scheduled_from + Duration::from_secs(1))
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(!exact.is_finished());
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(!exact.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(exact.await.expect("deadline waiter does not panic"), Ok(()));

    let cancellable = DhtOutboundRateLimiter::new();
    wait_exactly(&cancellable, ADDR_A, 4).await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let waiting = tokio::spawn({
        let limiter = cancellable.clone();
        async move {
            entered_tx.send(()).expect("test awaits typed wait entry");
            limiter
                .wait_with(ADDR_A, None, async move {
                    let _ = cancel_rx.await;
                })
                .await
        }
    });
    entered_rx.await.expect("typed wait task started");
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    cancel_tx
        .send(())
        .expect("typed waiter retains cancellation");
    assert_eq!(
        waiting.await.expect("typed waiter does not panic"),
        Err(DhtRateLimitWaitError::Cancelled)
    );

    let replacement = tokio::spawn({
        let limiter = cancellable.clone();
        async move { limiter.wait(ADDR_A).await }
    });
    tokio::task::yield_now().await;
    assert!(!replacement.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    replacement.await.expect("replacement wait does not panic");

    let tied = DhtOutboundRateLimiter::new();
    wait_exactly(&tied, ADDR_A, 4).await;
    let tie_at = tokio::time::Instant::now() + Duration::from_secs(1);
    let tied_wait = tokio::spawn({
        let limiter = tied.clone();
        async move {
            limiter
                .wait_with(ADDR_A, None, tokio::time::sleep_until(tie_at))
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(!tied_wait.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        tied_wait.await.expect("typed tie waiter does not panic"),
        Err(DhtRateLimitWaitError::Cancelled)
    );
    let mut restored_at_tie = Box::pin(tied.wait(ADDR_A));
    assert!(poll_once(restored_at_tie.as_mut()).await.is_ready());

    let exact_now = DhtOutboundRateLimiter::new();
    assert_eq!(
        exact_now
            .wait_until(ADDR_A, tokio::time::Instant::now())
            .await,
        Ok(())
    );
}

#[tokio::test(start_paused = true)]
async fn fixed_capacity_uses_access_recency_and_evicts_the_oldest_key() {
    let retained = DhtInboundRateLimiter::new();
    allow_exactly(&retained, ADDR_A, 10);
    for index in 0..999 {
        let _ = retained.allow(generated_addr(index));
    }
    assert!(!retained.allow(ADDR_A)); // refresh A as the most recent key
    let _ = retained.allow(generated_addr(999));
    tokio::time::advance(Duration::from_millis(400)).await;
    assert!(
        !retained.allow(ADDR_A),
        "a recently touched exhausted key must not be replaced"
    );

    let evicted = DhtInboundRateLimiter::new();
    allow_exactly(&evicted, ADDR_A, 10);
    for index in 0..1_000 {
        let _ = evicted.allow(generated_addr(index));
    }
    tokio::time::advance(Duration::from_millis(400)).await;
    assert!(
        evicted.allow(ADDR_A),
        "the oldest of 1,001 keys must be replaced with a full bucket"
    );

    let outbound = DhtOutboundRateLimiter::new();
    wait_exactly(&outbound, ADDR_A, 4).await;
    for index in 0..999 {
        outbound.wait(generated_addr(index)).await;
    }
    let mut touch = Box::pin(outbound.wait(ADDR_A));
    assert!(poll_once(touch.as_mut()).await.is_pending());
    drop(touch);
    outbound.wait(generated_addr(999)).await;
    let mut still_retained = Box::pin(outbound.wait(ADDR_A));
    assert!(poll_once(still_retained.as_mut()).await.is_pending());
    drop(still_retained);

    let outbound_evicted = DhtOutboundRateLimiter::new();
    wait_exactly(&outbound_evicted, ADDR_A, 4).await;
    for index in 0..1_000 {
        outbound_evicted.wait(generated_addr(index)).await;
    }
    let mut fresh = Box::pin(outbound_evicted.wait(ADDR_A));
    assert!(poll_once(fresh.as_mut()).await.is_ready());
}

#[tokio::test(start_paused = true)]
async fn fixed_ttl_keeps_the_deadline_entry_and_resets_strictly_after_twenty_seconds() {
    let inbound = DhtInboundRateLimiter::new();
    allow_exactly(&inbound, ADDR_A, 10);
    tokio::time::advance(Duration::from_secs(19)).await;
    allow_exactly(&inbound, ADDR_A, 10);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(inbound.allow(ADDR_A));
    assert!(
        !inbound.allow(ADDR_A),
        "the original bucket remains live at its exact deadline"
    );
    tokio::time::advance(Duration::from_nanos(1)).await;
    allow_exactly(&inbound, ADDR_A, 10);
    assert!(!inbound.allow(ADDR_A));

    let outbound = DhtOutboundRateLimiter::new();
    wait_exactly(&outbound, ADDR_B, 4).await;
    tokio::time::advance(Duration::from_secs(19)).await;
    wait_exactly(&outbound, ADDR_B, 4).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    outbound.wait(ADDR_B).await;
    let mut at_deadline = Box::pin(outbound.wait(ADDR_B));
    assert!(poll_once(at_deadline.as_mut()).await.is_pending());
    drop(at_deadline);
    tokio::time::advance(Duration::from_nanos(1)).await;
    wait_exactly(&outbound, ADDR_B, 4).await;
    let mut after_new_burst = Box::pin(outbound.wait(ADDR_B));
    assert!(poll_once(after_new_burst.as_mut()).await.is_pending());
}

#[test]
fn clones_share_one_policy_under_high_contention_without_panicking() {
    let limiter = DhtInboundRateLimiter::new();
    let barrier = Arc::new(Barrier::new(129));
    let mut threads = Vec::new();
    for _ in 0..128 {
        let limiter = limiter.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            catch_unwind(AssertUnwindSafe(|| limiter.allow(ADDR_A)))
        }));
    }
    barrier.wait();
    let outcomes = threads
        .into_iter()
        .map(|thread| thread.join().expect("outer thread does not panic"))
        .collect::<Vec<_>>();
    assert!(outcomes.iter().all(Result::is_ok));
    assert_eq!(
        outcomes
            .into_iter()
            .filter_map(Result::ok)
            .filter(|allowed| *allowed)
            .count(),
        10
    );

    fn assert_clone<T: Clone>() {}
    assert_clone::<DhtInboundRateLimiter>();
    assert_clone::<DhtOutboundRateLimiter>();
    let _: DhtInboundRateLimiter = Default::default();
    let _: DhtOutboundRateLimiter = Default::default();
}
