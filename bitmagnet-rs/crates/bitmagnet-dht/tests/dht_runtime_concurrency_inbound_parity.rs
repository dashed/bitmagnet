use serde::Deserialize;

const FIXTURE: &str =
    include_str!("../../../../testdata/parity/dht/dht_runtime_concurrency_inbound.jsonl");

const BLOCKED_QUERY_WIRE: &str = "64313a6164323a696432303a000000000000000000000000000000000000001165313a71343a70696e67313a74323a5131313a79313a7165";
const CORRELATED_RESPONSE_WIRE: &str = "64313a7264323a696432303a000000000000000000000000000000000000002265313a74323a5231313a79313a7265";
const BLOCKED_REPLY_WIRE: &str = "64313a7264323a696432303a000000000000000000000000000000000000003365313a74323a5131313a79313a7265";
const LIMITER_QUERY_WIRE: &str = "64313a6164323a696432303a000000000000000000000000000000000000004465313a71343a70696e67313a74323a4c31313a79313a7165";
const DENIAL_REPLY_WIRE: &str =
    "64313a656c693230316531373a746f6f206d616e7920726571756573747365313a74323a4c31313a79313a7265";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    runtime: Runtime,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Runtime {
    int_bits: u32,
    implementation: String,
    coordination: String,
    existing_wrapper_evidence: String,
    composition_limit: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    invocation: String,
    messages: Vec<InputMessage>,
    pending: Pending,
    responder_kind: String,
    limiter: Limiter,
    socket_kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputMessage {
    role: String,
    delivery: String,
    source: Addr,
    wire_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Pending {
    present: bool,
    tid_hex: String,
    expected_source: Addr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Limiter {
    kind: String,
    overall_limit_per_second: f64,
    overall_burst: i64,
    per_ip_limit_per_second: f64,
    per_ip_burst: i64,
    per_ip_capacity: i64,
    per_ip_ttl_nanos: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    receive_calls: usize,
    responder_calls: usize,
    limiter_calls: usize,
    delegate_calls: usize,
    table_effect_calls: usize,
    partial_order: PartialOrder,
    delivery: Delivery,
    pending_entry_present_after_delivery: bool,
    handler_deadline_present: bool,
    denial_error_identity_exact: bool,
    denial_error_source: String,
    sends: Vec<Send>,
    terminal: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialOrder {
    query_send_entered: bool,
    later_response_delivered_before_send_release: bool,
    read_advanced_after_script_before_send_release: bool,
    query_send_completed_before_release: bool,
    query_send_completed_after_release: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Delivery {
    present: bool,
    source: Addr,
    wire_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Send {
    destination: Addr,
    wire_hex: String,
    envelope: Envelope,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Envelope {
    tid_hex: String,
    type_hex: String,
    presence: Presence,
    return_id_hex: String,
    error: WireError,
    canonical: bool,
    tid_echoed: bool,
    request_fields_cleared: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Presence {
    #[serde(rename = "q")]
    query: bool,
    #[serde(rename = "a")]
    arguments: bool,
    #[serde(rename = "r")]
    returned: bool,
    #[serde(rename = "e")]
    error: bool,
    #[serde(rename = "ip")]
    ip: bool,
    #[serde(rename = "ro")]
    read_only: bool,
    #[serde(rename = "v")]
    client_id: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireError {
    present: bool,
    code: i64,
    message_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Addr {
    ip: String,
    port: u16,
    scope: u32,
}

#[test]
fn frozen_go_rows_are_strictly_and_exhaustively_consumed() {
    let mut rows = FIXTURE.lines().map(|line| {
        assert!(!line.is_empty(), "fixture must not contain blank rows");
        serde_json::from_str::<Fixture>(line).expect("strict Go runtime concurrency/inbound row")
    });

    let blocked = rows.next().expect("blocked-query row");
    let denial = rows.next().expect("limiter-denial row");
    assert!(
        rows.next().is_none(),
        "fixture must contain exactly two rows"
    );

    assert_blocked_query_row(blocked);
    assert_limiter_denial_row(denial);
}

fn assert_blocked_query_row(fixture: Fixture) {
    let Fixture {
        id,
        subsystem,
        runtime,
        input,
        expected,
    } = fixture;
    assert_eq!(id, "blocked_query_reply_later_response_delivered");
    assert_eq!(subsystem, "dht_runtime_concurrency_inbound");
    assert_runtime(runtime);

    let Input {
        invocation,
        messages,
        pending,
        responder_kind,
        limiter,
        socket_kind,
    } = input;
    assert_eq!(invocation, "server.read");
    let mut messages = messages.into_iter();
    assert_input_message(
        messages.next().expect("query message"),
        "query",
        "socket_receive_1",
        ("192.0.2.1", 6_881, 0),
        BLOCKED_QUERY_WIRE,
    );
    assert_input_message(
        messages.next().expect("response message"),
        "correlated_response",
        "socket_receive_2",
        ("198.51.100.2", 6_882, 0),
        CORRELATED_RESPONSE_WIRE,
    );
    assert!(messages.next().is_none(), "blocked row message count");
    let Pending {
        present,
        tid_hex,
        expected_source,
    } = pending;
    assert!(present);
    assert_eq!(tid_hex, "5231");
    assert_addr(expected_source, "198.51.100.2", 6_882, 0);
    assert_eq!(responder_kind, "fixed_success");
    assert_limiter(limiter, "none", 0.0, 0, 0.0, 0, 0, 0);
    assert_eq!(socket_kind, "scripted_receive_and_blocked_send");

    let Expected {
        receive_calls,
        responder_calls,
        limiter_calls,
        delegate_calls,
        table_effect_calls,
        partial_order,
        delivery,
        pending_entry_present_after_delivery,
        handler_deadline_present,
        denial_error_identity_exact,
        denial_error_source,
        sends,
        terminal,
    } = expected;
    assert_eq!(receive_calls, 3);
    assert_eq!(responder_calls, 1);
    assert_eq!(limiter_calls, 0);
    assert_eq!(delegate_calls, 0);
    assert_eq!(table_effect_calls, 0);
    let PartialOrder {
        query_send_entered,
        later_response_delivered_before_send_release,
        read_advanced_after_script_before_send_release,
        query_send_completed_before_release,
        query_send_completed_after_release,
    } = partial_order;
    assert!(query_send_entered);
    assert!(later_response_delivered_before_send_release);
    assert!(read_advanced_after_script_before_send_release);
    assert!(!query_send_completed_before_release);
    assert!(query_send_completed_after_release);
    let Delivery {
        present,
        source,
        wire_hex,
    } = delivery;
    assert!(present);
    assert_addr(source, "198.51.100.2", 6_882, 0);
    assert_eq!(wire_hex, CORRELATED_RESPONSE_WIRE);
    assert!(pending_entry_present_after_delivery);
    assert!(handler_deadline_present);
    assert!(!denial_error_identity_exact);
    assert_eq!(denial_error_source, "");
    let mut sends = sends.into_iter();
    assert_send(
        sends.next().expect("blocked reply send"),
        ("192.0.2.1", 6_881, 0),
        BLOCKED_REPLY_WIRE,
        "5131",
        true,
        false,
        "0000000000000000000000000000000000000033",
        (false, 0, ""),
    );
    assert!(sends.next().is_none(), "blocked row send count");
    assert_eq!(terminal, "read_returned_after_cancel");
}

fn assert_limiter_denial_row(fixture: Fixture) {
    let Fixture {
        id,
        subsystem,
        runtime,
        input,
        expected,
    } = fixture;
    assert_eq!(id, "limiter_denial_exact_response_wire");
    assert_eq!(subsystem, "dht_runtime_concurrency_inbound");
    assert_runtime(runtime);

    let Input {
        invocation,
        messages,
        pending,
        responder_kind,
        limiter,
        socket_kind,
    } = input;
    assert_eq!(invocation, "server.handleQuery");
    let mut messages = messages.into_iter();
    assert_input_message(
        messages.next().expect("limiter query message"),
        "query",
        "direct_handle_query",
        ("203.0.113.9", 6_999, 0),
        LIMITER_QUERY_WIRE,
    );
    assert!(messages.next().is_none(), "limiter row message count");
    let Pending {
        present,
        tid_hex,
        expected_source,
    } = pending;
    assert!(!present);
    assert_eq!(tid_hex, "");
    assert_addr(expected_source, "", 0, 0);
    assert_eq!(responder_kind, "actual_limiter_exact_denial_adapter");
    assert_limiter(
        limiter,
        "responder.NewLimiter",
        0.0,
        0,
        0.0,
        0,
        1,
        3_600_000_000_000,
    );
    assert_eq!(socket_kind, "capture_send_success");

    let Expected {
        receive_calls,
        responder_calls,
        limiter_calls,
        delegate_calls,
        table_effect_calls,
        partial_order,
        delivery,
        pending_entry_present_after_delivery,
        handler_deadline_present,
        denial_error_identity_exact,
        denial_error_source,
        sends,
        terminal,
    } = expected;
    assert_eq!(receive_calls, 0);
    assert_eq!(responder_calls, 1);
    assert_eq!(limiter_calls, 1);
    assert_eq!(delegate_calls, 0);
    assert_eq!(table_effect_calls, 0);
    let PartialOrder {
        query_send_entered,
        later_response_delivered_before_send_release,
        read_advanced_after_script_before_send_release,
        query_send_completed_before_release,
        query_send_completed_after_release,
    } = partial_order;
    assert!(!query_send_entered);
    assert!(!later_response_delivered_before_send_release);
    assert!(!read_advanced_after_script_before_send_release);
    assert!(!query_send_completed_before_release);
    assert!(!query_send_completed_after_release);
    let Delivery {
        present,
        source,
        wire_hex,
    } = delivery;
    assert!(!present);
    assert_addr(source, "", 0, 0);
    assert_eq!(wire_hex, "");
    assert!(!pending_entry_present_after_delivery);
    assert!(handler_deadline_present);
    assert!(denial_error_identity_exact);
    assert_eq!(denial_error_source, "responder.ErrTooManyRequests");
    let mut sends = sends.into_iter();
    assert_send(
        sends.next().expect("limiter denial send"),
        ("203.0.113.9", 6_999, 0),
        DENIAL_REPLY_WIRE,
        "4c31",
        false,
        true,
        "",
        (true, 201, "746f6f206d616e79207265717565737473"),
    );
    assert!(sends.next().is_none(), "limiter row send count");
    assert_eq!(terminal, "handle_query_returned_after_send");
}

fn assert_runtime(runtime: Runtime) {
    let Runtime {
        int_bits,
        implementation,
        coordination,
        existing_wrapper_evidence,
        composition_limit,
    } = runtime;
    assert_eq!(int_bits, 64);
    assert_eq!(implementation, "go_production_paths_with_oracle_only_gates");
    assert_eq!(coordination, "channels_only_no_sleeps");
    assert_eq!(
        existing_wrapper_evidence,
        "testdata/parity/dht/responder_limiter.jsonl#outer_denial_and_delegate_effects"
    );
    assert_eq!(
        composition_limit,
        "private responderLimiter is proven separately; denial row composes actual exported limiter and exact denial sentinel through private server.handleQuery"
    );
}

fn assert_input_message(
    message: InputMessage,
    expected_role: &str,
    expected_delivery: &str,
    expected_source: (&str, u16, u32),
    expected_wire: &str,
) {
    let InputMessage {
        role,
        delivery,
        source,
        wire_hex,
    } = message;
    assert_eq!(role, expected_role);
    assert_eq!(delivery, expected_delivery);
    assert_addr(
        source,
        expected_source.0,
        expected_source.1,
        expected_source.2,
    );
    assert_eq!(wire_hex, expected_wire);
}

#[allow(clippy::too_many_arguments)]
fn assert_limiter(
    limiter: Limiter,
    expected_kind: &str,
    expected_overall_limit: f64,
    expected_overall_burst: i64,
    expected_per_ip_limit: f64,
    expected_per_ip_burst: i64,
    expected_per_ip_capacity: i64,
    expected_per_ip_ttl_nanos: i64,
) {
    let Limiter {
        kind,
        overall_limit_per_second,
        overall_burst,
        per_ip_limit_per_second,
        per_ip_burst,
        per_ip_capacity,
        per_ip_ttl_nanos,
    } = limiter;
    assert_eq!(kind, expected_kind);
    assert_eq!(overall_limit_per_second, expected_overall_limit);
    assert_eq!(overall_burst, expected_overall_burst);
    assert_eq!(per_ip_limit_per_second, expected_per_ip_limit);
    assert_eq!(per_ip_burst, expected_per_ip_burst);
    assert_eq!(per_ip_capacity, expected_per_ip_capacity);
    assert_eq!(per_ip_ttl_nanos, expected_per_ip_ttl_nanos);
}

#[allow(clippy::too_many_arguments)]
fn assert_send(
    send: Send,
    expected_destination: (&str, u16, u32),
    expected_wire: &str,
    expected_tid_hex: &str,
    expected_return_presence: bool,
    expected_error_presence: bool,
    expected_return_id_hex: &str,
    expected_error: (bool, i64, &str),
) {
    let Send {
        destination,
        wire_hex,
        envelope,
    } = send;
    assert_addr(
        destination,
        expected_destination.0,
        expected_destination.1,
        expected_destination.2,
    );
    assert_eq!(wire_hex, expected_wire);
    let Envelope {
        tid_hex,
        type_hex,
        presence,
        return_id_hex,
        error,
        canonical,
        tid_echoed,
        request_fields_cleared,
    } = envelope;
    assert_eq!(tid_hex, expected_tid_hex);
    assert_eq!(type_hex, "72");
    let Presence {
        query,
        arguments,
        returned,
        error: error_present,
        ip,
        read_only,
        client_id,
    } = presence;
    assert!(!query);
    assert!(!arguments);
    assert_eq!(returned, expected_return_presence);
    assert_eq!(error_present, expected_error_presence);
    assert!(!ip);
    assert!(!read_only);
    assert!(!client_id);
    assert_eq!(return_id_hex, expected_return_id_hex);
    let WireError {
        present,
        code,
        message_hex,
    } = error;
    assert_eq!(present, expected_error.0);
    assert_eq!(code, expected_error.1);
    assert_eq!(message_hex, expected_error.2);
    assert!(canonical);
    assert!(tid_echoed);
    assert!(request_fields_cleared);
}

fn assert_addr(addr: Addr, expected_ip: &str, expected_port: u16, expected_scope: u32) {
    let Addr { ip, port, scope } = addr;
    assert_eq!(ip, expected_ip);
    assert_eq!(port, expected_port);
    assert_eq!(scope, expected_scope);
}
