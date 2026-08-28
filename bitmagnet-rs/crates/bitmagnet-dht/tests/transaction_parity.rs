//! Differential transaction-correlation proof from the real Go server oracle.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, DeliveryOutcome, Id20, KrpcError, KrpcMessage, MessageArgs, MessageReturn,
    RegisterSendError, TransactionId, TransactionIdIssuer, TransactionIdSourceError,
    TransactionRegistry, TransactionWaitOutcome,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: FixtureInput,
    expected: FixtureExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureInput {
    issuer_tids: Vec<String>,
    remotes: Vec<String>,
    query: String,
    address_cases: Vec<AddressCase>,
    deliveries: Vec<DeliveryInput>,
}

#[derive(Deserialize)]
struct AddressCase {
    left: String,
    right: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryInput {
    kind: String,
    tid: String,
    from: String,
    #[serde(default)]
    client_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureExpected {
    sends: Vec<ExpectedSend>,
    pending_while_sent: usize,
    pending_after_cancel: usize,
    send_failure_was_returned: bool,
    pending_after_send_error: usize,
    address_matches: Vec<bool>,
    delivery_observations: Vec<String>,
    first_client_id: String,
    pending_after_delivery: usize,
    terminal_cases: Vec<TerminalCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedSend {
    tid: String,
    addr: String,
    wire_hex: String,
    registered_at_send: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TerminalCase {
    name: String,
    outcome: String,
    pending_after: usize,
}

struct ScriptedIssuer(VecDeque<Result<TransactionId, TransactionIdSourceError>>);

impl TransactionIdIssuer for ScriptedIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .unwrap_or_else(|| Err(TransactionIdSourceError::new("fixture issuer exhausted")))
    }
}

fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/transaction.jsonl");
    let file = File::open(&path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    let lines = BufReader::new(file)
        .lines()
        .map(|line| line.expect("read transaction fixture line"))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        lines.len(),
        1,
        "expected one aggregate Go transaction fixture"
    );
    serde_json::from_str(&lines[0]).expect("decode transaction fixture")
}

fn issuer(hex_tids: &[String]) -> ScriptedIssuer {
    ScriptedIssuer(
        hex_tids
            .iter()
            .map(|value| {
                let bytes = hex::decode(value).expect("decode fixture TID");
                TransactionId::from_slice(&bytes).map_err(|error| {
                    TransactionIdSourceError::new(format!("invalid fixture TID: {error}"))
                })
            })
            .collect(),
    )
}

fn one_issuer(value: &[u8; 2]) -> ScriptedIssuer {
    ScriptedIssuer(VecDeque::from([Ok(TransactionId::from(*value))]))
}

fn args() -> MessageArgs {
    MessageArgs {
        id: Id20::ZERO,
        info_hash: None,
        target: None,
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: None,
        no_seed: 0,
        scrape: 0,
    }
}

fn response(tid: &[u8], kind: &[u8], body: bool, client_id: &[u8]) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(tid),
        message_type: ByteString::new(kind),
        query: ByteString::default(),
        args: None,
        response: (kind == b"r" && body).then_some(MessageReturn {
            id: Id20::ZERO,
            nodes: None,
            nodes6: None,
            token: None,
            values: None,
            interval: None,
            num: None,
            samples: None,
            seeders_bloom: None,
            peers_bloom: None,
        }),
        error: (kind == b"e" && body).then(|| KrpcError {
            code: 201,
            message: ByteString::new(b"remote"),
        }),
        observed_addr: None,
        read_only: false,
        client_id: ByteString::new(client_id),
    }
}

#[tokio::test]
async fn go_registration_wire_address_and_first_wins_match() {
    let fixture = fixture();
    assert_eq!(fixture.id, "go_server_transaction_core");
    assert_eq!(fixture.subsystem, "dht_transaction");
    assert_eq!(fixture.expected.sends.len(), 2);
    assert_eq!(fixture.expected.pending_while_sent, 2);
    assert!(fixture.expected.send_failure_was_returned);

    let registry = TransactionRegistry::new(issuer(&fixture.input.issuer_tids));
    let mut pending = Vec::new();
    for (remote, expected) in fixture.input.remotes.iter().zip(&fixture.expected.sends) {
        let remote: SocketAddr = remote.parse().expect("parse fixture remote");
        let pending_query = registry
            .register_before_send(
                remote,
                ByteString::new(fixture.input.query.as_bytes()),
                args(),
                |registered| {
                    assert_eq!(registered.remote(), remote);
                    assert_eq!(registered.message().query.as_bytes(), b"ping");
                    assert!(registry.is_pending(registered.transaction_id()));
                    assert!(expected.registered_at_send);
                    assert_eq!(registered.transaction_id().to_hex(), expected.tid);
                    assert_eq!(hex::encode(registered.wire().unwrap()), expected.wire_hex);
                    Ok::<_, ()>(())
                },
            )
            .unwrap();
        assert_eq!(remote.to_string(), expected.addr);
        pending.push(pending_query);
    }
    assert_eq!(
        registry.pending_count(),
        fixture.expected.pending_while_sent
    );
    for pending_query in pending {
        assert_eq!(pending_query.cancel(), TransactionWaitOutcome::Cancelled);
    }
    assert_eq!(
        registry.pending_count(),
        fixture.expected.pending_after_cancel
    );

    let failure_registry = TransactionRegistry::new(one_issuer(b"C3"));
    let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
    assert!(matches!(
        failure_registry.register_before_send(
            remote,
            ByteString::new(b"ping"),
            args(),
            |registered| {
                assert!(failure_registry.is_pending(registered.transaction_id()));
                let _wire = registered.wire().unwrap();
                Err("oracle send failure")
            }
        ),
        Err(RegisterSendError::Send("oracle send failure"))
    ));
    assert_eq!(
        failure_registry.pending_count(),
        fixture.expected.pending_after_send_error
    );

    for (index, case) in fixture.input.address_cases.iter().enumerate() {
        let tid = (index as u16).to_be_bytes();
        let registry = TransactionRegistry::new(one_issuer(&tid));
        let pending_query = registry
            .register(
                case.left.parse().expect("parse expected source"),
                ByteString::new(b"ping"),
                args(),
            )
            .unwrap()
            .mark_sent();
        let outcome = registry.deliver(
            case.right.parse().expect("parse actual source"),
            response(&tid, b"r", true, b""),
        );
        assert_eq!(
            outcome == DeliveryOutcome::Delivered,
            fixture.expected.address_matches[index]
        );
        drop(pending_query);
    }

    let delivery_registry = TransactionRegistry::new(one_issuer(b"D4"));
    let delivery_pending = delivery_registry
        .register(remote, ByteString::new(b"ping"), args())
        .unwrap()
        .mark_sent();
    let mut observed = Vec::new();
    for delivery in &fixture.input.deliveries {
        let tid = hex::decode(&delivery.tid).unwrap();
        let client_id = hex::decode(&delivery.client_id).unwrap();
        let outcome = delivery_registry.deliver(
            delivery.from.parse().unwrap(),
            response(&tid, b"r", true, &client_id),
        );
        observed.push(match outcome {
            DeliveryOutcome::UnknownTransaction => "unknown",
            DeliveryOutcome::AddressMismatch { .. } => "address_mismatch",
            DeliveryOutcome::Delivered => "delivered",
            DeliveryOutcome::Duplicate => "duplicate",
            other => panic!("unexpected delivery outcome: {other:?}"),
        });
        assert_eq!(observed.last().unwrap(), &delivery.kind);
    }
    assert_eq!(observed, fixture.expected.delivery_observations);
    assert_eq!(
        delivery_registry.pending_count(),
        fixture.expected.pending_after_delivery
    );
    let outcome = delivery_pending.wait(Duration::from_secs(1)).await;
    assert!(matches!(
        outcome,
        TransactionWaitOutcome::Response { message, source, .. }
            if source == remote
                && hex::encode(message.client_id.as_bytes()) == fixture.expected.first_client_id
    ));
}

#[tokio::test(start_paused = true)]
async fn go_terminal_cleanup_matrix_has_typed_rust_outcomes() {
    let fixture = fixture();
    assert_eq!(fixture.expected.terminal_cases.len(), 7);
    let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();

    for terminal in &fixture.expected.terminal_cases {
        assert_eq!(terminal.pending_after, 0, "Go leaked {}", terminal.name);
        let tid: [u8; 2] = match terminal.name.as_str() {
            "happy" => *b"H1",
            "remote_error" => *b"E1",
            "missing_error_body" => *b"E2",
            "missing_return_body" => *b"R0",
            "pre_cancelled" => *b"C0",
            "timeout" => *b"T0",
            "late_after_cancel" => *b"L0",
            other => panic!("unknown terminal case {other}"),
        };
        let registry = TransactionRegistry::new(one_issuer(&tid));
        let pending = registry
            .register(remote, ByteString::new(b"ping"), args())
            .unwrap()
            .mark_sent();

        let outcome = match terminal.name.as_str() {
            "happy" => {
                assert_eq!(
                    registry.deliver(remote, response(&tid, b"r", true, b"")),
                    DeliveryOutcome::Delivered
                );
                pending.wait(Duration::from_secs(1)).await
            }
            "remote_error" => {
                registry.deliver(remote, response(&tid, b"e", true, b""));
                pending.wait(Duration::from_secs(1)).await
            }
            "missing_error_body" => {
                registry.deliver(remote, response(&tid, b"e", false, b""));
                pending.wait(Duration::from_secs(1)).await
            }
            "missing_return_body" => {
                registry.deliver(remote, response(&tid, b"r", false, b""));
                pending.wait(Duration::from_secs(1)).await
            }
            "pre_cancelled" => pending.cancel(),
            "timeout" => {
                let waiter = tokio::spawn(pending.wait(Duration::from_secs(1)));
                tokio::time::advance(Duration::from_secs(1)).await;
                waiter.await.unwrap()
            }
            "late_after_cancel" => {
                let outcome = pending.cancel();
                assert_eq!(
                    registry.deliver(remote, response(&tid, b"r", true, b"")),
                    DeliveryOutcome::UnknownTransaction
                );
                outcome
            }
            _ => unreachable!(),
        };

        match (terminal.name.as_str(), terminal.outcome.as_str(), outcome) {
            ("happy", "success", TransactionWaitOutcome::Response { .. })
            | (
                "remote_error",
                "KRPC error 201: remote",
                TransactionWaitOutcome::RemoteError { .. },
            ) => {}
            (
                "missing_error_body",
                "typed_nil_error",
                TransactionWaitOutcome::MissingErrorBody { source, message },
            ) if source == remote
                && message.transaction_id.as_bytes() == b"E2"
                && message.message_type.as_bytes() == b"e" => {}
            (
                "missing_return_body",
                "return data missing from response",
                TransactionWaitOutcome::MissingReturnBody { source, message },
            ) if source == remote
                && message.transaction_id.as_bytes() == b"R0"
                && message.message_type.as_bytes() == b"r" => {}
            (
                "pre_cancelled" | "late_after_cancel",
                "cancelled",
                TransactionWaitOutcome::Cancelled,
            )
            | ("timeout", "timeout", TransactionWaitOutcome::Timeout) => {}
            (_, _, other) => panic!(
                "Rust outcome {other:?} disagrees with Go {} ({})",
                terminal.name, terminal.outcome
            ),
        }
        assert_eq!(registry.pending_count(), terminal.pending_after);
    }
}
