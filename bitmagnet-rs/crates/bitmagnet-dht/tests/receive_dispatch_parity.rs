//! Differential and boundary tests for one-datagram receive/dispatch.

use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, DatagramReceiver, DeliveryOutcome, Id20, KrpcMessage, MessageArgs,
    ReceiveDispatchError, ReceiveDispatchOutcome, ReceiveDispatcher, ReceivedDatagram,
    TransactionId, TransactionIdIssuer, TransactionIdSourceError, TransactionRegistry,
    TransactionWaitOutcome, MAX_INBOUND_DATAGRAM_BYTES,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: Input,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Input {
    wire_hex: String,
    source: FixtureAddr,
    #[serde(default)]
    pending_tid_hex: String,
    expected_source: Option<FixtureAddr>,
    #[serde(default)]
    duplicate_filled: bool,
}

#[derive(Deserialize)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    go_outcome: String,
    rust_outcome: String,
    #[serde(default)]
    canonical_wire_hex: String,
    pending_after: bool,
    rust_pending_after: bool,
    #[serde(default)]
    delivered_wire_hex: String,
    #[serde(default)]
    delivered_source: String,
    #[serde(default)]
    registry_unaffected: bool,
}

struct ScriptedIssuer(VecDeque<TransactionId>);

impl TransactionIdIssuer for ScriptedIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .ok_or_else(|| TransactionIdSourceError::new("scripted issuer exhausted"))
    }
}

#[derive(Clone)]
struct Packet {
    wire: Vec<u8>,
    source: SocketAddr,
    reported: Option<usize>,
}

struct FakeReceiver {
    packets: VecDeque<Result<Packet, &'static str>>,
}

impl DatagramReceiver for FakeReceiver {
    type Error = &'static str;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let packet = self.packets.pop_front().expect("scripted packet")?;
            let copied = packet.wire.len().min(buffer.len());
            buffer[..copied].copy_from_slice(&packet.wire[..copied]);
            Ok(ReceivedDatagram {
                length: packet.reported.unwrap_or(packet.wire.len()),
                source: packet.source,
            })
        })
    }
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/receive_dispatch.jsonl");
    BufReader::new(File::open(&path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn socket_addr(value: &FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => SocketAddr::V4(SocketAddrV4::new(ip, value.port)),
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
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

#[tokio::test]
async fn real_go_read_dispatch_matches_rust_harness() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 17);
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_receive_dispatch");
        assert!(!fixture.expected.go_outcome.is_empty());
        if fixture.expected.rust_outcome == "invalid_tid" {
            assert_eq!(fixture.expected.go_outcome, "response_delivered");
            assert!(fixture.expected.pending_after);
            assert!(!fixture.expected.rust_pending_after);
        } else {
            assert_eq!(fixture.expected.go_outcome, fixture.expected.rust_outcome);
            assert_eq!(
                fixture.expected.pending_after,
                fixture.expected.rust_pending_after
            );
        }
        let wire = hex::decode(&fixture.input.wire_hex).unwrap();
        let source = socket_addr(&fixture.input.source);
        let pending_bytes = hex::decode(&fixture.input.pending_tid_hex).unwrap();
        let valid_pending = TransactionId::from_slice(&pending_bytes).ok();
        let registry = TransactionRegistry::new(ScriptedIssuer(
            valid_pending.into_iter().collect::<VecDeque<_>>(),
        ));
        let mut pending = if let (Some(transaction_id), Some(expected)) =
            (valid_pending, fixture.input.expected_source.as_ref())
        {
            let registered = registry
                .register(socket_addr(expected), ByteString::new(b"ping"), args())
                .unwrap();
            assert_eq!(registered.transaction_id(), transaction_id);
            Some(registered.mark_sent())
        } else {
            None
        };

        if fixture.input.duplicate_filled {
            let first = KrpcMessage::decode_inbound(&wire).unwrap();
            assert_eq!(registry.deliver(source, first), DeliveryOutcome::Delivered);
        }

        let receiver = FakeReceiver {
            packets: VecDeque::from([Ok(Packet {
                wire,
                source,
                reported: None,
            })]),
        };
        let mut dispatcher = ReceiveDispatcher::new(receiver, registry.clone());
        let outcome = dispatcher.receive_one().await.unwrap();
        let label = match &outcome {
            ReceiveDispatchOutcome::ZeroLength { .. } => "zero",
            ReceiveDispatchOutcome::DecodeRejected { .. } => "decode_rejected",
            ReceiveDispatchOutcome::Query {
                message,
                source: got,
            } => {
                assert_eq!(*got, source);
                assert_eq!(
                    hex::encode(message.encode().unwrap()),
                    fixture.expected.canonical_wire_hex
                );
                "query"
            }
            ReceiveDispatchOutcome::Response {
                source: got,
                delivery,
            } => {
                assert_eq!(*got, source);
                match delivery {
                    DeliveryOutcome::Delivered => "response_delivered",
                    DeliveryOutcome::Duplicate => "duplicate",
                    DeliveryOutcome::UnknownTransaction => "unknown_tid",
                    DeliveryOutcome::InvalidTransactionId => "invalid_tid",
                    DeliveryOutcome::AddressMismatch { .. } => "address_mismatch",
                    other => panic!("[{}] unexpected response outcome {other:?}", fixture.id),
                }
            }
            ReceiveDispatchOutcome::Error {
                source: got,
                delivery,
            } => {
                assert_eq!(*got, source);
                match delivery {
                    DeliveryOutcome::Delivered => "error_delivered",
                    other => panic!("[{}] unexpected error outcome {other:?}", fixture.id),
                }
            }
            ReceiveDispatchOutcome::Ignored {
                message,
                source: got,
            } => {
                assert_eq!(*got, source);
                assert!(!matches!(
                    message.message_type.as_bytes(),
                    b"q" | b"r" | b"e"
                ));
                "ignored"
            }
        };
        assert_eq!(label, fixture.expected.rust_outcome, "[{}]", fixture.id);
        assert_eq!(
            registry.pending_count() != 0,
            fixture.expected.rust_pending_after,
            "[{}] Rust pending state",
            fixture.id
        );
        if fixture.expected.registry_unaffected && valid_pending.is_some() {
            assert_eq!(registry.pending_count(), 1, "[{}] registry", fixture.id);
        }

        if matches!(label, "response_delivered" | "error_delivered") {
            let waited = pending
                .take()
                .expect("delivered fixture owns pending transaction")
                .wait(Duration::from_secs(1))
                .await;
            let (accepted_source, accepted_message) = match waited {
                TransactionWaitOutcome::Response {
                    source, message, ..
                }
                | TransactionWaitOutcome::RemoteError {
                    source, message, ..
                } => (source, message),
                other => panic!("[{}] unexpected wait outcome {other:?}", fixture.id),
            };
            assert_eq!(
                accepted_source.to_string(),
                fixture.expected.delivered_source
            );
            assert_eq!(
                hex::encode(accepted_message.encode().unwrap()),
                fixture.expected.delivered_wire_hex
            );
        }
    }
}

#[tokio::test]
async fn malformed_then_valid_preserves_registration_and_buffer_outputs_are_owned() {
    let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([TransactionId::from(
        *b"R1",
    )])));
    let pending = registry
        .register(remote, ByteString::new(b"ping"), args())
        .unwrap()
        .mark_sent();
    let first_query =
        b"d1:ad2:id20:00000000000000000000e1:q4:ping1:t2:Q11:v5:first1:y1:qe".to_vec();
    let valid_response = b"d1:rd2:id20:00000000000000000000e1:t2:R11:y1:re".to_vec();
    let receiver = FakeReceiver {
        packets: VecDeque::from([
            Ok(Packet {
                wire: b"d1:t2:X1".to_vec(),
                source: remote,
                reported: None,
            }),
            Ok(Packet {
                wire: valid_response,
                source: remote,
                reported: None,
            }),
            Ok(Packet {
                wire: first_query,
                source: remote,
                reported: None,
            }),
            Ok(Packet {
                wire: b"d1:t2:Q21:v6:second1:y1:xe".to_vec(),
                source: "[2001:db8::1]:6882".parse().unwrap(),
                reported: None,
            }),
        ]),
    };
    let mut dispatcher = ReceiveDispatcher::new(receiver, registry.clone());
    assert!(matches!(
        dispatcher.receive_one().await.unwrap(),
        ReceiveDispatchOutcome::DecodeRejected { .. }
    ));
    assert_eq!(registry.pending_count(), 1);
    assert!(matches!(
        dispatcher.receive_one().await.unwrap(),
        ReceiveDispatchOutcome::Response {
            delivery: DeliveryOutcome::Delivered,
            ..
        }
    ));
    assert!(matches!(
        pending.wait(Duration::from_secs(1)).await,
        TransactionWaitOutcome::Response { .. }
    ));

    let owned = match dispatcher.receive_one().await.unwrap() {
        ReceiveDispatchOutcome::Query { message, source } => (message, source),
        other => panic!("unexpected first owned outcome {other:?}"),
    };
    assert!(matches!(
        dispatcher.receive_one().await.unwrap(),
        ReceiveDispatchOutcome::Ignored { .. }
    ));
    assert_eq!(owned.0.client_id.as_bytes(), b"first");
    assert_eq!(owned.1, remote);
}

#[tokio::test]
async fn transport_and_overreported_lengths_are_typed() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
    let receiver = FakeReceiver {
        packets: VecDeque::from([
            Err("receive failed"),
            Ok(Packet {
                wire: Vec::new(),
                source: "127.0.0.1:1".parse().unwrap(),
                reported: Some(MAX_INBOUND_DATAGRAM_BYTES + 1),
            }),
        ]),
    };
    let mut dispatcher = ReceiveDispatcher::new(receiver, registry);
    assert_eq!(
        dispatcher.receive_one().await,
        Err(ReceiveDispatchError::Transport("receive failed"))
    );
    assert_eq!(
        dispatcher.receive_one().await,
        Err(ReceiveDispatchError::OverreportedLength {
            reported: MAX_INBOUND_DATAGRAM_BYTES + 1,
            capacity: MAX_INBOUND_DATAGRAM_BYTES,
        })
    );
}

#[tokio::test]
async fn closed_registry_delivery_remains_typed() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
    registry.close();
    let receiver = FakeReceiver {
        packets: VecDeque::from([Ok(Packet {
            wire: b"d1:rd2:id20:00000000000000000000e1:t2:C11:y1:re".to_vec(),
            source: "127.0.0.1:6881".parse().unwrap(),
            reported: None,
        })]),
    };
    let mut dispatcher = ReceiveDispatcher::new(receiver, registry);
    assert!(matches!(
        dispatcher.receive_one().await.unwrap(),
        ReceiveDispatchOutcome::Response {
            delivery: DeliveryOutcome::RegistryClosed,
            ..
        }
    ));
}

#[test]
fn fixture_address_supports_native_numeric_scopes() {
    let addr = socket_addr(&FixtureAddr {
        ip: IpAddr::V6("fe80::1".parse::<Ipv6Addr>().unwrap()),
        port: 6881,
        scope: 3,
    });
    assert_eq!(addr.to_string(), "[fe80::1%3]:6881");
}
