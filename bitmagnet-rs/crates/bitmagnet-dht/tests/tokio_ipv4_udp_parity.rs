//! Real loopback and production-Go parity for the bounded Tokio IPv4 UDP seam.

use std::collections::VecDeque;
use std::fs::File;
use std::future::{pending, Future};
use std::io::{BufRead, BufReader};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::num::NonZeroU8;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bitmagnet_dht::{
    DatagramReceiver, DatagramSender, Id20, NodeTable, PingFindNodeClient, PingFindNodeSupervisor,
    PingFindNodeSupervisorExit, ReceivedDatagram, TokioIpv4UdpError, TokioIpv4UdpReceiver,
    TokioIpv4UdpSender, TokioIpv4UdpTransport, TransactionId, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry, MAX_INBOUND_DATAGRAM_BYTES,
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
    #[serde(default)]
    payload_hex: String,
    payload_length: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    sent: bool,
    received: bool,
    length: usize,
    sha256_hex: String,
    #[serde(rename = "sourceIPv4")]
    source_ipv4: bool,
    source_port_nonzero: bool,
    #[serde(rename = "destinationIPv4")]
    destination_ipv4: bool,
    destination_port_nonzero: bool,
}

struct Issuer(VecDeque<TransactionId>);

impl TransactionIdIssuer for Issuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .ok_or_else(|| TransactionIdSourceError::new("scripted issuer exhausted"))
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/tokio_ipv4_udp.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

async fn pair() -> (
    TokioIpv4UdpReceiver,
    TokioIpv4UdpSender,
    TokioIpv4UdpReceiver,
    TokioIpv4UdpSender,
) {
    let (left_receiver, left_sender) = TokioIpv4UdpTransport::bind_loopback()
        .await
        .unwrap()
        .into_parts();
    let (right_receiver, right_sender) = TokioIpv4UdpTransport::bind_loopback()
        .await
        .unwrap()
        .into_parts();
    (left_receiver, left_sender, right_receiver, right_sender)
}

async fn receive_bounded(
    receiver: &mut TokioIpv4UdpReceiver,
    buffer: &mut [u8],
) -> ReceivedDatagram {
    tokio::time::timeout(Duration::from_secs(2), receiver.receive(buffer))
        .await
        .expect("loopback receive exceeded the test bound")
        .unwrap()
}

#[tokio::test]
async fn actual_go_production_socket_fixture_matches_real_tokio_loopback() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 3);
    let mut ids = Vec::with_capacity(fixtures.len());
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_tokio_ipv4_udp");
        let (mut receiver, _, _, mut sender) = pair().await;
        let payload = hex::decode(&fixture.input.payload_hex).unwrap();
        assert_eq!(payload.len(), fixture.input.payload_length);
        match fixture.id.as_str() {
            "zero_length" => {
                assert!(payload.is_empty());
                assert_eq!(
                    fixture.expected.sha256_hex,
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                );
            }
            "binary" => {
                assert_eq!(payload, [0, 1, 2, 0x7f, 0x80, 0xfe, 0xff]);
                assert_eq!(
                    fixture.expected.sha256_hex,
                    "7bb6463b30f9e301fed333cdf8960ca9497b602ccd8eeb46ae42693fdea15a4d"
                );
            }
            "safe_8192" => {
                assert_eq!(
                    payload,
                    (0..8192)
                        .map(|index| (index % 251) as u8)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    fixture.expected.sha256_hex,
                    "25df2449b2e5a35fea14e02a7158e283801a1069c9f84631b9a9dacb2f809a7f"
                );
            }
            other => panic!("unexpected Tokio IPv4 UDP fixture {other}"),
        }
        let destination = SocketAddr::V4(receiver.local_addr());
        sender.send(destination, &payload).await.unwrap();
        let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
        let received = receive_bounded(&mut receiver, &mut buffer).await;
        assert_eq!(&buffer[..received.length], payload);
        assert_eq!(received.source, SocketAddr::V4(sender.local_addr()));
        assert_eq!(received.length, fixture.expected.length);
        assert_eq!(
            hex::encode(&buffer[..received.length]),
            fixture.input.payload_hex
        );
        assert!(!fixture.expected.sha256_hex.is_empty());
        assert!(fixture.expected.sent && fixture.expected.received);
        assert!(fixture.expected.source_ipv4 && fixture.expected.destination_ipv4);
        assert!(fixture.expected.source_port_nonzero && fixture.expected.destination_port_nonzero);
        ids.push(fixture.id);
    }
    ids.sort();
    assert_eq!(ids, ["binary", "safe_8192", "zero_length"]);
}

#[tokio::test]
async fn address_family_and_datagram_ceiling_fail_before_io() {
    let (mut receiver, _, _, mut sender) = pair().await;
    for destination in [
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 77, 9)),
        SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped(),
            6881,
            123,
            4,
        )),
    ] {
        assert!(matches!(
            sender.send(destination, b"x").await,
            Err(TokioIpv4UdpError::UnsupportedDestinationFamily(actual)) if actual == destination
        ));
    }
    let too_large = vec![0; MAX_INBOUND_DATAGRAM_BYTES + 1];
    assert!(matches!(
        sender
            .send(SocketAddr::V4(receiver.local_addr()), &too_large)
            .await,
        Err(TokioIpv4UdpError::DatagramTooLarge { actual, maximum })
            if actual == MAX_INBOUND_DATAGRAM_BYTES + 1 && maximum == MAX_INBOUND_DATAGRAM_BYTES
    ));
    let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.receive(&mut buffer))
            .await
            .is_err(),
        "preflight rejections must not place a datagram on the peer socket"
    );
}

#[tokio::test]
async fn maximum_is_admitted_to_one_syscall_and_platform_io_failure_is_preserved() {
    let (mut receiver, _, _, mut sender) = pair().await;
    let payload = vec![0x5a; MAX_INBOUND_DATAGRAM_BYTES];
    match sender
        .send(SocketAddr::V4(receiver.local_addr()), &payload)
        .await
    {
        Ok(()) => {
            let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
            let received = receive_bounded(&mut receiver, &mut buffer).await;
            assert_eq!(received.length, payload.len());
            assert_eq!(buffer, payload);
        }
        Err(TokioIpv4UdpError::SendIo {
            destination,
            source: _,
        }) => {
            assert_eq!(destination, receiver.local_addr());
        }
        other => panic!("maximum payload was not admitted to the OS: {other:?}"),
    }
}

#[tokio::test]
async fn too_small_receive_buffer_rejects_without_consuming_the_datagram() {
    let (mut receiver, _, _, mut sender) = pair().await;
    sender
        .send(SocketAddr::V4(receiver.local_addr()), b"retained")
        .await
        .unwrap();
    let mut short = vec![0; MAX_INBOUND_DATAGRAM_BYTES - 1];
    assert!(matches!(
        receiver.receive(&mut short).await,
        Err(TokioIpv4UdpError::ReceiveBufferTooSmall { actual, minimum })
            if actual == MAX_INBOUND_DATAGRAM_BYTES - 1 && minimum == MAX_INBOUND_DATAGRAM_BYTES
    ));
    let mut full = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
    let received = receive_bounded(&mut receiver, &mut full).await;
    assert_eq!(&full[..received.length], b"retained");
}

#[tokio::test]
async fn cancelled_receive_is_reusable_and_consumes_nothing() {
    let (mut receiver, _, _, mut sender) = pair().await;
    let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
    let mut receive = Box::pin(receiver.receive(&mut buffer));
    assert!(poll_once(receive.as_mut()).is_pending());
    drop(receive);
    sender
        .send(SocketAddr::V4(receiver.local_addr()), b"after-cancel")
        .await
        .unwrap();
    let received = receive_bounded(&mut receiver, &mut buffer).await;
    assert_eq!(&buffer[..received.length], b"after-cancel");
}

#[tokio::test]
async fn biased_pre_ready_send_cancellation_is_reusable_and_sends_nothing() {
    let (mut receiver, _, _, mut sender) = pair().await;
    let destination = SocketAddr::V4(receiver.local_addr());
    tokio::select! {
        biased;
        () = std::future::ready(()) => {}
        result = sender.send(destination, b"cancelled") => {
            panic!("biased pre-ready cancellation polled the send branch: {result:?}")
        }
    }
    let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.receive(&mut buffer))
            .await
            .is_err(),
        "the cancelled send must place no datagram on the peer socket"
    );
    sender.send(destination, b"settled").await.unwrap();
    let received = receive_bounded(&mut receiver, &mut buffer).await;
    assert_eq!(&buffer[..received.length], b"settled");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.receive(&mut buffer))
            .await
            .is_err(),
        "cancelling or completing one send must not leave a duplicate datagram"
    );
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn bounded_real_client_supervisor_ping_round_trip_cleans_registry() {
    let (server_receiver, server_sender) = TokioIpv4UdpTransport::bind_loopback()
        .await
        .unwrap()
        .into_parts();
    let (client_receiver, client_sender) = TokioIpv4UdpTransport::bind_loopback()
        .await
        .unwrap()
        .into_parts();
    let registry = TransactionRegistry::new(Issuer(VecDeque::from([TransactionId::from(*b"P1")])));
    let table = NodeTable::new(id(9));
    let server_addr = server_sender.local_addr();
    let client_addr = client_sender.local_addr();
    let mut supervisor = PingFindNodeSupervisor::new(
        server_receiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        server_sender,
        &table,
    );
    let mut client_sender = client_sender;
    let client = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(2));
    let mut client_receive =
        bitmagnet_dht::ReceiveDispatcher::new(client_receiver, registry.clone());
    let (supervisor_exit, client_result, receive_result) =
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                supervisor.drive_batch(NonZeroU8::new(1).unwrap(), pending()),
                client.ping(&mut client_sender, SocketAddr::V4(server_addr)),
                client_receive.receive_one(),
            )
        })
        .await
        .expect("bounded real-loopback client/supervisor exchange timed out");
    assert!(
        matches!(supervisor_exit, PingFindNodeSupervisorExit::BudgetExhausted { completed } if completed.len() == 1)
    );
    assert_eq!(client_result.unwrap().id, id(9));
    assert!(matches!(
        receive_result.unwrap(),
        bitmagnet_dht::ReceiveDispatchOutcome::Response {
            source,
            delivery: bitmagnet_dht::DeliveryOutcome::Delivered,
        } if source == SocketAddr::V4(server_addr)
    ));
    assert_eq!(client_sender.local_addr(), client_addr);
    assert_eq!(registry.pending_count(), 0);
}
