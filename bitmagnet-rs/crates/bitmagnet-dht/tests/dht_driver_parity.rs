//! Full-DHT one-step composition, routing, effect, and lifecycle gates.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bitmagnet_dht::{
    ByteString, DatagramReceiver, DatagramSender, DeliveryOutcome, DhtDispatchOutcome, DhtDriver,
    DhtDriverError, DhtDriverOutcome, DhtReply, DhtResponder, DhtResponderLookup,
    DhtResponderSample, DhtResponderTable, DhtSendError, Id20, KTable, KTableCommand,
    KTableHashPeer, KrpcError, KrpcMessage, MessageArgs, MessageReturn, PingFindNodeDriverError,
    PingFindNodeDriverOutcome, ReceiveDispatchError, ReceiveDispatchOutcome, ReceivedDatagram,
    RoutingNode, TransactionId, TransactionIdIssuer, TransactionIdSourceError, TransactionRegistry,
    TransactionWaitOutcome, MAX_INBOUND_DATAGRAM_BYTES,
};
use serde::Deserialize;

const TOKEN_SECRET: [u8; 20] = [0x5a; 20];

const RUNTIME_BRIDGE_FIXTURE_IDS: [&str; 12] = [
    "ping_success_empty_tid_mixed_fields",
    "find_node_populated_mapped_source",
    "get_peers_found_values_token",
    "get_peers_miss_nodes_token",
    "announce_peer_valid_mutates_before_send",
    "sample_infohashes_populated_scoped_source",
    "unknown_method_204",
    "missing_args_precedes_unknown_203",
    "unsorted_duplicate_query_decode",
    "announce_send_transport_failure_mutation_survives",
    "receive_transport_error_panics",
    "overreported_length_panics",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgeFixture {
    id: String,
    subsystem: String,
    runtime: BridgeRuntime,
    input: BridgeInput,
    expected: BridgeExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeRuntime {
    int_bits: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeInput {
    wire_hex: String,
    source: BridgeAddr,
    config: BridgeConfig,
    table: BridgeTableScript,
    socket: BridgeSocketScript,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeConfig {
    node_id: Id20,
    token_secret_hex: String,
    sample_info_hashes_interval: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeSocketScript {
    receive_kind: String,
    send_kind: String,
    reported_length: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeExpected {
    classification: String,
    go_terminal: String,
    rust_terminal: String,
    receive_calls: usize,
    responder_calls: usize,
    responder_input_exact: bool,
    send_calls: usize,
    destination_present: bool,
    destination: BridgeAddr,
    wire_present: bool,
    wire_hex: String,
    continuation_receive_entered: bool,
    send_after_responder_return: bool,
    send_failure_logged: bool,
    send_failure_identity_exact: bool,
    receive_panic_retains_transport: bool,
    panic_class: String,
    table_calls: Vec<BridgeTableCall>,
    state: BridgeExpectedState,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BridgeExpectedState {
    before: Vec<BridgePutHash>,
    #[serde(rename = "atSend")]
    at_send: Vec<BridgePutHash>,
    after: Vec<BridgePutHash>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeTableScript {
    closest_nodes: Vec<BridgeNode>,
    lookup_found: bool,
    lookup_hash_id: String,
    lookup_peers: Vec<BridgeAddr>,
    lookup_closest_nodes: Vec<BridgeNode>,
    sample_hashes: Vec<Id20>,
    sample_nodes: Vec<BridgeNode>,
    sample_total_hashes: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BridgeAddr {
    ip: String,
    port: u16,
    scope: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BridgeNode {
    id: Id20,
    addr: BridgeAddr,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeTableCall {
    method: String,
    id: String,
    command_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgePutHash {
    id: Id20,
    peers: Vec<BridgeAddr>,
    options_count: usize,
}

#[derive(Clone, Debug)]
struct Sentinel(Arc<()>);

impl Display for Sentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("full driver sentinel")
    }
}

impl Error for Sentinel {}

struct Issuer(VecDeque<TransactionId>);

impl TransactionIdIssuer for Issuer {
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

#[derive(Default)]
struct Observations {
    events: Vec<&'static str>,
    receive_calls: usize,
    send_calls: usize,
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
    send_active: bool,
    send_completions: usize,
    send_cancellations: usize,
    bridge_puts_at_send: Vec<BridgePutHash>,
}

struct QueueReceiver {
    packets: Arc<Mutex<VecDeque<Result<Packet, Sentinel>>>>,
    observations: Arc<Mutex<Observations>>,
}

impl DatagramReceiver for QueueReceiver {
    type Error = Sentinel;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        let packet = self
            .packets
            .lock()
            .unwrap()
            .pop_front()
            .expect("bounded driver receive script");
        {
            let mut observations = self.observations.lock().unwrap();
            observations.events.push("receive");
            observations.receive_calls += 1;
        }
        Box::pin(async move {
            let packet = packet?;
            let copied = packet.wire.len().min(buffer.len());
            buffer[..copied].copy_from_slice(&packet.wire[..copied]);
            Ok(ReceivedDatagram {
                length: packet.reported.unwrap_or(packet.wire.len()),
                source: packet.source,
            })
        })
    }
}

enum SendAction {
    Complete(Result<(), Sentinel>),
    Pending,
    Gate(Arc<AtomicBool>),
    PanicConstruction(&'static str),
    PanicPoll(&'static str),
}

struct QueueSender {
    actions: Arc<Mutex<VecDeque<SendAction>>>,
    observations: Arc<Mutex<Observations>>,
    bridge_table: Option<Arc<Mutex<BridgeObserved>>>,
}

struct ScriptedSendFuture {
    action: SendAction,
    observations: Arc<Mutex<Observations>>,
    completed: bool,
}

impl Future for ScriptedSendFuture {
    type Output = Result<(), Sentinel>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let outcome = match &self.action {
            SendAction::Complete(result) => Poll::Ready(result.clone()),
            SendAction::Pending => Poll::Pending,
            SendAction::Gate(gate) if gate.load(Ordering::SeqCst) => Poll::Ready(Ok(())),
            SendAction::Gate(_) => Poll::Pending,
            SendAction::PanicPoll(message) => panic!("{message}"),
            SendAction::PanicConstruction(_) => unreachable!("construction panic has no future"),
        };
        if outcome.is_ready() {
            self.completed = true;
            let mut observations = self.observations.lock().unwrap();
            observations.send_active = false;
            observations.send_completions += 1;
        }
        outcome
    }
}

impl Drop for ScriptedSendFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut observations = self.observations.lock().unwrap();
        observations.send_active = false;
        observations.send_cancellations += 1;
    }
}

impl DatagramSender for QueueSender {
    type Error = Sentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let action = self
            .actions
            .lock()
            .unwrap()
            .pop_front()
            .expect("bounded driver send script");
        {
            let bridge_puts_at_send = self
                .bridge_table
                .as_ref()
                .map(|table| table.lock().unwrap().put_hashes.clone())
                .unwrap_or_default();
            let mut observations = self.observations.lock().unwrap();
            assert!(
                !observations.send_active,
                "cancelled sender was not reusable"
            );
            observations.events.push("send");
            observations.send_calls += 1;
            observations.destinations.push(destination);
            observations.wires.push(datagram.to_vec());
            observations.bridge_puts_at_send = bridge_puts_at_send;
        }
        if let SendAction::PanicConstruction(message) = action {
            panic!("{message}");
        }
        self.observations.lock().unwrap().send_active = true;
        Box::pin(ScriptedSendFuture {
            action,
            observations: Arc::clone(&self.observations),
            completed: false,
        })
    }
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

fn source() -> SocketAddr {
    "192.0.2.1:6881".parse().unwrap()
}

fn args() -> MessageArgs {
    MessageArgs {
        id: id(2),
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

fn message(
    transaction_id: &[u8],
    message_type: &[u8],
    method: &[u8],
    args: Option<MessageArgs>,
) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(transaction_id),
        message_type: ByteString::new(message_type),
        query: ByteString::new(method),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn empty_return(local_id: Id20) -> MessageReturn {
    MessageReturn {
        id: local_id,
        nodes: None,
        nodes6: None,
        token: None,
        values: None,
        interval: None,
        num: None,
        samples: None,
        seeders_bloom: None,
        peers_bloom: None,
    }
}

fn packet(message: KrpcMessage) -> Packet {
    Packet {
        wire: message.encode().unwrap(),
        source: source(),
        reported: None,
    }
}

fn driver<T: DhtResponderTable>(
    packets: Arc<Mutex<VecDeque<Result<Packet, Sentinel>>>>,
    actions: Arc<Mutex<VecDeque<SendAction>>>,
    observations: Arc<Mutex<Observations>>,
    registry: TransactionRegistry<Issuer>,
    table: T,
) -> DhtDriver<QueueReceiver, QueueSender, Issuer, T> {
    let dispatcher = bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        table,
        TOKEN_SECRET,
        300,
    ));
    DhtDriver::from_dispatcher(
        QueueReceiver {
            packets,
            observations: Arc::clone(&observations),
        },
        registry,
        QueueSender {
            actions,
            observations,
            bridge_table: None,
        },
        dispatcher,
    )
}

fn sent(outcome: DhtDriverOutcome) -> DhtDispatchOutcome {
    let DhtDriverOutcome::Sent(outcome) = outcome else {
        panic!("query did not produce a sent outcome")
    };
    *outcome
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[derive(Default)]
struct BridgeObserved {
    calls: Vec<BridgeTableCall>,
    put_hashes: Vec<BridgePutHash>,
}

#[derive(Clone)]
struct BridgeTable {
    origin: Id20,
    script: BridgeTableScript,
    observed: Arc<Mutex<BridgeObserved>>,
}

impl BridgeTable {
    fn new(origin: Id20, script: BridgeTableScript) -> Self {
        Self {
            origin,
            script,
            observed: Arc::new(Mutex::new(BridgeObserved::default())),
        }
    }

    fn calls(&self) -> Vec<BridgeTableCall> {
        self.observed.lock().unwrap().calls.clone()
    }

    fn put_hashes(&self) -> Vec<BridgePutHash> {
        self.observed.lock().unwrap().put_hashes.clone()
    }

    fn record_call(&self, method: &str, id: String, command_count: usize) {
        self.observed.lock().unwrap().calls.push(BridgeTableCall {
            method: method.to_owned(),
            id,
            command_count,
        });
    }
}

impl DhtResponderTable for BridgeTable {
    fn origin(&self) -> Id20 {
        self.origin
    }

    fn closest_nodes(&self, id: Id20) -> Vec<RoutingNode> {
        self.record_call("GetClosestNodes", id.to_hex(), 0);
        self.script
            .closest_nodes
            .iter()
            .map(bridge_routing_node)
            .collect()
    }

    fn get_hash_or_closest_nodes(&self, id: Id20) -> DhtResponderLookup {
        self.record_call("GetHashOrClosestNodes", id.to_hex(), 0);
        if self.script.lookup_found {
            assert_eq!(
                Id20::from_hex(&self.script.lookup_hash_id).expect("bridge lookup hash ID"),
                id,
                "bridge lookup hash identity"
            );
            DhtResponderLookup::Found {
                peers: self
                    .script
                    .lookup_peers
                    .iter()
                    .map(|addr| KTableHashPeer {
                        addr: bridge_socket_addr(addr),
                    })
                    .collect(),
            }
        } else {
            assert!(self.script.lookup_hash_id.is_empty());
            assert!(self.script.lookup_peers.is_empty());
            DhtResponderLookup::ClosestNodes(
                self.script
                    .lookup_closest_nodes
                    .iter()
                    .map(bridge_routing_node)
                    .collect(),
            )
        }
    }

    fn batch_command(&self, commands: &[KTableCommand]) {
        self.record_call("BatchCommand", String::new(), commands.len());
        let mut observed = self.observed.lock().unwrap();
        for command in commands {
            let KTableCommand::PutHash { id, peers } = command else {
                panic!("bridge responder emitted a non-PutHash command: {command:?}")
            };
            observed.put_hashes.push(BridgePutHash {
                id: *id,
                peers: peers
                    .iter()
                    .map(|peer| bridge_fixture_addr(peer.addr))
                    .collect(),
                options_count: 0,
            });
        }
    }

    fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
        self.record_call("SampleHashesAndNodes", String::new(), 0);
        DhtResponderSample {
            hashes: self.script.sample_hashes.clone(),
            nodes: self
                .script
                .sample_nodes
                .iter()
                .map(bridge_routing_node)
                .collect(),
            total_hashes: self.script.sample_total_hashes,
        }
    }
}

fn bridge_fixtures() -> Vec<BridgeFixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/dht_runtime_bridge.jsonl");
    BufReader::new(File::open(path).expect("checked DHT runtime bridge fixture"))
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn bridge_socket_addr(value: &BridgeAddr) -> SocketAddr {
    let ip: IpAddr = value.ip.parse().expect("nonempty bridge IP address");
    match ip {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0, "IPv4 bridge address scope");
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn bridge_fixture_addr(value: SocketAddr) -> BridgeAddr {
    match value {
        SocketAddr::V4(value) => BridgeAddr {
            ip: value.ip().to_string(),
            port: value.port(),
            scope: 0,
        },
        SocketAddr::V6(value) => BridgeAddr {
            ip: value.ip().to_string(),
            port: value.port(),
            scope: value.scope_id(),
        },
    }
}

fn bridge_routing_node(value: &BridgeNode) -> RoutingNode {
    RoutingNode {
        id: value.id,
        addr: bridge_socket_addr(&value.addr),
    }
}

fn assert_bridge_addr(value: &BridgeAddr, allow_absent: bool, fixture_id: &str) {
    let BridgeAddr { ip, port, scope } = value;
    if ip.is_empty() {
        assert!(allow_absent, "{fixture_id}: empty present address");
        assert_eq!((*port, *scope), (0, 0), "{fixture_id}: absent address");
        return;
    }
    let parsed: IpAddr = ip.parse().expect("bridge fixture IP address");
    if parsed.is_ipv4() {
        assert_eq!(*scope, 0, "{fixture_id}: IPv4 scope");
    }
    let projected = bridge_fixture_addr(bridge_socket_addr(value));
    assert_eq!(&projected, value, "{fixture_id}: address projection");
}

fn assert_bridge_node(value: &BridgeNode, fixture_id: &str) {
    let BridgeNode { id, addr } = value;
    assert_eq!(
        Id20::from_hex(&id.to_hex()).unwrap(),
        *id,
        "{fixture_id}: node ID"
    );
    assert_bridge_addr(addr, false, fixture_id);
}

fn assert_bridge_call(value: &BridgeTableCall, fixture_id: &str) {
    let BridgeTableCall {
        method,
        id,
        command_count,
    } = value;
    match method.as_str() {
        "GetClosestNodes" | "GetHashOrClosestNodes" => {
            Id20::from_hex(id).expect("bridge table-call ID");
            assert_eq!(*command_count, 0, "{fixture_id}: lookup command count");
        }
        "BatchCommand" => {
            assert!(id.is_empty(), "{fixture_id}: batch ID");
            assert_eq!(*command_count, 1, "{fixture_id}: batch command count");
        }
        "SampleHashesAndNodes" => {
            assert!(id.is_empty(), "{fixture_id}: sample ID");
            assert_eq!(*command_count, 0, "{fixture_id}: sample command count");
        }
        other => panic!("{fixture_id}: unknown table method {other}"),
    }
}

fn assert_bridge_put(value: &BridgePutHash, fixture_id: &str) {
    let BridgePutHash {
        id,
        peers,
        options_count,
    } = value;
    assert_eq!(
        Id20::from_hex(&id.to_hex()).unwrap(),
        *id,
        "{fixture_id}: put ID"
    );
    assert_eq!(*options_count, 0, "{fixture_id}: put options");
    for peer in peers {
        assert_bridge_addr(peer, false, fixture_id);
    }
}

fn assert_bridge_fixture_contract(fixture: &BridgeFixture) {
    let BridgeFixture {
        id,
        subsystem,
        runtime,
        input,
        expected,
    } = fixture;
    assert_eq!(subsystem, "dht_runtime_bridge", "{id}: subsystem");
    let BridgeRuntime { int_bits } = runtime;
    assert_eq!(*int_bits, 64, "{id}: deterministic Go int width");

    let BridgeInput {
        wire_hex,
        source,
        config,
        table,
        socket,
    } = input;
    let wire = hex::decode(wire_hex).expect("bridge input wire hex");
    assert_bridge_addr(source, false, id);
    let BridgeConfig {
        node_id,
        token_secret_hex,
        sample_info_hashes_interval,
    } = config;
    assert_eq!(node_id.to_hex(), "00112233445566778899aabbccddeeff10203040");
    assert_eq!(
        hex::decode(token_secret_hex).unwrap().len(),
        20,
        "{id}: token secret"
    );
    assert_eq!(*sample_info_hashes_interval, 10, "{id}: sample interval");
    let BridgeTableScript {
        closest_nodes,
        lookup_found,
        lookup_hash_id,
        lookup_peers,
        lookup_closest_nodes,
        sample_hashes,
        sample_nodes,
        sample_total_hashes,
    } = table;
    for node in closest_nodes
        .iter()
        .chain(lookup_closest_nodes)
        .chain(sample_nodes)
    {
        assert_bridge_node(node, id);
    }
    for peer in lookup_peers {
        assert_bridge_addr(peer, false, id);
    }
    for hash in sample_hashes {
        assert_eq!(
            Id20::from_hex(&hash.to_hex()).unwrap(),
            *hash,
            "{id}: sample hash"
        );
    }
    if *lookup_found {
        Id20::from_hex(lookup_hash_id).expect("found bridge lookup hash ID");
        assert!(lookup_closest_nodes.is_empty(), "{id}: found lookup nodes");
    } else {
        assert!(lookup_hash_id.is_empty(), "{id}: missing lookup hash ID");
        assert!(lookup_peers.is_empty(), "{id}: missing lookup peers");
    }
    assert!(*sample_total_hashes >= 0, "{id}: sample total");

    let BridgeSocketScript {
        receive_kind,
        send_kind,
        reported_length,
    } = socket;
    let datagram = receive_kind == "datagram";
    assert!(
        datagram || matches!(receive_kind.as_str(), "error" | "overreported"),
        "{id}: receive kind"
    );
    assert!(
        matches!(send_kind.as_str(), "success" | "error"),
        "{id}: send kind"
    );
    assert_eq!(
        *reported_length,
        if receive_kind == "overreported" {
            MAX_INBOUND_DATAGRAM_BYTES + 1
        } else {
            0
        },
        "{id}: reported length"
    );
    assert_eq!(wire.is_empty(), !datagram, "{id}: input wire presence");

    let BridgeExpected {
        classification,
        go_terminal,
        rust_terminal,
        receive_calls,
        responder_calls,
        responder_input_exact,
        send_calls,
        destination_present,
        destination,
        wire_present,
        wire_hex: expected_wire_hex,
        continuation_receive_entered,
        send_after_responder_return,
        send_failure_logged,
        send_failure_identity_exact,
        receive_panic_retains_transport,
        panic_class,
        table_calls,
        state,
    } = expected;
    let expected_classification = match id.as_str() {
        "unknown_method_204" => "protocol_204",
        "missing_args_precedes_unknown_203" | "unsorted_duplicate_query_decode" => "protocol_203",
        "receive_transport_error_panics" => "receive_transport",
        "overreported_length_panics" => "overreported_length",
        _ => "success",
    };
    assert_eq!(
        classification, expected_classification,
        "{id}: classification"
    );
    let expected_go_terminal = if !datagram {
        "panicked"
    } else if send_kind == "error" {
        "send_failure_swallowed"
    } else {
        "reply_sent"
    };
    assert_eq!(go_terminal, expected_go_terminal, "{id}: Go terminal");
    let expected_rust_terminal = match receive_kind.as_str() {
        "error" => "failed_receive",
        "overreported" => "failed_overreported_length",
        _ if send_kind == "error" => "failed_send",
        _ => "reply_sent",
    };
    assert_eq!(rust_terminal, expected_rust_terminal, "{id}: Rust terminal");
    assert_eq!(
        *receive_calls,
        if datagram { 2 } else { 1 },
        "{id}: Go receive calls"
    );
    assert_eq!(
        *responder_calls,
        usize::from(datagram),
        "{id}: responder calls"
    );
    assert_eq!(
        *responder_input_exact, datagram,
        "{id}: responder input identity"
    );
    assert_eq!(*send_calls, usize::from(datagram), "{id}: Go send calls");
    assert_eq!(*destination_present, datagram, "{id}: destination presence");
    assert_bridge_addr(destination, !datagram, id);
    if datagram {
        assert_eq!(destination, source, "{id}: destination");
    }
    assert_eq!(*wire_present, datagram, "{id}: output wire presence");
    assert_eq!(
        hex::decode(expected_wire_hex).unwrap().is_empty(),
        !datagram,
        "{id}: output wire"
    );
    assert_eq!(
        *continuation_receive_entered, datagram,
        "{id}: Go continuation receive"
    );
    assert_eq!(
        *send_after_responder_return, datagram,
        "{id}: Go send order"
    );
    let send_failure = id == "announce_send_transport_failure_mutation_survives";
    assert_eq!(*send_failure_logged, send_failure, "{id}: Go send log");
    assert_eq!(
        *send_failure_identity_exact, send_failure,
        "{id}: Go send identity"
    );
    let receive_failure = id == "receive_transport_error_panics";
    assert_eq!(
        *receive_panic_retains_transport, receive_failure,
        "{id}: Go receive identity"
    );
    assert_eq!(
        panic_class,
        match receive_kind.as_str() {
            "error" => "receive_transport",
            "overreported" => "overreported_length",
            _ => "",
        },
        "{id}: panic class"
    );
    for call in table_calls {
        assert_bridge_call(call, id);
    }
    let BridgeExpectedState {
        before,
        at_send,
        after,
    } = state;
    assert!(before.is_empty(), "{id}: initial state");
    assert_eq!(at_send, after, "{id}: Go send-time state");
    for put in before.iter().chain(at_send).chain(after) {
        assert_bridge_put(put, id);
    }
    let mutates = matches!(
        id.as_str(),
        "announce_peer_valid_mutates_before_send"
            | "announce_send_transport_failure_mutation_survives"
    );
    assert_eq!(
        after.len(),
        usize::from(mutates),
        "{id}: mutation cardinality"
    );
}

#[tokio::test]
async fn real_go_runtime_bridge_fixture_replays_exact_full_driver_boundaries() {
    let fixtures = bridge_fixtures();
    assert_eq!(fixtures.len(), RUNTIME_BRIDGE_FIXTURE_IDS.len());
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        RUNTIME_BRIDGE_FIXTURE_IDS
    );

    for fixture in fixtures {
        assert_bridge_fixture_contract(&fixture);
        let BridgeFixture {
            id,
            subsystem: _,
            runtime: _,
            input,
            expected,
        } = fixture;
        let BridgeInput {
            wire_hex,
            source,
            config,
            table: table_script,
            socket,
        } = input;
        let BridgeConfig {
            node_id,
            token_secret_hex,
            sample_info_hashes_interval,
        } = config;
        let token_secret: [u8; 20] = hex::decode(token_secret_hex)
            .unwrap()
            .try_into()
            .expect("20-byte bridge token secret");
        let source = bridge_socket_addr(&source);
        let table = BridgeTable::new(node_id, table_script);
        assert_eq!(
            table.put_hashes(),
            expected.state.before,
            "{id}: Rust before state"
        );
        let receive_sentinel = Sentinel(Arc::new(()));
        let send_sentinel = Sentinel(Arc::new(()));
        let packet = match socket.receive_kind.as_str() {
            "datagram" => Ok(Packet {
                wire: hex::decode(wire_hex).unwrap(),
                source,
                reported: None,
            }),
            "error" => Err(receive_sentinel.clone()),
            "overreported" => Ok(Packet {
                wire: hex::decode(wire_hex).unwrap(),
                source,
                reported: Some(socket.reported_length),
            }),
            other => panic!("{id}: unsupported bridge receive kind {other}"),
        };
        let packets = Arc::new(Mutex::new(VecDeque::from([packet])));
        let actions = Arc::new(Mutex::new(VecDeque::new()));
        if socket.receive_kind == "datagram" {
            actions.lock().unwrap().push_back(SendAction::Complete(
                if socket.send_kind == "error" {
                    Err(send_sentinel.clone())
                } else {
                    Ok(())
                },
            ));
        }
        let observations = Arc::new(Mutex::new(Observations::default()));
        let dispatcher =
            bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
                table.clone(),
                token_secret,
                sample_info_hashes_interval,
            ));
        let mut driver = DhtDriver::from_dispatcher(
            QueueReceiver {
                packets,
                observations: Arc::clone(&observations),
            },
            TransactionRegistry::new(Issuer(VecDeque::new())),
            QueueSender {
                actions,
                observations: Arc::clone(&observations),
                bridge_table: Some(Arc::clone(&table.observed)),
            },
            dispatcher,
        );

        match expected.rust_terminal.as_str() {
            "reply_sent" => {
                let DhtDriverOutcome::Sent(outcome) = driver.drive_one().await.unwrap() else {
                    panic!("{id}: expected sent outcome")
                };
                assert!(
                    matches!(*outcome, DhtDispatchOutcome::Reply(_)),
                    "{id}: reply outcome"
                );
            }
            "failed_send" => {
                let DhtDriverError::Send { prepared, error } =
                    driver.drive_one().await.unwrap_err()
                else {
                    panic!("{id}: expected send error")
                };
                assert!(
                    matches!(*prepared, DhtDispatchOutcome::Reply(_)),
                    "{id}: prepared reply"
                );
                let DhtSendError::Transport(actual) = error else {
                    panic!("{id}: expected transport send error")
                };
                assert!(
                    Arc::ptr_eq(&actual.0, &send_sentinel.0),
                    "{id}: send identity"
                );
            }
            "failed_receive" => {
                let DhtDriverError::Receive(ReceiveDispatchError::Transport(actual)) =
                    driver.drive_one().await.unwrap_err()
                else {
                    panic!("{id}: expected receive transport error")
                };
                assert!(
                    Arc::ptr_eq(&actual.0, &receive_sentinel.0),
                    "{id}: receive identity"
                );
            }
            "failed_overreported_length" => assert!(
                matches!(
                    driver.drive_one().await,
                    Err(DhtDriverError::Receive(ReceiveDispatchError::OverreportedLength {
                        reported,
                        capacity: MAX_INBOUND_DATAGRAM_BYTES,
                    })) if reported == socket.reported_length
                ),
                "{id}: overreported boundary"
            ),
            other => panic!("{id}: unsupported Rust terminal {other}"),
        }

        let observations = observations.lock().unwrap();
        assert_eq!(
            observations.receive_calls, 1,
            "{id}: bounded Rust receive calls"
        );
        assert_eq!(
            observations.send_calls, expected.send_calls,
            "{id}: Rust send calls"
        );
        assert_eq!(
            observations.destinations,
            if expected.destination_present {
                vec![bridge_socket_addr(&expected.destination)]
            } else {
                Vec::new()
            },
            "{id}: exact destination"
        );
        assert_eq!(
            observations.wires,
            if expected.wire_present {
                vec![hex::decode(&expected.wire_hex).unwrap()]
            } else {
                Vec::new()
            },
            "{id}: exact response wire"
        );
        assert_eq!(
            observations.bridge_puts_at_send, expected.state.at_send,
            "{id}: send-time state"
        );
        drop(observations);
        assert_eq!(
            table.calls(),
            expected.table_calls,
            "{id}: table call trace"
        );
        assert_eq!(
            table.put_hashes(),
            expected.state.after,
            "{id}: final table state"
        );
    }
}

#[tokio::test]
async fn all_methods_unknown_and_missing_args_are_owned_but_nonqueries_never_dispatch() {
    let table = KTable::new(id(0x90));
    let info_hash = id(4);
    let packets = Arc::new(Mutex::new(VecDeque::new()));
    let actions = Arc::new(Mutex::new(VecDeque::new()));
    let observations = Arc::new(Mutex::new(Observations::default()));

    let mut find_args = args();
    find_args.target = Some(id(3));
    let mut peer_args = args();
    peer_args.info_hash = Some(info_hash);
    let mut announce_args = peer_args.clone();
    announce_args.token = ByteString::new(b"bad token");
    let queries = [
        message(b"P1", b"q", b"ping", Some(args())),
        message(b"F1", b"q", b"find_node", Some(find_args)),
        message(b"G1", b"q", b"get_peers", Some(peer_args)),
        message(b"A1", b"q", b"announce_peer", Some(announce_args.clone())),
        message(b"S1", b"q", b"sample_infohashes", Some(args())),
        message(b"U1", b"q", b"unknown", Some(args())),
        message(b"M1", b"q", b"ping", None),
    ];
    let query_count = queries.len();
    packets
        .lock()
        .unwrap()
        .extend(queries.into_iter().map(|query| Ok(packet(query))));
    actions
        .lock()
        .unwrap()
        .extend(std::iter::repeat_with(|| SendAction::Complete(Ok(()))).take(query_count));
    let mut driver = driver(
        Arc::clone(&packets),
        Arc::clone(&actions),
        Arc::clone(&observations),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        table.clone(),
    );

    let mut replies = Vec::new();
    for _ in 0..query_count {
        replies.push(sent(driver.drive_one().await.unwrap()));
    }
    let codes = replies
        .iter()
        .map(|outcome| {
            outcome
                .reply()
                .message
                .error
                .as_ref()
                .map(|error| error.code)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        codes,
        [None, None, None, Some(203), None, Some(204), Some(203)]
    );
    assert!(table.hash(info_hash).is_none());

    let mixed_nonquery = |tid: &[u8], kind: &[u8]| {
        let mut value = message(tid, kind, b"announce_peer", Some(announce_args.clone()));
        value.response = Some(empty_return(id(0xee)));
        value.error = Some(KrpcError {
            code: 999,
            message: ByteString::new(b"mixed"),
        });
        value
    };
    packets.lock().unwrap().extend([
        Ok(packet(mixed_nonquery(b"R1", b"r"))),
        Ok(packet(mixed_nonquery(b"E1", b"e"))),
        Ok(packet(mixed_nonquery(b"X1", b"x"))),
        Ok(Packet {
            wire: b"not-bencode".to_vec(),
            source: source(),
            reported: None,
        }),
        Ok(Packet {
            wire: Vec::new(),
            source: source(),
            reported: None,
        }),
    ]);

    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::Response {
            delivery: DeliveryOutcome::UnknownTransaction,
            ..
        })
    ));
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::Error {
            delivery: DeliveryOutcome::UnknownTransaction,
            ..
        })
    ));
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::Ignored { .. })
    ));
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::DecodeRejected { .. })
    ));
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::ZeroLength { .. })
    ));
    assert_eq!(observations.lock().unwrap().send_calls, query_count);
    assert!(table.hash(info_hash).is_none());
}

#[tokio::test]
async fn responses_and_errors_are_delivered_to_the_shared_registry_without_a_send() {
    let tids = VecDeque::from([
        TransactionId::from_slice(b"R1").unwrap(),
        TransactionId::from_slice(b"E1").unwrap(),
    ]);
    let registry = TransactionRegistry::new(Issuer(tids));
    let pending_response = registry
        .register(source(), ByteString::new(b"ping"), args())
        .unwrap()
        .mark_sent();
    let mut response = message(b"R1", b"r", b"", None);
    response.response = Some(empty_return(id(0x90)));

    let packets = Arc::new(Mutex::new(VecDeque::from([Ok(packet(response))])));
    let actions = Arc::new(Mutex::new(VecDeque::new()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut driver = driver(
        Arc::clone(&packets),
        Arc::clone(&actions),
        Arc::clone(&observations),
        registry.clone(),
        KTable::new(id(0x90)),
    );
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::Response {
            delivery: DeliveryOutcome::Delivered,
            ..
        })
    ));
    assert!(matches!(
        pending_response.wait(std::time::Duration::ZERO).await,
        TransactionWaitOutcome::Response { .. }
    ));

    let pending_error = registry
        .register(source(), ByteString::new(b"ping"), args())
        .unwrap()
        .mark_sent();
    let mut error = message(b"E1", b"e", b"", None);
    error.error = Some(KrpcError {
        code: 201,
        message: ByteString::new(b"remote"),
    });
    packets.lock().unwrap().push_back(Ok(packet(error)));
    assert!(matches!(
        driver.drive_one().await.unwrap(),
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::Error {
            delivery: DeliveryOutcome::Delivered,
            ..
        })
    ));
    assert!(matches!(
        pending_error.wait(std::time::Duration::ZERO).await,
        TransactionWaitOutcome::RemoteError { .. }
    ));
    assert_eq!(registry.pending_count(), 0);
    assert_eq!(observations.lock().unwrap().send_calls, 0);
}

#[derive(Clone)]
struct NativeTable {
    origin: Id20,
    node: RoutingNode,
}

impl DhtResponderTable for NativeTable {
    fn origin(&self) -> Id20 {
        self.origin
    }

    fn closest_nodes(&self, _id: Id20) -> Vec<RoutingNode> {
        vec![self.node]
    }

    fn get_hash_or_closest_nodes(&self, _id: Id20) -> DhtResponderLookup {
        DhtResponderLookup::ClosestNodes(vec![self.node])
    }

    fn batch_command(&self, _commands: &[KTableCommand]) {}

    fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
        DhtResponderSample {
            hashes: Vec::new(),
            nodes: vec![self.node],
            total_hashes: 0,
        }
    }
}

#[tokio::test]
async fn receive_send_and_native_local_failures_retain_exact_typed_identity() {
    let receive_sentinel = Sentinel(Arc::new(()));
    let packets = Arc::new(Mutex::new(VecDeque::from([Err(receive_sentinel.clone())])));
    let actions = Arc::new(Mutex::new(VecDeque::new()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut receive_driver = driver(
        packets,
        actions,
        Arc::clone(&observations),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        KTable::new(id(0x90)),
    );
    let DhtDriverError::Receive(ReceiveDispatchError::Transport(actual)) =
        receive_driver.drive_one().await.unwrap_err()
    else {
        panic!("wrong receive error")
    };
    assert!(Arc::ptr_eq(&actual.0, &receive_sentinel.0));
    assert_eq!(observations.lock().unwrap().send_calls, 0);

    let packets = Arc::new(Mutex::new(VecDeque::from([Ok(Packet {
        wire: Vec::new(),
        source: source(),
        reported: Some(MAX_INBOUND_DATAGRAM_BYTES + 1),
    })])));
    let mut overreport_driver = driver(
        packets,
        Arc::new(Mutex::new(VecDeque::new())),
        Arc::new(Mutex::new(Observations::default())),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        KTable::new(id(0x90)),
    );
    assert!(matches!(
        overreport_driver.drive_one().await,
        Err(DhtDriverError::Receive(
            ReceiveDispatchError::OverreportedLength { .. }
        ))
    ));

    let node = RoutingNode {
        id: id(0x41),
        addr: SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x41),
            6881,
            17,
            9,
        )),
    };
    let table = NativeTable {
        origin: id(0x90),
        node,
    };
    let mut find_args = args();
    find_args.target = Some(id(3));
    let query = packet(message(b"N1", b"q", b"find_node", Some(find_args)));

    let success_observations = Arc::new(Mutex::new(Observations::default()));
    let mut success_driver = driver(
        Arc::new(Mutex::new(VecDeque::from([Ok(query.clone())]))),
        Arc::new(Mutex::new(VecDeque::from([SendAction::Complete(Ok(()))]))),
        Arc::clone(&success_observations),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        table.clone(),
    );
    let DhtDispatchOutcome::LocalFailure { cause, reply } =
        sent(success_driver.drive_one().await.unwrap())
    else {
        panic!("native node did not retain local failure")
    };
    assert_eq!(
        cause,
        bitmagnet_dht::DhtResponderError::NativeIpv6Node(node)
    );
    assert_eq!(reply.message.error.as_ref().unwrap().code, 202);
    assert_eq!(
        success_observations.lock().unwrap().destinations,
        [source()]
    );

    let send_sentinel = Sentinel(Arc::new(()));
    let failure_observations = Arc::new(Mutex::new(Observations::default()));
    let mut failure_driver = driver(
        Arc::new(Mutex::new(VecDeque::from([Ok(query)]))),
        Arc::new(Mutex::new(VecDeque::from([SendAction::Complete(Err(
            send_sentinel.clone(),
        ))]))),
        Arc::clone(&failure_observations),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        table,
    );
    let DhtDriverError::Send { prepared, error } = failure_driver.drive_one().await.unwrap_err()
    else {
        panic!("wrong send error")
    };
    let DhtDispatchOutcome::LocalFailure { cause, .. } = *prepared else {
        panic!("send error lost local cause")
    };
    assert_eq!(
        cause,
        bitmagnet_dht::DhtResponderError::NativeIpv6Node(node)
    );
    let DhtSendError::Transport(actual) = error else {
        panic!("wrong nested send error")
    };
    assert!(Arc::ptr_eq(&actual.0, &send_sentinel.0));
    assert_eq!(failure_observations.lock().unwrap().send_calls, 1);
}

fn response_token(observations: &Arc<Mutex<Observations>>) -> ByteString {
    let wire = observations.lock().unwrap().wires.last().unwrap().clone();
    KrpcMessage::decode(&wire)
        .unwrap()
        .response
        .unwrap()
        .token
        .unwrap()
}

#[tokio::test]
async fn announce_mutation_precedes_send_and_survives_failure_backpressure_and_drop() {
    let table = KTable::new(id(0x90));
    let info_hash = id(4);
    let packets = Arc::new(Mutex::new(VecDeque::new()));
    let actions = Arc::new(Mutex::new(VecDeque::from([SendAction::Complete(Ok(()))])));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut get_args = args();
    get_args.info_hash = Some(info_hash);
    packets.lock().unwrap().push_back(Ok(packet(message(
        b"G1",
        b"q",
        b"get_peers",
        Some(get_args),
    ))));
    let mut driver = driver(
        Arc::clone(&packets),
        Arc::clone(&actions),
        Arc::clone(&observations),
        TransactionRegistry::new(Issuer(VecDeque::new())),
        table.clone(),
    );
    sent(driver.drive_one().await.unwrap());
    let token = response_token(&observations);

    let mut announce_args = args();
    announce_args.info_hash = Some(info_hash);
    announce_args.token = token.clone();
    packets.lock().unwrap().push_back(Ok(packet(message(
        b"A1",
        b"q",
        b"announce_peer",
        Some(announce_args.clone()),
    ))));
    let send_sentinel = Sentinel(Arc::new(()));
    actions
        .lock()
        .unwrap()
        .push_back(SendAction::Complete(Err(send_sentinel.clone())));
    let DhtDriverError::Send { error, .. } = driver.drive_one().await.unwrap_err() else {
        panic!("announce send failure was not retained")
    };
    let DhtSendError::Transport(actual) = error else {
        panic!("announce send failure changed type")
    };
    assert!(Arc::ptr_eq(&actual.0, &send_sentinel.0));
    assert_eq!(table.hash(info_hash).unwrap().peers[0].addr, source());

    let gate = Arc::new(AtomicBool::new(false));
    packets.lock().unwrap().push_back(Ok(packet(message(
        b"A2",
        b"q",
        b"announce_peer",
        Some(announce_args.clone()),
    ))));
    actions
        .lock()
        .unwrap()
        .push_back(SendAction::Gate(Arc::clone(&gate)));
    let mut pending = Box::pin(driver.drive_one());
    assert!(poll_once(pending.as_mut()).is_pending());
    assert_eq!(table.hash(info_hash).unwrap().peers[0].addr, source());
    assert_eq!(observations.lock().unwrap().send_calls, 3);
    gate.store(true, Ordering::SeqCst);
    assert!(matches!(
        poll_once(pending.as_mut()),
        Poll::Ready(Ok(DhtDriverOutcome::Sent(_)))
    ));
    drop(pending);

    packets.lock().unwrap().push_back(Ok(packet(message(
        b"A3",
        b"q",
        b"announce_peer",
        Some(announce_args),
    ))));
    actions.lock().unwrap().push_back(SendAction::Pending);
    let mut pending = Box::pin(driver.drive_one());
    assert!(poll_once(pending.as_mut()).is_pending());
    drop(pending);
    let observations = observations.lock().unwrap();
    assert_eq!(observations.send_calls, 4);
    assert_eq!(observations.send_completions, 3);
    assert_eq!(observations.send_cancellations, 1);
    assert!(!observations.send_active);
    drop(observations);
    assert_eq!(table.hash(info_hash).unwrap().peers[0].addr, source());
}

struct OpaqueError;
struct OpaqueReceiver;
struct OpaqueSender;

impl DatagramReceiver for OpaqueReceiver {
    type Error = OpaqueError;

    fn receive<'a>(
        &'a mut self,
        _buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

impl DatagramSender for OpaqueSender {
    type Error = OpaqueError;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

struct NonCloneTable;

impl DhtResponderTable for NonCloneTable {
    fn origin(&self) -> Id20 {
        id(0x90)
    }

    fn closest_nodes(&self, _id: Id20) -> Vec<RoutingNode> {
        Vec::new()
    }

    fn get_hash_or_closest_nodes(&self, _id: Id20) -> DhtResponderLookup {
        DhtResponderLookup::Found { peers: Vec::new() }
    }

    fn batch_command(&self, _commands: &[KTableCommand]) {}

    fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
        DhtResponderSample {
            hashes: Vec::new(),
            nodes: Vec::new(),
            total_hashes: 0,
        }
    }
}

fn full_outcome_exhaustive(outcome: &DhtDriverOutcome) -> &'static str {
    match outcome {
        DhtDriverOutcome::NoReply(_) => "no_reply",
        DhtDriverOutcome::Sent(_) => "sent",
    }
}

fn full_error_exhaustive(error: &DhtDriverError<Sentinel, Sentinel>) -> &'static str {
    match error {
        DhtDriverError::Receive(_) => "receive",
        DhtDriverError::Send {
            prepared: _,
            error: _,
        } => "send",
    }
}

fn legacy_outcome_exhaustive(outcome: &PingFindNodeDriverOutcome) -> &'static str {
    match outcome {
        PingFindNodeDriverOutcome::NoReply(_) => "no_reply",
        PingFindNodeDriverOutcome::Sent(_) => "sent",
    }
}

fn legacy_error_exhaustive(error: &PingFindNodeDriverError<Sentinel, Sentinel>) -> &'static str {
    match error {
        PingFindNodeDriverError::Receive(_) => "receive",
        PingFindNodeDriverError::Send {
            prepared: _,
            error: _,
        } => "send",
    }
}

#[test]
fn public_bounds_errors_and_legacy_enums_remain_explicit() {
    fn assert_error<T: Error>() {}
    assert_error::<DhtDriverError<Sentinel, Sentinel>>();
    assert_error::<PingFindNodeDriverError<Sentinel, Sentinel>>();

    let sentinel = Sentinel(Arc::new(()));
    let receive = DhtDriverError::Receive(ReceiveDispatchError::Transport(sentinel.clone()));
    assert_eq!(full_error_exhaustive(&receive), "receive");
    assert!(receive.source().is_some());

    let send: DhtDriverError<Sentinel, Sentinel> = DhtDriverError::Send {
        prepared: Box::new(DhtDispatchOutcome::Reply(DhtReply {
            destination: source(),
            message: message(b"S1", b"r", b"", None),
        })),
        error: DhtSendError::Transport(sentinel.clone()),
    };
    assert_eq!(full_error_exhaustive(&send), "send");
    assert!(send.source().is_some());
    assert!(send.source().unwrap().source().is_none());

    let dispatcher = bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        NonCloneTable,
        TOKEN_SECRET,
        300,
    ));
    let mut driver = DhtDriver::from_dispatcher(
        OpaqueReceiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        OpaqueSender,
        dispatcher,
    );
    let future = driver.drive_one();
    drop(future);

    let _ = full_outcome_exhaustive;
    let _ = legacy_outcome_exhaustive;
    let _ = legacy_error_exhaustive;
    let _ = IpAddr::V4;
    let _ = KTableHashPeer { addr: source() };
    let _ = SendAction::PanicConstruction("compile-only");
    let _ = SendAction::PanicPoll("compile-only");
}
