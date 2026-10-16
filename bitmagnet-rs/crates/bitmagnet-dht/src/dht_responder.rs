use std::fmt;
use std::net::{IpAddr, SocketAddr, SocketAddrV6};

use crate::announce_token::AnnounceToken;
use crate::{
    ByteString, CompactAddr, CompactNode, Id20, KTable, KTableCommand, KTableHashPeer,
    KTableLookup, KrpcError, KrpcMessage, MessageArgs, MessageReturn, RoutingNode,
};

const PROTOCOL_ERROR: i64 = 203;
const METHOD_UNKNOWN: i64 = 204;

/// A protocol response error or a local projection failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DhtResponderError {
    Protocol(KrpcError),
    /// Go later panics while encoding native IPv6 nodes in its compact-IPv4
    /// response field. Rust stops at this typed boundary instead.
    NativeIpv6Node(RoutingNode),
}

impl fmt::Display for DhtResponderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => write!(
                formatter,
                "KRPC error {}: {}",
                error.code,
                String::from_utf8_lossy(error.message.as_bytes())
            ),
            Self::NativeIpv6Node(node) => write!(
                formatter,
                "native IPv6 node cannot be projected into compact IPv4 response: {node:?}"
            ),
        }
    }
}

impl std::error::Error for DhtResponderError {}

/// Responder-specific projection of Go's hash-or-closest-nodes query.
///
/// Implementations preserve their own ordering and duplicates. The production
/// [`KTable`] implementation exposes its deliberately normalized snapshots;
/// parity backends can expose exact scripted Go results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DhtResponderLookup {
    Found { peers: Vec<KTableHashPeer> },
    ClosestNodes(Vec<RoutingNode>),
}

/// Responder-specific projection of Go's `SampleHashesAndNodes` query.
///
/// `total_hashes` remains signed because the injected Go table interface can
/// return any native 64-bit `int`, independent of production KTable bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtResponderSample {
    pub hashes: Vec<Id20>,
    pub nodes: Vec<RoutingNode>,
    pub total_hashes: i64,
}

/// The synchronous KTable surface consumed by the pure responder.
///
/// This mirrors Go's injected `ktable.Table` boundary without exposing the
/// rest of either implementation. Methods may be called concurrently through
/// cloned responders and must not re-enter the responder.
pub trait DhtResponderTable: Send + Sync {
    fn origin(&self) -> Id20;
    fn closest_nodes(&self, id: Id20) -> Vec<RoutingNode>;
    fn get_hash_or_closest_nodes(&self, id: Id20) -> DhtResponderLookup;
    fn batch_command(&self, commands: &[KTableCommand]);
    fn sample_hashes_and_nodes(&self) -> DhtResponderSample;
}

impl DhtResponderTable for KTable {
    fn origin(&self) -> Id20 {
        KTable::origin(self)
    }

    fn closest_nodes(&self, id: Id20) -> Vec<RoutingNode> {
        KTable::closest_nodes(self, id)
            .into_iter()
            .map(|node| node.routing_node())
            .collect()
    }

    fn get_hash_or_closest_nodes(&self, id: Id20) -> DhtResponderLookup {
        match KTable::get_hash_or_closest_nodes(self, id) {
            KTableLookup::Found(hash) => DhtResponderLookup::Found { peers: hash.peers },
            KTableLookup::ClosestNodes(nodes) => DhtResponderLookup::ClosestNodes(
                nodes.into_iter().map(|node| node.routing_node()).collect(),
            ),
        }
    }

    fn batch_command(&self, commands: &[KTableCommand]) {
        KTable::batch_command(self, commands);
    }

    fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
        let sample = KTable::sample_hashes_and_nodes(self);
        DhtResponderSample {
            hashes: sample.hashes.into_iter().map(|hash| hash.id).collect(),
            nodes: sample
                .nodes
                .into_iter()
                .map(|node| node.routing_node())
                .collect(),
            total_hashes: sample.total_hashes as i64,
        }
    }
}

/// Pure, cloneable responder for the deployed BEP-5 and BEP-51 query surface.
///
/// The production responder owns a clone of the shared KTable; generic parity
/// backends mirror Go's injected table interface. It performs no I/O, async
/// work, rate limiting, discovery notification, or response-envelope routing.
#[derive(Clone)]
pub struct DhtResponder<T = KTable> {
    table: T,
    local_id: Id20,
    announce_token: AnnounceToken,
    sample_infohashes_interval: i64,
}

impl DhtResponder<KTable> {
    /// Construct a production responder with a fresh process-lifetime secret.
    pub fn new(table: &KTable, sample_infohashes_interval: i64) -> Result<Self, getrandom::Error> {
        let local_id = table.origin();
        let mut token_secret = [0; 20];
        getrandom::fill(&mut token_secret)?;
        Ok(Self::from_parts(
            table.clone(),
            local_id,
            token_secret,
            sample_infohashes_interval,
        ))
    }
}

impl<T: DhtResponderTable> DhtResponder<T> {
    /// Construct a deterministic responder for parity tests and embedding.
    #[must_use]
    pub fn with_token_secret(
        table: T,
        token_secret: [u8; 20],
        sample_infohashes_interval: i64,
    ) -> Self {
        let local_id = table.origin();
        Self::from_parts(table, local_id, token_secret, sample_infohashes_interval)
    }

    fn from_parts(
        table: T,
        local_id: Id20,
        token_secret: [u8; 20],
        sample_infohashes_interval: i64,
    ) -> Self {
        Self {
            table,
            local_id,
            announce_token: AnnounceToken::new(token_secret),
            sample_infohashes_interval,
        }
    }

    /// Respond to all five deployed query methods or return Go's protocol
    /// error for an unknown raw method.
    ///
    /// Argument presence is checked before method dispatch, so every method,
    /// including an unknown one, returns `missing arguments` when `a` is absent.
    pub fn respond(
        &self,
        source: SocketAddr,
        message: &KrpcMessage,
    ) -> Result<MessageReturn, DhtResponderError> {
        let args = message.args.as_ref().ok_or_else(missing_arguments)?;
        match message.query.as_bytes() {
            b"ping" => Ok(empty_return(self.local_id)),
            b"find_node" => self.respond_find_node(args),
            b"get_peers" => self.respond_get_peers(source, args),
            b"announce_peer" => self.respond_announce_peer(source, args),
            b"sample_infohashes" => self.respond_sample_infohashes(),
            _ => Err(method_unknown()),
        }
    }

    fn respond_find_node(&self, args: &MessageArgs) -> Result<MessageReturn, DhtResponderError> {
        let target = required_id(args.target)?;
        let nodes = project_nodes(self.table.closest_nodes(target))?;
        Ok(MessageReturn {
            nodes,
            ..empty_return(self.local_id)
        })
    }

    fn respond_get_peers(
        &self,
        source: SocketAddr,
        args: &MessageArgs,
    ) -> Result<MessageReturn, DhtResponderError> {
        let info_hash = required_id(args.info_hash)?;
        let mut response = empty_return(self.local_id);
        match self.table.get_hash_or_closest_nodes(info_hash) {
            DhtResponderLookup::Found { peers } => {
                response.values = Some(
                    peers
                        .into_iter()
                        .map(|peer| compact_addr(peer.addr))
                        .collect(),
                );
            }
            DhtResponderLookup::ClosestNodes(nodes) => {
                response.nodes = project_nodes(nodes)?;
            }
        }
        response.token = Some(
            self.announce_token
                .issue(self.local_id, info_hash, args.id, source),
        );
        Ok(response)
    }

    fn respond_announce_peer(
        &self,
        source: SocketAddr,
        args: &MessageArgs,
    ) -> Result<MessageReturn, DhtResponderError> {
        let info_hash = required_id(args.info_hash)?;
        let expected_token = self
            .announce_token
            .issue(self.local_id, info_hash, args.id, source);
        if args.token != expected_token {
            return Err(invalid_token());
        }

        let port = if args.implied_port {
            source.port()
        } else {
            args.port.map_or(source.port(), |port| port as u16)
        };
        self.table.batch_command(&[KTableCommand::PutHash {
            id: info_hash,
            peers: vec![KTableHashPeer {
                addr: source_with_port(source, port),
            }],
        }]);
        Ok(empty_return(self.local_id))
    }

    fn respond_sample_infohashes(&self) -> Result<MessageReturn, DhtResponderError> {
        let sample = self.table.sample_hashes_and_nodes();
        let nodes = project_nodes(sample.nodes)?;
        Ok(MessageReturn {
            nodes,
            interval: Some(self.sample_infohashes_interval),
            num: Some(sample.total_hashes),
            samples: Some(sample.hashes),
            ..empty_return(self.local_id)
        })
    }
}

fn required_id(id: Option<Id20>) -> Result<Id20, DhtResponderError> {
    id.filter(|id| *id != Id20::ZERO)
        .ok_or_else(missing_arguments)
}

fn project_nodes(nodes: Vec<RoutingNode>) -> Result<Option<Vec<CompactNode>>, DhtResponderError> {
    if nodes.is_empty() {
        return Ok(None);
    }
    nodes
        .into_iter()
        .map(compact_ipv4_node)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn compact_ipv4_node(node: RoutingNode) -> Result<CompactNode, DhtResponderError> {
    let ip = match node.addr {
        SocketAddr::V4(addr) => IpAddr::V4(*addr.ip()),
        SocketAddr::V6(addr) => match addr.ip().to_ipv4_mapped() {
            Some(ip) => IpAddr::V4(ip),
            None => return Err(DhtResponderError::NativeIpv6Node(node)),
        },
    };
    Ok(CompactNode {
        id: node.id,
        addr: CompactAddr {
            ip,
            port: node.addr.port(),
        },
    })
}

fn compact_addr(addr: SocketAddr) -> CompactAddr {
    CompactAddr {
        ip: addr.ip(),
        port: addr.port(),
    }
}

fn source_with_port(source: SocketAddr, port: u16) -> SocketAddr {
    match source {
        SocketAddr::V4(source) => SocketAddr::new(IpAddr::V4(*source.ip()), port),
        SocketAddr::V6(source) => {
            SocketAddr::V6(SocketAddrV6::new(*source.ip(), port, 0, source.scope_id()))
        }
    }
}

fn missing_arguments() -> DhtResponderError {
    protocol_error(PROTOCOL_ERROR, b"missing arguments")
}

fn invalid_token() -> DhtResponderError {
    protocol_error(PROTOCOL_ERROR, b"invalid token")
}

fn method_unknown() -> DhtResponderError {
    protocol_error(METHOD_UNKNOWN, b"method Unknown")
}

fn protocol_error(code: i64, message: &[u8]) -> DhtResponderError {
    DhtResponderError::Protocol(KrpcError {
        code,
        message: ByteString::new(message.to_vec()),
    })
}

fn empty_return(id: Id20) -> MessageReturn {
    MessageReturn {
        id,
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

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;
    use crate::RoutingPutResult;

    const SECRET: [u8; 20] = [0x11; 20];

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
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

    fn message(method: &[u8], args: Option<MessageArgs>) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new([1, 2]),
            message_type: ByteString::new(b"q".to_vec()),
            query: ByteString::new(method.to_vec()),
            args,
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
    }

    fn responder(table: &KTable, interval: i64) -> DhtResponder {
        DhtResponder::with_token_secret(table.clone(), SECRET, interval)
    }

    fn source() -> SocketAddr {
        "192.0.2.1:6881".parse().unwrap()
    }

    #[test]
    fn args_presence_precedes_dispatch_and_errors_match_go() {
        let table = KTable::new(id(1));
        let responder = responder(&table, 10);
        for method in [
            b"ping".as_slice(),
            b"find_node",
            b"get_peers",
            b"announce_peer",
            b"sample_infohashes",
            b"unknown",
        ] {
            assert_eq!(
                responder.respond(source(), &message(method, None)),
                Err(missing_arguments())
            );
        }
        for method in [b"".as_slice(), b"unknown", b"PING", &[0, 255]] {
            assert_eq!(
                responder.respond(source(), &message(method, Some(args()))),
                Err(method_unknown())
            );
        }
        assert_eq!(
            responder
                .respond(source(), &message(b"ping", Some(args())))
                .unwrap(),
            empty_return(id(1))
        );
        assert_eq!(
            missing_arguments().to_string(),
            "KRPC error 203: missing arguments"
        );
        assert_eq!(
            method_unknown().to_string(),
            "KRPC error 204: method Unknown"
        );
        assert_eq!(invalid_token().to_string(), "KRPC error 203: invalid token");
    }

    #[test]
    fn production_constructor_owns_a_clone_of_the_shared_table() {
        let table = KTable::new(id(1));
        let responder = DhtResponder::new(&table, 10).unwrap();
        let node = RoutingNode {
            id: id(3),
            addr: "192.0.2.3:3003".parse().unwrap(),
        };
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
        let mut find_args = args();
        find_args.target = Some(id(3));
        assert_eq!(
            responder
                .respond(source(), &message(b"find_node", Some(find_args)))
                .unwrap()
                .nodes
                .unwrap()[0]
                .id,
            id(3)
        );
    }

    #[test]
    fn find_node_validates_target_and_hardens_native_ipv6_projection() {
        let table = KTable::new(id(1));
        let responder = responder(&table, 10);
        assert_eq!(
            responder.respond(source(), &message(b"find_node", Some(args()))),
            Err(missing_arguments())
        );

        let native = RoutingNode {
            id: id(3),
            addr: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 3),
        };
        assert_eq!(table.put_node(native), RoutingPutResult::Accepted);
        let mut find_args = args();
        find_args.target = Some(id(3));
        assert_eq!(
            responder.respond(source(), &message(b"find_node", Some(find_args))),
            Err(DhtResponderError::NativeIpv6Node(native))
        );
    }

    #[test]
    fn get_peers_distinguishes_found_empty_and_ignores_scrape() {
        let table = KTable::new(id(1));
        let node = RoutingNode {
            id: id(3),
            addr: "198.51.100.3:3003".parse().unwrap(),
        };
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
        let responder = responder(&table, 10);
        let mut get_args = args();
        get_args.info_hash = Some(id(4));
        get_args.scrape = 1;
        let miss = responder
            .respond(source(), &message(b"get_peers", Some(get_args.clone())))
            .unwrap();
        assert_eq!(miss.nodes.unwrap()[0].id, id(3));
        assert_eq!(miss.values, None);
        assert!(miss.token.is_some());
        assert_eq!(miss.seeders_bloom, None);
        assert_eq!(miss.peers_bloom, None);

        assert_eq!(table.put_hash(id(4), &[]), RoutingPutResult::Accepted);
        get_args.scrape = -1;
        let found = responder
            .respond(source(), &message(b"get_peers", Some(get_args)))
            .unwrap();
        assert_eq!(found.nodes, None);
        assert_eq!(found.values, Some(Vec::new()));
        assert_eq!(found.token, miss.token);
        assert_eq!(found.seeders_bloom, None);
        assert_eq!(found.peers_bloom, None);
    }

    #[test]
    fn announce_validation_and_go_port_rules_control_one_hash_put() {
        let table = KTable::new(id(1));
        let responder = responder(&table, 10);
        let mut announce_args = args();
        announce_args.token = ByteString::new(b"not-a-token".to_vec());
        assert_eq!(
            responder.respond(
                source(),
                &message(b"announce_peer", Some(announce_args.clone())),
            ),
            Err(missing_arguments())
        );
        assert_eq!(table.hash_count(), 0);

        announce_args.info_hash = Some(id(4));
        assert_eq!(
            responder.respond(
                source(),
                &message(b"announce_peer", Some(announce_args.clone())),
            ),
            Err(invalid_token())
        );
        assert_eq!(table.hash_count(), 0);

        let mut get_args = args();
        get_args.info_hash = Some(id(4));
        announce_args.token = responder
            .respond(source(), &message(b"get_peers", Some(get_args)))
            .unwrap()
            .token
            .unwrap();
        announce_args.port = Some(-1);
        assert_eq!(
            responder
                .respond(
                    source(),
                    &message(b"announce_peer", Some(announce_args.clone())),
                )
                .unwrap(),
            empty_return(id(1))
        );
        assert_eq!(table.hash(id(4)).unwrap().peers[0].addr.port(), 65535);

        announce_args.port = Some(65_536);
        responder
            .respond(
                source(),
                &message(b"announce_peer", Some(announce_args.clone())),
            )
            .unwrap();
        assert_eq!(table.hash(id(4)).unwrap().peers[0].addr.port(), 0);

        announce_args.implied_port = true;
        announce_args.port = Some(1234);
        responder
            .respond(source(), &message(b"announce_peer", Some(announce_args)))
            .unwrap();
        assert_eq!(table.hash(id(4)).unwrap().peers[0].addr.port(), 6881);
    }

    #[test]
    fn announce_ignores_void_batch_rejection_and_still_succeeds() {
        let table = KTable::new(id(1));
        let responder = responder(&table, 10);
        // The routing tree rejects its own origin ID.
        let rejected_hash = id(1);
        let mut get_args = args();
        get_args.info_hash = Some(rejected_hash);
        let token = responder
            .respond(source(), &message(b"get_peers", Some(get_args)))
            .unwrap()
            .token
            .unwrap();
        let mut announce_args = args();
        announce_args.info_hash = Some(rejected_hash);
        announce_args.token = token;
        assert_eq!(
            responder
                .respond(source(), &message(b"announce_peer", Some(announce_args)),)
                .unwrap(),
            empty_return(id(1))
        );
        assert_eq!(table.hash_count(), 0);
        assert_eq!(table.hash(rejected_hash), None);
    }

    #[test]
    fn scoped_announce_preserves_scope_zeroes_flowinfo_and_defaults_port() {
        let table = KTable::new(id(1));
        let responder = responder(&table, 10);
        let scoped = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6882, 99, 7));
        let mut get_args = args();
        get_args.info_hash = Some(id(4));
        let token = responder
            .respond(scoped, &message(b"get_peers", Some(get_args)))
            .unwrap()
            .token
            .unwrap();
        let mut announce_args = args();
        announce_args.info_hash = Some(id(4));
        announce_args.token = token;
        responder
            .respond(scoped, &message(b"announce_peer", Some(announce_args)))
            .unwrap();
        let stored = table.hash(id(4)).unwrap().peers[0].addr;
        assert_eq!(stored.port(), 6882);
        let SocketAddr::V6(stored) = stored else {
            panic!("scoped source must remain IPv6");
        };
        assert_eq!(stored.flowinfo(), 0);
        assert_eq!(stored.scope_id(), 7);
    }

    #[test]
    fn sample_is_always_present_and_target_is_ignored() {
        let table = KTable::new(id(1));
        let responder = responder(&table, -7);
        let mut sample_args = args();
        sample_args.target = Some(id(9));
        let empty = responder
            .respond(
                source(),
                &message(b"sample_infohashes", Some(sample_args.clone())),
            )
            .unwrap();
        assert_eq!(empty.nodes, None);
        assert_eq!(empty.samples, Some(Vec::new()));
        assert_eq!(empty.num, Some(0));
        assert_eq!(empty.interval, Some(-7));

        assert_eq!(table.put_hash(id(4), &[]), RoutingPutResult::Accepted);
        sample_args.target = Some(Id20::ZERO);
        let populated = responder
            .respond(source(), &message(b"sample_infohashes", Some(sample_args)))
            .unwrap();
        assert_eq!(populated.samples, Some(vec![id(4)]));
        assert_eq!(populated.num, Some(1));
        assert_eq!(populated.interval, Some(-7));
    }

    #[test]
    fn mapped_nodes_project_as_ipv4_and_peer_values_preserve_address_family() {
        let table = KTable::new(id(1));
        let mapped = RoutingNode {
            id: id(3),
            addr: "[::ffff:192.0.2.3]:3003".parse().unwrap(),
        };
        assert_eq!(table.put_node(mapped), RoutingPutResult::Accepted);
        assert_eq!(
            table.put_hash(id(4), &[KTableHashPeer { addr: mapped.addr }],),
            RoutingPutResult::Accepted
        );
        let responder = responder(&table, 10);
        let mut find_args = args();
        find_args.target = Some(id(3));
        assert_eq!(
            responder
                .respond(source(), &message(b"find_node", Some(find_args)))
                .unwrap()
                .nodes
                .unwrap()[0]
                .addr
                .ip,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 3))
        );

        let mut get_args = args();
        get_args.info_hash = Some(id(4));
        assert_eq!(
            responder
                .respond(source(), &message(b"get_peers", Some(get_args)))
                .unwrap()
                .values
                .unwrap()[0]
                .ip,
            mapped.addr.ip()
        );
        assert!(matches!(
            table.get_hash_or_closest_nodes(id(4)),
            KTableLookup::Found(_)
        ));
    }
}
