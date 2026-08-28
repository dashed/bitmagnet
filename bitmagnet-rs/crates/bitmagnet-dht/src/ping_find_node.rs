use std::net::{IpAddr, SocketAddr};

use crate::{
    ByteString, CompactAddr, CompactNode, Id20, KrpcError, KrpcMessage, MessageReturn, NodeTable,
    RoutingNode,
};

const PROTOCOL_ERROR: i64 = 203;

/// A local failure that must not be converted into a KRPC protocol error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PingFindNodeError {
    Protocol(KrpcError),
    /// Go's responder puts native IPv6 nodes in its compact-IPv4 field and its
    /// later encoder panics. Rust stops at this typed boundary instead.
    NativeIpv6Node(RoutingNode),
}

/// Pure ownership-limited responder for only BEP-5 `ping` and `find_node`.
pub struct PingFindNodeResponder<'a> {
    table: &'a NodeTable,
}

impl<'a> PingFindNodeResponder<'a> {
    #[must_use]
    pub const fn new(table: &'a NodeTable) -> Self {
        Self { table }
    }

    /// Respond only when this seam owns the exact raw method.
    ///
    /// Method ownership is decided before argument validation. A future full
    /// router remains responsible for unknown-method protocol errors.
    #[must_use]
    pub fn respond(
        &self,
        message: &KrpcMessage,
    ) -> Option<Result<MessageReturn, PingFindNodeError>> {
        match message.query.as_bytes() {
            b"ping" => Some(self.respond_ping(message)),
            b"find_node" => Some(self.respond_find_node(message)),
            _ => None,
        }
    }

    fn respond_ping(&self, message: &KrpcMessage) -> Result<MessageReturn, PingFindNodeError> {
        message.args.as_ref().ok_or_else(missing_arguments)?;
        Ok(empty_return(self.table.origin()))
    }

    fn respond_find_node(&self, message: &KrpcMessage) -> Result<MessageReturn, PingFindNodeError> {
        let args = message.args.as_ref().ok_or_else(missing_arguments)?;
        let target = args
            .target
            .filter(|target| *target != Id20::ZERO)
            .ok_or_else(missing_arguments)?;

        let closest = self.table.closest(target);
        let nodes = if closest.is_empty() {
            None
        } else {
            Some(
                closest
                    .into_iter()
                    .map(compact_ipv4_node)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };
        Ok(MessageReturn {
            nodes,
            ..empty_return(self.table.origin())
        })
    }
}

fn compact_ipv4_node(node: RoutingNode) -> Result<CompactNode, PingFindNodeError> {
    let ip = match node.addr {
        SocketAddr::V4(addr) => IpAddr::V4(*addr.ip()),
        SocketAddr::V6(addr) => match addr.ip().to_ipv4_mapped() {
            Some(ip) => IpAddr::V4(ip),
            None => return Err(PingFindNodeError::NativeIpv6Node(node)),
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

fn missing_arguments() -> PingFindNodeError {
    PingFindNodeError::Protocol(KrpcError {
        code: PROTOCOL_ERROR,
        message: ByteString::new(b"missing arguments".to_vec()),
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
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use super::*;
    use crate::{MessageArgs, RoutingPutResult};

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

    fn args(target: Option<Id20>) -> MessageArgs {
        MessageArgs {
            id: Id20::ZERO,
            info_hash: None,
            target,
            token: ByteString::default(),
            port: None,
            implied_port: false,
            want: None,
            no_seed: 0,
            scrape: 0,
        }
    }

    fn id(value: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn non_owned_methods_return_none_before_argument_validation() {
        let table = NodeTable::new(id(9));
        let responder = PingFindNodeResponder::new(&table);
        for method in [b"".as_slice(), b"get_peers", b"PING", &[0, 255]] {
            assert_eq!(responder.respond(&message(method, None)), None);
            assert_eq!(responder.respond(&message(method, Some(args(None)))), None);
        }
        assert_eq!(table.count(), 0);
    }

    #[test]
    fn owned_methods_validate_arguments_without_mutating_the_table() {
        let mut table = NodeTable::new(id(9));
        let node = RoutingNode {
            id: id(1),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        };
        assert_eq!(table.put(node), RoutingPutResult::Accepted);
        let before = table.closest(id(1));
        let responder = PingFindNodeResponder::new(&table);

        assert_eq!(
            responder.respond(&message(b"ping", None)),
            Some(Err(missing_arguments()))
        );
        assert_eq!(
            responder.respond(&message(b"find_node", Some(args(Some(Id20::ZERO))))),
            Some(Err(missing_arguments()))
        );
        let ping = responder
            .respond(&message(b"ping", Some(args(None))))
            .unwrap()
            .unwrap();
        assert_eq!(ping, empty_return(id(9)));
        let find = responder
            .respond(&message(b"find_node", Some(args(Some(id(1))))))
            .unwrap()
            .unwrap();
        assert_eq!(find.nodes.as_ref().unwrap()[0].id, id(1));
        assert_eq!(table.count(), 1);
        assert_eq!(table.closest(id(1)), before);
    }

    #[test]
    fn mapped_ipv4_is_canonical_and_native_ipv6_fails_closed() {
        let mut mapped_table = NodeTable::new(id(9));
        let mapped = RoutingNode {
            id: id(1),
            addr: SocketAddr::V6(SocketAddrV6::new(
                Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(),
                6881,
                7,
                3,
            )),
        };
        assert_eq!(mapped_table.put(mapped), RoutingPutResult::Accepted);
        let mapped_response = PingFindNodeResponder::new(&mapped_table)
            .respond(&message(b"find_node", Some(args(Some(id(1))))))
            .unwrap()
            .unwrap();
        assert_eq!(
            mapped_response.nodes.unwrap()[0].addr,
            CompactAddr {
                ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                port: 6881,
            }
        );

        let mut native_table = NodeTable::new(id(9));
        let native = RoutingNode {
            id: id(2),
            addr: SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 8, 4)),
        };
        assert_eq!(native_table.put(native), RoutingPutResult::Accepted);
        let stored = native_table.closest(id(2))[0];
        assert_eq!(
            stored.addr,
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 4))
        );
        assert_eq!(
            PingFindNodeResponder::new(&native_table)
                .respond(&message(b"find_node", Some(args(Some(id(2)))))),
            Some(Err(PingFindNodeError::NativeIpv6Node(stored)))
        );
    }
}
