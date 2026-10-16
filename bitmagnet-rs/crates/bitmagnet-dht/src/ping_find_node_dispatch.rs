use std::net::SocketAddr;

use crate::{
    ByteString, KrpcError, KrpcMessage, MessageReturn, NodeTable, PingFindNodeError,
    PingFindNodeResponder, WireError,
};

const SERVER_ERROR: i64 = 202;

/// One fully composed response envelope and its exact reply destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PingFindNodeReply {
    pub destination: SocketAddr,
    pub message: KrpcMessage,
}

impl PingFindNodeReply {
    pub fn wire(&self) -> Result<Vec<u8>, WireError> {
        self.message.encode()
    }
}

/// The partial dispatch result. Local responder failures retain their cause
/// while exposing only Go's generic server-error envelope to the peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PingFindNodeDispatchOutcome {
    Reply(PingFindNodeReply),
    LocalFailure {
        reply: PingFindNodeReply,
        cause: PingFindNodeError,
    },
}

/// Pure dispatch and envelope composition for the two owned methods.
pub struct PingFindNodeDispatcher<'a> {
    responder: PingFindNodeResponder<'a>,
}

impl<'a> PingFindNodeDispatcher<'a> {
    #[must_use]
    pub const fn new(table: &'a NodeTable) -> Self {
        Self {
            responder: PingFindNodeResponder::new(table),
        }
    }

    /// Dispatch only exact raw `ping` and `find_node` methods.
    ///
    /// The caller retains the request, so `None` can be passed to a future
    /// router. Source addresses and transaction bytes are echoed without
    /// normalization. Envelope type and unrelated request fields are ignored.
    #[must_use]
    pub fn dispatch(
        &self,
        source: SocketAddr,
        request: &KrpcMessage,
    ) -> Option<PingFindNodeDispatchOutcome> {
        match self.responder.respond(request)? {
            Ok(response) => Some(PingFindNodeDispatchOutcome::Reply(reply(
                source,
                request,
                Some(response),
                None,
            ))),
            Err(PingFindNodeError::Protocol(error)) => Some(PingFindNodeDispatchOutcome::Reply(
                reply(source, request, None, Some(error)),
            )),
            Err(cause @ PingFindNodeError::NativeIpv6Node(_)) => {
                Some(PingFindNodeDispatchOutcome::LocalFailure {
                    reply: reply(source, request, None, Some(generic_server_error())),
                    cause,
                })
            }
        }
    }
}

fn reply(
    destination: SocketAddr,
    request: &KrpcMessage,
    response: Option<MessageReturn>,
    error: Option<KrpcError>,
) -> PingFindNodeReply {
    PingFindNodeReply {
        destination,
        message: KrpcMessage {
            transaction_id: request.transaction_id.clone(),
            message_type: ByteString::new(b"r".to_vec()),
            query: ByteString::default(),
            args: None,
            response,
            error,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        },
    }
}

fn generic_server_error() -> KrpcError {
    KrpcError {
        code: SERVER_ERROR,
        message: ByteString::new(b"server error".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddrV6};

    use super::*;
    use crate::{Id20, MessageArgs, RoutingNode, RoutingPutResult};

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

    fn request(method: &[u8], transaction_id: &[u8]) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new(transaction_id.to_vec()),
            message_type: ByteString::new(b"not-q".to_vec()),
            query: ByteString::new(method.to_vec()),
            args: Some(args(None)),
            response: Some(MessageReturn {
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
            error: Some(KrpcError {
                code: 999,
                message: ByteString::new(b"request-only".to_vec()),
            }),
            observed_addr: None,
            read_only: true,
            client_id: ByteString::new(b"client".to_vec()),
        }
    }

    #[test]
    fn ownership_precedes_arguments_and_non_owned_requests_are_untouched() {
        let table = NodeTable::new(Id20::ZERO);
        let dispatcher = PingFindNodeDispatcher::new(&table);
        for method in [b"".as_slice(), b"PING", b"get_peers", &[0, 255]] {
            let mut query = request(method, b"raw");
            query.args = None;
            assert_eq!(
                dispatcher.dispatch("127.0.0.1:1".parse().unwrap(), &query),
                None
            );
        }
        assert_eq!(table.count(), 0);
    }

    #[test]
    fn reply_echoes_tid_and_source_and_clears_request_only_fields() {
        let table = NodeTable::new(Id20::ZERO);
        let dispatcher = PingFindNodeDispatcher::new(&table);
        let source = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 17, 4));
        let reply = match dispatcher.dispatch(source, &request(b"ping", &[0, 255, 1])) {
            Some(PingFindNodeDispatchOutcome::Reply(reply)) => reply,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(reply.destination, source);
        assert_eq!(reply.message.transaction_id.as_bytes(), &[0, 255, 1]);
        assert_eq!(reply.message.message_type.as_bytes(), b"r");
        assert!(reply.message.query.is_empty());
        assert!(reply.message.args.is_none());
        assert!(reply.message.error.is_none());
        assert!(reply.message.response.is_some());
        assert!(reply.message.observed_addr.is_none());
        assert!(!reply.message.read_only);
        assert!(reply.message.client_id.is_empty());
        assert!(reply.wire().is_ok());
    }

    #[test]
    fn native_ipv6_is_a_generic_peer_error_with_a_retained_local_cause() {
        let mut table = NodeTable::new(Id20::ZERO);
        let id = Id20::from_hex("0000000000000000000000000000000000000001").unwrap();
        let node = RoutingNode {
            id,
            addr: SocketAddr::V6(SocketAddrV6::new(
                "2001:db8::1".parse().unwrap(),
                6881,
                0,
                0,
            )),
        };
        assert_eq!(table.put(node), RoutingPutResult::Accepted);
        let mut query = request(b"find_node", b"N1");
        query.args = Some(args(Some(id)));
        let outcome = PingFindNodeDispatcher::new(&table)
            .dispatch("192.0.2.1:1".parse().unwrap(), &query)
            .unwrap();
        let PingFindNodeDispatchOutcome::LocalFailure { reply, cause } = outcome else {
            panic!("expected local failure")
        };
        assert_eq!(cause, PingFindNodeError::NativeIpv6Node(node));
        assert!(reply.message.response.is_none());
        assert_eq!(reply.message.error, Some(generic_server_error()));
        assert!(reply.wire().is_ok());
        assert_eq!(table.closest(id), vec![node]);
    }
}
