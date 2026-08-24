use std::net::SocketAddr;

use crate::reply::{compose_error, compose_response, generic_server_error};
use crate::{DhtReply, DhtResponder, DhtResponderError, DhtResponderTable, KTable, KrpcMessage};

/// A complete DHT dispatch result.
///
/// Local responder failures retain their typed cause while exposing only
/// Go's generic server-error response envelope to the peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DhtDispatchOutcome {
    Reply(DhtReply),
    LocalFailure {
        reply: DhtReply,
        cause: DhtResponderError,
    },
}

impl DhtDispatchOutcome {
    /// Borrow the reply to send for either outcome without discarding a local
    /// failure's cause.
    #[must_use]
    pub const fn reply(&self) -> &DhtReply {
        match self {
            Self::Reply(reply) | Self::LocalFailure { reply, .. } => reply,
        }
    }
}

/// Pure dispatch and response-envelope composition for all owned DHT methods.
#[derive(Clone)]
pub struct DhtDispatcher<T = KTable> {
    responder: DhtResponder<T>,
}

impl<T> DhtDispatcher<T> {
    /// Construct a dispatcher around one already-configured responder.
    #[must_use]
    pub const fn from_responder(responder: DhtResponder<T>) -> Self {
        Self { responder }
    }
}

impl<T: DhtResponderTable> DhtDispatcher<T> {
    /// Dispatch one query already routed to the DHT query path.
    ///
    /// The caller must establish that `request` is a query before calling;
    /// this layer deliberately does not revalidate `y`. Every raw method is
    /// handled: unknown methods receive protocol error 204, so the result is
    /// total under that routing precondition.
    #[must_use]
    pub fn dispatch(&self, source: SocketAddr, request: &KrpcMessage) -> DhtDispatchOutcome {
        match self.responder.respond(source, request) {
            Ok(response) => DhtDispatchOutcome::Reply(compose_response(source, request, response)),
            Err(DhtResponderError::Protocol(error)) => {
                DhtDispatchOutcome::Reply(compose_error(source, request, error))
            }
            Err(cause @ DhtResponderError::NativeIpv6Node(_)) => DhtDispatchOutcome::LocalFailure {
                reply: compose_error(source, request, generic_server_error()),
                cause,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddrV6};

    use super::*;
    use crate::{
        ByteString, DhtResponderLookup, DhtResponderSample, Id20, KTableCommand, KTableHashPeer,
        KrpcError, MessageArgs, MessageReturn, RoutingNode,
    };

    #[derive(Clone)]
    struct Table {
        node: Option<RoutingNode>,
    }

    impl DhtResponderTable for Table {
        fn origin(&self) -> Id20 {
            Id20::ZERO
        }

        fn closest_nodes(&self, _id: Id20) -> Vec<RoutingNode> {
            self.node.into_iter().collect()
        }

        fn get_hash_or_closest_nodes(&self, _id: Id20) -> DhtResponderLookup {
            DhtResponderLookup::Found {
                peers: Vec::<KTableHashPeer>::new(),
            }
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

    fn request(method: &[u8], args: Option<MessageArgs>) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new(vec![0, 255, 1]),
            message_type: ByteString::new(b"already-routed".to_vec()),
            query: ByteString::new(method.to_vec()),
            args,
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

    #[test]
    fn public_outcome_api_composes_a_clean_protocol_error_reply() {
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: None },
            [0; 20],
            300,
        ));
        let source = "192.0.2.1:6881".parse().unwrap();
        let outcome = dispatcher.dispatch(source, &request(b"unknown", Some(args(None))));
        let DhtDispatchOutcome::Reply(reply) = &outcome else {
            panic!("expected protocol reply")
        };
        assert_eq!(outcome.reply(), reply);
        assert_eq!(reply.destination, source);
        assert_eq!(reply.message.transaction_id.as_bytes(), &[0, 255, 1]);
        assert_eq!(reply.message.message_type.as_bytes(), b"r");
        assert!(reply.message.query.is_empty());
        assert!(reply.message.args.is_none());
        assert!(reply.message.response.is_none());
        assert_eq!(reply.message.error.as_ref().unwrap().code, 204);
        assert!(reply.message.observed_addr.is_none());
        assert!(!reply.message.read_only);
        assert!(reply.message.client_id.is_empty());
    }

    #[test]
    fn native_ipv6_becomes_generic_reply_and_retains_exact_cause() {
        let node = RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
            addr: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
                6881,
                0,
                0,
            )),
        };
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: Some(node) },
            [0; 20],
            300,
        ));
        let outcome = dispatcher.dispatch(
            "192.0.2.1:1".parse().unwrap(),
            &request(b"find_node", Some(args(Some(node.id)))),
        );
        let DhtDispatchOutcome::LocalFailure { reply, cause } = outcome else {
            panic!("expected local failure")
        };
        assert_eq!(cause, DhtResponderError::NativeIpv6Node(node));
        assert!(reply.message.response.is_none());
        assert_eq!(reply.message.error.unwrap().code, 202);
    }
}
