use std::net::SocketAddr;

use crate::reply::{compose_error, compose_response, generic_server_error};
use crate::{
    DhtDiscoverySender, DhtReply, DhtResponder, DhtResponderError, DhtResponderTable, KTable,
    KrpcMessage, RoutingNode,
};

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

/// Synchronous dispatch and response-envelope composition for all owned DHT methods.
///
/// [`Self::from_responder`] has no side effects beyond the configured responder.
/// [`Self::with_discovery`] additively enables one bounded best-effort offer on
/// each responder success.
#[derive(Clone)]
pub struct DhtDispatcher<T = KTable> {
    responder: DhtResponder<T>,
    discovery: Option<DhtDiscoverySender>,
}

impl<T> DhtDispatcher<T> {
    /// Construct a dispatcher around one already-configured responder.
    #[must_use]
    pub const fn from_responder(responder: DhtResponder<T>) -> Self {
        Self {
            responder,
            discovery: None,
        }
    }

    /// Add a bounded best-effort node-discovery handoff.
    ///
    /// The handoff is attempted only after an exact responder success. Queue
    /// pressure or receiver shutdown never changes the reply outcome.
    #[must_use]
    pub fn with_discovery(mut self, discovery: DhtDiscoverySender) -> Self {
        self.discovery = Some(discovery);
        self
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
            Ok(response) => {
                if let Some(discovery) = &self.discovery {
                    let args = request
                        .args
                        .as_ref()
                        .expect("a successful DHT response requires request arguments");
                    let _offer = discovery.offer(RoutingNode {
                        id: args.id,
                        addr: source,
                    });
                }
                DhtDispatchOutcome::Reply(compose_response(source, request, response))
            }
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
    use std::future::Future;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;
    use crate::{
        dht_discovery_channel, ByteString, CryptoTransactionIdIssuer, DatagramReceiver,
        DatagramSender, DhtDiscoveryOffer, DhtDiscoveryStats, DhtDriver, DhtDriverError,
        DhtResponderLookup, DhtResponderSample, DhtSendError, Id20, KTableCommand, KTableHashPeer,
        KrpcError, MessageArgs, MessageReturn, ReceivedDatagram, RoutingNode, TransactionRegistry,
    };

    #[derive(Clone)]
    struct Table {
        node: Option<RoutingNode>,
    }

    struct OneReceiver {
        source: SocketAddr,
        wire: Option<Vec<u8>>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct SendFailure;

    struct FailingSender;

    impl DatagramReceiver for OneReceiver {
        type Error = std::convert::Infallible;

        fn receive<'a>(
            &'a mut self,
            buffer: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>>
        {
            let source = self.source;
            let wire = self.wire.take().expect("one scripted receive");
            Box::pin(async move {
                buffer[..wire.len()].copy_from_slice(&wire);
                Ok(ReceivedDatagram {
                    length: wire.len(),
                    source,
                })
            })
        }
    }

    impl DatagramSender for FailingSender {
        type Error = SendFailure;

        fn send<'a>(
            &'a mut self,
            _destination: SocketAddr,
            _datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Err(SendFailure) })
        }
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
        let (discovery, mut discovered) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: Some(node) },
            [0; 20],
            300,
        ))
        .with_discovery(discovery.clone());
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
        assert_eq!(discovery.stats(), DhtDiscoveryStats::default());
        assert_eq!(
            discovered.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
    }

    #[test]
    fn successful_dispatch_offers_exact_node_without_queue_or_receiver_backpressure() {
        let (discovery, mut discovered) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: None },
            [0; 20],
            300,
        ))
        .with_discovery(discovery.clone());
        let source = "[fe80::1%7]:6881".parse().unwrap();
        let mut first_args = args(None);
        first_args.id = Id20::from_hex("0000000000000000000000000000000000000001").unwrap();

        assert!(matches!(
            dispatcher.dispatch(source, &request(b"ping", Some(first_args))),
            DhtDispatchOutcome::Reply(DhtReply { .. })
        ));
        assert!(matches!(
            dispatcher.dispatch(source, &request(b"ping", Some(args(None)))),
            DhtDispatchOutcome::Reply(DhtReply { .. })
        ));
        assert_eq!(
            discovered.try_recv().unwrap(),
            RoutingNode {
                id: Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
                addr: source,
            }
        );
        assert_eq!(
            discovery.stats(),
            DhtDiscoveryStats {
                offered: 2,
                queued: 1,
                full_dropped: 1,
                receiver_closed_dropped: 0,
            }
        );

        drop(discovered);
        assert!(matches!(
            dispatcher.dispatch(source, &request(b"ping", Some(args(None)))),
            DhtDispatchOutcome::Reply(DhtReply { .. })
        ));
        assert_eq!(
            discovery.stats(),
            DhtDiscoveryStats {
                offered: 3,
                queued: 1,
                full_dropped: 1,
                receiver_closed_dropped: 1,
            }
        );
        assert_eq!(
            discovery.offer(RoutingNode {
                id: Id20::ZERO,
                addr: source,
            }),
            DhtDiscoveryOffer::ReceiverClosed
        );
    }

    #[test]
    fn concurrent_dispatcher_clones_queue_every_success_without_drops() {
        const WORKERS: u8 = 16;
        let (discovery, mut discovered) = dht_discovery_channel(
            NonZeroUsize::new(usize::from(WORKERS)).expect("nonzero workers"),
        );
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: None },
            [0; 20],
            300,
        ))
        .with_discovery(discovery.clone());
        let barrier = Arc::new(Barrier::new(usize::from(WORKERS)));
        let handles = (1..=WORKERS)
            .map(|value| {
                let dispatcher = dispatcher.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let source: SocketAddr =
                        format!("192.0.2.{value}:{}", 6_880 + u16::from(value))
                            .parse()
                            .unwrap();
                    let mut request_args = args(None);
                    let mut bytes = [0_u8; 20];
                    bytes[19] = value;
                    request_args.id = Id20::from_slice(&bytes).unwrap();
                    barrier.wait();
                    assert!(matches!(
                        dispatcher.dispatch(source, &request(b"ping", Some(request_args))),
                        DhtDispatchOutcome::Reply(DhtReply { .. })
                    ));
                    RoutingNode {
                        id: Id20::from_slice(&bytes).unwrap(),
                        addr: source,
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut expected = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let mut actual = (0..WORKERS)
            .map(|_| discovered.try_recv().expect("one event per worker"))
            .collect::<Vec<_>>();
        expected.sort_by_key(|node| node.id);
        actual.sort_by_key(|node| node.id);
        assert_eq!(actual, expected);
        assert_eq!(
            discovery.stats(),
            DhtDiscoveryStats {
                offered: u64::from(WORKERS),
                queued: u64::from(WORKERS),
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[test]
    fn receiver_closes_only_after_the_last_dispatcher_clone_drops() {
        let (discovery, mut discovered) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: None },
            [0; 20],
            300,
        ))
        .with_discovery(discovery);
        let retained = dispatcher.clone();
        let source = "192.0.2.1:6881".parse().unwrap();
        assert!(matches!(
            dispatcher.dispatch(source, &request(b"ping", Some(args(None)))),
            DhtDispatchOutcome::Reply(DhtReply { .. })
        ));

        drop(dispatcher);
        assert_eq!(discovered.try_recv().unwrap().addr, source);
        assert_eq!(
            discovered.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        drop(retained);
        assert_eq!(
            discovered.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn discovery_survives_exact_reply_transport_failure() {
        let (discovery, mut discovered) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            Table { node: None },
            [0; 20],
            300,
        ))
        .with_discovery(discovery.clone());
        let source = "192.0.2.1:6881".parse().unwrap();
        let mut request_args = args(None);
        request_args.id = Id20::from_hex("0000000000000000000000000000000000000042").unwrap();
        let mut request = request(b"ping", Some(request_args));
        request.message_type = ByteString::new(b"q".to_vec());
        request.response = None;
        request.error = None;
        let mut driver = DhtDriver::from_dispatcher(
            OneReceiver {
                source,
                wire: Some(request.encode().unwrap()),
            },
            TransactionRegistry::<CryptoTransactionIdIssuer>::default(),
            FailingSender,
            dispatcher,
        );

        let Err(DhtDriverError::Send { prepared, error }) = driver.drive_one().await else {
            panic!("expected exact reply transport failure")
        };
        assert!(matches!(*prepared, DhtDispatchOutcome::Reply(_)));
        assert!(matches!(error, DhtSendError::Transport(SendFailure)));
        assert_eq!(
            discovered.try_recv().unwrap(),
            RoutingNode {
                id: Id20::from_hex("0000000000000000000000000000000000000042").unwrap(),
                addr: source,
            }
        );
        assert_eq!(
            discovery.stats(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }
}
