use std::net::SocketAddr;
use std::time::Duration;

use crate::{
    register_and_send_query, ByteString, CompactAddr, CompactNode, DatagramSender, Id20, KrpcError,
    KrpcMessage, MessageArgs, MessageReturn, QuerySendError, RoutingNode, ScrapeBloomFilter,
    TransactionIdIssuer, TransactionRegistry, TransactionWaitOutcome,
};

/// The Go-compatible projection of a successful `ping` response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PingResult {
    pub id: Id20,
}

/// The Go-compatible projection of a successful `find_node` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindNodeResult {
    pub id: Id20,
    pub nodes: Vec<RoutingNode>,
}

/// The Go-compatible projection of a successful `get_peers` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetPeersResult {
    pub id: Id20,
    pub values: Vec<SocketAddr>,
    pub nodes: Vec<RoutingNode>,
}

/// The Go-compatible projection of a successful BEP-33 scrape response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetPeersScrapeResult {
    pub id: Id20,
    pub values: Vec<SocketAddr>,
    pub nodes: Vec<RoutingNode>,
    pub peers_bloom: ScrapeBloomFilter,
    pub seeders_bloom: ScrapeBloomFilter,
}

/// The Go-compatible projection of a successful `sample_infohashes` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SampleInfoHashesResult {
    pub id: Id20,
    /// `None` is an absent `samples` field; `Some(empty)` advertises BEP-51.
    pub samples: Option<Vec<Id20>>,
    pub nodes: Vec<RoutingNode>,
    pub num: i64,
    pub interval: i64,
}

/// Typed failures from query registration/send, response waiting, and typed
/// response semantics.
#[derive(Debug, thiserror::Error)]
pub enum DhtClientError<E> {
    /// Registration, encoding, or the exact underlying transport failure.
    #[error("DHT query failed: {0}")]
    QuerySend(
        #[from]
        #[source]
        QuerySendError<E>,
    ),
    /// An address-verified KRPC error envelope from the queried node.
    #[error("remote KRPC error {error:?}")]
    RemoteError {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
        error: KrpcError,
    },
    /// An address-verified `y=r` envelope omitted its return dictionary.
    #[error("return data missing from response")]
    MissingReturnBody {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    /// An address-verified `y=e` envelope omitted its error body.
    #[error("error missing from response")]
    MissingErrorBody {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    /// A successful BEP-33 scrape response omitted one or both filters.
    #[error(
        "missing bloom filter in scrape response (missing peers: {missing_peers}, missing seeders: {missing_seeders})"
    )]
    MissingScrapeBloomFilters {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
        missing_peers: bool,
        missing_seeders: bool,
    },
    /// The configured query timeout elapsed after the datagram send succeeded.
    #[error("query timed out")]
    Timeout,
    /// The shared transaction registry closed while the query was pending.
    #[error("transaction registry closed")]
    RegistryClosed,
}

/// Backward-compatible failures from the original `ping`/`find_node` client.
///
/// This deliberately remains a separate, closed enum so legacy exhaustive
/// matches and error text are unaffected by methods added to [`DhtClient`].
#[derive(Debug, thiserror::Error)]
pub enum PingFindNodeClientError<E> {
    /// Registration, encoding, or the exact underlying transport failure.
    #[error("ping/find-node query failed: {0}")]
    QuerySend(
        #[from]
        #[source]
        QuerySendError<E>,
    ),
    /// An address-verified KRPC error envelope from the queried node.
    #[error("remote KRPC error {error:?}")]
    RemoteError {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
        error: KrpcError,
    },
    /// An address-verified `y=r` envelope omitted its return dictionary.
    #[error("return data missing from response")]
    MissingReturnBody {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    /// An address-verified `y=e` envelope omitted its error body.
    #[error("error missing from response")]
    MissingErrorBody {
        response_source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    /// The configured query timeout elapsed after the datagram send succeeded.
    #[error("query timed out")]
    Timeout,
    /// The shared transaction registry closed while the query was pending.
    #[error("transaction registry closed")]
    RegistryClosed,
}

struct AcceptedQueryResponse {
    response_source: SocketAddr,
    message: Box<KrpcMessage>,
    response: Box<MessageReturn>,
}

/// A typed client for production Go's outbound DHT adapter methods.
///
/// Clones share the transaction registry while retaining the same local ID and
/// post-send query timeout.
pub struct DhtClient<I> {
    local_id: Id20,
    registry: TransactionRegistry<I>,
    query_timeout: Duration,
}

impl<I> Clone for DhtClient<I> {
    fn clone(&self) -> Self {
        Self {
            local_id: self.local_id,
            registry: self.registry.clone(),
            query_timeout: self.query_timeout,
        }
    }
}

impl<I> DhtClient<I>
where
    I: TransactionIdIssuer,
{
    #[must_use]
    pub fn new(local_id: Id20, registry: &TransactionRegistry<I>, query_timeout: Duration) -> Self {
        Self {
            local_id,
            registry: registry.clone(),
            query_timeout,
        }
    }

    /// Send one `ping`, await its response, and project only the responder ID.
    pub async fn ping<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
    ) -> Result<PingResult, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let accepted = self
            .query(
                sender,
                remote,
                ByteString::new(b"ping"),
                self.args(None, None, 0),
            )
            .await?;
        Ok(PingResult {
            id: accepted.response.id,
        })
    }

    /// Send one `find_node` and preserve only ordered `r.nodes` entries.
    pub async fn find_node<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<FindNodeResult, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let accepted = self
            .query(
                sender,
                remote,
                ByteString::new(b"find_node"),
                self.args(None, Some(target), 0),
            )
            .await?;
        let response = *accepted.response;
        Ok(FindNodeResult {
            id: response.id,
            nodes: project_nodes(response.nodes),
        })
    }

    /// Send one `get_peers` query and preserve ordered peer/node entries.
    pub async fn get_peers<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        info_hash: Id20,
    ) -> Result<GetPeersResult, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let accepted = self
            .query(
                sender,
                remote,
                ByteString::new(b"get_peers"),
                self.args(Some(info_hash), None, 0),
            )
            .await?;
        let response = *accepted.response;
        Ok(GetPeersResult {
            id: response.id,
            values: project_values(response.values),
            nodes: project_nodes(response.nodes),
        })
    }

    /// Send one BEP-33 scrape query and require both response bloom filters.
    pub async fn get_peers_scrape<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        info_hash: Id20,
    ) -> Result<GetPeersScrapeResult, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let accepted = self
            .query(
                sender,
                remote,
                ByteString::new(b"get_peers"),
                self.args(Some(info_hash), None, 1),
            )
            .await?;
        let response = *accepted.response;
        let missing_peers = response.peers_bloom.is_none();
        let missing_seeders = response.seeders_bloom.is_none();
        let (peers_bloom, seeders_bloom) = match (response.peers_bloom, response.seeders_bloom) {
            (Some(peers_bloom), Some(seeders_bloom)) => (peers_bloom, seeders_bloom),
            _ => {
                return Err(DhtClientError::MissingScrapeBloomFilters {
                    response_source: accepted.response_source,
                    message: accepted.message,
                    missing_peers,
                    missing_seeders,
                });
            }
        };
        Ok(GetPeersScrapeResult {
            id: response.id,
            values: project_values(response.values),
            nodes: project_nodes(response.nodes),
            peers_bloom,
            seeders_bloom,
        })
    }

    /// Send one BEP-51 query, preserving samples-field presence and signed
    /// counter/interval values.
    pub async fn sample_infohashes<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<SampleInfoHashesResult, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let accepted = self
            .query(
                sender,
                remote,
                ByteString::new(b"sample_infohashes"),
                self.args(None, Some(target), 0),
            )
            .await?;
        let response = *accepted.response;
        Ok(SampleInfoHashesResult {
            id: response.id,
            samples: response.samples,
            nodes: project_nodes(response.nodes),
            num: response.num.unwrap_or_default(),
            interval: response.interval.unwrap_or_default(),
        })
    }

    fn args(&self, info_hash: Option<Id20>, target: Option<Id20>, scrape: i64) -> MessageArgs {
        MessageArgs {
            id: self.local_id,
            info_hash,
            target,
            token: ByteString::default(),
            port: None,
            implied_port: false,
            want: None,
            no_seed: 0,
            scrape,
        }
    }

    async fn query<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        query: ByteString,
        args: MessageArgs,
    ) -> Result<AcceptedQueryResponse, DhtClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let pending = register_and_send_query(&self.registry, sender, remote, query, args)
            .await
            .map_err(DhtClientError::QuerySend)?;

        match pending.wait(self.query_timeout).await {
            TransactionWaitOutcome::Response {
                source,
                message,
                response,
            } => Ok(AcceptedQueryResponse {
                response_source: source,
                message,
                response,
            }),
            TransactionWaitOutcome::RemoteError {
                source,
                message,
                error,
            } => Err(DhtClientError::RemoteError {
                response_source: source,
                message,
                error,
            }),
            TransactionWaitOutcome::MissingReturnBody { source, message } => {
                Err(DhtClientError::MissingReturnBody {
                    response_source: source,
                    message,
                })
            }
            TransactionWaitOutcome::MissingErrorBody { source, message } => {
                Err(DhtClientError::MissingErrorBody {
                    response_source: source,
                    message,
                })
            }
            TransactionWaitOutcome::Timeout => Err(DhtClientError::Timeout),
            TransactionWaitOutcome::RegistryClosed => Err(DhtClientError::RegistryClosed),
            TransactionWaitOutcome::Cancelled => {
                unreachable!("PendingTransaction::wait never returns cancellation")
            }
        }
    }
}

/// The original borrowed two-method client surface.
///
/// This wrapper preserves the legacy lifetime, generic shape, `const` constructor,
/// and borrowed registry ownership. Each operation delegates through a fresh
/// owned [`DhtClient`] handle whose registry clone shares the same state.
pub struct PingFindNodeClient<'a, I> {
    local_id: Id20,
    registry: &'a TransactionRegistry<I>,
    query_timeout: Duration,
}

impl<'a, I> PingFindNodeClient<'a, I>
where
    I: TransactionIdIssuer,
{
    #[must_use]
    pub const fn new(
        local_id: Id20,
        registry: &'a TransactionRegistry<I>,
        query_timeout: Duration,
    ) -> Self {
        Self {
            local_id,
            registry,
            query_timeout,
        }
    }

    /// Send one `ping` through the generalized client core.
    pub async fn ping<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
    ) -> Result<PingResult, PingFindNodeClientError<S::Error>>
    where
        S: DatagramSender,
    {
        self.owned_client()
            .ping(sender, remote)
            .await
            .map_err(map_legacy_client_error)
    }

    /// Send one `find_node` through the generalized client core.
    pub async fn find_node<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<FindNodeResult, PingFindNodeClientError<S::Error>>
    where
        S: DatagramSender,
    {
        self.owned_client()
            .find_node(sender, remote, target)
            .await
            .map_err(map_legacy_client_error)
    }

    fn owned_client(&self) -> DhtClient<I> {
        DhtClient::new(self.local_id, self.registry, self.query_timeout)
    }
}

fn map_legacy_client_error<E>(error: DhtClientError<E>) -> PingFindNodeClientError<E> {
    match error {
        DhtClientError::QuerySend(error) => PingFindNodeClientError::QuerySend(error),
        DhtClientError::RemoteError {
            response_source,
            message,
            error,
        } => PingFindNodeClientError::RemoteError {
            response_source,
            message,
            error,
        },
        DhtClientError::MissingReturnBody {
            response_source,
            message,
        } => PingFindNodeClientError::MissingReturnBody {
            response_source,
            message,
        },
        DhtClientError::MissingErrorBody {
            response_source,
            message,
        } => PingFindNodeClientError::MissingErrorBody {
            response_source,
            message,
        },
        DhtClientError::Timeout => PingFindNodeClientError::Timeout,
        DhtClientError::RegistryClosed => PingFindNodeClientError::RegistryClosed,
        DhtClientError::MissingScrapeBloomFilters { .. } => {
            unreachable!("ping/find-node methods cannot validate scrape bloom filters")
        }
    }
}

fn project_nodes(nodes: Option<Vec<CompactNode>>) -> Vec<RoutingNode> {
    nodes
        .unwrap_or_default()
        .into_iter()
        .map(|node| RoutingNode {
            id: node.id,
            addr: SocketAddr::new(node.addr.ip, node.addr.port),
        })
        .collect()
}

fn project_values(values: Option<Vec<CompactAddr>>) -> Vec<SocketAddr> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|value| SocketAddr::new(value.ip, value.port))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::Future;
    use std::pin::Pin;

    use crate::{DeliveryOutcome, RegisterError, TransactionId, TransactionIdSourceError};

    use super::*;

    struct NonCloneIssuer(u16);

    impl TransactionIdIssuer for NonCloneIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            let value = self.0;
            self.0 = self.0.wrapping_add(1);
            Ok(TransactionId::from(value.to_be_bytes()))
        }
    }

    const fn legacy_const_client<'a>(
        registry: &'a TransactionRegistry<NonCloneIssuer>,
    ) -> PingFindNodeClient<'a, NonCloneIssuer> {
        PingFindNodeClient::new(Id20::ZERO, registry, Duration::ZERO)
    }

    fn legacy_error_variant<E>(error: &PingFindNodeClientError<E>) -> &'static str {
        match error {
            PingFindNodeClientError::QuerySend(_) => "query_send",
            PingFindNodeClientError::RemoteError { .. } => "remote_error",
            PingFindNodeClientError::MissingReturnBody { .. } => "missing_return_body",
            PingFindNodeClientError::MissingErrorBody { .. } => "missing_error_body",
            PingFindNodeClientError::Timeout => "timeout",
            PingFindNodeClientError::RegistryClosed => "registry_closed",
        }
    }

    struct RespondingSender<I> {
        registry: TransactionRegistry<I>,
        source: SocketAddr,
        response: MessageReturn,
        queries: Vec<KrpcMessage>,
    }

    impl<I> DatagramSender for RespondingSender<I>
    where
        I: TransactionIdIssuer,
    {
        type Error = Infallible;

        fn send<'a>(
            &'a mut self,
            _destination: SocketAddr,
            datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            let query = KrpcMessage::decode(datagram).expect("typed client query decodes");
            let response = KrpcMessage {
                transaction_id: query.transaction_id.clone(),
                message_type: ByteString::new(b"r"),
                query: ByteString::default(),
                args: None,
                response: Some(self.response.clone()),
                error: None,
                observed_addr: None,
                read_only: false,
                client_id: ByteString::default(),
            };
            assert_eq!(
                self.registry.deliver(self.source, response),
                DeliveryOutcome::Delivered
            );
            self.queries.push(query);
            Box::pin(async { Ok(()) })
        }
    }

    fn id(last: u8) -> Id20 {
        let mut value = [0; 20];
        value[19] = last;
        Id20::from_slice(&value).expect("twenty bytes")
    }

    fn response(id: Id20) -> MessageReturn {
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

    fn harness(
        response: MessageReturn,
    ) -> (
        DhtClient<NonCloneIssuer>,
        RespondingSender<NonCloneIssuer>,
        SocketAddr,
    ) {
        let registry = TransactionRegistry::new(NonCloneIssuer(1));
        let remote = "192.0.2.1:6881".parse().expect("test address");
        let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
        let sender = RespondingSender {
            registry,
            source: remote,
            response,
            queries: Vec::new(),
        };
        (client, sender, remote)
    }

    #[tokio::test]
    async fn clones_share_a_registry_without_requiring_the_issuer_to_clone() {
        let (client, mut sender, remote) = harness(response(id(2)));
        let clone = client.clone();
        assert_eq!(
            clone
                .ping(&mut sender, remote)
                .await
                .expect("ping response"),
            PingResult { id: id(2) }
        );
        assert_eq!(sender.queries.len(), 1);
    }

    #[tokio::test]
    async fn legacy_wrapper_retains_its_lifetime_const_constructor_and_error_contract() {
        let registry = TransactionRegistry::new(NonCloneIssuer(1));
        let remote = "192.0.2.1:6881".parse().expect("test address");
        let mut sender = RespondingSender {
            registry: registry.clone(),
            source: remote,
            response: response(id(2)),
            queries: Vec::new(),
        };
        let client: PingFindNodeClient<'_, NonCloneIssuer> = legacy_const_client(&registry);
        assert_eq!(
            client
                .ping(&mut sender, remote)
                .await
                .expect("ping response"),
            PingResult { id: id(2) }
        );

        let error = PingFindNodeClientError::<Infallible>::QuerySend(QuerySendError::Register(
            RegisterError::RegistryClosed,
        ));
        assert_eq!(legacy_error_variant(&error), "query_send");
        assert_eq!(
            error.to_string(),
            "ping/find-node query failed: could not register KRPC query: transaction registry is closed"
        );
        assert_eq!(
            Error::source(&error)
                .expect("legacy query-send source")
                .to_string(),
            "could not register KRPC query: transaction registry is closed"
        );
    }

    #[tokio::test]
    async fn peer_and_sample_methods_build_exact_arguments_and_preserve_projections() {
        let node = CompactNode {
            id: id(3),
            addr: CompactAddr {
                ip: "192.0.2.3".parse().expect("test IP"),
                port: 0,
            },
        };
        let value = CompactAddr {
            ip: "2001:db8::4".parse().expect("test IP"),
            port: u16::MAX,
        };
        let mut get_response = response(id(2));
        get_response.nodes = Some(vec![node, node]);
        get_response.nodes6 = Some(vec![node]);
        get_response.values = Some(vec![value, value]);
        get_response.samples = Some(vec![id(9)]);
        let (client, mut sender, remote) = harness(get_response);
        let result = client
            .get_peers(&mut sender, remote, Id20::ZERO)
            .await
            .expect("get_peers response");
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.nodes[0], result.nodes[1]);
        assert_eq!(result.values.len(), 2);
        assert_eq!(result.values[0], result.values[1]);
        let query = &sender.queries[0];
        assert_eq!(query.query.as_bytes(), b"get_peers");
        let args = query.args.as_ref().expect("query args");
        assert_eq!(args.info_hash, None, "zero info hash is omitted by codec");
        assert_eq!(args.target, None);
        assert_eq!(args.want, None);
        assert_eq!(args.no_seed, 0);
        assert_eq!(args.scrape, 0);

        let mut sample_response = response(id(5));
        sample_response.samples = Some(Vec::new());
        sample_response.num = Some(i64::MIN);
        sample_response.interval = Some(i64::MAX);
        sender.response = sample_response;
        let result = client
            .sample_infohashes(&mut sender, remote, Id20::ZERO)
            .await
            .expect("sample_infohashes response");
        assert_eq!(result.samples, Some(Vec::new()));
        assert_eq!(result.num, i64::MIN);
        assert_eq!(result.interval, i64::MAX);
        let query = &sender.queries[1];
        assert_eq!(query.query.as_bytes(), b"sample_infohashes");
        let args = query.args.as_ref().expect("query args");
        assert_eq!(args.target, None, "zero target is omitted by codec");
        assert_eq!(args.info_hash, None);
        assert_eq!(args.want, None);
        assert_eq!(args.no_seed, 0);
        assert_eq!(args.scrape, 0);
    }

    #[tokio::test]
    async fn scrape_requires_both_filters_and_retains_the_accepted_envelope() {
        let (client, mut sender, remote) = harness(response(id(2)));
        let error = client
            .get_peers_scrape(&mut sender, remote, id(7))
            .await
            .expect_err("missing scrape filters");
        let DhtClientError::MissingScrapeBloomFilters {
            response_source,
            message,
            missing_peers,
            missing_seeders,
        } = error
        else {
            panic!("unexpected scrape error")
        };
        assert_eq!(response_source, remote);
        assert!(missing_peers);
        assert!(missing_seeders);
        assert_eq!(message.response.as_ref().expect("return body").id, id(2));
        assert!(error_message_prefix(missing_peers, missing_seeders)
            .starts_with("missing bloom filter in scrape response"));
        let args = sender.queries[0].args.as_ref().expect("query args");
        assert_eq!(args.info_hash, Some(id(7)));
        assert_eq!(args.scrape, 1);
        assert_eq!(args.want, None);
        assert_eq!(args.no_seed, 0);

        let mut complete = response(id(8));
        complete.peers_bloom = Some(ScrapeBloomFilter::EMPTY);
        complete.seeders_bloom = Some(ScrapeBloomFilter::EMPTY);
        sender.response = complete;
        let result = client
            .get_peers_scrape(&mut sender, remote, id(7))
            .await
            .expect("all-zero scrape filters are valid");
        assert_eq!(result.peers_bloom, ScrapeBloomFilter::EMPTY);
        assert_eq!(result.seeders_bloom, ScrapeBloomFilter::EMPTY);
    }

    fn error_message_prefix(missing_peers: bool, missing_seeders: bool) -> String {
        DhtClientError::<Infallible>::MissingScrapeBloomFilters {
            response_source: "192.0.2.1:1".parse().expect("test address"),
            message: Box::new(KrpcMessage {
                transaction_id: ByteString::default(),
                message_type: ByteString::new(b"r"),
                query: ByteString::default(),
                args: None,
                response: None,
                error: None,
                observed_addr: None,
                read_only: false,
                client_id: ByteString::default(),
            }),
            missing_peers,
            missing_seeders,
        }
        .to_string()
    }
}
