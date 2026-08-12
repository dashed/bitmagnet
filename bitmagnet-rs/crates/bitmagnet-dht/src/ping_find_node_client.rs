use std::net::SocketAddr;
use std::time::Duration;

use crate::{
    register_and_send_query, ByteString, DatagramSender, Id20, KrpcError, KrpcMessage, MessageArgs,
    MessageReturn, QuerySendError, RoutingNode, TransactionIdIssuer, TransactionRegistry,
    TransactionWaitOutcome,
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

/// Typed failures from query registration/send and post-send response waiting.
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

/// A typed client for only production Go's `ping` and `find_node` adapter.
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

    /// Send one `ping`, await its response, and project only the responder ID.
    pub async fn ping<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
    ) -> Result<PingResult, PingFindNodeClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let response = self
            .query(sender, remote, ByteString::new(b"ping"), None)
            .await?;
        Ok(PingResult { id: response.id })
    }

    /// Send one `find_node` and preserve only ordered `r.nodes` entries.
    pub async fn find_node<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<FindNodeResult, PingFindNodeClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let response = self
            .query(sender, remote, ByteString::new(b"find_node"), Some(target))
            .await?;
        Ok(FindNodeResult {
            id: response.id,
            nodes: response
                .nodes
                .unwrap_or_default()
                .into_iter()
                .map(|node| RoutingNode {
                    id: node.id,
                    addr: SocketAddr::new(node.addr.ip, node.addr.port),
                })
                .collect(),
        })
    }

    async fn query<S>(
        &self,
        sender: &mut S,
        remote: SocketAddr,
        query: ByteString,
        target: Option<Id20>,
    ) -> Result<MessageReturn, PingFindNodeClientError<S::Error>>
    where
        S: DatagramSender,
    {
        let pending = register_and_send_query(
            self.registry,
            sender,
            remote,
            query,
            MessageArgs {
                id: self.local_id,
                info_hash: None,
                target,
                token: ByteString::default(),
                port: None,
                implied_port: false,
                want: None,
                no_seed: 0,
                scrape: 0,
            },
        )
        .await
        .map_err(PingFindNodeClientError::QuerySend)?;

        match pending.wait(self.query_timeout).await {
            TransactionWaitOutcome::Response { response, .. } => Ok(*response),
            TransactionWaitOutcome::RemoteError {
                source,
                message,
                error,
            } => Err(PingFindNodeClientError::RemoteError {
                response_source: source,
                message,
                error,
            }),
            TransactionWaitOutcome::MissingReturnBody { source, message } => {
                Err(PingFindNodeClientError::MissingReturnBody {
                    response_source: source,
                    message,
                })
            }
            TransactionWaitOutcome::MissingErrorBody { source, message } => {
                Err(PingFindNodeClientError::MissingErrorBody {
                    response_source: source,
                    message,
                })
            }
            TransactionWaitOutcome::Timeout => Err(PingFindNodeClientError::Timeout),
            TransactionWaitOutcome::RegistryClosed => Err(PingFindNodeClientError::RegistryClosed),
            TransactionWaitOutcome::Cancelled => {
                unreachable!("PendingTransaction::wait never returns cancellation")
            }
        }
    }
}
