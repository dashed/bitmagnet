use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use crate::{PingFindNodeReply, WireError};

/// A fakeable single-datagram send seam.
///
/// The caller owns all retry, timeout, queueing, and socket-lifecycle policy.
/// Implementations must not retain the borrowed datagram after the returned
/// future completes.
pub trait DatagramSender {
    type Error: 'static;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;
}

/// The two failure boundaries for one prepared reply send.
#[derive(Debug, thiserror::Error)]
pub enum PingFindNodeSendError<E> {
    /// Encoding completes before a sender future is created.
    #[error("could not encode ping/find-node reply: {0}")]
    Encode(#[source] WireError),
    /// The sender's original error value, with no retry or translation.
    #[error("could not send ping/find-node reply: {0}")]
    Transport(E),
}

/// Encode one borrowed reply completely, then await exactly one datagram send.
///
/// A local-failure caller can borrow the reply embedded in its dispatch
/// outcome and retain the typed local cause. This helper neither consumes nor
/// mutates the reply.
pub async fn send_ping_find_node_reply<S>(
    sender: &mut S,
    reply: &PingFindNodeReply,
) -> Result<(), PingFindNodeSendError<S::Error>>
where
    S: DatagramSender,
{
    let wire = reply.wire().map_err(PingFindNodeSendError::Encode)?;
    sender
        .send(reply.destination, &wire)
        .await
        .map_err(PingFindNodeSendError::Transport)
}
