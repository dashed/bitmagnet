use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use crate::{
    DeliveryOutcome, InboundError, KrpcMessage, TransactionIdIssuer, TransactionRegistry,
    MAX_INBOUND_DATAGRAM_BYTES,
};

/// The owned metadata returned for one datagram written into the supplied
/// receive buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceivedDatagram {
    pub length: usize,
    pub source: SocketAddr,
}

/// A fakeable one-datagram receive seam. It deliberately owns neither a socket
/// lifecycle nor a receive loop.
pub trait DatagramReceiver {
    type Error: 'static;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>>;
}

/// One bounded receive/decode/dispatch result.
#[derive(Debug, PartialEq, Eq)]
pub enum ReceiveDispatchOutcome {
    ZeroLength {
        source: SocketAddr,
    },
    DecodeRejected {
        source: SocketAddr,
        error: InboundError,
    },
    Query {
        source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    Response {
        source: SocketAddr,
        delivery: DeliveryOutcome,
    },
    Error {
        source: SocketAddr,
        delivery: DeliveryOutcome,
    },
    Ignored {
        source: SocketAddr,
        message: Box<KrpcMessage>,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReceiveDispatchError<E> {
    #[error("datagram receive failed: {0}")]
    Transport(E),
    #[error("datagram receiver reported {reported} bytes for a {capacity}-byte buffer")]
    OverreportedLength { reported: usize, capacity: usize },
}

/// Pure one-datagram harness with a fixed reusable receive buffer.
pub struct ReceiveDispatcher<R, I> {
    receiver: R,
    registry: TransactionRegistry<I>,
    buffer: Box<[u8]>,
}

impl<R, I> ReceiveDispatcher<R, I> {
    #[must_use]
    pub fn new(receiver: R, registry: TransactionRegistry<I>) -> Self {
        Self {
            receiver,
            registry,
            buffer: vec![0; MAX_INBOUND_DATAGRAM_BYTES].into_boxed_slice(),
        }
    }
}

impl<R, I> ReceiveDispatcher<R, I>
where
    R: DatagramReceiver,
    I: TransactionIdIssuer,
{
    /// Receive and dispatch exactly one datagram.
    ///
    /// Query and ignored envelopes are returned as owned values. Response and
    /// error envelopes move directly into the transaction registry, which
    /// remains the sole address-normalization and first-wins boundary.
    pub async fn receive_one(
        &mut self,
    ) -> Result<ReceiveDispatchOutcome, ReceiveDispatchError<R::Error>> {
        let received = self
            .receiver
            .receive(&mut self.buffer)
            .await
            .map_err(ReceiveDispatchError::Transport)?;
        if received.length > self.buffer.len() {
            return Err(ReceiveDispatchError::OverreportedLength {
                reported: received.length,
                capacity: self.buffer.len(),
            });
        }
        if received.length == 0 {
            return Ok(ReceiveDispatchOutcome::ZeroLength {
                source: received.source,
            });
        }

        let message = match KrpcMessage::decode_inbound(&self.buffer[..received.length]) {
            Ok(message) => message,
            Err(error) => {
                return Ok(ReceiveDispatchOutcome::DecodeRejected {
                    source: received.source,
                    error,
                });
            }
        };

        match message.message_type.as_bytes() {
            b"q" => Ok(ReceiveDispatchOutcome::Query {
                source: received.source,
                message: Box::new(message),
            }),
            b"r" => Ok(ReceiveDispatchOutcome::Response {
                source: received.source,
                delivery: self.registry.deliver(received.source, message),
            }),
            b"e" => Ok(ReceiveDispatchOutcome::Error {
                source: received.source,
                delivery: self.registry.deliver(received.source, message),
            }),
            _ => Ok(ReceiveDispatchOutcome::Ignored {
                source: received.source,
                message: Box::new(message),
            }),
        }
    }
}
