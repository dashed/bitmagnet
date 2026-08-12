use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::{DatagramReceiver, DatagramSender, ReceivedDatagram, MAX_INBOUND_DATAGRAM_BYTES};

/// Errors at the bounded real IPv4 UDP transport boundary.
#[derive(Debug, thiserror::Error)]
pub enum TokioIpv4UdpError {
    #[error("could not bind IPv4 UDP socket: {0}")]
    Bind(#[source] io::Error),
    #[error("could not read local IPv4 UDP address: {0}")]
    LocalAddr(#[source] io::Error),
    #[error("bound UDP socket unexpectedly reported non-IPv4 address {0}")]
    UnexpectedLocalFamily(SocketAddr),
    #[error("IPv4 UDP receiver requires at least {minimum} bytes, got {actual}")]
    ReceiveBufferTooSmall { actual: usize, minimum: usize },
    #[error("IPv4 UDP receive failed: {0}")]
    ReceiveIo(#[source] io::Error),
    #[error("IPv4 UDP socket unexpectedly received from non-IPv4 address {0}")]
    UnexpectedSourceFamily(SocketAddr),
    #[error("IPv4 UDP transport cannot send to {0}")]
    UnsupportedDestinationFamily(SocketAddr),
    #[error("UDP datagram is {actual} bytes, exceeding the {maximum}-byte protocol ceiling")]
    DatagramTooLarge { actual: usize, maximum: usize },
    #[error("IPv4 UDP send to {destination} failed: {source}")]
    SendIo {
        destination: SocketAddrV4,
        #[source]
        source: io::Error,
    },
    #[error("IPv4 UDP send reported {sent} bytes for a {expected}-byte datagram")]
    ShortSend { sent: usize, expected: usize },
}

/// An unopened ownership split over one bound IPv4-only UDP socket.
///
/// Consuming this value produces one non-cloneable receive owner and a
/// cloneable send handle. Both share the same socket and cached bound address.
/// Tokio's `recv_from` and `send_to` futures are cancellation-safe, satisfying
/// the finite supervisor's receiver and sender admission contract.
#[derive(Debug)]
pub struct TokioIpv4UdpTransport {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddrV4,
}

/// The unique receive owner for one bound IPv4 UDP socket.
#[derive(Debug)]
pub struct TokioIpv4UdpReceiver {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddrV4,
}

/// A cloneable send handle sharing the receiver's bound IPv4 UDP socket.
#[derive(Clone, Debug)]
pub struct TokioIpv4UdpSender {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddrV4,
}

impl TokioIpv4UdpTransport {
    /// Bind one IPv4 UDP socket and cache its actual bound address.
    pub async fn bind(local_addr: SocketAddrV4) -> Result<Self, TokioIpv4UdpError> {
        let socket = UdpSocket::bind(local_addr)
            .await
            .map_err(TokioIpv4UdpError::Bind)?;
        let bound = socket.local_addr().map_err(TokioIpv4UdpError::LocalAddr)?;
        let SocketAddr::V4(local_addr) = bound else {
            return Err(TokioIpv4UdpError::UnexpectedLocalFamily(bound));
        };
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
        })
    }

    /// The cached actual IPv4 bind address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    /// Bind to IPv4 loopback with an OS-assigned port.
    pub async fn bind_loopback() -> Result<Self, TokioIpv4UdpError> {
        Self::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await
    }

    /// Consume the bound transport into one receive owner and a cloneable
    /// sender. No second receive owner can be constructed through this API.
    #[must_use]
    pub fn into_parts(self) -> (TokioIpv4UdpReceiver, TokioIpv4UdpSender) {
        let receiver = TokioIpv4UdpReceiver {
            socket: Arc::clone(&self.socket),
            local_addr: self.local_addr,
        };
        let sender = TokioIpv4UdpSender {
            socket: self.socket,
            local_addr: self.local_addr,
        };
        (receiver, sender)
    }
}

impl TokioIpv4UdpReceiver {
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }
}

impl TokioIpv4UdpSender {
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }
}

impl DatagramReceiver for TokioIpv4UdpReceiver {
    type Error = TokioIpv4UdpError;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            if buffer.len() < MAX_INBOUND_DATAGRAM_BYTES {
                return Err(TokioIpv4UdpError::ReceiveBufferTooSmall {
                    actual: buffer.len(),
                    minimum: MAX_INBOUND_DATAGRAM_BYTES,
                });
            }
            let (length, source) = self
                .socket
                .recv_from(buffer)
                .await
                .map_err(TokioIpv4UdpError::ReceiveIo)?;
            if !source.is_ipv4() {
                return Err(TokioIpv4UdpError::UnexpectedSourceFamily(source));
            }
            Ok(ReceivedDatagram { length, source })
        })
    }
}

impl DatagramSender for TokioIpv4UdpSender {
    type Error = TokioIpv4UdpError;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let SocketAddr::V4(destination) = destination else {
                return Err(TokioIpv4UdpError::UnsupportedDestinationFamily(destination));
            };
            if datagram.len() > MAX_INBOUND_DATAGRAM_BYTES {
                return Err(TokioIpv4UdpError::DatagramTooLarge {
                    actual: datagram.len(),
                    maximum: MAX_INBOUND_DATAGRAM_BYTES,
                });
            }
            let sent = self
                .socket
                .send_to(datagram, destination)
                .await
                .map_err(|source| TokioIpv4UdpError::SendIo {
                    destination,
                    source,
                })?;
            if sent != datagram.len() {
                return Err(TokioIpv4UdpError::ShortSend {
                    sent,
                    expected: datagram.len(),
                });
            }
            Ok(())
        })
    }
}
