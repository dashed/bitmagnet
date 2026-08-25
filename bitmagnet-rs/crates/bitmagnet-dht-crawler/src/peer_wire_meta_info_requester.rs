//! Bounded IPv4 BitTorrent peer-wire metadata requester.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;

use async_trait::async_trait;
use bendy::decoding::{Decoder, Object};
use bitmagnet_dht::Id20;
use bitmagnet_metainfo::{parse_info_bytes, ParseInfoError, ParsedInfo};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;

use crate::{DhtMetaInfoRequester, RequestMetaInfoCollaboratorError};

/// Per-connect timeout used by [`DhtPeerWireMetaInfoRequester::default_config`].
pub const DHT_PEER_WIRE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Timeout budget checked at asynchronous yield points across one request.
///
/// Bounded synchronous decode, assembly, or final parsing already executing in
/// one poll cannot be preempted and may finish after this wall-clock budget.
pub const DHT_PEER_WIRE_REQUEST_TIMEOUT: Duration = Duration::from_secs(6);
/// Inclusive maximum BitTorrent frame body length and exclusive metadata-size upper bound.
pub const DHT_PEER_WIRE_MAX_METADATA_SIZE: usize = 10 * 1024 * 1024;
/// BEP 9 metadata piece width.
pub const DHT_PEER_WIRE_METADATA_PIECE_SIZE: usize = 16 * 1024;
/// Locally advertised extension-message ID for incoming `ut_metadata` responses.
pub const DHT_PEER_WIRE_LOCAL_UT_METADATA_ID: u8 = 1;

pub(super) const HANDSHAKE_SIZE: usize = 68;
const MAX_BENCODE_NESTING_DEPTH: usize = 64;
const EXTENDED_MESSAGE_ID: u8 = 20;
const EXTENSION_HANDSHAKE_ID: u8 = 0;
const PROTOCOL: &[u8; 20] = b"\x13BitTorrent protocol";
pub(super) const ADVERTISED_EXTENSION_BITS: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0x01];
pub(super) const EXTENSION_HANDSHAKE_REQUEST: &[u8] =
    b"\x00\x00\x00\x1a\x14\x00d1:md11:ut_metadatai1eee";

/// Requester time limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtPeerWireMetaInfoRequesterConfig {
    /// Maximum time spent establishing the IPv4 TCP connection.
    pub connect_timeout: Duration,
    /// Maximum request budget enforced at asynchronous yield points, including
    /// connection setup and every peer-wire I/O wait.
    ///
    /// Bounded synchronous work already executing in one poll is not
    /// preemptible by Tokio's timeout.
    pub request_timeout: Duration,
}

impl Default for DhtPeerWireMetaInfoRequesterConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DHT_PEER_WIRE_CONNECT_TIMEOUT,
            request_timeout: DHT_PEER_WIRE_REQUEST_TIMEOUT,
        }
    }
}

/// Wire step attached to an I/O failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtPeerWireMetaInfoRequesterStage {
    BitTorrentHandshakeWrite,
    BitTorrentHandshakeRead,
    ExtensionHandshakeWrite,
    MessageLengthRead,
    MessageBodyRead,
    MetadataRequestWrite,
}

impl fmt::Display for DhtPeerWireMetaInfoRequesterStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BitTorrentHandshakeWrite => "BitTorrent handshake write",
            Self::BitTorrentHandshakeRead => "BitTorrent handshake read",
            Self::ExtensionHandshakeWrite => "extension handshake write",
            Self::MessageLengthRead => "message length read",
            Self::MessageBodyRead => "message body read",
            Self::MetadataRequestWrite => "metadata request write",
        })
    }
}

/// Typed failure returned by [`DhtPeerWireMetaInfoRequester`].
#[derive(Debug, thiserror::Error)]
pub enum DhtPeerWireMetaInfoRequesterError {
    #[error("peer-wire metainfo requester supports IPv4 peers only, got {0}")]
    UnsupportedAddressFamily(SocketAddr),
    #[error("peer-wire metainfo request for {peer} timed out after {timeout:?}")]
    RequestTimeout {
        peer: SocketAddrV4,
        timeout: Duration,
    },
    #[error("TCP connect to {peer} timed out after {timeout:?}")]
    ConnectTimeout {
        peer: SocketAddrV4,
        timeout: Duration,
    },
    #[error("TCP connect to {peer} failed: {source}")]
    Connect {
        peer: SocketAddrV4,
        #[source]
        source: io::Error,
    },
    #[error("setting TCP_NODELAY for {peer} failed: {source}")]
    SetNoDelay {
        peer: SocketAddrV4,
        #[source]
        source: io::Error,
    },
    #[error("setting zero TCP linger for {peer} failed: {source}")]
    SetLinger {
        peer: SocketAddrV4,
        #[source]
        source: io::Error,
    },
    #[error("{stage} for {peer} failed: {source}")]
    Io {
        peer: SocketAddrV4,
        stage: DhtPeerWireMetaInfoRequesterStage,
        #[source]
        source: io::Error,
    },
    #[error("invalid BitTorrent handshake protocol")]
    InvalidHandshakeProtocol,
    #[error("peer does not advertise BEP 10 extension support")]
    ExtensionProtocolUnsupported,
    #[error("peer handshake returned a different info hash")]
    InfoHashMismatch,
    #[error("first peer extension message has ID {actual}, expected extension handshake ID 0")]
    FirstExtensionMessageNotHandshake { actual: u8 },
    #[error("invalid bencode: {0}")]
    Bencode(#[source] bendy::decoding::Error),
    #[error("invalid peer-wire bencode structure: {0}")]
    ProtocolBencode(String),
    #[error("extension handshake is missing {field}")]
    MissingExtensionHandshakeField { field: &'static str },
    #[error("extension handshake field {field} must be an integer")]
    InvalidIntegerType { field: &'static str },
    #[error("extension handshake field {field} has invalid integer {value:?}")]
    InvalidIntegerValue { field: &'static str, value: String },
    #[error("metadata size {0} is outside 1..{DHT_PEER_WIRE_MAX_METADATA_SIZE}")]
    InvalidMetadataSize(i64),
    #[error("peer ut_metadata ID {0} is outside 1..=254")]
    InvalidRemoteUtMetadataId(i64),
    #[error("peer frame body length {length} exceeds inclusive maximum {DHT_PEER_WIRE_MAX_METADATA_SIZE}")]
    MessageTooLong { length: usize },
    #[error("ut_metadata message is missing {field}")]
    MissingMetadataMessageField { field: &'static str },
    #[error("peer rejected metadata piece {piece}")]
    MetadataRejected { piece: i64 },
    #[error("metadata piece index {piece} is outside 0..{piece_count}")]
    InvalidPieceIndex { piece: i64, piece_count: usize },
    #[error("metadata piece {piece} was received more than once")]
    DuplicatePiece { piece: usize },
    #[error("metadata piece {piece} has length {actual}, expected {expected}")]
    InvalidPieceLength {
        piece: usize,
        actual: usize,
        expected: usize,
    },
    #[error("metadata response total_size {actual} does not match negotiated size {expected}")]
    MetadataTotalSizeMismatch { actual: i64, expected: usize },
    #[error("verified metainfo parsing failed: {0}")]
    Parse(#[source] ParseInfoError),
}

/// Concrete IPv4 peer-wire implementation of [`DhtMetaInfoRequester`].
///
/// The injected peer ID is stable for the lifetime of this value. Each call is
/// attempted once, configures `TCP_NODELAY` and zero linger, and verifies the
/// exact raw info dictionary against the requested 20-byte identity.
#[derive(Clone, Copy, Debug)]
pub struct DhtPeerWireMetaInfoRequester {
    peer_id: Id20,
    config: DhtPeerWireMetaInfoRequesterConfig,
}

impl DhtPeerWireMetaInfoRequester {
    #[must_use]
    pub const fn new(peer_id: Id20) -> Self {
        Self {
            peer_id,
            config: DhtPeerWireMetaInfoRequesterConfig {
                connect_timeout: DHT_PEER_WIRE_CONNECT_TIMEOUT,
                request_timeout: DHT_PEER_WIRE_REQUEST_TIMEOUT,
            },
        }
    }

    #[must_use]
    pub const fn with_config(peer_id: Id20, config: DhtPeerWireMetaInfoRequesterConfig) -> Self {
        Self { peer_id, config }
    }

    #[must_use]
    pub const fn peer_id(&self) -> Id20 {
        self.peer_id
    }

    #[must_use]
    pub const fn config(&self) -> DhtPeerWireMetaInfoRequesterConfig {
        self.config
    }

    #[must_use]
    pub const fn default_config() -> DhtPeerWireMetaInfoRequesterConfig {
        DhtPeerWireMetaInfoRequesterConfig {
            connect_timeout: DHT_PEER_WIRE_CONNECT_TIMEOUT,
            request_timeout: DHT_PEER_WIRE_REQUEST_TIMEOUT,
        }
    }

    async fn request_ipv4_with_connector<C, S>(
        &self,
        info_hash: Id20,
        peer: SocketAddrV4,
        connector: C,
    ) -> Result<ParsedInfo, DhtPeerWireMetaInfoRequesterError>
    where
        C: Future<Output = Result<S, DhtPeerWireMetaInfoRequesterError>>,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        timeout(self.config.request_timeout, async {
            let mut stream = timeout(self.config.connect_timeout, connector)
                .await
                .map_err(|_| DhtPeerWireMetaInfoRequesterError::ConnectTimeout {
                    peer,
                    timeout: self.config.connect_timeout,
                })??;
            request_over_stream(&mut stream, peer, info_hash, self.peer_id).await
        })
        .await
        .map_err(|_| DhtPeerWireMetaInfoRequesterError::RequestTimeout {
            peer,
            timeout: self.config.request_timeout,
        })?
    }
}

async fn connect_ipv4(peer: SocketAddrV4) -> Result<TcpStream, DhtPeerWireMetaInfoRequesterError> {
    let socket = TcpSocket::new_v4()
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::Connect { peer, source })?;
    socket
        .set_nodelay(true)
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::SetNoDelay { peer, source })?;
    socket
        .set_zero_linger()
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::SetLinger { peer, source })?;
    socket
        .connect(peer.into())
        .await
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::Connect { peer, source })
}

#[async_trait]
impl DhtMetaInfoRequester for DhtPeerWireMetaInfoRequester {
    async fn request(
        &self,
        info_hash: Id20,
        peer: SocketAddr,
    ) -> Result<ParsedInfo, RequestMetaInfoCollaboratorError> {
        let SocketAddr::V4(peer) = peer else {
            return Err(Box::new(
                DhtPeerWireMetaInfoRequesterError::UnsupportedAddressFamily(peer),
            ));
        };
        self.request_ipv4_with_connector(info_hash, peer, connect_ipv4(peer))
            .await
            .map_err(|error| Box::new(error) as RequestMetaInfoCollaboratorError)
    }
}

async fn request_over_stream<S>(
    stream: &mut S,
    peer: SocketAddrV4,
    info_hash: Id20,
    peer_id: Id20,
) -> Result<ParsedInfo, DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    perform_bit_torrent_handshake(stream, peer, info_hash, peer_id).await?;
    let extension = perform_extension_handshake(stream, peer).await?;
    request_all_pieces(
        stream,
        peer,
        extension.metadata_size,
        extension.remote_ut_metadata_id,
    )
    .await?;
    let raw_info = read_all_pieces(stream, peer, extension.metadata_size).await?;
    parse_info_bytes(*info_hash.as_bytes(), &raw_info)
        .map_err(DhtPeerWireMetaInfoRequesterError::Parse)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ExtensionHandshake {
    pub(super) metadata_size: usize,
    pub(super) remote_ut_metadata_id: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MetadataHeader {
    message_type: i64,
    piece: i64,
    total_size: Option<i64>,
}

pub(super) fn handshake_request(info_hash: Id20, peer_id: Id20) -> [u8; HANDSHAKE_SIZE] {
    let mut output = [0_u8; HANDSHAKE_SIZE];
    output[..20].copy_from_slice(PROTOCOL);
    output[20..28].copy_from_slice(&ADVERTISED_EXTENSION_BITS);
    output[28..48].copy_from_slice(info_hash.as_bytes());
    output[48..].copy_from_slice(peer_id.as_bytes());
    output
}

pub(super) async fn perform_bit_torrent_handshake<S>(
    stream: &mut S,
    peer: SocketAddrV4,
    info_hash: Id20,
    peer_id: Id20,
) -> Result<[u8; 20], DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(&handshake_request(info_hash, peer_id))
        .await
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::Io {
            peer,
            stage: DhtPeerWireMetaInfoRequesterStage::BitTorrentHandshakeWrite,
            source,
        })?;
    let mut response = [0_u8; HANDSHAKE_SIZE];
    stream.read_exact(&mut response).await.map_err(|source| {
        DhtPeerWireMetaInfoRequesterError::Io {
            peer,
            stage: DhtPeerWireMetaInfoRequesterStage::BitTorrentHandshakeRead,
            source,
        }
    })?;
    if &response[..20] != PROTOCOL {
        return Err(DhtPeerWireMetaInfoRequesterError::InvalidHandshakeProtocol);
    }
    if response[25] & 0x10 == 0 {
        return Err(DhtPeerWireMetaInfoRequesterError::ExtensionProtocolUnsupported);
    }
    if &response[28..48] != info_hash.as_bytes() {
        return Err(DhtPeerWireMetaInfoRequesterError::InfoHashMismatch);
    }
    Ok(response[48..]
        .try_into()
        .expect("handshake peer ID slice is always 20 bytes"))
}

pub(super) async fn perform_extension_handshake<S>(
    stream: &mut S,
    peer: SocketAddrV4,
) -> Result<ExtensionHandshake, DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(EXTENSION_HANDSHAKE_REQUEST)
        .await
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::Io {
            peer,
            stage: DhtPeerWireMetaInfoRequesterStage::ExtensionHandshakeWrite,
            source,
        })?;
    loop {
        let message = read_message(stream, peer).await?;
        if message.len() < 2 || message[0] != EXTENDED_MESSAGE_ID {
            continue;
        }
        if message[1] != EXTENSION_HANDSHAKE_ID {
            return Err(
                DhtPeerWireMetaInfoRequesterError::FirstExtensionMessageNotHandshake {
                    actual: message[1],
                },
            );
        }
        return decode_extension_handshake(&message[2..]);
    }
}

pub(super) fn decode_extension_handshake(
    payload: &[u8],
) -> Result<ExtensionHandshake, DhtPeerWireMetaInfoRequesterError> {
    let mut decoder = Decoder::new(payload).with_max_depth(MAX_BENCODE_NESTING_DEPTH);
    let (metadata_size, remote_ut_metadata_id) = {
        let object = decoder
            .next_object()
            .map_err(bencode_error)?
            .ok_or_else(|| bencode_message("empty extension handshake"))?;
        let Object::Dict(mut root) = object else {
            return Err(bencode_message(
                "extension handshake root must be a dictionary",
            ));
        };
        let mut metadata_size = None;
        let mut remote_ut_metadata_id = None;
        while let Some((key, value)) = root.next_pair().map_err(bencode_error)? {
            match key {
                b"metadata_size" => {
                    metadata_size = Some(parse_integer(value, "metadata_size")?);
                }
                b"m" => {
                    let Object::Dict(mut extensions) = value else {
                        return Err(bencode_message(
                            "extension handshake m must be a dictionary",
                        ));
                    };
                    while let Some((extension, value)) =
                        extensions.next_pair().map_err(bencode_error)?
                    {
                        if extension == b"ut_metadata" {
                            remote_ut_metadata_id = Some(parse_integer(value, "m.ut_metadata")?);
                        }
                    }
                }
                _ => drop(value),
            }
        }
        (metadata_size, remote_ut_metadata_id)
    };
    if decoder.next_object().map_err(bencode_error)?.is_some() {
        return Err(bencode_message("trailing extension handshake object"));
    }
    let metadata_size = metadata_size.ok_or(
        DhtPeerWireMetaInfoRequesterError::MissingExtensionHandshakeField {
            field: "metadata_size",
        },
    )?;
    if !(1..DHT_PEER_WIRE_MAX_METADATA_SIZE as i64).contains(&metadata_size) {
        return Err(DhtPeerWireMetaInfoRequesterError::InvalidMetadataSize(
            metadata_size,
        ));
    }
    let remote_ut_metadata_id = remote_ut_metadata_id.ok_or(
        DhtPeerWireMetaInfoRequesterError::MissingExtensionHandshakeField {
            field: "m.ut_metadata",
        },
    )?;
    let remote_ut_metadata_id = u8::try_from(remote_ut_metadata_id)
        .ok()
        .filter(|id| (1..=254).contains(id))
        .ok_or(
            DhtPeerWireMetaInfoRequesterError::InvalidRemoteUtMetadataId(remote_ut_metadata_id),
        )?;
    Ok(ExtensionHandshake {
        metadata_size: usize::try_from(metadata_size)
            .expect("validated positive metadata size fits usize"),
        remote_ut_metadata_id,
    })
}

pub(super) async fn request_all_pieces<S>(
    stream: &mut S,
    peer: SocketAddrV4,
    metadata_size: usize,
    remote_ut_metadata_id: u8,
) -> Result<(), DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncWrite + Unpin,
{
    let piece_count = metadata_size.div_ceil(DHT_PEER_WIRE_METADATA_PIECE_SIZE);
    for piece in 0..piece_count {
        let header = format!("d8:msg_typei0e5:piecei{piece}ee");
        let body_length = 2_usize
            .checked_add(header.len())
            .expect("bounded request header length cannot overflow");
        let body_length: u32 = body_length
            .try_into()
            .expect("bounded request header length fits u32");
        let mut frame = Vec::with_capacity(4 + body_length as usize);
        frame.extend_from_slice(&body_length.to_be_bytes());
        frame.push(EXTENDED_MESSAGE_ID);
        frame.push(remote_ut_metadata_id);
        frame.extend_from_slice(header.as_bytes());
        stream
            .write_all(&frame)
            .await
            .map_err(|source| DhtPeerWireMetaInfoRequesterError::Io {
                peer,
                stage: DhtPeerWireMetaInfoRequesterStage::MetadataRequestWrite,
                source,
            })?;
    }
    Ok(())
}

pub(super) async fn read_message<S>(
    stream: &mut S,
    peer: SocketAddrV4,
) -> Result<Vec<u8>, DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncRead + Unpin,
{
    let mut length_bytes = [0_u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .await
        .map_err(|source| DhtPeerWireMetaInfoRequesterError::Io {
            peer,
            stage: DhtPeerWireMetaInfoRequesterStage::MessageLengthRead,
            source,
        })?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length > DHT_PEER_WIRE_MAX_METADATA_SIZE {
        return Err(DhtPeerWireMetaInfoRequesterError::MessageTooLong { length });
    }
    let mut message = vec![0; length];
    stream.read_exact(&mut message).await.map_err(|source| {
        DhtPeerWireMetaInfoRequesterError::Io {
            peer,
            stage: DhtPeerWireMetaInfoRequesterStage::MessageBodyRead,
            source,
        }
    })?;
    Ok(message)
}

pub(super) async fn read_all_pieces<S>(
    stream: &mut S,
    peer: SocketAddrV4,
    metadata_size: usize,
) -> Result<Vec<u8>, DhtPeerWireMetaInfoRequesterError>
where
    S: AsyncRead + Unpin,
{
    let piece_count = metadata_size.div_ceil(DHT_PEER_WIRE_METADATA_PIECE_SIZE);
    let mut pieces = vec![None; piece_count];
    let mut remaining = piece_count;
    while remaining != 0 {
        let message = read_message(stream, peer).await?;
        if message.len() < 2
            || message[0] != EXTENDED_MESSAGE_ID
            || message[1] != DHT_PEER_WIRE_LOCAL_UT_METADATA_ID
        {
            continue;
        }
        let (header, payload) = decode_metadata_message(&message[2..])?;
        if header.message_type == 2 {
            return Err(DhtPeerWireMetaInfoRequesterError::MetadataRejected {
                piece: header.piece,
            });
        }
        if header.message_type != 1 {
            continue;
        }
        if let Some(total_size) = header.total_size {
            if total_size != metadata_size as i64 {
                return Err(
                    DhtPeerWireMetaInfoRequesterError::MetadataTotalSizeMismatch {
                        actual: total_size,
                        expected: metadata_size,
                    },
                );
            }
        }
        let piece = usize::try_from(header.piece)
            .ok()
            .filter(|piece| *piece < piece_count)
            .ok_or(DhtPeerWireMetaInfoRequesterError::InvalidPieceIndex {
                piece: header.piece,
                piece_count,
            })?;
        if pieces[piece].is_some() {
            return Err(DhtPeerWireMetaInfoRequesterError::DuplicatePiece { piece });
        }
        let expected = if piece + 1 == piece_count {
            metadata_size - piece * DHT_PEER_WIRE_METADATA_PIECE_SIZE
        } else {
            DHT_PEER_WIRE_METADATA_PIECE_SIZE
        };
        if payload.len() != expected {
            return Err(DhtPeerWireMetaInfoRequesterError::InvalidPieceLength {
                piece,
                actual: payload.len(),
                expected,
            });
        }
        pieces[piece] = Some(payload.to_vec());
        remaining -= 1;
    }
    Ok(pieces
        .into_iter()
        .flat_map(|piece| piece.expect("completion requires every unique piece"))
        .collect())
}

fn decode_metadata_message(
    payload: &[u8],
) -> Result<(MetadataHeader, &[u8]), DhtPeerWireMetaInfoRequesterError> {
    let mut decoder = Decoder::new(payload).with_max_depth(MAX_BENCODE_NESTING_DEPTH);
    let object = decoder
        .next_object()
        .map_err(bencode_error)?
        .ok_or_else(|| bencode_message("empty ut_metadata message"))?;
    let Object::Dict(mut root) = object else {
        return Err(bencode_message("ut_metadata header must be a dictionary"));
    };
    let mut message_type = None;
    let mut piece = None;
    let mut total_size = None;
    while let Some((key, value)) = root.next_pair().map_err(bencode_error)? {
        match key {
            b"msg_type" => message_type = Some(parse_integer(value, "msg_type")?),
            b"piece" => piece = Some(parse_integer(value, "piece")?),
            b"total_size" => total_size = Some(parse_integer(value, "total_size")?),
            _ => drop(value),
        }
    }
    let raw = root.into_raw().map_err(bencode_error)?;
    Ok((
        MetadataHeader {
            message_type: message_type.ok_or(
                DhtPeerWireMetaInfoRequesterError::MissingMetadataMessageField {
                    field: "msg_type",
                },
            )?,
            piece: piece.ok_or(
                DhtPeerWireMetaInfoRequesterError::MissingMetadataMessageField { field: "piece" },
            )?,
            total_size,
        },
        &payload[raw.len()..],
    ))
}

fn parse_integer(
    value: Object<'_, '_>,
    field: &'static str,
) -> Result<i64, DhtPeerWireMetaInfoRequesterError> {
    let Object::Integer(value) = value else {
        return Err(DhtPeerWireMetaInfoRequesterError::InvalidIntegerType { field });
    };
    value
        .parse()
        .map_err(|_| DhtPeerWireMetaInfoRequesterError::InvalidIntegerValue {
            field,
            value: value.to_owned(),
        })
}

fn bencode_error(error: bendy::decoding::Error) -> DhtPeerWireMetaInfoRequesterError {
    DhtPeerWireMetaInfoRequesterError::Bencode(error)
}

fn bencode_message(message: impl Into<String>) -> DhtPeerWireMetaInfoRequesterError {
    DhtPeerWireMetaInfoRequesterError::ProtocolBencode(message.into())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use sha1::{Digest, Sha1};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadBuf};

    use super::*;

    fn id(start: u8) -> Id20 {
        Id20::from_slice(&(start..start + 20).collect::<Vec<_>>()).expect("fixture ID")
    }

    fn peer_id(start: u8) -> Id20 {
        id(start)
    }

    fn duplex_with_input(input: &[u8]) -> DuplexStream {
        let (client, mut server) = tokio::io::duplex(input.len().max(1) + 128);
        let input = input.to_vec();
        tokio::spawn(async move {
            server.write_all(&input).await.expect("script input");
        });
        client
    }

    fn test_peer() -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881)
    }

    fn frame(body: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(4 + body.len());
        output.extend_from_slice(&(body.len() as u32).to_be_bytes());
        output.extend_from_slice(body);
        output
    }

    fn metadata_frame(extension_id: u8, piece: usize, payload: &[u8]) -> Vec<u8> {
        let mut body = vec![EXTENDED_MESSAGE_ID, extension_id];
        body.extend_from_slice(format!("d8:msg_typei1e5:piecei{piece}ee").as_bytes());
        body.extend_from_slice(payload);
        frame(&body)
    }

    #[test]
    fn defaults_and_public_traits_are_exact() {
        let requester = DhtPeerWireMetaInfoRequester::new(peer_id(0x20));
        assert_eq!(
            requester.config(),
            DhtPeerWireMetaInfoRequesterConfig::default()
        );
        assert_eq!(requester.config().connect_timeout, Duration::from_secs(3));
        assert_eq!(requester.config().request_timeout, Duration::from_secs(6));
        assert_eq!(requester.peer_id(), peer_id(0x20));
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DhtPeerWireMetaInfoRequester>();
        assert_send_sync::<DhtPeerWireMetaInfoRequesterError>();
    }

    #[tokio::test]
    async fn ipv6_is_rejected_before_connect() {
        let requester = DhtPeerWireMetaInfoRequester::new(peer_id(0x20));
        let error = requester
            .request(id(0), SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 1))
            .await
            .expect_err("IPv6 must be rejected");
        assert!(error
            .downcast_ref::<DhtPeerWireMetaInfoRequesterError>()
            .is_some_and(|error| matches!(
                error,
                DhtPeerWireMetaInfoRequesterError::UnsupportedAddressFamily(_)
            )));
    }

    #[tokio::test]
    async fn handshake_request_and_validation_are_exact() {
        let info_hash = id(0);
        let local_peer_id = peer_id(0x20);
        let remote_peer_id = peer_id(0x40);
        let mut response = handshake_request(info_hash, remote_peer_id);
        let (mut client, mut server) = tokio::io::duplex(256);
        let server_task = tokio::spawn(async move {
            let mut request = [0; HANDSHAKE_SIZE];
            server.read_exact(&mut request).await.expect("request");
            server.write_all(&response).await.expect("response");
            request
        });
        let actual =
            perform_bit_torrent_handshake(&mut client, test_peer(), info_hash, local_peer_id)
                .await
                .expect("valid handshake");
        assert_eq!(actual, *remote_peer_id.as_bytes());
        assert_eq!(
            server_task.await.expect("server"),
            handshake_request(info_hash, local_peer_id)
        );

        response[25] &= !0x10;
        let (mut client, mut server) = tokio::io::duplex(256);
        tokio::spawn(async move {
            let mut request = [0; HANDSHAKE_SIZE];
            server.read_exact(&mut request).await.expect("request");
            server.write_all(&response).await.expect("response");
        });
        assert!(matches!(
            perform_bit_torrent_handshake(&mut client, test_peer(), info_hash, local_peer_id).await,
            Err(DhtPeerWireMetaInfoRequesterError::ExtensionProtocolUnsupported)
        ));
    }

    #[test]
    fn extension_handshake_boundaries_and_strict_bencode() {
        for (size, id) in [(1, 1), (DHT_PEER_WIRE_MAX_METADATA_SIZE - 1, 254)] {
            let encoded = format!("d1:md11:ut_metadatai{id}ee13:metadata_sizei{size}ee");
            assert_eq!(
                decode_extension_handshake(encoded.as_bytes()).expect("boundary"),
                ExtensionHandshake {
                    metadata_size: size,
                    remote_ut_metadata_id: id as u8,
                }
            );
        }
        for size in [0, DHT_PEER_WIRE_MAX_METADATA_SIZE] {
            let encoded = format!("d1:md11:ut_metadatai1ee13:metadata_sizei{size}ee");
            assert!(matches!(
                decode_extension_handshake(encoded.as_bytes()),
                Err(DhtPeerWireMetaInfoRequesterError::InvalidMetadataSize(_))
            ));
        }
        let error = decode_extension_handshake(
            b"d1:md11:ut_metadatai1e11:ut_metadatai2ee13:metadata_sizei1ee",
        )
        .expect_err("duplicate key");
        assert!(matches!(
            error,
            DhtPeerWireMetaInfoRequesterError::Bencode(_)
        ));
        assert!(std::error::Error::source(&error)
            .is_some_and(|source| source.downcast_ref::<bendy::decoding::Error>().is_some()));
        assert!(matches!(
            decode_extension_handshake(b"le"),
            Err(DhtPeerWireMetaInfoRequesterError::ProtocolBencode(_))
        ));
        let deeply_nested = format!(
            "d1:md11:ut_metadatai1ee13:metadata_sizei1e1:x{}{}e",
            "l".repeat(65),
            "e".repeat(65)
        );
        assert!(matches!(
            decode_extension_handshake(deeply_nested.as_bytes()),
            Err(DhtPeerWireMetaInfoRequesterError::Bencode(_))
        ));
    }

    #[tokio::test]
    async fn request_frames_use_remote_directional_id() {
        let (mut client, mut server) = tokio::io::duplex(256);
        request_all_pieces(&mut client, test_peer(), 16_385, 254)
            .await
            .expect("requests");
        client.shutdown().await.expect("shutdown");
        let mut actual = Vec::new();
        server
            .read_to_end(&mut actual)
            .await
            .expect("read requests");
        assert_eq!(
            actual,
            b"\x00\x00\x00\x1b\x14\xfed8:msg_typei0e5:piecei0ee\x00\x00\x00\x1b\x14\xfed8:msg_typei0e5:piecei1ee"
        );
    }

    #[tokio::test]
    async fn integrated_stream_uses_peer_id_for_requests_and_local_id_for_responses() {
        let mut raw_info = b"d6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:".to_vec();
        raw_info.extend_from_slice(&[0x70; 20]);
        raw_info.push(b'e');
        let requested = Id20::from_slice(&Sha1::digest(&raw_info)).expect("SHA-1 width");
        let local_peer_id = peer_id(0x20);
        let remote_peer_id = peer_id(0x40);
        let (mut client, mut server) = tokio::io::duplex(2048);
        let server_raw = raw_info.clone();
        let server_task = tokio::spawn(async move {
            let mut handshake = [0; HANDSHAKE_SIZE];
            server
                .read_exact(&mut handshake)
                .await
                .expect("handshake request");
            assert_eq!(handshake, handshake_request(requested, local_peer_id));
            server
                .write_all(&handshake_request(requested, remote_peer_id))
                .await
                .expect("handshake response");

            let mut extension_request = vec![0; EXTENSION_HANDSHAKE_REQUEST.len()];
            server
                .read_exact(&mut extension_request)
                .await
                .expect("extension request");
            assert_eq!(extension_request, EXTENSION_HANDSHAKE_REQUEST);
            let extension_body = format!(
                "d1:md11:ut_metadatai254ee13:metadata_sizei{}ee",
                server_raw.len()
            );
            let mut extension_message = vec![EXTENDED_MESSAGE_ID, EXTENSION_HANDSHAKE_ID];
            extension_message.extend_from_slice(extension_body.as_bytes());
            server
                .write_all(&frame(&extension_message))
                .await
                .expect("extension response");

            let mut length = [0; 4];
            server
                .read_exact(&mut length)
                .await
                .expect("request length");
            let mut request = vec![0; u32::from_be_bytes(length) as usize];
            server
                .read_exact(&mut request)
                .await
                .expect("piece request");
            assert_eq!(request[0], EXTENDED_MESSAGE_ID);
            assert_eq!(request[1], 254, "peer-advertised ID is outbound only");
            server
                .write_all(&metadata_frame(
                    DHT_PEER_WIRE_LOCAL_UT_METADATA_ID,
                    0,
                    &server_raw,
                ))
                .await
                .expect("piece response");
        });
        let parsed = request_over_stream(&mut client, test_peer(), requested, local_peer_id)
            .await
            .expect("integrated stream request");
        assert_eq!(parsed.info_hash_v1(), Some(*requested.as_bytes()));
        assert_eq!(parsed.info().name(), b"x");
        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn piece_reader_requires_unique_exact_out_of_order_coverage() {
        let mut wire = frame(&[1]);
        wire.extend_from_slice(&metadata_frame(
            1,
            1,
            &[0xb2; DHT_PEER_WIRE_METADATA_PIECE_SIZE],
        ));
        wire.extend_from_slice(&metadata_frame(
            1,
            0,
            &[0xa1; DHT_PEER_WIRE_METADATA_PIECE_SIZE],
        ));
        wire.extend_from_slice(&metadata_frame(1, 2, &[0xc3]));
        let mut input = duplex_with_input(&wire);
        let output = read_all_pieces(
            &mut input,
            test_peer(),
            2 * DHT_PEER_WIRE_METADATA_PIECE_SIZE + 1,
        )
        .await
        .expect("out-of-order complete pieces");
        assert_eq!(
            &output[..DHT_PEER_WIRE_METADATA_PIECE_SIZE],
            &[0xa1; DHT_PEER_WIRE_METADATA_PIECE_SIZE]
        );
        assert_eq!(
            &output[DHT_PEER_WIRE_METADATA_PIECE_SIZE..2 * DHT_PEER_WIRE_METADATA_PIECE_SIZE],
            &[0xb2; DHT_PEER_WIRE_METADATA_PIECE_SIZE]
        );
        assert_eq!(output.last(), Some(&0xc3));

        let mut wire = metadata_frame(1, 0, &[0xa1; DHT_PEER_WIRE_METADATA_PIECE_SIZE]);
        wire.extend_from_slice(&metadata_frame(
            1,
            0,
            &[0xb2; DHT_PEER_WIRE_METADATA_PIECE_SIZE],
        ));
        let mut input = duplex_with_input(&wire);
        assert!(matches!(
            read_all_pieces(
                &mut input,
                test_peer(),
                2 * DHT_PEER_WIRE_METADATA_PIECE_SIZE
            )
            .await,
            Err(DhtPeerWireMetaInfoRequesterError::DuplicatePiece { piece: 0 })
        ));

        let mut input = duplex_with_input(&metadata_frame(1, 1, &[0xc3]));
        assert!(matches!(
            read_all_pieces(&mut input, test_peer(), 1).await,
            Err(DhtPeerWireMetaInfoRequesterError::InvalidPieceIndex {
                piece: 1,
                piece_count: 1
            })
        ));

        let reject = frame(b"\x14\x01d8:msg_typei2e5:piecei0ee");
        let mut input = duplex_with_input(&reject);
        assert!(matches!(
            read_all_pieces(&mut input, test_peer(), 1).await,
            Err(DhtPeerWireMetaInfoRequesterError::MetadataRejected { piece: 0 })
        ));
    }

    #[tokio::test]
    async fn framed_message_maximum_is_inclusive() {
        let mut input = (DHT_PEER_WIRE_MAX_METADATA_SIZE as u32)
            .to_be_bytes()
            .to_vec();
        input.resize(4 + DHT_PEER_WIRE_MAX_METADATA_SIZE, 0);
        let mut client = duplex_with_input(&input);
        assert_eq!(
            read_message(&mut client, test_peer())
                .await
                .expect("inclusive")
                .len(),
            DHT_PEER_WIRE_MAX_METADATA_SIZE
        );

        let input = ((DHT_PEER_WIRE_MAX_METADATA_SIZE + 1) as u32).to_be_bytes();
        let mut client = duplex_with_input(&input);
        assert!(matches!(
            read_message(&mut client, test_peer()).await,
            Err(DhtPeerWireMetaInfoRequesterError::MessageTooLong { length })
                if length == DHT_PEER_WIRE_MAX_METADATA_SIZE + 1
        ));
    }

    #[test]
    fn metadata_header_retains_payload_and_is_strict() {
        let encoded = b"d8:msg_typei1e5:piecei0e10:total_sizei1ee\xc3";
        let (header, payload) = decode_metadata_message(encoded).expect("header");
        assert_eq!(
            header,
            MetadataHeader {
                message_type: 1,
                piece: 0,
                total_size: Some(1)
            }
        );
        assert_eq!(payload, b"\xc3");
        assert!(matches!(
            decode_metadata_message(b"d8:msg_typei1e8:msg_typei1e5:piecei0ee"),
            Err(DhtPeerWireMetaInfoRequesterError::Bencode(_))
        ));
    }

    #[test]
    fn socket_types_are_available() {
        let _ = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1);
    }

    struct FailingWriteStream;

    impl AsyncRead for FailingWriteStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for FailingWriteStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::other("extension write sentinel")))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn extension_handshake_write_error_is_propagated() {
        assert!(matches!(
            perform_extension_handshake(&mut FailingWriteStream, test_peer()).await,
            Err(DhtPeerWireMetaInfoRequesterError::Io {
                stage: DhtPeerWireMetaInfoRequesterStage::ExtensionHandshakeWrite,
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn connect_and_whole_request_timeouts_are_distinct_and_deterministic() {
        let connector = DhtPeerWireMetaInfoRequester::with_config(
            peer_id(0x20),
            DhtPeerWireMetaInfoRequesterConfig {
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(5),
            },
        );
        let task = tokio::spawn(async move {
            connector
                .request_ipv4_with_connector(
                    id(0),
                    test_peer(),
                    pending::<Result<DuplexStream, DhtPeerWireMetaInfoRequesterError>>(),
                )
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            task.await.expect("connect timeout task"),
            Err(DhtPeerWireMetaInfoRequesterError::ConnectTimeout { timeout, .. })
                if timeout == Duration::from_secs(2)
        ));

        let requester = DhtPeerWireMetaInfoRequester::with_config(
            peer_id(0x20),
            DhtPeerWireMetaInfoRequesterConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
            },
        );
        let task = tokio::spawn(async move {
            requester
                .request_ipv4_with_connector(
                    id(0),
                    test_peer(),
                    pending::<Result<DuplexStream, DhtPeerWireMetaInfoRequesterError>>(),
                )
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            task.await.expect("request timeout task"),
            Err(DhtPeerWireMetaInfoRequesterError::RequestTimeout { timeout, .. })
                if timeout == Duration::from_secs(2)
        ));
    }

    struct DropPending {
        dropped: Arc<AtomicBool>,
    }

    impl Future for DropPending {
        type Output = Result<DuplexStream, DhtPeerWireMetaInfoRequesterError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for DropPending {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_request_future_cancels_connector_without_retry() {
        let dropped = Arc::new(AtomicBool::new(false));
        let requester = DhtPeerWireMetaInfoRequester::new(peer_id(0x20));
        let connector = DropPending {
            dropped: Arc::clone(&dropped),
        };
        let task = tokio::spawn(async move {
            requester
                .request_ipv4_with_connector(id(0), test_peer(), connector)
                .await
        });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.expect_err("aborted requester").is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));

        let requester = DhtPeerWireMetaInfoRequester::new(peer_id(0x20));
        let future = requester.request(id(0), SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 1));
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&future);
    }
}
