use std::net::SocketAddr;

use crate::{ByteString, KrpcError, KrpcMessage, MessageReturn, WireError};

const SERVER_ERROR: i64 = 202;

/// One fully composed DHT response envelope and its exact reply destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtReply {
    pub destination: SocketAddr,
    pub message: KrpcMessage,
}

impl DhtReply {
    /// Encode this reply without consuming it.
    pub fn wire(&self) -> Result<Vec<u8>, WireError> {
        self.message.encode()
    }
}

pub(crate) fn compose_response(
    destination: SocketAddr,
    request: &KrpcMessage,
    response: MessageReturn,
) -> DhtReply {
    compose_reply(destination, request, Some(response), None)
}

pub(crate) fn compose_error(
    destination: SocketAddr,
    request: &KrpcMessage,
    error: KrpcError,
) -> DhtReply {
    compose_reply(destination, request, None, Some(error))
}

fn compose_reply(
    destination: SocketAddr,
    request: &KrpcMessage,
    response: Option<MessageReturn>,
    error: Option<KrpcError>,
) -> DhtReply {
    DhtReply {
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

pub(crate) fn generic_server_error() -> KrpcError {
    KrpcError {
        code: SERVER_ERROR,
        message: ByteString::new(b"server error".to_vec()),
    }
}
