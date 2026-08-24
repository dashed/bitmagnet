use std::net::SocketAddr;

use crate::{ByteString, KrpcError, KrpcMessage, MessageReturn, WireError};

const SERVER_ERROR: i64 = 202;
const TOO_MANY_REQUESTS: i64 = 201;

/// One fully composed DHT response envelope and its exact reply destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtReply {
    pub destination: SocketAddr,
    pub message: KrpcMessage,
}

impl DhtReply {
    /// Compose Go's exact inbound-overload response for an arbitrary transaction ID.
    ///
    /// Go intentionally emits this protocol error in a `y=r` envelope rather
    /// than a `y=e` envelope. The reply carries only `e`, `t`, and `y`: all
    /// request fields and return data are cleared.
    #[must_use]
    pub fn too_many_requests(destination: SocketAddr, transaction_id: ByteString) -> Self {
        compose_reply_for_transaction(
            destination,
            transaction_id,
            None,
            Some(KrpcError {
                code: TOO_MANY_REQUESTS,
                message: ByteString::new(b"too many requests".to_vec()),
            }),
        )
    }

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
    compose_reply_for_transaction(destination, request.transaction_id.clone(), response, error)
}

fn compose_reply_for_transaction(
    destination: SocketAddr,
    transaction_id: ByteString,
    response: Option<MessageReturn>,
    error: Option<KrpcError>,
) -> DhtReply {
    DhtReply {
        destination,
        message: KrpcMessage {
            transaction_id,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_many_requests_preserves_empty_binary_and_long_transaction_ids() {
        let destination = "203.0.113.9:6999".parse().unwrap();
        let cases = [
            (
                Vec::new(),
                Some("64313a656c693230316531373a746f6f206d616e7920726571756573747365313a74303a313a79313a7265"),
            ),
            (
                vec![0, 255],
                Some("64313a656c693230316531373a746f6f206d616e7920726571756573747365313a74323a00ff313a79313a7265"),
            ),
            (vec![0xab; 257], None),
        ];

        for (transaction_id, exact_wire_hex) in cases {
            let reply =
                DhtReply::too_many_requests(destination, ByteString::new(transaction_id.clone()));
            assert_eq!(reply.destination, destination);
            assert_eq!(reply.message.transaction_id.as_bytes(), transaction_id);
            assert_eq!(reply.message.message_type.as_bytes(), b"r");
            assert!(reply.message.query.is_empty());
            assert!(reply.message.args.is_none());
            assert!(reply.message.response.is_none());
            let error = reply.message.error.as_ref().unwrap();
            assert_eq!(error.code, 201);
            assert_eq!(error.message.as_bytes(), b"too many requests");
            assert!(reply.message.observed_addr.is_none());
            assert!(!reply.message.read_only);
            assert!(reply.message.client_id.is_empty());

            let wire = reply.wire().unwrap();
            if let Some(exact_wire_hex) = exact_wire_hex {
                assert_eq!(hex::encode(wire), exact_wire_hex);
            }
        }
    }
}
