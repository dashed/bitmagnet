use crate::{DatagramSender, DhtReply, WireError};

/// The two failure boundaries for one prepared DHT reply send.
#[derive(Debug, thiserror::Error)]
pub enum DhtSendError<E> {
    /// Encoding completes before a sender future is created.
    #[error("could not encode DHT reply: {0}")]
    Encode(#[source] WireError),
    /// The sender's original error value, with no retry or translation.
    #[error("could not send DHT reply: {0}")]
    Transport(E),
}

/// Encode one borrowed reply completely, then await exactly one datagram send.
///
/// Backpressure and cancellation are inherited directly from that one sender
/// future. This helper performs no retry and neither consumes nor mutates the
/// reply.
pub async fn send_dht_reply<S>(
    sender: &mut S,
    reply: &DhtReply,
) -> Result<(), DhtSendError<S::Error>>
where
    S: DatagramSender,
{
    let wire = reply.wire().map_err(DhtSendError::Encode)?;
    sender
        .send(reply.destination, &wire)
        .await
        .map_err(DhtSendError::Transport)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;

    use super::*;
    use crate::{ByteString, CompactAddr, CompactNode, Id20, KrpcMessage, MessageReturn};

    #[derive(Debug, PartialEq, Eq)]
    struct SendFailure(u8);

    struct Sender {
        calls: Vec<(SocketAddr, Vec<u8>)>,
        result: Option<Result<(), SendFailure>>,
    }

    struct OpaqueError;

    struct OpaqueSender;

    impl DatagramSender for OpaqueSender {
        type Error = OpaqueError;

        fn send<'a>(
            &'a mut self,
            _destination: SocketAddr,
            _datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Err(OpaqueError) })
        }
    }

    impl DatagramSender for Sender {
        type Error = SendFailure;

        fn send<'a>(
            &'a mut self,
            destination: SocketAddr,
            datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            self.calls.push((destination, datagram.to_vec()));
            let result = self.result.take().expect("one expected send");
            Box::pin(async move { result })
        }
    }

    fn reply() -> DhtReply {
        DhtReply {
            destination: "192.0.2.1:6881".parse().unwrap(),
            message: KrpcMessage {
                transaction_id: ByteString::new(b"t1".to_vec()),
                message_type: ByteString::new(b"r".to_vec()),
                query: ByteString::default(),
                args: None,
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
                error: None,
                observed_addr: None,
                read_only: false,
                client_id: ByteString::default(),
            },
        }
    }

    #[test]
    fn sender_error_needs_no_debug_display_or_error_bound() {
        let mut sender = OpaqueSender;
        let reply = reply();
        let future = send_dht_reply(&mut sender, &reply);
        drop(future);
    }

    #[tokio::test]
    async fn public_send_api_uses_one_send_and_preserves_transport_value() {
        let reply = reply();
        let mut sender = Sender {
            calls: Vec::new(),
            result: Some(Err(SendFailure(7))),
        };
        let error = send_dht_reply(&mut sender, &reply).await.unwrap_err();
        assert!(matches!(error, DhtSendError::Transport(SendFailure(7))));
        assert_eq!(sender.calls.len(), 1);
        assert_eq!(sender.calls[0].0, reply.destination);
        assert_eq!(sender.calls[0].1, reply.wire().unwrap());
    }

    #[tokio::test]
    async fn encode_failure_never_creates_a_sender_future() {
        let mut reply = reply();
        reply.message.response.as_mut().unwrap().nodes = Some(vec![CompactNode {
            id: Id20::ZERO,
            addr: CompactAddr {
                ip: "2001:db8::1".parse().unwrap(),
                port: 6881,
            },
        }]);
        let mut sender = Sender {
            calls: Vec::new(),
            result: Some(Ok(())),
        };
        assert!(matches!(
            send_dht_reply(&mut sender, &reply).await,
            Err(DhtSendError::Encode(_))
        ));
        assert!(sender.calls.is_empty());
    }
}
