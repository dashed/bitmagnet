use std::net::SocketAddr;

use crate::{
    ByteString, DatagramSender, MessageArgs, PendingTransaction, RegisterError, RegisteredQuery,
    TransactionIdIssuer, TransactionRegistry, WireError,
};

/// The three failure boundaries before an outbound query becomes pending.
#[derive(Debug, thiserror::Error)]
pub enum QuerySendError<E> {
    /// No transaction registration could be acquired. The sender is untouched.
    #[error("could not register KRPC query: {0}")]
    Register(#[source] RegisterError),
    /// The registered query could not be encoded. Its registration is removed.
    #[error("could not encode KRPC query: {0}")]
    Encode(#[source] WireError),
    /// The sender's original error value. Its registration is removed.
    #[error("could not send KRPC query: {0}")]
    Transport(E),
}

/// Atomically register, fully encode, and send one KRPC query.
///
/// The registration exists before the sender is invoked and remains owned by
/// this future until the exact send succeeds. Any error, cancellation, unwind,
/// or dropped future therefore removes the generation-specific registration
/// through `RegisteredQuery`'s guard. This helper owns no timeout or retry.
pub async fn register_and_send_query<S, I>(
    registry: &TransactionRegistry<I>,
    sender: &mut S,
    remote: SocketAddr,
    query: ByteString,
    args: MessageArgs,
) -> Result<PendingTransaction, QuerySendError<S::Error>>
where
    S: DatagramSender,
    I: TransactionIdIssuer,
{
    register_encode_and_send_query(registry, sender, remote, query, args, RegisteredQuery::wire)
        .await
}

async fn register_encode_and_send_query<S, I>(
    registry: &TransactionRegistry<I>,
    sender: &mut S,
    remote: SocketAddr,
    query: ByteString,
    args: MessageArgs,
    encode: impl FnOnce(&RegisteredQuery) -> Result<Vec<u8>, WireError>,
) -> Result<PendingTransaction, QuerySendError<S::Error>>
where
    S: DatagramSender,
    I: TransactionIdIssuer,
{
    let registered = registry
        .register(remote, query, args)
        .map_err(QuerySendError::Register)?;
    let wire = encode(&registered).map_err(QuerySendError::Encode)?;
    sender
        .send(registered.remote(), &wire)
        .await
        .map_err(QuerySendError::Transport)?;
    Ok(registered.mark_sent())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use super::*;
    use crate::{Id20, TransactionId, TransactionIdSourceError};

    struct OneIssuer;

    impl TransactionIdIssuer for OneIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            Ok(TransactionId::from(*b"A1"))
        }
    }

    struct CountingSender(usize);

    impl DatagramSender for CountingSender {
        type Error = std::convert::Infallible;

        fn send<'a>(
            &'a mut self,
            _destination: SocketAddr,
            _datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            self.0 += 1;
            Box::pin(async { Ok(()) })
        }
    }

    fn args() -> MessageArgs {
        MessageArgs {
            id: Id20::ZERO,
            info_hash: None,
            target: None,
            token: ByteString::default(),
            port: None,
            implied_port: false,
            want: None,
            no_seed: 0,
            scrape: 0,
        }
    }

    #[tokio::test]
    async fn encode_failure_drops_registration_before_sender_creation() {
        let registry = TransactionRegistry::new(OneIssuer);
        let mut sender = CountingSender(0);
        let result = register_encode_and_send_query(
            &registry,
            &mut sender,
            "192.0.2.1:1".parse().unwrap(),
            ByteString::new(b"ping"),
            args(),
            |_| Err(WireError::Invalid("synthetic encode failure".to_owned())),
        )
        .await;
        assert!(matches!(result, Err(QuerySendError::Encode(_))));
        assert_eq!(sender.0, 0);
        assert_eq!(registry.pending_count(), 0);
    }
}
