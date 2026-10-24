use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{
    send_dht_reply, DatagramReceiver, DatagramSender, DhtDispatchOutcome, DhtDispatcher,
    DhtResponderTable, DhtSendError, KTable, ReceiveDispatchError, ReceiveDispatchOutcome,
    ReceiveDispatcher, TransactionIdIssuer, TransactionRegistry,
};

/// One completed bounded full-DHT receive/dispatch/send step.
#[derive(Debug, PartialEq, Eq)]
pub enum DhtDriverOutcome {
    /// The receive result required no reply.
    NoReply(ReceiveDispatchOutcome),
    /// The exact prepared normal or local-failure dispatch was sent. Its reply
    /// and any local cause remain structurally inseparable.
    Sent(Box<DhtDispatchOutcome>),
}

/// The two failing boundaries of one full-DHT driver step.
#[derive(Debug)]
pub enum DhtDriverError<R, S> {
    /// The one receive attempt failed before dispatch.
    Receive(ReceiveDispatchError<R>),
    /// A fully composed reply could not be encoded or sent. The owned reply
    /// and any local responder cause remain structurally inseparable.
    Send {
        prepared: Box<DhtDispatchOutcome>,
        error: DhtSendError<S>,
    },
}

impl<R, S> Display for DhtDriverError<R, S>
where
    R: Display,
    S: Display,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receive(error) => write!(formatter, "DHT receive failed: {error}"),
            Self::Send { error, .. } => write!(formatter, "DHT send failed: {error}"),
        }
    }
}

impl<R, S> Error for DhtDriverError<R, S>
where
    R: Error + 'static,
    S: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Receive(error) => Some(error),
            Self::Send { error, .. } => Some(error),
        }
    }
}

/// A fakeable one-datagram driver for the complete deployed DHT query surface.
pub struct DhtDriver<R, S, I, T = KTable> {
    receiver: ReceiveDispatcher<R, I>,
    sender: S,
    dispatcher: DhtDispatcher<T>,
}

impl<R, S, I, T> DhtDriver<R, S, I, T> {
    /// Construct a driver around one already-configured full-DHT dispatcher.
    #[must_use]
    pub fn from_dispatcher(
        receiver: R,
        registry: TransactionRegistry<I>,
        sender: S,
        dispatcher: DhtDispatcher<T>,
    ) -> Self {
        Self {
            receiver: ReceiveDispatcher::new(receiver, registry),
            sender,
            dispatcher,
        }
    }
}

impl<R, S, I, T> DhtDriver<R, S, I, T>
where
    R: DatagramReceiver,
    S: DatagramSender,
    I: TransactionIdIssuer,
    T: DhtResponderTable,
{
    /// Receive exactly one datagram and send at most one full-DHT reply.
    ///
    /// No loop, retry, timeout, detached task, logging, or socket lifecycle is
    /// owned by this step. Backpressure and cancellation are inherited by
    /// awaiting the exact receiver and sender calls. Responses and KRPC errors
    /// are delivered by the receive dispatcher and never reach the responder.
    pub async fn drive_one(
        &mut self,
    ) -> Result<DhtDriverOutcome, DhtDriverError<R::Error, S::Error>> {
        let received = self
            .receiver
            .receive_one()
            .await
            .map_err(DhtDriverError::Receive)?;
        let ReceiveDispatchOutcome::Query { source, message } = received else {
            return Ok(DhtDriverOutcome::NoReply(received));
        };

        let dispatched = self.dispatcher.dispatch(source, &message);
        if let Err(error) = send_dht_reply(&mut self.sender, dispatched.reply()).await {
            return Err(DhtDriverError::Send {
                prepared: Box::new(dispatched),
                error,
            });
        }
        Ok(DhtDriverOutcome::Sent(Box::new(dispatched)))
    }
}
