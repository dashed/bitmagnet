use crate::{
    send_ping_find_node_reply, DatagramReceiver, DatagramSender, NodeTable,
    PingFindNodeDispatchOutcome, PingFindNodeDispatcher, PingFindNodeSendError,
    ReceiveDispatchError, ReceiveDispatchOutcome, ReceiveDispatcher, TransactionIdIssuer,
    TransactionRegistry,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// One completed no-socket receive/dispatch/send step.
#[derive(Debug, PartialEq, Eq)]
pub enum PingFindNodeDriverOutcome {
    /// The receive result required no reply. An unowned query remains intact
    /// for a future router.
    NoReply(ReceiveDispatchOutcome),
    /// The exact prepared normal or local-failure dispatch was sent. Its reply
    /// and any local cause remain structurally inseparable.
    Sent(Box<PingFindNodeDispatchOutcome>),
}

/// The two failing boundaries of one driver step.
#[derive(Debug)]
pub enum PingFindNodeDriverError<R, S> {
    /// The one receive attempt failed before dispatch.
    Receive(ReceiveDispatchError<R>),
    /// A fully composed reply could not be encoded or sent. The owned reply
    /// and any local responder cause remain structurally inseparable.
    Send {
        prepared: Box<PingFindNodeDispatchOutcome>,
        error: PingFindNodeSendError<S>,
    },
}

impl<R, S> Display for PingFindNodeDriverError<R, S>
where
    R: Display,
    S: Display,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receive(error) => write!(formatter, "ping/find-node receive failed: {error}"),
            Self::Send { error, .. } => write!(formatter, "ping/find-node send failed: {error}"),
        }
    }
}

impl<R, S> Error for PingFindNodeDriverError<R, S>
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

/// A fakeable one-datagram driver for only `ping` and `find_node` queries.
pub struct PingFindNodeDriver<'a, R, S, I> {
    receiver: ReceiveDispatcher<R, I>,
    sender: S,
    dispatcher: PingFindNodeDispatcher<'a>,
}

impl<'a, R, S, I> PingFindNodeDriver<'a, R, S, I> {
    #[must_use]
    pub fn new(
        receiver: R,
        registry: TransactionRegistry<I>,
        sender: S,
        table: &'a NodeTable,
    ) -> Self {
        Self {
            receiver: ReceiveDispatcher::new(receiver, registry),
            sender,
            dispatcher: PingFindNodeDispatcher::new(table),
        }
    }
}

impl<R, S, I> PingFindNodeDriver<'_, R, S, I>
where
    R: DatagramReceiver,
    S: DatagramSender,
    I: TransactionIdIssuer,
{
    /// Receive exactly one datagram and send at most one owned-method reply.
    ///
    /// No loop, retry, timeout, detached task, or socket lifecycle is owned by
    /// this step. Backpressure is inherited by awaiting the exact sender call.
    pub async fn drive_one(
        &mut self,
    ) -> Result<PingFindNodeDriverOutcome, PingFindNodeDriverError<R::Error, S::Error>> {
        let received = self
            .receiver
            .receive_one()
            .await
            .map_err(PingFindNodeDriverError::Receive)?;
        let ReceiveDispatchOutcome::Query { source, message } = received else {
            return Ok(PingFindNodeDriverOutcome::NoReply(received));
        };
        let Some(dispatched) = self.dispatcher.dispatch(source, &message) else {
            return Ok(PingFindNodeDriverOutcome::NoReply(
                ReceiveDispatchOutcome::Query { source, message },
            ));
        };

        let reply = match &dispatched {
            PingFindNodeDispatchOutcome::Reply(reply)
            | PingFindNodeDispatchOutcome::LocalFailure { reply, .. } => reply,
        };
        if let Err(error) = send_ping_find_node_reply(&mut self.sender, reply).await {
            return Err(PingFindNodeDriverError::Send {
                prepared: Box::new(dispatched),
                error,
            });
        }
        Ok(PingFindNodeDriverOutcome::Sent(Box::new(dispatched)))
    }
}
