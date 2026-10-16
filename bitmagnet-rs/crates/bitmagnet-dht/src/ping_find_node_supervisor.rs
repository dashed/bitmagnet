use std::future::Future;
use std::num::NonZeroU8;

use crate::{
    DatagramReceiver, DatagramSender, NodeTable, PingFindNodeDriver, PingFindNodeDriverError,
    PingFindNodeDriverOutcome, ReceiveDispatchOutcome, TransactionIdIssuer, TransactionRegistry,
};

/// The bounded terminal state of one finite supervisor batch.
#[derive(Debug)]
pub enum PingFindNodeSupervisorExit<R, S> {
    /// The exact caller-supplied number of steps completed successfully.
    BudgetExhausted {
        completed: Vec<PingFindNodeDriverOutcome>,
    },
    /// Shutdown won before another step completed.
    Shutdown {
        completed: Vec<PingFindNodeDriverOutcome>,
    },
    /// The partial router does not own this intact query. No later datagram was
    /// read, so a future full router can decide how to continue.
    UnownedQuery {
        completed: Vec<PingFindNodeDriverOutcome>,
        query: ReceiveDispatchOutcome,
    },
    /// One receive, encode, or send boundary failed after the retained prefix.
    Failed {
        completed: Vec<PingFindNodeDriverOutcome>,
        error: PingFindNodeDriverError<R, S>,
    },
}

/// A finite, sequential supervisor around the partial ping/find-node driver.
pub struct PingFindNodeSupervisor<'a, R, S, I> {
    driver: PingFindNodeDriver<'a, R, S, I>,
}

impl<'a, R, S, I> PingFindNodeSupervisor<'a, R, S, I> {
    #[must_use]
    pub fn new(
        receiver: R,
        registry: TransactionRegistry<I>,
        sender: S,
        table: &'a NodeTable,
    ) -> Self {
        Self {
            driver: PingFindNodeDriver::new(receiver, registry, sender, table),
        }
    }
}

impl<R, S, I> PingFindNodeSupervisor<'_, R, S, I>
where
    R: DatagramReceiver,
    S: DatagramSender,
    I: TransactionIdIssuer,
{
    /// Drive at most `budget` datagrams, sequentially, until one typed boundary
    /// stops the batch.
    ///
    /// Shutdown is the first biased branch for every step. Winning shutdown
    /// drops the in-flight driver future, so both receiver and sender
    /// implementations admitted to this supervisor must remain reusable when
    /// their returned future is cancelled. A cancelled send is not a completed
    /// outcome and is never retried by this supervisor. A settled send completes
    /// before the next receive begins. Zero-length, malformed, ignored,
    /// response, and error datagrams each consume one unit of budget. An
    /// unowned query and a driver failure stop immediately and do not enter the
    /// completed prefix.
    pub async fn drive_batch<F>(
        &mut self,
        budget: NonZeroU8,
        shutdown: F,
    ) -> PingFindNodeSupervisorExit<R::Error, S::Error>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut completed = Vec::with_capacity(usize::from(budget.get()));

        while completed.len() < usize::from(budget.get()) {
            let step = tokio::select! {
                biased;
                () = &mut shutdown => {
                    return PingFindNodeSupervisorExit::Shutdown { completed };
                }
                step = self.driver.drive_one() => step,
            };

            match step {
                Ok(PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Query {
                    source,
                    message,
                })) => {
                    return PingFindNodeSupervisorExit::UnownedQuery {
                        completed,
                        query: ReceiveDispatchOutcome::Query { source, message },
                    };
                }
                Ok(outcome) => completed.push(outcome),
                Err(error) => {
                    return PingFindNodeSupervisorExit::Failed { completed, error };
                }
            }
        }

        PingFindNodeSupervisorExit::BudgetExhausted { completed }
    }
}
