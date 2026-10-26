use std::future::Future;
use std::num::NonZeroU8;

use crate::{
    DatagramReceiver, DatagramSender, DhtDriver, DhtDriverError, DhtDriverOutcome,
    DhtResponderTable, KTable, TransactionIdIssuer,
};

/// The bounded terminal state of one finite full-DHT supervisor batch.
#[derive(Debug)]
pub enum DhtSupervisorExit<R, S> {
    /// The exact caller-supplied number of steps completed successfully.
    BudgetExhausted { completed: Vec<DhtDriverOutcome> },
    /// Shutdown won before another step completed.
    Shutdown { completed: Vec<DhtDriverOutcome> },
    /// One receive, encode, or send boundary failed after the retained prefix.
    Failed {
        completed: Vec<DhtDriverOutcome>,
        error: DhtDriverError<R, S>,
    },
}

/// A finite, sequential supervisor around the full-DHT driver.
pub struct DhtSupervisor<R, S, I, T = KTable> {
    driver: DhtDriver<R, S, I, T>,
}

impl<R, S, I, T> DhtSupervisor<R, S, I, T> {
    /// Construct a supervisor around one already-configured full-DHT driver.
    #[must_use]
    pub const fn from_driver(driver: DhtDriver<R, S, I, T>) -> Self {
        Self { driver }
    }
}

impl<R, S, I, T> DhtSupervisor<R, S, I, T>
where
    R: DatagramReceiver,
    S: DatagramSender,
    I: TransactionIdIssuer,
    T: DhtResponderTable,
{
    /// Drive at most `budget` datagrams, sequentially, until one typed boundary
    /// stops the batch.
    ///
    /// Shutdown is the first biased branch for every step. Winning shutdown
    /// drops the in-flight driver future, so admitted receiver and sender
    /// futures must remain reusable after cancellation. A cancelled send is not
    /// a completed outcome and is never retried. A settled send completes before
    /// the next receive begins. Every successful driver outcome consumes one
    /// unit of budget; a driver failure stops and does not enter the prefix.
    pub async fn drive_batch<F>(
        &mut self,
        budget: NonZeroU8,
        shutdown: F,
    ) -> DhtSupervisorExit<R::Error, S::Error>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut completed = Vec::with_capacity(usize::from(budget.get()));

        while completed.len() < usize::from(budget.get()) {
            let step = tokio::select! {
                biased;
                () = &mut shutdown => {
                    return DhtSupervisorExit::Shutdown { completed };
                }
                step = self.driver.drive_one() => step,
            };

            match step {
                Ok(outcome) => completed.push(outcome),
                Err(error) => return DhtSupervisorExit::Failed { completed, error },
            }
        }

        DhtSupervisorExit::BudgetExhausted { completed }
    }
}
