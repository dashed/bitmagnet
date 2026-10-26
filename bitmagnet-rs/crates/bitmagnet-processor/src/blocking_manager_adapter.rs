//! Processor projection for the persistent blocking manager.
//!
//! The processor owns its narrow pre-transaction [`BlockingManager`] trait. The
//! concrete persistent manager implements that trait directly so an application
//! can share one manager with the crawler adapter now and with later API and
//! shutdown wiring without another processor-specific wrapper.

use std::error::Error;
use std::future::Future;

use bitmagnet_blocking::BlockingManager as PersistentBlockingManager;
use bitmagnet_model::InfoHash;
use futures::future::BoxFuture;

use crate::{BlockingManager, BoxError};

impl BlockingManager for PersistentBlockingManager {
    fn block<'a>(&'a self, info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            adapt_block(info_hashes, |model_hashes, flush| async move {
                PersistentBlockingManager::block(self, &model_hashes, flush).await
            })
            .await
        })
    }
}

async fn adapt_block<F, Fut, E>(info_hashes: &[String], delegate: F) -> Result<(), BoxError>
where
    F: FnOnce(Vec<InfoHash>, bool) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Error + Send + Sync + 'static,
{
    let model_hashes = info_hashes
        .iter()
        .map(|info_hash| info_hash.parse())
        .collect::<Result<Vec<InfoHash>, _>>()
        .map_err(|error| Box::new(error) as BoxError)?;

    delegate(model_hashes, false)
        .await
        .map_err(|error| Box::new(error) as BoxError)
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bitmagnet_model::InfoHashError;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const HASH_A: &str = "1111111111111111111111111111111111111111";
    const HASH_B: &str = "2222222222222222222222222222222222222222";

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[tokio::test]
    async fn delegate_is_called_once_with_false_and_preserves_order_and_duplicates() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let hashes = vec![HASH_A.to_owned(), HASH_B.to_owned(), HASH_A.to_owned()];

        adapt_block(&hashes, move |model_hashes, flush| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            assert!(!flush);
            assert_eq!(
                model_hashes,
                vec![
                    HASH_A.parse().unwrap(),
                    HASH_B.parse().unwrap(),
                    HASH_A.parse().unwrap(),
                ]
            );
            async { Ok::<_, TestError>(()) }
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn invalid_hash_fails_before_delegate() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let hashes = vec![HASH_A.to_owned(), "not-an-info-hash".to_owned()];

        let error = adapt_block(&hashes, move |_, _| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, TestError>(()) }
        })
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(error.downcast::<InfoHashError>().is_ok());
    }

    #[tokio::test]
    async fn delegate_error_is_boxed_without_retyping_or_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let error = adapt_block(&[HASH_A.to_owned()], move |_, _| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(TestError("exact delegate error")) }
        })
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let error = error.downcast::<TestError>().unwrap();
        assert_eq!(error.0, "exact delegate error");
    }

    #[tokio::test]
    async fn empty_input_is_delegated_once_with_false() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();

        adapt_block(&[], move |model_hashes, flush| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            assert!(model_hashes.is_empty());
            assert!(!flush);
            async { Ok::<_, TestError>(()) }
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn manager_is_send_sync_and_usable_as_shared_processor_trait_object() {
        assert_send_sync::<PersistentBlockingManager>();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let manager = Arc::new(PersistentBlockingManager::new(pool));
        let collaborator: Arc<dyn BlockingManager> = manager.clone();

        drop(collaborator);
        assert_eq!(Arc::strong_count(&manager), 1);
    }
}
