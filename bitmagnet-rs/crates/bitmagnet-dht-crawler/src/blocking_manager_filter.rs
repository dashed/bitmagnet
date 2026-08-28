//! Thin crawler projections for the persistent blocking manager.
//!
//! The crawler owns [`DhtInfoHashBlockFilter`] and [`DhtInfoHashBlocker`], so
//! implementing both directly for [`BlockingManager`] keeps the public surface
//! smaller than wrappers while still allowing one application-owned manager to
//! serve both trait objects. The application remains responsible for
//! constructing and sharing the manager and for its final flush lifecycle.

use std::error::Error;
use std::future::Future;

use async_trait::async_trait;
use bitmagnet_blocking::BlockingManager;
use bitmagnet_dht::Id20;
use bitmagnet_model::InfoHash;

use crate::{
    DhtInfoHashBlockFilter, DhtInfoHashBlocker, RequestMetaInfoCollaboratorError,
    TriageCollaboratorError,
};

#[async_trait]
impl DhtInfoHashBlockFilter for BlockingManager {
    async fn filter(&self, info_hashes: &[Id20]) -> Result<Vec<Id20>, TriageCollaboratorError> {
        adapt_filter(info_hashes, |model_hashes| async move {
            BlockingManager::filter(self, &model_hashes).await
        })
        .await
    }
}

#[async_trait]
impl DhtInfoHashBlocker for BlockingManager {
    async fn block(
        &self,
        info_hashes: &[Id20],
        flush: bool,
    ) -> Result<(), RequestMetaInfoCollaboratorError> {
        adapt_block(info_hashes, flush, |model_hashes, flush| async move {
            BlockingManager::block(self, &model_hashes, flush).await
        })
        .await
    }
}

async fn adapt_filter<F, Fut, E>(
    info_hashes: &[Id20],
    delegate: F,
) -> Result<Vec<Id20>, TriageCollaboratorError>
where
    F: FnOnce(Vec<InfoHash>) -> Fut,
    Fut: Future<Output = Result<Vec<InfoHash>, E>>,
    E: Error + Send + Sync + 'static,
{
    let model_hashes = info_hashes.iter().copied().map(id20_to_info_hash).collect();
    let eligible = delegate(model_hashes)
        .await
        .map_err(|error| Box::new(error) as TriageCollaboratorError)?;
    Ok(eligible.into_iter().map(info_hash_to_id20).collect())
}

async fn adapt_block<F, Fut, E>(
    info_hashes: &[Id20],
    flush: bool,
    delegate: F,
) -> Result<(), RequestMetaInfoCollaboratorError>
where
    F: FnOnce(Vec<InfoHash>, bool) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Error + Send + Sync + 'static,
{
    let model_hashes = info_hashes.iter().copied().map(id20_to_info_hash).collect();
    delegate(model_hashes, flush)
        .await
        .map_err(|error| Box::new(error) as RequestMetaInfoCollaboratorError)
}

fn id20_to_info_hash(info_hash: Id20) -> InfoHash {
    InfoHash::new(*info_hash.as_bytes())
}

fn info_hash_to_id20(info_hash: InfoHash) -> Id20 {
    Id20::from_slice(info_hash.as_slice())
        .expect("InfoHash and Id20 have the same fixed 20-byte width")
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[derive(Debug)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("test blocking error")
        }
    }

    impl Error for TestError {}

    fn id(bytes: [u8; 20]) -> Id20 {
        Id20::from_slice(&bytes).unwrap()
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn conversions_are_lossless_for_all_byte_values() {
        for offset in 0..=u8::MAX {
            let mut bytes = [0_u8; 20];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = offset.wrapping_add(index as u8);
            }
            let original = id(bytes);
            assert_eq!(info_hash_to_id20(id20_to_info_hash(original)), original);
        }
    }

    #[tokio::test]
    async fn delegate_is_called_once_and_output_order_and_duplicates_are_preserved() {
        let first = id([0x11; 20]);
        let second = id([0x22; 20]);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();

        let eligible = adapt_filter(&[first, second, first], move |model_hashes| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(
                model_hashes,
                vec![
                    id20_to_info_hash(first),
                    id20_to_info_hash(second),
                    id20_to_info_hash(first),
                ]
            );
            async move {
                Ok::<_, TestError>(vec![
                    id20_to_info_hash(second),
                    id20_to_info_hash(first),
                    id20_to_info_hash(second),
                ])
            }
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(eligible, vec![second, first, second]);
    }

    #[tokio::test]
    async fn delegate_error_is_boxed_without_retyping() {
        let error = adapt_filter(&[id([0x33; 20])], |_| async {
            Err::<Vec<InfoHash>, _>(TestError)
        })
        .await
        .unwrap_err();

        assert!(error.downcast::<TestError>().is_ok());
    }

    #[tokio::test]
    async fn block_delegate_preserves_order_duplicates_and_flush() {
        let first = id([0x44; 20]);
        let second = id([0x55; 20]);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();

        adapt_block(&[first, second, first], true, move |model_hashes, flush| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            assert!(flush);
            assert_eq!(
                model_hashes,
                vec![
                    id20_to_info_hash(first),
                    id20_to_info_hash(second),
                    id20_to_info_hash(first),
                ]
            );
            async { Ok::<_, TestError>(()) }
        })
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn block_delegate_forwards_false_and_boxes_error_without_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();

        let error = adapt_block(&[id([0x66; 20])], false, move |_, flush| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            assert!(!flush);
            async { Err::<(), _>(TestError) }
        })
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(error.downcast::<TestError>().is_ok());
    }

    #[tokio::test]
    async fn manager_is_send_sync_and_usable_as_shared_crawler_trait_object() {
        assert_send_sync::<BlockingManager>();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let manager = Arc::new(BlockingManager::new(pool));
        let filter: Arc<dyn DhtInfoHashBlockFilter> = manager.clone();
        let blocker: Arc<dyn DhtInfoHashBlocker> = manager.clone();

        drop((filter, blocker));
        assert_eq!(Arc::strong_count(&manager), 1);
    }
}
