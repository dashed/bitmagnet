//! Go-compatible per-address admission in front of peer-wire requests.
//!
//! The limiter is deliberately an outer decorator: admission wait time is not
//! charged to the peer-wire request timeout owned by the inner requester.

use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::{DhtOutboundRateLimiter, Id20};

use crate::{DhtMetaInfoRequester, RequestMetaInfoCollaboratorError};

pub(crate) const METAINFO_RATE_LIMIT_INTERVAL: Duration = Duration::from_millis(500);

/// Shared outer admission policy for a metainfo requester.
#[derive(Clone)]
pub(crate) struct DhtRateLimitedMetaInfoRequester<R> {
    requester: R,
    limiter: DhtOutboundRateLimiter,
}

impl<R> DhtRateLimitedMetaInfoRequester<R> {
    pub(crate) fn new(requester: R) -> Self {
        let limiter = DhtOutboundRateLimiter::try_with_interval(METAINFO_RATE_LIMIT_INTERVAL)
            .expect("the fixed metainfo limiter is faster than the DHT default");
        Self { requester, limiter }
    }
}

#[async_trait]
impl<R> DhtMetaInfoRequester for DhtRateLimitedMetaInfoRequester<R>
where
    R: DhtMetaInfoRequester,
{
    #[cfg(test)]
    fn peer_wire_config_for_test(&self) -> Option<crate::DhtPeerWireMetaInfoRequesterConfig> {
        self.requester.peer_wire_config_for_test()
    }

    #[cfg(test)]
    fn is_rate_limited_for_test(&self) -> bool {
        true
    }

    async fn request(
        &self,
        info_hash: Id20,
        peer: std::net::SocketAddr,
    ) -> Result<bitmagnet_metainfo::ParsedInfo, RequestMetaInfoCollaboratorError> {
        self.limiter.wait(peer).await;
        self.requester.request(info_hash, peer).await
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{DhtPeerWireMetaInfoRequester, DhtPeerWireMetaInfoRequesterError};

    #[derive(Clone, Default)]
    struct RecordingRequester {
        calls: Arc<Mutex<Vec<SocketAddr>>>,
    }

    impl RecordingRequester {
        fn calls(&self) -> Vec<SocketAddr> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtMetaInfoRequester for RecordingRequester {
        async fn request(
            &self,
            _info_hash: Id20,
            peer: SocketAddr,
        ) -> Result<bitmagnet_metainfo::ParsedInfo, RequestMetaInfoCollaboratorError> {
            self.calls.lock().unwrap().push(peer);
            Err(Box::new(io::Error::other("delegate reached")))
        }
    }

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
    }

    fn peer(ip: Ipv4Addr, port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(ip, port))
    }

    async fn request_error<R>(requester: &R, peer: SocketAddr)
    where
        R: DhtMetaInfoRequester,
    {
        requester
            .request(id(1), peer)
            .await
            .expect_err("the test delegate always fails");
    }

    #[tokio::test(start_paused = true)]
    async fn exact_shared_policy_limits_same_ip_and_keeps_distinct_ips_independent() {
        let delegate = RecordingRequester::default();
        let requester = DhtRateLimitedMetaInfoRequester::new(delegate.clone());
        let first_ip = Ipv4Addr::new(192, 0, 2, 1);

        for port in 1..=4 {
            request_error(&requester, peer(first_ip, port)).await;
        }
        assert_eq!(delegate.calls().len(), 4);

        let fifth = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(2), peer(first_ip, 5)).await }
        });
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        assert_eq!(delegate.calls().len(), 4);

        request_error(&requester, peer(Ipv4Addr::new(192, 0, 2, 2), 1)).await;
        assert_eq!(delegate.calls().len(), 5);

        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        assert_eq!(delegate.calls().len(), 5);

        tokio::time::advance(Duration::from_millis(1)).await;
        fifth
            .await
            .expect("fifth task")
            .expect_err("the delegate is reached at 500ms");
        assert_eq!(delegate.calls().len(), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn clones_share_admission_but_separate_decorators_do_not() {
        let shared_delegate = RecordingRequester::default();
        let requester = DhtRateLimitedMetaInfoRequester::new(shared_delegate.clone());
        let peer = peer(Ipv4Addr::new(198, 51, 100, 1), 6_881);
        for _ in 0..4 {
            request_error(&requester, peer).await;
        }

        let blocked = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(3), peer).await }
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        let separate_delegate = RecordingRequester::default();
        let separate = DhtRateLimitedMetaInfoRequester::new(separate_delegate.clone());
        request_error(&separate, peer).await;
        assert_eq!(separate_delegate.calls(), vec![peer]);

        blocked.abort();
        assert!(blocked.await.expect_err("aborted wait").is_cancelled());
        assert_eq!(shared_delegate.calls().len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_blocked_wait_rolls_back_without_calling_the_delegate() {
        let delegate = RecordingRequester::default();
        let requester = DhtRateLimitedMetaInfoRequester::new(delegate.clone());
        let peer = peer(Ipv4Addr::new(203, 0, 113, 1), 6_881);
        for _ in 0..4 {
            request_error(&requester, peer).await;
        }

        let cancelled = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(4), peer).await }
        });
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.expect_err("aborted wait").is_cancelled());
        assert_eq!(delegate.calls().len(), 4);

        let replacement = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(5), peer).await }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        assert_eq!(delegate.calls().len(), 4);

        tokio::time::advance(Duration::from_millis(1)).await;
        replacement
            .await
            .expect("replacement task")
            .expect_err("delegate reached after one refill");
        assert_eq!(delegate.calls().len(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn admission_wait_is_outside_the_inner_request_budget() {
        #[derive(Clone, Default)]
        struct BudgetedRequester {
            calls: Arc<Mutex<Vec<SocketAddr>>>,
        }

        #[async_trait]
        impl DhtMetaInfoRequester for BudgetedRequester {
            async fn request(
                &self,
                _info_hash: Id20,
                peer: SocketAddr,
            ) -> Result<bitmagnet_metainfo::ParsedInfo, RequestMetaInfoCollaboratorError>
            {
                self.calls.lock().unwrap().push(peer);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Err(Box::new(io::Error::other("inner budget elapsed")))
            }
        }

        let delegate = BudgetedRequester::default();
        let requester = DhtRateLimitedMetaInfoRequester::new(delegate.clone());
        let peer = peer(Ipv4Addr::new(192, 0, 2, 20), 6_881);
        let initial = (0..4)
            .map(|_| {
                let requester = requester.clone();
                tokio::spawn(async move { requester.request(id(8), peer).await })
            })
            .collect::<Vec<_>>();
        tokio::task::yield_now().await;
        assert_eq!(delegate.calls.lock().unwrap().len(), 4);
        tokio::time::advance(Duration::from_millis(100)).await;
        for task in initial {
            task.await
                .expect("initial task")
                .expect_err("initial inner budget");
        }

        let fifth = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(9), peer).await }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(399)).await;
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        assert_eq!(delegate.calls.lock().unwrap().len(), 4);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        assert_eq!(delegate.calls.lock().unwrap().len(), 5);

        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        fifth
            .await
            .expect("fifth task")
            .expect_err("inner budget starts only after admission");
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_runs_before_the_inner_ipv4_only_check() {
        let requester =
            DhtRateLimitedMetaInfoRequester::new(DhtPeerWireMetaInfoRequester::new(id(0x20)));
        let peer = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 6_881);
        for _ in 0..4 {
            let error = requester.request(id(6), peer).await.unwrap_err();
            assert!(error
                .downcast_ref::<DhtPeerWireMetaInfoRequesterError>()
                .is_some_and(|error| matches!(
                    error,
                    DhtPeerWireMetaInfoRequesterError::UnsupportedAddressFamily(_)
                )));
        }

        let fifth = tokio::spawn({
            let requester = requester.clone();
            async move { requester.request(id(7), peer).await }
        });
        tokio::task::yield_now().await;
        assert!(!fifth.is_finished());
        tokio::time::advance(METAINFO_RATE_LIMIT_INTERVAL).await;
        let error = fifth
            .await
            .expect("fifth IPv6 task")
            .expect_err("inner requester still rejects IPv6");
        assert!(error
            .downcast_ref::<DhtPeerWireMetaInfoRequesterError>()
            .is_some_and(|error| matches!(
                error,
                DhtPeerWireMetaInfoRequesterError::UnsupportedAddressFamily(_)
            )));
    }
}
