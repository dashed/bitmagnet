//! Production-shaped DHT rate policies without a cleanup task.
//!
//! The keyed caches expire entries lazily during later accesses instead of
//! spawning the perpetual cleanup goroutine used by Go's expirable LRU. This is
//! a deliberate lifecycle hardening: capacity remains bounded, expiration is
//! strict, and an idle limiter owns no background work.

use std::collections::{HashMap, VecDeque};
use std::future::{pending, ready, Future};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::time::{sleep_until, Instant};

const KEY_CAPACITY: usize = 1_000;
const KEY_TTL: Duration = Duration::from_secs(20);
const PER_IP_INTERVAL: Duration = Duration::from_secs(1);
const INBOUND_PER_IP_BURST: u32 = 10;
const INBOUND_GLOBAL_INTERVAL: Duration = Duration::from_millis(20);
const INBOUND_GLOBAL_BURST: u32 = 20;
const OUTBOUND_PER_IP_BURST: u32 = 4;
const GO_MAX_RATE_DELAY: Duration = Duration::from_nanos(i64::MAX as u64);

/// Production inbound DHT admission policy.
///
/// Each address has a one-token-per-second bucket with an initial burst of ten.
/// A successful per-address admission is consumed before the shared
/// fifty-per-second, burst-twenty bucket is consulted, exactly matching Go's
/// short-circuit ordering. Ports and IPv6 flow information are never part of
/// the key. IPv4, mapped IPv4, and native IPv6 identities remain distinct, and
/// a nonzero IPv6 scope ID is represented by the same numeric `%N` suffix as
/// Go's `netip.Addr.String` zone.
#[derive(Clone)]
pub struct DhtInboundRateLimiter {
    per_ip: KeyedBuckets,
    global: Arc<TokenBucket>,
}

/// The production inbound policy boundary that denied one query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtInboundRateLimitDenial {
    /// The source-IP bucket had no token available.
    PerIp,
    /// The shared global bucket had no token available after the source-IP
    /// token was consumed.
    Global,
}

impl DhtInboundRateLimiter {
    /// Construct the deployed inbound policy with initially full buckets.
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(
            PER_IP_INTERVAL,
            INBOUND_PER_IP_BURST,
            KEY_CAPACITY,
            KEY_TTL,
            INBOUND_GLOBAL_INTERVAL,
            INBOUND_GLOBAL_BURST,
        )
    }

    /// Consume one inbound admission when both the address and global policies
    /// allow it now.
    ///
    /// A per-address token remains consumed when the later global check rejects
    /// the request. A per-address rejection does not touch the global bucket.
    pub fn allow(&self, addr: SocketAddr) -> bool {
        self.admit(addr).is_ok()
    }

    /// Consume one inbound admission or identify the exact denying policy.
    ///
    /// This is the typed form of [`Self::allow`]. It retains the same single
    /// clock observation and per-address-before-global consumption order.
    pub fn admit(&self, addr: SocketAddr) -> Result<(), DhtInboundRateLimitDenial> {
        self.admit_at(addr, Instant::now())
    }

    fn with_policy(
        per_ip_interval: Duration,
        per_ip_burst: u32,
        capacity: usize,
        ttl: Duration,
        global_interval: Duration,
        global_burst: u32,
    ) -> Self {
        let now = Instant::now();
        Self {
            per_ip: KeyedBuckets::new(per_ip_interval, per_ip_burst, capacity, ttl),
            global: Arc::new(TokenBucket::new(global_interval, global_burst, now)),
        }
    }

    #[cfg(test)]
    fn allow_at(&self, addr: SocketAddr, now: Instant) -> bool {
        self.admit_at(addr, now).is_ok()
    }

    fn admit_at(&self, addr: SocketAddr, now: Instant) -> Result<(), DhtInboundRateLimitDenial> {
        let key = rate_limit_key(addr);
        if !self.per_ip.get_at(key, now).allow_at(now) {
            return Err(DhtInboundRateLimitDenial::PerIp);
        }
        if !self.global.allow_at(now) {
            return Err(DhtInboundRateLimitDenial::Global);
        }
        Ok(())
    }
}

impl Default for DhtInboundRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Production outbound DHT waiting policy.
///
/// Each address has a one-token-per-second bucket with an initial burst of four.
/// Calls reserve in lock-acquisition order and then sleep without holding a
/// cache or bucket lock. Dropping a pending future cancels its reservation as
/// far as later reservations permit, following `golang.org/x/time/rate` rather
/// than blindly returning a token.
#[derive(Clone)]
pub struct DhtOutboundRateLimiter {
    per_ip: KeyedBuckets,
}

/// A caller-controlled outbound DHT wait did not produce an admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DhtRateLimitWaitError {
    /// The caller's cancellation future completed before admission.
    #[error("DHT rate-limit wait cancelled")]
    Cancelled,
    /// The next admission would occur after the caller's deadline.
    #[error("DHT rate-limit reservation would exceed deadline")]
    WouldExceedDeadline,
}

impl DhtOutboundRateLimiter {
    /// Construct the deployed outbound policy with initially full buckets.
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(
            PER_IP_INTERVAL,
            OUTBOUND_PER_IP_BURST,
            KEY_CAPACITY,
            KEY_TTL,
        )
    }

    /// Wait until one outbound admission for `addr` is available.
    ///
    /// Dropping this future cancels a pending reservation. An admission whose
    /// wait already completed remains consumed.
    pub async fn wait(&self, addr: SocketAddr) {
        match self.wait_with(addr, None, pending()).await {
            Ok(()) => {}
            Err(error) => unreachable!("an unbounded wait cannot fail: {error}"),
        }
    }

    /// Wait until one outbound admission is available, provided it is no later
    /// than `deadline`.
    ///
    /// An already-expired deadline, or a reservation scheduled strictly after
    /// the deadline, is rejected without changing the token bucket. A
    /// reservation exactly at the deadline is accepted.
    pub async fn wait_until(
        &self,
        addr: SocketAddr,
        deadline: Instant,
    ) -> Result<(), DhtRateLimitWaitError> {
        self.wait_with(addr, Some(deadline), pending()).await
    }

    /// Wait with an optional deadline and caller-owned cancellation future.
    ///
    /// Cancellation is polled first, before any cache lookup or reservation,
    /// and wins when it becomes ready alongside admission. Dropping this wait,
    /// or cancellation winning while it sleeps, cancels the pending
    /// reservation as far as later reservations permit.
    pub async fn wait_with<F>(
        &self,
        addr: SocketAddr,
        deadline: Option<Instant>,
        cancellation: F,
    ) -> Result<(), DhtRateLimitWaitError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(cancellation);
        tokio::select! {
            biased;
            () = &mut cancellation => return Err(DhtRateLimitWaitError::Cancelled),
            () = ready(()) => {}
        }

        let now = Instant::now();
        if deadline.is_some_and(|deadline| deadline < now) {
            return Err(DhtRateLimitWaitError::WouldExceedDeadline);
        }

        let bucket = self.per_ip.get_at(rate_limit_key(addr), now);
        let mut reservation = bucket.reserve_at(now, deadline)?;
        tokio::select! {
            biased;
            () = &mut cancellation => Err(DhtRateLimitWaitError::Cancelled),
            () = sleep_until(reservation.time_to_act) => {
                reservation.commit();
                Ok(())
            }
        }
    }

    fn with_policy(interval: Duration, burst: u32, capacity: usize, ttl: Duration) -> Self {
        Self {
            per_ip: KeyedBuckets::new(interval, burst, capacity, ttl),
        }
    }
}

impl Default for DhtOutboundRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct KeyedBuckets {
    inner: Arc<KeyedBucketsInner>,
}

struct KeyedBucketsInner {
    state: Mutex<KeyedBucketsState>,
    interval: Duration,
    burst: u32,
    capacity: usize,
    ttl: Duration,
}

#[derive(Default)]
struct KeyedBucketsState {
    entries: HashMap<String, KeyedBucketEntry>,
    /// Oldest at the front, most recently accessed at the back.
    recency: VecDeque<String>,
}

struct KeyedBucketEntry {
    bucket: Arc<TokenBucket>,
    /// Fixed from insertion/replacement; cache hits do not extend it.
    expires_at: Instant,
}

impl KeyedBuckets {
    fn new(interval: Duration, burst: u32, capacity: usize, ttl: Duration) -> Self {
        assert!(capacity > 0, "keyed limiter capacity must be nonzero");
        assert!(burst > 0, "token bucket burst must be nonzero");
        assert!(!interval.is_zero(), "token interval must be nonzero");
        assert!(!ttl.is_zero(), "keyed limiter TTL must be nonzero");
        Self {
            inner: Arc::new(KeyedBucketsInner {
                state: Mutex::new(KeyedBucketsState::default()),
                interval,
                burst,
                capacity,
                ttl,
            }),
        }
    }

    fn get_at(&self, key: String, now: Instant) -> Arc<TokenBucket> {
        let mut state = lock(&self.inner.state);
        state.remove_expired(now);

        if let Some(bucket) = state
            .entries
            .get(&key)
            .map(|entry| Arc::clone(&entry.bucket))
        {
            state.touch(&key);
            return bucket;
        }

        let bucket = Arc::new(TokenBucket::new(self.inner.interval, self.inner.burst, now));
        state.entries.insert(
            key.clone(),
            KeyedBucketEntry {
                bucket: Arc::clone(&bucket),
                expires_at: now + self.inner.ttl,
            },
        );
        state.recency.push_back(key);

        while state.entries.len() > self.inner.capacity {
            let oldest = state
                .recency
                .pop_front()
                .expect("nonempty keyed limiter has an LRU entry");
            state.entries.remove(&oldest);
        }
        bucket
    }
}

impl KeyedBucketsState {
    fn remove_expired(&mut self, now: Instant) {
        // Strict `After`: an entry remains valid exactly at its deadline.
        self.entries.retain(|_, entry| now <= entry.expires_at);
        self.recency.retain(|key| self.entries.contains_key(key));
    }

    fn touch(&mut self, key: &str) {
        let position = self
            .recency
            .iter()
            .position(|candidate| candidate == key)
            .expect("cached keyed limiter has an LRU entry");
        let key = self
            .recency
            .remove(position)
            .expect("located LRU entry remains present");
        self.recency.push_back(key);
    }
}

struct TokenBucket {
    interval: Duration,
    burst: f64,
    state: Mutex<TokenBucketState>,
}

struct TokenBucketState {
    tokens: f64,
    last: Instant,
    last_event: Instant,
}

impl TokenBucket {
    fn new(interval: Duration, burst: u32, now: Instant) -> Self {
        Self {
            interval,
            burst: f64::from(burst),
            state: Mutex::new(TokenBucketState {
                tokens: f64::from(burst),
                last: now,
                last_event: now,
            }),
        }
    }

    fn allow_at(&self, now: Instant) -> bool {
        let mut state = lock(&self.state);
        let tokens = self.advanced_tokens(&state, now) - 1.0;
        if tokens < 0.0 {
            return false;
        }
        state.last = now;
        state.tokens = tokens;
        state.last_event = now;
        true
    }

    fn reserve_at(
        self: &Arc<Self>,
        now: Instant,
        deadline: Option<Instant>,
    ) -> Result<Reservation, DhtRateLimitWaitError> {
        let time_to_act = {
            let mut state = lock(&self.state);
            let tokens = self.advanced_tokens(&state, now) - 1.0;
            let wait = if tokens < 0.0 {
                self.duration_from_tokens(-tokens)
            } else {
                Duration::ZERO
            };
            let time_to_act = now + wait;
            if deadline.is_some_and(|deadline| time_to_act > deadline) {
                return Err(DhtRateLimitWaitError::WouldExceedDeadline);
            }
            state.last = now;
            state.tokens = tokens;
            state.last_event = time_to_act;
            time_to_act
        };
        Ok(Reservation {
            bucket: Arc::downgrade(self),
            time_to_act,
            active: true,
        })
    }

    fn advanced_tokens(&self, state: &TokenBucketState, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(state.last);
        (state.tokens + self.tokens_from_duration(elapsed)).min(self.burst)
    }

    fn tokens_from_duration(&self, duration: Duration) -> f64 {
        let limit = 1.0 / self.interval.as_secs_f64();
        duration.as_secs_f64() * limit
    }

    fn duration_from_tokens(&self, tokens: f64) -> Duration {
        let limit = 1.0 / self.interval.as_secs_f64();
        let nanoseconds = (tokens / limit) * 1_000_000_000.0;
        // Deliberately saturate at Go's maximum positive duration rather than
        // reproducing `time.Duration` overflow for astronomical queues.
        if !nanoseconds.is_finite() || nanoseconds >= i64::MAX as f64 {
            return GO_MAX_RATE_DELAY;
        }
        // Go converts the floating nanosecond count to `time.Duration`, whose
        // integer conversion truncates toward zero.
        Duration::from_nanos(nanoseconds.trunc() as u64)
    }

    fn cancel_at(&self, time_to_act: Instant, now: Instant) {
        if time_to_act < now {
            return;
        }

        let mut state = lock(&self.state);
        let later_reservations = state.last_event.saturating_duration_since(time_to_act);
        let restore_tokens = 1.0 - self.tokens_from_duration(later_reservations);
        if restore_tokens <= 0.0 {
            return;
        }

        state.tokens = (self.advanced_tokens(&state, now) + restore_tokens).min(self.burst);
        state.last = now;

        if time_to_act == state.last_event {
            if let Some(previous_event) = time_to_act.checked_sub(self.interval) {
                if previous_event >= now {
                    state.last_event = previous_event;
                }
            }
        }
    }
}

struct Reservation {
    bucket: Weak<TokenBucket>,
    time_to_act: Instant,
    active: bool,
}

impl Reservation {
    fn commit(&mut self) {
        self.active = false;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.active {
            if let Some(bucket) = self.bucket.upgrade() {
                bucket.cancel_at(self.time_to_act, Instant::now());
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn rate_limit_key(addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => {
            let ip = addr.ip();
            if addr.scope_id() == 0 {
                ip.to_string()
            } else {
                format!("{ip}%{}", addr.scope_id())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};
    use std::rc::Rc;
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    const IPV4_A: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6_881));
    const IPV4_B: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 2), 6_881));
    const IPV4_C: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 3), 6_881));

    #[test]
    fn inbound_bursts_refill_and_check_per_ip_before_global() {
        let limiter = DhtInboundRateLimiter::with_policy(
            Duration::from_secs(10),
            1,
            10,
            Duration::from_secs(20),
            Duration::from_secs(1),
            1,
        );
        let start = Instant::now();

        assert!(limiter.allow_at(IPV4_A, start));
        assert!(!limiter.allow_at(IPV4_B, start));
        assert!(!limiter.allow_at(IPV4_B, start + Duration::from_secs(1)));
        assert!(limiter.allow_at(IPV4_C, start + Duration::from_secs(1)));
    }

    #[test]
    fn per_ip_rejection_does_not_consume_a_global_token() {
        let limiter = DhtInboundRateLimiter::with_policy(
            Duration::from_secs(10),
            1,
            10,
            Duration::from_secs(20),
            Duration::from_secs(10),
            2,
        );
        let now = Instant::now();

        assert!(limiter.allow_at(IPV4_A, now));
        assert!(!limiter.allow_at(IPV4_A, now));
        assert!(limiter.allow_at(IPV4_B, now));
        assert!(!limiter.allow_at(IPV4_C, now));
    }

    #[test]
    fn fixed_ttl_uses_a_strict_boundary_and_replacement_is_full() {
        let ttl = Duration::from_secs(20);
        let keyed = KeyedBuckets::new(Duration::from_secs(1), 2, 2, ttl);
        let start = Instant::now();
        let first = keyed.get_at("a".to_owned(), start);
        assert!(first.allow_at(start));
        assert!(first.allow_at(start));
        assert!(!first.allow_at(start));

        let at_deadline = keyed.get_at("a".to_owned(), start + ttl);
        assert!(Arc::ptr_eq(&first, &at_deadline));

        let replacement = keyed.get_at("a".to_owned(), start + ttl + Duration::from_nanos(1));
        assert!(!Arc::ptr_eq(&first, &replacement));
        assert!(replacement.allow_at(start + ttl + Duration::from_nanos(1)));
        assert!(replacement.allow_at(start + ttl + Duration::from_nanos(1)));
    }

    #[test]
    fn access_recency_drives_capacity_eviction() {
        let keyed = KeyedBuckets::new(Duration::from_secs(1), 1, 2, Duration::from_secs(20));
        let now = Instant::now();
        let a = keyed.get_at("a".to_owned(), now);
        let b = keyed.get_at("b".to_owned(), now);
        assert!(Arc::ptr_eq(&a, &keyed.get_at("a".to_owned(), now)));
        let _c = keyed.get_at("c".to_owned(), now);
        let replacement_b = keyed.get_at("b".to_owned(), now);
        assert!(!Arc::ptr_eq(&b, &replacement_b));
    }

    #[test]
    fn socket_keys_ignore_ports_and_flowinfo_but_preserve_address_identity_and_scope() {
        let limiter = DhtInboundRateLimiter::with_policy(
            Duration::from_secs(10),
            1,
            20,
            Duration::from_secs(20),
            Duration::from_secs(10),
            20,
        );
        let now = Instant::now();
        let ipv4_other_port =
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 65_535));
        let mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(),
            6_881,
            0,
            0,
        ));
        let native_scope_7 =
            SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6_881, 42, 7));
        let native_scope_7_other_flow_and_port =
            SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 65_535, 99, 7));
        let native_scope_8 =
            SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6_881, 42, 8));

        assert_eq!(rate_limit_key(IPV4_A), "192.0.2.1");
        assert_eq!(rate_limit_key(ipv4_other_port), "192.0.2.1");
        assert_eq!(rate_limit_key(mapped), "::ffff:192.0.2.1");
        assert_eq!(rate_limit_key(native_scope_7), "fe80::1%7");
        assert_eq!(
            rate_limit_key(native_scope_7_other_flow_and_port),
            "fe80::1%7"
        );
        assert_eq!(rate_limit_key(native_scope_8), "fe80::1%8");

        assert!(limiter.allow_at(IPV4_A, now));
        assert!(!limiter.allow_at(ipv4_other_port, now));
        assert!(limiter.allow_at(mapped, now));
        assert!(limiter.allow_at(native_scope_7, now));
        assert!(!limiter.allow_at(native_scope_7_other_flow_and_port, now));
        assert!(limiter.allow_at(native_scope_8, now));
        assert!(!limiter.allow_at(mapped, now));
        assert!(!limiter.allow_at(native_scope_8, now));
    }

    #[test]
    fn duration_conversion_truncates_fractional_nanoseconds_like_go() {
        let now = Instant::now();
        let bucket = TokenBucket::new(Duration::from_nanos(3), 1, now);
        assert_eq!(bucket.duration_from_tokens(0.5), Duration::from_nanos(1));
        assert_eq!(bucket.duration_from_tokens(f64::MAX), GO_MAX_RATE_DELAY);
    }

    #[test]
    fn poisoned_bucket_and_cache_mutexes_recover_without_panicking_again() {
        let now = Instant::now();
        let bucket = TokenBucket::new(Duration::from_secs(1), 1, now);
        let bucket_panic = std::panic::catch_unwind(|| {
            let _guard = bucket.state.lock().unwrap();
            panic!("poison token bucket");
        });
        assert!(bucket_panic.is_err());
        assert!(bucket.allow_at(now));

        let keyed = KeyedBuckets::new(Duration::from_secs(1), 1, 2, Duration::from_secs(20));
        let keyed_panic = std::panic::catch_unwind(|| {
            let _guard = keyed.inner.state.lock().unwrap();
            panic!("poison keyed cache");
        });
        assert!(keyed_panic.is_err());
        assert!(keyed.get_at("after-poison".to_owned(), now).allow_at(now));
    }

    #[tokio::test(start_paused = true)]
    async fn pre_cancellation_wins_before_deadline_and_does_not_create_a_bucket() {
        let limiter = DhtOutboundRateLimiter::new();
        let now = Instant::now();
        let expired = now.checked_sub(Duration::from_nanos(1)).unwrap();

        assert_eq!(
            limiter.wait_with(IPV4_A, Some(expired), ready(())).await,
            Err(DhtRateLimitWaitError::Cancelled)
        );
        assert!(lock(&limiter.per_ip.inner.state).entries.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_deadline_does_not_create_a_bucket() {
        let limiter = DhtOutboundRateLimiter::new();
        let expired = Instant::now().checked_sub(Duration::from_nanos(1)).unwrap();

        assert_eq!(
            limiter.wait_until(IPV4_A, expired).await,
            Err(DhtRateLimitWaitError::WouldExceedDeadline)
        );
        assert!(lock(&limiter.per_ip.inner.state).entries.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn exact_deadline_is_accepted_and_wait_with_needs_no_send_bound() {
        let limiter = DhtOutboundRateLimiter::with_policy(
            Duration::from_secs(1),
            2,
            10,
            Duration::from_secs(20),
        );
        let now = Instant::now();
        limiter.wait_until(IPV4_A, now).await.unwrap();

        let not_send = Rc::new(());
        let cancellation = async move {
            let _keep_alive = not_send;
            pending::<()>().await;
        };
        limiter.wait_with(IPV4_A, None, cancellation).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn insufficient_deadline_does_not_mutate_bucket_and_exact_schedule_remains_available() {
        let limiter = DhtOutboundRateLimiter::with_policy(
            Duration::from_secs(1),
            1,
            10,
            Duration::from_secs(20),
        );
        let now = Instant::now();
        limiter.wait(IPV4_A).await;

        let bucket = limiter
            .per_ip
            .get_at(rate_limit_key(IPV4_A), Instant::now());
        let before = {
            let state = lock(&bucket.state);
            (state.tokens, state.last, state.last_event)
        };

        assert_eq!(
            limiter
                .wait_until(IPV4_A, now + Duration::from_millis(999))
                .await,
            Err(DhtRateLimitWaitError::WouldExceedDeadline)
        );
        {
            let state = lock(&bucket.state);
            assert_eq!((state.tokens, state.last, state.last_event), before);
        }

        let exact = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter
                    .wait_until(IPV4_A, now + Duration::from_secs(1))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!exact.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(exact.await.unwrap(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn typed_cancellation_while_sleeping_rolls_back_the_reservation() {
        let limiter = DhtOutboundRateLimiter::with_policy(
            Duration::from_secs(1),
            1,
            10,
            Duration::from_secs(20),
        );
        limiter.wait(IPV4_A).await;

        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let waiting = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter
                    .wait_with(IPV4_A, None, async move {
                        let _ = cancel_rx.await;
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        cancel_tx.send(()).unwrap();
        assert_eq!(
            waiting.await.unwrap(),
            Err(DhtRateLimitWaitError::Cancelled)
        );

        let replacement = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.wait(IPV4_A).await }
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        replacement.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_latest_wait_removes_only_its_scheduled_delay() {
        let limiter = DhtOutboundRateLimiter::with_policy(
            Duration::from_secs(1),
            1,
            10,
            Duration::from_secs(20),
        );
        limiter.wait(IPV4_A).await;

        let pending = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.wait(IPV4_A).await }
        });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());

        let replacement = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.wait(IPV4_A).await }
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        replacement.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn canceling_an_older_wait_does_not_over_restore_a_later_one() {
        let limiter = DhtOutboundRateLimiter::with_policy(
            Duration::from_secs(1),
            1,
            10,
            Duration::from_secs(20),
        );
        limiter.wait(IPV4_A).await;

        let older = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.wait(IPV4_A).await }
        });
        tokio::task::yield_now().await;
        let later = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.wait(IPV4_A).await }
        });
        tokio::task::yield_now().await;
        older.abort();
        assert!(older.await.unwrap_err().is_cancelled());

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!later.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        later.await.unwrap();
    }

    #[test]
    fn cloned_inbound_limiter_serializes_concurrent_burst_consumption() {
        let limiter = DhtInboundRateLimiter::new();
        let barrier = Arc::new(Barrier::new(33));
        let mut threads = Vec::new();
        for _ in 0..32 {
            let limiter = limiter.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                limiter.allow(IPV4_A)
            }));
        }
        barrier.wait();
        let admitted = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|admitted| *admitted)
            .count();
        assert_eq!(admitted, 10);
    }
}
