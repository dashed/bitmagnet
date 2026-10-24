//! Go's `requesterLimiter` — `golang.org/x/time/rate` in front of every call.
//!
//! Go builds `rate.NewLimiter(rate.Every(config.RateLimit), config.RateLimitBurst)`
//! and `Wait`s on it (`requester_limiter.go`). That is a token bucket: `burst`
//! tokens available at once, refilled one per `interval`.
//!
//! This is the equivalent as a **virtual-scheduling (GCRA)** bucket, which is
//! the same admission decision expressed as arithmetic on one timestamp rather
//! than a floating token count — it needs no background task and no clock
//! thread, and [`RateLimiter::reserve`] is a pure function of `now`, so the
//! schedule is testable without sleeping.
//!
//! # Where it sits, and what that costs
//!
//! 🚨 The limiter is **inside** the semaphore and **outside** the retry loop, as
//! in Go. So a retried attempt does *not* take a second token: a burst of
//! transport failures can exceed the configured rate. That is Go's behaviour
//! (resty's retry lives below this layer), not an oversight.
//!
//! # Fairness
//!
//! `rate.Limiter` hands out reservations in call order and this does the same,
//! but neither guarantees that the waiters *wake* in that order — here the
//! reservation is computed under the lock and the sleep happens after it is
//! released. Under bitmagnet's two-permit semaphore at most two callers are ever
//! queued, so the distinction is theoretical.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) struct RateLimiter {
    /// Go `rate.Every(interval)`: the emission interval. Zero means
    /// `rate.Inf` — Go's `rate.Every` returns `Inf` for a non-positive
    /// interval, and an `Inf` limiter never blocks.
    interval: Duration,
    /// Bucket capacity.
    burst: u32,
    /// The theoretical arrival time of the next emission: the instant the
    /// bucket would be empty given everything already granted. `None` until the
    /// first call, because there is no const `Instant` to start from — and
    /// starting the clock at construction would leak wall time between building
    /// the client and using it into the first burst.
    next_emission: Mutex<Option<Instant>>,
}

impl RateLimiter {
    /// `burst` is clamped to at least 1: Go's `Wait` fails outright on a
    /// zero-burst finite limiter ("exceeds limiter's burst"), which would turn a
    /// misconfiguration into a permanent, silent outage rather than a slow
    /// client.
    pub(crate) fn new(interval: Duration, burst: u32) -> Self {
        Self {
            interval,
            burst: burst.max(1),
            next_emission: Mutex::new(None),
        }
    }

    /// Reserves one token and returns how long the caller must wait for it.
    ///
    /// Pure in `now` apart from advancing the reservation clock, so a test can
    /// walk the schedule forward without real time passing.
    pub(crate) fn reserve(&self, now: Instant) -> Duration {
        if self.interval.is_zero() {
            return Duration::ZERO;
        }

        // Everything more than `tolerance` in the past is forgotten: that is
        // what makes the bucket hold `burst` and not more.
        let tolerance = self.interval * (self.burst - 1);

        let mut state = self.next_emission.lock().expect("rate limiter poisoned");
        let arrival = state.unwrap_or(now).max(now);
        let wait = arrival
            .checked_sub(tolerance)
            .map_or(Duration::ZERO, |allowed_at| {
                allowed_at.saturating_duration_since(now)
            });
        *state = Some(arrival + self.interval);

        wait
    }

    /// Go `limiter.Wait(ctx)`. Cancellation is by dropping the future rather
    /// than by a context error; the reservation is not returned to the bucket,
    /// which is also true of Go's `Wait` once it has begun sleeping.
    pub(crate) async fn wait(&self) {
        let wait = self.reserve(Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bitmagnet's default: 20 requests/second with a burst of 5
    /// (`config.go`'s `time.Second / 20`). The burst must be immediate — a
    /// limiter that spaced the first five calls would halve throughput on every
    /// classification that fans out.
    #[test]
    fn the_burst_is_free_and_then_the_rate_applies() {
        let limiter = RateLimiter::new(Duration::from_millis(50), 5);
        let now = Instant::now();

        for i in 0..5 {
            assert_eq!(limiter.reserve(now), Duration::ZERO, "burst token {i}");
        }

        assert_eq!(limiter.reserve(now), Duration::from_millis(50));
        assert_eq!(limiter.reserve(now), Duration::from_millis(100));
    }

    /// Tokens accrue while the client is idle, up to the burst — and no
    /// further. An idle hour must not buy an hour's worth of requests.
    #[test]
    fn idling_refills_to_the_burst_but_not_past_it() {
        let limiter = RateLimiter::new(Duration::from_millis(50), 5);
        let start = Instant::now();

        for _ in 0..5 {
            let _ = limiter.reserve(start);
        }

        // One interval later, exactly one token is back.
        let later = start + Duration::from_millis(50);
        assert_eq!(limiter.reserve(later), Duration::ZERO);
        assert_eq!(limiter.reserve(later), Duration::from_millis(50));

        // An hour later the bucket is full again, but only to `burst`.
        let much_later = start + Duration::from_secs(3600);
        for i in 0..5 {
            assert_eq!(limiter.reserve(much_later), Duration::ZERO, "refilled {i}");
        }
        assert_eq!(limiter.reserve(much_later), Duration::from_millis(50));
    }

    /// Go's `rate.Every` returns `rate.Inf` for a non-positive interval, and an
    /// `Inf` limiter never waits. Tests rely on this to isolate other layers.
    #[test]
    fn a_zero_interval_is_gos_rate_inf() {
        let limiter = RateLimiter::new(Duration::ZERO, 1);
        let now = Instant::now();

        for _ in 0..1000 {
            assert_eq!(limiter.reserve(now), Duration::ZERO);
        }
    }

    /// The default-API-key path in `newRequester` drops to one request per
    /// second with a burst of 8; the shape must hold at that setting too.
    #[test]
    fn the_default_key_setting_spaces_calls_one_second_apart() {
        let limiter = RateLimiter::new(Duration::from_secs(1), 8);
        let now = Instant::now();

        for _ in 0..8 {
            assert_eq!(limiter.reserve(now), Duration::ZERO);
        }
        assert_eq!(limiter.reserve(now), Duration::from_secs(1));
    }
}
