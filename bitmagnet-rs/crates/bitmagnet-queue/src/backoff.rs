//! Retry backoff, mirroring `queue.CalculateBackoff`
//! (`internal/queue/helpers.go:17-25`, contract §1.4).
//!
//! `CalculateBackoff(retryCount)` returns `now + (round(retryCount^4) + 15 +
//! RandInt(30)*retryCount + 1)` seconds. Splitting deterministic from jitter:
//!
//! - **deterministic** = `retryCount^4 + 16` seconds (the `round(pow(n,4))` is
//!   exact for integer `n`; `15 + 1 = 16`).
//! - **jitter** = `RandInt(30) * retryCount`, with `RandInt(30) ∈ [0, 29]`
//!   (`rand.Intn(30)`, seeded by wall clock → nondeterministic). So jitter is
//!   bounded to `[0, 29 * retryCount]`.
//!
//! The golden (`testdata/parity/queue/backoff.jsonl`) pins only the
//! deterministic base and the jitter bounds, never an exact value.

use std::time::Duration;

/// Number of distinct jitter values Go draws (`rand.Intn(30)` → `[0, 29]`).
pub const JITTER_MODULUS: u32 = 30;

/// The deterministic + bounded-jitter envelope for a given retry count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffEnvelope {
    /// `retryCount^4 + 16` — the value with zero jitter.
    pub deterministic_seconds: u64,
    /// Lower jitter bound (always 0).
    pub jitter_min_seconds: u64,
    /// Upper jitter bound, `29 * retryCount`.
    pub jitter_max_seconds: u64,
}

impl BackoffEnvelope {
    /// Smallest total backoff (deterministic base, zero jitter).
    #[must_use]
    pub const fn min_seconds(self) -> u64 {
        self.deterministic_seconds + self.jitter_min_seconds
    }

    /// Largest total backoff (deterministic base + max jitter).
    #[must_use]
    pub const fn max_seconds(self) -> u64 {
        self.deterministic_seconds + self.jitter_max_seconds
    }
}

/// The deterministic base of the backoff: `retryCount^4 + 16` seconds.
#[must_use]
pub fn deterministic_seconds(retry_count: u32) -> u64 {
    u64::from(retry_count).pow(4) + 16
}

/// The full envelope (deterministic base + `[0, 29*retryCount]` jitter bound).
#[must_use]
pub fn envelope(retry_count: u32) -> BackoffEnvelope {
    BackoffEnvelope {
        deterministic_seconds: deterministic_seconds(retry_count),
        jitter_min_seconds: 0,
        jitter_max_seconds: u64::from(JITTER_MODULUS - 1) * u64::from(retry_count),
    }
}

/// The exact backoff seconds for a given jitter draw, mirroring the Go formula
/// with `RandInt(30) = jitter`. `jitter` must be in `[0, 29]`.
///
/// # Panics
/// Panics if `jitter >= 30` (an invalid `rand.Intn(30)` result).
#[must_use]
pub fn backoff_seconds_with_jitter(retry_count: u32, jitter: u32) -> u64 {
    assert!(jitter < JITTER_MODULUS, "jitter must be < {JITTER_MODULUS}");
    deterministic_seconds(retry_count) + u64::from(jitter) * u64::from(retry_count)
}

/// `RandInt(max)` — `rand.Intn(max)`, a wall-clock-seeded draw in `[0, max)`
/// (`helpers.go:10-13`). This reproduces the *shape* (uniform, time-seeded),
/// not Go's exact PRNG sequence, which is irrelevant: the value is
/// nondeterministic by design and only its bound is contractual.
fn rand_int(max: u32) -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};

    // splitmix64 over the wall-clock nanos — a dependency-free uniform source.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z % u64::from(max)) as u32
}

/// The runtime backoff delay: deterministic base + a live nondeterministic
/// jitter draw. Callers add this to `now().UTC()` to get `run_after`.
#[must_use]
pub fn calculate_backoff(retry_count: u32) -> Duration {
    Duration::from_secs(backoff_seconds_with_jitter(
        retry_count,
        rand_int(JITTER_MODULUS),
    ))
}
