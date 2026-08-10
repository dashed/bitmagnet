//! The bottom of the chain: one HTTP round trip, and Go's retry policy around
//! it.
//!
//! Go's innermost `requester` is a resty client configured in
//! `requester_lazy.go:63-72`:
//!
//! ```text
//! resty.New().
//!     SetBaseURL(config.BaseURL).
//!     SetQueryParam("api_key", config.APIKey).
//!     SetRetryCount(3).
//!     SetRetryWaitTime(2 * time.Second).
//!     SetRetryMaxWaitTime(20 * time.Second).
//!     SetTimeout(10 * time.Second)
//! ```
//!
//! # 🚨 What resty actually retries
//!
//! bitmagnet registers **no** `RetryCondition`, and resty's default
//! (`retry.go:127`) is `needsRetry = err != nil && err == unwrapNoRetryErr(err)`
//! — a *transport* error. A 500, a 429, a 401: resty returns those with a nil
//! error and **does not retry them**. Retrying a 429 would look like the obvious
//! thing to do and would be a divergence, so the retry here is likewise keyed on
//! the transport failure alone. That is why [`Transport::execute`] returns
//! `Result<HttpResponse, String>` with the status *inside* the `Ok`: a status
//! code is not an error at this layer, and the type says so.
//!
//! `SetRetryCount(3)` is three *retries* — resty loops `attempt <= maxRetries`,
//! so up to **four** attempts. The 10s timeout is per attempt on both sides
//! (Go's `http.Client.Timeout`, reqwest's `ClientBuilder::timeout`).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::Url;

use crate::error::TmdbError;

/// A response as the layers above it need it: a status to classify and a body
/// to decode. Deliberately not a `reqwest::Response`, so the chain above is
/// testable without a socket and cannot accidentally depend on streaming.
#[derive(Debug, Clone, Default)]
pub struct HttpResponse {
    pub status: u16,
    /// The `Content-Type` header, or `None` when the server sent none.
    ///
    /// Carried because resty decodes a body *only* for a JSON (or XML) content
    /// type and silently leaves the result at its zero value otherwise — see
    /// [`crate::TmdbClient`]'s decode rule. Dropping the header would turn that
    /// into a decode error and diverge on exactly the responses (proxy error
    /// pages, captive portals) where the difference shows up.
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

/// One HTTP round trip.
///
/// `Err` is reserved for a **transport** failure — no usable HTTP response at
/// all (connection refused, TLS, timeout). An HTTP error status is an `Ok`
/// carrying that status; see the module docs for why the distinction is
/// load-bearing.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn execute(&self, url: &Url) -> Result<HttpResponse, String>;
}

/// So a shared transport can be handed to several clients — and so a test can
/// keep a handle on the one it scripted.
#[async_trait]
impl<T: Transport + ?Sized> Transport for Arc<T> {
    async fn execute(&self, url: &Url) -> Result<HttpResponse, String> {
        (**self).execute(url).await
    }
}

/// The live transport.
#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    /// `timeout` is Go's `SetTimeout`, applied per attempt.
    ///
    /// # Errors
    ///
    /// If the TLS backend cannot be initialised.
    pub fn new(timeout: Duration) -> Result<Self, TmdbError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|err| TmdbError::Transport(err.without_url().to_string()))?;

        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn execute(&self, url: &Url) -> Result<HttpResponse, String> {
        // 🚨 `without_url` is not cosmetic: reqwest's `Error` renders the URL it
        // failed on, and this URL carries `api_key`. Without this the credential
        // ends up in every timeout message, in the logs, and — if a recording is
        // running — in a tape that is documented to hold no secrets.
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|err| err.without_url().to_string())?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response
            .bytes()
            .await
            .map_err(|err| err.without_url().to_string())?;

        Ok(HttpResponse {
            status,
            content_type,
            body: body.to_vec(),
        })
    }
}

/// Go's resty retry settings.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// `SetRetryCount` — retries *after* the first attempt.
    pub count: u32,
    /// `SetRetryWaitTime`.
    pub wait: Duration,
    /// `SetRetryMaxWaitTime`.
    pub max_wait: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            count: 3,
            wait: Duration::from_secs(2),
            max_wait: Duration::from_secs(20),
        }
    }
}

impl RetryPolicy {
    /// resty's `jitterBackoff` (`retry.go:207`), for the wait *after* `attempt`.
    fn backoff(&self, attempt: u32) -> Duration {
        jitter_backoff(self.wait, self.max_wait, attempt, next_random())
    }
}

/// resty `jitterBackoff` + `randDuration`, with the random draw injected so the
/// schedule is assertable.
///
/// `ri = min(max, wait * 2^attempt) / 2`, then a uniform draw in `[ri, 2*ri)`,
/// then clamped up to `wait`. With bitmagnet's 2s/20s that means the first wait
/// is *always* 2s (the draw lands below the clamp), and only later attempts
/// spread out — a detail that is easy to get wrong by implementing the
/// "obvious" exponential backoff instead.
fn jitter_backoff(wait: Duration, max_wait: Duration, attempt: u32, random: u64) -> Duration {
    let capped = wait
        .checked_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .unwrap_or(max_wait)
        .min(max_wait);

    // resty floors the half-interval at a nanosecond so the modulo below is
    // always defined.
    let half = (capped.as_nanos() / 2).max(1);
    let nanos = half + u128::from(random) % half;
    let jittered = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));

    jittered.max(wait)
}

/// resty keeps one package-level `math/rand` source behind a mutex (`retry.go:227`).
/// This is the same: a non-cryptographic sequence, seeded once from the clock.
fn next_random() -> u64 {
    static RNG: OnceLock<Mutex<u64>> = OnceLock::new();

    let mut state = RNG
        .get_or_init(|| {
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
            Mutex::new(seed ^ 0x9e37_79b9_7f4a_7c15)
        })
        .lock()
        .expect("backoff rng poisoned");

    // SplitMix64.
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Go's resty retry loop, around one URL.
///
/// Cancellation differs in kind, not in effect: Go checks `ctx.Err()` before
/// each retry, while here dropping the future stops the loop at its next await.
pub(crate) async fn execute_with_retry<T: Transport + ?Sized>(
    transport: &T,
    url: &Url,
    policy: &RetryPolicy,
) -> Result<HttpResponse, String> {
    let mut attempt = 0;

    loop {
        match transport.execute(url).await {
            // Includes every HTTP error status: not retried, see module docs.
            Ok(response) => return Ok(response),
            Err(err) if attempt >= policy.count => return Err(err),
            Err(_) => {
                tokio::time::sleep(policy.backoff(attempt)).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With bitmagnet's 2s wait the clamp swallows the whole first draw, so the
    /// first retry is always exactly 2s however the dice fall. A naive
    /// `wait * 2^attempt` would give the same answer here and diverge from
    /// attempt 1 onwards, which is why the whole schedule is pinned.
    #[test]
    fn the_first_backoff_is_always_the_configured_wait() {
        let wait = Duration::from_secs(2);
        let max = Duration::from_secs(20);

        for random in [0, 1, u64::MAX / 2, u64::MAX] {
            assert_eq!(jitter_backoff(wait, max, 0, random), wait);
        }
    }

    /// Later attempts draw uniformly from `[ri, 2*ri)` with `ri` doubling —
    /// [2s,4s) then [4s,8s) — so a retry storm spreads out instead of
    /// synchronising.
    #[test]
    fn later_backoffs_spread_within_gos_bounds() {
        let wait = Duration::from_secs(2);
        let max = Duration::from_secs(20);

        for random in [0, 7, u64::MAX / 3, u64::MAX] {
            let second = jitter_backoff(wait, max, 1, random);
            assert!(
                second >= Duration::from_secs(2) && second < Duration::from_secs(4),
                "attempt 1 drew {second:?}"
            );

            let third = jitter_backoff(wait, max, 2, random);
            assert!(
                third >= Duration::from_secs(4) && third < Duration::from_secs(8),
                "attempt 2 drew {third:?}"
            );
        }
    }

    /// The cap binds beyond the attempts bitmagnet configures, but a change to
    /// `SetRetryCount` must not let the wait run away.
    #[test]
    fn the_max_wait_caps_the_growth() {
        let backoff = jitter_backoff(
            Duration::from_secs(2),
            Duration::from_secs(20),
            30,
            u64::MAX,
        );

        assert!(
            backoff >= Duration::from_secs(10) && backoff < Duration::from_secs(20),
            "capped backoff drew {backoff:?}"
        );
    }
}
