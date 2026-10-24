use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest, Sha1};

/// BEP-33 fixes the scrape bloom filter at 2,048 bits with two SHA-1-derived
/// indexes.
pub const SCRAPE_BLOOM_BYTES: usize = 256;
const SCRAPE_BLOOM_BITS: usize = SCRAPE_BLOOM_BYTES * 8;
const SCRAPE_BLOOM_HASHES: f64 = 2.0;

/// A BEP-33 scrape bloom filter.
///
/// Go's `AddIP` hashes the bytes of its `net.IP` slice verbatim, so this API
/// deliberately accepts raw IP bytes. Callers must choose the canonical four-
/// byte IPv4 or sixteen-byte IPv6 representation before insertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScrapeBloomFilter([u8; SCRAPE_BLOOM_BYTES]);

impl ScrapeBloomFilter {
    pub const EMPTY: Self = Self([0; SCRAPE_BLOOM_BYTES]);

    pub fn from_slice(value: &[u8]) -> Result<Self, ScrapeBloomError> {
        let bytes: [u8; SCRAPE_BLOOM_BYTES] = value
            .try_into()
            .map_err(|_| ScrapeBloomError::InvalidLength(value.len()))?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SCRAPE_BLOOM_BYTES] {
        &self.0
    }

    /// Insert the exact IP byte representation used by Go.
    pub fn add_ip_bytes(&mut self, ip: &[u8]) {
        let digest = Sha1::digest(ip);
        self.set_index(usize::from(digest[0]) | (usize::from(digest[1]) << 8));
        self.set_index(usize::from(digest[2]) | (usize::from(digest[3]) << 8));
    }

    /// Match Go's `ScrapeBloomFilter.EstimateCount`, including its empty-filter
    /// half-item estimate caused by clamping the zero count to `m - 1`.
    #[must_use]
    pub fn estimate_count(&self) -> f64 {
        let zeroes = self.count_zeroes().min(SCRAPE_BLOOM_BITS - 1) as f64;
        let bits = SCRAPE_BLOOM_BITS as f64;
        (zeroes / bits).ln() / (SCRAPE_BLOOM_HASHES * (1.0 - 1.0 / bits).ln())
    }

    /// Match the rounded `bits-and-blooms` size used by Go persistence for
    /// finite BEP-33 filters.
    #[must_use]
    pub fn approximated_size(&self) -> u32 {
        let ones = (SCRAPE_BLOOM_BITS - self.count_zeroes()) as f64;
        let bits = SCRAPE_BLOOM_BITS as f64;
        (-bits / SCRAPE_BLOOM_HASHES * (1.0 - ones / bits).ln()).round() as u32
    }

    fn set_index(&mut self, index: usize) {
        let index = index % SCRAPE_BLOOM_BITS;
        self.0[index / 8] |= 1 << (index % 8);
    }

    fn count_zeroes(&self) -> usize {
        self.0.iter().map(|byte| byte.count_zeros() as usize).sum()
    }
}

impl Default for ScrapeBloomFilter {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl From<[u8; SCRAPE_BLOOM_BYTES]> for ScrapeBloomFilter {
    fn from(value: [u8; SCRAPE_BLOOM_BYTES]) -> Self {
        Self(value)
    }
}

impl Serialize for ScrapeBloomFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ScrapeBloomFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(serde::de::Error::custom(
                "scrape bloom hex must be lowercase",
            ));
        }
        let decoded = hex::decode(value).map_err(serde::de::Error::custom)?;
        Self::from_slice(&decoded).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScrapeBloomError {
    #[error("BEP-33 scrape bloom has length {0}; expected 256")]
    InvalidLength(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_and_duplicate_insertion_are_deterministic() {
        for length in [0, 1, 255, 257] {
            assert!(ScrapeBloomFilter::from_slice(&vec![0; length]).is_err());
        }
        assert!(ScrapeBloomFilter::from_slice(&[0; SCRAPE_BLOOM_BYTES]).is_ok());

        let mut once = ScrapeBloomFilter::default();
        once.add_ip_bytes(&[127, 0, 0, 1]);
        let mut twice = once;
        twice.add_ip_bytes(&[127, 0, 0, 1]);
        assert_eq!(once.as_bytes(), twice.as_bytes());
    }

    #[test]
    fn empty_estimates_match_go_boundaries() {
        let filter = ScrapeBloomFilter::default();
        assert_eq!(filter.approximated_size(), 0);
        assert!(filter.estimate_count() > 0.0);
        assert!(filter.estimate_count() < 1.0);

        let full = ScrapeBloomFilter::from([u8::MAX; SCRAPE_BLOOM_BYTES]);
        assert!(full.estimate_count().is_infinite());
    }
}
