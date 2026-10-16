//! Fixed, process-local stable Bloom deduplication for sampled DHT info hashes.
//!
//! This is a probabilistic observation filter, not a set. False positives and
//! false negatives are possible, every observation runs the stable-eviction/add
//! transition, and no state is persisted across process restarts. The fixed
//! cell geometry and hash/index derivation match the Go crawler's pinned
//! BoomFilters dependency; exact random decrement offsets and retention age
//! deliberately do not.

#[cfg(test)]
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use bitmagnet_bloom::{
    DecrementStartSource as BloomDecrementStartSource, RandomDecrementStartSource,
    StableBloomFilter, StableBloomGeometry,
};

use crate::Id20;

const CELL_COUNT: usize = 10_000_000;
const BITS_PER_CELL: usize = 2;
const HASH_FUNCTIONS: usize = 5;
const DECREMENT_CELLS: usize = 49;
#[cfg(test)]
const MAX_CELL_VALUE: u8 = 3;
#[cfg(test)]
const CELL_PAYLOAD_BYTES: usize = CELL_COUNT * BITS_PER_CELL / 8;

/// Cloneable, fixed-geometry stable Bloom deduper for v1 info hashes.
///
/// Every clone shares one process-local filter and one random decrement stream.
/// [`Self::test_and_add`] serializes its complete membership test and mutation
/// under a blocking mutex, starts no task, and performs no I/O. A fresh value
/// allocates exactly 2,500,000 bytes for packed cells, excluding ordinary Rust
/// allocation and synchronization overhead.
#[derive(Clone)]
pub struct DhtInfoHashDeduper {
    inner: Arc<Mutex<DhtInfoHashDeduperState>>,
}

struct DhtInfoHashDeduperState {
    filter: StableBloomFilter,
    decrement_starts: DecrementStartSource,
}

enum DecrementStartSource {
    Random(RandomDecrementStartSource),
    #[cfg(test)]
    Scripted(VecDeque<usize>),
}

impl Default for DhtInfoHashDeduper {
    fn default() -> Self {
        Self::new()
    }
}

impl DhtInfoHashDeduper {
    /// Construct one fresh filter with the fixed Go production geometry.
    #[must_use]
    pub fn new() -> Self {
        Self::with_source(DecrementStartSource::Random(
            RandomDecrementStartSource::new(),
        ))
    }

    /// Test whether all membership cells were nonzero, then add the info hash.
    ///
    /// `true` means the hash appeared present immediately before this call;
    /// `false` means at least one membership cell was zero. Neither result is
    /// exact set membership. The stable-eviction mutation runs for both results.
    #[must_use]
    pub fn test_and_add(&self, info_hash: Id20) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("DHT info-hash deduper state lock poisoned");
        let state = &mut *state;
        state
            .filter
            .test_and_add(info_hash.as_bytes(), &mut state.decrement_starts)
    }

    fn with_source(decrement_starts: DecrementStartSource) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DhtInfoHashDeduperState {
                filter: StableBloomFilter::new(deduper_geometry()),
                decrement_starts,
            })),
        }
    }

    #[cfg(test)]
    fn with_decrement_starts(starts: impl IntoIterator<Item = usize>) -> Self {
        let starts = starts.into_iter().collect::<VecDeque<_>>();
        assert!(
            starts.iter().all(|&start| start < CELL_COUNT),
            "scripted decrement starts must be valid cell indexes"
        );
        Self::with_source(DecrementStartSource::Scripted(starts))
    }
}

impl BloomDecrementStartSource for DecrementStartSource {
    fn next_start(&mut self, cell_count: NonZeroUsize) -> usize {
        match self {
            Self::Random(source) => source.next_start(cell_count),
            #[cfg(test)]
            Self::Scripted(starts) => starts
                .pop_front()
                .expect("scripted decrement start exhausted"),
        }
    }
}

fn deduper_geometry() -> StableBloomGeometry {
    StableBloomGeometry::new(
        CELL_COUNT,
        BITS_PER_CELL as u8,
        HASH_FUNCTIONS,
        DECREMENT_CELLS,
    )
    .expect("fixed DHT info-hash deduper geometry is valid")
}

#[cfg(test)]
fn fnv1_64(bytes: &[u8]) -> u64 {
    bitmagnet_bloom::fnv1_64(bytes)
}

#[cfg(test)]
fn hash_indices(info_hash: Id20) -> [usize; HASH_FUNCTIONS] {
    bitmagnet_bloom::hash_indices(info_hash.as_bytes(), deduper_geometry())
        .try_into()
        .expect("fixed hash-function count")
}

#[cfg(test)]
#[path = "dht_info_hash_deduper_parity.rs"]
mod parity;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn id(value: &str) -> Id20 {
        Id20::from_hex(value).unwrap()
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn fixed_geometry_is_exact_bounded_and_taskless() {
        assert_eq!(CELL_COUNT, 10_000_000);
        assert_eq!(BITS_PER_CELL, 2);
        assert_eq!(HASH_FUNCTIONS, 5);
        assert_eq!(DECREMENT_CELLS, 49);
        assert_eq!(MAX_CELL_VALUE, 3);
        assert_eq!(CELL_PAYLOAD_BYTES, 2_500_000);
        assert_send_sync::<DhtInfoHashDeduper>();

        let deduper = DhtInfoHashDeduper::default();
        let state = deduper.inner.lock().unwrap();
        assert_eq!(state.filter.geometry(), deduper_geometry());
        assert_eq!(state.filter.packed_cells().len(), CELL_PAYLOAD_BYTES);
    }

    #[test]
    fn fnv1_and_double_hash_vectors_match_go() {
        let vectors = [
            (
                "0000000000000000000000000000000000000000",
                0xee85_fafd_354b_0935,
                [4_110_005, 5_867_954, 7_625_903, 9_383_852, 1_141_801],
            ),
            (
                "00000000000000000000000000000000000000a1",
                0xee85_fafd_354b_0994,
                [4_110_100, 5_868_049, 7_625_998, 9_383_947, 1_141_896],
            ),
            (
                "00000000000000000000000000000000000000b2",
                0xee85_fafd_354b_0987,
                [4_110_087, 5_868_036, 7_625_985, 9_383_934, 1_141_883],
            ),
            (
                "00000000000000000000000000000000000000c3",
                0xee85_fafd_354b_09f6,
                [4_110_198, 5_868_147, 7_626_096, 9_384_045, 1_141_994],
            ),
            (
                "000102030405060708090a0b0c0d0e0f10111213",
                0x122b_1725_fda2_3eb1,
                [5_268_529, 82_390, 4_896_251, 9_710_112, 4_523_973],
            ),
        ];

        for (hex, sum, indices) in vectors {
            let info_hash = id(hex);
            assert_eq!(fnv1_64(info_hash.as_bytes()), sum, "{hex}");
            assert_eq!(hash_indices(info_hash), indices, "{hex}");
        }

        let runtime_indices = [
            hash_indices(id("00000000000000000000000000000000000000a1")),
            hash_indices(id("00000000000000000000000000000000000000b2")),
            hash_indices(id("00000000000000000000000000000000000000c3")),
        ];
        for left in 0..runtime_indices.len() {
            for right in left + 1..runtime_indices.len() {
                assert!(
                    runtime_indices[left]
                        .iter()
                        .all(|index| !runtime_indices[right].contains(index)),
                    "oracle A/B/C membership indexes must be pairwise disjoint"
                );
            }
        }
    }

    #[test]
    fn membership_test_precedes_decrement_and_every_call_restores_maximum() {
        let info_hash = id("00000000000000000000000000000000000000a1");
        let indices = hash_indices(info_hash);
        let deduper = DhtInfoHashDeduper::with_decrement_starts([5_000_000, indices[0], 5_000_000]);

        assert!(!deduper.test_and_add(info_hash));
        assert!(deduper.test_and_add(info_hash));
        assert!(deduper.test_and_add(info_hash));
    }

    #[test]
    fn fresh_observation_is_absent_and_adjacent_duplicate_is_present() {
        let info_hash = id("00000000000000000000000000000000000000a1");
        let deduper = DhtInfoHashDeduper::with_decrement_starts([0, 0]);

        assert!(!deduper.test_and_add(info_hash));
        assert!(deduper.test_and_add(info_hash));
    }

    #[test]
    fn scripted_stable_eviction_can_make_a_prior_hash_absent() {
        let victim = id("00000000000000000000000000000000000000a1");
        let filler = Id20::ZERO;
        let victim_indices = hash_indices(victim);
        let starts = std::iter::once(5_000_000)
            .chain(
                victim_indices
                    .into_iter()
                    .flat_map(|index| [index, index, index]),
            )
            .chain(std::iter::once(5_000_000));
        let deduper = DhtInfoHashDeduper::with_decrement_starts(starts);

        assert!(!deduper.test_and_add(victim));
        for _ in 0..HASH_FUNCTIONS * usize::from(MAX_CELL_VALUE) {
            let _already_present = deduper.test_and_add(filler);
        }
        assert!(!deduper.test_and_add(victim));
    }

    #[test]
    fn clones_share_state_while_fresh_instances_are_process_local() {
        let info_hash = id("00000000000000000000000000000000000000b2");
        let first = DhtInfoHashDeduper::with_decrement_starts([0, 0]);
        let clone = first.clone();
        let separate = DhtInfoHashDeduper::with_decrement_starts([0]);

        assert!(!first.test_and_add(info_hash));
        assert!(clone.test_and_add(info_hash));
        assert!(!separate.test_and_add(info_hash));
    }

    #[test]
    fn concurrent_clones_serialize_test_and_add_into_one_miss() {
        const CALLS: usize = 8;
        let info_hash = id("00000000000000000000000000000000000000c3");
        let deduper = DhtInfoHashDeduper::new();
        let barrier = Arc::new(Barrier::new(CALLS));
        let handles = (0..CALLS)
            .map(|_| {
                let deduper = deduper.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    deduper.test_and_add(info_hash)
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|&&present| !present).count(), 1);
        assert_eq!(results.iter().filter(|&&present| present).count(), 7);
        assert!(deduper.test_and_add(info_hash));
    }
}
