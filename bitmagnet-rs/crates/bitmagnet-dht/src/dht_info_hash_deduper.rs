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
use std::sync::{Arc, Mutex};

use crate::Id20;

const CELL_COUNT: usize = 10_000_000;
const BITS_PER_CELL: usize = 2;
const CELLS_PER_BYTE: usize = 8 / BITS_PER_CELL;
const HASH_FUNCTIONS: usize = 5;
const DECREMENT_CELLS: usize = 49;
const MAX_CELL_VALUE: u8 = 3;
const CELL_PAYLOAD_BYTES: usize = CELL_COUNT * BITS_PER_CELL / 8;
const FNV1_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1_PRIME: u64 = 0x0000_0100_0000_01b3;

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
    cells: Box<[u8]>,
    index_buffer: [usize; HASH_FUNCTIONS],
    decrement_starts: DecrementStartSource,
}

enum DecrementStartSource {
    Random(fastrand::Rng),
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
        Self::with_source(DecrementStartSource::Random(fastrand::Rng::new()))
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
        state.index_buffer = hash_indices(info_hash);
        let already_present = state
            .index_buffer
            .iter()
            .all(|&index| get_cell(&state.cells, index) != 0);

        let decrement_start = state.decrement_starts.next();
        decrement_adjacent(
            &mut state.cells,
            decrement_start,
            DECREMENT_CELLS,
            CELL_COUNT,
        );

        let indices = state.index_buffer;
        for index in indices {
            set_cell(&mut state.cells, index, MAX_CELL_VALUE);
        }
        already_present
    }

    fn with_source(decrement_starts: DecrementStartSource) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DhtInfoHashDeduperState {
                cells: vec![0; CELL_PAYLOAD_BYTES].into_boxed_slice(),
                index_buffer: [0; HASH_FUNCTIONS],
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

impl DecrementStartSource {
    fn next(&mut self) -> usize {
        match self {
            Self::Random(rng) => rng.usize(..CELL_COUNT),
            #[cfg(test)]
            Self::Scripted(starts) => starts
                .pop_front()
                .expect("scripted decrement start exhausted"),
        }
    }
}

fn fnv1_64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV1_OFFSET, |hash, &byte| {
        hash.wrapping_mul(FNV1_PRIME) ^ u64::from(byte)
    })
}

fn hash_indices(info_hash: Id20) -> [usize; HASH_FUNCTIONS] {
    let sum = fnv1_64(info_hash.as_bytes());
    let low = u64::from(sum as u32);
    let high = u64::from((sum >> 32) as u32);
    std::array::from_fn(|index| ((low + high * index as u64) % CELL_COUNT as u64) as usize)
}

fn get_cell(cells: &[u8], index: usize) -> u8 {
    let byte_index = index / CELLS_PER_BYTE;
    let shift = (index % CELLS_PER_BYTE) * BITS_PER_CELL;
    (cells[byte_index] >> shift) & MAX_CELL_VALUE
}

fn set_cell(cells: &mut [u8], index: usize, value: u8) {
    debug_assert!(value <= MAX_CELL_VALUE);
    let byte_index = index / CELLS_PER_BYTE;
    let shift = (index % CELLS_PER_BYTE) * BITS_PER_CELL;
    let mask = MAX_CELL_VALUE << shift;
    cells[byte_index] = (cells[byte_index] & !mask) | ((value & MAX_CELL_VALUE) << shift);
}

fn decrement_cell(cells: &mut [u8], index: usize) {
    let value = get_cell(cells, index);
    if value != 0 {
        set_cell(cells, index, value - 1);
    }
}

fn decrement_adjacent(cells: &mut [u8], start: usize, count: usize, cell_count: usize) {
    debug_assert!(start < cell_count);
    debug_assert!(cell_count <= cells.len() * CELLS_PER_BYTE);
    for offset in 0..count {
        decrement_cell(cells, (start + offset) % cell_count);
    }
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
        assert_eq!(state.cells.len(), CELL_PAYLOAD_BYTES);
        assert_eq!(state.index_buffer.len(), HASH_FUNCTIONS);
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
    fn packed_cells_preserve_all_four_neighbors_and_clamp_at_zero() {
        let mut cells = [0_u8; 2];
        for (index, value) in [1, 2, 3, 1, 2, 3, 1, 2].into_iter().enumerate() {
            set_cell(&mut cells, index, value);
        }
        assert_eq!(cells, [0b01_11_10_01, 0b10_01_11_10]);

        decrement_cell(&mut cells, 2);
        decrement_cell(&mut cells, 0);
        decrement_cell(&mut cells, 0);
        assert_eq!(
            (0..8)
                .map(|index| get_cell(&cells, index))
                .collect::<Vec<_>>(),
            vec![0, 2, 2, 1, 2, 3, 1, 2]
        );
    }

    #[test]
    fn adjacent_decrement_wraps_at_the_last_cell() {
        let mut cells = [0_u8; 2];
        for index in [0, 1, 6, 7] {
            set_cell(&mut cells, index, 1);
        }
        set_cell(&mut cells, 2, 2);

        decrement_adjacent(&mut cells, 6, 4, 8);

        assert_eq!(get_cell(&cells, 6), 0);
        assert_eq!(get_cell(&cells, 7), 0);
        assert_eq!(get_cell(&cells, 0), 0);
        assert_eq!(get_cell(&cells, 1), 0);
        assert_eq!(get_cell(&cells, 2), 2);
    }

    #[test]
    fn membership_test_precedes_decrement_and_every_call_restores_maximum() {
        let info_hash = id("00000000000000000000000000000000000000a1");
        let indices = hash_indices(info_hash);
        let deduper = DhtInfoHashDeduper::with_decrement_starts([indices[0]]);
        {
            let mut state = deduper.inner.lock().unwrap();
            for index in indices {
                set_cell(&mut state.cells, index, 1);
            }
        }

        assert!(deduper.test_and_add(info_hash));
        let state = deduper.inner.lock().unwrap();
        assert!(indices
            .into_iter()
            .all(|index| get_cell(&state.cells, index) == MAX_CELL_VALUE));
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
