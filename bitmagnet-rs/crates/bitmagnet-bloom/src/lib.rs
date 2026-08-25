//! A bounded stable Bloom filter compatible with the raw binary format and
//! membership math of bitmagnet's pinned Go `BoomFilters` dependency.
//!
//! This crate deliberately separates deterministic filter state from the
//! decrement-start source. Callers may use [`RandomDecrementStartSource`] in
//! production and inject a scripted source in tests. Random generator state is
//! neither encoded nor part of the compatibility contract.
//!
//! The codec is the byte stream written by Go's
//! `(*StableBloomFilter).WriteTo`, not an outer `encoding/gob` envelope. Decode
//! requires an already validated expected geometry, bounds all reads by that
//! geometry, and rejects mismatched headers, invalid cached indexes, truncated
//! input, and trailing bytes before publishing a filter.

use std::fmt;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;

const FNV1_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1_PRIME: u64 = 0x0000_0100_0000_01b3;
const RAW_FIXED_BYTES: usize = 3 * size_of::<u64>()
    + size_of::<u8>()
    + size_of::<u64>()
    + 2 * size_of::<u8>()
    + 2 * size_of::<u64>();

/// Validated stable-Bloom geometry recorded in the Go-compatible byte stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StableBloomGeometry {
    cell_count: NonZeroUsize,
    bits_per_cell: u8,
    hash_functions: NonZeroUsize,
    decrement_cells: usize,
    packed_bytes: usize,
    encoded_bytes: usize,
}

impl StableBloomGeometry {
    /// Validate an explicit geometry.
    ///
    /// `bits_per_cell` may be 1 through 8, matching the representable Go
    /// bucket widths. Hash functions and decrement cells may not exceed the
    /// cell count; zero decrement cells is the non-evicting special case.
    pub fn new(
        cell_count: usize,
        bits_per_cell: u8,
        hash_functions: usize,
        decrement_cells: usize,
    ) -> Result<Self, StableBloomGeometryError> {
        let cell_count =
            NonZeroUsize::new(cell_count).ok_or(StableBloomGeometryError::ZeroCellCount)?;
        if !(1..=8).contains(&bits_per_cell) {
            return Err(StableBloomGeometryError::InvalidBitsPerCell(bits_per_cell));
        }
        let hash_functions =
            NonZeroUsize::new(hash_functions).ok_or(StableBloomGeometryError::ZeroHashFunctions)?;
        if hash_functions.get() > cell_count.get() {
            return Err(StableBloomGeometryError::TooManyHashFunctions {
                hash_functions: hash_functions.get(),
                cell_count: cell_count.get(),
            });
        }
        if decrement_cells > cell_count.get() {
            return Err(StableBloomGeometryError::TooManyDecrementCells {
                decrement_cells,
                cell_count: cell_count.get(),
            });
        }

        let packed_bits = cell_count
            .get()
            .checked_mul(usize::from(bits_per_cell))
            .ok_or(StableBloomGeometryError::PackedLengthOverflow)?;
        let packed_bytes = packed_bits
            .checked_add(7)
            .ok_or(StableBloomGeometryError::PackedLengthOverflow)?
            / 8;
        let index_bytes = hash_functions
            .get()
            .checked_mul(size_of::<u64>())
            .ok_or(StableBloomGeometryError::EncodedLengthOverflow)?;
        let encoded_bytes = RAW_FIXED_BYTES
            .checked_add(index_bytes)
            .and_then(|value| value.checked_add(packed_bytes))
            .ok_or(StableBloomGeometryError::EncodedLengthOverflow)?;
        // The pinned Go Buckets writer computes each cell's starting bit as a
        // uint32 multiplication and advances within a split write with uint32
        // addition. Keep the complete last cell inside that 2^32-bit address
        // space so neither operation can wrap relative to this usize-based
        // implementation.
        const GO_MUTATION_BIT_CAPACITY: u128 = u32::MAX as u128 + 1;
        if packed_bits as u128 > GO_MUTATION_BIT_CAPACITY {
            return Err(StableBloomGeometryError::GoMutationOffsetOverflow {
                cell_count: cell_count.get(),
                bits_per_cell,
            });
        }

        Ok(Self {
            cell_count,
            bits_per_cell,
            hash_functions,
            decrement_cells,
            packed_bytes,
            encoded_bytes,
        })
    }

    #[must_use]
    pub const fn cell_count(self) -> usize {
        self.cell_count.get()
    }

    #[must_use]
    pub const fn bits_per_cell(self) -> u8 {
        self.bits_per_cell
    }

    #[must_use]
    pub const fn hash_functions(self) -> usize {
        self.hash_functions.get()
    }

    #[must_use]
    pub const fn decrement_cells(self) -> usize {
        self.decrement_cells
    }

    #[must_use]
    pub const fn max_cell_value(self) -> u8 {
        if self.bits_per_cell == 8 {
            u8::MAX
        } else {
            ((1_u16 << self.bits_per_cell) - 1) as u8
        }
    }

    #[must_use]
    pub const fn packed_bytes(self) -> usize {
        self.packed_bytes
    }

    /// Exact number of bytes emitted by the compatible raw codec.
    #[must_use]
    pub const fn encoded_bytes(self) -> usize {
        self.encoded_bytes
    }
}

/// Invalid or unrepresentable filter geometry.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum StableBloomGeometryError {
    #[error("stable Bloom filter cell count must be nonzero")]
    ZeroCellCount,
    #[error("stable Bloom filter bits per cell must be in 1..=8, got {0}")]
    InvalidBitsPerCell(u8),
    #[error("stable Bloom filter hash-function count must be nonzero")]
    ZeroHashFunctions,
    #[error(
        "stable Bloom filter hash-function count {hash_functions} exceeds cell count {cell_count}"
    )]
    TooManyHashFunctions {
        hash_functions: usize,
        cell_count: usize,
    },
    #[error(
        "stable Bloom filter decrement count {decrement_cells} exceeds cell count {cell_count}"
    )]
    TooManyDecrementCells {
        decrement_cells: usize,
        cell_count: usize,
    },
    #[error("stable Bloom filter packed-cell length overflow")]
    PackedLengthOverflow,
    #[error("stable Bloom filter encoded length overflow")]
    EncodedLengthOverflow,
    #[error(
        "stable Bloom filter geometry ({cell_count} cells at {bits_per_cell} bits each) exceeds the pinned Go writer's 2^32-bit mutation-offset domain"
    )]
    GoMutationOffsetOverflow {
        cell_count: usize,
        bits_per_cell: u8,
    },
}

/// Supplies one valid random or scripted start for each stable-eviction pass.
pub trait DecrementStartSource {
    fn next_start(&mut self, cell_count: NonZeroUsize) -> usize;
}

/// Per-instance non-cryptographic decrement source.
pub struct RandomDecrementStartSource {
    rng: fastrand::Rng,
}

impl RandomDecrementStartSource {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rng: fastrand::Rng::new(),
        }
    }
}

impl Default for RandomDecrementStartSource {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RandomDecrementStartSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RandomDecrementStartSource(..)")
    }
}

impl DecrementStartSource for RandomDecrementStartSource {
    fn next_start(&mut self, cell_count: NonZeroUsize) -> usize {
        self.rng.usize(..cell_count.get())
    }
}

/// Packed stable Bloom state with BoomFilters-compatible membership math.
pub struct StableBloomFilter {
    geometry: StableBloomGeometry,
    cells: Box<[u8]>,
    index_buffer: Box<[usize]>,
}

impl fmt::Debug for StableBloomFilter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StableBloomFilter")
            .field("geometry", &self.geometry)
            .field("packed_bytes", &self.cells.len())
            .finish_non_exhaustive()
    }
}

impl StableBloomFilter {
    /// Allocate one empty filter with the supplied validated geometry.
    #[must_use]
    pub fn new(geometry: StableBloomGeometry) -> Self {
        Self {
            geometry,
            cells: vec![0; geometry.packed_bytes()].into_boxed_slice(),
            index_buffer: vec![0; geometry.hash_functions()].into_boxed_slice(),
        }
    }

    #[must_use]
    pub const fn geometry(&self) -> StableBloomGeometry {
        self.geometry
    }

    /// Packed cell payload, excluding the raw codec headers.
    #[must_use]
    pub fn packed_cells(&self) -> &[u8] {
        &self.cells
    }

    /// Test probabilistic membership without mutating the filter.
    #[must_use]
    pub fn test(&self, data: &[u8]) -> bool {
        let (low, high) = hash_kernel(data);
        (0..self.geometry.hash_functions()).all(|ordinal| {
            let index = bloom_index(low, high, ordinal, self.geometry.cell_count());
            get_cell(&self.cells, self.geometry.bits_per_cell(), index) != 0
        })
    }

    /// Evict one adjacent run, then set every membership cell to its maximum.
    pub fn add<S: DecrementStartSource + ?Sized>(
        &mut self,
        data: &[u8],
        source: &mut S,
    ) -> &mut Self {
        self.decrement(source);
        let (low, high) = hash_kernel(data);
        for ordinal in 0..self.geometry.hash_functions() {
            let index = bloom_index(low, high, ordinal, self.geometry.cell_count());
            set_cell(
                &mut self.cells,
                self.geometry.bits_per_cell(),
                index,
                self.geometry.max_cell_value(),
            );
        }
        self
    }

    /// Test before eviction, then perform the same mutation as [`Self::add`].
    #[must_use]
    pub fn test_and_add<S: DecrementStartSource + ?Sized>(
        &mut self,
        data: &[u8],
        source: &mut S,
    ) -> bool {
        let (low, high) = hash_kernel(data);
        let bits_per_cell = self.geometry.bits_per_cell();
        let cell_count = self.geometry.cell_count();
        let mut member = true;
        for (ordinal, slot) in self.index_buffer.iter_mut().enumerate() {
            *slot = bloom_index(low, high, ordinal, cell_count);
            if get_cell(&self.cells, bits_per_cell, *slot) == 0 {
                member = false;
            }
        }

        self.decrement(source);
        let max = self.geometry.max_cell_value();
        for &index in self.index_buffer.iter() {
            set_cell(&mut self.cells, bits_per_cell, index, max);
        }
        member
    }

    /// Encode the exact raw stream produced by BoomFilters `WriteTo`.
    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<usize, StableBloomCodecError> {
        write_u64(&mut writer, self.geometry.cell_count())?;
        write_u64(&mut writer, self.geometry.decrement_cells())?;
        write_u64(&mut writer, self.geometry.hash_functions())?;
        writer.write_all(&[self.geometry.max_cell_value()])?;
        write_u64(&mut writer, self.index_buffer.len())?;
        for &index in self.index_buffer.iter() {
            write_u64(&mut writer, index)?;
        }
        writer.write_all(&[
            self.geometry.bits_per_cell(),
            self.geometry.max_cell_value(),
        ])?;
        write_u64(&mut writer, self.geometry.cell_count())?;
        write_u64(&mut writer, self.cells.len())?;
        writer.write_all(&self.cells)?;
        Ok(self.geometry.encoded_bytes())
    }

    /// Decode one exact raw BoomFilters stream for `expected` geometry.
    ///
    /// The expected encoded length is checked before any filter allocation.
    pub fn from_bytes(
        bytes: &[u8],
        expected: StableBloomGeometry,
    ) -> Result<Self, StableBloomCodecError> {
        match bytes.len().cmp(&expected.encoded_bytes()) {
            std::cmp::Ordering::Less => {
                return Err(StableBloomCodecError::Truncated {
                    expected: expected.encoded_bytes(),
                    actual: bytes.len(),
                })
            }
            std::cmp::Ordering::Greater => {
                return Err(StableBloomCodecError::TrailingBytes {
                    expected: expected.encoded_bytes(),
                    actual_at_least: bytes.len(),
                })
            }
            std::cmp::Ordering::Equal => {}
        }

        let mut cursor = SliceCursor::new(bytes);
        expect_u64(&mut cursor, "cell_count", expected.cell_count())?;
        expect_u64(&mut cursor, "decrement_cells", expected.decrement_cells())?;
        expect_u64(&mut cursor, "hash_functions", expected.hash_functions())?;
        expect_u8(&mut cursor, "max_cell_value", expected.max_cell_value())?;
        expect_u64(
            &mut cursor,
            "index_buffer_length",
            expected.hash_functions(),
        )?;

        let mut index_buffer = Vec::with_capacity(expected.hash_functions());
        for ordinal in 0..expected.hash_functions() {
            let raw = cursor.read_u64();
            let index =
                usize::try_from(raw).map_err(|_| StableBloomCodecError::CachedIndexOutOfRange {
                    ordinal,
                    index: raw,
                    cell_count: expected.cell_count(),
                })?;
            if index >= expected.cell_count() {
                return Err(StableBloomCodecError::CachedIndexOutOfRange {
                    ordinal,
                    index: raw,
                    cell_count: expected.cell_count(),
                });
            }
            index_buffer.push(index);
        }

        expect_u8(&mut cursor, "bucket_bits", expected.bits_per_cell())?;
        expect_u8(&mut cursor, "bucket_max", expected.max_cell_value())?;
        expect_u64(&mut cursor, "bucket_count", expected.cell_count())?;
        expect_u64(&mut cursor, "bucket_data_length", expected.packed_bytes())?;
        let cells = cursor
            .take(expected.packed_bytes())
            .to_vec()
            .into_boxed_slice();
        debug_assert_eq!(cursor.remaining(), 0);

        Ok(Self {
            geometry: expected,
            cells,
            index_buffer: index_buffer.into_boxed_slice(),
        })
    }

    /// Read and strictly decode one bounded raw stream.
    pub fn read_from<R: Read>(
        reader: R,
        expected: StableBloomGeometry,
    ) -> Result<Self, StableBloomCodecError> {
        let limit = expected
            .encoded_bytes()
            .checked_add(1)
            .ok_or(StableBloomCodecError::ReadLimitOverflow)?;
        let mut bytes = Vec::with_capacity(limit);
        let limit = u64::try_from(limit).map_err(|_| StableBloomCodecError::ReadLimitOverflow)?;
        reader.take(limit).read_to_end(&mut bytes)?;
        Self::from_bytes(&bytes, expected)
    }

    fn decrement<S: DecrementStartSource + ?Sized>(&mut self, source: &mut S) {
        let start = source.next_start(self.geometry.cell_count);
        assert!(
            start < self.geometry.cell_count(),
            "decrement source returned out-of-range cell {start} for {} cells",
            self.geometry.cell_count()
        );
        decrement_adjacent(
            &mut self.cells,
            self.geometry.bits_per_cell(),
            start,
            self.geometry.decrement_cells(),
            self.geometry.cell_count(),
        );
    }
}

/// Strict raw-codec failure.
#[derive(Debug, thiserror::Error)]
pub enum StableBloomCodecError {
    #[error("stable Bloom filter I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("stable Bloom filter stream is truncated: expected {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },
    #[error(
        "stable Bloom filter stream has trailing bytes: expected {expected}, got at least {actual_at_least}"
    )]
    TrailingBytes {
        expected: usize,
        actual_at_least: usize,
    },
    #[error("stable Bloom filter field {field} mismatch: expected {expected}, got {actual}")]
    HeaderMismatch {
        field: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("stable Bloom filter cached index {ordinal} is {index}, outside {cell_count} cells")]
    CachedIndexOutOfRange {
        ordinal: usize,
        index: u64,
        cell_count: usize,
    },
    #[error("stable Bloom filter bounded read length overflow")]
    ReadLimitOverflow,
}

/// Go-compatible FNV-1 64-bit hash.
#[must_use]
pub fn fnv1_64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV1_OFFSET, |hash, &byte| {
        hash.wrapping_mul(FNV1_PRIME) ^ u64::from(byte)
    })
}

/// Go-compatible FNV-1 double-hash indexes for explicit geometry.
#[must_use]
pub fn hash_indices(data: &[u8], geometry: StableBloomGeometry) -> Vec<usize> {
    let (low, high) = hash_kernel(data);
    (0..geometry.hash_functions())
        .map(|ordinal| bloom_index(low, high, ordinal, geometry.cell_count()))
        .collect()
}

fn hash_kernel(data: &[u8]) -> (u64, u64) {
    let sum = fnv1_64(data);
    (u64::from(sum as u32), u64::from((sum >> 32) as u32))
}

fn bloom_index(low: u64, high: u64, ordinal: usize, cell_count: usize) -> usize {
    (low.wrapping_add(high.wrapping_mul(ordinal as u64)) % cell_count as u64) as usize
}

fn get_cell(cells: &[u8], bits_per_cell: u8, index: usize) -> u8 {
    get_bits(cells, index * usize::from(bits_per_cell), bits_per_cell)
}

fn set_cell(cells: &mut [u8], bits_per_cell: u8, index: usize, value: u8) {
    set_bits(
        cells,
        index * usize::from(bits_per_cell),
        bits_per_cell,
        value,
    );
}

fn get_bits(data: &[u8], mut offset: usize, mut length: u8) -> u8 {
    let mut value = 0_u8;
    let mut output_shift = 0_u8;
    while length != 0 {
        let byte_index = offset / 8;
        let byte_offset = (offset % 8) as u8;
        let take = length.min(8 - byte_offset);
        let mask = if take == 8 {
            u8::MAX
        } else {
            (1_u8 << take) - 1
        };
        value |= ((data[byte_index] >> byte_offset) & mask) << output_shift;
        offset += usize::from(take);
        output_shift += take;
        length -= take;
    }
    value
}

fn set_bits(data: &mut [u8], mut offset: usize, mut length: u8, mut value: u8) {
    while length != 0 {
        let byte_index = offset / 8;
        let byte_offset = (offset % 8) as u8;
        let take = length.min(8 - byte_offset);
        let low_mask = if take == 8 {
            u8::MAX
        } else {
            (1_u8 << take) - 1
        };
        let mask = low_mask << byte_offset;
        data[byte_index] = (data[byte_index] & !mask) | ((value & low_mask) << byte_offset);
        offset += usize::from(take);
        value = if take == 8 { 0 } else { value >> take };
        length -= take;
    }
}

fn decrement_adjacent(
    cells: &mut [u8],
    bits_per_cell: u8,
    start: usize,
    count: usize,
    cell_count: usize,
) {
    for offset in 0..count {
        let index = (start + offset) % cell_count;
        let value = get_cell(cells, bits_per_cell, index);
        if value != 0 {
            set_cell(cells, bits_per_cell, index, value - 1);
        }
    }
}

fn write_u64(writer: &mut impl Write, value: usize) -> io::Result<()> {
    let value = u64::try_from(value).expect("validated Bloom geometry fits u64");
    writer.write_all(&value.to_be_bytes())
}

fn expect_u64(
    cursor: &mut SliceCursor<'_>,
    field: &'static str,
    expected: usize,
) -> Result<(), StableBloomCodecError> {
    let actual = cursor.read_u64();
    let expected = u64::try_from(expected).expect("validated Bloom geometry fits u64");
    if actual != expected {
        return Err(StableBloomCodecError::HeaderMismatch {
            field,
            expected,
            actual,
        });
    }
    Ok(())
}

fn expect_u8(
    cursor: &mut SliceCursor<'_>,
    field: &'static str,
    expected: u8,
) -> Result<(), StableBloomCodecError> {
    let actual = cursor.read_u8();
    if actual != expected {
        return Err(StableBloomCodecError::HeaderMismatch {
            field,
            expected: u64::from(expected),
            actual: u64::from(actual),
        });
    }
    Ok(())
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u8(&mut self) -> u8 {
        let value = self.bytes[self.position];
        self.position += 1;
        value
    }

    fn read_u64(&mut self) -> u64 {
        let bytes: [u8; 8] = self.take(8).try_into().expect("length prevalidated");
        u64::from_be_bytes(bytes)
    }

    fn take(&mut self, length: usize) -> &'a [u8] {
        let end = self.position + length;
        let bytes = &self.bytes[self.position..end];
        self.position = end;
        bytes
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Cursor;

    use super::*;

    #[derive(Debug)]
    struct ScriptedStarts(VecDeque<usize>);

    impl ScriptedStarts {
        fn new(starts: impl IntoIterator<Item = usize>) -> Self {
            Self(starts.into_iter().collect())
        }
    }

    impl DecrementStartSource for ScriptedStarts {
        fn next_start(&mut self, _cell_count: NonZeroUsize) -> usize {
            self.0.pop_front().expect("scripted start exhausted")
        }
    }

    fn geometry(cells: usize, bits: u8, hashes: usize, decrement: usize) -> StableBloomGeometry {
        StableBloomGeometry::new(cells, bits, hashes, decrement).unwrap()
    }

    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}

    #[test]
    fn geometry_is_validated_and_lengths_are_bounded() {
        assert_eq!(
            StableBloomGeometry::new(0, 2, 5, 49),
            Err(StableBloomGeometryError::ZeroCellCount)
        );
        assert_eq!(
            StableBloomGeometry::new(1, 0, 1, 0),
            Err(StableBloomGeometryError::InvalidBitsPerCell(0))
        );
        assert_eq!(
            StableBloomGeometry::new(1, 9, 1, 0),
            Err(StableBloomGeometryError::InvalidBitsPerCell(9))
        );
        assert_eq!(
            StableBloomGeometry::new(1, 1, 0, 0),
            Err(StableBloomGeometryError::ZeroHashFunctions)
        );
        assert!(matches!(
            StableBloomGeometry::new(1, 1, 2, 0),
            Err(StableBloomGeometryError::TooManyHashFunctions { .. })
        ));
        assert!(matches!(
            StableBloomGeometry::new(1, 1, 1, 2),
            Err(StableBloomGeometryError::TooManyDecrementCells { .. })
        ));
        assert_eq!(
            StableBloomGeometry::new(usize::MAX, 8, 1, 0),
            Err(StableBloomGeometryError::PackedLengthOverflow)
        );
        assert_eq!(
            StableBloomGeometry::new(usize::MAX / 4, 1, usize::MAX / 4, 0),
            Err(StableBloomGeometryError::EncodedLengthOverflow)
        );

        let process_local = geometry(10_000_000, 2, 5, 49);
        assert_eq!(process_local.packed_bytes(), 2_500_000);
        assert_eq!(process_local.encoded_bytes(), 2_500_091);
        let persistent = geometry(100_000_000, 2, 5, 49);
        assert_eq!(persistent.packed_bytes(), 25_000_000);
        assert_eq!(persistent.encoded_bytes(), 25_000_091);
        assert_eq!(persistent.max_cell_value(), 3);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn geometry_stays_within_the_go_uint32_mutation_offset_domain() {
        let last_compatible_cell_count = (u32::MAX as usize + 1) / 8;
        let boundary = geometry(last_compatible_cell_count, 8, 1, 0);
        assert_eq!(boundary.packed_bytes(), 1_usize << 29);

        assert_eq!(
            StableBloomGeometry::new(last_compatible_cell_count + 1, 8, 1, 0),
            Err(StableBloomGeometryError::GoMutationOffsetOverflow {
                cell_count: last_compatible_cell_count + 1,
                bits_per_cell: 8,
            })
        );
    }

    #[test]
    fn fnv1_and_double_hash_vectors_match_go() {
        let bytes = hex_bytes("000102030405060708090a0b0c0d0e0f10111213");
        assert_eq!(fnv1_64(&bytes), 0x122b_1725_fda2_3eb1);
        assert_eq!(
            hash_indices(&bytes, geometry(10_000_000, 2, 5, 49)),
            [5_268_529, 82_390, 4_896_251, 9_710_112, 4_523_973]
        );
    }

    #[test]
    fn arbitrary_bucket_widths_pack_across_byte_boundaries() {
        let mut bytes = [0_u8; 3];
        for (index, value) in [1, 6, 3, 7, 4].into_iter().enumerate() {
            set_cell(&mut bytes, 3, index, value);
        }
        assert_eq!(
            (0..5)
                .map(|index| get_cell(&bytes, 3, index))
                .collect::<Vec<_>>(),
            [1, 6, 3, 7, 4]
        );
        assert_eq!(get_bits(&bytes, 15, 3), 0);

        let mut full_byte = [0_u8; 1];
        set_cell(&mut full_byte, 8, 0, u8::MAX);
        assert_eq!(get_cell(&full_byte, 8, 0), u8::MAX);
    }

    #[test]
    fn deterministic_decrement_wraps_and_clamps() {
        let mut bytes = [0_u8; 2];
        for index in [0, 1, 6, 7] {
            set_cell(&mut bytes, 2, index, 1);
        }
        set_cell(&mut bytes, 2, 2, 2);

        decrement_adjacent(&mut bytes, 2, 6, 4, 8);

        assert_eq!(
            (0..8)
                .map(|index| get_cell(&bytes, 2, index))
                .collect::<Vec<_>>(),
            [0, 0, 2, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_add_and_test_and_add_match_boom_order() {
        let geometry = geometry(128, 2, 5, 4);
        let data = b"twenty-byte-ish-hash";
        let indices = hash_indices(data, geometry);
        let mut filter = StableBloomFilter::new(geometry);
        for &index in &indices {
            set_cell(&mut filter.cells, 2, index, 1);
        }
        let mut starts = ScriptedStarts::new([indices[0], 0]);

        assert!(filter.test_and_add(data, &mut starts));
        assert!(indices
            .iter()
            .all(|&index| get_cell(&filter.cells, 2, index) == 3));
        filter.add(data, &mut starts);
        assert!(filter.test(data));
    }

    #[test]
    fn zero_decrement_geometry_still_consumes_one_go_compatible_start() {
        let geometry = geometry(8, 1, 1, 0);
        let mut filter = StableBloomFilter::new(geometry);
        let mut starts = ScriptedStarts::new([0, 1]);

        filter.add(b"first", &mut starts);
        filter.add(b"second", &mut starts);
        assert!(starts.0.is_empty());
    }

    #[test]
    fn raw_codec_offsets_and_round_trip_match_write_to_shape() {
        let geometry = geometry(8, 2, 2, 1);
        let mut filter = StableBloomFilter::new(geometry);
        let mut starts = ScriptedStarts::new([0]);
        let _ = filter.test_and_add(b"codec", &mut starts);
        let mut encoded = Vec::new();

        assert_eq!(filter.write_to(&mut encoded).unwrap(), 69);
        assert_eq!(encoded.len(), 69);
        assert_eq!(&encoded[0..8], &8_u64.to_be_bytes());
        assert_eq!(&encoded[8..16], &1_u64.to_be_bytes());
        assert_eq!(&encoded[16..24], &2_u64.to_be_bytes());
        assert_eq!(encoded[24], 3);
        assert_eq!(&encoded[25..33], &2_u64.to_be_bytes());
        assert_eq!(encoded[49], 2);
        assert_eq!(encoded[50], 3);
        assert_eq!(&encoded[51..59], &8_u64.to_be_bytes());
        assert_eq!(&encoded[59..67], &2_u64.to_be_bytes());

        let decoded = StableBloomFilter::from_bytes(&encoded, geometry).unwrap();
        assert_eq!(decoded.geometry(), geometry);
        assert_eq!(decoded.packed_cells(), filter.packed_cells());
        assert_eq!(decoded.index_buffer, filter.index_buffer);
        let mut reencoded = Vec::new();
        decoded.write_to(&mut reencoded).unwrap();
        assert_eq!(reencoded, encoded);
    }

    #[test]
    fn codec_rejects_every_header_family_and_cached_index() {
        let geometry = geometry(8, 2, 2, 1);
        let filter = StableBloomFilter::new(geometry);
        let mut encoded = Vec::new();
        filter.write_to(&mut encoded).unwrap();

        for (offset, field) in [
            (0, "cell_count"),
            (8, "decrement_cells"),
            (16, "hash_functions"),
            (25, "index_buffer_length"),
            (51, "bucket_count"),
            (59, "bucket_data_length"),
        ] {
            let mut malformed = encoded.clone();
            malformed[offset..offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());
            assert!(matches!(
                StableBloomFilter::from_bytes(&malformed, geometry),
                Err(StableBloomCodecError::HeaderMismatch { field: actual, .. }) if actual == field
            ));
        }
        for (offset, field) in [
            (24, "max_cell_value"),
            (49, "bucket_bits"),
            (50, "bucket_max"),
        ] {
            let mut malformed = encoded.clone();
            malformed[offset] ^= u8::MAX;
            assert!(matches!(
                StableBloomFilter::from_bytes(&malformed, geometry),
                Err(StableBloomCodecError::HeaderMismatch { field: actual, .. }) if actual == field
            ));
        }

        let mut malformed = encoded;
        malformed[33..41].copy_from_slice(&8_u64.to_be_bytes());
        assert!(matches!(
            StableBloomFilter::from_bytes(&malformed, geometry),
            Err(StableBloomCodecError::CachedIndexOutOfRange {
                ordinal: 0,
                index: 8,
                cell_count: 8
            })
        ));
    }

    #[test]
    fn codec_rejects_truncation_and_trailing_bytes_with_bounded_reads() {
        let geometry = geometry(8, 2, 2, 1);
        let filter = StableBloomFilter::new(geometry);
        let mut encoded = Vec::new();
        filter.write_to(&mut encoded).unwrap();

        assert!(matches!(
            StableBloomFilter::from_bytes(&encoded[..encoded.len() - 1], geometry),
            Err(StableBloomCodecError::Truncated { .. })
        ));
        encoded.extend([1, 2, 3]);
        assert!(matches!(
            StableBloomFilter::from_bytes(&encoded, geometry),
            Err(StableBloomCodecError::TrailingBytes {
                actual_at_least: 72,
                ..
            })
        ));
        assert!(matches!(
            StableBloomFilter::read_from(Cursor::new(encoded), geometry),
            Err(StableBloomCodecError::TrailingBytes {
                actual_at_least: 70,
                ..
            })
        ));
    }

    #[test]
    fn core_and_random_source_have_expected_thread_traits() {
        assert_send_sync::<StableBloomFilter>();
        assert_send::<RandomDecrementStartSource>();
        let mut source = RandomDecrementStartSource::new();
        let cells = NonZeroUsize::new(4).unwrap();
        for _ in 0..100 {
            assert!(source.next_start(cells) < cells.get());
        }
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid test hex"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }
}
