//! [`InfoHash`] — the 20-byte BitTorrent v1 info hash, mirroring Go's
//! `protocol.ID` (`internal/protocol/id.go`).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Length in bytes of a BitTorrent v1 info hash.
pub const INFO_HASH_LEN: usize = 20;

/// A 20-byte BitTorrent v1 info hash.
///
/// Mirrors Go's `protocol.ID` (`type ID [20]byte`): in PostgreSQL it is stored
/// as the raw 20 bytes (`bytea`), while its canonical text form is lowercase
/// hex (see [`fmt::Display`], mirroring `protocol.ID.String()`). Serde
/// (de)serialises it as that hex string, matching the Go JSON representation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InfoHash([u8; INFO_HASH_LEN]);

impl InfoHash {
    /// Wraps a fixed 20-byte array.
    pub const fn new(bytes: [u8; INFO_HASH_LEN]) -> Self {
        Self(bytes)
    }

    /// Builds an info hash from a byte slice, which must be exactly 20 bytes
    /// (mirrors `protocol.NewIDFromByteSlice`).
    pub fn from_slice(bytes: &[u8]) -> Result<Self, InfoHashError> {
        let arr: [u8; INFO_HASH_LEN] = bytes
            .try_into()
            .map_err(|_| InfoHashError::Length(bytes.len()))?;
        Ok(Self(arr))
    }

    /// The raw 20 bytes, as stored in PostgreSQL (`bytea`).
    pub const fn as_bytes(&self) -> &[u8; INFO_HASH_LEN] {
        &self.0
    }

    /// The raw bytes as a slice (convenient for binding to SQL parameters).
    pub const fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHash({self})")
    }
}

impl FromStr for InfoHash {
    type Err = InfoHashError;

    /// Parses lowercase or uppercase hex, optionally prefixed with `0x`
    /// (mirrors `protocol.ParseID`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        if s.len() != INFO_HASH_LEN * 2 {
            return Err(InfoHashError::Length(s.len() / 2));
        }
        let bytes = s.as_bytes();
        let mut out = [0u8; INFO_HASH_LEN];
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = hex_val(bytes[i * 2])?;
            let lo = hex_val(bytes[i * 2 + 1])?;
            *slot = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

fn hex_val(c: u8) -> Result<u8, InfoHashError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(InfoHashError::InvalidHex(other as char)),
    }
}

impl Serialize for InfoHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for InfoHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Errors constructing an [`InfoHash`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InfoHashError {
    /// The input did not decode to exactly 20 bytes.
    #[error("info hash must be 20 bytes, got {0}")]
    Length(usize),
    /// A non-hex character was found while parsing.
    #[error("invalid hex character: {0:?}")]
    InvalidHex(char),
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn hex_round_trip() {
        let ih: InfoHash = HEX.parse().unwrap();
        assert_eq!(ih.to_string(), HEX);
        assert_eq!(ih.as_slice().len(), 20);
        assert_eq!(ih.as_bytes()[0], 0x01);
    }

    #[test]
    fn accepts_0x_prefix_and_uppercase() {
        let lower: InfoHash = HEX.parse().unwrap();
        let prefixed: InfoHash = format!("0x{}", HEX.to_uppercase()).parse().unwrap();
        assert_eq!(lower, prefixed);
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!("abc".parse::<InfoHash>(), Err(InfoHashError::Length(1)));
        // Right length, but contains a non-hex character ('z').
        let bad = "z".repeat(40);
        assert_eq!(bad.parse::<InfoHash>(), Err(InfoHashError::InvalidHex('z')));
        assert!(InfoHash::from_slice(&[0u8; 19]).is_err());
        assert!(InfoHash::from_slice(&[0u8; 20]).is_ok());
    }

    #[test]
    fn serde_is_hex_string() {
        // Verified via MessagePack (rmp-serde, already a dependency): the value
        // is encoded as the hex *string*, and round-trips back to the same hash.
        let ih: InfoHash = HEX.parse().unwrap();
        let bytes = rmp_serde::to_vec(&ih).unwrap();
        let as_string: String = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(as_string, HEX);
        let back: InfoHash = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(ih, back);
    }
}
