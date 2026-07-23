//! `ProtocolId` — the 20-byte BitTorrent info-hash, mirroring Go's
//! `internal/protocol.ID`.
//!
//! JSON marshaling parity (`internal/protocol/id.go:171-173, 105-107`):
//! `MarshalJSON` emits `hex.EncodeToString(id[:])` — a 40-char **lowercase**
//! hex string. Because it is a fixed-size `[20]byte` **array**, Go's
//! `encoding/json` never treats it as empty for `omitempty`, so a zero id is
//! always serialized as 40 hex zeros (contract §1.2, the classic Rust-port
//! trap that a naive serde `Option` would wrongly omit).

use serde::{Serialize, Serializer};

/// A 20-byte info-hash. Serializes to a 40-char lowercase hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolId([u8; 20]);

impl ProtocolId {
    /// The zero info-hash (`"0000…0000"`, 40 hex zeros).
    #[must_use]
    pub const fn zero() -> Self {
        Self([0u8; 20])
    }

    /// Parse a 40-char hex string into a `ProtocolId`.
    ///
    /// # Errors
    /// Returns an error if `s` is not exactly 20 bytes of hex.
    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let mut bytes = [0u8; 20];
        hex::decode_to_slice(s, &mut bytes)?;
        Ok(Self(bytes))
    }
}

impl Serialize for ProtocolId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(self.0))
    }
}
