//! `ProtocolId` — the 20-byte BitTorrent info-hash, mirroring Go's
//! `internal/protocol.ID`.
//!
//! JSON marshaling parity (`internal/protocol/id.go:171-173, 105-107`):
//! `MarshalJSON` emits `hex.EncodeToString(id[:])` — a 40-char **lowercase**
//! hex string. Because it is a fixed-size `[20]byte` **array**, Go's
//! `encoding/json` never treats it as empty for `omitempty`, so a zero id is
//! always serialized as 40 hex zeros (contract §1.2, the classic Rust-port
//! trap that a naive serde `Option` would wrongly omit).

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A 20-byte info-hash. Serializes to a 40-char lowercase hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolId([u8; 20]);

impl ProtocolId {
    /// The zero info-hash (`"0000…0000"`, 40 hex zeros).
    #[must_use]
    pub const fn zero() -> Self {
        Self([0u8; 20])
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
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

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl TryFrom<&[u8]> for ProtocolId {
    type Error = usize;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let length = value.len();
        let bytes = <[u8; 20]>::try_from(value).map_err(|_| length)?;
        Ok(Self::from_bytes(bytes))
    }
}

impl Serialize for ProtocolId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ProtocolId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::ProtocolId;

    #[test]
    fn json_round_trip_preserves_the_go_wire_shape() {
        let raw = "\"0123456789abcdef0123456789abcdef01234567\"";
        let id: ProtocolId = serde_json::from_str(raw).expect("valid protocol id");
        assert_eq!(serde_json::to_string(&id).unwrap(), raw);
    }

    #[test]
    fn json_decode_rejects_non_twenty_byte_hex() {
        assert!(serde_json::from_str::<ProtocolId>("\"short\"").is_err());
    }

    #[test]
    fn database_bytes_require_exactly_twenty_bytes() {
        assert_eq!(ProtocolId::try_from([7_u8; 19].as_slice()), Err(19));
        let id = ProtocolId::try_from([7_u8; 20].as_slice()).expect("20-byte protocol id");
        assert_eq!(id.as_bytes(), &[7_u8; 20]);
        assert_eq!(ProtocolId::try_from([7_u8; 21].as_slice()), Err(21));
    }
}
