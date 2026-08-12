use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A BitTorrent node ID or v1 info hash.
#[derive(Clone, Copy, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id20([u8; 20]);

impl Id20 {
    pub const ZERO: Self = Self([0; 20]);

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn from_slice(value: &[u8]) -> Result<Self, CompactCodecError> {
        let bytes: [u8; 20] = value
            .try_into()
            .map_err(|_| CompactCodecError::InvalidIdLength(value.len()))?;
        Ok(Self(bytes))
    }

    pub fn from_hex(value: &str) -> Result<Self, CompactCodecError> {
        if value.len() != 40
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(CompactCodecError::InvalidIdHex);
        }
        let decoded = hex::decode(value).map_err(|_| CompactCodecError::InvalidIdHex)?;
        Self::from_slice(&decoded)
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for Id20 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Id20").field(&self.to_hex()).finish()
    }
}

impl fmt::Display for Id20 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl FromStr for Id20 {
    type Err = CompactCodecError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl Serialize for Id20 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Id20 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// An IP address and big-endian port in BEP compact form.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactAddr {
    pub ip: IpAddr,
    pub port: u16,
}

impl CompactAddr {
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut output = Vec::with_capacity(match self.ip {
            IpAddr::V4(_) => 6,
            IpAddr::V6(_) => 18,
        });
        match self.ip {
            IpAddr::V4(ip) => output.extend_from_slice(&ip.octets()),
            IpAddr::V6(ip) => output.extend_from_slice(&ip.octets()),
        }
        output.extend_from_slice(&self.port.to_be_bytes());
        output
    }

    pub fn decode(value: &[u8]) -> Result<Self, CompactCodecError> {
        let (ip, port_bytes) = match value.len() {
            6 => (
                IpAddr::V4(Ipv4Addr::new(value[0], value[1], value[2], value[3])),
                &value[4..],
            ),
            18 => {
                let octets: [u8; 16] = value[..16]
                    .try_into()
                    .expect("length branch guarantees sixteen IP bytes");
                (IpAddr::V6(Ipv6Addr::from(octets)), &value[16..])
            }
            length => return Err(CompactCodecError::InvalidAddressLength(length)),
        };
        let port = u16::from_be_bytes(
            port_bytes
                .try_into()
                .expect("length branch guarantees two port bytes"),
        );
        Ok(Self { ip, port })
    }
}

/// One compact node entry: 20-byte node ID followed by a compact address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactNode {
    pub id: Id20,
    pub addr: CompactAddr,
}

impl CompactNode {
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let address = self.addr.encode();
        let mut output = Vec::with_capacity(20 + address.len());
        output.extend_from_slice(self.id.as_bytes());
        output.extend_from_slice(&address);
        output
    }

    pub fn decode(value: &[u8]) -> Result<Self, CompactCodecError> {
        if !matches!(value.len(), 26 | 38) {
            return Err(CompactCodecError::InvalidNodeLength(value.len()));
        }
        Ok(Self {
            id: Id20::from_slice(&value[..20])?,
            addr: CompactAddr::decode(&value[20..])?,
        })
    }
}

pub(crate) fn encode_nodes(
    nodes: &[CompactNode],
    ipv6: bool,
) -> Result<Vec<u8>, CompactCodecError> {
    let mut output = Vec::with_capacity(nodes.len() * if ipv6 { 38 } else { 26 });
    for node in nodes {
        if node.addr.ip.is_ipv6() != ipv6 {
            return Err(CompactCodecError::WrongAddressFamily {
                expected: if ipv6 { "IPv6" } else { "IPv4" },
            });
        }
        output.extend_from_slice(&node.encode());
    }
    Ok(output)
}

pub(crate) fn decode_nodes(
    value: &[u8],
    ipv6: bool,
) -> Result<Vec<CompactNode>, CompactCodecError> {
    let width = if ipv6 { 38 } else { 26 };
    if !value.len().is_multiple_of(width) {
        return Err(CompactCodecError::MisalignedNodeList {
            length: value.len(),
            width,
        });
    }
    value.chunks_exact(width).map(CompactNode::decode).collect()
}

pub(crate) fn encode_samples(samples: &[Id20]) -> Vec<u8> {
    let mut output = Vec::with_capacity(samples.len() * 20);
    for sample in samples {
        output.extend_from_slice(sample.as_bytes());
    }
    output
}

pub(crate) fn decode_samples(value: &[u8]) -> Result<Vec<Id20>, CompactCodecError> {
    if !value.len().is_multiple_of(20) {
        return Err(CompactCodecError::MisalignedInfoHashes(value.len()));
    }
    value.chunks_exact(20).map(Id20::from_slice).collect()
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompactCodecError {
    #[error("20-byte ID has length {0}")]
    InvalidIdLength(usize),
    #[error("20-byte ID is not lowercase hexadecimal")]
    InvalidIdHex,
    #[error("compact address has length {0}; expected 6 or 18")]
    InvalidAddressLength(usize),
    #[error("compact node has length {0}; expected 26 or 38")]
    InvalidNodeLength(usize),
    #[error("compact node list length {length} is not divisible by {width}")]
    MisalignedNodeList { length: usize, width: usize },
    #[error("compact info-hash list length {0} is not divisible by 20")]
    MisalignedInfoHashes(usize),
    #[error("compact entry has the wrong address family; expected {expected}")]
    WrongAddressFamily { expected: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_addresses_and_nodes_round_trip() {
        for addr in [
            CompactAddr {
                ip: "1.2.3.4".parse().unwrap(),
                port: 0x1234,
            },
            CompactAddr {
                ip: "2001:db8::1".parse().unwrap(),
                port: 0xabcd,
            },
        ] {
            assert_eq!(CompactAddr::decode(&addr.encode()).unwrap(), addr);
            let node = CompactNode {
                id: Id20::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap(),
                addr,
            };
            assert_eq!(CompactNode::decode(&node.encode()).unwrap(), node);
        }
    }

    #[test]
    fn malformed_compact_values_fail_closed() {
        assert!(Id20::from_hex("0123456789ABCDEF0123456789ABCDEF01234567").is_err());
        assert!(Id20::from_hex("00").is_err());
        for length in [0, 19, 21] {
            assert!(Id20::from_slice(&vec![0; length]).is_err());
        }
        for length in [0, 1, 2, 5, 7, 17, 19] {
            assert!(CompactAddr::decode(&vec![0; length]).is_err());
        }
        for length in [0, 20, 25, 27, 37, 39] {
            assert!(CompactNode::decode(&vec![0; length]).is_err());
        }
        assert!(matches!(
            decode_nodes(&[0; 27], false),
            Err(CompactCodecError::MisalignedNodeList { .. })
        ));
        assert!(matches!(
            decode_samples(&[0; 21]),
            Err(CompactCodecError::MisalignedInfoHashes(21))
        ));
        assert!(decode_samples(&[0; 19]).is_err());
        for (ipv6, width) in [(false, 26), (true, 38)] {
            for length in [width - 1, width + 1, width * 2 + 1] {
                assert!(decode_nodes(&vec![0; length], ipv6).is_err());
            }
        }
    }

    #[test]
    fn compact_ports_cover_wire_boundaries() {
        for ip in [
            "0.0.0.0",
            "255.255.255.255",
            "::",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        ] {
            for port in [0, u16::MAX] {
                let addr = CompactAddr {
                    ip: ip.parse().unwrap(),
                    port,
                };
                assert_eq!(CompactAddr::decode(&addr.encode()).unwrap(), addr);
            }
        }
    }

    #[test]
    fn node_lists_reject_mixed_address_families() {
        let node = CompactNode {
            id: Id20::ZERO,
            addr: CompactAddr {
                ip: "::1".parse().unwrap(),
                port: 1,
            },
        };
        assert!(matches!(
            encode_nodes(&[node], false),
            Err(CompactCodecError::WrongAddressFamily { expected: "IPv4" })
        ));
    }
}
